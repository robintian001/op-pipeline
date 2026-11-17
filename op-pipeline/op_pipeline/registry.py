"""算子注册表与插件发现。

算子通过 ``@op("namespace.name")`` 自注册；流水线配置里写的是注册名，
因此"算子库"与"流水线定义"完全解耦，新增算子不需要改编排层。
"""
from __future__ import annotations

import difflib
import importlib
import importlib.util
import itertools
import sys
import threading
from contextvars import ContextVar
from pathlib import Path
from typing import Any, Iterator, Sequence

from .errors import DuplicateOperatorError, OperatorNotFoundError, PipelineError


class Registry:
    """算子注册表（名字 -> 算子类）。

    内部状态由 ``RLock`` 保护，可安全地多线程并发注册/查询/加载插件。
    """

    def __init__(self) -> None:
        self._ops: dict[str, type] = {}
        self._aliases: dict[str, str] = {}
        # 已加载进本表的插件文件（绝对路径），用于"同表去重、跨表重执行"
        self._plugin_files: set[str] = set()
        # 保护 _ops/_aliases/_plugin_files 的重入锁：
        # 并发注册/并发加载插件时保持"检查-写入"的原子性
        self._lock = threading.RLock()

    # ------------------------------------------------------------------ #
    def register(
        self,
        cls: type,
        name: str | None = None,
        *,
        aliases: Sequence[str] = (),
        replace: bool = False,
    ) -> type:
        from .operator import normalize_operator_class  # 延迟导入避免循环

        cls = normalize_operator_class(cls)
        op_name = name or getattr(cls, "name", "") or cls.__name__
        if not op_name:
            raise PipelineError(f"算子 {cls!r} 缺少 name")

        with self._lock:
            if op_name in self._ops and not replace:
                existing = self._ops[op_name]
                raise DuplicateOperatorError(
                    f"算子名 '{op_name}' 已被 {existing.__module__}.{existing.__qualname__} 占用；"
                    "如需覆盖请传 replace=True"
                )
            cls.name = op_name
            self._ops[op_name] = cls
            for alias in aliases:
                self._aliases[str(alias)] = op_name
        return cls

    # ------------------------------------------------------------------ #
    def canonical_name(self, name: str) -> str:
        with self._lock:
            return self._aliases.get(name, name)

    def get(self, name: str) -> type:
        with self._lock:
            canonical = self._aliases.get(name, name)
            if canonical not in self._ops:
                close = difflib.get_close_matches(name, self._ops.keys(), n=3, cutoff=0.6)
                hint = f"；你是否想用：{', '.join(close)}" if close else ""
                raise OperatorNotFoundError(f"未注册的算子 '{name}'{hint}")
            return self._ops[canonical]

    def __contains__(self, name: str) -> bool:
        with self._lock:
            return self._aliases.get(name, name) in self._ops

    def __iter__(self) -> Iterator[str]:
        return iter(self.names())

    def __len__(self) -> int:
        with self._lock:
            return len(self._ops)

    def names(self) -> list[str]:
        with self._lock:
            return sorted(self._ops)

    def describe(self) -> list[dict[str, Any]]:
        with self._lock:
            classes = [self._ops[n] for n in sorted(self._ops)]
        # 在锁外调用算子类的 describe，避免持锁执行用户代码
        return [cls.describe() for cls in classes]

    def clear(self) -> None:
        with self._lock:
            self._ops.clear()
            self._aliases.clear()
            self._plugin_files.clear()

    # ------------------------------------------------------------------ #
    # 插件加载槽位（内部：load_plugins 使用，保证"同表去重"的原子性）
    def _occupy_plugin_slot(self, key: str) -> bool:
        """原子占位：该插件文件未加载过则登记并返回 True；已加载过返回 False。"""
        with self._lock:
            if key in self._plugin_files:
                return False
            self._plugin_files.add(key)
            return True

    def _release_plugin_slot(self, key: str) -> None:
        """插件文件执行失败时回滚占位，下次调用可以重新执行。"""
        with self._lock:
            self._plugin_files.discard(key)


default_registry = Registry()

# 环境注册表：load_plugins(registry=...) 加载插件文件期间生效，
# 使插件源码里的裸 @op("x.y") 落到目标注册表而非全局表（contextvar，线程/任务隔离）。
_ambient_registry: ContextVar[Registry | None] = ContextVar(
    "op_pipeline_ambient_registry", default=None
)

# 独立注册表重执行插件文件的模块名序号（每注册表独立去重，跨注册表各执行一次）
_plugin_seq = itertools.count()


def _copy_builtin_ops(target: Registry) -> None:
    """把内置算子补齐到目标注册表。

    内置算子包通常在 import op_pipeline 时已执行过，注册只落在 default_registry；
    对独立注册表按模块前缀识别内置算子并复制（已存在的名字不覆盖）。
    """
    for name in default_registry.names():
        cls = default_registry.get(name)
        if cls.__module__.startswith("op_pipeline.ops") and name not in target:
            target.register(cls, replace=True)


def op(
    name: str | type | None = None,
    *,
    aliases: Sequence[str] = (),
    replace: bool = False,
    registry: Registry | None = None,
):
    """算子注册装饰器。

    用法::

        @op("table.filter")
        class Filter(Operator): ...

        @op(aliases=["mean"])          # 名字沿用类名
        class Average(Operator): ...
    """

    def decorator(cls: type) -> type:
        # 显式 None 判断：空注册表 len==0 为 falsy，不能用 `or` 短路
        target = registry if registry is not None else _ambient_registry.get() or default_registry
        target.register(cls, None if isinstance(name, type) else name, aliases=aliases, replace=replace)
        return cls

    if isinstance(name, type):  # 直接 @op 用在类上
        return decorator(name)
    return decorator


# --------------------------------------------------------------------------- #
# 插件加载
# --------------------------------------------------------------------------- #
def load_plugins(
    modules: Sequence[str] = (),
    paths: Sequence[str | Path] = (),
    *,
    base_dir: str | Path | None = None,
    package: str | None = "op_pipeline.ops",
    registry: Registry | None = None,
) -> list[str]:
    """加载算子插件。

    :param modules: 模块路径列表，如 ``["my_company.ops"]``
    :param paths: 文件路径列表，如 ``["examples/custom_ops.py"]``（相对 ``base_dir`` 解析）
    :param base_dir: 解析相对 ``paths`` 的根目录
    :param package: 需要导入的内置算子包（默认内置算子库）
    :param registry: 目标注册表；None 时注册到 ``default_registry``。
        指定后：加载期间插件源码里的裸 ``@op`` 落到该表、内置算子自动补齐、
        同一插件文件可在不同注册表各执行一次（模块级状态互不影响）。
        注意 ``modules`` 受 Python import 幂等性限制：已导入过的模块不会重新执行。
    :return: 本次实际加载的模块名列表
    """
    loaded: list[str] = []
    token = _ambient_registry.set(registry) if registry is not None else None
    try:
        if package:
            try:
                importlib.import_module(package)
                loaded.append(package)
            except ImportError:  # 内置包缺失时不应阻断自定义插件
                pass
            if registry is not None:
                _copy_builtin_ops(registry)

        for mod in modules:
            importlib.import_module(mod)
            loaded.append(mod)

        root = Path(base_dir).expanduser() if base_dir else None
        for raw in paths:
            path = Path(raw).expanduser()
            if not path.is_absolute() and root is not None:
                path = root / path
            path = path.resolve()
            if not path.exists():
                raise PipelineError(f"插件文件不存在：{path}")
            key = str(path)
            if registry is not None:
                # 同表去重（原子占位，并发加载同一文件只执行一次）；
                # 跨表重执行：其它注册表可各执行一次（模块级状态互不影响）
                if not registry._occupy_plugin_slot(key):
                    loaded.append(key)
                    continue
                mod_name = f"_op_plugin_{path.stem}_{next(_plugin_seq)}"
            else:
                mod_name = f"_op_plugin_{path.stem}"
                if mod_name in sys.modules:
                    loaded.append(key)
                    continue
            spec = importlib.util.spec_from_file_location(mod_name, path)
            if spec is None or spec.loader is None:  # pragma: no cover
                if registry is not None:
                    registry._release_plugin_slot(key)
                raise PipelineError(f"无法加载插件文件：{path}")
            module = importlib.util.module_from_spec(spec)
            sys.modules[mod_name] = module
            try:
                spec.loader.exec_module(module)
            except BaseException:
                if registry is not None:
                    registry._release_plugin_slot(key)  # 失败回滚占位，下次可重试
                raise
            loaded.append(key)
    finally:
        if token is not None:
            _ambient_registry.reset(token)

    return loaded
