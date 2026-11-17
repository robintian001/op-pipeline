"""算子注册表与插件发现。

算子通过 ``@op("namespace.name")`` 自注册；流水线配置里写的是注册名，
因此"算子库"与"流水线定义"完全解耦，新增算子不需要改编排层。
"""
from __future__ import annotations

import difflib
import importlib
import importlib.util
import sys
from pathlib import Path
from typing import Any, Iterator, Sequence

from .errors import DuplicateOperatorError, OperatorNotFoundError, PipelineError


class Registry:
    """算子注册表（名字 -> 算子类）。"""

    def __init__(self) -> None:
        self._ops: dict[str, type] = {}
        self._aliases: dict[str, str] = {}

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
        return self._aliases.get(name, name)

    def get(self, name: str) -> type:
        canonical = self.canonical_name(name)
        if canonical not in self._ops:
            close = difflib.get_close_matches(name, self._ops.keys(), n=3, cutoff=0.6)
            hint = f"；你是否想用：{', '.join(close)}" if close else ""
            raise OperatorNotFoundError(f"未注册的算子 '{name}'{hint}")
        return self._ops[canonical]

    def __contains__(self, name: str) -> bool:
        return self.canonical_name(name) in self._ops

    def __iter__(self) -> Iterator[str]:
        return iter(sorted(self._ops))

    def __len__(self) -> int:
        return len(self._ops)

    def names(self) -> list[str]:
        return sorted(self._ops)

    def describe(self) -> list[dict[str, Any]]:
        return [self._ops[n].describe() for n in self.names()]

    def clear(self) -> None:
        self._ops.clear()
        self._aliases.clear()


default_registry = Registry()


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
        target = registry or default_registry
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
) -> list[str]:
    """加载算子插件。

    :param modules: 模块路径列表，如 ``["my_company.ops"]``
    :param paths: 文件路径列表，如 ``["examples/custom_ops.py"]``（相对 ``base_dir`` 解析）
    :param base_dir: 解析相对 ``paths`` 的根目录
    :param package: 需要导入的内置算子包（默认内置算子库）
    :return: 本次实际加载的模块名列表
    """
    loaded: list[str] = []

    if package:
        try:
            importlib.import_module(package)
            loaded.append(package)
        except ImportError:  # 内置包缺失时不应阻断自定义插件
            pass

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
        mod_name = f"_op_plugin_{path.stem}"
        if mod_name in sys.modules:
            loaded.append(str(path))
            continue
        spec = importlib.util.spec_from_file_location(mod_name, path)
        if spec is None or spec.loader is None:  # pragma: no cover
            raise PipelineError(f"无法加载插件文件：{path}")
        module = importlib.util.module_from_spec(spec)
        sys.modules[mod_name] = module
        spec.loader.exec_module(module)
        loaded.append(str(path))

    return loaded
