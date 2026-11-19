"""句柄存储抽象：把"节点输出存在哪儿"与"引擎怎么消费"解耦。

句柄（handle）= 节点输出端口的默认名（`<node_id>.<port>`），是引用解析
`${...}` 的查表键。M2-3 之前引擎把它写死为 `Engine.handles` 内存字典；
抽象后写收敛在 `Engine._publish`（put）、读收敛在 `RefResolver._lookup`
（snapshot 后按 Mapping 查）——两处收敛点保持不变。

`InMemoryHandleStore` 平移原字典行为（加 RLock，跨节点并发发布安全）；
`SpillHandleStore`（M2-4）在值超过阈值时 pickle 落盘、读取时按需加载；
`SharedHandleStore`（M5-1）恒落盘的跨进程共享目录实现——远程 worker 与引擎
经同一目录交换句柄值，不可 pickle 的值直接抛 ``RuntimeError``（绝不静默回退）。
自定义实现注意：

1. `snapshot()` 应返回一次性浅拷贝——引用解析期间其它线程可能并发 put；
2. `get()` 缺失抛 `KeyError`（RefResolver 负责转成 RefResolutionError）；
3. `put()` 必须线程安全（引擎多节点并行发布）。
"""
from __future__ import annotations

import hashlib
import logging
import os
import pickle
import shutil
import threading
from pathlib import Path
from typing import Any, Iterable, Mapping, Protocol, runtime_checkable

logger = logging.getLogger("op_pipeline.handlestore")


@runtime_checkable
class HandleStore(Protocol):
    """句柄存取接口（M2-3）。"""

    def get(self, name: str) -> Any:
        """读取句柄值；缺失抛 KeyError。"""
        ...

    def put(self, name: str, value: Any) -> None:
        """写入/覆盖句柄值；须线程安全。"""
        ...

    def snapshot(self) -> Mapping[str, Any]:
        """一次性浅拷贝视图（引用解析、类型检查、报告汇总用）。"""
        ...

    def items(self) -> Iterable[tuple[str, Any]]:
        """键值对集合（遍历场景）。"""
        ...


def as_mapping(handles: Any) -> Mapping[str, Any]:
    """HandleStore / Mapping 归一：store 取 snapshot，Mapping 原样返回。"""
    if hasattr(handles, "snapshot"):
        return handles.snapshot()
    return handles or {}


class InMemoryHandleStore:
    """默认实现：RLock + dict，平移原 `Engine.handles` 字典行为。"""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._data: dict[str, Any] = {}

    def get(self, name: str) -> Any:
        with self._lock:
            return self._data[name]

    def put(self, name: str, value: Any) -> None:
        with self._lock:
            self._data[name] = value

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return dict(self._data)

    def items(self) -> list[tuple[str, Any]]:
        with self._lock:
            return list(self._data.items())


class _SpilledRef:
    """落盘句柄在表内的占位：文件路径 + blob 大小。"""

    __slots__ = ("path", "size")

    def __init__(self, path: Path, size: int) -> None:
        self.path = path
        self.size = size


class _LazySnapshot(Mapping):
    """惰性快照视图：`in`/len/迭代只查键，按键访问才对落盘值读盘。

    引用解析对快照的高频操作是成员检查（整键命中判断）与迭代；
    这两类必须零 IO，否则每个节点解析参数都会把全部大句柄加载回内存，
    落盘就失去了意义。只有真正取值时才付读盘成本。
    """

    def __init__(self, data: dict[str, Any]) -> None:
        self._data = data

    def __contains__(self, key: object) -> bool:
        return key in self._data

    def __getitem__(self, key: str) -> Any:
        ref = self._data[key]
        if isinstance(ref, _SpilledRef):
            return pickle.loads(ref.path.read_bytes())
        return ref

    def __iter__(self):
        return iter(self._data)

    def __len__(self) -> int:
        return len(self._data)


class SpillHandleStore:
    """超阈值落盘实现（M2-4）。

    `put` 时把值 pickle 一遍量大小：blob 超过 ``threshold``（字节）的写进
    落盘目录，表内只存 ``_SpilledRef``（路径 + 大小），读取（get / 快照按键
    访问）时按需 ``pickle.load``。小值与不可 pickle 的值留在内存，后者记
    warning；未指定落盘目录时同样退化为内存保留——落盘是内存优化，
    不承担正确性职责，绝不因它打断流水线。

    目录默认挂本次运行的 ``<artifacts_dir>/spill``：`Engine` 构造时对有
    ``attach(context)`` 钩子的实现注入 `RunContext`，生命周期与 run 目录
    一致（复跑 = 新 run 目录，互不干扰）。显式传 ``spill_dir`` 时以显式
    值为准，由调用方管理生命周期。

    文件名 = ``<句柄名>.<blake2b 摘要>.pkl``：摘要防大小写不敏感文件系统
    上的名字碰撞（Windows 把 ``A.x`` / ``a.x`` 视为同一文件）。

    ``cleanup``（M7-4 Task 10）控制运行结束后的落盘目录清理：

    - ``"never"``（默认，兼容 v0.7.0）：目录保留，行为不变；
    - ``"on_run_success"``：本次运行成功时由引擎删除 attach 注入的
      落盘目录（失败/取消保留，便于排障）。显式传 ``spill_dir`` 时目录
      生命周期归调用方，引擎不会删除。
    """

    _CLEANUP_POLICIES = ("never", "on_run_success")

    def __init__(
        self,
        *,
        threshold: int = 1_000_000,
        spill_dir: str | os.PathLike | None = None,
        cleanup: str = "never",
    ) -> None:
        if cleanup not in self._CLEANUP_POLICIES:
            raise ValueError(
                "cleanup 只能是 'never' 或 'on_run_success'，"
                f"实际是 {cleanup!r}"
            )
        self._lock = threading.RLock()
        self._threshold = int(threshold)
        self._cleanup = cleanup
        self._data: dict[str, Any] = {}
        self._explicit_dir = spill_dir is not None
        self._spill_dir = (
            Path(spill_dir).expanduser() if spill_dir is not None else None
        )
        # attach 注入的目录（≠ 显式 spill_dir）：清理只针对它，避免误删调用方目录
        self._attached_dir: Path | None = None

    @property
    def spill_dir(self) -> Path | None:
        """当前落盘目录（attach 前可能为 None）。"""
        return self._spill_dir

    @property
    def cleanup(self) -> str:
        """清理策略（``never`` / ``on_run_success``）。"""
        return self._cleanup

    def attach(self, context: Any) -> None:
        """`Engine` 构造时注入运行上下文：未显式指定目录则挂 artifacts_dir。

        M7-4 Task 10：同一 store 复用多次运行时重新挂到本次 run 的目录，
        上次 run 的目录不会被误删（清理只删本次 attach 的目录）。
        """
        if self._explicit_dir:
            return
        base = getattr(context, "artifacts_dir", None)
        if base is not None:
            self._spill_dir = Path(base) / "spill"
            self._attached_dir = self._spill_dir

    def cleanup_run_dir(self) -> None:
        """删除本次运行 attach 的落盘目录（M7-4 Task 10）。

        只删 attach 注入的目录（显式 ``spill_dir`` 归调用方管理）；删除前
        先把落盘句柄加载回内存，保证运行结束后 store 仍可读（``run_pipeline``
        会在 run 结束后物化全量快照）。失败/取消时调用方不调用本方法。
        """
        target = self._attached_dir
        if target is None:
            return
        self._attached_dir = None
        with self._lock:
            for name, ref in list(self._data.items()):
                if not isinstance(ref, _SpilledRef):
                    continue
                try:
                    self._data[name] = pickle.loads(ref.path.read_bytes())
                except OSError:
                    # 文件已不在：保持原引用，读取时按既有语义报错
                    continue
        if target.is_dir():
            shutil.rmtree(target, ignore_errors=False)

    def put(self, name: str, value: Any) -> None:
        try:
            blob = pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)
        except Exception:
            logger.warning("句柄 %s 的值不可 pickle，SpillHandleStore 退化为内存保留", name)
            with self._lock:
                self._data[name] = value
            return
        if len(blob) <= self._threshold:
            with self._lock:
                self._data[name] = value
            return
        target = self._spill_dir
        if target is None:
            logger.warning(
                "句柄 %s 超过落盘阈值但未指定落盘目录，退化为内存保留", name
            )
            with self._lock:
                self._data[name] = value
            return
        target.mkdir(parents=True, exist_ok=True)
        digest = hashlib.blake2b(name.encode("utf-8"), digest_size=8).hexdigest()
        path = target / f"{name}.{digest}.pkl"
        path.write_bytes(blob)
        with self._lock:
            self._data[name] = _SpilledRef(path, len(blob))

    def get(self, name: str) -> Any:
        with self._lock:
            ref = self._data[name]  # 缺失抛 KeyError，由 RefResolver 转语义
        if isinstance(ref, _SpilledRef):
            return pickle.loads(ref.path.read_bytes())
        return ref

    def snapshot(self) -> Mapping[str, Any]:
        """惰性快照：键集即时拷贝，值（含落盘项）在访问时才加载。

        与协议文档"一次性浅拷贝"的语义一致——引用解析期间的并发 put 不影响
        本视图；落盘项仅在真正取值时读盘。
        """
        with self._lock:
            return _LazySnapshot(dict(self._data))

    def items(self) -> list[tuple[str, Any]]:
        """键值对列表（遍历场景，落盘项在此时加载）。"""
        with self._lock:
            names = list(self._data)
        return [(name, self.get(name)) for name in names]


class SharedHandleStore(SpillHandleStore):
    """跨进程共享目录句柄存储（M5-1）：值恒落盘、按需读盘。

    远程执行的基础设施：worker 进程把节点输出写进共享目录，引擎进程（另一个
    实例）从同一目录读取。与 ``SpillHandleStore`` 的区别：

    1. ``threshold=0``——值恒落盘，没有内存保留的分支；
    2. 不可 pickle 的值直接抛 ``RuntimeError``（跨进程只有落盘一条路，绝不
       像 SpillHandleStore 那样退化为内存保留——那会让远端永远读不到）；
    3. ``get``/``snapshot`` 会扫描共享目录——其它进程写好的句柄文件即使本地
       视图没有也能读到（跨进程交换的关键）。

    文件名 = ``<句柄名>.<blake2b 摘要>.pkl``，先写 ``.tmp`` 再原子 ``replace``，
    worker 侧读取永远不会看到半个文件（与 ``remote.consume_jobs`` 的结果回写
    同一套原子语义）。
    """

    def __init__(self, shared_dir: str | os.PathLike) -> None:
        super().__init__(threshold=0, spill_dir=shared_dir)

    @property
    def shared_dir(self) -> Path:
        """共享目录（构造时即确定，必非 None）。"""
        assert self._spill_dir is not None
        return self._spill_dir

    def put(self, name: str, value: Any) -> None:
        try:
            blob = pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)
        except Exception as exc:
            # 跨进程没有内存退化的余地：不可 pickle 的值无法传给远端，直接报错
            raise RuntimeError(
                f"句柄 {name} 的值不可 pickle，无法写入共享目录：{exc}"
            ) from exc
        target = self._spill_dir
        assert target is not None
        target.mkdir(parents=True, exist_ok=True)
        digest = hashlib.blake2b(name.encode("utf-8"), digest_size=8).hexdigest()
        path = target / f"{name}.{digest}.pkl"
        tmp = path.with_name(path.name + ".tmp")
        tmp.write_bytes(blob)
        tmp.replace(path)  # 原子发布：读取方不会看到半个文件
        with self._lock:
            self._data[name] = _SpilledRef(path, len(blob))

    def _scan(self) -> dict[str, _SpilledRef]:
        """扫描共享目录，把其它进程写好的句柄文件收进本地视图。"""
        target = self._spill_dir
        if target is None or not target.is_dir():
            return {}
        found: dict[str, _SpilledRef] = {}
        for path in target.iterdir():
            if path.suffix != ".pkl" or path.name.endswith(".tmp"):
                continue  # 跳过 .tmp 与无关文件
            stem = path.stem  # <name>.<digest>
            if len(stem) <= 16:
                continue
            digest = stem[-16:]
            name = stem[:-17]
            if hashlib.blake2b(name.encode("utf-8"), digest_size=8).hexdigest() != digest:
                continue  # 摘要不匹配：不是本存储写的文件，忽略
            found[name] = _SpilledRef(path, path.stat().st_size)
        return found

    def get(self, name: str) -> Any:
        with self._lock:
            ref = self._data.get(name)
        if ref is None:
            # 跨进程：本地视图可能没有，直接从共享目录读
            remote = self._scan().get(name)
            if remote is None:
                raise KeyError(name)
            ref = remote
        if isinstance(ref, _SpilledRef):
            return pickle.loads(ref.path.read_bytes())
        return ref

    def snapshot(self) -> Mapping[str, Any]:
        """惰性快照：本地视图 + 扫描目录合并（本地优先），取值时才读盘。"""
        with self._lock:
            data = dict(self._data)
        for name, ref in self._scan().items():
            data.setdefault(name, ref)
        return _LazySnapshot(data)

    def items(self) -> list[tuple[str, Any]]:
        """键值对列表（本地 + 扫描目录，落盘项在此时加载）。"""
        with self._lock:
            names = list(self._data)
        for name in self._scan():
            if name not in names:
                names.append(name)
        return [(name, self.get(name)) for name in names]
