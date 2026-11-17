"""句柄存储抽象：把"节点输出存在哪儿"与"引擎怎么消费"解耦。

句柄（handle）= 节点输出端口的默认名（`<node_id>.<port>`），是引用解析
`${...}` 的查表键。M2-3 之前引擎把它写死为 `Engine.handles` 内存字典；
抽象后写收敛在 `Engine._publish`（put）、读收敛在 `RefResolver._lookup`
（snapshot 后按 Mapping 查）——两处收敛点保持不变。

`InMemoryHandleStore` 平移原字典行为（加 RLock，跨节点并发发布安全）；
`SpillHandleStore`（M2-4）在值超过阈值时 pickle 落盘、读取时按需加载。
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
    """

    def __init__(
        self,
        *,
        threshold: int = 1_000_000,
        spill_dir: str | os.PathLike | None = None,
    ) -> None:
        self._lock = threading.RLock()
        self._threshold = int(threshold)
        self._data: dict[str, Any] = {}
        self._spill_dir = (
            Path(spill_dir).expanduser() if spill_dir is not None else None
        )

    @property
    def spill_dir(self) -> Path | None:
        """当前落盘目录（attach 前可能为 None）。"""
        return self._spill_dir

    def attach(self, context: Any) -> None:
        """`Engine` 构造时注入运行上下文：未显式指定目录则挂 artifacts_dir。"""
        if self._spill_dir is None:
            base = getattr(context, "artifacts_dir", None)
            if base is not None:
                self._spill_dir = Path(base) / "spill"

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
