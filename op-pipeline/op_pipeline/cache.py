"""中间结果缓存：按内容指纹复用节点输出。

命中条件 = 算子名 + 算子版本 + 解析后的参数 + 上游输入指纹 全部一致。
这样"改了下游逻辑不影响上游重算"，"上游数据变了自动失效"。
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import pickle
import threading
from pathlib import Path
from typing import Any

log = logging.getLogger("op_pipeline.cache")


def fingerprint(value: Any, _depth: int = 0) -> str:
    """对任意值做稳定指纹（无法序列化时退化为 repr）。"""
    if _depth > 6:
        return repr(value)[:200]
    try:
        if isinstance(value, (str, int, float, bool, type(None))):
            payload = f"{type(value).__name__}:{value!r}"
        elif isinstance(value, (list, tuple, set, frozenset)):
            items = sorted(value) if isinstance(value, (set, frozenset)) else value
            payload = type(value).__name__ + "[" + ",".join(
                fingerprint(v, _depth + 1) for v in items
            ) + "]"
        elif isinstance(value, dict):
            payload = "dict{" + ",".join(
                f"{k!r}:{fingerprint(v, _depth + 1)}" for k, v in sorted(value.items(), key=lambda kv: str(kv[0]))
            ) + "}"
        elif hasattr(value, "shape") and hasattr(value, "columns"):  # 类似 DataFrame
            payload = f"{type(value).__name__}:{value.shape}:{value.to_json()[:2000]}"
        else:
            payload = f"{type(value).__name__}:{value!r}"
    except Exception:  # pragma: no cover - 极端情况
        payload = f"{type(value).__name__}:<unhashable>"

    return hashlib.blake2b(payload.encode("utf-8", "replace"), digest_size=16).hexdigest()


class CacheStore:
    """极简文件缓存，空目录即关闭缓存。

    M7-4 Task 11：可选容量上限 ``max_entries`` / ``max_bytes``（None = 不限）。
    启用后每次写入按文件 mtime 升序淘汰最旧条目直至满足容量；命中读取时
    更新 mtime（LRU 语义）。未配置容量时行为与 v0.7.0 完全一致。
    同时提供按 key 的进程内单飞锁（``key_lock``），供引擎保证同 cache_key
    只有一个线程计算。
    """

    def __init__(
        self,
        directory: str | Path | None,
        base_dir: str | Path | None = None,
        include_source: bool = True,
        max_entries: int | None = None,
        max_bytes: int | None = None,
    ) -> None:
        for name, value in (("max_entries", max_entries), ("max_bytes", max_bytes)):
            if value is not None and value <= 0:
                raise ValueError(f"{name} 必须是正整数，实际是 {value!r}")
        path = Path(directory).expanduser() if directory else None
        # 相对缓存目录以 base_dir（通常为 workdir）为基准，而非进程 CWD：
        # 换个目录启动 CLI/库调用仍命中同一份缓存
        if path is not None and not path.is_absolute() and base_dir is not None:
            path = Path(base_dir).expanduser() / path
        self.directory = path
        # M2-5（T7 出债）：compute_key 纳入算子源码摘要——源码变化即缓存失效；
        # M5-4（Task 9）起默认开启（算子源码变更后默认参数重跑自动失效），
        # 显式 include_source=False 仍可关闭（保持旧行为，兼容不变）
        self.include_source = include_source
        self.max_entries = max_entries
        self.max_bytes = max_bytes
        # 单飞锁表：key -> Lock，元锁保护表本身（M7-4 Task 11）
        self._key_locks: dict[str, threading.Lock] = {}
        self._key_locks_guard = threading.Lock()
        if self.directory:
            self.directory.mkdir(parents=True, exist_ok=True)

    @property
    def enabled(self) -> bool:
        return self.directory is not None

    @property
    def limited(self) -> bool:
        """是否启用了容量上限（决定是否做 LRU mtime 更新与淘汰）。"""
        return self.max_entries is not None or self.max_bytes is not None

    def key_lock(self, key: str) -> threading.Lock:
        """取该 key 的进程内单飞锁（M7-4 Task 11）。

        同一 ``cache_key`` 的并发线程共享一把锁：抢到锁的线程先二次查缓存，
        再计算并写入，其余线程等待后直接读缓存——同 key 只计算一次。
        """
        with self._key_locks_guard:
            lock = self._key_locks.get(key)
            if lock is None:
                lock = threading.Lock()
                self._key_locks[key] = lock
            return lock

    def compute_key(
        self,
        op_name: str,
        op_version: str,
        params: dict[str, Any],
        inputs: dict[str, Any],
        source: str | None = None,
    ) -> str:
        payload = {
            "op": op_name,
            "version": op_version,
            "params": fingerprint(params),
            "inputs": {k: fingerprint(v) for k, v in inputs.items()},
        }
        if self.include_source and source is not None:
            payload["source"] = hashlib.blake2b(
                source.encode("utf-8", "replace"), digest_size=16
            ).hexdigest()
        return hashlib.blake2b(
            json.dumps(payload, sort_keys=True, ensure_ascii=False).encode("utf-8"),
            digest_size=20,
        ).hexdigest()

    def _path(self, key: str) -> Path:
        assert self.directory is not None
        return self.directory / f"{key}.pkl"

    def load(self, key: str) -> dict[str, Any] | None:
        if not self.enabled:
            return None
        path = self._path(key)
        if not path.exists():
            return None
        try:
            with path.open("rb") as fh:
                data = pickle.load(fh)
            if self.limited:
                # M7-4 Task 11：LRU——仅在启用容量上限时刷新 mtime，
                # 未配置容量时保持 v0.7.0 的默认行为（不触碰文件时间）
                try:
                    os.utime(path, None)
                except OSError:
                    pass
            log.debug("缓存命中 %s", key[:12])
            return data
        except Exception as exc:  # pragma: no cover
            log.warning("缓存文件损坏，忽略：%s (%s)", path, exc)
            return None

    def store(self, key: str, outputs: dict[str, Any]) -> None:
        if not self.enabled:
            return
        try:
            tmp = self._path(key).with_suffix(".tmp")
            with tmp.open("wb") as fh:
                pickle.dump(outputs, fh, protocol=pickle.HIGHEST_PROTOCOL)
            os.replace(tmp, self._path(key))
        except Exception as exc:  # pragma: no cover
            log.warning("缓存写入失败：%s", exc)
            return
        # M7-4 Task 11：写入后按容量淘汰（未配置容量时是空操作）
        self._evict()

    def usage(self) -> tuple[int, int]:
        """当前缓存目录的 (条目数, 总字节)，只统计 ``*.pkl``。"""
        entries = self._entries()
        return len(entries), sum(item[2] for item in entries)

    def _entries(self) -> list[tuple[Path, float, int]]:
        """列出缓存条目（路径, mtime, 字节），跳过临时文件与不可读项。"""
        assert self.directory is not None
        found: list[tuple[Path, float, int]] = []
        for path in self.directory.glob("*.pkl"):
            try:
                stat = path.stat()
            except OSError:
                continue
            found.append((path, stat.st_mtime, stat.st_size))
        return found

    def _evict(self) -> None:
        """按 mtime 升序删除最旧条目，直到满足条目数/字节上限（M7-4 Task 11）。

        只处理 ``*.pkl``——正在写入的 ``<key>.tmp`` 不会被误删。单个文件在
        Windows 上可能因句柄占用无法删除：跳过并在下一轮写入时再尝试。
        """
        if not self.limited or not self.enabled:
            return
        entries = sorted(self._entries(), key=lambda item: item[1])
        count = len(entries)
        total = sum(item[2] for item in entries)
        index = 0
        while index < len(entries):
            over_entries = self.max_entries is not None and count > self.max_entries
            over_bytes = self.max_bytes is not None and total > self.max_bytes
            if not (over_entries or over_bytes):
                break
            path, _mtime, size = entries[index]
            index += 1
            try:
                path.unlink()
            except FileNotFoundError:
                pass
            except OSError:  # pragma: no cover - Windows 句柄占用等瞬时失败
                continue
            count -= 1
            total -= size

    def clear(self) -> int:
        if not self.enabled:
            return 0
        count = 0
        for path in self.directory.glob("*.pkl"):
            path.unlink(missing_ok=True)
            count += 1
        return count
