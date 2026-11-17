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
    """极简文件缓存，空目录即关闭缓存。"""

    def __init__(self, directory: str | Path | None) -> None:
        self.directory = Path(directory).expanduser() if directory else None
        if self.directory:
            self.directory.mkdir(parents=True, exist_ok=True)

    @property
    def enabled(self) -> bool:
        return self.directory is not None

    def compute_key(
        self,
        op_name: str,
        op_version: str,
        params: dict[str, Any],
        inputs: dict[str, Any],
    ) -> str:
        payload = {
            "op": op_name,
            "version": op_version,
            "params": fingerprint(params),
            "inputs": {k: fingerprint(v) for k, v in inputs.items()},
        }
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

    def clear(self) -> int:
        if not self.enabled:
            return 0
        count = 0
        for path in self.directory.glob("*.pkl"):
            path.unlink(missing_ok=True)
            count += 1
        return count
