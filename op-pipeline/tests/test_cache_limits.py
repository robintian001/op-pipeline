"""M7-4 Task 11：CacheStore 容量上限（条目/字节）与同 key 单飞。

覆盖：条目数淘汰、字节淘汰、最旧先淘汰、淘汰后剩余条目可完整读取、
同 key 并发只计算一次、非法参数抛 ValueError、未配置容量时不淘汰。
"""
from __future__ import annotations

import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from op_pipeline import Operator, Param, PipelineSpec, default_registry, op, run_pipeline  # noqa: E402
from op_pipeline.cache import CacheStore  # noqa: E402

_COUNTER = {"count": 0}
_COUNTER_LOCK = threading.Lock()


@op("m74.count_once")
class CountOnce(Operator):
    """同参数节点应只执行一次（单飞）；计数器验证实际计算次数。"""

    outputs = [("value", str)]
    params = {"tag": Param(str, default="x")}

    def run(self, ctx, tag):
        with _COUNTER_LOCK:
            _COUNTER["count"] += 1
        time.sleep(0.2)
        return {"value": tag}


def spec_of(nodes, outputs=None) -> PipelineSpec:
    return PipelineSpec.from_dict(
        {"name": "m74cache", "params": {}, "nodes": nodes, "outputs": outputs or {}}
    )


class CacheLimitsTest(unittest.TestCase):
    def setUp(self):
        with _COUNTER_LOCK:
            _COUNTER["count"] = 0

    @staticmethod
    def _write(store: CacheStore, key: str, value, delay: float = 0.02) -> None:
        store.store(key, {"value": value})
        time.sleep(delay)  # 保证 mtime 可区分（淘汰按 mtime 升序）

    def test_evict_by_entries_keeps_newest(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = CacheStore(tmp, max_entries=3)
            for i in range(5):
                self._write(store, f"k{i}", i)
            self.assertEqual(len(list(Path(tmp).glob("*.pkl"))), 3)
            self.assertEqual(store.usage()[0], 3)
            # 最旧的两条被淘汰，最新的三条可完整读取
            self.assertIsNone(store.load("k0"))
            self.assertIsNone(store.load("k1"))
            for i in (2, 3, 4):
                self.assertEqual(store.load(f"k{i}"), {"value": i})

    def test_evict_by_bytes(self):
        payload = "x" * 300
        with tempfile.TemporaryDirectory() as tmp:
            store = CacheStore(tmp, max_bytes=900)
            for i in range(3):
                self._write(store, f"b{i}", {"blob": payload, "i": i})
            count, total = store.usage()
            self.assertEqual(count, 2, "总字节应淘汰到上限以内，只保留较新的两条")
            self.assertLessEqual(total, 900)
            self.assertIsNone(store.load("b0"), "最旧的一条应先被淘汰")
            self.assertIsNotNone(store.load("b1"))
            self.assertIsNotNone(store.load("b2"))

    def test_remaining_entries_readable_after_evict(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = CacheStore(tmp, max_entries=2)
            self._write(store, "old", {"items": list(range(100))})
            self._write(store, "mid", {"items": list(range(50))})
            self._write(store, "new", {"items": list(range(10))})
            self.assertEqual(store.load("mid")["value"]["items"], list(range(50)))
            self.assertEqual(store.load("new")["value"]["items"], list(range(10)))

    def test_no_limits_keeps_all(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = CacheStore(tmp)
            for i in range(5):
                self._write(store, f"n{i}", i)
            self.assertEqual(len(list(Path(tmp).glob("*.pkl"))), 5, "未配置容量时不应淘汰")
            for i in range(5):
                self.assertEqual(store.load(f"n{i}"), {"value": i})

    def test_invalid_limits_raise(self):
        for kwargs in (
            {"max_entries": 0},
            {"max_entries": -1},
            {"max_bytes": 0},
            {"max_bytes": -5},
        ):
            with self.subTest(**kwargs):
                with self.assertRaises(ValueError):
                    CacheStore(None, **kwargs)

    def test_same_key_concurrent_single_flight(self):
        with tempfile.TemporaryDirectory() as tmp:
            report, values = run_pipeline(
                spec_of(
                    [
                        {
                            "id": "a",
                            "op": "m74.count_once",
                            "params": {"tag": "same"},
                            "cache": True,
                        },
                        {
                            "id": "b",
                            "op": "m74.count_once",
                            "params": {"tag": "same"},
                            "cache": True,
                        },
                    ],
                    outputs={"v": "${a.value}"},
                ),
                registry=default_registry,
                workdir=tmp,
                cache_dir=str(Path(tmp) / "cache"),
                max_workers=2,
            )
            self.assertEqual(report.status, "success")
            with _COUNTER_LOCK:
                self.assertEqual(_COUNTER["count"], 1, "同 cache_key 只应计算一次")
            self.assertEqual(values["a.value"], "same")
            self.assertEqual(values["b.value"], "same")
            # 一个真实计算、一个命中缓存
            self.assertEqual(sum(1 for n in report.nodes if n.cached), 1)


if __name__ == "__main__":
    unittest.main()
