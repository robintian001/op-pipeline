"""时间算子测试（M8-4 Task 10）：time.parse / time.shift / time.now。

运行：``python -m unittest discover -s tests -k test_datetime_ops -v``
"""
from __future__ import annotations

import sys
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from op_pipeline import (  # noqa: E402
    PipelineSpec,
    RunContext,
    default_registry,
    run_pipeline,
)


def _make_ctx(op_name: str):
    """构造最小 NodeContext（与既有算子测试同一套路）。"""
    run_ctx = RunContext.create(pipeline="t", params={}, workdir=tempfile.gettempdir())
    return run_ctx.for_node("n", op_name)


def _run(name: str, inputs: dict | None = None, **params) -> dict:
    op_ = default_registry.get(name)(**params)
    return op_.execute(ctx=_make_ctx(name), inputs=inputs or {})


class TimeParseTest(unittest.TestCase):
    """time.parse：ISO / 自定义格式 / 非法输入。"""

    def test_parse_iso(self):
        out = _run("time.parse", {"text": "2024-06-01T08:30:00"})
        self.assertEqual(out["datetime"], "2024-06-01T08:30:00")
        self.assertIsInstance(out["ts"], float)
        # ts 与 datetime 一致（朴素时间按本地时区解释，往返应相等）
        self.assertEqual(datetime.fromtimestamp(out["ts"]), datetime(2024, 6, 1, 8, 30))

    def test_parse_iso_with_offset(self):
        out = _run("time.parse", {"text": "2024-06-01T08:30:00+08:00"})
        dt = datetime.fromisoformat(out["datetime"])
        self.assertEqual(dt.utcoffset(), timedelta(hours=8))
        # aware 时间戳与 datetime 一致：按原时区还原应得到同一时刻
        self.assertEqual(datetime.fromtimestamp(out["ts"], tz=dt.tzinfo), dt)

    def test_parse_with_format(self):
        out = _run("time.parse", {"text": "2024/06/01 08:30"}, format="%Y/%m/%d %H:%M")
        self.assertEqual(out["datetime"], "2024-06-01T08:30:00")

    def test_parse_illegal_text_raises_value_error(self):
        with self.assertRaises(ValueError):
            _run("time.parse", {"text": "not-a-time"})

    def test_parse_format_mismatch_raises_value_error(self):
        with self.assertRaises(ValueError):
            _run("time.parse", {"text": "2024-06-01"}, format="%Y/%m/%d")

    def test_parse_empty_text_raises_value_error(self):
        with self.assertRaises(ValueError):
            _run("time.parse", {"text": "  "})


class TimeShiftTest(unittest.TestCase):
    """time.shift：ISO 文本与 ts 双输入、偏移正确性。"""

    def test_shift_from_iso_text(self):
        out = _run("time.shift", {"text": "2024-06-01T08:30:00"},
                   days=1, hours=2, minutes=3, seconds=4)
        self.assertEqual(out["datetime"], "2024-06-02T10:33:04")

    def test_shift_from_ts(self):
        ts = datetime(2024, 6, 1, 8, 30).timestamp()
        out = _run("time.shift", {"ts": ts}, minutes=90)
        self.assertEqual(out["datetime"], "2024-06-01T10:00:00")
        self.assertAlmostEqual(out["ts"], ts + 90 * 60, places=6)

    def test_shift_accepts_int_ts(self):
        out = _run("time.shift", {"ts": 1_700_000_000}, seconds=1)
        self.assertAlmostEqual(out["ts"], 1_700_000_001.0, places=6)

    def test_shift_negative(self):
        out = _run("time.shift", {"text": "2024-06-02T00:00:00"}, days=-1)
        self.assertEqual(out["datetime"], "2024-06-01T00:00:00")

    def test_shift_zero_is_identity(self):
        out = _run("time.shift", {"text": "2024-06-01T08:30:00"})
        self.assertEqual(out["datetime"], "2024-06-01T08:30:00")
        self.assertEqual(out["ts"], datetime(2024, 6, 1, 8, 30).timestamp())

    def test_shift_missing_both_inputs_raises(self):
        with self.assertRaises(ValueError):
            _run("time.shift")

    def test_shift_illegal_text_raises(self):
        with self.assertRaises(ValueError):
            _run("time.shift", {"text": "garbage"}, days=1)


class TimeNowTest(unittest.TestCase):
    """time.now：三视图字段与 deterministic=False。"""

    def test_now_outputs(self):
        before = datetime.now()
        out = _run("time.now")
        after = datetime.now()
        dt = datetime.fromisoformat(out["datetime"])
        self.assertTrue(before <= dt.replace(tzinfo=None) <= after)
        self.assertGreaterEqual(out["ts"], before.timestamp())
        self.assertLessEqual(out["ts"], after.timestamp())
        self.assertRegex(out["date"], r"^\d{4}-\d{2}-\d{2}$")
        self.assertEqual(out["date"], out["datetime"][:10])

    def test_now_is_nondeterministic(self):
        cls = default_registry.get("time.now")
        self.assertFalse(cls.deterministic, "time.now 涉及外部状态，不得缓存")

    def test_now_never_lands_in_cache(self):
        # 对照实验：同一流水线里 const（deterministic）第二次命中缓存，
        # now（deterministic=False）即使配置 cache=True 也不落缓存。
        spec = PipelineSpec.from_dict({
            "name": "t",
            "nodes": [
                {"id": "c", "op": "control.const", "params": {"value": 1}, "cache": True},
                {"id": "now", "op": "time.now", "cache": True},
            ],
        })
        with tempfile.TemporaryDirectory() as tmp:
            first, _ = run_pipeline(spec, registry=default_registry, workdir=tmp, cache_dir=f"{tmp}/cache")
            second, _ = run_pipeline(spec, registry=default_registry, workdir=tmp, cache_dir=f"{tmp}/cache")
        self.assertFalse(first.node("now").cached)
        self.assertFalse(second.node("now").cached, "time.now 不得命中缓存")
        self.assertTrue(second.node("c").cached, "对照节点（deterministic）应命中缓存")


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
