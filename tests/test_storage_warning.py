"""M7-4 Task 12：缓存/spill 目录容量告警。

覆盖：超阈值有中文 warning（含目录与占用）、未超阈值无 warning、
告警不影响 status/outputs、阈值配置为 None 时静默。
"""
from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from op_pipeline import Operator, Param, PipelineSpec, default_registry, op, run_pipeline  # noqa: E402


@op("m74.storage_const")
class StorageConst(Operator):
    """纯常量节点：不产生任何落盘。"""

    outputs = [("value", int)]
    params = {"value": Param(int, default=7)}

    def run(self, ctx, value):
        return {"value": value}


@op("m74.storage_spill")
class StorageSpill(Operator):
    """向本次 run 的 artifacts/spill 目录写入指定大小的文件。"""

    outputs = [("value", int)]
    params = {"size": Param(int, default=0)}

    def run(self, ctx, size):
        target = Path(ctx.run.artifacts_dir) / "spill"
        target.mkdir(parents=True, exist_ok=True)
        (target / "blob.bin").write_bytes(b"x" * size)
        return {"value": size}


def spec_of(nodes, outputs=None) -> PipelineSpec:
    return PipelineSpec.from_dict(
        {"name": "m74warn", "params": {}, "nodes": nodes, "outputs": outputs or {}}
    )


class StorageWarningTest(unittest.TestCase):
    @staticmethod
    def _pipeline():
        return spec_of([{"id": "src", "op": "m74.storage_const"}], outputs={"v": "${src.value}"})

    def test_cache_over_limit_warns(self):
        with tempfile.TemporaryDirectory() as tmp:
            cache_dir = Path(tmp) / "cache"
            cache_dir.mkdir()
            (cache_dir / "stale.pkl").write_bytes(b"x" * 2000)
            report, values = run_pipeline(
                self._pipeline(),
                registry=default_registry,
                workdir=tmp,
                cache_dir=str(cache_dir),
                cache_max_bytes=100,
            )
        self.assertTrue(
            any("缓存目录" in w and str(cache_dir) in w for w in report.warnings),
            f"超阈值应追加缓存目录告警，实际：{report.warnings}",
        )
        # 告警不影响运行结果
        self.assertEqual(report.status, "success")
        self.assertEqual(values["src.value"], 7)

    def test_cache_under_limit_silent(self):
        with tempfile.TemporaryDirectory() as tmp:
            cache_dir = Path(tmp) / "cache"
            cache_dir.mkdir()
            (cache_dir / "small.pkl").write_bytes(b"x" * 10)
            report, _values = run_pipeline(
                self._pipeline(),
                registry=default_registry,
                workdir=tmp,
                cache_dir=str(cache_dir),
                cache_max_bytes=100_000,
            )
        self.assertFalse(any("缓存目录" in w for w in report.warnings))

    def test_spill_over_threshold_warns(self):
        with tempfile.TemporaryDirectory() as tmp:
            report, values = run_pipeline(
                spec_of(
                    [{"id": "src", "op": "m74.storage_spill", "params": {"size": 2000}}],
                    outputs={"v": "${src.value}"},
                ),
                registry=default_registry,
                workdir=tmp,
                cache_dir=None,
                spill_warn_bytes=100,
            )
            spill = Path(report.artifacts_dir) / "spill"
            self.assertTrue(
                any("落盘目录" in w and str(spill) in w for w in report.warnings),
                f"超阈值应追加落盘目录告警，实际：{report.warnings}",
            )
            self.assertEqual(report.status, "success")
            self.assertEqual(values["src.value"], 2000)

    def test_spill_under_threshold_silent(self):
        with tempfile.TemporaryDirectory() as tmp:
            report, _values = run_pipeline(
                spec_of(
                    [{"id": "src", "op": "m74.storage_spill", "params": {"size": 10}}]
                ),
                registry=default_registry,
                workdir=tmp,
                cache_dir=None,
                spill_warn_bytes=100_000,
            )
        self.assertFalse(any("落盘目录" in w for w in report.warnings))

    def test_none_thresholds_silent(self):
        with tempfile.TemporaryDirectory() as tmp:
            cache_dir = Path(tmp) / "cache"
            cache_dir.mkdir()
            (cache_dir / "big.pkl").write_bytes(b"x" * 2000)
            report, values = run_pipeline(
                spec_of(
                    [{"id": "src", "op": "m74.storage_spill", "params": {"size": 2000}}]
                ),
                registry=default_registry,
                workdir=tmp,
                cache_dir=str(cache_dir),
            )
        self.assertFalse(report.warnings, f"未配置阈值时不应告警：{report.warnings}")
        self.assertEqual(report.status, "success")
        self.assertEqual(values["src.value"], 2000)


if __name__ == "__main__":
    unittest.main()
