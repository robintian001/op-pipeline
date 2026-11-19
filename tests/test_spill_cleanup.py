"""M7-4 Task 10：SpillHandleStore 清理策略（on_run_success）。

覆盖：成功删除本次 run 的落盘目录、失败/取消保留、默认不删、非法策略报错、
同一 store 连续两次 run 的目录互不影响、显式 spill_dir 不被引擎删除。
"""
from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from op_pipeline import (  # noqa: E402
    Engine,
    Operator,
    Param,
    PipelineSpec,
    RunContext,
    SpillHandleStore,
    default_registry,
    op,
)
from op_pipeline.spec import build_plan  # noqa: E402


@op("m74.spill_rows")
class SpillRows(Operator):
    """产出大表（list[int]），n 够大时超过落盘阈值。"""

    outputs = [("rows", list)]
    params = {"n": Param(int, default=2000)}

    def run(self, ctx, n):
        return {"rows": list(range(n))}


@op("m74.spill_fail")
class SpillFail(Operator):
    """依赖大表后必失败，用于验证失败时保留落盘目录。"""

    inputs = [("rows", list)]
    outputs = [("value", int)]

    def run(self, ctx, rows):
        raise RuntimeError("模拟节点失败")


def spec_of(nodes, outputs=None) -> PipelineSpec:
    return PipelineSpec.from_dict(
        {"name": "m74spill", "params": {}, "nodes": nodes, "outputs": outputs or {}}
    )


def _spill_files(dir_path: Path) -> list[Path]:
    return list(dir_path.glob("*.pkl")) if dir_path.is_dir() else []


class SpillCleanupTest(unittest.TestCase):
    def _run(self, workdir: str, store, nodes):
        plan = build_plan(spec_of(nodes), default_registry)
        context = RunContext.create(pipeline="t", params={}, workdir=workdir)
        engine = Engine(plan, context, handle_store=store)
        return engine.run(), context

    def test_success_deletes_spill_dir(self):
        store = SpillHandleStore(threshold=64, cleanup="on_run_success")
        with tempfile.TemporaryDirectory() as tmp:
            report, context = self._run(
                tmp, store,
                [{"id": "big", "op": "m74.spill_rows", "params": {"n": 2000}}],
            )
            self.assertEqual(report.status, "success")
            spill = context.artifacts_dir / "spill"
            self.assertFalse(spill.exists(), "成功且 on_run_success 时应删除落盘目录")
            self.assertTrue(context.artifacts_dir.exists(), "只删 spill 子目录，不删父目录")
            # 清理前已把落盘值加载回内存：运行结束后 store 仍可读
            self.assertEqual(store.get("big.rows")[:3], [0, 1, 2])

    def test_failure_keeps_spill_dir(self):
        store = SpillHandleStore(threshold=64, cleanup="on_run_success")
        with tempfile.TemporaryDirectory() as tmp:
            report, context = self._run(
                tmp, store,
                [
                    {"id": "big", "op": "m74.spill_rows", "params": {"n": 2000}},
                    {
                        "id": "boom",
                        "op": "m74.spill_fail",
                        "inputs": {"rows": "${big.rows}"},
                    },
                ],
            )
            self.assertEqual(report.status, "failed")
            spill = context.artifacts_dir / "spill"
            self.assertTrue(spill.exists(), "失败时落盘目录必须保留以便排障")
            self.assertGreaterEqual(len(_spill_files(spill)), 1)

    def test_default_policy_keeps_spill_dir(self):
        store = SpillHandleStore(threshold=64)  # 默认 cleanup="never"
        with tempfile.TemporaryDirectory() as tmp:
            report, context = self._run(
                tmp, store,
                [{"id": "big", "op": "m74.spill_rows", "params": {"n": 2000}}],
            )
            self.assertEqual(report.status, "success")
            spill = context.artifacts_dir / "spill"
            self.assertTrue(spill.exists(), "默认策略下成功运行也不删除落盘目录")
            self.assertGreaterEqual(len(_spill_files(spill)), 1)

    def test_invalid_cleanup_policy_raises(self):
        for policy in ("always", "", "on_success"):
            with self.subTest(policy=policy):
                with self.assertRaises(ValueError):
                    SpillHandleStore(cleanup=policy)

    def test_repeated_runs_do_not_touch_each_other(self):
        store = SpillHandleStore(threshold=64, cleanup="on_run_success")
        with tempfile.TemporaryDirectory() as tmp:
            first, ctx1 = self._run(
                tmp, store,
                [
                    {"id": "big", "op": "m74.spill_rows", "params": {"n": 2000}},
                    {
                        "id": "boom",
                        "op": "m74.spill_fail",
                        "inputs": {"rows": "${big.rows}"},
                    },
                ],
            )
            self.assertEqual(first.status, "failed")
            dir1 = ctx1.artifacts_dir / "spill"
            self.assertTrue(dir1.exists())

            second, ctx2 = self._run(
                tmp, store,
                [{"id": "big", "op": "m74.spill_rows", "params": {"n": 2000}}],
            )
            self.assertEqual(second.status, "success")
            dir2 = ctx2.artifacts_dir / "spill"
            self.assertNotEqual(dir1, dir2, "两次 run 的落盘目录应各自独立")
            self.assertFalse(dir2.exists(), "第二次成功运行应只删自己的落盘目录")
            self.assertTrue(dir1.exists(), "第一次失败运行的目录不应被误删")
            self.assertGreaterEqual(len(_spill_files(dir1)), 1)

    def test_explicit_spill_dir_is_not_deleted(self):
        with tempfile.TemporaryDirectory() as tmp:
            keep = Path(tmp) / "caller_spill"
            store = SpillHandleStore(
                threshold=64, spill_dir=keep, cleanup="on_run_success"
            )
            report, _ctx = self._run(
                tmp, store,
                [{"id": "big", "op": "m74.spill_rows", "params": {"n": 2000}}],
            )
            self.assertEqual(report.status, "success")
            self.assertTrue(keep.exists(), "显式 spill_dir 的生命周期归调用方，引擎不应删除")


if __name__ == "__main__":
    unittest.main()
