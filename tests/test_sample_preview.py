"""M8-2 Task 4：采样预览（``sample_rows``）。

覆盖：list 输入截断（实际传给算子的句柄即截断后的）、dict/str 非 list 原样、
采样模式缓存读写全部禁用（缓存目录无新增条目）、报告标注（run 级
``sample_rows`` 字段 + 节点级 ``sampled`` 标记）、非法 N 抛 ``ValueError``、
未传参数时行为与 v0.8.0 一致（报告无新字段）。
"""
from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from op_pipeline import PipelineSpec, default_registry, run_pipeline  # noqa: E402


def _spec():
    """五路节点：list / dict / str 三种句柄类型 + 透传消费。"""
    return PipelineSpec.from_dict(
        {
            "name": "sample_demo",
            "nodes": [
                {"id": "rows", "op": "control.const",
                 "params": {"value": [0, 1, 2, 3, 4]}},
                {"id": "pass", "op": "control.passthrough",
                 "inputs": {"value": "${rows.value}"}},
                {"id": "blob", "op": "control.const",
                 "params": {"value": {"a": 1, "b": [1, 2, 3]}}},
                {"id": "blob_pass", "op": "control.passthrough",
                 "inputs": {"value": "${blob.value}"}},
                {"id": "text", "op": "control.const",
                 "params": {"value": "hello"}},
                {"id": "text_pass", "op": "control.passthrough",
                 "inputs": {"value": "${text.value}"}},
            ],
        }
    )


def _cached_spec():
    """透传节点声明 cache=True，用于验证采样模式下缓存被禁用。"""
    return PipelineSpec.from_dict(
        {
            "name": "sample_cache",
            "nodes": [
                {"id": "rows", "op": "control.const",
                 "params": {"value": [0, 1, 2, 3, 4]}},
                {"id": "pass", "op": "control.passthrough",
                 "inputs": {"value": "${rows.value}"}, "cache": True},
            ],
        }
    )


class SamplePreviewTests(unittest.TestCase):
    """采样模式语义。"""

    def test_list_input_truncated(self):
        """list 类型输入句柄截断为前 N 行；上游发布值不受影响。"""
        report, values = run_pipeline(
            _spec(), registry=default_registry, cache_dir=None, sample_rows=2
        )
        self.assertEqual(report.status, "success")
        self.assertEqual(values["pass.value"], [0, 1])
        self.assertEqual(values["rows.value"], [0, 1, 2, 3, 4])
        # 行数血缘反映采样后的实际传入规模
        self.assertEqual(report.node("pass").rows_in, {"value": 2})

    def test_non_list_input_untouched(self):
        """dict/str/其他类型句柄原样（dict 内层 list 也不动）。"""
        report, values = run_pipeline(
            _spec(), registry=default_registry, cache_dir=None, sample_rows=1
        )
        self.assertEqual(values["blob_pass.value"], {"a": 1, "b": [1, 2, 3]})
        self.assertEqual(values["text_pass.value"], "hello")

    def test_cache_read_and_write_disabled(self):
        """采样模式缓存读写全部禁用：跑两遍缓存目录也无任何条目。"""
        with tempfile.TemporaryDirectory() as tmp:
            cache_dir = Path(tmp) / "cache"
            first, _ = run_pipeline(
                _cached_spec(), registry=default_registry,
                cache_dir=cache_dir, sample_rows=2,
            )
            second, _ = run_pipeline(
                _cached_spec(), registry=default_registry,
                cache_dir=cache_dir, sample_rows=2,
            )
            # 目录都不该被创建（缓存上下文未构造）；即便存在也必须为空
            entries = list(cache_dir.glob("**/*")) if cache_dir.exists() else []
            self.assertEqual(entries, [])
            self.assertTrue(all(not n.cached and not n.resumed
                                for n in second.nodes))
            # 对照组：不采样时第二遍应命中缓存——证明禁用确由采样触发
            with tempfile.TemporaryDirectory() as tmp2:
                cache_dir2 = Path(tmp2) / "cache"
                run_pipeline(
                    _cached_spec(), registry=default_registry, cache_dir=cache_dir2,
                )
                third, _ = run_pipeline(
                    _cached_spec(), registry=default_registry, cache_dir=cache_dir2,
                )
                self.assertTrue(third.node("pass").cached)

    def test_report_annotations(self):
        """报告标注：run 级 sample_rows 字段 + 节点级 sampled 标记。"""
        report, _ = run_pipeline(
            _spec(), registry=default_registry, cache_dir=None, sample_rows=2
        )
        self.assertEqual(report.sample_rows, 2)
        data = report.to_dict()
        self.assertEqual(data["sample_rows"], 2)
        self.assertTrue(all(n["sampled"] is True for n in data["nodes"]))

    def test_invalid_sample_rows_raises(self):
        """非法 N：0 / 负数 / 小数 / bool / 字符串均抛 ValueError。"""
        for bad in (0, -1, 1.5, True, "2"):
            with self.assertRaises(ValueError):
                run_pipeline(
                    _spec(), registry=default_registry, cache_dir=None,
                    sample_rows=bad,
                )

    def test_no_sample_keeps_v080_report_shape(self):
        """未传参数时行为与 v0.8.0 一致：报告无 sample_rows/sampled 新字段。"""
        report, values = run_pipeline(
            _spec(), registry=default_registry, cache_dir=None
        )
        self.assertEqual(values["pass.value"], [0, 1, 2, 3, 4])
        data = report.to_dict()
        self.assertNotIn("sample_rows", data)
        self.assertTrue(all("sampled" not in n for n in data["nodes"]))


if __name__ == "__main__":
    unittest.main()
