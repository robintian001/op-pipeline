"""M8-2 Task 5：节点行数血缘（``rows_in`` / ``rows_out``）。

覆盖：多端口行数、非 list 端口不记录、cached 节点（缓存命中后反序列化的
list 输出同样记录）、``lineage=False`` 时报告节点不含这两个键、
采集不改变 status/outputs/缓存语义。
"""
from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from op_pipeline import (  # noqa: E402
    Operator,
    PipelineSpec,
    default_registry,
    run_pipeline,
)
from op_pipeline.registry import op as register_op  # noqa: E402


@register_op("m82.lineage_split", replace=True)
class LineageSplit(Operator):
    """把整数列表拆成偶数/奇数两个 list 输出（多端口行数验证）。"""

    description = "按奇偶拆分"
    inputs = [("rows", list)]
    outputs = [("evens", list), ("odds", list)]

    def run(self, ctx, rows):
        return {
            "evens": [x for x in rows if x % 2 == 0],
            "odds": [x for x in rows if x % 2],
        }


@register_op("m82.lineage_zip", replace=True)
class LineageZip(Operator):
    """双 list 输入逐位配对（多端口 rows_in 验证）。"""

    description = "逐位配对"
    inputs = [("left", list), ("right", list)]
    outputs = [("pairs", list)]

    def run(self, ctx, left, right):
        return {"pairs": list(zip(left, right))}


def _spec(with_cache: bool = False):
    return PipelineSpec.from_dict(
        {
            "name": "lineage_demo",
            "outputs": {"result": "${zip.pairs}"},
            "nodes": [
                {"id": "src", "op": "control.const",
                 "params": {"value": [1, 2, 3, 4]}},
                {"id": "split", "op": "m82.lineage_split",
                 "inputs": {"rows": "${src.value}"},
                 **({"cache": True} if with_cache else {})},
                {"id": "zip", "op": "m82.lineage_zip",
                 "inputs": {"left": "${split.evens}", "right": "${split.odds}"}},
            ],
        }
    )


def _text_spec():
    """非 list 句柄：str 常量 + 透传，验证行数不记录。"""
    return PipelineSpec.from_dict(
        {
            "name": "lineage_text",
            "nodes": [
                {"id": "text", "op": "control.const",
                 "params": {"value": "hello"}},
                {"id": "pass", "op": "control.passthrough",
                 "inputs": {"value": "${text.value}"}},
            ],
        }
    )


class RowLineageTests(unittest.TestCase):
    """行数血缘采集语义。"""

    def test_multi_port_rows_recorded(self):
        """多端口：rows_in/rows_out 按端口名记录 list 行数，报告平铺两键。"""
        report, values = run_pipeline(
            _spec(), registry=default_registry, cache_dir=None
        )
        self.assertEqual(report.status, "success")
        split = report.node("split")
        self.assertEqual(split.rows_in, {"rows": 4})
        self.assertEqual(split.rows_out, {"evens": 2, "odds": 2})
        split_dict = split.to_dict()
        self.assertEqual(split_dict["rows_in"], {"rows": 4})
        self.assertEqual(split_dict["rows_out"], {"evens": 2, "odds": 2})
        # 双 list 输入节点
        zipped = report.node("zip")
        self.assertEqual(zipped.rows_in, {"left": 2, "right": 2})
        self.assertEqual(zipped.rows_out, {"pairs": 2})
        self.assertEqual(values["zip.pairs"], [(2, 1), (4, 3)])

    def test_non_list_ports_not_recorded(self):
        """非 list 端口不记录：纯 str 流的节点行数字典为空、报告无键。"""
        report, _ = run_pipeline(
            _text_spec(), registry=default_registry, cache_dir=None
        )
        for node_result in report.nodes:
            self.assertEqual(node_result.rows_in, {})
            self.assertEqual(node_result.rows_out, {})
            self.assertNotIn("rows_in", node_result.to_dict())
            self.assertNotIn("rows_out", node_result.to_dict())

    def test_cached_node_records_rows(self):
        """cached 节点：命中缓存后反序列化的 list 输出同样记录行数。"""
        with tempfile.TemporaryDirectory() as tmp:
            spec = _spec(with_cache=True)
            cache_dir = Path(tmp) / "cache"
            first, _ = run_pipeline(
                spec, registry=default_registry, cache_dir=cache_dir
            )
            second, _ = run_pipeline(
                spec, registry=default_registry, cache_dir=cache_dir
            )
            self.assertTrue(second.node("split").cached)
            self.assertEqual(second.node("split").rows_in, {"rows": 4})
            self.assertEqual(
                second.node("split").rows_out, {"evens": 2, "odds": 2}
            )

    def test_lineage_false_omits_keys(self):
        """lineage=False：报告节点不含 rows_in/rows_out，其余行为不变。"""
        report, values = run_pipeline(
            _spec(), registry=default_registry, cache_dir=None, lineage=False
        )
        self.assertEqual(report.status, "success")
        self.assertEqual(values["zip.pairs"], [(2, 1), (4, 3)])
        data = report.to_dict()
        self.assertTrue(all("rows_in" not in n and "rows_out" not in n
                            for n in data["nodes"]))
        # 输出描述等其他字段照常
        self.assertEqual(data["outputs"]["result"], [(2, 1), (4, 3)])

    def test_lineage_does_not_change_semantics(self):
        """采集开关不改变 status/outputs/缓存语义。"""
        with tempfile.TemporaryDirectory() as tmp:
            cache_dir = Path(tmp) / "cache"
            run_pipeline(
                _spec(with_cache=True), registry=default_registry,
                cache_dir=cache_dir,
            )
            on_second, values_on = run_pipeline(
                _spec(with_cache=True), registry=default_registry,
                cache_dir=cache_dir,
            )
            cache_dir_off = Path(tmp) / "cache_off"
            run_pipeline(
                _spec(with_cache=True), registry=default_registry,
                cache_dir=cache_dir_off, lineage=False,
            )
            off_second, values_off = run_pipeline(
                _spec(with_cache=True), registry=default_registry,
                cache_dir=cache_dir_off, lineage=False,
            )
            # status 一致
            self.assertEqual(on_second.status, off_second.status)
            # 缓存语义一致：第二遍都命中（行数采集不参与 cache_key）
            self.assertTrue(on_second.node("split").cached)
            self.assertTrue(off_second.node("split").cached)
            # outputs 一致（数值相同，duration 等时变字段不在比较范围）
            self.assertEqual(values_on["zip.pairs"], values_off["zip.pairs"])


if __name__ == "__main__":
    unittest.main()
