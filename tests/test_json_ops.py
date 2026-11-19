"""JSON/结构算子测试（M8-4 Task 11）：json.flatten + table.explode。

运行：``python -m unittest discover -s tests -k test_json_ops -v``
"""
from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from op_pipeline import RunContext, default_registry  # noqa: E402


def _make_ctx(op_name: str):
    run_ctx = RunContext.create(pipeline="t", params={}, workdir=tempfile.gettempdir())
    return run_ctx.for_node("n", op_name)


def _run(name: str, inputs: dict | None = None, **params) -> dict:
    op_ = default_registry.get(name)(**params)
    return op_.execute(ctx=_make_ctx(name), inputs=inputs or {})


class FlattenTest(unittest.TestCase):
    """json.flatten：多层嵌套 / 自定义 sep / max_depth / list 叶子。"""

    def test_flatten_nested(self):
        out = _run("json.flatten", {"data": {"a": {"b": {"c": 1}}, "d": 2}})
        self.assertEqual(out["data"], {"a.b.c": 1, "d": 2})
        self.assertEqual(out["count"], 2)

    def test_flatten_custom_sep(self):
        out = _run("json.flatten", {"data": {"a": {"b": 1}}}, sep="/")
        self.assertEqual(out["data"], {"a/b": 1})

    def test_list_value_stays_leaf(self):
        out = _run("json.flatten", {"data": {"a": [1, 2], "b": {"c": 3}}})
        self.assertEqual(out["data"], {"a": [1, 2], "b.c": 3})
        self.assertEqual(out["count"], 2)

    def test_empty_dict_stays_leaf(self):
        out = _run("json.flatten", {"data": {"a": {}, "b": 1}})
        self.assertEqual(out["data"], {"a": {}, "b": 1})

    def test_max_depth_limits_expansion(self):
        data = {"a": {"b": {"c": 1}}}
        # 1 层：只展开顶层嵌套 dict，更深的保留为叶子
        out = _run("json.flatten", {"data": data}, max_depth=1)
        self.assertEqual(out["data"], {"a.b": {"c": 1}})
        # 2 层：全部展开
        out = _run("json.flatten", {"data": data}, max_depth=2)
        self.assertEqual(out["data"], {"a.b.c": 1})

    def test_flat_input_unchanged(self):
        out = _run("json.flatten", {"data": {"a": 1, "b": 2}})
        self.assertEqual(out["data"], {"a": 1, "b": 2})
        self.assertEqual(out["count"], 2)


class ExplodeTest(unittest.TestCase):
    """table.explode：展开 / 索引列 / 空 list 两种模式 / 报错。"""

    TABLE = [
        {"id": 1, "tags": ["a", "b"]},
        {"id": 2, "tags": []},
        {"id": 3, "tags": ["c"]},
    ]

    def test_explode_basic(self):
        out = _run("table.explode", {"table": self.TABLE}, column="tags")
        self.assertEqual(out["table"], [
            {"id": 1, "tags": "a"},
            {"id": 1, "tags": "b"},
            {"id": 3, "tags": "c"},
        ])
        self.assertEqual(out["rows"], 3)  # 默认丢弃空 list 行

    def test_explode_keep_empty(self):
        out = _run("table.explode", {"table": self.TABLE}, column="tags", keep_empty=True)
        self.assertEqual(out["rows"], 4)
        self.assertEqual(out["table"][2], {"id": 2, "tags": None})

    def test_explode_index_column(self):
        out = _run("table.explode", {"table": self.TABLE}, column="tags",
                   index_column="src", keep_empty=True)
        self.assertEqual([row["src"] for row in out["table"]], [0, 0, 1, 2])
        self.assertEqual(out["table"][1], {"id": 1, "tags": "b", "src": 0})

    def test_explode_dict_elements(self):
        table = [{"k": "x", "v": [{"n": 1}, {"n": 2}]}]
        out = _run("table.explode", {"table": table}, column="v")
        self.assertEqual(out["table"], [{"k": "x", "v": {"n": 1}}, {"k": "x", "v": {"n": 2}}])

    def test_explode_missing_column_raises(self):
        with self.assertRaises(KeyError):
            _run("table.explode", {"table": [{"id": 1}]}, column="tags")

    def test_explode_non_list_column_raises(self):
        with self.assertRaises(ValueError):
            _run("table.explode", {"table": [{"tags": "not-a-list"}]}, column="tags")

    def test_explode_empty_table(self):
        out = _run("table.explode", {"table": []}, column="tags")
        self.assertEqual(out["table"], [])
        self.assertEqual(out["rows"], 0)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
