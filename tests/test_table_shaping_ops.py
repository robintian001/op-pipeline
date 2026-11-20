"""表清洗与整形算子测试（M9 Wave 1 P1 Task 2）：
table.fill_null / table.replace / table.pivot / table.unpivot。

覆盖：仅 None 填充与计数、== 替换与正则全量替换、透视各聚合与重复键报错、
逆透视往返与 None 行、空表、列缺失 KeyError、输入行不被原地修改。

运行：``python -m unittest discover -s tests -k TableShaping -v``
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


class TableShapingTest(unittest.TestCase):
    """table.fill_null / replace / pivot / unpivot：清洗与整形语义。"""

    # ------------------------------ fill_null ------------------------------ #
    def test_fill_null_only_none_cells_counted(self):
        table = [
            {"a": None, "b": "", "c": 0},
            {"a": "x", "b": None},
            {"d": None},  # 行内缺 a/b 键，不处理也不计数
        ]
        out = _run("table.fill_null", {"table": table}, value=0)
        self.assertEqual(out["table"], [
            {"a": 0, "b": "", "c": 0},
            {"a": "x", "b": 0},
            {"d": 0},
        ])
        self.assertEqual(out["filled"], 3)

    def test_fill_null_selected_columns(self):
        table = [{"a": None, "b": None}]
        out = _run("table.fill_null", {"table": table}, value="x", columns=["b"])
        self.assertEqual(out["table"], [{"a": None, "b": "x"}])
        self.assertEqual(out["filled"], 1)

    def test_fill_null_unknown_column_raises(self):
        with self.assertRaises(KeyError):
            _run("table.fill_null", {"table": [{"a": None}]}, value=0, columns=["nope"])

    def test_fill_null_input_not_mutated(self):
        table = [{"a": None, "b": 1}]
        _run("table.fill_null", {"table": table}, value=9)
        self.assertEqual(table, [{"a": None, "b": 1}])

    # ------------------------------- replace ------------------------------- #
    def test_replace_exact_match_and_count(self):
        table = [{"s": "a", "n": 1}, {"s": "b", "n": 2}]
        out = _run("table.replace", {"table": table}, mapping={"a": "A", 1: "one"})
        self.assertEqual(out["table"], [{"s": "A", "n": "one"}, {"s": "b", "n": 2}])
        self.assertEqual(out["replaced"], 2)

    def test_replace_selected_columns(self):
        table = [{"a": "x", "b": "x"}]
        out = _run("table.replace", {"table": table}, mapping={"x": "y"}, columns=["b"])
        self.assertEqual(out["table"], [{"a": "x", "b": "y"}])
        self.assertEqual(out["replaced"], 1)

    def test_replace_regex_only_str_cells_and_counts_changed(self):
        table = [{"v": "id-001"}, {"v": "id-002"}, {"v": 7}, {"v": None}]
        out = _run("table.replace", {"table": table}, mapping={r"id-": ""}, regex=True)
        self.assertEqual(out["table"], [{"v": "001"}, {"v": "002"}, {"v": 7}, {"v": None}])
        self.assertEqual(out["replaced"], 2)

    def test_replace_regex_replaces_all_occurrences(self):
        out = _run("table.replace", {"table": [{"v": "a1b22"}]}, mapping={r"\d+": "#"}, regex=True)
        self.assertEqual(out["table"], [{"v": "a#b#"}])
        self.assertEqual(out["replaced"], 1)

    def test_replace_regex_no_match_not_counted(self):
        out = _run("table.replace", {"table": [{"v": "abc"}]}, mapping={r"\d+": "0"}, regex=True)
        self.assertEqual(out["table"], [{"v": "abc"}])
        self.assertEqual(out["replaced"], 0)

    def test_replace_unknown_column_raises(self):
        with self.assertRaises(KeyError):
            _run("table.replace", {"table": [{"a": 1}]}, mapping={"a": "b"}, columns=["nope"])

    # -------------------------------- pivot -------------------------------- #
    def test_pivot_layout_rows_sorted_and_key_columns_sorted(self):
        table = [
            {"city": "sh", "month": "2月", "sales": 3},
            {"city": "bj", "month": "1月", "sales": 1},
            {"city": "sh", "month": "1月", "sales": 2},
        ]
        out = _run("table.pivot", {"table": table}, index="city", columns="month", values="sales")
        self.assertEqual(out["rows"], 2)
        self.assertEqual(out["table"], [
            {"city": "bj", "1月": 1},
            {"city": "sh", "1月": 2, "2月": 3},
        ])

    def test_pivot_multi_index_in_declared_order(self):
        table = [
            {"region": "r2", "city": "b", "month": "1月", "v": 1},
            {"region": "r1", "city": "a", "month": "1月", "v": 2},
        ]
        out = _run("table.pivot", {"table": table}, index=["region", "city"],
                   columns="month", values="v")
        self.assertEqual(out["table"], [
            {"region": "r1", "city": "a", "1月": 2},
            {"region": "r2", "city": "b", "1月": 1},
        ])

    def test_pivot_without_agg_rejects_duplicates_with_sample(self):
        table = [
            {"city": "sh", "month": "1月", "sales": 10},
            {"city": "sh", "month": "1月", "sales": 20},
        ]
        with self.assertRaises(ValueError) as cm:
            _run("table.pivot", {"table": table}, index="city", columns="month", values="sales")
        message = str(cm.exception)
        self.assertIn("sh", message)  # 报错含重复样例
        self.assertIn("1月", message)

    def test_pivot_with_agg_allows_duplicates(self):
        table = [
            {"k": "a", "c": "x", "v": 1},
            {"k": "a", "c": "x", "v": 2},
        ]
        out = _run("table.pivot", {"table": table}, index="k", columns="c", values="v", agg="sum")
        self.assertEqual(out["table"], [{"k": "a", "x": 3}])
        self.assertEqual(out["rows"], 1)

    def test_pivot_agg_sum_mean_count(self):
        table = [
            {"k": "a", "c": "x", "v": 1},
            {"k": "a", "c": "x", "v": 3},
            {"k": "a", "c": "y", "v": None},
            {"k": "b", "c": "x", "v": True},   # bool 不计入 sum/mean
            {"k": "b", "c": "y", "v": "txt"},  # 非数值
        ]
        summed = _run("table.pivot", {"table": table}, index="k", columns="c", values="v", agg="sum")
        self.assertEqual(summed["table"], [
            {"k": "a", "x": 4, "y": None},
            {"k": "b", "x": None, "y": None},
        ])
        averaged = _run("table.pivot", {"table": table}, index="k", columns="c", values="v", agg="mean")
        self.assertEqual(averaged["table"], [
            {"k": "a", "x": 2.0, "y": None},
            {"k": "b", "x": None, "y": None},
        ])
        counted = _run("table.pivot", {"table": table}, index="k", columns="c", values="v", agg="count")
        self.assertEqual(counted["table"], [
            {"k": "a", "x": 2, "y": 0},  # y 组只有 None，非 None 计数为 0
            {"k": "b", "x": 1, "y": 1},
        ])

    def test_pivot_agg_first_last_by_occurrence(self):
        table = [
            {"k": "a", "c": "x", "v": None},
            {"k": "a", "c": "x", "v": "first-real"},
            {"k": "a", "c": "x", "v": "last-real"},
            {"k": "a", "c": "y", "v": "only"},
        ]
        first = _run("table.pivot", {"table": table}, index="k", columns="c", values="v", agg="first")
        self.assertEqual(first["table"], [{"k": "a", "x": "first-real", "y": "only"}])
        last = _run("table.pivot", {"table": table}, index="k", columns="c", values="v", agg="last")
        self.assertEqual(last["table"], [{"k": "a", "x": "last-real", "y": "only"}])

    def test_pivot_empty_table(self):
        out = _run("table.pivot", {"table": []}, index="k", columns="c", values="v")
        self.assertEqual(out["table"], [])
        self.assertEqual(out["rows"], 0)

    # ------------------------------- unpivot ------------------------------- #
    def test_unpivot_pivot_roundtrip(self):
        wide = [
            {"city": "sh", "1月": 2, "2月": 3},
            {"city": "bj", "1月": 1, "2月": None},
        ]
        long_out = _run("table.unpivot", {"table": wide}, columns=["1月", "2月"], index="city")
        self.assertEqual(long_out["rows"], 4)
        self.assertEqual(long_out["table"], [
            {"city": "sh", "variable": "1月", "value": 2},
            {"city": "sh", "variable": "2月", "value": 3},
            {"city": "bj", "variable": "1月", "value": 1},
            {"city": "bj", "variable": "2月", "value": None},  # None 值也出行
        ])
        back = _run("table.pivot", {"table": long_out["table"]},
                    index="city", columns="variable", values="value")
        self.assertEqual(back["table"], [
            {"city": "bj", "1月": 1, "2月": None},
            {"city": "sh", "1月": 2, "2月": 3},
        ])

    def test_unpivot_missing_key_emits_null_value(self):
        out = _run("table.unpivot", {"table": [{"id": 1, "a": 5}, {"id": 2}]}, columns=["a"])
        self.assertEqual(out["table"], [
            {"id": 1, "variable": "a", "value": 5},
            {"id": 2, "variable": "a", "value": None},
        ])
        self.assertEqual(out["rows"], 2)

    def test_unpivot_index_none_derives_all_other_columns(self):
        table = [{"id": 1, "extra": "e", "a": 1, "b": 2}]
        out = _run("table.unpivot", {"table": table}, columns=["a", "b"])
        self.assertEqual(out["table"], [
            {"id": 1, "extra": "e", "variable": "a", "value": 1},
            {"id": 1, "extra": "e", "variable": "b", "value": 2},
        ])

    def test_unpivot_custom_names(self):
        out = _run("table.unpivot", {"table": [{"id": 1, "a": 2}]}, columns=["a"], index="id",
                   name_to="key", value_to="val")
        self.assertEqual(out["table"], [{"id": 1, "key": "a", "val": 2}])

    def test_unpivot_empty_table(self):
        out = _run("table.unpivot", {"table": []}, columns=["a"])
        self.assertEqual(out["table"], [])
        self.assertEqual(out["rows"], 0)

    def test_unpivot_missing_column_raises(self):
        with self.assertRaises(KeyError):
            _run("table.unpivot", {"table": [{"a": 1}]}, columns=["nope"])

    def test_unpivot_missing_index_column_raises(self):
        with self.assertRaises(KeyError):
            _run("table.unpivot", {"table": [{"a": 1}]}, columns=["a"], index=["nope"])

    def test_unpivot_input_not_mutated(self):
        table = [{"id": 1, "a": None}]
        _run("table.unpivot", {"table": table}, columns=["a"], index=["id"])
        self.assertEqual(table, [{"id": 1, "a": None}])


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
