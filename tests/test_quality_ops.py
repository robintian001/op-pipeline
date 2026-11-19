"""数据质量算子测试（M8-4 Task 12）：quality.null_check / unique / schema_check。

覆盖：error/warn 双模式、table 原样透传、边界（空表/全空列/None 跳过/类型名非法）。

运行：``python -m unittest discover -s tests -k test_quality_ops -v``
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


class NullCheckTest(unittest.TestCase):
    """quality.null_check：error/warn、阈值、空值定义。"""

    TABLE = [
        {"name": "a", "phone": "13800000000"},
        {"name": "b", "phone": None},
        {"name": "", "phone": " "},
    ]

    def test_error_mode_raises_with_column_and_ratio(self):
        with self.assertRaises(AssertionError) as cm:
            _run("quality.null_check", {"table": self.TABLE}, columns=["name", "phone"])
        message = str(cm.exception)
        self.assertIn("name", message)
        self.assertIn("phone", message)
        # name 空值率 1/3（空串），phone 空值率 2/3（None + 空白串）
        self.assertIn("0.6667", message)
        self.assertIn("0.3333", message)

    def test_error_mode_passes_below_threshold(self):
        out = _run("quality.null_check", {"table": self.TABLE},
                   columns=["name", "phone"], max_null_ratio=1.0)
        self.assertEqual(out["violations"], [])

    def test_warn_mode_reports_without_raising(self):
        out = _run("quality.null_check", {"table": self.TABLE}, columns=["name", "phone"], mode="warn")
        self.assertEqual(len(out["violations"]), 2)
        by_column = {v["column"]: v for v in out["violations"]}
        # name 只有第 2 行空串；phone 第 1 行 None + 第 2 行空白串
        self.assertEqual(by_column["name"]["nulls"], 1)
        self.assertEqual(by_column["name"]["ratio"], 0.3333)
        self.assertEqual(by_column["phone"]["nulls"], 2)
        self.assertEqual(by_column["phone"]["row_indices"], [1, 2])
        self.assertEqual(by_column["phone"]["samples"][0], self.TABLE[1])

    def test_columns_accept_single_str(self):
        with self.assertRaises(AssertionError) as cm:
            _run("quality.null_check", {"table": self.TABLE}, columns="name")
        self.assertIn("name", str(cm.exception))

    def test_blank_string_counts_as_null_zero_does_not(self):
        table = [{"v": ""}, {"v": "  "}, {"v": 0}, {"v": False}]
        out = _run("quality.null_check", {"table": table}, columns="v", mode="warn")
        self.assertEqual(out["violations"][0]["nulls"], 2)

    def test_table_passthrough_unchanged(self):
        out = _run("quality.null_check", {"table": self.TABLE}, columns="phone", mode="warn")
        self.assertEqual(out["table"], self.TABLE)

    def test_empty_table_no_violations(self):
        out = _run("quality.null_check", {"table": []}, columns="name")
        self.assertEqual(out["violations"], [])

    def test_unknown_column_raises_key_error(self):
        with self.assertRaises(KeyError):
            _run("quality.null_check", {"table": self.TABLE}, columns="nope")

    def test_all_null_column(self):
        table = [{"v": None}, {"v": ""}]
        with self.assertRaises(AssertionError) as cm:
            _run("quality.null_check", {"table": table}, columns="v", max_null_ratio=0.5)
        self.assertIn("1.0", str(cm.exception))
        out = _run("quality.null_check", {"table": table}, columns="v", max_null_ratio=1.0)
        self.assertEqual(out["violations"], [])


class UniqueCheckTest(unittest.TestCase):
    """quality.unique：按键组合查重、violations 样例与次数。"""

    TABLE = [
        {"id": 1, "city": "sh", "v": "a"},
        {"id": 2, "city": "bj", "v": "b"},
        {"id": 3, "city": "sh", "v": "c"},
        {"id": 4, "city": "sh", "v": "d"},
    ]

    def test_error_mode_raises_on_duplicates(self):
        with self.assertRaises(AssertionError) as cm:
            _run("quality.unique", {"table": self.TABLE}, keys="city")
        self.assertIn("sh", str(cm.exception))
        self.assertIn("3", str(cm.exception))

    def test_error_mode_passes_unique_table(self):
        out = _run("quality.unique", {"table": self.TABLE}, keys="id")
        self.assertEqual(out["violations"], [])

    def test_warn_mode_violation_details(self):
        out = _run("quality.unique", {"table": self.TABLE}, keys=["city"], mode="warn")
        self.assertEqual(len(out["violations"]), 1)
        violation = out["violations"][0]
        self.assertEqual(violation["keys"], {"city": "sh"})
        self.assertEqual(violation["count"], 3)
        self.assertEqual(violation["row_indices"], [0, 2, 3])

    def test_composite_keys(self):
        table = [
            {"a": 1, "b": 1},
            {"a": 1, "b": 2},
            {"a": 1, "b": 1},
        ]
        out = _run("quality.unique", {"table": table}, keys=["a", "b"], mode="warn")
        self.assertEqual(out["violations"][0]["keys"], {"a": 1, "b": 1})
        self.assertEqual(out["violations"][0]["count"], 2)

    def test_violations_capped_at_ten(self):
        table = [{"k": i % 15} for i in range(30)]  # 15 组重复键
        out = _run("quality.unique", {"table": table}, keys="k", mode="warn")
        self.assertEqual(len(out["violations"]), 10)

    def test_table_passthrough_unchanged(self):
        out = _run("quality.unique", {"table": self.TABLE}, keys="city", mode="warn")
        self.assertEqual(out["table"], self.TABLE)

    def test_empty_table_no_violations(self):
        out = _run("quality.unique", {"table": []}, keys="id")
        self.assertEqual(out["violations"], [])


class SchemaCheckTest(unittest.TestCase):
    """quality.schema_check：类型白名单、None 跳过、strict_columns、非法类型名。"""

    def test_error_mode_raises_on_mismatch(self):
        table = [{"n": 1}, {"n": "not-int"}]
        with self.assertRaises(AssertionError) as cm:
            _run("quality.schema_check", {"table": table}, expected={"n": "int"})
        self.assertIn("n", str(cm.exception))
        self.assertIn("str", str(cm.exception))

    def test_warn_mode_violation_details(self):
        table = [{"n": "x", "f": 1.5, "d": {"a": 1}, "l": [1], "s": 7}]
        out = _run("quality.schema_check", {"table": table},
                   expected={"n": "str", "f": "float", "d": "dict", "l": "list", "s": "str"},
                   mode="warn")
        self.assertEqual(out["table"], table)
        # n/f/d/l 均符合声明，只有 s（int 声明为 str）违例
        flagged = {(v["column"], v["expected"], v["actual"]) for v in out["violations"]}
        self.assertEqual(flagged, {("s", "str", "int")})

    def test_none_values_are_skipped(self):
        table = [{"n": None}, {"n": 2}]
        out = _run("quality.schema_check", {"table": table}, expected={"n": "int"})
        self.assertEqual(out["violations"], [])

    def test_bool_is_not_int(self):
        table = [{"n": True}]
        out = _run("quality.schema_check", {"table": table}, expected={"n": "int"}, mode="warn")
        self.assertEqual(len(out["violations"]), 1)
        out = _run("quality.schema_check", {"table": table}, expected={"n": "bool"})
        self.assertEqual(out["violations"], [])

    def test_int_is_not_float(self):
        out = _run("quality.schema_check", {"table": [{"f": 1}]}, expected={"f": "float"}, mode="warn")
        self.assertEqual(len(out["violations"]), 1)

    def test_any_accepts_everything(self):
        table = [{"v": 1}, {"v": "x"}, {"v": None}]
        out = _run("quality.schema_check", {"table": table}, expected={"v": "any"})
        self.assertEqual(out["violations"], [])

    def test_missing_column_ignored_unless_strict(self):
        table = [{"a": 1}]  # 没有 expected 声明的 b 列
        out = _run("quality.schema_check", {"table": table}, expected={"b": "str"})
        self.assertEqual(out["violations"], [])
        out = _run("quality.schema_check", {"table": table}, expected={"b": "str"},
                   strict_columns=True, mode="warn")
        self.assertEqual(out["violations"][0]["type"], "missing_column")

    def test_strict_columns_no_extra_row_violations(self):
        table = [{"a": 1}, {"a": 2, "extra": 1}]
        out = _run("quality.schema_check", {"table": table}, expected={"a": "int"}, strict_columns=True)
        self.assertEqual(out["violations"], [])

    def test_illegal_type_name_raises_value_error(self):
        with self.assertRaises(ValueError):
            _run("quality.schema_check", {"table": [{"n": 1}]}, expected={"n": "decimal"})

    def test_empty_table_no_violations(self):
        out = _run("quality.schema_check", {"table": []}, expected={"n": "int"}, strict_columns=True)
        self.assertEqual(out["violations"], [])

    def test_table_passthrough_unchanged(self):
        table = [{"n": "x"}]
        out = _run("quality.schema_check", {"table": table}, expected={"n": "int"}, mode="warn")
        self.assertEqual(out["table"], table)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
