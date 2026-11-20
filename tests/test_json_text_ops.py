"""JSON 文本算子测试（M9 Wave 1 P1 Task 1）：json.parse + json.stringify。

覆盖：parse→stringify 往返、非法 JSON / 非 str 输入报错、
indent / ensure_ascii / sort_keys 行为、UTF-8 字节数计算、list / 标量序列化。

运行：``python -m unittest discover -s tests -k JsonText -v``
"""
from __future__ import annotations

import json
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


class JsonTextOpsTest(unittest.TestCase):
    """json.parse / json.stringify：往返、报错、序列化选项与字节数。"""

    def test_parse_stringify_roundtrip(self):
        source = '{"name": "张三", "scores": [90, 85], "active": true}'
        parsed = _run("json.parse", {"text": source})
        self.assertEqual(parsed["data"], {"name": "张三", "scores": [90, 85], "active": True})
        stringified = _run("json.stringify", {"data": parsed["data"]})
        self.assertEqual(json.loads(stringified["text"]), parsed["data"])
        again = _run("json.parse", {"text": stringified["text"]})
        self.assertEqual(again["data"], parsed["data"])

    def test_parse_invalid_json_raises_value_error(self):
        with self.assertRaises(ValueError):
            _run("json.parse", {"text": "{not-valid-json"})

    def test_parse_non_str_input_raises_value_error(self):
        with self.assertRaises(ValueError):
            _run("json.parse", {"text": 123})
        with self.assertRaises(ValueError):
            _run("json.parse", {"text": {"a": 1}})

    def test_parse_scalar_and_null(self):
        self.assertEqual(_run("json.parse", {"text": "42"})["data"], 42)
        self.assertIsNone(_run("json.parse", {"text": "null"})["data"])
        self.assertEqual(_run("json.parse", {"text": '"txt"'})["data"], "txt")

    def test_stringify_default_indent_two(self):
        out = _run("json.stringify", {"data": {"a": 1, "b": [2, 3]}})
        self.assertEqual(out["text"], '{\n  "a": 1,\n  "b": [\n    2,\n    3\n  ]\n}')
        self.assertEqual(out["bytes"], len(out["text"].encode("utf-8")))

    def test_stringify_preserves_key_order_by_default(self):
        out = _run("json.stringify", {"data": {"b": 1, "a": 2}})
        self.assertEqual(out["text"], '{\n  "b": 1,\n  "a": 2\n}')
        sorted_out = _run("json.stringify", {"data": {"b": 1, "a": 2}}, sort_keys=True)
        self.assertEqual(sorted_out["text"], '{\n  "a": 2,\n  "b": 1\n}')

    def test_stringify_ensure_ascii_default_false(self):
        out = _run("json.stringify", {"data": {"名": "值"}})
        self.assertIn("名", out["text"])
        escaped = _run("json.stringify", {"data": {"名": "值"}}, ensure_ascii=True)
        self.assertNotIn("名", escaped["text"])
        self.assertIn("\\u", escaped["text"])

    def test_stringify_bytes_counts_utf8_not_chars(self):
        out = _run("json.stringify", {"data": "中"})
        self.assertEqual(out["text"], '"中"')
        self.assertEqual(out["bytes"], 5)  # 两个引号 + 汉字 3 字节
        self.assertGreater(out["bytes"], len(out["text"]))

    def test_stringify_accepts_list_and_scalars(self):
        out = _run("json.stringify", {"data": [1, "x", None]})
        self.assertEqual(json.loads(out["text"]), [1, "x", None])
        self.assertEqual(out["bytes"], len(out["text"].encode("utf-8")))
        self.assertEqual(_run("json.stringify", {"data": None})["text"], "null")
        self.assertEqual(_run("json.stringify", {"data": True})["text"], "true")
        self.assertEqual(_run("json.stringify", {"data": 42})["text"], "42")

    def test_stringify_unserializable_raises_type_error(self):
        with self.assertRaises(TypeError):
            _run("json.stringify", {"data": {1, 2}})  # set 不可 JSON 序列化


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
