"""text.hash 算子测试（M9 Wave 1 Task 3）。

运行：``python -m unittest discover -s tests -k TextHash -v``
"""
from __future__ import annotations

import hashlib
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


class TextHashTest(unittest.TestCase):
    """text.hash：算法摘要值、非法算法、空串、非 ASCII、length、encoding。"""

    def test_default_sha256_matches_hashlib(self):
        out = _run("text.hash", {"text": "hello world"})
        self.assertEqual(out["digest"], hashlib.sha256(b"hello world").hexdigest())
        self.assertEqual(out["length"], 11)

    def test_all_five_algorithms_match_hashlib(self):
        text = "Hello, 世界!"
        for algo in ("md5", "sha1", "sha256", "sha512", "blake2b"):
            with self.subTest(algorithm=algo):
                out = _run("text.hash", {"text": text}, algorithm=algo)
                expected = hashlib.new(algo, text.encode("utf-8")).hexdigest()
                self.assertEqual(out["digest"], expected)
                self.assertEqual(out["length"], len(text))

    def test_invalid_algorithm_raises_value_error(self):
        with self.assertRaises(ValueError):
            _run("text.hash", {"text": "x"}, algorithm="sha3")

    def test_empty_text(self):
        out = _run("text.hash", {"text": ""})
        self.assertEqual(out["digest"], hashlib.sha256(b"").hexdigest())
        self.assertEqual(out["length"], 0)

    def test_non_ascii_text(self):
        text = "中文摘要测试✨"
        out = _run("text.hash", {"text": text})
        self.assertEqual(out["digest"], hashlib.sha256(text.encode("utf-8")).hexdigest())
        self.assertEqual(out["length"], len(text))

    def test_encoding_param(self):
        text = "中文编码"
        out = _run("text.hash", {"text": text}, encoding="gbk")
        self.assertEqual(out["digest"], hashlib.sha256(text.encode("gbk")).hexdigest())
        self.assertEqual(out["length"], len(text))


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
