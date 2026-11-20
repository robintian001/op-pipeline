"""压缩算子测试（M9 Wave 1 Task 4）：compress.gzip + compress.ungzip。

运行：``python -m unittest discover -s tests -k CompressOps -v``
"""
from __future__ import annotations

import gzip
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from op_pipeline import RunContext, default_registry  # noqa: E402


def _run_in(workdir: str, name: str, inputs: dict | None = None, **params) -> dict:
    run_ctx = RunContext.create(pipeline="t", params={}, workdir=workdir)
    op_ = default_registry.get(name)(**params)
    return op_.execute(ctx=run_ctx.for_node("n", name), inputs=inputs or {})


class CompressOpsTest(unittest.TestCase):
    """compress.gzip / compress.ungzip：往返、确定性、编码、大文本、路径解析。"""

    def test_roundtrip_content_matches(self):
        with tempfile.TemporaryDirectory() as tmp:
            text = "压缩往返测试 Hello!\n第二行"
            out = _run_in(tmp, "compress.gzip", {"text": text}, path="a.txt.gz")
            target = Path(out["path"])
            self.assertTrue(target.exists())
            self.assertEqual(out["bytes"], target.stat().st_size)
            back = _run_in(tmp, "compress.ungzip", path="a.txt.gz")
            self.assertEqual(back["text"], text)
            self.assertEqual(back["length"], len(text))

    def test_same_content_bytes_identical(self):
        with tempfile.TemporaryDirectory() as tmp:
            text = "same content 同内容"
            first = _run_in(tmp, "compress.gzip", {"text": text}, path="one.gz")
            second = _run_in(tmp, "compress.gzip", {"text": text}, path="two.gz")
            self.assertEqual(Path(first["path"]).read_bytes(), Path(second["path"]).read_bytes())
            # gzip 头 mtime 字段（第 5-8 字节）固定为 0
            raw = Path(first["path"]).read_bytes()
            self.assertEqual(raw[:2], b"\x1f\x8b")
            self.assertEqual(raw[4:8], b"\x00\x00\x00\x00")

    def test_encoding_param(self):
        with tempfile.TemporaryDirectory() as tmp:
            text = "编码参数中文"
            _run_in(tmp, "compress.gzip", {"text": text}, path="e.bin", encoding="gbk")
            raw = (Path(tmp) / "e.bin").read_bytes()
            self.assertEqual(gzip.decompress(raw), text.encode("gbk"))
            back = _run_in(tmp, "compress.ungzip", path="e.bin", encoding="gbk")
            self.assertEqual(back["text"], text)
            self.assertEqual(back["length"], len(text))

    def test_large_text_roundtrip(self):
        with tempfile.TemporaryDirectory() as tmp:
            text = "op_pipeline 大文本测试 abc 123\n" * 20000
            out = _run_in(tmp, "compress.gzip", {"text": text}, path="big.txt.gz")
            self.assertLess(out["bytes"], len(text.encode("utf-8")))
            back = _run_in(tmp, "compress.ungzip", path="big.txt.gz")
            self.assertEqual(back["text"], text)
            self.assertEqual(back["length"], len(text))

    def test_parent_dirs_created(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = _run_in(tmp, "compress.gzip", {"text": "x"}, path="deep/nested/dir/a.gz")
            self.assertTrue(Path(out["path"]).exists())

    def test_relative_path_resolves_to_workdir(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = _run_in(tmp, "compress.gzip", {"text": "x"}, path="rel.gz")
            self.assertEqual(Path(out["path"]).parent, Path(tmp).resolve())
            self.assertEqual(Path(out["path"]).name, "rel.gz")
            back = _run_in(tmp, "compress.ungzip", path="rel.gz")
            self.assertEqual(back["text"], "x")
            self.assertEqual(back["length"], 1)

    def test_missing_file_raises_file_not_found(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(FileNotFoundError):
                _run_in(tmp, "compress.ungzip", path="nope.gz")


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
