"""M7-2 Task 4：``validate`` 子命令（--json、退出码、节点清单、零副作用）。

统一直接调用 :func:`op_pipeline.cli.main` 并捕获 stdout，避免子进程开销；
退出码用 ``main`` 的返回值断言。全部配置写在临时目录，用例结束后目录自动清理。
"""
from __future__ import annotations

import contextlib
import io
import json
import logging
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from op_pipeline.cli import main  # noqa: E402


def _run(argv: list[str]) -> tuple[int, str, str]:
    root = logging.getLogger()
    before = list(root.handlers)
    out, err = io.StringIO(), io.StringIO()
    try:
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = main(argv)
    finally:
        # main 首次调用会 basicConfig 到被重定向的 stderr；还原 root handlers，
        # 避免污染同进程内的其他测试。
        for handler in list(root.handlers):
            if handler not in before:
                root.removeHandler(handler)
    return code, out.getvalue(), err.getvalue()


class ValidateCliTest(unittest.TestCase):
    @staticmethod
    def _write(tmp: str, name: str, text: str) -> Path:
        path = Path(tmp) / name
        path.write_text(text, encoding="utf-8")
        return path

    def _valid(self, tmp: str) -> Path:
        return self._write(
            tmp,
            "ok.toml",
            'name = "ok_pipeline"\n'
            "[[nodes]]\n"
            'id = "src"\n'
            'op = "control.const"\n'
            "[nodes.params]\n"
            "value = 42\n",
        )

    # ------------------------------------------------------------------ #
    def test_valid_config_human_output(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = self._valid(tmp)
            code, out, _err = _run(["validate", str(cfg)])
            self.assertEqual(0, code)
            self.assertIn("校验通过", out)
            self.assertIn("src", out)
            self.assertEqual(
                ["ok.toml"], sorted(p.name for p in Path(tmp).iterdir())
            )
            self.assertFalse((Path(tmp) / ".pipeline_cache").exists())
            self.assertFalse((Path(tmp) / "artifacts").exists())

    def test_valid_config_json(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = self._valid(tmp)
            code, out, _err = _run(["validate", str(cfg), "--json"])
            self.assertEqual(0, code)
            payload = json.loads(out)
            self.assertEqual(
                {"ok", "errors", "warnings", "nodes"}, set(payload)
            )
            self.assertTrue(payload["ok"])
            self.assertEqual([], payload["errors"])
            self.assertEqual([], payload["warnings"])
            self.assertEqual(
                [{"id": "src", "op": "control.const"}], payload["nodes"]
            )
            self.assertEqual(
                ["ok.toml"], sorted(p.name for p in Path(tmp).iterdir())
            )

    def test_json_collects_warnings(self):
        """未知键告警进入 warnings，退出码仍为 0。"""
        with tempfile.TemporaryDirectory() as tmp:
            cfg = self._write(
                tmp,
                "typo.toml",
                'name = "typo"\n'
                'param = { a = 1 }\n'  # 拼错：应为 params
                "[[nodes]]\n"
                'id = "src"\n'
                'op = "control.const"\n'
                "[nodes.params]\n"
                "value = 1\n",
            )
            code, out, _err = _run(["validate", str(cfg), "--json"])
            self.assertEqual(0, code)
            payload = json.loads(out)
            self.assertTrue(payload["ok"])
            self.assertTrue(
                any("未知键" in w for w in payload["warnings"]),
                payload["warnings"],
            )

    def test_missing_operator_fails_with_exit_code_1(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = self._write(
                tmp,
                "bad.toml",
                'name = "bad"\n'
                "[[nodes]]\n"
                'id = "x"\n'
                'op = "no_such.operator"\n',
            )
            code, out, _err = _run(["validate", str(cfg), "--json"])
            self.assertEqual(1, code)
            payload = json.loads(out)
            self.assertFalse(payload["ok"])
            self.assertTrue(
                any("no_such.operator" in e for e in payload["errors"]),
                payload["errors"],
            )
            self.assertEqual(
                [{"id": "x", "op": "no_such.operator"}], payload["nodes"]
            )

    def test_cycle_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = self._write(
                tmp,
                "cycle.toml",
                'name = "cycle"\n'
                "[[nodes]]\n"
                'id = "a"\n'
                'op = "control.passthrough"\n'
                "[nodes.inputs]\n"
                'value = "${b.value}"\n'
                "[[nodes]]\n"
                'id = "b"\n'
                'op = "control.passthrough"\n'
                "[nodes.inputs]\n"
                'value = "${a.value}"\n',
            )
            code, out, _err = _run(["validate", str(cfg), "--json"])
            self.assertEqual(1, code)
            payload = json.loads(out)
            self.assertFalse(payload["ok"])
            self.assertTrue(
                any("环" in e for e in payload["errors"]), payload["errors"]
            )
            self.assertEqual(2, len(payload["nodes"]))

    def test_param_type_error_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = self._write(
                tmp,
                "type.toml",
                'name = "type"\n'
                "[[nodes]]\n"
                'id = "src"\n'
                'op = "control.const"\n'
                "[nodes.params]\n"
                "value = [1, 2, 3]\n"
                "[[nodes]]\n"
                'id = "lim"\n'
                'op = "table.limit"\n'
                "[nodes.inputs]\n"
                'table = "${src.value}"\n'
                "[nodes.params]\n"
                'n = "not-an-int"\n',
            )
            code, out, _err = _run(["validate", str(cfg), "--json"])
            self.assertEqual(1, code)
            payload = json.loads(out)
            self.assertFalse(payload["ok"])
            self.assertTrue(
                any("not-an-int" in e or "int" in e for e in payload["errors"]),
                payload["errors"],
            )

    def test_failure_human_output_goes_to_stderr(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = self._write(
                tmp,
                "bad.toml",
                'name = "bad"\n'
                "[[nodes]]\n"
                'id = "x"\n'
                'op = "no_such.operator"\n',
            )
            code, out, err = _run(["validate", str(cfg)])
            self.assertEqual(1, code)
            self.assertIn("校验失败", err)
            self.assertIn("no_such.operator", err)
            self.assertNotIn("校验通过", out)


if __name__ == "__main__":
    unittest.main()
