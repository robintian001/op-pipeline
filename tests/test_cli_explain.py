"""M8-2 Task 6：``explain`` 子命令。

覆盖：env 替换后值可见、defaults 合并生效、``--param`` 注入生效、
``--node`` 过滤、``--json`` 结构与退出码 0/1、无文件产出断言。
"""
from __future__ import annotations

import contextlib
import io
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from op_pipeline.cli import main as cli_main  # noqa: E402

CONFIG_TEMPLATE = "\n".join(
    [
        'name = "explain_demo"',
        'version = "2"',
        "[params]",
        'env = "${env:M82_TZ:-utc}"',
        "[defaults]",
        "retries = 2",
        "[[nodes]]",
        'id = "src"',
        'op = "control.const"',
        "[nodes.params]",
        'value = "${param:greeting:-hi}"',
        "[[nodes]]",
        'id = "next"',
        "op = 'control.passthrough'",
        "[nodes.inputs]",
        'value = "${src.value}"',
    ]
)


class ExplainCommandTests(unittest.TestCase):
    """explain 加载/输出语义。"""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.cfg = Path(self._tmp.name) / "pipeline.toml"
        self.cfg.write_text(CONFIG_TEMPLATE, encoding="utf-8")

    def tearDown(self):
        self._tmp.cleanup()

    def _run(self, *extra, env=None):
        out, err = io.StringIO(), io.StringIO()
        environ = {} if env is None else {"M82_TZ": env}
        with mock.patch.dict(os.environ, environ, clear=False):
            with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
                code = cli_main(["explain", str(self.cfg), *extra])
        return code, out.getvalue(), err.getvalue()

    def test_env_placeholder_resolved_in_output(self):
        """env 替换后值可见：默认值与注入值两条路径。"""
        code, text, _ = self._run()
        self.assertEqual(code, 0)
        self.assertIn("utc", text)
        code, text, _ = self._run(env="asia/shanghai")
        self.assertEqual(code, 0)
        self.assertIn("asia/shanghai", text)

    def test_defaults_merged_into_policy(self):
        """defaults 合并生效：未显式设置 retries 的节点展示 defaults.retries。"""
        code, payload = self._run_json()
        self.assertEqual(code, 0)
        for node in payload["nodes"]:
            self.assertEqual(node["policy"]["retries"], 2)

    def test_param_injection_visible(self):
        """--param 注入生效：节点参数展示替换后的值。"""
        code, text, _ = self._run("--param", "greeting=你好")
        self.assertEqual(code, 0)
        self.assertIn("你好", text)
        code, payload = self._run_json("--param", "greeting=你好")
        src = next(n for n in payload["nodes"] if n["id"] == "src")
        self.assertEqual(src["params"]["value"], "你好")

    def test_param_injection_masked_for_sensitive_keys(self):
        """敏感键注入沿用 M7-2 打码：参数区显示 ***。"""
        code, payload = self._run_json("--param", "greeting=ABC", "--param", "API_KEY=xyz")
        self.assertEqual(code, 0)
        self.assertEqual(payload["params"]["API_KEY"], "***")

    def test_node_filter(self):
        """--node <id> 只输出该节点。"""
        code, payload = self._run_json("--node", "src")
        self.assertEqual(code, 0)
        self.assertEqual([n["id"] for n in payload["nodes"]], ["src"])
        code, _, err = self._run("--node", "nope")
        self.assertEqual(code, 1)
        self.assertIn("nope", err)

    def test_json_structure_and_exit_code(self):
        """--json 结构固定且退出码 0。"""
        code, payload = self._run_json()
        self.assertEqual(code, 0)
        self.assertEqual(
            set(payload),
            {"pipeline", "version", "params", "nodes", "warnings"},
        )
        self.assertEqual(payload["pipeline"], "explain_demo")
        self.assertEqual(payload["version"], "2")
        self.assertEqual(payload["params"]["env"], "utc")
        self.assertIsInstance(payload["warnings"], list)
        for node in payload["nodes"]:
            self.assertEqual(
                set(node),
                {"id", "op", "params", "inputs", "when", "policy", "depends_on"},
            )
        self.assertEqual([n["id"] for n in payload["nodes"]], ["src", "next"])
        self.assertEqual(payload["nodes"][1]["depends_on"], ["src"])

    def test_invalid_config_exit_code_1(self):
        """配置非法退出码 1，错误信息可见。"""
        bad = Path(self._tmp.name) / "bad.toml"
        bad.write_text(
            "\n".join(
                [
                    'name = "bad"',
                    "[[nodes]]",
                    'id = "src"',
                    'op = "no.such.op"',
                ]
            ),
            encoding="utf-8",
        )
        code, text, err = self._run_cfg(bad)
        self.assertEqual(code, 1)
        self.assertIn("no.such.op", err)
        code, text_json = self._run_json_cfg(bad)
        self.assertEqual(code, 1)
        self.assertIn("errors", json.loads(text_json))

    def test_no_files_written(self):
        """explain 不执行节点、不创建 artifacts/.pipeline_cache/任何输出文件。"""
        workdir = Path(self._tmp.name)
        before = sorted(p.name for p in workdir.iterdir())
        code, _, _ = self._run("--json")
        self.assertEqual(code, 0)
        after = sorted(p.name for p in workdir.iterdir())
        self.assertEqual(after, before)
        self.assertEqual(after, ["pipeline.toml"])

    # ---- 辅助 ----
    def _run_json(self, *extra):
        code, out, _ = self._run("--json", *extra)
        return code, json.loads(out)

    def _run_cfg(self, cfg, *extra):
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = cli_main(["explain", str(cfg), *extra])
        return code, out.getvalue(), err.getvalue()

    def _run_json_cfg(self, cfg):
        code, out, _ = self._run_cfg(cfg, "--json")
        return code, out


if __name__ == "__main__":
    unittest.main()
