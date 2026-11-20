"""M9 Wave 3 Task 8：``--param-file`` 与 ``--report-out``（类名含 CliDx 供 -k 过滤）。

覆盖：JSON 参数文件加载与 ``--param`` 合并覆盖、坏文件（不存在/非 object/非法
JSON）报错退出码非 0、report-out 全量报告落盘（含 rows_in/rows_out）与默认不写、
写出失败只记日志不掩盖运行状态。
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
    """执行 CLI 并返回 (退出码, stdout, stderr)；捕获 SystemExit 转为退出码。"""
    root = logging.getLogger()
    before = list(root.handlers)
    out, err = io.StringIO(), io.StringIO()
    code: int | None = None
    try:
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            try:
                code = main(argv)
            except SystemExit as exc:  # --param/--param-file 用法错误按退出码处理
                code = exc.code if isinstance(exc.code, int) else 0
    finally:
        for handler in list(root.handlers):
            if handler not in before:
                root.removeHandler(handler)
    return code if code is not None else -1, out.getvalue(), err.getvalue()


_PARAM_PIPELINE = """\
name = "param_demo"
version = "1"

[params]
GREETING = "默认问候"
EXTRA = "默认额外"

[[nodes]]
id = "src"
op = "control.const"

  [nodes.params]
  value = "${param:GREETING}|${param:EXTRA}"

[outputs]
out = "${src.value}"
"""

_ROW_PIPELINE = """\
name = "dx"
version = "1"

[[nodes]]
id = "src"
op = "control.const"

  [nodes.params]
  value = [{ k = 1 }, { k = 2 }, { k = 3 }]

[[nodes]]
id = "lim"
op = "table.limit"

  [nodes.inputs]
  table = "${src.value}"

  [nodes.params]
  n = 2
"""

_FAILING_PIPELINE = """\
name = "failing"
version = "1"

[[nodes]]
id = "chk"
op = "control.assert"

  [nodes.inputs]
  value = 1

  [nodes.params]
  that = "value > 10"
  message = "数值太小"
"""


def _src_param_value(data: dict) -> str:
    """从 explain --json 结果中取节点 src 的 params.value。"""
    for node in data["nodes"]:
        if node["id"] == "src":
            return node["params"]["value"]
    raise AssertionError("explain --json 缺少节点 src")


class CliDxTest(unittest.TestCase):
    def test_param_file_loaded(self):
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            base = Path(tmp)
            cfg = base / "pipeline.toml"
            f = base / "params.json"
            cfg.write_text(_PARAM_PIPELINE, encoding="utf-8")
            f.write_text(json.dumps({"GREETING": "来自文件"}), encoding="utf-8")

            code, out, err = _run(
                ["explain", str(cfg), "--json", "--param-file", str(f)]
            )
            self.assertEqual(0, code, out + err)
            self.assertEqual("来自文件|默认额外", _src_param_value(json.loads(out)))

    def test_param_file_merged_and_cli_overrides(self):
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            base = Path(tmp)
            cfg = base / "pipeline.toml"
            f = base / "params.json"
            cfg.write_text(_PARAM_PIPELINE, encoding="utf-8")
            f.write_text(json.dumps({"GREETING": "来自文件", "EXTRA": "文件值"}), encoding="utf-8")

            # 命令行 --param 覆盖文件中的同名键
            code, out, err = _run(
                ["explain", str(cfg), "--json", "--param-file", str(f), "--param", "EXTRA=命令行"]
            )
            self.assertEqual(0, code, out + err)
            self.assertEqual("来自文件|命令行", _src_param_value(json.loads(out)))

    def test_param_file_multiple_files_accumulate(self):
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            base = Path(tmp)
            cfg = base / "pipeline.toml"
            f1 = base / "a.json"
            f2 = base / "b.json"
            cfg.write_text(_PARAM_PIPELINE, encoding="utf-8")
            f1.write_text(json.dumps({"GREETING": "第一份"}), encoding="utf-8")
            f2.write_text(json.dumps({"GREETING": "第二份", "EXTRA": "补充值"}), encoding="utf-8")

            # 后加载的文件覆盖先加载的文件（再被 --param 覆盖）
            code, out, err = _run(
                ["explain", str(cfg), "--json",
                 "--param-file", str(f1), "--param-file", str(f2)]
            )
            self.assertEqual(0, code, out + err)
            self.assertEqual("第二份|补充值", _src_param_value(json.loads(out)))

    def test_param_file_missing_error(self):
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            cfg = Path(tmp) / "pipeline.toml"
            cfg.write_text(_PARAM_PIPELINE, encoding="utf-8")
            code, _out, err = _run(
                ["explain", str(cfg), "--param-file", str(Path(tmp) / "nope.json")]
            )
            self.assertEqual(2, code)
            self.assertIn("无法读取", err)

    def test_param_file_non_object_error(self):
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            cfg = Path(tmp) / "pipeline.toml"
            f = Path(tmp) / "params.json"
            cfg.write_text(_PARAM_PIPELINE, encoding="utf-8")
            f.write_text("[1, 2, 3]", encoding="utf-8")
            code, _out, err = _run(
                ["explain", str(cfg), "--param-file", str(f)]
            )
            self.assertEqual(2, code)
            self.assertIn("顶层必须是 JSON 对象", err)

    def test_param_file_invalid_json_error(self):
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            cfg = Path(tmp) / "pipeline.toml"
            f = Path(tmp) / "params.json"
            cfg.write_text(_PARAM_PIPELINE, encoding="utf-8")
            f.write_text("这不是 JSON", encoding="utf-8")
            code, _out, err = _run(
                ["explain", str(cfg), "--param-file", str(f)]
            )
            self.assertEqual(2, code)
            self.assertIn("不是合法 JSON", err)

    def test_run_param_file_rejects_bad_file(self):
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            cfg = Path(tmp) / "pipeline.toml"
            cfg.write_text(_PARAM_PIPELINE, encoding="utf-8")
            code, _out, err = _run(
                ["run", str(cfg), "--param-file", str(Path(tmp) / "nope.json")]
            )
            self.assertNotEqual(0, code)
            self.assertIn("无法读取", err)

    def test_run_report_out_written_with_rows(self):
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            base = Path(tmp)
            cfg = base / "pipeline.toml"
            report = base / "out" / "report.json"
            cfg.write_text(_ROW_PIPELINE, encoding="utf-8")

            code, _out, err = _run(
                ["run", str(cfg), "--report-out", str(report)]
            )
            self.assertEqual(0, code, err)
            self.assertTrue(report.is_file(), "report-out 未落盘")

            data = json.loads(report.read_text(encoding="utf-8"))
            self.assertEqual("success", data["status"])
            self.assertEqual("dx", data["pipeline"])
            self.assertTrue(data["run_id"])
            nodes = {n["id"]: n for n in data["nodes"]}
            self.assertEqual({"table": 3}, nodes["lim"]["rows_in"])
            self.assertEqual({"table": 2}, nodes["lim"]["rows_out"])
            self.assertEqual({"value": 3}, nodes["src"]["rows_out"])

    def test_run_report_out_not_written_by_default(self):
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            base = Path(tmp)
            cfg = base / "pipeline.toml"
            cfg.write_text(_ROW_PIPELINE, encoding="utf-8")

            code, _out, err = _run(["run", str(cfg)])
            self.assertEqual(0, code, err)
            self.assertFalse((base / "report.json").exists())
            # 工作目录（= 配置目录）里也不应出现 report 文件
            self.assertEqual(
                [], [p for p in base.glob("*.json") if "report" in p.name]
            )

    def test_run_report_out_write_failure_does_not_mask_status(self):
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            base = Path(tmp)
            cfg = base / "pipeline.toml"
            cfg.write_text(_ROW_PIPELINE, encoding="utf-8")
            blocker = base / "blocker"
            blocker.write_text("占位文件", encoding="utf-8")
            target = blocker / "report.json"  # 父路径是普通文件 -> mkdir 必然失败

            code, _out, err = _run(
                ["run", str(cfg), "--report-out", str(target)]
            )
            # 写出失败只记日志，绝不掩盖运行结果（成功 -> 退出码 0）
            self.assertEqual(0, code, err)
            self.assertIn("运行报告写入失败", err)

    def test_run_report_out_written_on_failed_run(self):
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            base = Path(tmp)
            cfg = base / "pipeline.toml"
            report = base / "report.json"
            cfg.write_text(_FAILING_PIPELINE, encoding="utf-8")

            code, _out, err = _run(
                ["run", str(cfg), "--report-out", str(report)]
            )
            # 运行失败 -> 退出码 1，但报告仍落盘
            self.assertEqual(1, code, err)
            self.assertTrue(report.is_file())
            data = json.loads(report.read_text(encoding="utf-8"))
            self.assertEqual("failed", data["status"])


if __name__ == "__main__":
    unittest.main()
