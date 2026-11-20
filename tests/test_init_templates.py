"""M9 Wave 3 Task 9：``init`` 新模板 quality / subpipeline（类名含 InitTemplate）。

覆盖：quality 模板 validate 通过（含实际运行冒烟）、subpipeline 恰好生成两个文件
且父/子配置均可独立 validate、父配置可运行并注入 ``${param:}``、既有模板回归。
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
        for handler in list(root.handlers):
            if handler not in before:
                root.removeHandler(handler)
    return code, out.getvalue(), err.getvalue()


class InitTemplateTest(unittest.TestCase):
    def test_quality_template_validates_and_runs(self):
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            target = Path(tmp) / "proj"
            code, out, _err = _run(
                ["init", str(target), "--template", "quality"]
            )
            self.assertEqual(0, code, out)
            self.assertTrue((target / "pipeline.toml").is_file())

            # 与既有模板一致的配套骨架
            for rel in ("plugins/my_ops.py", "tests/test_my_ops.py"):
                self.assertTrue((target / rel).is_file(), f"缺少 {rel}")

            # 生成的配置 validate 通过（全部内置算子，无需 --plugin）
            code, out, err = _run(["validate", str(target / "pipeline.toml")])
            self.assertEqual(0, code, f"quality 模板校验失败：{out}{err}")

            # 冒烟：实际可运行（audit 因 region 重复失败但 allow_failure，
            # 整体 status 为 partial，退出码仍为 0）
            code, out, err = _run(["run", str(target / "pipeline.toml")])
            self.assertEqual(0, code, out + err)
            self.assertIn("partial", out)

    def test_subpipeline_generates_two_files(self):
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            target = Path(tmp) / "proj"
            code, out, _err = _run(
                ["init", str(target), "--template", "subpipeline"]
            )
            self.assertEqual(0, code, out)

            # 恰好生成两个文件：父配置 + 子配置
            generated = {
                p.relative_to(target).as_posix()
                for p in target.rglob("*") if p.is_file()
            }
            self.assertEqual({"pipeline.toml", "child.toml"}, generated)

            # 父配置与子配置均可独立 validate
            code, out, err = _run(["validate", str(target / "pipeline.toml")])
            self.assertEqual(0, code, f"父配置校验失败：{out}{err}")
            code, out, err = _run(["validate", str(target / "child.toml")])
            self.assertEqual(0, code, f"子配置校验失败：{out}{err}")

    def test_subpipeline_parent_runs_with_param_injection(self):
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            target = Path(tmp) / "proj"
            code, out, _err = _run(
                ["init", str(target), "--template", "subpipeline"]
            )
            self.assertEqual(0, code, out)

            # 父配置经 pipeline.run 调用 child.toml，并把 --param 注入子流水线
            report = Path(tmp) / "report.json"
            code, out, err = _run(
                ["run", str(target / "pipeline.toml"),
                 "--param", "CITY=上海", "--report-out", str(report)]
            )
            self.assertEqual(0, code, out + err)
            data = json.loads(report.read_text(encoding="utf-8"))
            self.assertEqual("subpipeline_demo", data["pipeline"])
            self.assertEqual("你好，上海！", data["outputs"]["out"])

    def test_existing_templates_regression(self):
        for template in ("minimal", "data", "scheduler"):
            with self.subTest(template=template), \
                    tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
                target = Path(tmp) / "proj"
                code, out, _err = _run(
                    ["init", str(target), "--template", template]
                )
                self.assertEqual(0, code, out)
                code, out, err = _run(
                    ["validate", str(target / "pipeline.toml")]
                )
                self.assertEqual(0, code, f"{template} 模板回归失败：{out}{err}")


if __name__ == "__main__":
    unittest.main()
