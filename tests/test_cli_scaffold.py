"""M7-2 Task 5：``init`` 模板与 ``scaffold op`` 骨架。

插件算子必须注册到独立 ``Registry``（避免污染全局 ``default_registry``，
也避免影响按 ``op_pipeline.ops.*`` 前缀统计的文档一致性测试）。
"""
from __future__ import annotations

import contextlib
import importlib.util
import io
import logging
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from op_pipeline import Registry, load_plugins  # noqa: E402
from op_pipeline.cli import main  # noqa: E402

_MODULE_SEQ = 0


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


def _import_file(path: Path) -> None:
    """按文件路径执行 Python 模块（用于断言生成物可 import）。"""
    global _MODULE_SEQ
    _MODULE_SEQ += 1
    name = f"_scaffold_import_{_MODULE_SEQ}"
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    try:
        spec.loader.exec_module(module)
    finally:
        sys.modules.pop(name, None)


class InitCommandTest(unittest.TestCase):
    def test_all_templates_generate_and_validate(self):
        for template in ("minimal", "data", "scheduler"):
            with self.subTest(template=template), tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
                target = Path(tmp) / "proj"
                code, out, _err = _run(
                    ["init", str(target), "--template", template]
                )
                self.assertEqual(0, code, out)

                for rel in ("pipeline.toml", "plugins/my_ops.py", "tests/test_my_ops.py"):
                    self.assertTrue(
                        (target / rel).is_file(), f"{template} 模板缺少 {rel}"
                    )
                self.assertEqual(
                    template == "scheduler", (target / "scheduler.toml").is_file()
                )

                # 不传 --plugin 也必须校验通过（模板只用内置算子）
                code, out, err = _run(["validate", str(target / "pipeline.toml")])
                self.assertEqual(0, code, f"{template} 模板校验失败：{out}{err}")

                # 示例插件可加载（import 成功）并注册到独立注册表
                registry = Registry()
                load_plugins(paths=[target / "plugins" / "my_ops.py"], registry=registry)
                self.assertIn("my_ops.uppercase", registry)

                # 生成的测试骨架可 import（其中不含模块级 @op，不会污染全局表）
                _import_file(target / "tests" / "test_my_ops.py")

    def test_non_empty_directory_rejected(self):
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            target = Path(tmp) / "proj"
            target.mkdir()
            keep = target / "keep.txt"
            keep.write_text("keep", encoding="utf-8")

            code, _out, err = _run(["init", str(target), "--template", "minimal"])
            self.assertNotEqual(0, code)
            self.assertIn("非空", err)
            self.assertTrue(keep.exists())
            self.assertFalse((target / "pipeline.toml").exists())

    def test_existing_empty_directory_allowed(self):
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            target = Path(tmp) / "proj"
            target.mkdir()
            code, _out, _err = _run(["init", str(target)])
            self.assertEqual(0, code)
            self.assertTrue((target / "pipeline.toml").is_file())


class ScaffoldOpCommandTest(unittest.TestCase):
    def test_scaffold_op_registers_in_isolated_registry(self):
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            base = Path(tmp)
            code, out, _err = _run(
                ["scaffold", "op", "etl.dedupe", "--dir", str(base)]
            )
            self.assertEqual(0, code, out)

            module_path = base / "dedupe_op.py"
            test_path = base / "tests" / "test_dedupe_op.py"
            self.assertTrue(module_path.is_file())
            self.assertTrue(test_path.is_file())
            source = module_path.read_text(encoding="utf-8")
            self.assertIn('@op("etl.dedupe")', source)
            self.assertIn("inputs", source)
            self.assertIn("outputs", source)
            self.assertIn("params", source)
            # 骨架测试文件语法合法（不执行，避免注册进全局表）
            compile(test_path.read_text(encoding="utf-8"), str(test_path), "exec")

            registry = Registry()
            load_plugins(paths=[module_path], registry=registry)
            self.assertIn("etl.dedupe", registry)
            self.assertEqual(
                "etl.dedupe", registry.get("etl.dedupe").name
            )

    def test_invalid_operator_names_rejected_without_files(self):
        bad_names = (
            "",
            "etl dedupe",
            " etl.dedupe",
            "etl.dedupe ",
            ".etl.dedupe",
            "etl.dedupe.",
            "etl..dedupe",
            "etl/dedupe",
        )
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            base = Path(tmp)
            for name in bad_names:
                with self.subTest(name=name):
                    code, _out, err = _run(
                        ["scaffold", "op", name, "--dir", str(base)]
                    )
                    self.assertNotEqual(0, code)
                    self.assertIn("非法算子名", err)
            self.assertEqual([], list(base.iterdir()))

    def test_existing_target_not_overwritten(self):
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            base = Path(tmp)
            existing = base / "dedupe_op.py"
            existing.write_text("# 手工文件\n", encoding="utf-8")

            code, _out, err = _run(
                ["scaffold", "op", "etl.dedupe", "--dir", str(base)]
            )
            self.assertNotEqual(0, code)
            self.assertIn("已存在", err)
            self.assertEqual("# 手工文件\n", existing.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
