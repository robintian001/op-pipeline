"""M9 Wave 3 Task 10：``runs`` 运行历史查询子命令（类名含 CliRuns）。

覆盖：列表（全量/--status/--pipeline 子串/--last/--json）、详情（run 摘要 +
节点状态/attempts/错误摘要/行数）、库不存在退出码 1、库存在但无匹配退出码 0。
"""
from __future__ import annotations

import contextlib
import io
import json
import logging
import re
import shutil
import sqlite3
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


def _seed_db(path: Path) -> None:
    """写入 3 条运行记录（r1/r2 带节点明细，r3 无节点明细）。"""
    from op_pipeline.store import RunStore

    with RunStore(path) as store:
        store.record_run("r1", "demo_pipeline", "1", "success", 1000.0, 1001.0, {"k": 1})
        store.record_node(
            "r1", "src", "control.const", "success", 1, False, None, "k1",
            1000.5, 0.1, None, {"value": 3},
        )
        store.record_node(
            "r1", "lim", "table.limit", "success", 1, False, None, "k2",
            1000.6, 0.2, {"table": 3}, {"table": 2},
        )
        store.record_run("r2", "other_pipeline", "2", "failed", 2000.0, 2001.0, {})
        store.record_node(
            "r2", "chk", "control.assert", "failed", 2, False, "断言失败：数值太小", "k3",
            2000.5, 0.05, {"value": 1}, None,
        )
        store.record_run("r3", "demo_pipeline", "1", "success", 3000.0, 3000.5, {})


class CliRunsTest(unittest.TestCase):
    def _db(self) -> Path:
        tmp = tempfile.mkdtemp(prefix="cli_runs_")
        db = Path(tmp) / "runs.db"
        _seed_db(db)
        return db

    def test_list_all(self):
        db = self._db()
        try:
            code, out, err = _run(["runs", "--db", str(db)])
            self.assertEqual(0, code, err)
            self.assertIn("r1", out)
            self.assertIn("r2", out)
            self.assertIn("r3", out)
            self.assertIn("demo_pipeline", out)
        finally:
            shutil.rmtree(db.parent, ignore_errors=True)

    def test_list_filter_status(self):
        db = self._db()
        try:
            code, out, err = _run(["runs", "--db", str(db), "--status", "success"])
            self.assertEqual(0, code, err)
            self.assertIn("r1", out)
            self.assertIn("r3", out)
            self.assertNotIn("other_pipeline", out)
        finally:
            shutil.rmtree(db.parent, ignore_errors=True)

    def test_list_filter_pipeline_substring(self):
        db = self._db()
        try:
            code, out, err = _run(["runs", "--db", str(db), "--pipeline", "demo"])
            self.assertEqual(0, code, err)
            self.assertIn("r1", out)
            self.assertIn("r3", out)
            self.assertNotIn("other_pipeline", out)
        finally:
            shutil.rmtree(db.parent, ignore_errors=True)

    def test_list_last_limit(self):
        db = self._db()
        try:
            # 按 started_at 倒序：r3(3000) -> r2(2000) -> r1(1000)
            code, out, err = _run(["runs", "--db", str(db), "--last", "2"])
            self.assertEqual(0, code, err)
            self.assertIn("r3", out)
            self.assertIn("other_pipeline", out)  # r2 在最近 2 条内
            self.assertEqual(1, out.count("demo_pipeline"))  # 仅 r3
        finally:
            shutil.rmtree(db.parent, ignore_errors=True)

    def test_list_json(self):
        db = self._db()
        try:
            code, out, err = _run(["runs", "--db", str(db), "--json"])
            self.assertEqual(0, code, err)
            items = json.loads(out)
            self.assertEqual(3, len(items))
            self.assertEqual("r1", items[-1]["run_id"])  # 时间倒序，r1 排最后
            self.assertEqual("demo_pipeline", items[-1]["pipeline"])
            self.assertEqual("success", items[-1]["status"])
            self.assertIn("started_at", items[-1])
        finally:
            shutil.rmtree(db.parent, ignore_errors=True)

    def test_detail_output(self):
        db = self._db()
        try:
            code, out, err = _run(["runs", "r1", "--db", str(db)])
            self.assertEqual(0, code, err)
            self.assertIn("run_id   : r1", out)
            self.assertIn("demo_pipeline", out)
            self.assertIn("success", out)
            self.assertIn("src", out)
            self.assertIn("lim", out)
            # 行数血缘摘要（src: out=3；lim: in=3、out=2）
            self.assertIn("out=3", out)
            self.assertIn("in=3", out)
            self.assertIn("out=2", out)
        finally:
            shutil.rmtree(db.parent, ignore_errors=True)

    def test_detail_error_and_attempts_summary(self):
        db = self._db()
        try:
            code, out, err = _run(["runs", "r2", "--db", str(db)])
            self.assertEqual(0, code, err)
            self.assertIn("failed", out)
            self.assertIn("chk", out)
            self.assertIn("断言失败：数值太小", out)  # 错误摘要
            # chk 行：状态 failed、尝试次数 2、耗时 0.05s、行数 in=1
            self.assertTrue(
                re.search(r"chk\s+failed\s+2\s+0\.05s\s+in=1\s+control\.assert", out),
                f"chk 行摘要缺失：{out}",
            )
        finally:
            shutil.rmtree(db.parent, ignore_errors=True)

    def test_missing_db_exit_1(self):
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            code, _out, err = _run(["runs", "--db", str(Path(tmp) / "nope.db")])
            self.assertEqual(1, code)
            self.assertIn("运行记录库不存在", err)

    def test_empty_db_no_match_exit_0(self):
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            db = Path(tmp) / "empty.db"
            sqlite3.connect(db).close()  # 库文件存在但无任何运行记录
            code, out, _err = _run(["runs", "--db", str(db)])
            self.assertEqual(0, code)
            self.assertIn("无匹配", out)

    def test_detail_unknown_run_id_exit_0(self):
        db = self._db()
        try:
            code, _out, err = _run(["runs", "ghost", "--db", str(db)])
            self.assertEqual(0, code)
            self.assertIn("运行记录不存在", err)
        finally:
            shutil.rmtree(db.parent, ignore_errors=True)

    def test_last_invalid_exit_2(self):
        db = self._db()
        try:
            code, _out, err = _run(["runs", "--db", str(db), "--last", "0"])
            self.assertEqual(2, code)
            self.assertIn("≥1", err)
        finally:
            shutil.rmtree(db.parent, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
