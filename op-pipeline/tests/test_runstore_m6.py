"""RunStore 增强测试（M6-2 Task 5）：节点耗时列、旧库迁移、历史清理。

运行：``python -m unittest discover -s tests -k test_runstore_m6 -v``

覆盖：
- 新建库 nodes 表含 duration 列，record_node 写入的耗时可查；
- 旧库（手工建表、无 duration）打开后自动补列，旧行耗时为 0；
- ``prune(keep_last)`` 按 started_at DESC + run_id 兜底保留最近 N 条并级联删除 nodes；
- ``keep_last <= 0`` 抛 ValueError；空库返回 0；只有 runs 表（无 nodes）不报错。

遵循现有测试风格：仅文件顶部做路径准备，op_pipeline 的 import 放在测试方法内。
"""
from __future__ import annotations

import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

# 旧版 nodes 建表语句（v0.6 及之前，无 duration 列），用于模拟历史库。
_OLD_SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    run_id      TEXT PRIMARY KEY,
    pipeline    TEXT NOT NULL,
    version     TEXT NOT NULL,
    status      TEXT NOT NULL,
    started_at  REAL NOT NULL,
    finished_at REAL NOT NULL,
    params_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS nodes (
    run_id      TEXT NOT NULL,
    node_id     TEXT NOT NULL,
    op          TEXT NOT NULL,
    status      TEXT NOT NULL,
    attempts    INTEGER NOT NULL,
    cached      INTEGER NOT NULL,
    error       TEXT,
    cache_key   TEXT,
    finished_at REAL NOT NULL,
    PRIMARY KEY (run_id, node_id)
);
"""


def _columns(conn: sqlite3.Connection, table: str) -> list[str]:
    return [row[1] for row in conn.execute(f"PRAGMA table_info({table})")]


class RunStoreDurationTest(unittest.TestCase):
    """Task 5：nodes.duration 建表、写入与旧库迁移。"""

    def test_new_db_has_duration_column(self):
        from op_pipeline import RunStore

        with tempfile.TemporaryDirectory() as d:
            db = Path(d) / "runs.db"
            with RunStore(db) as store:
                store.record_run("r1", "demo", "1.0", "success", 1.0, 2.0, {})
            conn = sqlite3.connect(db)
            try:
                self.assertIn("duration", _columns(conn, "nodes"))
            finally:
                conn.close()

    def test_record_node_writes_duration(self):
        from op_pipeline import RunStore

        with tempfile.TemporaryDirectory() as d:
            db = Path(d) / "runs.db"
            with RunStore(db) as store:
                store.record_run("r1", "demo", "1.0", "success", 1.0, 2.0, {})
                store.record_node(
                    "r1", "a", "control.const", "success", 1, False, None, None, 1.5,
                    duration=1.25,
                )
                rows = store.query(
                    "SELECT duration FROM nodes WHERE run_id = ? AND node_id = ?",
                    ("r1", "a"),
                )
            self.assertEqual(rows[0][0], 1.25)

    def test_record_node_positional_call_keeps_default_duration(self):
        """既有位置参数调用兼容：不传 duration 时落 0。"""
        from op_pipeline import RunStore

        with tempfile.TemporaryDirectory() as d:
            db = Path(d) / "runs.db"
            with RunStore(db) as store:
                store.record_run("r1", "demo", "1.0", "success", 1.0, 2.0, {})
                store.record_node(
                    "r1", "a", "control.const", "success", 1, False, None, "k", 1.5,
                )
                rows = store.query("SELECT duration FROM nodes")
            self.assertEqual(rows[0][0], 0)

    def test_old_db_gets_duration_column_and_zero_default(self):
        """旧库无 duration：打开后自动 ALTER 补列，旧行取默认 0。"""
        from op_pipeline import RunStore

        with tempfile.TemporaryDirectory() as d:
            db = Path(d) / "old.db"
            conn = sqlite3.connect(db)
            conn.executescript(_OLD_SCHEMA)
            conn.execute(
                "INSERT INTO runs VALUES (?, ?, ?, ?, ?, ?, ?)",
                ("old-run", "legacy", "0.6", "success", 10.0, 11.0, "{}"),
            )
            conn.execute(
                "INSERT INTO nodes VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                ("old-run", "n1", "control.const", "success", 1, 0, None, None, 10.5),
            )
            conn.commit()
            conn.close()

            with RunStore(db) as store:
                rows = store.query("SELECT duration FROM nodes WHERE run_id = 'old-run'")
                cols = store.query("PRAGMA table_info(nodes)")
            self.assertIn("duration", [row[1] for row in cols])
            self.assertEqual(rows[0][0], 0, "旧行 duration 应取默认值 0")

    def test_old_db_only_runs_table_is_migrated(self):
        """旧库只有 runs 表（无 nodes）：建表后自带 duration，无异常。"""
        from op_pipeline import RunStore

        with tempfile.TemporaryDirectory() as d:
            db = Path(d) / "runs_only.db"
            conn = sqlite3.connect(db)
            conn.executescript(
                "CREATE TABLE runs (run_id TEXT PRIMARY KEY, pipeline TEXT NOT NULL,"
                " version TEXT NOT NULL, status TEXT NOT NULL, started_at REAL NOT NULL,"
                " finished_at REAL NOT NULL, params_json TEXT NOT NULL);"
            )
            conn.commit()
            conn.close()

            with RunStore(db) as store:
                cols = store.query("PRAGMA table_info(nodes)")
            self.assertIn("duration", [row[1] for row in cols])


class RunStorePruneTest(unittest.TestCase):
    """Task 5：prune 保留最近 N 条并按 run_id 兜底排序。"""

    @staticmethod
    def _seed(store, count: int, start: float = 1000.0) -> None:
        for i in range(count):
            run_id = f"r{i}"
            store.record_run(run_id, "demo", "1.0", "success", start + i, start + i + 0.5, {})
            store.record_node(
                run_id, "n1", "control.const", "success", 1, False, None, None,
                start + i + 0.4, duration=0.4,
            )

    def test_prune_keeps_recent_runs_and_deletes_nodes(self):
        from op_pipeline import RunStore

        with tempfile.TemporaryDirectory() as d:
            db = Path(d) / "runs.db"
            with RunStore(db) as store:
                self._seed(store, 5)
                deleted = store.prune(2)
                runs = [row[0] for row in store.query("SELECT run_id FROM runs")]
                nodes = [row[0] for row in store.query("SELECT run_id FROM nodes")]
            self.assertEqual(deleted, 3)
            self.assertEqual(sorted(runs), ["r3", "r4"], "应保留最近 2 条")
            self.assertEqual(sorted(nodes), ["r3", "r4"], "nodes 应级联删除")

    def test_prune_orders_same_timestamp_by_run_id(self):
        from op_pipeline import RunStore

        with tempfile.TemporaryDirectory() as d:
            db = Path(d) / "runs.db"
            with RunStore(db) as store:
                for run_id in ("r1", "r2", "r3"):
                    store.record_run(run_id, "demo", "1.0", "success", 1000.0, 1001.0, {})
                self.assertEqual(store.prune(2), 1)
                runs = [row[0] for row in store.query("SELECT run_id FROM runs")]
            self.assertEqual(sorted(runs), ["r2", "r3"], "同秒按 run_id DESC 保留")

    def test_prune_rejects_non_positive(self):
        from op_pipeline import RunStore

        with tempfile.TemporaryDirectory() as d:
            db = Path(d) / "runs.db"
            with RunStore(db) as store:
                with self.assertRaises(ValueError):
                    store.prune(0)
                with self.assertRaises(ValueError):
                    store.prune(-1)

    def test_prune_empty_db_returns_zero(self):
        from op_pipeline import RunStore

        with tempfile.TemporaryDirectory() as d:
            db = Path(d) / "runs.db"
            with RunStore(db) as store:
                self.assertEqual(store.prune(5), 0)

    def test_prune_without_nodes_table(self):
        """外部库只有 runs 表：prune 不报错，仅删 runs。"""
        from op_pipeline import RunStore

        with tempfile.TemporaryDirectory() as d:
            db = Path(d) / "runs_only.db"
            conn = sqlite3.connect(db)
            conn.executescript(
                "CREATE TABLE runs (run_id TEXT PRIMARY KEY, pipeline TEXT NOT NULL,"
                " version TEXT NOT NULL, status TEXT NOT NULL, started_at REAL NOT NULL,"
                " finished_at REAL NOT NULL, params_json TEXT NOT NULL);"
            )
            conn.execute(
                "INSERT INTO runs VALUES ('r1', 'demo', '1.0', 'success', 1.0, 2.0, '{}')"
            )
            conn.execute(
                "INSERT INTO runs VALUES ('r2', 'demo', '1.0', 'success', 3.0, 4.0, '{}')"
            )
            conn.commit()
            conn.close()

            with RunStore(db) as store:
                self.assertEqual(store.prune(1), 1)
                runs = [row[0] for row in store.query("SELECT run_id FROM runs")]
            self.assertEqual(runs, ["r2"])


if __name__ == "__main__":
    unittest.main()
