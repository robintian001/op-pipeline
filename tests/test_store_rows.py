"""RunStore 行数列测试（M8-3 Task 9.1）：nodes 表 ``rows_in``/``rows_out`` 落库与引擎透传。

运行：``python -m unittest discover -s tests -k test_store_rows -v``

覆盖：
- record_node 写入 rows_in/rows_out（JSON 文本），读回反序列化值一致；
- None / 空 dict 落 NULL（空 dict 对应报告"无键"语义）；
- 旧库（v0.8.0 schema、无 rows 列）打开后自动补列，旧行该列为 NULL，新行照常写 JSON；
- 迁移幂等：同一库连续开两个 RunStore 实例不报错、列不重复；
- 引擎集成：带 run_store 的小流水线 nodes 表行数与报告节点值一致；lineage=False 落 NULL；
- cached 命中节点行数同样落库。

遵循现有测试风格：仅文件顶部做路径准备，op_pipeline 的 import 放在测试方法内。
"""
from __future__ import annotations

import json
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from op_pipeline import (  # noqa: E402
    Operator,
    PipelineSpec,
    default_registry,
    run_pipeline,
)
from op_pipeline.registry import op as register_op  # noqa: E402

# v0.8.0 nodes 建表语句（有 duration、无 rows_in/rows_out），用于模拟历史库。
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
    duration    REAL DEFAULT 0,
    PRIMARY KEY (run_id, node_id)
);
"""


@register_op("m89.rows_double", replace=True)
class RowsDouble(Operator):
    """把 list 输入逐元素 ×2（list 进 list 出，验证行数透传落库）。"""

    description = "列表元素翻倍"
    inputs = [("rows", list)]
    outputs = [("rows", list)]

    def run(self, ctx, rows):
        return {"rows": [x * 2 for x in rows]}


def _spec(with_cache: bool = False):
    """小流水线：内存常量产出 list → list 算子翻倍（不落盘，专注落库验证）。"""
    return PipelineSpec.from_dict(
        {
            "name": "store_rows_demo",
            "outputs": {"result": "${dbl.rows}"},
            "nodes": [
                {"id": "src", "op": "control.const",
                 "params": {"value": [1, 2, 3, 4]}},
                {"id": "dbl", "op": "m89.rows_double",
                 "inputs": {"rows": "${src.value}"},
                 **({"cache": True} if with_cache else {})},
            ],
        }
    )


def _columns(conn: sqlite3.Connection, table: str) -> list[str]:
    return [row[1] for row in conn.execute(f"PRAGMA table_info({table})")]


class StoreRowsWriteTest(unittest.TestCase):
    """Task 9.1：record_node 行数列写入与读回。"""

    def test_record_node_writes_rows_json(self):
        """传 dict：库中为 JSON 文本，读回反序列化与写入值一致。"""
        from op_pipeline import RunStore

        rows_in = {"rows": 4}
        rows_out = {"evens": 2, "odds": 2}
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as d:
            db = Path(d) / "runs.db"
            with RunStore(db) as store:
                store.record_run("r1", "demo", "1.0", "success", 1.0, 2.0, {})
                store.record_node(
                    "r1", "a", "m89.rows_double", "success", 1, False, None, None, 1.5,
                    duration=0.5, rows_in=rows_in, rows_out=rows_out,
                )
                raw = store.query(
                    "SELECT rows_in, rows_out FROM nodes "
                    "WHERE run_id = ? AND node_id = ?",
                    ("r1", "a"),
                )
            self.assertEqual(len(raw), 1)
            rows_in_text, rows_out_text = raw[0]
            self.assertIsInstance(rows_in_text, str)
            self.assertIsInstance(rows_out_text, str)
            self.assertEqual(json.loads(rows_in_text), rows_in)
            self.assertEqual(json.loads(rows_out_text), rows_out)

    def test_record_node_none_rows_writes_null(self):
        """不传行数（既有位置调用兼容）：rows_in/rows_out 落 NULL。"""
        from op_pipeline import RunStore

        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as d:
            db = Path(d) / "runs.db"
            with RunStore(db) as store:
                store.record_run("r1", "demo", "1.0", "success", 1.0, 2.0, {})
                store.record_node(
                    "r1", "a", "control.const", "success", 1, False, None, "k", 1.5,
                )
                raw = store.query(
                    "SELECT rows_in, rows_out FROM nodes WHERE node_id = 'a'"
                )
            self.assertEqual(raw[0], (None, None))

    def test_record_node_empty_dict_writes_null(self):
        """空 dict 视同未采集：落 NULL（报告无键 ↔ DB NULL）。"""
        from op_pipeline import RunStore

        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as d:
            db = Path(d) / "runs.db"
            with RunStore(db) as store:
                store.record_run("r1", "demo", "1.0", "success", 1.0, 2.0, {})
                store.record_node(
                    "r1", "a", "control.const", "success", 1, False, None, None, 1.5,
                    rows_in={}, rows_out={},
                )
                raw = store.query("SELECT rows_in, rows_out FROM nodes")
            self.assertEqual(raw[0], (None, None))


class StoreRowsMigrationTest(unittest.TestCase):
    """Task 9.1：旧库补列迁移与幂等性。"""

    def test_old_db_gets_rows_columns_and_old_row_is_null(self):
        """v0.8.0 旧库打开后自动补列：旧行 rows 列为 NULL，新行照常写 JSON。"""
        from op_pipeline import RunStore

        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as d:
            db = Path(d) / "old.db"
            conn = sqlite3.connect(db)
            conn.executescript(_OLD_SCHEMA)
            conn.execute(
                "INSERT INTO runs VALUES (?, ?, ?, ?, ?, ?, ?)",
                ("old-run", "legacy", "0.8", "success", 10.0, 11.0, "{}"),
            )
            conn.execute(
                "INSERT INTO nodes VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                ("old-run", "n1", "control.const", "success", 1, 0,
                 None, None, 10.5, 0.5),
            )
            conn.commit()
            conn.close()

            with RunStore(db) as store:
                cols = [row[1] for row in store.query("PRAGMA table_info(nodes)")]
                old = store.query(
                    "SELECT rows_in, rows_out FROM nodes WHERE run_id = 'old-run'"
                )
                # 迁移后新写入照常带行数
                store.record_run("new-run", "demo", "0.9", "success", 20.0, 21.0, {})
                store.record_node(
                    "new-run", "n1", "m89.rows_double", "success", 1, False, None, None,
                    20.5, duration=0.5, rows_in={"rows": 4}, rows_out={"rows": 4},
                )
                new = store.query(
                    "SELECT rows_in, rows_out FROM nodes WHERE run_id = 'new-run'"
                )
            self.assertIn("rows_in", cols)
            self.assertIn("rows_out", cols)
            self.assertEqual(old[0], (None, None), "旧行 rows 列应为 NULL（展示为空）")
            self.assertEqual(json.loads(new[0][0]), {"rows": 4})
            self.assertEqual(json.loads(new[0][1]), {"rows": 4})

    def test_migration_idempotent_across_instances(self):
        """同一库连续开两个 RunStore：不报错、列不重复。"""
        from op_pipeline import RunStore

        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as d:
            db = Path(d) / "runs.db"
            with RunStore(db):
                pass
            with RunStore(db) as store:
                cols = [row[1] for row in store.query("PRAGMA table_info(nodes)")]
            self.assertEqual(cols.count("rows_in"), 1)
            self.assertEqual(cols.count("rows_out"), 1)
            self.assertEqual(cols.count("duration"), 1)


class EngineRowsStoreTest(unittest.TestCase):
    """Task 9.1：引擎 → RunStore 行数透传集成。"""

    def test_pipeline_rows_persisted_match_report(self):
        """带 run_store 跑小流水线：nodes 表 rows_in/rows_out 与报告节点值一致。"""
        from op_pipeline import RunStore

        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as d:
            db = Path(d) / "runs.db"
            with RunStore(db) as store:
                report, values = run_pipeline(
                    _spec(), registry=default_registry,
                    cache_dir=None, run_store=store,
                )
                raw = {
                    row[0]: (row[1], row[2])
                    for row in store.query(
                        "SELECT node_id, rows_in, rows_out FROM nodes "
                        "WHERE run_id = ?",
                        (report.run_id,),
                    )
                }
            self.assertEqual(report.status, "success")
            self.assertEqual(values["dbl.rows"], [2, 4, 6, 8])
            # dbl 节点：DB JSON 反序列化与报告一致
            self.assertEqual(json.loads(raw["dbl"][0]), report.node("dbl").rows_in)
            self.assertEqual(json.loads(raw["dbl"][1]), report.node("dbl").rows_out)
            self.assertEqual(report.node("dbl").rows_in, {"rows": 4})
            self.assertEqual(report.node("dbl").rows_out, {"rows": 4})
            # src 节点（const 实际产出 list）：rows_out 有 JSON、rows_in 无输入 → NULL
            self.assertEqual(report.node("src").rows_in, {})
            self.assertIsNone(raw["src"][0])
            self.assertEqual(json.loads(raw["src"][1]),
                             report.node("src").rows_out)
            self.assertEqual(json.loads(raw["src"][1]), {"value": 4})

    def test_lineage_false_persists_null(self):
        """lineage=False：nodes 表 rows 列落 NULL（与报告无键对应）。"""
        from op_pipeline import RunStore

        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as d:
            db = Path(d) / "runs.db"
            with RunStore(db) as store:
                report, _ = run_pipeline(
                    _spec(), registry=default_registry,
                    cache_dir=None, run_store=store, lineage=False,
                )
                raw = store.query(
                    "SELECT rows_in, rows_out FROM nodes WHERE run_id = ?",
                    (report.run_id,),
                )
            self.assertEqual(len(raw), 2)
            self.assertTrue(
                all(r_in is None and r_out is None for r_in, r_out in raw)
            )

    def test_cached_node_rows_persisted(self):
        """cached 命中节点：行数同样落库（缓存命中后反序列化的 list 输出记录行数）。"""
        from op_pipeline import RunStore

        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as d:
            spec = _spec(with_cache=True)
            cache_dir = Path(d) / "cache"
            with RunStore(Path(d) / "runs.db") as store:
                run_pipeline(spec, registry=default_registry,
                             cache_dir=cache_dir, run_store=store)
                second, _ = run_pipeline(
                    spec, registry=default_registry,
                    cache_dir=cache_dir, run_store=store,
                )
                self.assertTrue(second.node("dbl").cached)
                raw = store.query(
                    "SELECT rows_in, rows_out FROM nodes "
                    "WHERE run_id = ? AND node_id = 'dbl'",
                    (second.run_id,),
                )
            self.assertEqual(json.loads(raw[0][0]), {"rows": 4})
            self.assertEqual(json.loads(raw[0][1]), {"rows": 4})


if __name__ == "__main__":
    unittest.main()
