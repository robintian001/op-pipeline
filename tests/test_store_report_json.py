"""RunStore 全量报告 JSON 测试（M9 Task 11）：runs 表 ``report_json`` 落库与引擎透传。

运行：``python -m unittest discover -s tests -k StoreReportJson -v``

覆盖：
- record_run 写入 report_json（原样文本），读回一致；默认不传落 NULL；
- 旧库（v0.9.0 schema、无 report_json 列）打开后自动补列，旧行该列为 NULL，
  新行照常写 JSON；
- 迁移幂等：同一库连续开两个 RunStore 实例不报错、列不重复；
- 引擎集成：带 run_store 的小流水线（value_preview 开启）落库的 report_json
  与 ``report.to_dict()`` 完全一致（同一 dict），含 preview_in/preview_out 新字段
  与 params/warnings/nodes 完整性，且序列化文本与 ``RunReport.to_json`` 一致。

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
    PipelineSpec,
    default_registry,
    run_pipeline,
)

# v0.9.0 runs 建表语句（有 params_json、无 report_json），用于模拟历史库。
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
    rows_in     TEXT,
    rows_out    TEXT,
    PRIMARY KEY (run_id, node_id)
);
"""


def _spec():
    """小流水线：const 产出 list → table.limit 截断（不落盘，专注落库验证）。

    params 带嵌套结构验证完整性；``lim`` 的 n 引用流水线参数 ``${params.n}``，
    warnings 由故意抛错的钩子注入（见 :class:`_BoomHook`）。
    """
    return PipelineSpec.from_dict(
        {
            "name": "store_report_json_demo",
            "params": {"n": 5, "nested": {"k": [1, 2]}},
            "outputs": {"result": "${lim.table}"},
            "nodes": [
                {"id": "src", "op": "control.const",
                 "params": {"value": list(range(10))}},
                {"id": "lim", "op": "table.limit",
                 "inputs": {"table": "${src.value}"},
                 "params": {"n": "${params.n}"}},
            ],
        }
    )


class _BoomHook:
    """M2-7：故意抛异常的钩子——引擎捕获并记为告警，验证 warnings 完整性。"""

    def on_node_start(self, *args):  # noqa: ARG002
        raise RuntimeError("钩子主动抛错（测试用）")


class StoreReportJsonWriteTest(unittest.TestCase):
    """Task 11.1：record_run 的 report_json 写入与读回。"""

    def test_record_run_writes_report_json(self):
        """传 report_json 文本：库中原样存储，读回字符串一致。"""
        from op_pipeline import RunStore

        text = json.dumps({"run_id": "r1", "status": "success"})
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as d:
            db = Path(d) / "runs.db"
            with RunStore(db) as store:
                store.record_run("r1", "demo", "1.0", "success", 1.0, 2.0, {},
                                 report_json=text)
                raw = store.query(
                    "SELECT report_json FROM runs WHERE run_id = 'r1'"
                )
            self.assertEqual(raw[0], (text,))

    def test_record_run_without_report_json_writes_null(self):
        """不传 report_json（既有位置调用兼容）：该列落 NULL。"""
        from op_pipeline import RunStore

        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as d:
            db = Path(d) / "runs.db"
            with RunStore(db) as store:
                store.record_run("r1", "demo", "1.0", "success", 1.0, 2.0, {})
                raw = store.query(
                    "SELECT report_json FROM runs WHERE run_id = 'r1'"
                )
            self.assertEqual(raw[0], (None,))


class StoreReportJsonMigrationTest(unittest.TestCase):
    """Task 11.1：旧库补列迁移与幂等性。"""

    def test_old_db_gets_column_and_old_row_is_null(self):
        """v0.9.0 旧库打开后自动补列：旧行 report_json 为 NULL，新行照常写。"""
        from op_pipeline import RunStore

        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as d:
            db = Path(d) / "old.db"
            conn = sqlite3.connect(db)
            conn.executescript(_OLD_SCHEMA)
            conn.execute(
                "INSERT INTO runs VALUES (?, ?, ?, ?, ?, ?, ?)",
                ("old-run", "legacy", "0.9", "success", 10.0, 11.0, "{}"),
            )
            conn.commit()
            conn.close()

            with RunStore(db) as store:
                cols = [row[1] for row in store.query("PRAGMA table_info(runs)")]
                old = store.query(
                    "SELECT report_json FROM runs WHERE run_id = 'old-run'"
                )
                # 迁移后新写入照常带 report_json
                store.record_run("new-run", "demo", "0.9.1", "success", 20.0, 21.0,
                                 {}, report_json=json.dumps({"run_id": "new-run"}))
                new = store.query(
                    "SELECT report_json FROM runs WHERE run_id = 'new-run'"
                )
            self.assertIn("report_json", cols)
            self.assertEqual(old[0], (None,), "旧行 report_json 应为 NULL（读取 None 安全）")
            self.assertEqual(json.loads(new[0][0]), {"run_id": "new-run"})

    def test_migration_idempotent_across_instances(self):
        """同一库连续开两个 RunStore：不报错、report_json 列不重复。"""
        from op_pipeline import RunStore

        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as d:
            db = Path(d) / "runs.db"
            with RunStore(db):
                pass
            with RunStore(db) as store:
                cols = [row[1] for row in store.query("PRAGMA table_info(runs)")]
            self.assertEqual(cols.count("report_json"), 1)


class EngineStoreReportJsonTest(unittest.TestCase):
    """Task 11.2：引擎 → RunStore 全量报告 JSON 透传集成。"""

    def test_report_json_equals_run_report_to_dict(self):
        """带 run_store 跑小流水线（value_preview 开启）：落库 JSON 与 ``report.to_dict()``
        同一 dict，含 preview_in/preview_out 新字段与 params/warnings/nodes 完整性，
        序列化文本与 ``RunReport.to_json`` 输出一致。"""
        from op_pipeline import RunStore

        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as d:
            db = Path(d) / "runs.db"
            with RunStore(db) as store:
                report, _ = run_pipeline(
                    _spec(), registry=default_registry,
                    cache_dir=None, run_store=store, value_preview=2,
                    hooks=[_BoomHook()],
                )
                raw = store.query(
                    "SELECT report_json FROM runs WHERE run_id = ?",
                    (report.run_id,),
                )
            db_text = raw[0][0]
            stored = json.loads(db_text)
            # 与报告同一 dict
            self.assertEqual(stored, report.to_dict())
            # 序列化规则与 RunReport.to_json 完全一致（同一文本）
            with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as d2:
                out = report.to_json(Path(d2) / "report.json")
                self.assertEqual(db_text, out.read_text(encoding="utf-8"))
            # preview 新字段完整入库
            self.assertEqual(stored["nodes"][0]["preview_out"]["value"], [0, 1])
            self.assertEqual(stored["nodes"][1]["preview_in"]["table"], [0, 1])
            self.assertEqual(stored["nodes"][1]["preview_out"]["table"], [0, 1])
            # params/warnings/nodes 完整性
            self.assertEqual(stored["params"], {"n": 5, "nested": {"k": [1, 2]}})
            self.assertEqual(stored["warnings"], report.warnings)
            self.assertTrue(any("钩子主动抛错" in w for w in stored["warnings"]))
            self.assertEqual([n["id"] for n in stored["nodes"]], ["src", "lim"])
            self.assertEqual(len(stored["nodes"]), len(report.nodes))

    def test_report_json_written_without_value_preview(self):
        """不开启 value_preview：report_json 照常落库且与 to_dict 一致（无 preview 键）。"""
        from op_pipeline import RunStore

        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as d:
            db = Path(d) / "runs.db"
            with RunStore(db) as store:
                report, _ = run_pipeline(
                    _spec(), registry=default_registry,
                    cache_dir=None, run_store=store,
                )
                raw = store.query(
                    "SELECT report_json FROM runs WHERE run_id = ?",
                    (report.run_id,),
                )
            stored = json.loads(raw[0][0])
            self.assertEqual(stored, report.to_dict())
            self.assertNotIn("preview_in", stored["nodes"][0])
            self.assertNotIn("preview_out", stored["nodes"][0])


if __name__ == "__main__":
    unittest.main()
