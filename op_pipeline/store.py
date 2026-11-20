"""运行记录存储：把每次运行与节点状态落到 SQLite，供查询与断点续跑（M2-6）。

不传 ``--runs``（即不构造 :class:`RunStore`）时引擎完全不触碰 SQLite——
本模块是"可选的运行元数据持久化"，默认关闭、零副作用。
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any, Mapping

_SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    run_id      TEXT PRIMARY KEY,
    pipeline    TEXT NOT NULL,
    version     TEXT NOT NULL,
    status      TEXT NOT NULL,
    started_at  REAL NOT NULL,
    finished_at REAL NOT NULL,
    params_json TEXT NOT NULL,
    report_json TEXT
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


class RunStore:
    """SQLite 运行记录库：``record_run / record_node / query``。

    - 表在首次写入时惰性创建；
    - 连接由调用方负责关闭（``close`` 或 ``with``）；
    - ``params`` 以 JSON 文本存储，``query`` 返回原始行。
    """

    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.db_path)
        self._tables_ready = False

    # ------------------------------------------------------------------ #
    def _ensure_tables(self) -> None:
        if self._tables_ready:
            return
        self.conn.executescript(_SCHEMA)
        self._migrate_nodes_duration()
        self._migrate_nodes_rows()
        self._migrate_runs_report_json()
        self._tables_ready = True

    def _migrate_nodes_duration(self) -> None:
        """M6：旧库 nodes 表补 ``duration`` 列（幂等；旧行取默认 0）。"""
        columns = [row[1] for row in self.conn.execute("PRAGMA table_info(nodes)")]
        if "duration" not in columns:
            self.conn.execute("ALTER TABLE nodes ADD COLUMN duration REAL DEFAULT 0")
            self.conn.commit()

    def _migrate_nodes_rows(self) -> None:
        """M8 Task 9.1：旧库 nodes 表补 ``rows_in``/``rows_out`` 列（幂等；旧行取 NULL）。"""
        columns = [row[1] for row in self.conn.execute("PRAGMA table_info(nodes)")]
        added = False
        if "rows_in" not in columns:
            self.conn.execute("ALTER TABLE nodes ADD COLUMN rows_in TEXT")
            added = True
        if "rows_out" not in columns:
            self.conn.execute("ALTER TABLE nodes ADD COLUMN rows_out TEXT")
            added = True
        if added:
            self.conn.commit()

    def _migrate_runs_report_json(self) -> None:
        """M9 Task 11：旧库 runs 表补 ``report_json`` 列（幂等；旧行取 NULL）。

        仿 rows_in 先例：新库由 ``_SCHEMA`` 直接建出该列，旧库（v0.9.0 前）
        打开时 ALTER TABLE 追加；已存在则跳过，重复初始化不报错。
        """
        columns = [row[1] for row in self.conn.execute("PRAGMA table_info(runs)")]
        if "report_json" not in columns:
            self.conn.execute("ALTER TABLE runs ADD COLUMN report_json TEXT")
            self.conn.commit()

    def record_run(
        self,
        run_id: str,
        pipeline: str,
        version: str,
        status: str,
        started_at: float,
        finished_at: float,
        params: dict[str, Any],
        report_json: str | None = None,
    ) -> None:
        """写入运行记录；``report_json``（M9 Task 11）为可选末尾参数（兼容既有位置调用）。

        ``report_json`` 为引擎序列化好的全量报告 JSON 文本：None 落 NULL，
        非 None 原样写入；旧库旧行该列读回为 None（读取时 None 安全）。
        """
        self._ensure_tables()
        self.conn.execute(
            "INSERT OR REPLACE INTO runs "
            "(run_id, pipeline, version, status, started_at, finished_at, "
            "params_json, report_json) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (run_id, pipeline, version, status, started_at, finished_at,
             json.dumps(params, ensure_ascii=False, default=str), report_json),
        )
        self.conn.commit()

    def record_node(
        self,
        run_id: str,
        node_id: str,
        op: str,
        status: str,
        attempts: int,
        cached: bool,
        error: str | None,
        cache_key: str | None,
        finished_at: float,
        duration: float = 0.0,
        rows_in: Mapping[str, int] | None = None,
        rows_out: Mapping[str, int] | None = None,
    ) -> None:
        """写入单节点结果；``duration``/``rows_in``/``rows_out`` 为可选末尾参数（兼容既有位置调用）。

        ``rows_in``/``rows_out`` 为行数血缘（M8 Task 9.1）：None 或空 dict 写 NULL，
        非空 dict 以 JSON 文本落库——与报告"无键 ↔ NULL"语义对应。
        """
        self._ensure_tables()
        rows_in_json = (
            json.dumps(rows_in, ensure_ascii=False, sort_keys=True) if rows_in else None
        )
        rows_out_json = (
            json.dumps(rows_out, ensure_ascii=False, sort_keys=True)
            if rows_out
            else None
        )
        self.conn.execute(
            "INSERT OR REPLACE INTO nodes "
            "(run_id, node_id, op, status, attempts, cached, error, cache_key, "
            "finished_at, duration, rows_in, rows_out) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (run_id, node_id, op, status, attempts, int(cached), error, cache_key,
             finished_at, duration, rows_in_json, rows_out_json),
        )
        self.conn.commit()

    def prune(self, keep_last: int) -> int:
        """M6：仅保留最近 ``keep_last`` 条运行记录，返回删除的运行数。

        排序为 ``started_at DESC, run_id DESC``（同一时间戳时用 run_id 兜底，
        保证结果确定）；对应节点记录级联删除。``keep_last <= 0`` 抛 ValueError。
        """
        if keep_last <= 0:
            raise ValueError(f"keep_last 必须是正数，实际是 {keep_last!r}")
        self._ensure_tables()
        cursor = self.conn.execute(
            "DELETE FROM runs WHERE run_id NOT IN ("
            " SELECT run_id FROM runs ORDER BY started_at DESC, run_id DESC LIMIT ?"
            ")",
            (keep_last,),
        )
        deleted = max(0, cursor.rowcount)
        # nodes 可能只存在于外部手工库（无 nodes 表），此时跳过级联删除
        has_nodes = self.conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'nodes'"
        ).fetchone()
        if has_nodes is not None:
            self.conn.execute(
                "DELETE FROM nodes WHERE run_id NOT IN (SELECT run_id FROM runs)"
            )
        self.conn.commit()
        return deleted

    def query(self, sql: str, params: tuple[Any, ...] | None = None) -> list[tuple[Any, ...]]:
        self._ensure_tables()
        return list(self.conn.execute(sql, params or ()).fetchall())

    def prev_node(self, run_id: str, node_id: str) -> tuple[str, str | None] | None:
        """M2-6：取参考 run 里某节点的 (status, cache_key)；无记录返回 None。"""
        rows = self.query(
            "SELECT status, cache_key FROM nodes WHERE run_id = ? AND node_id = ?",
            (run_id, node_id),
        )
        if not rows:
            return None
        status, cache_key = rows[0]
        return status, cache_key

    def close(self) -> None:
        self.conn.close()

    def __enter__(self) -> "RunStore":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()
