"""运行记录存储：把每次运行与节点状态落到 SQLite，供查询与断点续跑（M2-6）。

不传 ``--runs``（即不构造 :class:`RunStore`）时引擎完全不触碰 SQLite——
本模块是"可选的运行元数据持久化"，默认关闭、零副作用。
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

_SCHEMA = """
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
        self._tables_ready = True

    def record_run(
        self,
        run_id: str,
        pipeline: str,
        version: str,
        status: str,
        started_at: float,
        finished_at: float,
        params: dict[str, Any],
    ) -> None:
        self._ensure_tables()
        self.conn.execute(
            "INSERT OR REPLACE INTO runs "
            "(run_id, pipeline, version, status, started_at, finished_at, params_json) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (run_id, pipeline, version, status, started_at, finished_at,
             json.dumps(params, ensure_ascii=False, default=str)),
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
    ) -> None:
        self._ensure_tables()
        self.conn.execute(
            "INSERT OR REPLACE INTO nodes "
            "(run_id, node_id, op, status, attempts, cached, error, cache_key, finished_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (run_id, node_id, op, status, attempts, int(cached), error, cache_key, finished_at),
        )
        self.conn.commit()

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
