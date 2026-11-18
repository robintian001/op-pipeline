"""DB 算子：读写 SQLite（标准库 sqlite3，零第三方依赖）。

每个节点每次运行各自打开/关闭一个连接；``db=":memory:"`` 仅适用于单节点临时查询，
跨节点的建表-写-读闭环请使用文件库（各节点共享同一文件即共享状态）。
引擎按依赖边调度，写后读的节点必须显式接线（可选输入端口 ``gate``）保证顺序，
否则多个 db 节点会并行执行而互相踩踏。
"""
from __future__ import annotations

import sqlite3

from ..operator import Operator
from ..params import Param
from ..registry import op

_MEMORY = ":memory:"


@op("db.sqlite_exec", aliases=["sqlite_exec"])
class SqliteExec(Operator):
    """执行 SQL 写语句（INSERT/UPDATE/DELETE/DDL），返回受影响行数与最后插入行号。"""

    description = "执行 SQLite 写语句"
    inputs = [("gate", object, True)]
    outputs = [("rowcount", int), ("lastrowid", int, True)]
    params = {
        "db": Param(str, doc="SQLite 库路径，相对工作目录；':memory:' 为单节点内存库"),
        "query": Param(str, doc="要执行的 SQL 语句"),
        "params": Param(list, default=None, doc="位置参数，按 ? 占位符绑定"),
    }
    deterministic = False  # 外部状态：写操作改变库内容，不可缓存

    def run(self, ctx, gate, db, query, params):
        target = _MEMORY if db == _MEMORY else ctx.resolve_path(db)
        conn = sqlite3.connect(str(target))
        try:
            cur = conn.execute(query, list(params or []))
            conn.commit()
        finally:
            conn.close()
        ctx.log(f"sqlite_exec 影响 {cur.rowcount} 行 <- {target}")
        return {"rowcount": cur.rowcount, "lastrowid": cur.lastrowid}


@op("db.sqlite_query", aliases=["sqlite_query"])
class SqliteQuery(Operator):
    """执行 SELECT 查询，返回行字典表（list[dict]）与列名。"""

    description = "查询 SQLite 库"
    inputs = [("gate", object, True)]
    outputs = [("rows", list), ("count", int), ("columns", list)]
    params = {
        "db": Param(str, doc="SQLite 库路径，相对工作目录；':memory:' 为单节点内存库"),
        "query": Param(str, doc="SELECT 语句"),
        "params": Param(list, default=None, doc="位置参数，按 ? 占位符绑定"),
    }
    deterministic = False  # 外部状态：查询结果依赖库当前内容，不可缓存

    def run(self, ctx, gate, db, query, params):
        target = _MEMORY if db == _MEMORY else ctx.resolve_path(db)
        conn = sqlite3.connect(str(target))
        try:
            cur = conn.execute(query, list(params or []))
            columns = [c[0] for c in cur.description] if cur.description else []
            rows = [dict(zip(columns, row)) for row in cur.fetchall()]
        finally:
            conn.close()
        ctx.log(f"sqlite_query 返回 {len(rows)} 行 <- {target}")
        return {"rows": rows, "count": len(rows), "columns": columns}
