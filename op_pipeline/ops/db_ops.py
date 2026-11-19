"""DB 算子：SQLite（标准库 sqlite3，零第三方依赖）+ MySQL / PostgreSQL（可选 extras）。

MySQL / PostgreSQL 是可选依赖（``pip install op-pipeline[mysql]`` / ``[postgres]``），
只在 ``run()`` 内延迟导入：未安装时本模块照常导入、算子照常注册，调用对应算子才报清晰错误。

每个节点每次运行各自打开/关闭一个连接；``db=":memory:"`` 仅适用于单节点临时查询，
跨节点的建表-写-读闭环请使用文件库（各节点共享同一文件即共享状态）。
引擎按依赖边调度，写后读的节点必须显式接线（可选输入端口 ``gate``）保证顺序，
否则多个 db 节点会并行执行而互相踩踏。
"""
from __future__ import annotations

import sqlite3
from typing import Any
from urllib.parse import parse_qs, urlparse

from ..errors import ParamValidationError
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


def _pymysql():
    try:
        import pymysql
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            "mysql 算子需要 pymysql，请安装：pip install op-pipeline[mysql]"
        ) from exc
    return pymysql


def _psycopg2():
    try:
        import psycopg2
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            "postgres 算子需要 psycopg2，请安装：pip install op-pipeline[postgres]"
        ) from exc
    return psycopg2


def _parse_dsn(dsn: str) -> tuple[dict[str, Any], str, str]:
    """解析 ``scheme://user:pass@host:port/db`` 形式 DSN。

    返回三元组 (连接关键字, 库名, charset)：charset 为空串表示 DSN 未指定编码；
    pymysql 的 connect 不接受 dsn 关键字，需拆成关键字参数再连。
    """
    parts = urlparse(dsn)
    if not parts.scheme or not parts.hostname:
        raise ValueError(f"DSN 格式非法（期望 scheme://user:pass@host:port/db）：{dsn!r}")
    kwargs: dict[str, Any] = {
        "user": parts.username or "",
        "password": parts.password or "",
        "host": parts.hostname,
    }
    if parts.port:
        kwargs["port"] = parts.port  # 未写端口时不传，交给驱动默认（mysql 3306 / postgres 5432）
    database = parts.path.lstrip("/")
    charset = parse_qs(parts.query).get("charset", [""])[0]
    return kwargs, database, charset


@op("db.read_mysql", aliases=["read_mysql"])
class ReadMysql(Operator):
    """执行 SELECT 查询，返回行字典表（list[dict]）与列名。

    ``query`` 与 ``table`` 至少提供其一：给 ``query`` 直接执行原始 SQL；
    给 ``table`` 自动拼 ``SELECT * FROM <table>``，可用 ``where``/``limit`` 附加过滤。
    """

    description = "查询 MySQL 库"
    inputs = [("gate", object, True)]
    outputs = [("rows", list), ("count", int), ("columns", list)]
    params = {
        "dsn": Param(str, doc="MySQL DSN，形如 mysql://user:pass@host:port/db?charset=utf8mb4"),
        "query": Param(str, default=None, doc="原始 SELECT 语句；与 table 至少提供其一"),
        "table": Param(str, default=None, doc="目标表名；与 query 至少提供其一"),
        "where": Param(str, default=None, doc="table 模式下附加的 WHERE 条件（原始 SQL 文本）"),
        "limit": Param(int, default=None, doc="table 模式下附加的 LIMIT 行数"),
    }
    deterministic = False  # 外部状态：查询结果依赖库当前内容，不可缓存

    def run(self, ctx, gate, dsn, query, table, where, limit):
        if query is None and table is None:
            raise ParamValidationError("db.read_mysql 需要提供 query 或 table 至少一个")
        pymysql = _pymysql()
        kwargs, database, charset = _parse_dsn(dsn)
        if charset:
            kwargs["charset"] = charset
        conn = pymysql.connect(**kwargs, database=database)
        try:
            cur = conn.cursor()
            if query:
                sql = query
            else:
                sql = f"SELECT * FROM {table}"
                if where:
                    sql += f" WHERE {where}"
                if limit is not None:
                    sql += f" LIMIT {int(limit)}"
            cur.execute(sql)
            columns = [c[0] for c in cur.description] if cur.description else []
            rows = [dict(zip(columns, row)) for row in cur.fetchall()]
        finally:
            conn.close()
        ctx.log(f"read_mysql 返回 {len(rows)} 行 <- {dsn}")
        return {"rows": rows, "count": len(rows), "columns": columns}


@op("db.write_mysql", aliases=["write_mysql"])
class WriteMysql(Operator):
    """把行字典表（list[dict]）批量 INSERT 进 MySQL 表。"""

    description = "行字典表写入 MySQL 表"
    inputs = [("rows", list, True), ("gate", object, True)]
    outputs = [("rowcount", int), ("lastrowid", int, True)]
    params = {
        "dsn": Param(str, doc="MySQL DSN，形如 mysql://user:pass@host:port/db?charset=utf8mb4"),
        "table": Param(str, doc="目标表名"),
    }
    deterministic = False  # 外部状态：写操作改变库内容，不可缓存

    def run(self, ctx, rows, gate, dsn, table):
        pymysql = _pymysql()
        kwargs, database, charset = _parse_dsn(dsn)
        if charset:
            kwargs["charset"] = charset
        rows = list(rows or [])
        if not rows:
            raise ValueError("db.write_mysql 行表为空，无法推断列名")
        columns = list(rows[0].keys())
        names = ", ".join(columns)
        placeholders = ", ".join(["%s"] * len(columns))
        conn = pymysql.connect(**kwargs, database=database)
        try:
            cur = conn.cursor()
            cur.executemany(
                f"INSERT INTO {table} ({names}) VALUES ({placeholders})",
                [tuple(row.get(k) for k in columns) for row in rows],
            )
            conn.commit()
        finally:
            conn.close()
        ctx.log(f"write_mysql 影响 {cur.rowcount} 行 -> {table}")
        return {"rowcount": cur.rowcount, "lastrowid": cur.lastrowid}


@op("db.read_postgres", aliases=["read_postgres"])
class ReadPostgres(Operator):
    """执行 SELECT 查询，返回行字典表（list[dict]）与列名。

    ``query`` 与 ``table`` 至少提供其一：给 ``query`` 直接执行原始 SQL；
    给 ``table`` 自动拼 ``SELECT * FROM <table>``，可用 ``where``/``limit`` 附加过滤。
    """

    description = "查询 PostgreSQL 库"
    inputs = [("gate", object, True)]
    outputs = [("rows", list), ("count", int), ("columns", list)]
    params = {
        "dsn": Param(str, doc="PostgreSQL DSN，形如 postgresql://user:pass@host:port/db"),
        "query": Param(str, default=None, doc="原始 SELECT 语句；与 table 至少提供其一"),
        "table": Param(str, default=None, doc="目标表名；与 query 至少提供其一"),
        "where": Param(str, default=None, doc="table 模式下附加的 WHERE 条件（原始 SQL 文本）"),
        "limit": Param(int, default=None, doc="table 模式下附加的 LIMIT 行数"),
    }
    deterministic = False  # 外部状态：查询结果依赖库当前内容，不可缓存

    def run(self, ctx, gate, dsn, query, table, where, limit):
        if query is None and table is None:
            raise ParamValidationError("db.read_postgres 需要提供 query 或 table 至少一个")
        psycopg2 = _psycopg2()
        kwargs, database, _ = _parse_dsn(dsn)
        conn = psycopg2.connect(**kwargs, dbname=database)
        try:
            cur = conn.cursor()
            if query:
                sql = query
            else:
                sql = f"SELECT * FROM {table}"
                if where:
                    sql += f" WHERE {where}"
                if limit is not None:
                    sql += f" LIMIT {int(limit)}"
            cur.execute(sql)
            columns = [d[0] for d in cur.description] if cur.description else []
            rows = [dict(zip(columns, row)) for row in cur.fetchall()]
        finally:
            conn.close()
        ctx.log(f"read_postgres 返回 {len(rows)} 行 <- {dsn}")
        return {"rows": rows, "count": len(rows), "columns": columns}


@op("db.write_postgres", aliases=["write_postgres"])
class WritePostgres(Operator):
    """把行字典表（list[dict]）批量 INSERT 进 PostgreSQL 表。"""

    description = "行字典表写入 PostgreSQL 表"
    inputs = [("rows", list, True), ("gate", object, True)]
    outputs = [("rowcount", int), ("lastrowid", int, True)]
    params = {
        "dsn": Param(str, doc="PostgreSQL DSN，形如 postgresql://user:pass@host:port/db"),
        "table": Param(str, doc="目标表名"),
    }
    deterministic = False  # 外部状态：写操作改变库内容，不可缓存

    def run(self, ctx, rows, gate, dsn, table):
        psycopg2 = _psycopg2()
        kwargs, database, _ = _parse_dsn(dsn)
        rows = list(rows or [])
        if not rows:
            raise ValueError("db.write_postgres 行表为空，无法推断列名")
        columns = list(rows[0].keys())
        names = ", ".join(columns)
        placeholders = ", ".join(["%s"] * len(columns))
        conn = psycopg2.connect(**kwargs, dbname=database)
        try:
            cur = conn.cursor()
            cur.executemany(
                f"INSERT INTO {table} ({names}) VALUES ({placeholders})",
                [tuple(row.get(k) for k in columns) for row in rows],
            )
            conn.commit()
        finally:
            conn.close()
        ctx.log(f"write_postgres 影响 {cur.rowcount} 行 -> {table}")
        return {"rowcount": cur.rowcount, "lastrowid": cur.lastrowid}
