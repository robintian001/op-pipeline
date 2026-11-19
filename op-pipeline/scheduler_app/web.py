"""轻量 Web UI（M5-2 Task 6 / M6-2 Task 7）：``http.server`` 读 RunStore SQLite 渲染 HTML。

- 列表页 ``/``：run_id / status / 时间 / 节点数（HTML 表格，全部动态文本转义防注入）；
  M6 支持 ``?page=N&size=M&status=S`` 分页与状态筛选（SQL LIMIT/OFFSET 参数化）；
- 详情页 ``/run/<run_id>``：该次运行各节点状态 / 完成时间 / **耗时** / 错误摘要；
- :func:`create_server(db_path, port=0)`` 返回 server（port=0 自动分配）便于测试；
- 仅使用标准库：http.server / sqlite3 / html / time / urllib.parse。

注意：M6 起 nodes 表持久化单节点 ``duration``（旧库自动补列）；若外部库没有该列，
详情页回退显示 ``0.0s`` 而不报错。错误摘要来自节点 error 字段，截断展示。
"""
from __future__ import annotations

import html
import logging
import sqlite3
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlencode, urlparse

log = logging.getLogger("scheduler_app.web")

_ERROR_CLIP = 300  # 错误摘要截断长度（字符）
_PAGE_SIZE_DEFAULT = 50  # 列表页默认每页条数
_PAGE_SIZE_MAX = 200  # 每页条数上限
_ALLOWED_STATUSES = ("success", "failed", "partial", "skipped")


def create_server(db_path: str | Path, port: int = 0) -> ThreadingHTTPServer:
    """创建 Web 服务器：读 ``db_path`` 处的 RunStore SQLite 渲染页面。

    ``port=0`` 时由系统自动分配空闲端口（测试用），实际端口在
    ``server.server_address[1]``。返回已绑定但未启动的 server，
    调用方负责 ``serve_forever()`` / ``shutdown()`` / ``server_close()``。
    """
    handler = _make_handler(db_path)
    return ThreadingHTTPServer(("127.0.0.1", port), handler)


# --------------------------------------------------------------------------- #
# 数据访问（每次请求独立连接：ThreadingHTTPServer 每个请求一个线程，
# sqlite3 连接有线程亲和性，不能跨线程共享）
# --------------------------------------------------------------------------- #
def _connect(db_path: str | Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path), timeout=5)
    conn.row_factory = sqlite3.Row
    return conn


def _has_table(conn: sqlite3.Connection, name: str) -> bool:
    """判断表是否存在（runs/nodes 为惰性建表，外部写入时可能只有其一）。"""
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", (name,)
    ).fetchone()
    return row is not None


def _query_runs(
    conn: sqlite3.Connection,
    *,
    status: str | None = None,
    limit: int | None = None,
    offset: int = 0,
) -> list[sqlite3.Row]:
    """分页查询运行记录：筛选与分页全部下推到 SQL（避免全量加载）。"""
    sql = (
        "SELECT run_id, pipeline, version, status, started_at, finished_at FROM runs"
    )
    params: list[object] = []
    if status:
        sql += " WHERE status = ?"
        params.append(status)
    sql += " ORDER BY started_at DESC, run_id DESC"
    if limit is not None:
        sql += " LIMIT ? OFFSET ?"
        params.extend((limit, offset))
    return list(conn.execute(sql, params))


def _count_runs(conn: sqlite3.Connection, status: str | None = None) -> int:
    if status:
        row = conn.execute(
            "SELECT COUNT(*) FROM runs WHERE status = ?", (status,)
        ).fetchone()
    else:
        row = conn.execute("SELECT COUNT(*) FROM runs").fetchone()
    return int(row[0]) if row else 0


def _node_counts(
    conn: sqlite3.Connection, run_ids: list[str]
) -> dict[str, int]:
    """只统计当前页涉及节点的数量（IN 参数化，避免全表 GROUP BY）。"""
    if not run_ids or not _has_table(conn, "nodes"):
        return {}
    placeholders = ",".join("?" for _ in run_ids)
    return {
        row["run_id"]: row["n"]
        for row in conn.execute(
            f"SELECT run_id, COUNT(*) AS n FROM nodes "
            f"WHERE run_id IN ({placeholders}) GROUP BY run_id",
            run_ids,
        )
    }


def _query_run(conn: sqlite3.Connection, run_id: str) -> sqlite3.Row | None:
    rows = list(conn.execute(
        "SELECT run_id, pipeline, version, status, started_at, finished_at "
        "FROM runs WHERE run_id = ?",
        (run_id,),
    ))
    return rows[0] if rows else None


def _query_nodes(conn: sqlite3.Connection, run_id: str) -> list[sqlite3.Row]:
    if not _has_table(conn, "nodes"):
        return []
    columns = [row[1] for row in conn.execute("PRAGMA table_info(nodes)")]
    # 旧库（未走 RunStore 初始化）可能没有 duration 列：回退常量 0.0，页面不报错
    duration_expr = "duration" if "duration" in columns else "0.0 AS duration"
    return list(conn.execute(
        f"SELECT node_id, op, status, attempts, cached, error, finished_at, "
        f"{duration_expr} FROM nodes WHERE run_id = ? ORDER BY finished_at",
        (run_id,),
    ))


def _parse_list_query(
    query: dict[str, list[str]],
) -> tuple[int, int, str | None]:
    """解析列表页查询参数：非法值回落默认，page<1→1，size 限 1..200。"""
    page = _parse_int(query.get("page", [""])[0], default=1)
    size = _parse_int(query.get("size", [""])[0], default=_PAGE_SIZE_DEFAULT)
    page = max(1, page)
    size = min(_PAGE_SIZE_MAX, max(1, size))
    raw_status = query.get("status", [""])[0]
    status = raw_status if raw_status in _ALLOWED_STATUSES else None
    return page, size, status


def _parse_int(raw: str, default: int) -> int:
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


def _page_href(page: int, size: int, status: str | None) -> str:
    params: dict[str, str] = {"page": str(page), "size": str(size)}
    if status:
        params["status"] = status
    return "/?" + urlencode(params)


# --------------------------------------------------------------------------- #
# 渲染辅助
# --------------------------------------------------------------------------- #
def _fmt_time(ts: float) -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts))


def _fmt_duration(started: float, finished: float) -> str:
    return f"{max(0.0, finished - started):.2f}s"


def _clip(text: str | None, limit: int = _ERROR_CLIP) -> str:
    """截断错误摘要：超出部分以省略标记提示（保留末尾异常信息）。"""
    if not text:
        return ""
    if len(text) <= limit:
        return html.escape(text)
    return html.escape(f"...（前 {len(text) - limit} 字符已省略）...\n{text[-limit:]}")


_PAGE = """<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<title>{title}</title>
<style>
body {{ font-family: "Microsoft YaHei", sans-serif; margin: 2rem; color: #333; }}
h1 {{ font-size: 1.4rem; }}
table {{ border-collapse: collapse; width: 100%; margin-top: 1rem; }}
th, td {{ border: 1px solid #ddd; padding: 6px 10px; text-align: left; font-size: 0.9rem; }}
th {{ background: #f5f5f5; }}
tr:nth-child(even) {{ background: #fafafa; }}
.status-success {{ color: #1a7f37; }}
.status-failed {{ color: #c62828; }}
.status-skipped {{ color: #777; }}
.status-partial {{ color: #b26a00; }}
pre {{ background: #f5f5f5; padding: 8px; font-size: 0.85rem; white-space: pre-wrap; }}
a {{ color: #1565c0; text-decoration: none; }}
.meta {{ color: #666; font-size: 0.9rem; margin-bottom: 1rem; }}
</style>
</head>
<body>
{body}
</body>
</html>"""


def _render_index(
    conn: sqlite3.Connection,
    page: int = 1,
    size: int = _PAGE_SIZE_DEFAULT,
    status: str | None = None,
) -> str:
    total = _count_runs(conn, status)
    pages = max(1, (total + size - 1) // size)
    page = min(max(1, page), pages)  # 越界页回落到合法范围
    offset = (page - 1) * size
    runs = _query_runs(conn, status=status, limit=size, offset=offset)
    counts = _node_counts(conn, [r["run_id"] for r in runs])
    rows = "".join(
        f"<tr><td><a href='/run/{html.escape(r['run_id'])}'>{html.escape(r['run_id'])}</a></td>"
        f"<td>{html.escape(r['pipeline'])}</td>"
        f"<td><span class='status-{html.escape(r['status'])}'>{html.escape(r['status'])}</span></td>"
        f"<td>{_fmt_time(r['started_at'])}</td>"
        f"<td>{_fmt_duration(r['started_at'], r['finished_at'])}</td>"
        f"<td>{counts.get(r['run_id'], 0)}</td></tr>"
        for r in runs
    )
    prev_link = (
        f"<a href='{html.escape(_page_href(page - 1, size, status))}'>← 上一页</a>"
        if page > 1 else "<span>← 上一页</span>"
    )
    next_link = (
        f"<a href='{html.escape(_page_href(page + 1, size, status))}'>下一页 →</a>"
        if page < pages else "<span>下一页 →</span>"
    )
    filter_meta = f" · 状态筛选 {html.escape(status)}" if status else ""
    body = f"""<h1>op_pipeline 运行历史</h1>
<div class="meta">第 {page}/{pages} 页，共 {total} 条运行记录 · 每页 {size} 条{filter_meta}
· scheduler_app Web UI</div>
<table>
<tr><th>run_id</th><th>流水线</th><th>状态</th><th>开始时间</th><th>耗时</th><th>节点数</th></tr>
{rows or "<tr><td colspan='6' style='color:#999'>暂无运行记录</td></tr>"}
</table>
<div class="meta">{prev_link} · {next_link}</div>"""
    return _PAGE.format(title="运行历史", body=body)


def _render_detail(conn: sqlite3.Connection, run_id: str) -> str:
    run = _query_run(conn, run_id)
    nodes = _query_nodes(conn, run_id)
    node_rows = "".join(
        f"<tr><td>{html.escape(n['node_id'])}</td>"
        f"<td>{html.escape(n['op'])}</td>"
        f"<td><span class='status-{html.escape(n['status'])}'>{html.escape(n['status'])}</span></td>"
        f"<td>{n['attempts']}</td>"
        f"<td>{'是' if n['cached'] else '否'}</td>"
        f"<td>{_fmt_time(n['finished_at'])}</td>"
        f"<td>{float(n['duration'] or 0.0):.1f}s</td>"
        f"<td>{_clip(n['error'])}</td></tr>"
        for n in nodes
    )
    body = f"""<h1>运行详情：{html.escape(run['run_id'])}</h1>
<div class="meta">
流水线 {html.escape(run['pipeline'])}（v{html.escape(run['version'])}） ·
状态 <span class='status-{html.escape(run['status'])}'>{html.escape(run['status'])}</span> ·
开始 {_fmt_time(run['started_at'])} ·
总耗时 {_fmt_duration(run['started_at'], run['finished_at'])}
</div>
<table>
<tr><th>节点</th><th>算子</th><th>状态</th><th>尝试</th><th>缓存</th><th>完成时间</th><th>耗时</th><th>错误摘要</th></tr>
{node_rows or "<tr><td colspan='8' style='color:#999'>该运行无节点记录</td></tr>"}
</table>
<p class="meta">错误摘要为节点 error 字段（截断展示）；耗时为单节点执行时长。</p>"""
    return _PAGE.format(title=f"运行详情 {run_id}", body=body)


def _render_404(message: str) -> str:
    body = f"""<h1>404 未找到</h1>
<div class="meta">{html.escape(message)}</div>
<p><a href="/">← 返回运行历史</a></p>"""
    return _PAGE.format(title="404 未找到", body=body)


# --------------------------------------------------------------------------- #
# HTTP 处理器
# --------------------------------------------------------------------------- #
def _make_handler(db_path: str | Path) -> type[BaseHTTPRequestHandler]:
    """闭包注入 db_path，返回可传给 ThreadingHTTPServer 的处理器类。"""
    resolved = str(db_path)

    class _Handler(BaseHTTPRequestHandler):
        server_version = "scheduler_app-web/0.1"

        def log_message(self, fmt: str, *args: object) -> None:
            log.debug(fmt, *args)

        def _send(self, code: int, body: str) -> None:
            data = body.encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def do_GET(self) -> None:  # noqa: N802 - http.server 协议方法命名
            parsed = urlparse(self.path)
            path = parsed.path
            query = parse_qs(parsed.query)
            conn = _connect(resolved)
            try:
                if path in ("/", ""):
                    page, size, status = _parse_list_query(query)
                    self._send(200, _render_index(
                        conn, page=page, size=size, status=status,
                    ))
                elif path.startswith("/run/"):
                    run_id = unquote(path[len("/run/"):])
                    if run_id and _query_run(conn, run_id) is not None:
                        self._send(200, _render_detail(conn, run_id))
                    else:
                        self._send(404, _render_404(f"未找到运行记录：{run_id}"))
                else:
                    self._send(404, _render_404(f"未知路径：{path}"))
            finally:
                conn.close()

    return _Handler
