"""轻量 Web UI（M5-2 Task 6）：``http.server`` 读 RunStore SQLite 渲染 HTML。

- 列表页 ``/``：run_id / status / 时间 / 节点数（HTML 表格，全部动态文本转义防注入）；
- 详情页 ``/run/<run_id>``：该次运行各节点状态 / 完成时间 / 错误摘要（截断展示）；
- :func:`create_server(db_path, port=0)`` 返回 server（port=0 自动分配）便于测试；
- 仅使用标准库：http.server / sqlite3 / html / time。

注意：nodes 表只持久化节点**结束时间**（见 op_pipeline.store.record_node），
未持久化单节点耗时，故详情页在头部展示本次运行总耗时，节点行展示完成时间；
错误摘要来自节点 error 字段（引擎写入的"异常类型: 信息"摘要），截断展示。
"""
from __future__ import annotations

import html
import logging
import sqlite3
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import unquote, urlparse

log = logging.getLogger("scheduler_app.web")

_ERROR_CLIP = 300  # 错误摘要截断长度（字符）


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


def _query_runs(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return list(conn.execute(
        "SELECT run_id, pipeline, version, status, started_at, finished_at "
        "FROM runs ORDER BY started_at DESC"
    ))


def _node_counts(conn: sqlite3.Connection) -> dict[str, int]:
    if not _has_table(conn, "nodes"):
        return {}
    return {
        row["run_id"]: row["n"]
        for row in conn.execute("SELECT run_id, COUNT(*) AS n FROM nodes GROUP BY run_id")
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
    return list(conn.execute(
        "SELECT node_id, op, status, attempts, cached, error, finished_at "
        "FROM nodes WHERE run_id = ? ORDER BY finished_at",
        (run_id,),
    ))


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


def _render_index(conn: sqlite3.Connection) -> str:
    runs = _query_runs(conn)
    counts = _node_counts(conn)
    rows = "".join(
        f"<tr><td><a href='/run/{html.escape(r['run_id'])}'>{html.escape(r['run_id'])}</a></td>"
        f"<td>{html.escape(r['pipeline'])}</td>"
        f"<td><span class='status-{html.escape(r['status'])}'>{html.escape(r['status'])}</span></td>"
        f"<td>{_fmt_time(r['started_at'])}</td>"
        f"<td>{_fmt_duration(r['started_at'], r['finished_at'])}</td>"
        f"<td>{counts.get(r['run_id'], 0)}</td></tr>"
        for r in runs
    )
    body = f"""<h1>op_pipeline 运行历史</h1>
<div class="meta">共 {len(runs)} 条运行记录 · scheduler_app Web UI</div>
<table>
<tr><th>run_id</th><th>流水线</th><th>状态</th><th>开始时间</th><th>耗时</th><th>节点数</th></tr>
{rows or "<tr><td colspan='6' style='color:#999'>暂无运行记录</td></tr>"}
</table>"""
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
<tr><th>节点</th><th>算子</th><th>状态</th><th>尝试</th><th>缓存</th><th>完成时间</th><th>错误摘要</th></tr>
{node_rows or "<tr><td colspan='7' style='color:#999'>该运行无节点记录</td></tr>"}
</table>
<p class="meta">错误摘要为节点 error 字段（截断展示）；nodes 表仅持久化节点结束时间。</p>"""
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
            path = urlparse(self.path).path
            conn = _connect(resolved)
            try:
                if path in ("/", ""):
                    self._send(200, _render_index(conn))
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
