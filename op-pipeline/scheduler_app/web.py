"""轻量 Web UI（M5-2 Task 6 / M6-2 Task 7 / M7-3 Task 7-8）：读 RunStore 渲染。

- 列表页 ``/``：run_id / status / 时间 / 节点数（HTML 表格，全部动态文本转义防注入）；
  M6 支持 ``?page=N&size=M&status=S`` 分页与状态筛选（SQL LIMIT/OFFSET 参数化）；
- 详情页 ``/run/<run_id>``：该次运行各节点状态 / 完成时间 / **耗时** / 错误摘要；
- 对比页 ``/compare?a=<run_id>&b=<run_id>``（M7-3 Task 7）：两份运行的总耗时与
  节点级差异（状态 / 耗时 / 缓存 / 耗时差），节点按 id 取并集并标注"仅 A/仅 B"；
- 只读 JSON API（M7-3 Task 8）：``/api/runs``（分页/筛选）、``/api/runs/<run_id>``；
- 指标 ``/metrics``（M7-3 Task 8）：Prometheus ``text/plain; version=0.0.4`` 文本；
- :func:`create_server(db_path, port=0)`` 返回 server（port=0 自动分配）便于测试；
- 仅使用标准库：http.server / sqlite3 / html / json / time / urllib.parse。

注意：M6 起 nodes 表持久化单节点 ``duration``（旧库自动补列）；若外部库没有该列，
详情页回退显示 ``0.0s`` 而不报错。nodes 表未持久化 ``resumed``，对比页的
"缓存/续跑"列以 ``cached`` 展示（resumed 无独立落库字段）。
"""
from __future__ import annotations

import html
import json
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


def _run_total_duration(run: sqlite3.Row) -> float:
    """运行总耗时（秒）：finished_at - started_at，负值归零。"""
    return max(0.0, float(run["finished_at"]) - float(run["started_at"]))


def _cached_label(node: sqlite3.Row | None) -> str:
    """节点缓存/续跑标记：nodes 表只持久化 cached，展示为"缓存"或"—"。"""
    if node is None:
        return "—"
    return "缓存" if node["cached"] else "—"


def _fmt_signed_duration(delta: float) -> str:
    """耗时差格式：``+1.0s`` / ``-2.0s``（1 位小数）。"""
    return f"{delta:+.1f}s"


def _node_cell(node: sqlite3.Row | None, missing_label: str) -> str:
    """节点状态单元格：缺失一侧显示"仅 A"/"仅 B"。"""
    if node is None:
        return f"<span class='meta'>{html.escape(missing_label)}</span>"
    return (
        f"<span class='status-{html.escape(node['status'])}'>"
        f"{html.escape(node['status'])}</span>"
    )


def _render_compare(conn: sqlite3.Connection, run_a: sqlite3.Row,
                    run_b: sqlite3.Row) -> str:
    """渲染两 run 对比页：概览 + 节点级差异表（并集，SQL 分 run 查询）。"""
    nodes_a = {n["node_id"]: n for n in _query_nodes(conn, run_a["run_id"])}
    nodes_b = {n["node_id"]: n for n in _query_nodes(conn, run_b["run_id"])}
    node_ids = sorted(set(nodes_a) | set(nodes_b))
    rows = []
    for node_id in node_ids:
        a, b = nodes_a.get(node_id), nodes_b.get(node_id)
        a_dur = float(a["duration"] or 0.0) if a is not None else None
        b_dur = float(b["duration"] or 0.0) if b is not None else None
        if a is not None and b is not None:
            diff = _fmt_signed_duration(a_dur - b_dur)
        else:
            diff = "—"
        rows.append(
            f"<tr><td>{html.escape(node_id)}</td>"
            f"<td>{_node_cell(a, '仅 B')}</td>"
            f"<td>{_node_cell(b, '仅 A')}</td>"
            f"<td>{'—' if a_dur is None else f'{a_dur:.1f}s'}</td>"
            f"<td>{'—' if b_dur is None else f'{b_dur:.1f}s'}</td>"
            f"<td>{html.escape(_cached_label(a))}</td>"
            f"<td>{html.escape(_cached_label(b))}</td>"
            f"<td>{diff}</td></tr>"
        )
    body = f"""<h1>运行对比</h1>
<div class="meta">A：<a href='/run/{html.escape(run_a['run_id'])}'>{html.escape(run_a['run_id'])}</a>
· B：<a href='/run/{html.escape(run_b['run_id'])}'>{html.escape(run_b['run_id'])}</a></div>
<table>
<tr><th>运行</th><th>流水线</th><th>状态</th><th>总耗时</th></tr>
<tr><td>A</td><td>{html.escape(run_a['pipeline'])}</td>
<td><span class='status-{html.escape(run_a['status'])}'>{html.escape(run_a['status'])}</span></td>
<td>{_fmt_duration(run_a['started_at'], run_a['finished_at'])}</td></tr>
<tr><td>B</td><td>{html.escape(run_b['pipeline'])}</td>
<td><span class='status-{html.escape(run_b['status'])}'>{html.escape(run_b['status'])}</span></td>
<td>{_fmt_duration(run_b['started_at'], run_b['finished_at'])}</td></tr>
</table>
<table>
<tr><th>节点</th><th>A 状态</th><th>B 状态</th><th>A 耗时</th><th>B 耗时</th>
<th>A 缓存/续跑</th><th>B 缓存/续跑</th><th>耗时差（A-B）</th></tr>
{''.join(rows) or "<tr><td colspan='8' style='color:#999'>两次运行均无节点记录</td></tr>"}
</table>
<p class="meta">节点按 id 取并集；仅一侧存在时标注"仅 A"/"仅 B"；
耗时差为正表示 A 更慢。nodes 表仅持久化 cached，故"缓存/续跑"列以缓存标记展示。</p>
<p><a href="/">← 返回运行历史</a></p>"""
    return _PAGE.format(
        title=f"运行对比 {html.escape(run_a['run_id'])} vs "
              f"{html.escape(run_b['run_id'])}",
        body=body,
    )


# --------------------------------------------------------------------------- #
# 只读 JSON API 与 Prometheus 指标（M7-3 Task 8）
# --------------------------------------------------------------------------- #
def _run_to_dict(run: sqlite3.Row) -> dict[str, object]:
    """run 行 → JSON 对象（字段与列表页一致，额外给 duration）。"""
    return {
        "run_id": run["run_id"],
        "pipeline": run["pipeline"],
        "version": run["version"],
        "status": run["status"],
        "started_at": run["started_at"],
        "finished_at": run["finished_at"],
        "duration": round(_run_total_duration(run), 6),
    }


def _node_to_dict(node: sqlite3.Row) -> dict[str, object]:
    """node 行 → JSON 对象（cached 转布尔，duration 转浮点）。"""
    return {
        "node_id": node["node_id"],
        "op": node["op"],
        "status": node["status"],
        "attempts": node["attempts"],
        "cached": bool(node["cached"]),
        "error": node["error"],
        "finished_at": node["finished_at"],
        "duration": float(node["duration"] or 0.0),
    }


def _render_api_runs(conn: sqlite3.Connection, query: dict[str, list[str]]) -> str:
    """``/api/runs``：分页/筛选语义与列表页完全一致（同一解析与 SQL）。"""
    page, size, status = _parse_list_query(query)
    total = _count_runs(conn, status)
    pages = max(1, (total + size - 1) // size)
    page = min(max(1, page), pages)
    runs = _query_runs(conn, status=status, limit=size, offset=(page - 1) * size)
    counts = _node_counts(conn, [r["run_id"] for r in runs])
    items = []
    for run in runs:
        item = _run_to_dict(run)
        item["node_count"] = counts.get(run["run_id"], 0)
        items.append(item)
    return json.dumps(
        {"items": items, "total": total, "page": page, "size": size},
        ensure_ascii=False,
    )


def _render_api_run(conn: sqlite3.Connection, run_id: str) -> str | None:
    """``/api/runs/<id>``：run + nodes；run 不存在返回 None（调用方回 404）。"""
    run = _query_run(conn, run_id)
    if run is None:
        return None
    nodes = [_node_to_dict(n) for n in _query_nodes(conn, run_id)]
    return json.dumps({"run": _run_to_dict(run), "nodes": nodes}, ensure_ascii=False)


def _render_metrics(conn: sqlite3.Connection) -> str:
    """Prometheus 文本指标：各状态运行计数 + 全部 run 耗时总和，末尾空行。"""
    counts = {status: 0 for status in _ALLOWED_STATUSES}
    placeholders = ",".join("?" for _ in _ALLOWED_STATUSES)
    for row in conn.execute(
        f"SELECT status, COUNT(*) FROM runs WHERE status IN ({placeholders}) "
        f"GROUP BY status",
        _ALLOWED_STATUSES,
    ):
        counts[row[0]] = int(row[1])
    total_row = conn.execute(
        "SELECT COALESCE(SUM(finished_at - started_at), 0.0) FROM runs"
    ).fetchone()
    total_duration = max(0.0, float(total_row[0] or 0.0))
    lines = [
        "# HELP op_pipeline_runs_total 按状态统计的运行次数",
        "# TYPE op_pipeline_runs_total counter",
    ]
    lines.extend(
        f'op_pipeline_runs_total{{status="{status}"}} {counts[status]}'
        for status in _ALLOWED_STATUSES
    )
    lines.extend([
        "# HELP op_pipeline_runs_duration_seconds 全部运行耗时总和（秒）",
        "# TYPE op_pipeline_runs_duration_seconds counter",
        f"op_pipeline_runs_duration_seconds {total_duration:.6f}",
    ])
    return "\n".join(lines) + "\n\n"


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

        def _send_body(
            self, code: int, body: str,
            content_type: str = "text/html; charset=utf-8",
        ) -> None:
            data = body.encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def _send(self, code: int, body: str) -> None:
            self._send_body(code, body)

        def _send_json(self, code: int, body: str) -> None:
            self._send_body(code, body, "application/json; charset=utf-8")

        def _send_plain(self, code: int, body: str) -> None:
            self._send_body(code, body, "text/plain; charset=utf-8")

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
                elif path == "/compare":
                    run_a = query.get("a", [""])[0]
                    run_b = query.get("b", [""])[0]
                    if not run_a or not run_b:
                        self._send_plain(
                            400, "参数错误：需要同时提供 a 与 b 两个 run_id\n"
                        )
                        return
                    row_a = _query_run(conn, run_a)
                    row_b = _query_run(conn, run_b)
                    if row_a is None or row_b is None:
                        missing = [rid for rid, row in ((run_a, row_a), (run_b, row_b))
                                   if row is None]
                        self._send_plain(
                            404, f"未找到运行记录：{', '.join(missing)}\n"
                        )
                        return
                    self._send(200, _render_compare(conn, row_a, row_b))
                elif path == "/api/runs":
                    self._send_json(200, _render_api_runs(conn, query))
                elif path.startswith("/api/runs/"):
                    run_id = unquote(path[len("/api/runs/"):])
                    payload = _render_api_run(conn, run_id)
                    if payload is None:
                        self._send_json(
                            404,
                            json.dumps(
                                {"error": f"未找到运行记录：{run_id}"},
                                ensure_ascii=False,
                            ),
                        )
                    else:
                        self._send_json(200, payload)
                elif path == "/metrics":
                    self._send_body(
                        200, _render_metrics(conn),
                        "text/plain; version=0.0.4; charset=utf-8",
                    )
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
