"""轻量 Web UI（M5-2 Task 6 / M6-2 Task 7 / M7-3 Task 7-8）：读 RunStore 渲染。

- 列表页 ``/``：run_id / status / 时间 / 节点数（HTML 表格，全部动态文本转义防注入）；
  M6 支持 ``?page=N&size=M&status=S`` 分页与状态筛选（SQL LIMIT/OFFSET 参数化）；
- 详情页 ``/run/<run_id>``：该次运行各节点状态 / 完成时间 / **耗时** / 错误摘要；
  M8-3 新增**节点执行时间线**：每节点一行的甘特式条形（内联 CSS，零外部资源）；
  M8 Task 9.2 新增 **In/Out 行数列**（``端口=行数`` 紧凑展示，旧行 NULL 显示为空）；
- 对比页 ``/compare?a=<run_id>&b=<run_id>``（M7-3 Task 7）：两份运行的总耗时与
  节点级差异（状态 / 耗时 / 缓存 / 耗时差），节点按 id 取并集并标注"仅 A/仅 B"；
  M8 Task 9.2 新增**行数差（A-B）列**（基于两侧 ``rows_out`` 求和，``+/-`` 标注）。
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
_TIMELINE_MIN_WINDOW = 1e-9  # 时间线窗口下限（各节点偏移全为 0 时防除零）


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


def _has_column(conn: sqlite3.Connection, table: str, column: str) -> bool:
    """判断表是否存在某列（旧库可能缺 report_json 等列，读前先探测）。"""
    if not _has_table(conn, table):
        return False
    return any(
        row[1] == column
        for row in conn.execute(f"PRAGMA table_info({table})")
    )


def _escape_like(text: str) -> str:
    """转义 LIKE 模式中的通配符（配合 ESCAPE '\\' 使用），使筛选按字面子串匹配。"""
    return text.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def _runs_where(
    status: str | None,
    pipeline: str | None,
    since: float | None,
    until: float | None,
) -> tuple[str, list[object]]:
    """组装 runs 表筛选条件（M9 Task 12.1）：status 精确 + pipeline 子串
    （不区分大小写）+ since/until 时间戳区间（含 since/until 全天）。"""
    clauses: list[str] = []
    params: list[object] = []
    if status:
        clauses.append("status = ?")
        params.append(status)
    if pipeline:
        clauses.append("LOWER(pipeline) LIKE LOWER(?) ESCAPE '\\'")
        params.append(f"%{_escape_like(pipeline)}%")
    if since is not None:
        clauses.append("started_at >= ?")
        params.append(since)
    if until is not None:
        clauses.append("started_at < ?")
        params.append(until)
    where = " WHERE " + " AND ".join(clauses) if clauses else ""
    return where, params


def _query_runs(
    conn: sqlite3.Connection,
    *,
    status: str | None = None,
    pipeline: str | None = None,
    since: float | None = None,
    until: float | None = None,
    limit: int | None = None,
    offset: int = 0,
) -> list[sqlite3.Row]:
    """分页查询运行记录：筛选与分页全部下推到 SQL（避免全量加载）。

    M9 Task 12.1：新增 ``pipeline``（子串、不区分大小写）与 ``since``/``until``
    （ISO 日期、含该天）筛选，与 status 任意组合。
    """
    sql = (
        "SELECT run_id, pipeline, version, status, started_at, finished_at FROM runs"
    )
    where, params = _runs_where(status, pipeline, since, until)
    sql += where
    sql += " ORDER BY started_at DESC, run_id DESC"
    if limit is not None:
        sql += " LIMIT ? OFFSET ?"
        params.extend((limit, offset))
    return list(conn.execute(sql, params))


def _count_runs(
    conn: sqlite3.Connection,
    status: str | None = None,
    pipeline: str | None = None,
    since: float | None = None,
    until: float | None = None,
) -> int:
    where, params = _runs_where(status, pipeline, since, until)
    row = conn.execute(f"SELECT COUNT(*) FROM runs{where}", params).fetchone()
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
    # M8 Task 9.2：行数列同理——旧库没有 rows_in/rows_out 列时回退 NULL，展示为空
    rows_in_expr = "rows_in" if "rows_in" in columns else "NULL AS rows_in"
    rows_out_expr = "rows_out" if "rows_out" in columns else "NULL AS rows_out"
    return list(conn.execute(
        f"SELECT node_id, op, status, attempts, cached, error, finished_at, "
        f"{duration_expr}, {rows_in_expr}, {rows_out_expr} "
        f"FROM nodes WHERE run_id = ? ORDER BY finished_at",
        (run_id,),
    ))


def _parse_day_start(raw: str) -> float | None:
    """ISO 日期 ``YYYY-MM-DD`` → 本地时区当日 00:00:00 时间戳；非法返回 None。

    since 直接用该值（``started_at >= since`` 含当天）；until 取
    ``since + 86400``（次日 00:00:00，``started_at < until`` 同样含当天）。
    """
    try:
        return time.mktime(time.strptime(raw, "%Y-%m-%d"))
    except (TypeError, ValueError, OverflowError):
        return None


def _parse_list_query(
    query: dict[str, list[str]],
) -> tuple[int, int, str | None, str | None, float | None, float | None]:
    """解析列表页查询参数：非法值回落默认，page<1→1，size 限 1..200。

    M9 Task 12.1：新增 ``pipeline``（子串、不区分大小写，可为空）、
    ``since``/``until``（ISO 日期、含该天；非法日期忽略该参数）。
    """
    page = _parse_int(query.get("page", [""])[0], default=1)
    size = _parse_int(query.get("size", [""])[0], default=_PAGE_SIZE_DEFAULT)
    page = max(1, page)
    size = min(_PAGE_SIZE_MAX, max(1, size))
    raw_status = query.get("status", [""])[0]
    status = raw_status if raw_status in _ALLOWED_STATUSES else None
    pipeline = query.get("pipeline", [""])[0] or None
    since = _parse_day_start(query.get("since", [""])[0])
    until_raw = query.get("until", [""])[0]
    until = (
        _parse_day_start(until_raw) + 86400
        if until_raw and _parse_day_start(until_raw) is not None else None
    )
    return page, size, status, pipeline, since, until


def _parse_int(raw: str, default: int) -> int:
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


def _page_href(
    page: int,
    size: int,
    status: str | None,
    pipeline: str | None = None,
    since: float | None = None,
    until: float | None = None,
) -> str:
    """翻页链接：保留全部筛选参数（M9 Task 12.1 起含 pipeline/since/until）。"""
    params: dict[str, str] = {"page": str(page), "size": str(size)}
    if status:
        params["status"] = status
    if pipeline:
        params["pipeline"] = pipeline
    if since is not None:
        params["since"] = time.strftime("%Y-%m-%d", time.localtime(since))
    if until is not None:
        params["until"] = time.strftime(
            "%Y-%m-%d", time.localtime(until - 86400)
        )
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


def _parse_rows(text: str | None) -> dict[str, int] | None:
    """行数 JSON 文本 → ``dict[端口名, 行数]``（M8 Task 9.2）。

    - NULL / 空串 / 非法 JSON / 非 dict / 无整数值 → 返回 None（旧行或坏数据，
      展示层一律显示为空，不猜 0、不报错）；
    - 值仅保留 ``int``（bool 是 int 子类，显式排除）；非整数值的端口跳过。
    """
    if not text:
        return None
    try:
        data = json.loads(text)
    except ValueError:
        return None
    if not isinstance(data, dict):
        return None
    rows: dict[str, int] = {}
    for port, value in data.items():
        if isinstance(value, bool) or not isinstance(value, int):
            continue
        rows[port] = value
    return rows or None


def _fmt_rows(text: str | None) -> str:
    """行数 dict 紧凑展示：``端口=行数`` 按端口名排序、逗号+空格分隔。

    旧行（NULL）/空串/非法 JSON 返回空字符串；端口名（动态文本）HTML 转义。
    """
    rows = _parse_rows(text)
    if rows is None:
        return ""
    return ", ".join(f"{html.escape(port)}={rows[port]}" for port in sorted(rows))


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
/* M8 Task 9.2：对比页行数差为正（A 比 B 多）标绿，为负用普通减号不加色 */
.rows-up {{ color: #1a7f37; }}
pre {{ background: #f5f5f5; padding: 8px; font-size: 0.85rem; white-space: pre-wrap; }}
a {{ color: #1565c0; text-decoration: none; }}
.meta {{ color: #666; font-size: 0.9rem; margin-bottom: 1rem; }}
/* M8-3：详情页节点执行时间线（甘特式条形，纯 CSS 实现，零外部资源） */
.timeline {{ margin-top: 1rem; }}
.tl-scale {{ display: flex; justify-content: space-between; font-size: 0.75rem;
  color: #888; border-bottom: 1px solid #ddd; padding-bottom: 2px; }}
.tl-row {{ display: flex; align-items: center; margin-top: 4px; }}
.tl-label {{ flex: none; width: 220px; overflow: hidden; text-overflow: ellipsis;
  white-space: nowrap; font-size: 0.85rem; }}
.tl-track {{ position: relative; flex: 1; height: 14px; background: #f7f7f7;
  border: 1px solid #eee; overflow: hidden; }}
.tl-bar {{ position: absolute; top: 2px; height: 10px; min-width: 2px;
  border-radius: 2px; }}
.tl-success {{ background: #1a7f37; }}
.tl-failed {{ background: #c62828; }}
.tl-cached {{ background: #9e9e9e; }}
.tl-skipped {{ background: #d5d5d5; }}
.tl-other {{ background: #b26a00; }}
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
    pipeline: str | None = None,
    since: float | None = None,
    until: float | None = None,
) -> str:
    total = _count_runs(conn, status, pipeline, since, until)
    pages = max(1, (total + size - 1) // size)
    page = min(max(1, page), pages)  # 越界页回落到合法范围
    offset = (page - 1) * size
    runs = _query_runs(
        conn, status=status, pipeline=pipeline, since=since, until=until,
        limit=size, offset=offset,
    )
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
        f"<a href='{html.escape(_page_href(page - 1, size, status, pipeline, since, until))}'>← 上一页</a>"
        if page > 1 else "<span>← 上一页</span>"
    )
    next_link = (
        f"<a href='{html.escape(_page_href(page + 1, size, status, pipeline, since, until))}'>下一页 →</a>"
        if page < pages else "<span>下一页 →</span>"
    )
    filters = []
    if status:
        filters.append(f"状态 {html.escape(status)}")
    if pipeline:
        filters.append(f"流水线 {html.escape(pipeline)}")
    if since is not None:
        filters.append(f"起始 {time.strftime('%Y-%m-%d', time.localtime(since))}")
    if until is not None:
        filters.append(f"截止 {time.strftime('%Y-%m-%d', time.localtime(until - 86400))}")
    filter_meta = f" · 筛选：{'、'.join(filters)}" if filters else ""
    body = f"""<h1>op_pipeline 运行历史</h1>
<div class="meta">第 {page}/{pages} 页，共 {total} 条运行记录 · 每页 {size} 条{filter_meta}
· scheduler_app Web UI</div>
<table>
<tr><th>run_id</th><th>流水线</th><th>状态</th><th>开始时间</th><th>耗时</th><th>节点数</th></tr>
{rows or "<tr><td colspan='6' style='color:#999'>暂无运行记录</td></tr>"}
</table>
<div class="meta">{prev_link} · {next_link}</div>"""
    return _PAGE.format(title="运行历史", body=body)


def _timeline_bar_class(status: str, cached: bool) -> str:
    """时间条颜色分类：cached（命中缓存的成功节点）优先于普通 success。"""
    if status == "success" and cached:
        return "tl-cached"
    return {
        "success": "tl-success",
        "failed": "tl-failed",
        "skipped": "tl-skipped",
    }.get(status, "tl-other")


def _render_timeline(run: sqlite3.Row, nodes: list[sqlite3.Row]) -> str:
    """节点执行时间线（M8-3 Task 7）：每节点一行的甘特式条形，纯 CSS 实现。

    - 起点 = ``finished_at - duration`` 相对 run ``started_at`` 的偏移（秒），
      条长 ∝ duration；窗口总时长 = max(各节点完成偏移, 最小值防除零)；
    - 顶部为相对刻度标尺（0%–100%，简洁起见不标注绝对秒数）；
    - 颜色语义：success 绿 / failed 红 / cached 灰 / skipped 淡灰（``.tl-*``）；
    - 异常数据健壮：duration=0 走 CSS ``min-width`` 保持可见；缺 ``finished_at``
      的旧行跳过；无节点 / 全失败 run 仍正常渲染；偏移与宽度越界一律收拢到
      [0%, 100%] 区间。数据源只用 nodes 表既有列，不新增列。
    """
    started = float(run["started_at"])
    valid = [
        (n, float(n["finished_at"]), max(0.0, float(n["duration"] or 0.0)))
        for n in nodes if n["finished_at"] is not None
    ]
    window = max(
        (max(0.0, fin - started) for _, fin, _ in valid), default=0.0
    )
    window = max(window, _TIMELINE_MIN_WINDOW)
    bars = []
    for n, fin, duration in valid:
        start_offset = max(0.0, fin - duration - started)
        left = min(100.0, start_offset / window * 100.0)
        width = min(100.0 - left, duration / window * 100.0)
        tip = f"{n['node_id']} · {n['status']} · {duration:.2f}s"
        bars.append(
            "<div class='tl-row'>"
            f"<span class='tl-label'>{html.escape(n['node_id'])}</span>"
            "<div class='tl-track'>"
            f"<div class='tl-bar {_timeline_bar_class(n['status'], bool(n['cached']))}' "
            f"style='left:{left:.3f}%;width:{width:.3f}%' "
            f"title='{html.escape(tip)}'></div>"
            "</div></div>"
        )
    scale = (
        "<div class='tl-scale'><span>0%</span><span>25%</span><span>50%</span>"
        "<span>75%</span><span>100%</span></div>"
    )
    empty_note = "" if bars else "<div class='meta'>该运行无可用时间线数据</div>"
    legend = (
        "<p class='meta'>时间线按节点完成顺序排列：条形左端为节点开始执行时刻"
        "（finished_at − duration）相对运行开始的偏移，条长与耗时成正比；"
        "绿=success、红=failed、灰=缓存命中、淡灰=skipped。</p>"
    )
    return (
        f"<h2>节点执行时间线</h2>"
        f"<div class='timeline'>{scale}{''.join(bars)}{empty_note}</div>"
        f"{legend}"
    )


def _query_report_json(conn: sqlite3.Connection, run_id: str) -> str | None:
    """读 runs.report_json（M9 Task 12.2/12.3）。

    返回 ``str``（可为空串 = 该列存在但为 NULL，即"报告未入库"）；
    仅当 **run 记录不存在** 或 runs 表缺列时返回 None（调用方据此回 404）。
    """
    if not _has_column(conn, "runs", "report_json"):
        return None
    row = conn.execute(
        "SELECT report_json FROM runs WHERE run_id = ?", (run_id,)
    ).fetchone()
    if row is None:
        return None
    return row[0] if row[0] is not None else ""


def _render_report_section(conn: sqlite3.Connection, run_id: str) -> str:
    """详情页"报告"折叠区块（M9 Task 12.2）。

    - ``report_json`` 非空：``<details>`` 内展示完整 params（JSON 缩进）、
      warnings 列表、失败节点（status=failed）的 error 与**完整 traceback**；
      所有动态文本一律 ``html.escape``（traceback 含 ``<script>`` 等也安全）；
    - ``report_json`` 为 NULL（未入库）或非法 JSON：显示占位文案，不 500；
    - 旧库 runs 无 report_json 列：整个区块不渲染。
    """
    raw = _query_report_json(conn, run_id)
    if raw is None or not raw.strip():
        return (
            "<h2>运行报告</h2>"
            "<details><summary>报告</summary>"
            "<div class='meta'>报告未入库（report_json 为空）</div></details>"
        )
    try:
        report = json.loads(raw)
    except ValueError:
        return (
            "<h2>运行报告</h2>"
            "<details><summary>报告</summary>"
            "<div class='meta'>报告数据异常（report_json 非法 JSON）</div></details>"
        )
    if not isinstance(report, dict):
        return (
            "<h2>运行报告</h2>"
            "<details><summary>报告</summary>"
            "<div class='meta'>报告数据异常（report_json 非对象）</div></details>"
        )

    # 参数：完整 JSON 缩进展示（值可能是嵌套 dict/list，统一序列化后转义）
    params_html = (
        html.escape(
            json.dumps(report.get("params", {}), ensure_ascii=False, indent=2,
                       default=str)
        )
        if report.get("params")
        else "<div class='meta'>无参数</div>"
    )
    warnings = report.get("warnings") or []
    warnings_html = (
        "".join(
            f"<li>{html.escape(str(w))}</li>" for w in warnings
        )
        if warnings else "<div class='meta'>无告警</div>"
    )
    # 失败节点：完整 error + 完整 traceback（traceback 为空显示占位）
    failed_nodes = [
        n for n in (report.get("nodes") or [])
        if isinstance(n, dict) and n.get("status") == "failed"
    ]
    failed_html = ""
    for node in failed_nodes:
        node_id = html.escape(str(node.get("id", "")))
        op = html.escape(str(node.get("op", "")))
        error = html.escape(str(node.get("error") or ""))
        traceback = html.escape(str(node.get("traceback") or ""))
        failed_html += (
            f"<h4>节点 {node_id}（{op}）</h4>"
            f"<div class='meta'>错误：{error or '（无错误信息）'}</div>"
            f"<pre>{traceback or '（无 traceback）'}</pre>"
        )
    failed_block = (
        failed_html
        if failed_html
        else "<div class='meta'>无失败节点</div>"
    )
    return (
        "<h2>运行报告</h2>"
        "<details><summary>报告（参数 / 告警 / 失败节点）</summary>"
        "<h3>参数（params）</h3><pre>{params}</pre>"
        "<h3>告警（warnings）</h3><ul>{warnings}</ul>"
        "<h3>失败节点（完整 traceback）</h3>{failed}"
        "</details>"
    ).format(params=params_html, warnings=warnings_html, failed=failed_block)


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
        f"<td>{_fmt_rows(n['rows_in'])}</td>"
        f"<td>{_fmt_rows(n['rows_out'])}</td>"
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
<tr><th>节点</th><th>算子</th><th>状态</th><th>尝试</th><th>缓存</th><th>完成时间</th><th>耗时</th><th>In</th><th>Out</th><th>错误摘要</th></tr>
{node_rows or "<tr><td colspan='10' style='color:#999'>该运行无节点记录</td></tr>"}
</table>
{_render_timeline(run, nodes)}
{_render_report_section(conn, run_id)}
<p class="meta">错误摘要为节点 error 字段（截断展示）；耗时为单节点执行时长；
In/Out 为各 list 端口行数血缘（``端口=行数`` 按端口名排序，旧行未采集显示为空）。</p>"""
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


def _rows_diff_cell(node_a: sqlite3.Row | None,
                    node_b: sqlite3.Row | None) -> str:
    """行数差异单元格（M8 Task 9.2）：两侧节点 ``rows_out`` 总行数之差（A−B）。

    - 节点仅一侧存在 → "仅 A"/"仅 B"（与状态列口径一致：按节点存在性标注）；
    - 两侧节点齐备但任一侧无行数（旧行 NULL / 坏数据）→ 空，不猜 0；
    - 两侧都有行数 → ``+N``（A 多，绿色标注）/ ``-N``（B 多，普通减号避免
      特殊字符问题）/ ``0``（相同）。差值基于各端口行数求和。
    """
    if node_a is None:
        return "<span class='meta'>仅 B</span>"
    if node_b is None:
        return "<span class='meta'>仅 A</span>"
    rows_a = _parse_rows(node_a["rows_out"])
    rows_b = _parse_rows(node_b["rows_out"])
    if rows_a is None or rows_b is None:
        return ""
    delta = sum(rows_a.values()) - sum(rows_b.values())
    if delta > 0:
        return f"<span class='rows-up'>+{delta}</span>"
    if delta < 0:
        return f"-{-delta}"
    return "0"


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
            f"<td>{diff}</td>"
            f"<td>{_rows_diff_cell(a, b)}</td></tr>"
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
<th>A 缓存/续跑</th><th>B 缓存/续跑</th><th>耗时差（A-B）</th><th>行数差（A-B）</th></tr>
{''.join(rows) or "<tr><td colspan='9' style='color:#999'>两次运行均无节点记录</td></tr>"}
</table>
<p class="meta">节点按 id 取并集；仅一侧存在时标注"仅 A"/"仅 B"；
耗时差为正表示 A 更慢；行数差基于两侧节点 ``rows_out`` 各端口求和（A−B），
为正标绿、为负用普通减号、任一侧旧行未采集则显示为空。
nodes 表仅持久化 cached，故"缓存/续跑"列以缓存标记展示。</p>
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
    """``/api/runs``：分页/筛选语义与列表页完全一致（同一解析与 SQL）。

    M9 Task 12.1 起含 pipeline/since/until 筛选（与列表页共用解析）。
    """
    page, size, status, pipeline, since, until = _parse_list_query(query)
    total = _count_runs(conn, status, pipeline, since, until)
    pages = max(1, (total + size - 1) // size)
    page = min(max(1, page), pages)
    runs = _query_runs(
        conn, status=status, pipeline=pipeline, since=since, until=until,
        limit=size, offset=(page - 1) * size,
    )
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


def _render_api_report(conn: sqlite3.Connection, run_id: str) -> str | None:
    """``/api/runs/<id>/report``（M9 Task 12.3）：report_json 解析后的完整对象。

    - run 不存在 → None（调用方回 404）；
    - report_json 为 NULL / 空 / 非法 JSON → ``{"report": null}``（与页面
      占位口径一致：未入库按无报告处理，不 500）。
    """
    raw = _query_report_json(conn, run_id)
    if raw is None:
        return None
    report = None
    if raw.strip():
        try:
            parsed = json.loads(raw)
            report = parsed if isinstance(parsed, dict) else None
        except ValueError:
            report = None
    return json.dumps({"report": report}, ensure_ascii=False)


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


# --------------------------------------------------------------------------- #
# /stats 聚合页（M9 Task 13）
# --------------------------------------------------------------------------- #
_STATS_DAYS_DEFAULT = 7  # 默认统计窗口（天）
_STATS_DAYS_MAX = 90  # 窗口上限；非法/超上限回退默认


def _parse_stats_days(query: dict[str, list[str]]) -> int:
    """解析 ``days`` 参数：1..90 合法；非法（非整数/≤0/超上限）回退默认 7。"""
    days = _parse_int(query.get("days", [""])[0], default=_STATS_DAYS_DEFAULT)
    if not (1 <= days <= _STATS_DAYS_MAX):
        days = _STATS_DAYS_DEFAULT
    return days


def _stats_by_pipeline(
    conn: sqlite3.Connection, since_ts: float
) -> list[sqlite3.Row]:
    """按 pipeline 汇总：运行总数 / 成功次数 / 成功率基数 / 平均耗时（秒）。

    全部下推到 SQLite 聚合（AVG/SUM/CASE），不加载全表。
    """
    return list(conn.execute(
        "SELECT pipeline, COUNT(*) AS runs, "
        "SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) AS successes, "
        "AVG(finished_at - started_at) AS avg_duration "
        "FROM runs WHERE started_at >= ? "
        "GROUP BY pipeline ORDER BY runs DESC, pipeline ASC",
        (since_ts,),
    ))


def _stats_failed_nodes(
    conn: sqlite3.Connection, since_ts: float
) -> list[sqlite3.Row]:
    """失败节点 Top10（M9 Task 13.1）：按 node_id 聚合失败次数，标注 op。

    窗口口径与按 pipeline 汇总一致（join runs 按 started_at 过滤）；
    op 取 MAX（同 id 多 op 时确定性取字典序最大者）。
    """
    if not _has_table(conn, "nodes"):
        return []
    return list(conn.execute(
        "SELECT n.node_id, MAX(n.op) AS op, COUNT(*) AS fails "
        "FROM nodes n JOIN runs r ON r.run_id = n.run_id "
        "WHERE n.status = 'failed' AND r.started_at >= ? "
        "GROUP BY n.node_id ORDER BY fails DESC, n.node_id ASC LIMIT 10",
        (since_ts,),
    ))


def _render_stats(conn: sqlite3.Connection, query: dict[str, list[str]]) -> str:
    """``/stats?days=N`` 聚合页：按 pipeline 汇总 + 失败节点 Top10。

    纯 SQLite 聚合 + 内联 HTML（复用 _PAGE 模板与转义风格），零外部资源。
    """
    days = _parse_stats_days(query)
    now = time.time()
    since_ts = now - days * 86400
    by_pipeline = _stats_by_pipeline(conn, since_ts)
    failed_nodes = _stats_failed_nodes(conn, since_ts)
    pipeline_rows = "".join(
        f"<tr><td>{html.escape(row['pipeline'])}</td>"
        f"<td>{row['runs']}</td>"
        f"<td>{row['successes']}/{row['runs']} "
        f"（{row['successes'] / row['runs'] * 100:.1f}%）</td>"
        f"<td>{float(row['avg_duration'] or 0.0):.2f}s</td></tr>"
        for row in by_pipeline
    )
    node_rows = "".join(
        f"<tr><td>{i + 1}</td>"
        f"<td>{html.escape(row['node_id'])}</td>"
        f"<td>{html.escape(row['op'])}</td>"
        f"<td>{row['fails']}</td></tr>"
        for i, row in enumerate(failed_nodes)
    )
    body = f"""<h1>运行统计</h1>
<div class="meta">统计窗口：最近 {days} 天（自 {_fmt_time(since_ts)} 起）· 共 {sum(row['runs'] for row in by_pipeline)} 条运行 · scheduler_app Web UI</div>
<h2>按流水线汇总</h2>
<table>
<tr><th>流水线</th><th>运行总数</th><th>成功率（成功/总数）</th><th>平均耗时</th></tr>
{pipeline_rows or "<tr><td colspan='4' style='color:#999'>窗口内无运行记录</td></tr>"}
</table>
<h2>失败节点 Top10</h2>
<table>
<tr><th>排名</th><th>节点</th><th>算子</th><th>失败次数</th></tr>
{node_rows or "<tr><td colspan='4' style='color:#999'>窗口内无失败节点</td></tr>"}
</table>
<p class="meta">成功率 = 成功次数/运行总数；平均耗时为 finished_at − started_at 均值；
失败节点按 node_id 跨运行聚合，窗口口径与流水线汇总一致（按 started_at）。</p>
<p><a href="/">← 返回运行历史</a></p>"""
    return _PAGE.format(title="运行统计", body=body)


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
                    page, size, status, pipeline, since, until = _parse_list_query(query)
                    self._send(200, _render_index(
                        conn, page=page, size=size, status=status,
                        pipeline=pipeline, since=since, until=until,
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
                elif path.startswith("/api/runs/") and path.endswith("/report"):
                    # M9 Task 12.3：报告 JSON（须先于通用 /api/runs/<id> 分支匹配）
                    run_id = unquote(path[len("/api/runs/"):-len("/report")])
                    payload = _render_api_report(conn, run_id)
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
                elif path == "/stats":
                    self._send(200, _render_stats(conn, query))
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
