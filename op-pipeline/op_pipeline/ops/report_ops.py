"""报告算子：把中间结果组装成 Markdown / HTML 产物。"""
from __future__ import annotations

import html
from typing import Any

from ..operator import Operator
from ..params import Param
from ..registry import op


def _cell(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).replace("|", "\\|")
    return text.replace("\n", " ")


def to_markdown_table(rows: list[dict[str, Any]], columns: list[str] | None = None, limit: int = 0) -> str:
    rows = list(rows or [])
    if not rows:
        return "_（无数据）_"
    cols = list(columns) if columns else list(rows[0].keys())
    body = rows[:limit] if limit else rows
    lines = [
        "| " + " | ".join(_cell(c) for c in cols) + " |",
        "| " + " | ".join("---" for _ in cols) + " |",
    ]
    for row in body:
        lines.append("| " + " | ".join(_cell(row.get(c)) for c in cols) + " |")
    if limit and len(rows) > limit:
        lines.append(f"\n_（共 {len(rows)} 行，已截断显示前 {limit} 行）_")
    return "\n".join(lines)


@op("report.markdown")
class MarkdownReport(Operator):
    """渲染 Markdown 报告。

    ``blocks`` 支持两类块：

    * ``{"heading": "标题", "level": 2}``
    * ``{"table": <行表>, "title": "表名", "columns": [...], "limit": 20}``
    * ``{"text": "一段文本"}``
    * ``{"kv": {"指标": 值, ...}}``
    """

    description = "渲染 Markdown 报告"
    outputs = [("text", str), ("length", int)]
    params = {
        "title": Param(str, doc="报告标题"),
        "blocks": Param(list, default=None, doc="报告内容块列表"),
        "meta": Param(dict, default=None, doc="元信息键值对，渲染为列表"),
        "generated_at": Param(str, default="", doc="生成时间文本，留空则自动取当前时间"),
    }

    def run(self, ctx, title, blocks, meta, generated_at):
        import time

        lines: list[str] = [f"# {title}", ""]
        stamp = generated_at or time.strftime("%Y-%m-%d %H:%M:%S")
        lines.append(f"> 生成时间：{stamp}　|　run_id：`{ctx.run_id}`")
        if meta:
            lines.append("")
            for key, value in meta.items():
                lines.append(f"- **{key}**：{value}")
        lines.append("")

        for block in blocks or []:
            if "heading" in block:
                level = int(block.get("level", 2))
                lines.append("#" * max(1, min(6, level)) + " " + str(block["heading"]))
                lines.append("")
            elif "kv" in block:
                if block.get("title"):
                    lines.append(f"**{block['title']}**")
                    lines.append("")
                for key, value in (block["kv"] or {}).items():
                    lines.append(f"- {key}：{value}")
                lines.append("")
            elif "table" in block:
                if block.get("title"):
                    lines.append(f"**{block['title']}**")
                    lines.append("")
                lines.append(
                    to_markdown_table(
                        block["table"],
                        columns=block.get("columns"),
                        limit=int(block.get("limit", 0) or 0),
                    )
                )
                lines.append("")
            elif "text" in block:
                lines.append(str(block["text"]))
                lines.append("")

        text = "\n".join(lines).rstrip() + "\n"
        return {"text": text, "length": len(text)}


@op("report.html")
class HtmlReport(Operator):
    """把 Markdown 风格块渲染成单文件 HTML（不依赖任何前端库）。"""

    description = "渲染单文件 HTML 报告"
    outputs = [("text", str), ("length", int)]
    params = {
        "title": Param(str),
        "blocks": Param(list, default=None),
        "subtitle": Param(str, default=""),
    }

    def run(self, ctx, title, blocks, subtitle):
        sections: list[str] = []
        for block in blocks or []:
            if "heading" in block:
                level = max(2, min(4, int(block.get("level", 2))))
                sections.append(f"<h{level}>{html.escape(str(block['heading']))}</h{level}>")
            elif "kv" in block:
                items = "".join(
                    f"<li><span>{html.escape(str(k))}</span><strong>{html.escape(str(v))}</strong></li>"
                    for k, v in (block["kv"] or {}).items()
                )
                heading = f"<h3>{html.escape(str(block['title']))}</h3>" if block.get("title") else ""
                sections.append(f'{heading}<ul class="kv">{items}</ul>')
            elif "table" in block:
                rows = list(block["table"] or [])
                cols = list(block.get("columns") or (rows[0].keys() if rows else []))
                head = "".join(f"<th>{html.escape(str(c))}</th>" for c in cols)
                body = "".join(
                    "<tr>" + "".join(f"<td>{html.escape(_cell(r.get(c)))}</td>" for c in cols) + "</tr>"
                    for r in rows
                )
                heading = f"<h3>{html.escape(str(block['title']))}</h3>" if block.get("title") else ""
                sections.append(
                    f'{heading}<table><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table>'
                )
            elif "text" in block:
                sections.append(f"<p>{html.escape(str(block['text']))}</p>")

        doc = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<title>{html.escape(title)}</title>
<style>
  :root {{ color-scheme: light; }}
  body {{ margin: 0; padding: 40px 24px; background: #f6f7f9;
         font-family: -apple-system, "Segoe UI", "Microsoft YaHei", sans-serif; color: #1f2328; }}
  .sheet {{ max-width: 880px; margin: 0 auto; background: #fff; border-radius: 14px;
            padding: 36px 40px; box-shadow: 0 6px 28px rgba(16,24,40,.08); }}
  h1 {{ font-size: 26px; margin: 0 0 6px; }}
  .sub {{ color: #6b7280; font-size: 13px; margin-bottom: 26px; }}
  h3 {{ font-size: 15px; margin: 26px 0 10px; color: #0f766e; }}
  ul.kv {{ list-style: none; padding: 0; margin: 0; display: grid;
           grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 10px; }}
  ul.kv li {{ background: #f2f7f6; border-radius: 8px; padding: 10px 12px;
              display: flex; justify-content: space-between; gap: 12px; font-size: 14px; }}
  ul.kv span {{ color: #55605e; }}
  table {{ width: 100%; border-collapse: collapse; font-size: 13px; margin-top: 6px; }}
  th {{ background: #0f766e; color: #fff; text-align: left; padding: 9px 10px; font-weight: 600; }}
  td {{ padding: 8px 10px; border-bottom: 1px solid #eceff1; }}
  tbody tr:nth-child(even) {{ background: #fafbfc; }}
  p {{ font-size: 14px; line-height: 1.7; color: #374151; }}
  footer {{ margin-top: 30px; font-size: 12px; color: #9ca3af; }}
</style>
</head>
<body>
  <div class="sheet">
    <h1>{html.escape(title)}</h1>
    <div class="sub">{html.escape(subtitle) or "生成于 " + ctx.run_id}</div>
    {''.join(sections)}
    <footer>op_pipeline · {html.escape(ctx.pipeline_name if hasattr(ctx, 'pipeline_name') else ctx.run.pipeline)}</footer>
  </div>
</body>
</html>
"""
        return {"text": doc, "length": len(doc)}
