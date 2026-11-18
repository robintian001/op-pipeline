"""Excel 算子：``list[dict]`` 行表与 .xlsx 文件双向转换（可选 extras，openpyxl）。

openpyxl 是可选依赖（``pip install op-pipeline[excel]``），只在 ``run()`` 内延迟导入：
未安装时本模块照常导入、算子照常注册，调用对应算子才报清晰错误。
"""
from __future__ import annotations

from typing import Any

from ..operator import Operator
from ..params import Param
from ..registry import op


def _openpyxl():
    try:
        import openpyxl
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            "excel 算子需要 openpyxl，请安装：pip install op-pipeline[excel]"
        ) from exc
    return openpyxl


def _write_rows(ws, rows: list[dict[str, Any]]) -> int:
    """把行表按「首行键序」写入工作表，返回写入行数。"""
    rows = list(rows or [])
    header = list(rows[0].keys()) if rows else []
    if header:
        ws.append(header)
    for row in rows:
        ws.append([row.get(k) for k in header])
    return len(rows)


def _read_rows(ws) -> list[dict[str, Any]]:
    """把工作表读成行表：首行作表头，其余行逐行映射。"""
    values = list(ws.iter_rows(values_only=True))
    if not values:
        return []
    header = [str(cell) for cell in values[0]]
    rows: list[dict[str, Any]] = []
    for record in values[1:]:
        rows.append({k: v for k, v in zip(header, record)})
    return rows


@op("io.write_excel", aliases=["write_excel"])
class WriteExcel(Operator):
    """把行字典表写成 .xlsx；支持经 ``sheets`` 一次性写多 sheet。"""

    description = "行字典表写出 Excel"
    inputs = [("table", list, True), ("sheets", dict, True)]
    outputs = [("path", str), ("rows", int)]
    params = {
        "path": Param(str, doc="输出 .xlsx 路径，相对工作目录"),
        "sheet": Param(str, default="Sheet1", doc="写 table 时使用的工作表名"),
    }

    def run(self, ctx, table, sheets, path, sheet):
        openpyxl = _openpyxl()
        sheets = dict(sheets or {})
        total = 0
        if sheets:
            workbook = openpyxl.Workbook()
            for index, (name, rows) in enumerate(sheets.items()):
                ws = workbook.active if index == 0 else workbook.create_sheet(name)
                ws.title = name
                total += _write_rows(ws, rows)
        elif table is not None:
            workbook = openpyxl.Workbook()
            workbook.active.title = sheet
            total = _write_rows(workbook.active, table)
        else:
            raise ValueError("io.write_excel 需要提供 table 或 sheets 至少一个")
        target = ctx.resolve_path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        workbook.save(str(target))
        ctx.log(f"写出 {total} 行 Excel -> {target}")
        return {"path": str(target), "rows": total}


@op("io.read_excel", aliases=["read_excel"])
class ReadExcel(Operator):
    """读取 .xlsx 的指定工作表为「行字典表」（list[dict]）。"""

    description = "读取 Excel 工作表为行表"
    outputs = [("table", list), ("rows", int), ("path", str)]
    params = {
        "path": Param(str, doc=".xlsx 路径，相对工作目录"),
        "sheet": Param(str, default=None, doc="工作表名；None 表示活动（首个）工作表"),
    }

    def run(self, ctx, path, sheet):
        openpyxl = _openpyxl()
        target = ctx.resolve_path(path)
        if not target.exists():
            raise FileNotFoundError(f"Excel 不存在：{target}")
        workbook = openpyxl.load_workbook(str(target), read_only=True, data_only=True)
        try:
            ws = workbook[sheet] if sheet else workbook.active
            rows = _read_rows(ws)
        finally:
            workbook.close()
        ctx.log(f"读取 {len(rows)} 行 <- {target}（sheet={sheet or ws.title}）")
        return {"table": rows, "rows": len(rows), "path": str(target)}
