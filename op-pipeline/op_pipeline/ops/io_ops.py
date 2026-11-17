"""IO 算子：把输入输出收进算子层，流水线只谈数据流不谈文件细节。"""
from __future__ import annotations

import csv
import json
from typing import Any

from ..operator import Operator
from ..params import Param
from ..registry import op


def _infer(text: str) -> Any:
    """把 CSV 的字符串单元格尽量还原成数字/布尔/空值。"""
    if text is None:
        return None
    raw = text.strip()
    if raw == "":
        return None
    low = raw.lower()
    if low in ("true", "false"):
        return low == "true"
    try:
        return int(raw)
    except ValueError:
        pass
    try:
        return float(raw)
    except ValueError:
        return text


@op("io.read_csv", aliases=["read_csv"])
class ReadCsv(Operator):
    """读取 CSV 为「行字典表」（list[dict]）。"""

    description = "读取 CSV 为行字典表"
    outputs = [("table", list), ("rows", int), ("path", str)]
    params = {
        "path": Param(str, doc="CSV 路径，相对工作目录"),
        "delimiter": Param(str, default=","),
        "encoding": Param(str, default="utf-8-sig"),
        "infer_types": Param(bool, default=True, doc="是否把数字/布尔单元格还原为对应类型"),
        "sample": Param(int, default=0, doc="只读前 N 行，0 表示全量", minimum=0),
    }

    def run(self, ctx, path, delimiter, encoding, infer_types, sample):
        target = ctx.resolve_path(path)
        if not target.exists():
            raise FileNotFoundError(f"CSV 不存在：{target}")
        rows: list[dict[str, Any]] = []
        with target.open("r", encoding=encoding, newline="") as fh:
            reader = csv.DictReader(fh, delimiter=delimiter)
            for index, record in enumerate(reader):
                if sample and index >= sample:
                    break
                rows.append(
                    {k: (_infer(v) if infer_types else v) for k, v in record.items() if k is not None}
                )
        ctx.log(f"读取 {len(rows)} 行 <- {target}")
        return {"table": rows, "rows": len(rows), "path": str(target)}


@op("io.write_csv", aliases=["write_csv"])
class WriteCsv(Operator):
    """把行字典表写成 CSV。"""

    description = "行字典表写出 CSV"
    inputs = ["table"]
    outputs = [("path", str), ("rows", int)]
    params = {
        "path": Param(str, doc="输出 CSV 路径，相对工作目录"),
        "columns": Param(list, default=None, doc="指定列顺序；None 表示取首行键序"),
        "encoding": Param(str, default="utf-8-sig"),
        "delimiter": Param(str, default=","),
    }

    def run(self, ctx, table, path, columns, encoding, delimiter):
        rows = list(table or [])
        header = list(columns) if columns else (list(rows[0].keys()) if rows else [])
        target = ctx.resolve_path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open("w", encoding=encoding, newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=header, delimiter=delimiter, extrasaction="ignore")
            writer.writeheader()
            for row in rows:
                writer.writerow({k: row.get(k, "") for k in header})
        ctx.log(f"写出 {len(rows)} 行 -> {target}")
        return {"path": str(target), "rows": len(rows)}


@op("io.read_json", aliases=["read_json"])
class ReadJson(Operator):
    """读取 JSON；若顶层是 list[dict] 则同时给出行表视图。"""

    description = "读取 JSON 文件"
    outputs = [("data", Any), ("table", list, True), ("path", str)]
    params = {
        "path": Param(str),
        "encoding": Param(str, default="utf-8"),
    }

    def run(self, ctx, path, encoding):
        target = ctx.resolve_path(path)
        if not target.exists():
            raise FileNotFoundError(f"JSON 不存在：{target}")
        data = json.loads(target.read_text(encoding=encoding))
        table = data if isinstance(data, list) and all(isinstance(r, dict) for r in data) else None
        ctx.log(f"读取 JSON -> {target}")
        return {"data": data, "table": table, "path": str(target)}


@op("io.write_json", aliases=["write_json"])
class WriteJson(Operator):
    """把任意可序列化对象写成 JSON。"""

    description = "写出 JSON 文件"
    inputs = [("data", Any)]
    outputs = [("path", str), ("bytes", int)]
    params = {
        "path": Param(str),
        "indent": Param(int, default=2, minimum=0, maximum=8),
        "ensure_ascii": Param(bool, default=False),
    }

    def run(self, ctx, data, path, indent, ensure_ascii):
        target = ctx.resolve_path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        text = json.dumps(data, ensure_ascii=ensure_ascii, indent=indent, default=str)
        target.write_text(text, encoding="utf-8")
        ctx.log(f"写出 JSON -> {target}（{len(text)} 字符）")
        return {"path": str(target), "bytes": len(text)}


@op("io.read_text", aliases=["read_text"])
class ReadText(Operator):
    """读取纯文本。"""

    description = "读取文本文件"
    outputs = [("text", str), ("length", int)]
    params = {
        "path": Param(str),
        "encoding": Param(str, default="utf-8"),
    }

    def run(self, ctx, path, encoding):
        target = ctx.resolve_path(path)
        if not target.exists():
            raise FileNotFoundError(f"文本不存在：{target}")
        text = target.read_text(encoding=encoding)
        return {"text": text, "length": len(text)}


@op("io.write_text", aliases=["write_text"])
class WriteText(Operator):
    """写出纯文本。"""

    description = "写出文本文件"
    inputs = [("text", str)]
    outputs = [("path", str), ("length", int)]
    params = {"path": Param(str), "encoding": Param(str, default="utf-8")}

    def run(self, ctx, text, path, encoding):
        target = ctx.resolve_path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(str(text), encoding=encoding)
        return {"path": str(target), "length": len(str(text))}
