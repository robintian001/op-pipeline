"""Parquet / Arrow 算子：``list[dict]`` 行表与 pyarrow Table / Parquet 文件双向转换（可选 extras）。

pyarrow 是可选依赖（``pip install op-pipeline[parquet]``），只在 ``run()`` 内延迟导入：
未安装时本模块照常导入、算子照常注册，调用对应算子才报清晰错误。
"""
from __future__ import annotations

from typing import Any

from ..operator import Operator
from ..params import Param
from ..registry import op


def _pyarrow():
    try:
        import pyarrow as pa
        import pyarrow.parquet  # noqa: F401  # 显式导入子模块，pa.parquet 才可用
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            "parquet/arrow 算子需要 pyarrow，请安装：pip install op-pipeline[parquet]"
        ) from exc
    return pa


@op("df.from_arrow", aliases=["from_arrow"])
class FromArrow(Operator):
    """把行字典表转成 pyarrow Table。"""

    description = "行表转 pyarrow Table"
    inputs = ["table"]
    outputs = [("arrow", Any), ("rows", int), ("columns", list)]
    params = {
        "columns": Param(list, default=None, doc="指定列顺序；None 表示按行数据键序推断"),
    }

    def run(self, ctx, table, columns):
        pa = _pyarrow()
        rows = list(table or [])
        if columns:
            rows = [{k: row.get(k) for k in columns} for row in rows]
        arrow = pa.Table.from_pylist(rows)
        ctx.log(f"行表 {len(rows)} 行 -> Arrow Table {arrow.num_rows} 行 / {arrow.num_columns} 列")
        return {"arrow": arrow, "rows": len(rows), "columns": list(arrow.column_names)}


@op("df.to_arrow", aliases=["to_arrow"])
class ToArrow(Operator):
    """把 pyarrow Table 转回行字典表。"""

    description = "pyarrow Table 转行表"
    inputs = [("arrow", Any)]
    outputs = [("table", list), ("rows", int)]

    def run(self, ctx, arrow):
        pa = _pyarrow()
        if not isinstance(arrow, pa.Table):
            raise TypeError(f"输入不是 pyarrow Table：{type(arrow).__name__}")
        rows = arrow.to_pylist()
        ctx.log(f"Arrow Table {arrow.num_rows} 行 -> 行表 {len(rows)} 行")
        return {"table": rows, "rows": len(rows)}


@op("io.read_parquet", aliases=["read_parquet"])
class ReadParquet(Operator):
    """读取 Parquet 文件为「行字典表」（list[dict]）。"""

    description = "读取 Parquet 文件为行表"
    outputs = [("table", list), ("rows", int), ("path", str)]
    params = {
        "path": Param(str, doc="Parquet 路径，相对工作目录"),
    }

    def run(self, ctx, path):
        pa = _pyarrow()
        target = ctx.resolve_path(path)
        if not target.exists():
            raise FileNotFoundError(f"Parquet 不存在：{target}")
        rows = pa.parquet.read_table(str(target)).to_pylist()
        ctx.log(f"读取 {len(rows)} 行 <- {target}")
        return {"table": rows, "rows": len(rows), "path": str(target)}


@op("io.write_parquet", aliases=["write_parquet"])
class WriteParquet(Operator):
    """把行字典表写成 Parquet。"""

    description = "行字典表写出 Parquet"
    inputs = ["table"]
    outputs = [("path", str), ("rows", int)]
    params = {
        "path": Param(str, doc="输出 Parquet 路径，相对工作目录"),
        "compression": Param(str, default="snappy", doc="压缩算法（snappy/gzip/zstd 等）"),
    }

    def run(self, ctx, table, path, compression):
        pa = _pyarrow()
        rows = list(table or [])
        target = ctx.resolve_path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        arrow = pa.Table.from_pylist(rows)
        pa.parquet.write_table(arrow, str(target), compression=compression)
        ctx.log(f"写出 {len(rows)} 行 -> {target}")
        return {"path": str(target), "rows": len(rows)}
