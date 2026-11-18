"""DataFrame 算子：``list[dict]`` 行表与 pandas DataFrame 双向转换（可选 extras）。

pandas 是可选依赖（``pip install op-pipeline[dataframe]``），只在 ``run()`` 内延迟导入：
未安装时本模块照常导入、算子照常注册，调用对应算子才报清晰错误。
"""
from __future__ import annotations

from typing import Any

from ..operator import Operator
from ..params import Param
from ..registry import op
from .table_ops import table_columns


def _pandas():
    try:
        import pandas
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            "df.* 算子需要 pandas，请安装：pip install op-pipeline[dataframe]"
        ) from exc
    return pandas


@op("df.from_table", aliases=["from_table"])
class FromTable(Operator):
    """把行字典表转成 pandas DataFrame。"""

    description = "行表转 DataFrame"
    inputs = ["table"]
    outputs = [("df", Any), ("shape", tuple)]
    params = {
        "columns": Param(list, default=None, doc="指定列顺序；None 表示全表列集合并集顺序"),
        "index_column": Param(str, default=None, doc="作为 DataFrame index 的列名"),
    }

    def run(self, ctx, table, columns, index_column):
        pd = _pandas()
        rows = list(table or [])
        cols = columns or table_columns(rows)
        df = pd.DataFrame.from_records(rows, columns=cols)
        if index_column and index_column in df.columns:
            df = df.set_index(index_column)
        ctx.log(f"行表 {len(rows)} 行 -> DataFrame {tuple(df.shape)}")
        return {"df": df, "shape": tuple(df.shape)}


@op("df.to_table", aliases=["to_table"])
class ToTable(Operator):
    """把 pandas DataFrame 转回行字典表（空值 NaN/NaT 还原为 None）。"""

    description = "DataFrame 转行表"
    inputs = [("df", Any)]
    outputs = [("table", list), ("rows", int), ("columns", list)]
    params = {
        "drop_index": Param(bool, default=True, doc="是否丢弃 DataFrame index"),
        "column_order": Param(list, default=None, doc="指定列顺序；None 表示保持 DataFrame 列序"),
    }

    def run(self, ctx, df, drop_index, column_order):
        pd = _pandas()
        frame = df.reset_index(drop=True) if drop_index else df
        columns = list(column_order) if column_order else list(frame.columns)
        rows: list[dict[str, Any]] = []
        for record in frame.to_dict("records"):
            row = {k: (_clean(v) if not _is_missing(v) else None) for k, v in record.items() if k in columns}
            rows.append(row)
        ctx.log(f"DataFrame {tuple(frame.shape)} -> 行表 {len(rows)} 行")
        return {"table": rows, "rows": len(rows), "columns": columns}


@op("df.to_csv", aliases=["to_csv"])
class ToCsv(Operator):
    """把 pandas DataFrame 写出 CSV（可带行索引列）。"""

    description = "DataFrame 写出 CSV"
    inputs = [("df", Any)]
    outputs = [("path", str), ("rows", int)]
    params = {
        "path": Param(str, doc="输出 CSV 路径，相对工作目录"),
        "index": Param(bool, default=False, doc="是否写入行索引列"),
        "encoding": Param(str, default="utf-8"),
    }

    def run(self, ctx, df, path, index, encoding):
        pd = _pandas()
        target = ctx.resolve_path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(target, index=index, encoding=encoding)
        ctx.log(f"DataFrame {tuple(df.shape)} -> {target}")
        return {"path": str(target), "rows": len(df)}


def _is_missing(value: Any) -> bool:
    """pandas 空值（NaN/NaT）判定：NaN 不等于自身，NaT 同理。"""
    try:
        return bool(value != value)
    except (TypeError, ValueError):
        return False


def _clean(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return value
    if hasattr(value, "item"):  # numpy 标量转原生 Python 类型
        try:
            return value.item()
        except (ValueError, TypeError):
            return value
    return value
