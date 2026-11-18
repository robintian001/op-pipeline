"""表算子：以 ``list[dict]`` 为表的通用数据处理。

用行字典而不是 DataFrame 是刻意选择：零依赖、可序列化、便于作为算子间的
"通用交换格式"。需要 pandas 时，只要写一个 adapter 算子把表转成 DataFrame，
编排层不用改。
"""
from __future__ import annotations

from typing import Any, Iterable

from ..expression import evaluate
from ..operator import Operator
from ..params import Param
from ..registry import op


def row_env(row: dict[str, Any], index: int | None = None) -> dict[str, Any]:
    """构造行级表达式环境：字段名可直接引用，也可用 ``row["x"]``。"""
    env = {k: v for k, v in row.items() if isinstance(k, str) and k.isidentifier()}
    env["row"] = row
    if index is not None:
        env["i"] = index
    return env


def table_columns(rows: Iterable[dict[str, Any]]) -> list[str]:
    """全表列集合并集（按首次出现顺序）。

    取代"首行代表全表列"假设（T15）：列参差的行（某列只在非首行出现）不再丢字段或误报。
    """
    seen: dict[str, None] = {}
    for row in rows:
        for key in row:
            if key not in seen:
                seen[key] = None
    return list(seen)


@op("table.filter_rows", aliases=["filter"])
class FilterRows(Operator):
    """按表达式过滤行，如 ``amount > 0 and region != "north"``。"""

    description = "按表达式过滤行"
    inputs = ["table"]
    outputs = [("table", list), ("dropped", int)]
    params = {"where": Param(str, doc="行级布尔表达式")}

    def run(self, ctx, table, where):
        source = list(table or [])
        kept = [row for i, row in enumerate(source) if evaluate(where, row_env(row, i))]
        ctx.log(f"过滤 {len(source)} -> {len(kept)} 行")
        return {"table": kept, "dropped": len(source) - len(kept)}


@op("table.select_columns", aliases=["select"])
class SelectColumns(Operator):
    """选择列（投影）。"""

    description = "选择列"
    inputs = ["table"]
    outputs = [("table", list)]
    params = {
        "columns": Param(list, doc="要保留的列名列表"),
        "missing": Param(str, default="error", choices=("error", "ignore")),
    }

    def run(self, ctx, table, columns, missing):
        rows = list(table or [])
        if not rows:
            return {"table": []}
        available = set(table_columns(rows))
        unknown = [c for c in columns if c not in available]
        if unknown and missing == "error":
            raise KeyError(f"表中不存在列：{', '.join(map(str, unknown))}；可用：{', '.join(sorted(available))}")
        cols = [c for c in columns if c in available]
        return {"table": [{c: row.get(c) for c in cols} for row in rows]}


@op("table.rename_columns")
class RenameColumns(Operator):
    """列重命名。"""

    description = "列重命名"
    inputs = ["table"]
    outputs = [("table", list)]
    params = {"mapping": Param(dict, doc="旧列名 -> 新列名")}

    def run(self, ctx, table, mapping):
        rows = list(table or [])
        renamed = [
            {mapping.get(k, k): v for k, v in row.items()}
            for row in rows
        ]
        return {"table": renamed}


@op("table.drop_columns")
class DropColumns(Operator):
    """丢弃列。"""

    description = "丢弃列"
    inputs = ["table"]
    outputs = [("table", list)]
    params = {"columns": Param(list)}

    def run(self, ctx, table, columns):
        drop = set(columns)
        return {"table": [{k: v for k, v in row.items() if k not in drop} for row in (table or [])]}


@op("table.derive_column")
class DeriveColumn(Operator):
    """用表达式新增/覆盖一列。"""

    description = "按表达式派生新列"
    inputs = ["table"]
    outputs = [("table", list)]
    params = {
        "name": Param(str, doc="新列名"),
        "expr": Param(str, doc="行级表达式"),
        "overwrite": Param(bool, default=True),
    }

    def run(self, ctx, table, name, expr, overwrite):
        rows = list(table or [])
        if rows and name in table_columns(rows) and not overwrite:
            raise ValueError(f"列 '{name}' 已存在；如需覆盖请设置 overwrite=true")
        result = []
        for i, row in enumerate(rows):
            new_row = dict(row)
            new_row[name] = evaluate(expr, row_env(row, i))
            result.append(new_row)
        return {"table": result}


@op("table.sort_rows")
class SortRows(Operator):
    """排序；None 值统一排在最后（升序时）。"""

    description = "按列排序"
    inputs = ["table"]
    outputs = [("table", list)]
    params = {
        "by": Param((str, list), doc="排序列，单列名或列表"),
        "desc": Param(bool, default=False),
    }

    def run(self, ctx, table, by, desc):
        rows = list(table or [])
        keys = [by] if isinstance(by, str) else list(by)
        if not keys:
            return {"table": rows}

        cols = table_columns(rows)
        missing = [k for k in keys if cols and k not in cols]
        if missing:
            raise KeyError(f"排序键不存在：{', '.join(missing)}")

        def sort_key(row: dict[str, Any]) -> tuple:
            values = []
            for key in keys:
                value = row.get(key)
                # (是否为 None, 归一化值) 避免 None 与字符串比较报错
                values.append((value is None, _normalize(value)))
            return tuple(values)

        return {"table": sorted(rows, key=sort_key, reverse=bool(desc))}


def _normalize(value: Any) -> Any:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float, str)):
        return value
    return str(value)


@op("table.dedupe")
class Dedupe(Operator):
    """去重，可按一组键或整行判重。"""

    description = "去重"
    inputs = ["table"]
    outputs = [("table", list), ("dropped", int)]
    params = {
        "keys": Param(list, default=None, doc="判重列；None 表示整行"),
        "keep": Param(str, default="first", choices=("first", "last")),
    }

    def run(self, ctx, table, keys, keep):
        rows = list(table or [])
        seen: dict[Any, int] = {}
        out: list[dict[str, Any]] = []
        for row in rows:
            signature = tuple(row.get(k) for k in keys) if keys else tuple(sorted(row.items(), key=lambda kv: str(kv[0])))
            if signature in seen:
                if keep == "last":
                    out[seen[signature]] = row
                continue
            seen[signature] = len(out)
            out.append(row)
        ctx.log(f"去重 {len(rows)} -> {len(out)}")
        return {"table": out, "dropped": len(rows) - len(out)}


@op("table.limit")
class Limit(Operator):
    """切片。"""

    description = "截取行区间"
    inputs = ["table"]
    outputs = [("table", list)]
    params = {
        "n": Param(int, doc="保留行数"),
        "offset": Param(int, default=0, minimum=0),
    }

    def run(self, ctx, table, n, offset):
        rows = list(table or [])
        return {"table": rows[offset: offset + n]}


@op("table.join")
class Join(Operator):
    """两表哈希连接。"""

    description = "按键连接两张表"
    inputs = [("left", list), ("right", list)]
    outputs = [("table", list), ("matched", int)]
    params = {
        "left_key": Param(str, doc="左表连接键"),
        "right_key": Param(str, default=None, doc="右表连接键，缺省与 left_key 相同"),
        "how": Param(str, default="inner", choices=("inner", "left", "right", "full")),
        "suffix": Param(str, default="_right", doc="右表同名列的后缀"),
    }

    def run(self, ctx, left, right, left_key, right_key, how, suffix):
        left_rows = list(left or [])
        right_rows = list(right or [])
        rkey = right_key or left_key

        index: dict[Any, list[dict[str, Any]]] = {}
        for row in right_rows:
            index.setdefault(row.get(rkey), []).append(row)

        right_columns = sorted({k for row in right_rows for k in row}) if right_rows else []
        out: list[dict[str, Any]] = []
        matched_right: set[int] = set()

        for lrow in left_rows:
            matches = index.get(lrow.get(left_key), [])
            if not matches:
                if how in ("left", "full"):
                    merged = dict(lrow)
                    for col in right_columns:
                        merged.setdefault(col, None)
                    out.append(merged)
                continue
            for rrow in matches:
                matched_right.add(id(rrow))
                merged = dict(lrow)
                for key, value in rrow.items():
                    if key == rkey and rkey == left_key:
                        continue
                    merged[key + suffix if key in merged else key] = value
                out.append(merged)

        if how in ("right", "full"):
            left_columns = table_columns(left_rows)
            for rrow in right_rows:
                if id(rrow) not in matched_right:
                    merged = {col: None for col in left_columns}
                    merged.update(rrow)
                    out.append(merged)

        ctx.log(f"连接 {len(left_rows)} x {len(right_rows)} -> {len(out)} 行（{how}）")
        return {"table": out, "matched": len(out)}


@op("table.union")
class Union(Operator):
    """纵向合并两张表。"""

    description = "纵向合并"
    inputs = [("left", list), ("right", list)]
    outputs = [("table", list)]
    params = {"distinct": Param(bool, default=False, doc="是否去掉完全重复行")}

    def run(self, ctx, left, right, distinct):
        rows = list(left or []) + list(right or [])
        if not distinct:
            return {"table": rows}
        seen: set[tuple] = set()
        out = []
        for row in rows:
            signature = tuple(sorted(row.items(), key=lambda kv: str(kv[0])))
            if signature in seen:
                continue
            seen.add(signature)
            out.append(row)
        return {"table": out}


@op("table.take_column")
class TakeColumn(Operator):
    """取单列成列表（便于下游做统计/文本处理）。"""

    description = "取单列为列表"
    inputs = ["table"]
    outputs = [("values", list), ("count", int)]
    params = {"column": Param(str), "skip_null": Param(bool, default=True)}

    def run(self, ctx, table, column, skip_null):
        values = [row.get(column) for row in (table or [])]
        if skip_null:
            values = [v for v in values if v is not None]
        return {"values": values, "count": len(values)}
