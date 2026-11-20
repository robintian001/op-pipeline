"""表算子：以 ``list[dict]`` 为表的通用数据处理。

用行字典而不是 DataFrame 是刻意选择：零依赖、可序列化、便于作为算子间的
"通用交换格式"。需要 pandas 时，只要写一个 adapter 算子把表转成 DataFrame，
编排层不用改。
"""
from __future__ import annotations

import re
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


def _resolve_columns(rows: list[dict[str, Any]], columns: list[str] | None) -> list[str]:
    """解析目标列：None 表示全部列（所有行键并集）；显式列名必须存在于全表列集。"""
    all_cols = table_columns(rows)
    if columns is None:
        return all_cols
    missing = [c for c in columns if c not in all_cols]
    if missing:
        raise KeyError(
            f"表中不存在列：{', '.join(map(str, missing))}；可用：{', '.join(sorted(map(str, all_cols)))}"
        )
    return list(columns)


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


@op("table.explode")
class Explode(Operator):
    """把 list 列展开成多行：每个元素一行，其余列复制（列值不变的"逆聚合"）。

    - 列不存在或某行该列的值不是 list 时报错（KeyError/ValueError）；
    - ``keep_empty=false``（默认）：空 list 的行被丢弃；
    - ``keep_empty=true``：空 list 保留一行、该列值置 ``None``；
    - ``index_column`` 给出时，展开出的每行写入原始行索引（int，从 0 起）。
    """

    description = "把 list 列展开成多行"
    inputs = ["table"]
    outputs = [("table", list), ("rows", int)]
    params = {
        "column": Param(str, doc="要展开的列，其值必须是 list"),
        "index_column": Param(str, default=None, doc="给出时写入原始行索引（int）"),
        "keep_empty": Param(bool, default=False, doc="空 list 行为：false 丢弃，true 保留一行且该列为 None"),
    }

    def run(self, ctx, table, column, index_column, keep_empty):
        rows = list(table or [])
        out: list[dict[str, Any]] = []
        for i, row in enumerate(rows):
            if column not in row:
                raise KeyError(f"表中不存在列 '{column}'（第 {i} 行）；可用列：{', '.join(sorted(row))}")
            values = row[column]
            if not isinstance(values, list):
                raise ValueError(
                    f"列 '{column}' 第 {i} 行的值不是 list（实际 {type(values).__name__}），无法展开"
                )
            if not values and keep_empty:
                new_row = dict(row)
                new_row[column] = None
                if index_column:
                    new_row[index_column] = i
                out.append(new_row)
                continue
            for element in values:
                new_row = dict(row)
                new_row[column] = element
                if index_column:
                    new_row[index_column] = i
                out.append(new_row)
        ctx.log(f"展开列 '{column}'：{len(rows)} -> {len(out)} 行")
        return {"table": out, "rows": len(out)}


@op("table.fill_null")
class FillNull(Operator):
    """填充 None 单元格。

    - 只处理「行内存在该键且值为 None」的单元格；行内缺键不处理也不计数，
      空串 / 0 / False 均不算缺失；
    - ``columns``：目标列，None 表示全部列（所有行键的并集），
      指定列不在全表列集时抛 KeyError；
    - ``filled`` 为本次填充的单元格数；输入行不原地修改。
    """

    description = "填充 None 单元格"
    inputs = ["table"]
    outputs = [("table", list), ("filled", int)]
    params = {
        "value": Param(Any, doc="填充值"),
        "columns": Param(list, default=None, doc="目标列；None 表示全部列"),
    }

    def run(self, ctx, table, value, columns):
        rows = list(table or [])
        target = _resolve_columns(rows, columns)
        filled = 0
        out: list[dict[str, Any]] = []
        for row in rows:
            new_row = dict(row)
            for col in target:
                if col in new_row and new_row[col] is None:
                    new_row[col] = value
                    filled += 1
            out.append(new_row)
        ctx.log(f"填充 {filled} 个 None 单元格")
        return {"table": out, "filled": filled}


@op("table.replace")
class Replace(Operator):
    """按映射替换单元格值。

    - 普通模式：单元格值与映射键相等（==）即替换为对应值；
    - ``regex=true``：仅对 str 单元格生效，映射键作为正则 pattern
      做全量替换（re.sub），替换结果与原值相同则不计数；
    - ``columns``：目标列，None 表示全部列；指定列不存在抛 KeyError；
    - ``replaced`` 为值被改写的单元格数；输入行不原地修改。
    """

    description = "按映射替换单元格值"
    inputs = ["table"]
    outputs = [("table", list), ("replaced", int)]
    params = {
        "mapping": Param(dict, doc="旧值 -> 新值映射；regex 模式下为 pattern -> 替换文本"),
        "columns": Param(list, default=None, doc="目标列；None 表示全部列"),
        "regex": Param(bool, default=False, doc="是否按正则全量替换（仅对 str 单元格生效）"),
    }

    def run(self, ctx, table, mapping, columns, regex):
        rows = list(table or [])
        target = _resolve_columns(rows, columns)
        pairs = list(mapping.items())
        replaced = 0
        out: list[dict[str, Any]] = []
        for row in rows:
            new_row = dict(row)
            for col in target:
                if col not in new_row:
                    continue
                old = new_row[col]
                if regex:
                    if not isinstance(old, str):
                        continue
                    new = old
                    for pattern, repl in pairs:
                        new = re.sub(pattern, repl, new)
                    if new != old:
                        new_row[col] = new
                        replaced += 1
                    continue
                for old_key, new_value in pairs:
                    if old == old_key:
                        new_row[col] = new_value
                        replaced += 1
                        break
            out.append(new_row)
        ctx.log(f"替换 {replaced} 个单元格")
        return {"table": out, "replaced": replaced}


def _pivot_agg(agg: str | None, values: list[Any]) -> Any:
    """透视单元格聚合：语义沿用 stats 组惯例。

    sum/mean 仅对 int/float 求值（bool 排除），组内无数值时结果为 None；
    count 统计非 None 个数；first/last 按出现顺序取首个/末个非 None 值。
    """
    if agg is None:
        return values[0]
    if agg == "count":
        return sum(1 for v in values if v is not None)
    if agg == "first":
        return next((v for v in values if v is not None), None)
    if agg == "last":
        return next((v for v in reversed(values) if v is not None), None)
    nums = [v for v in values if isinstance(v, (int, float)) and not isinstance(v, bool)]
    if not nums:
        return None
    if agg == "sum":
        return sum(nums)
    return sum(nums) / len(nums)  # mean


@op("table.pivot")
class Pivot(Operator):
    """长表转宽表：index 列标识行，columns 列的取值变成新列名，values 列为单元格值。

    - ``index``：行标识列，单列名或列表（输出时排在前，多列按声明顺序）；
    - ``agg``：None 时存在重复 (index值, 键值) 组合即抛 ValueError（含重复样例）；
      可选 first/last/sum/count/mean，语义见 ``_pivot_agg``；
    - 键列名为 str(键值) 且按字典序排布；行按 index 值的 str 排序。
    """

    description = "长表转宽表"
    inputs = ["table"]
    outputs = [("table", list), ("rows", int)]
    params = {
        "index": Param((str, list), doc="行标识列，单列名或列表"),
        "columns": Param(str, doc="键列，其取值展开为新列名"),
        "values": Param(str, doc="值列"),
        "agg": Param(str, default=None, choices=("first", "last", "sum", "count", "mean"),
                     doc="聚合方式；None 表示不允许重复组合"),
    }

    def run(self, ctx, table, index, columns, values, agg):
        rows = list(table or [])
        if not rows:
            return {"table": [], "rows": 0}
        index_cols = [index] if isinstance(index, str) else list(index)
        available = table_columns(rows)
        missing = [c for c in (*index_cols, columns, values) if c not in available]
        if missing:
            raise KeyError(
                f"表中不存在列：{', '.join(map(str, missing))}；可用：{', '.join(sorted(map(str, available)))}"
            )

        groups: dict[tuple, dict[Any, list[Any]]] = {}
        for row in rows:
            key = tuple(row.get(c) for c in index_cols)
            bucket = groups.setdefault(key, {})
            bucket.setdefault(row.get(columns), []).append(row.get(values))

        if agg is None:
            for key, bucket in groups.items():
                for col_key, cell_values in bucket.items():
                    if len(cell_values) > 1:
                        sample = dict(zip(index_cols, key))
                        raise ValueError(
                            f"agg=None 时存在重复的 (index, {columns}) 组合："
                            f"index={sample}, {columns}={col_key!r} 出现 {len(cell_values)} 次；"
                            "请指定 agg（first/last/sum/count/mean）或先对表去重"
                        )

        out: list[dict[str, Any]] = []
        for key in sorted(groups, key=lambda k: tuple(str(v) for v in k)):
            record: dict[str, Any] = dict(zip(index_cols, key))
            for col_key in sorted(groups[key], key=str):
                record[str(col_key)] = _pivot_agg(agg, groups[key][col_key])
            out.append(record)
        ctx.log(f"透视 {len(rows)} 行 -> {len(out)} 行")
        return {"table": out, "rows": len(out)}


@op("table.unpivot")
class Unpivot(Operator):
    """宽表转长表（逆透视）：把若干值列展开成 (variable, value) 两列。

    - 每个输入行按每个展开列输出一行（值为 None 也输出行；
      行内缺该键输出 value=None）；
    - ``columns``：要展开的值列，整表中完全不存在时抛 KeyError；
    - ``index``：保留的 id 列，None 表示除展开列外的全部列（所有行键的并集），
      指定列不存在同样抛 KeyError；
    - ``name_to`` / ``value_to``：原列名 / 原列值写入的新列名。
    """

    description = "宽表转长表"
    inputs = ["table"]
    outputs = [("table", list), ("rows", int)]
    params = {
        "columns": Param((str, list), doc="要展开的值列，单列名或列表"),
        "index": Param((str, list), default=None, doc="保留的 id 列；None 表示除展开列外的全部列"),
        "name_to": Param(str, default="variable", doc="原列名写入的新列名"),
        "value_to": Param(str, default="value", doc="原列值写入的新列名"),
    }

    def run(self, ctx, table, columns, index, name_to, value_to):
        rows = list(table or [])
        if not rows:
            return {"table": [], "rows": 0}
        expand = [columns] if isinstance(columns, str) else list(columns)
        available = table_columns(rows)
        missing = [c for c in expand if c not in available]
        if missing:
            raise KeyError(
                f"表中不存在列：{', '.join(map(str, missing))}；可用：{', '.join(sorted(map(str, available)))}"
            )
        if index is None:
            index_cols = [c for c in available if c not in expand]
        else:
            index_cols = [index] if isinstance(index, str) else list(index)
            missing = [c for c in index_cols if c not in available]
            if missing:
                raise KeyError(
                    f"表中不存在列：{', '.join(map(str, missing))}；可用：{', '.join(sorted(map(str, available)))}"
                )

        out: list[dict[str, Any]] = []
        for row in rows:
            for col in expand:
                new_row = {c: row.get(c) for c in index_cols}
                new_row[name_to] = col
                new_row[value_to] = row.get(col)
                out.append(new_row)
        ctx.log(f"逆透视 {len(rows)} 行 -> {len(out)} 行")
        return {"table": out, "rows": len(out)}
