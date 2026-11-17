"""统计算子：分组聚合、值分布、描述统计。"""
from __future__ import annotations

import statistics
from typing import Any, Iterable

from ..operator import Operator
from ..params import Param
from ..registry import op

_AGG_FUNCS = ("sum", "mean", "count", "min", "max", "median", "std", "nunique", "first", "last")


def _numbers(values: Iterable[Any]) -> list[float]:
    return [float(v) for v in values if isinstance(v, (int, float)) and not isinstance(v, bool)]


def _agg(func: str, values: list[Any]) -> Any:
    nums = _numbers(values)
    if func == "count":
        return sum(1 for v in values if v is not None)
    if func == "nunique":
        return len({v for v in values if v is not None})
    if func == "first":
        return next((v for v in values if v is not None), None)
    if func == "last":
        return next((v for v in reversed(values) if v is not None), None)
    if not nums:
        return None
    if func == "sum":
        return sum(nums)
    if func == "mean":
        return sum(nums) / len(nums)
    if func == "min":
        return min(nums)
    if func == "max":
        return max(nums)
    if func == "median":
        return statistics.median(nums)
    if func == "std":
        return statistics.pstdev(nums) if len(nums) > 1 else 0.0
    raise ValueError(f"未知聚合函数：{func}")


def _normalize_metrics(metrics: dict[str, Any]) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    for column, funcs in metrics.items():
        if isinstance(funcs, str):
            funcs = [funcs]
        if not isinstance(funcs, (list, tuple)):
            raise TypeError(f"metrics['{column}'] 必须是字符串或字符串列表")
        for func in funcs:
            if func not in _AGG_FUNCS:
                raise ValueError(f"不支持的聚合函数 '{func}'；可用：{', '.join(_AGG_FUNCS)}")
        out[str(column)] = [str(f) for f in funcs]
    return out


@op("stats.aggregate", aliases=["groupby"])
class Aggregate(Operator):
    """分组聚合。

    配置里 ``metrics`` 形如::

        [nodes.agg.params.metrics]
        amount = ["sum", "mean"]
        order_id = "count"
    """

    description = "分组聚合"
    inputs = ["table"]
    outputs = [("table", list), ("groups", int)]
    params = {
        "group_by": Param((str, list), default=None, doc="分组列，None 表示全表一组"),
        "metrics": Param(dict, doc="列 -> 聚合函数（或函数列表）"),
    }

    def run(self, ctx, table, group_by, metrics):
        rows = list(table or [])
        group_cols = [] if group_by is None else ([group_by] if isinstance(group_by, str) else list(group_by))
        spec = _normalize_metrics(metrics)

        buckets: dict[tuple, list[dict[str, Any]]] = {}
        for row in rows:
            key = tuple(row.get(c) for c in group_cols)
            buckets.setdefault(key, []).append(row)

        if not buckets:
            return {"table": [], "groups": 0}

        out: list[dict[str, Any]] = []
        for key, bucket in buckets.items():
            record: dict[str, Any] = dict(zip(group_cols, key))
            for column, funcs in spec.items():
                values = [r.get(column) for r in bucket]
                for func in funcs:
                    record[f"{column}_{func}"] = _agg(func, values)
            out.append(record)

        out.sort(key=lambda r: tuple(str(r.get(c)) for c in group_cols))
        ctx.log(f"聚合 {len(rows)} 行 -> {len(out)} 组")
        return {"table": out, "groups": len(out)}


@op("stats.count_rows")
class CountRows(Operator):
    """行数统计。"""

    description = "统计行数"
    inputs = ["table"]
    outputs = [("count", int), ("empty", bool)]
    params = {}

    def run(self, ctx, table):
        count = len(list(table or []))
        return {"count": count, "empty": count == 0}


@op("stats.value_counts")
class ValueCounts(Operator):
    """单列取值分布。"""

    description = "单列取值分布"
    inputs = ["table"]
    outputs = [("table", list), ("total", int)]
    params = {
        "column": Param(str),
        "top": Param(int, default=0, minimum=0, doc="只保留前 N 项，0 表示全部"),
    }

    def run(self, ctx, table, column, top):
        counts: dict[Any, int] = {}
        for row in table or []:
            value = row.get(column)
            counts[value] = counts.get(value, 0) + 1
        rows = [
            {"value": value, "count": count, "share": round(count / sum(counts.values()), 4)}
            for value, count in sorted(counts.items(), key=lambda kv: (-kv[1], str(kv[0])))
        ]
        total = len(list(table or []))
        if top:
            rows = rows[:top]
        return {"table": rows, "total": total}


@op("stats.describe")
class Describe(Operator):
    """数值列描述统计。"""

    description = "数值列描述统计"
    inputs = ["table"]
    outputs = [("table", list)]
    params = {"columns": Param(list, default=None, doc="None 表示自动识别数值列")}

    def run(self, ctx, table, columns):
        rows = list(table or [])
        if not rows:
            return {"table": []}
        if columns:
            targets = list(columns)
        else:
            targets = [
                key for key in rows[0]
                if any(isinstance(r.get(key), (int, float)) and not isinstance(r.get(key), bool) for r in rows)
            ]

        out = []
        for column in targets:
            values = [r.get(column) for r in rows]
            nums = _numbers(values)
            out.append(
                {
                    "column": column,
                    "count": sum(1 for v in values if v is not None),
                    "nulls": sum(1 for v in values if v is None),
                    "sum": round(sum(nums), 6) if nums else None,
                    "mean": round(sum(nums) / len(nums), 6) if nums else None,
                    "min": min(nums) if nums else None,
                    "max": max(nums) if nums else None,
                    "std": round(statistics.pstdev(nums), 6) if len(nums) > 1 else (0.0 if nums else None),
                    "median": statistics.median(nums) if nums else None,
                }
            )
        return {"table": out}
