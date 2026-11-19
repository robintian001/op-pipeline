"""数据质量算子：表级质检门禁（零第三方依赖）。

三个算子契约统一：输入 ``table:list``，输出 ``table:list``（**原样透传**）与
``violations:list``（违例明细，dict 形态，含列名/行索引/样例行等上下文）。

``mode`` 双模式：

- ``"error"``：发现违例即抛 ``AssertionError``（节点失败；与 ``control.assert``
  同类，属于普通异常会按 ``retries`` 重试——质检门禁场景建议 ``retries=0``）；
- ``"warn"``：不抛错，违例明细放进 ``violations`` 输出，由下游决定是否处理。

空值定义与分工：``None`` 与空白字符串视为空值（归 ``null_check`` 管）；
``schema_check`` 对 ``None`` 值跳过类型检查（空值不判类型错）。
"""
from __future__ import annotations

from typing import Any

from ..operator import Operator
from ..params import Param
from ..registry import op
from .table_ops import table_columns

_MAX_SAMPLES = 3  # 每条违例最多附带的样例行数
_MAX_DUPLICATE_SAMPLES = 10  # unique 违例最多列出的重复键组合数

_TYPE_NAMES = {
    "str": str,
    "int": int,
    "float": float,
    "bool": bool,
    "list": list,
    "dict": dict,
    "any": None,  # None 表示不检查类型
}


def _as_list(value: Any, param_name: str) -> list[str]:
    """把 str 或 list[str] 参数归一为 list[str]（空列表直接报错，防手滑）。"""
    items = [value] if isinstance(value, str) else list(value or [])
    if not items:
        raise ValueError(f"参数 '{param_name}' 不能为空（需要列名或列名列表）")
    return [str(item) for item in items]


def _is_null(value: Any) -> bool:
    """空值：None 或纯空白字符串（0/False 等不算空）。"""
    return value is None or (isinstance(value, str) and value.strip() == "")


def _check_mode(mode: str) -> str:
    if mode not in ("error", "warn"):
        raise ValueError(f"mode 只能是 'error' 或 'warn'，实际：{mode!r}")
    return mode


def _error(summary: str, violations: list[dict[str, Any]]) -> None:
    """error 模式的统一报错入口：摘要 + 前 10 条违例明细。"""
    detail = "; ".join(str(v) for v in violations[:10])
    raise AssertionError(f"{summary}（共 {len(violations)} 条违例）：{detail}")


@op("quality.null_check")
class NullCheck(Operator):
    """空值率检查：统计各列 None/空白值占比，超过 ``max_null_ratio`` 即违例。"""

    description = "空值率检查（error 抛错 / warn 出明细）"
    inputs = ["table"]
    outputs = [("table", list), ("violations", list)]
    params = {
        "columns": Param((str, list), doc="要检查的列，单列名或列名列表"),
        "max_null_ratio": Param(float, default=0.0, minimum=0.0, maximum=1.0,
                                doc="允许的最大空值率（0.0 = 不允许任何空值）"),
        "mode": Param(str, default="error", choices=("error", "warn"), doc="error 抛 AssertionError / warn 只出明细"),
    }

    def run(self, ctx, table, columns, max_null_ratio, mode):
        _check_mode(mode)
        rows = list(table or [])
        cols = _as_list(columns, "columns")
        if rows:
            available = table_columns(rows)
            missing = [c for c in cols if c not in available]
            if missing:
                raise KeyError(f"表中不存在列：{', '.join(missing)}；可用：{', '.join(sorted(available))}")

        violations: list[dict[str, Any]] = []
        for col in cols:
            null_indices = [i for i, row in enumerate(rows) if _is_null(row.get(col))]
            ratio = len(null_indices) / len(rows) if rows else 0.0
            if ratio <= max_null_ratio:
                continue
            violation = {
                "type": "null_ratio",
                "column": col,
                "nulls": len(null_indices),
                "total": len(rows),
                "ratio": round(ratio, 4),
                "threshold": max_null_ratio,
                "row_indices": null_indices[:_MAX_SAMPLES],
                "samples": [rows[i] for i in null_indices[:_MAX_SAMPLES]],
            }
            violations.append(violation)

        if violations and mode == "error":
            summary = "、".join(
                f"列 '{v['column']}' 空值率 {v['ratio']}（阈值 {v['threshold']}，{v['nulls']}/{v['total']}）"
                for v in violations
            )
            _error(f"quality.null_check 空值率超阈值：{summary}", violations)

        ctx.log(f"空值率检查（{mode}）：{len(rows)} 行 x {len(cols)} 列，违例 {len(violations)} 条")
        return {"table": rows, "violations": violations}


@op("quality.unique")
class UniqueCheck(Operator):
    """重复检查：按 ``keys`` 组合查重复行，violations 含前 10 个重复键样例与重复次数。"""

    description = "按键组合查重复行（error 抛错 / warn 出明细）"
    inputs = ["table"]
    outputs = [("table", list), ("violations", list)]
    params = {
        "keys": Param((str, list), doc="判重键，单列名或列名列表"),
        "mode": Param(str, default="error", choices=("error", "warn"), doc="error 抛 AssertionError / warn 只出明细"),
    }

    def run(self, ctx, table, keys, mode):
        _check_mode(mode)
        rows = list(table or [])
        keys_list = _as_list(keys, "keys")
        if rows:
            available = table_columns(rows)
            missing = [k for k in keys_list if k not in available]
            if missing:
                raise KeyError(f"表中不存在列：{', '.join(missing)}；可用：{', '.join(sorted(available))}")

        groups: dict[tuple, list[int]] = {}
        for i, row in enumerate(rows):
            signature = tuple(row.get(k) for k in keys_list)
            groups.setdefault(signature, []).append(i)

        violations: list[dict[str, Any]] = []
        for signature, indices in groups.items():
            if len(indices) <= 1:
                continue
            violations.append({
                "type": "duplicate_key",
                "keys": dict(zip(keys_list, signature)),
                "count": len(indices),
                "row_indices": indices[:_MAX_SAMPLES],
                "samples": [rows[i] for i in indices[:_MAX_SAMPLES]],
            })
            if len(violations) >= _MAX_DUPLICATE_SAMPLES:
                break

        if violations and mode == "error":
            summary = "；".join(f"键 {v['keys']} 重复 {v['count']} 次" for v in violations[:5])
            more = f"（等 {len(violations)} 组重复）" if len(violations) > 5 else ""
            _error(f"quality.unique 发现重复键：{summary}{more}", violations)

        ctx.log(f"重复检查（{mode}）：{len(rows)} 行，重复键组合 {len(violations)} 组")
        return {"table": rows, "violations": violations}


@op("quality.schema_check")
class SchemaCheck(Operator):
    """逐行类型检查：``expected`` 声明 列名 -> 类型名（``str|int|float|bool|list|dict|any``）。

    - ``None`` 值跳过类型检查（空值归 null_check 管）；
    - ``bool`` 是 ``int`` 的子类，声明 ``"int"`` 时 bool 视为类型不符；int 也不当作 float；
    - ``strict_columns=true`` 时，表的列集合缺 ``expected`` 中的列也算违例；
    - 类型名非法（不在白名单）抛 ``ValueError``（配置错误）。
    """

    description = "逐行类型检查（error 抛错 / warn 出明细）"
    inputs = ["table"]
    outputs = [("table", list), ("violations", list)]
    params = {
        "expected": Param(dict, doc="列名 -> 类型名（str/int/float/bool/list/dict/any）"),
        "strict_columns": Param(bool, default=False, doc="true 时表缺列也算违例"),
        "mode": Param(str, default="error", choices=("error", "warn"), doc="error 抛 AssertionError / warn 只出明细"),
    }

    def run(self, ctx, table, expected, strict_columns, mode):
        _check_mode(mode)
        expected = dict(expected or {})
        unknown_types = sorted(set(expected.values()) - set(_TYPE_NAMES))
        if unknown_types:
            raise ValueError(
                f"quality.schema_check 的类型名非法：{', '.join(map(str, unknown_types))}；"
                f"可用：{', '.join(_TYPE_NAMES)}"
            )

        rows = list(table or [])
        violations: list[dict[str, Any]] = []

        if rows and strict_columns:
            available = table_columns(rows)
            for col in expected:
                if col not in available:
                    violations.append({"type": "missing_column", "column": col, "expected": expected[col]})

        for i, row in enumerate(rows):
            for col, type_name in expected.items():
                value = row.get(col)
                if value is None:
                    continue  # 空值跳过类型检查（归 null_check 管）
                check_type = _TYPE_NAMES[type_name]
                if check_type is None:  # "any"
                    continue
                # bool 是 int 的子类：声明 int 时 bool 算类型不符；int 也不当作 float
                if check_type is int and isinstance(value, bool):
                    mismatch = True
                else:
                    mismatch = not isinstance(value, check_type)
                if mismatch:
                    violations.append({
                        "type": "type_mismatch",
                        "row": i,
                        "column": col,
                        "expected": type_name,
                        "actual": type(value).__name__,
                        "value": value,
                        "sample_row": row,
                    })

        if violations and mode == "error":
            summary = "；".join(
                f"第 {v['row']} 行列 '{v['column']}' 期望 {v['expected']} 实际 {v['actual']}"
                if v["type"] == "type_mismatch" else f"缺少列 '{v['column']}'"
                for v in violations[:5]
            )
            more = f"（等 {len(violations)} 条）" if len(violations) > 5 else ""
            _error(f"quality.schema_check 类型检查失败：{summary}{more}", violations)

        ctx.log(f"类型检查（{mode}）：{len(rows)} 行 x {len(expected)} 列，违例 {len(violations)} 条")
        return {"table": rows, "violations": violations}
