"""控制流与校验算子：让配置能表达分支、断言、常量注入。"""
from __future__ import annotations

from typing import Any

from ..expression import evaluate
from ..operator import Operator
from ..params import Param
from ..registry import op


@op("control.const", aliases=["const"])
class Const(Operator):
    """注入常量（常用于把流水线参数变成数据流）。"""

    description = "注入常量"
    outputs = [("value", Any)]
    params = {"value": Param(Any, doc="任意值")}

    def run(self, ctx, value):
        return {"value": value}


@op("control.if_else")
class IfElse(Operator):
    """二选一：条件为真走 ``when_true``，否则走 ``when_false``。"""

    description = "条件二选一"
    inputs = [("condition", Any), ("when_true", Any), ("when_false", Any, True)]
    outputs = [("value", Any), ("branch", str)]
    params = {"as_bool": Param(bool, default=True, doc="是否把 condition 转成布尔")}

    def run(self, ctx, condition, when_true, when_false, as_bool):
        truth = bool(condition) if as_bool else condition
        if truth:
            return {"value": when_true, "branch": "then"}
        return {"value": when_false, "branch": "else"}


@op("control.switch")
class Switch(Operator):
    """多路分支：按 ``cases`` 的键匹配，命中即返回对应值。"""

    description = "多路分支"
    inputs = [("key", Any)]
    outputs = [("value", Any), ("branch", str)]
    params = {
        "cases": Param(dict, doc="键 -> 值"),
        "default": Param(Any, default=None),
    }

    def run(self, ctx, key, cases, default):
        text_key = str(key)
        for case_key, value in (cases or {}).items():
            if str(case_key) == text_key:
                return {"value": value, "branch": text_key}
        return {"value": default, "branch": "default"}


@op("control.assert")
class Assert(Operator):
    """断言：条件不成立即失败，用于在流水线里做数据质量门禁。"""

    description = "断言校验（失败即中断该分支）"
    inputs = [("value", Any)]
    outputs = [("value", Any)]
    params = {
        "that": Param(str, doc="断言表达式，可引用 value 与 params"),
        "message": Param(str, default="断言失败", doc="失败时抛出的信息"),
    }

    def run(self, ctx, value, that, message):
        env = {"value": value, "params": dict(ctx.params)}
        if isinstance(value, dict):
            env.update({k: v for k, v in value.items() if isinstance(k, str)})
        if not evaluate(that, env):
            raise AssertionError(f"{message}（断言：{that}，value={value!r}）")
        return {"value": value}


@op("control.passthrough")
class Passthrough(Operator):
    """原样传递（用于并行扇出时给同一条数据流起别名）。"""

    description = "透传"
    inputs = [("value", Any)]
    outputs = [("value", Any)]

    def run(self, ctx, value):
        return {"value": value}


@op("control.merge_dicts")
class MergeDicts(Operator):
    """合并多个字典（浅合并，后者覆盖前者）。"""

    description = "字典合并"
    inputs = [("left", dict), ("right", dict)]
    outputs = [("value", dict)]
    params = {"deep": Param(bool, default=False)}

    def run(self, ctx, left, right, deep):
        if deep:
            return {"value": _deep_merge(dict(left or {}), dict(right or {}))}
        merged = dict(left or {})
        merged.update(right or {})
        return {"value": merged}


def _deep_merge(a: dict[str, Any], b: dict[str, Any]) -> dict[str, Any]:
    out = dict(a)
    for key, value in b.items():
        if isinstance(out.get(key), dict) and isinstance(value, dict):
            out[key] = _deep_merge(out[key], value)
        else:
            out[key] = value
    return out
