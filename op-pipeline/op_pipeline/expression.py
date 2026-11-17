"""安全表达式求值（用于节点的 ``when`` 条件）。

不用 ``eval`` 直接跑字符串：先用 ``ast`` 白名单过滤，只允许字面量、变量名、
比较、布尔运算、算术和少量白名单函数，避免配置里注入任意代码。
"""
from __future__ import annotations

import ast
import operator
from typing import Any, Mapping

from .errors import ExpressionError

_BIN_OPS = {
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Mult: operator.mul,
    ast.Div: operator.truediv,
    ast.FloorDiv: operator.floordiv,
    ast.Mod: operator.mod,
    ast.Pow: operator.pow,
}
_CMP_OPS = {
    ast.Eq: operator.eq,
    ast.NotEq: operator.ne,
    ast.Lt: operator.lt,
    ast.LtE: operator.le,
    ast.Gt: operator.gt,
    ast.GtE: operator.ge,
    ast.In: lambda a, b: a in b,
    ast.NotIn: lambda a, b: a not in b,
    ast.Is: operator.is_,
    ast.IsNot: operator.is_not,
}
_UNARY_OPS = {ast.Not: operator.not_, ast.USub: operator.neg, ast.UAdd: operator.pos}

_ALLOWED_FUNCS: dict[str, Any] = {
    "len": len,
    "str": str,
    "int": int,
    "float": float,
    "bool": bool,
    "abs": abs,
    "min": min,
    "max": max,
    "sum": sum,
    "sorted": sorted,
    "any": any,
    "all": all,
    "round": round,
    "isinstance": isinstance,
}

_ALLOWED_NODES = (
    ast.Expression, ast.BoolOp, ast.And, ast.Or, ast.UnaryOp, ast.Not, ast.USub, ast.UAdd,
    ast.BinOp, ast.Add, ast.Sub, ast.Mult, ast.Div, ast.FloorDiv, ast.Mod, ast.Pow,
    ast.Compare, ast.Eq, ast.NotEq, ast.Lt, ast.LtE, ast.Gt, ast.GtE, ast.In, ast.NotIn,
    ast.Is, ast.IsNot, ast.Name, ast.Load, ast.Constant, ast.Call, ast.keyword,
    ast.IfExp, ast.List, ast.Tuple, ast.Dict, ast.Set, ast.Subscript, ast.Slice,
    ast.BoolOp, ast.BinOp, ast.Attribute,
)


def _check(node: ast.AST) -> None:
    if not isinstance(node, _ALLOWED_NODES):
        raise ExpressionError(f"表达式包含不允许的语法：{type(node).__name__}")
    for child in ast.iter_child_nodes(node):
        _check(child)


def _eval(node: ast.AST, env: Mapping[str, Any]) -> Any:
    if isinstance(node, ast.Expression):
        return _eval(node.body, env)
    if isinstance(node, ast.Constant):
        return node.value
    if isinstance(node, ast.Name):
        if node.id in env:
            return env[node.id]
        raise ExpressionError(f"表达式引用了未知变量 '{node.id}'")
    if isinstance(node, (ast.List, ast.Tuple, ast.Set)):
        items = [_eval(e, env) for e in node.elts]
        if isinstance(node, ast.List):
            return items
        if isinstance(node, ast.Tuple):
            return tuple(items)
        return set(items)
    if isinstance(node, ast.Dict):
        return {
            _eval(k, env): _eval(v, env)
            for k, v in zip(node.keys, node.values)
            if k is not None
        }
    if isinstance(node, ast.Subscript):
        target = _eval(node.value, env)
        key = _eval(node.slice, env)
        return target[key]
    if isinstance(node, ast.Attribute):
        # 点语法只做映射键下钻（params.n / inputs.table.rows 等），
        # 目标不是映射或键不存在即拒绝——因此 x.__class__ 之类属性逃逸天然被挡住
        target = _eval(node.value, env)
        if isinstance(target, Mapping) and node.attr in target:
            return target[node.attr]
        raise ExpressionError(
            f"无法下钻 '.{node.attr}'：目标"
            f"{'不含该键' if isinstance(target, Mapping) else '不是映射（点语法仅支持映射下钻）'}"
        )
    if isinstance(node, ast.Slice):
        return slice(
            _eval(node.lower, env) if node.lower else None,
            _eval(node.upper, env) if node.upper else None,
            _eval(node.step, env) if node.step else None,
        )
    if isinstance(node, ast.UnaryOp):
        fn = _UNARY_OPS.get(type(node.op))
        if fn is None:
            raise ExpressionError(f"不支持的一元运算：{type(node.op).__name__}")
        return fn(_eval(node.operand, env))
    if isinstance(node, ast.BinOp):
        fn = _BIN_OPS.get(type(node.op))
        if fn is None:
            raise ExpressionError(f"不支持的二元运算：{type(node.op).__name__}")
        return fn(_eval(node.left, env), _eval(node.right, env))
    if isinstance(node, ast.BoolOp):
        if isinstance(node.op, ast.And):
            result: Any = True
            for value in node.values:
                result = _eval(value, env)
                if not result:
                    return result
            return result
        result = False
        for value in node.values:
            result = _eval(value, env)
            if result:
                return result
        return result
    if isinstance(node, ast.Compare):
        left = _eval(node.left, env)
        for op, comparator in zip(node.ops, node.comparators):
            fn = _CMP_OPS.get(type(op))
            if fn is None:
                raise ExpressionError(f"不支持的比较运算：{type(op).__name__}")
            right = _eval(comparator, env)
            if not fn(left, right):
                return False
            left = right
        return True
    if isinstance(node, ast.IfExp):
        return _eval(node.body, env) if _eval(node.test, env) else _eval(node.orelse, env)
    if isinstance(node, ast.Call):
        if not isinstance(node.func, ast.Name):
            raise ExpressionError("只允许调用白名单函数")
        fn = _ALLOWED_FUNCS.get(node.func.id)
        if fn is None:
            raise ExpressionError(
                f"函数 '{node.func.id}' 不在白名单内；可用：{', '.join(sorted(_ALLOWED_FUNCS))}"
            )
        args = [_eval(a, env) for a in node.args]
        kwargs = {kw.arg: _eval(kw.value, env) for kw in node.keywords if kw.arg}
        return fn(*args, **kwargs)
    raise ExpressionError(f"无法求值的节点：{type(node).__name__}")


def evaluate(expression: str, env: Mapping[str, Any]) -> Any:
    """求值一个表达式字符串。"""
    try:
        tree = ast.parse(expression, mode="eval")
    except SyntaxError as exc:
        raise ExpressionError(f"表达式语法错误：{expression!r} -> {exc.msg}") from exc
    _check(tree)
    return _eval(tree, env)


def evaluate_condition(expression: str, env: Mapping[str, Any]) -> bool:
    """把表达式结果转成布尔（``when`` 语义）。"""
    return bool(evaluate(expression, env))


def register_function(name: str, fn: Any) -> Any:
    """把领域函数注册进表达式白名单（全局生效，需在加载配置前调用）。

    适合日期解析、正则辅助等领域谓词，让复杂条件留在配置里而不是被迫写算子。
    约束：

    - ``fn`` 必须是纯函数（同输入同输出、无副作用）——表达式可能在编译期、
      调度期、重试期被多次求值，副作用语义不可靠；
    - 名字必须是合法标识符，且**禁止覆盖内置名**（防止偷偷改变既有配置的含义）；
    - 重复注册同一自定义名视为替换（便于开发迭代）。

    注册后即可在 ``when`` / 表算子行级表达式 / ``control.assert`` 中使用::

        from op_pipeline import register_function

        register_function("parse_int", lambda s: int(str(s).replace(",", "")))
        # 配置里：when = "parse_int(value) > 1000"
    """
    if name in _BUILTIN_FUNCS:
        raise ExpressionError(
            f"不能覆盖内置函数 '{name}'；内置：{', '.join(sorted(_BUILTIN_FUNCS))}"
        )
    if not isinstance(name, str) or not name.isidentifier():
        raise ExpressionError(f"函数名必须是合法标识符：{name!r}")
    if not callable(fn):
        raise ExpressionError(f"注册的必须是可调用对象：{fn!r}")
    _ALLOWED_FUNCS[name] = fn
    return fn


# 内置函数名快照（register_function 捉"覆盖内置"用）
_BUILTIN_FUNCS = frozenset(_ALLOWED_FUNCS)
