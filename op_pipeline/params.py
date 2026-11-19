"""算子参数声明、类型归一与校验。

配置驱动框架最容易出错的地方是"配置项写错/类型写错"，所以这里：
- 未声明的参数直接报错（防拼写错误静默失效）；
- 声明了默认值就自动补齐；
- 做有限而明确的类型归一（"3" -> 3，适合 TOML/环境变量等文本来源）。
"""
from __future__ import annotations

import copy
from dataclasses import dataclass
from typing import Any, Mapping

from .errors import ParamValidationError
from .ports import type_label

_UNSET = object()


@dataclass(frozen=True)
class Param:
    """算子参数声明。

    ``default`` 不给即表示必填；给了即表示可选并自动补齐。
    """

    type: Any = Any
    default: Any = _UNSET
    doc: str = ""
    choices: tuple[Any, ...] = ()
    minimum: float | None = None
    maximum: float | None = None

    @property
    def required(self) -> bool:
        return self.default is _UNSET


def as_params(spec: Any) -> dict[str, Param]:
    """归一化参数声明表。

    支持 ``{"path": Param(str), "limit": 10}`` 这种混写，裸值等价于
    ``Param(type(value), default=value)``。
    """
    if not spec:
        return {}
    if not isinstance(spec, Mapping):  # pragma: no cover
        raise TypeError("params 声明必须是 dict")
    out: dict[str, Param] = {}
    for key, value in spec.items():
        if isinstance(value, Param):
            out[str(key)] = value
        else:
            out[str(key)] = Param(type(value), default=value)
    return out


def _fail(name: str, spec: Param, reason: str) -> None:
    raise ParamValidationError(
        f"参数 '{name}' 校验失败：{reason}（期望类型 {type_label(spec.type)}）"
    )


def coerce(name: str, spec: Param, value: Any) -> Any:
    """把配置值转换为参数声明的类型，并做取值范围检查。"""
    if value is None:
        if spec.default is not _UNSET:
            return copy.deepcopy(spec.default)
        if spec.required:
            _fail(name, spec, "必填参数未提供")
        return None

    t = spec.type
    if t is None or t is Any:
        pass
    elif isinstance(t, tuple):
        if not isinstance(value, t):
            _fail(name, spec, f"实际得到 {type(value).__name__}")
    elif isinstance(t, type):
        if t is bool:
            if not isinstance(value, bool):
                if isinstance(value, str):
                    low = value.strip().lower()
                    if low in ("true", "yes", "1", "on"):
                        value = True
                    elif low in ("false", "no", "0", "off"):
                        value = False
                    else:
                        _fail(name, spec, f"无法把 {value!r} 解释为 bool")
                elif isinstance(value, (int, float)) and value in (0, 1):
                    value = bool(value)
                else:
                    _fail(name, spec, f"实际得到 {type(value).__name__}")
        elif t is int:
            if isinstance(value, bool):
                _fail(name, spec, "bool 不能当 int 使用")
            if isinstance(value, str):
                text = value.strip()
                try:
                    value = int(text) if "." not in text else int(float(text))
                except ValueError:
                    _fail(name, spec, f"无法把 {value!r} 解释为 int")
            elif isinstance(value, float):
                if not value.is_integer():
                    _fail(name, spec, f"{value!r} 不是整数")
                value = int(value)
            elif not isinstance(value, int):
                _fail(name, spec, f"实际得到 {type(value).__name__}")
        elif t is float:
            if isinstance(value, bool):
                _fail(name, spec, "bool 不能当 float 使用")
            if isinstance(value, (int, float)):
                value = float(value)
            elif isinstance(value, str):
                try:
                    value = float(value.strip())
                except ValueError:
                    _fail(name, spec, f"无法把 {value!r} 解释为 float")
            else:
                _fail(name, spec, f"实际得到 {type(value).__name__}")
        elif t is str:
            if isinstance(value, str):
                pass
            elif isinstance(value, (int, float, bool)):
                value = str(value)
            else:
                _fail(name, spec, f"实际得到 {type(value).__name__}")
        elif not isinstance(value, t):
            _fail(name, spec, f"实际得到 {type(value).__name__}")

    if spec.choices and value not in spec.choices:
        _fail(name, spec, f"{value!r} 不在可选范围 {list(spec.choices)} 内")
    if spec.minimum is not None and isinstance(value, (int, float)) and value < spec.minimum:
        _fail(name, spec, f"{value!r} 小于下限 {spec.minimum}")
    if spec.maximum is not None and isinstance(value, (int, float)) and value > spec.maximum:
        _fail(name, spec, f"{value!r} 大于上限 {spec.maximum}")
    return value


def validate_params(
    op_name: str, table: Mapping[str, Param], given: Mapping[str, Any] | None
) -> dict[str, Any]:
    """校验并补全一个算子实例的参数。"""
    given = dict(given or {})
    unknown = sorted(set(given) - set(table))
    if unknown:
        allowed = ", ".join(sorted(table)) or "（该算子无参数）"
        raise ParamValidationError(
            f"算子 {op_name} 收到未声明的参数：{', '.join(unknown)}；可用参数：{allowed}"
        )
    result: dict[str, Any] = {}
    for name, spec in table.items():
        result[name] = coerce(name, spec, given.get(name, None))
    return result
