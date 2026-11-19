"""端口（输入/输出）声明与类型检查。

端口是"算子级封装"的对外契约：算子只声明自己吃什么、吐什么，
编排层据此做连线校验、自动装配、文档生成，不关心算子内部实现。
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Sequence


@dataclass(frozen=True)
class Port:
    """一个命名端口。

    :param name: 端口名，同时也是算子 ``run()`` 的关键字参数名
    :param type: 期望的 Python 类型（``list``/``dict``/``str``...），``Any`` 表示不限制
    :param optional: 是否允许上游不接线（True 时 ``run()`` 收到 ``None``）
    :param doc: 端口用途说明，用于 ``describe``/文档生成
    """

    name: str
    type: Any = Any
    optional: bool = False
    doc: str = ""

    def label(self) -> str:
        return f"{self.name}:{type_label(self.type)}"


def type_label(annotation: Any) -> str:
    if annotation is None or annotation is Any:
        return "any"
    if isinstance(annotation, tuple):
        return "|".join(type_label(a) for a in annotation)
    return getattr(annotation, "__name__", str(annotation))


def as_ports(spec: Any) -> tuple[Port, ...]:
    """把多种声明写法归一化成 ``tuple[Port, ...]``。

    支持::

        "table"                              # 简写，等价 Port("table")
        ["table", "count"]                   # 多个简写
        {"table": list, "count": int}        # 名字 -> 类型
        [Port("table", list), ("count", int)]
    """
    if spec is None:
        return ()
    if isinstance(spec, Port):
        return (spec,)
    if isinstance(spec, Mapping):
        return tuple(
            v if isinstance(v, Port) else Port(str(k), v)
            for k, v in spec.items()
        )
    if isinstance(spec, str):
        return (Port(spec),)

    out: list[Port] = []
    for item in spec:
        if isinstance(item, Port):
            out.append(item)
        elif isinstance(item, str):
            out.append(Port(item))
        elif isinstance(item, Sequence) and not isinstance(item, str):
            name = str(item[0])
            ann = item[1] if len(item) > 1 else Any
            optional = bool(item[2]) if len(item) > 2 else False
            out.append(Port(name, ann, optional))
        else:  # pragma: no cover - 防御式
            raise TypeError(f"无法识别的端口声明：{item!r}")
    return tuple(out)


def value_matches(annotation: Any, value: Any) -> bool:
    """软类型检查：只做 ``isinstance``，不做深层结构校验。"""
    if annotation is None or annotation is Any or value is None:
        return True
    if isinstance(annotation, tuple):
        return any(value_matches(a, value) for a in annotation)
    if not isinstance(annotation, type):
        return True
    try:
        return isinstance(value, annotation)
    except TypeError:  # pragma: no cover
        return True


def port_warnings(port: Port, value: Any, node_id: str) -> list[str]:
    """输出端口类型不匹配时给出的告警（不阻断执行）。"""
    if value_matches(port.type, value):
        return []
    return [
        f"节点 [{node_id}] 输出端口 {port.name} 声明为 {type_label(port.type)}，"
        f"实际为 {type(value).__name__}（软校验，不阻断执行）"
    ]


def port_names(ports: Sequence[Port]) -> tuple[str, ...]:
    return tuple(p.name for p in ports)
