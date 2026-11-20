"""JSON / 结构算子：对 dict 结构做通用变换（零第三方依赖）。

与 ``table.*`` 系列互补：表算子处理 ``list[dict]``，这里处理单个结构对象。
"""
from __future__ import annotations

import json
from typing import Any

from ..operator import Operator
from ..params import Param
from ..registry import op


@op("json.flatten")
class Flatten(Operator):
    """嵌套 dict 扁平化：嵌套 dict 的键按路径拼接（``a.b.c``）展开为一层。

    规则：

    - 只有 dict 会被递归展开；**list 值作为叶子原样保留，不展开**；
    - 空 dict 视为叶子原样保留（避免键被吞掉）；
    - ``max_depth=0`` 表示不限层数；``max_depth=N`` 表示最多展开 N 层嵌套
      （``1`` = 只展开顶层嵌套 dict 一层，更深的 dict 保留为叶子）；
    - 输出键一律为字符串路径；``count`` 是叶子数（= 扁平化后键数）。
    """

    description = "嵌套 dict 扁平化（list 值作为叶子）"
    inputs = [("data", dict)]
    outputs = [("data", dict), ("count", int)]
    params = {
        "sep": Param(str, default=".", doc="路径拼接分隔符"),
        "max_depth": Param(int, default=0, minimum=0, doc="最多展开层数；0 表示不限"),
    }

    def run(self, ctx, data, sep, max_depth):
        flat: dict[str, Any] = {}

        def walk(prefix: str, obj: dict[str, Any], expanded: int) -> None:
            # expanded：到达当前 obj 已经展开的嵌套层数（顶层 dict 自身不计）
            for key, value in obj.items():
                path = f"{prefix}{sep}{key}" if prefix else str(key)
                if isinstance(value, dict) and value and (max_depth <= 0 or expanded < max_depth):
                    walk(path, value, expanded + 1)
                else:
                    flat[path] = value

        walk("", dict(data or {}), 0)
        ctx.log(f"扁平化 {len(flat)} 个叶子键")
        return {"data": flat, "count": len(flat)}


@op("json.parse")
class Parse(Operator):
    """JSON 文本 -> Python 结构（``json.loads`` 语义）。

    - ``text`` 不是 str、或不是合法 JSON 时抛 ValueError；
    - ``data`` 为解析结果，dict / list / 标量均可。
    """

    description = "解析 JSON 文本"
    inputs = [("text", str)]
    outputs = [("data", Any)]

    def run(self, ctx, text):
        if not isinstance(text, str):
            raise ValueError(f"json.parse 的输入必须是 str，实际得到 {type(text).__name__}")
        try:
            data = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError(f"非法 JSON：{exc}") from exc
        ctx.log(f"解析 JSON 文本（{len(text)} 字符）")
        return {"data": data}


@op("json.stringify")
class Stringify(Operator):
    """Python 结构 -> JSON 文本（默认 indent=2、ensure_ascii=False、不排序键）。

    - ``text`` 为 JSON 字符串，``bytes`` 为其 UTF-8 编码后的字节数；
    - ``data`` 无法序列化时让 TypeError 自然抛出。
    """

    description = "结构序列化为 JSON 文本"
    inputs = [("data", Any)]
    outputs = [("text", str), ("bytes", int)]
    params = {
        "indent": Param(int, default=2, doc="缩进空格数"),
        "ensure_ascii": Param(bool, default=False, doc="是否把非 ASCII 字符转义"),
        "sort_keys": Param(bool, default=False, doc="是否按键名排序"),
    }

    def run(self, ctx, data, indent, ensure_ascii, sort_keys):
        text = json.dumps(data, indent=indent, ensure_ascii=ensure_ascii, sort_keys=sort_keys)
        size = len(text.encode("utf-8"))
        ctx.log(f"序列化为 JSON 文本（{size} 字节）")
        return {"text": text, "bytes": size}
