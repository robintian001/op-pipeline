"""JSON / 结构算子：对 dict 结构做通用变换（零第三方依赖）。

与 ``table.*`` 系列互补：表算子处理 ``list[dict]``，这里处理单个结构对象。
"""
from __future__ import annotations

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
