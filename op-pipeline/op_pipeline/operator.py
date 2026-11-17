"""算子基类：算子级封装的核心契约。

一个算子 = 一份自描述的可执行单元：
    声明（name/inputs/outputs/params） + 实现（run） + 自检（describe/单测）

编排层只依赖声明，不依赖实现，因此算子可以被单独测试、被多个流水线复用、
也可以由不同团队各自维护后通过插件机制汇总。
"""
from __future__ import annotations

import asyncio
import inspect
from typing import Any, ClassVar, Mapping, Sequence

from .errors import PipelineError
from .params import Param, as_params, validate_params
from .ports import Port, as_ports, port_names, type_label


def normalize_operator_class(cls: type) -> type:
    """把类属性上的宽松声明归一化，保证注册后契约稳定可查。"""
    import keyword

    cls.inputs = as_ports(getattr(cls, "inputs", ()))
    cls.outputs = as_ports(getattr(cls, "outputs", ()))
    cls.param_table = as_params(getattr(cls, "params", {}))
    cls.version = str(getattr(cls, "version", "1"))
    cls.description = str(getattr(cls, "description", "") or (cls.__doc__ or "").strip().split("\n")[0])
    cls.deterministic = bool(getattr(cls, "deterministic", True))

    for port in (*cls.inputs, *cls.outputs):
        if not port.name.isidentifier() or keyword.iskeyword(port.name):
            raise PipelineError(
                f"算子 {cls.name or cls.__qualname__} 的端口名 '{port.name}' 不是合法标识符"
                "（端口名会作为 run() 的关键字参数，不能是 Python 关键字）"
            )

    collisions = sorted(set(port.name for port in cls.inputs) & set(cls.param_table))
    if collisions:
        raise PipelineError(
            f"算子 {cls.name or cls.__qualname__} 的输入端口与参数重名：{', '.join(collisions)}"
            "（两者都以关键字参数形式传给 run()，不能同名）"
        )
    return cls


class Operator:
    """算子基类。

    子类实现 ``run(self, ctx, **kwargs)``，``kwargs`` 同时包含：

    * 声明的输入端口（按端口名）；
    * 声明的参数（按参数名，已按类型校验/补默认值）。

    返回 ``dict`` 时按输出端口名取值；只有一个输出端口时可以直接返回裸值；
    ``run`` 可以是 ``async def``，框架会自动在事件循环里执行。
    """

    name: ClassVar[str] = ""
    version: ClassVar[str] = "1"
    description: ClassVar[str] = ""
    inputs: ClassVar[tuple[Port, ...]] = ()
    outputs: ClassVar[tuple[Port, ...]] = ()
    params: ClassVar[Mapping[str, Param]] = {}
    param_table: ClassVar[dict[str, Param]] = {}
    deterministic: ClassVar[bool] = True

    def __init__(self, **params: Any) -> None:
        self.params: dict[str, Any] = validate_params(
            self.name or type(self).__name__, self.param_table, params
        )

    # ------------------------------------------------------------------ #
    # 契约描述
    # ------------------------------------------------------------------ #
    @classmethod
    def describe(cls) -> dict[str, Any]:
        return {
            "name": cls.name,
            "version": cls.version,
            "description": cls.description,
            "module": f"{cls.__module__}.{cls.__qualname__}",
            "deterministic": cls.deterministic,
            "inputs": [
                {"name": p.name, "type": type_label(p.type), "optional": p.optional, "doc": p.doc}
                for p in cls.inputs
            ],
            "outputs": [
                {"name": p.name, "type": type_label(p.type), "optional": p.optional, "doc": p.doc}
                for p in cls.outputs
            ],
            "params": {
                name: {
                    "type": type_label(p.type),
                    "required": p.required,
                    "default": None if p.required else p.default,
                    "doc": p.doc,
                    "choices": list(p.choices),
                }
                for name, p in cls.param_table.items()
            },
        }

    @classmethod
    def input_names(cls) -> tuple[str, ...]:
        return port_names(cls.inputs)

    @classmethod
    def output_names(cls) -> tuple[str, ...]:
        return port_names(cls.outputs)

    # ------------------------------------------------------------------ #
    # 执行
    # ------------------------------------------------------------------ #
    def run(self, ctx: Any, **inputs: Any) -> Any:  # pragma: no cover - 抽象方法
        raise NotImplementedError(f"算子 {self.name} 未实现 run()")

    def execute(self, ctx: Any, inputs: Mapping[str, Any]) -> dict[str, Any]:
        """框架内部调用：做输入校验 -> 执行 -> 输出归一。"""
        prepared = self._prepare_inputs(inputs)
        kwargs = {**self.params, **prepared}
        fn = self.run
        if inspect.iscoroutinefunction(fn):
            result = asyncio.run(fn(ctx, **kwargs))
        else:
            result = fn(ctx, **kwargs)
        return self._normalize_output(result)

    # ------------------------------------------------------------------ #
    def _prepare_inputs(self, inputs: Mapping[str, Any]) -> dict[str, Any]:
        declared = {p.name: p for p in self.inputs}
        unknown = sorted(set(inputs) - set(declared))
        if unknown:
            raise PipelineError(
                f"算子 {self.name} 收到未声明的输入端口：{', '.join(unknown)}；"
                f"可用端口：{', '.join(declared) or '（无）'}"
            )
        missing = [
            p.name for p in self.inputs
            if not p.optional and p.name not in inputs
        ]
        if missing:
            raise PipelineError(
                f"算子 {self.name} 缺少必填输入端口：{', '.join(missing)}"
            )
        return {p.name: inputs.get(p.name) for p in self.inputs}

    def _normalize_output(self, result: Any) -> dict[str, Any]:
        if result is None:
            result = {}
        if not isinstance(result, Mapping):
            if len(self.outputs) != 1:
                raise PipelineError(
                    f"算子 {self.name} 有 {len(self.outputs)} 个输出端口，"
                    f"run() 必须返回 dict，实际返回 {type(result).__name__}"
                )
            result = {self.outputs[0].name: result}

        declared = {p.name: p for p in self.outputs}
        unknown = sorted(set(result) - set(declared))
        if unknown:
            raise PipelineError(
                f"算子 {self.name} 返回了未声明的输出：{', '.join(unknown)}"
            )
        missing = [p.name for p in self.outputs if not p.optional and p.name not in result]
        if missing:
            raise PipelineError(
                f"算子 {self.name} 未返回必填输出：{', '.join(missing)}"
            )
        return dict(result)

    def __repr__(self) -> str:  # pragma: no cover
        args = ", ".join(f"{k}={v!r}" for k, v in self.params.items())
        return f"<{self.name} {args}>"
