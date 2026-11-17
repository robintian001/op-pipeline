"""统一异常体系。

设计原则：
1. 所有框架异常都继承 :class:`PipelineError`，调用方可以只 catch 一个基类；
2. 配置类错误（写错了）与执行类错误（跑挂了）分开，便于 CLI 返回不同退出码；
3. 校验错误尽量携带"所有"问题清单，而不是遇到第一个就抛，方便一次性改完配置。
"""
from __future__ import annotations


class PipelineError(Exception):
    """框架根异常。"""


# --------------------------------------------------------------------------- #
# 配置 / 规格错误（静态，可离线校验）
# --------------------------------------------------------------------------- #
class ConfigError(PipelineError):
    """配置或流水线规格不合法。"""


class DuplicateOperatorError(ConfigError):
    """算子名重复注册。"""


class OperatorNotFoundError(ConfigError):
    """算子未注册。"""


class ParamValidationError(ConfigError):
    """算子参数校验失败。"""


class RefResolutionError(ConfigError):
    """`${...}` 引用无法解析。"""


class CycleError(ConfigError):
    """依赖图存在环。"""


class SpecValidationError(ConfigError):
    """流水线整体校验失败，聚合多条错误信息。"""

    def __init__(self, problems: list[str]) -> None:
        self.problems = list(problems)
        body = "\n".join(f"  - {p}" for p in self.problems)
        super().__init__(f"流水线校验失败（共 {len(self.problems)} 项）：\n{body}")


# --------------------------------------------------------------------------- #
# 执行错误（动态）
# --------------------------------------------------------------------------- #
class ModuleExecutionError(PipelineError):
    """节点执行失败，保留原始异常类型与调用栈信息。"""

    def __init__(self, node_id: str, op: str, cause: BaseException) -> None:
        self.node_id = node_id
        self.op = op
        self.cause = cause
        super().__init__(
            f"节点 [{node_id}] 算子 {op} 执行失败：{type(cause).__name__}: {cause}"
        )

    def __str__(self) -> str:  # pragma: no cover - 仅影响展示
        return super().__str__()


class ExpressionError(PipelineError):
    """表达式求值失败（或包含不安全语法）。"""
