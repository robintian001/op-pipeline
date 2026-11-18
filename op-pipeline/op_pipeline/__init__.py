"""op_pipeline —— 算子级、配置驱动的 Python 流水线框架。

最小用法::

    from op_pipeline import PipelineSpec, run_pipeline, default_registry

    spec = PipelineSpec.from_file("pipeline.toml")
    report, values = run_pipeline(spec, registry=default_registry, workdir=".")
    print(report.summary())

自定义算子::

    from op_pipeline import Operator, op, Param

    @op("demo.double")
    class Double(Operator):
        inputs = [("value", int)]
        outputs = [("value", int)]
        params = {"times": Param(int, default=2)}
        def run(self, ctx, value, times):
            return {"value": value * times}
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Mapping

from .cache import CacheStore, fingerprint
from .context import NodeContext, RunContext
from .engine import (
    Engine,
    Hook,
    NodeResult,
    RunReport,
    run_pipeline,
    run_pipeline_async,
)
from .errors import (
    ConfigError,
    CycleError,
    DuplicateOperatorError,
    ExpressionError,
    OperatorNotFoundError,
    ParamValidationError,
    PipelineError,
    RefResolutionError,
    SpecValidationError,
)
from .executor import Executor, ProcessExecutor, ThreadExecutor
from .remote import QueueWorkerExecutor  # M5-1：共享队列文件 + 结果目录的跨进程执行器
from .handlestore import HandleStore, InMemoryHandleStore, SharedHandleStore, SpillHandleStore
from .expression import evaluate, evaluate_condition, register_function
from .operator import Operator
from .params import Param
from .ports import Port, as_ports, type_label
from .registry import Registry, default_registry, load_plugins, op
from .spec import NodeSpec, PipelineSpec, Plan, PlanNode, RefResolver, build_plan
from .store import RunStore

logging.getLogger("op_pipeline").addHandler(logging.NullHandler())

__all__ = [
    "CacheStore",
    "ConfigError",
    "CycleError",
    "DuplicateOperatorError",
    "Engine",
    "ExpressionError",
    "Executor",
    "HandleStore",
    "Hook",
    "InMemoryHandleStore",
    "NodeContext",
    "NodeResult",
    "NodeSpec",
    "Operator",
    "OperatorNotFoundError",
    "Param",
    "ParamValidationError",
    "PipelineError",
    "PipelineSpec",
    "Plan",
    "PlanNode",
    "Port",
    "ProcessExecutor",
    "QueueWorkerExecutor",
    "RefResolver",
    "Registry",
    "RunContext",
    "RunReport",
    "RunStore",
    "SharedHandleStore",
    "SpecValidationError",
    "SpillHandleStore",
    "ThreadExecutor",
    "build_plan",
    "default_registry",
    "evaluate",
    "evaluate_condition",
    "fingerprint",
    "load_plugins",
    "op",
    "register_function",
    "run_pipeline",
    "run_pipeline_async",
    "type_label",
]


def load_builtin_ops() -> list[str]:
    """加载内置算子库（导入 op_pipeline 时自动执行一次，重复调用是幂等的）。"""
    return load_plugins(package="op_pipeline.ops")


def run(
    config: str | Path,
    *,
    workdir: str | Path | None = None,
    overrides: Mapping[str, Any] | None = None,
    cache_dir: str | Path | None = ".pipeline_cache",
    max_workers: int | None = None,
    fail_fast: bool = False,
    plugins: tuple[str, ...] = (),
    plugin_modules: tuple[str, ...] = (),
    registry: Registry | None = None,
    logger: logging.Logger | None = None,
    executor: str | Executor | None = None,
    handle_store: Any = None,
    run_store: Any = None,
    resume_run_id: str | None = None,
    hooks: list[Any] | None = None,
) -> tuple[RunReport, dict[str, Any]]:
    """从配置文件直接跑一遍，返回 (报告, 全部句柄值)。

    - ``plugins``：文件路径插件（相对配置文件所在目录解析）；
    - ``plugin_modules``：模块路径插件，如 ``("my_company.ops",)``；
      受 Python import 幂等性限制，已导入过的模块不会重新执行；
    - 传 ``registry`` 可获得算子集隔离的运行：插件加载进该表、内置算子自动补齐、
      全局 ``default_registry`` 不受任何影响；不传则与历史行为完全一致；
    - ``handle_store``：自定义句柄存储（如 ``SpillHandleStore``），透传给
      ``run_pipeline``；
    - ``run_store``：SQLite 运行记录（``RunStore``），透传给 ``run_pipeline``，
      默认 None = 不落库（连接生命周期由调用方管理）；
    - ``resume_run_id``（M2-6）：断点续跑——配合 ``run_store`` 参考该 run 的
      nodes 记录，节点 cache_key 一致且缓存可命中时复用并标记 [resumed]，
      失败段与下游照常重跑；默认 None = 普通运行。
    - ``hooks``（M2-7）：执行事件钩子列表（on_node_start / on_node_finish /
      on_run_finish），节点并行时可能被并发调用，异常只记告警不影响流水线；
      默认 None = 无钩子。
    - ``executor``（M5-1）：None / 字符串快捷方式（"thread"/"process"/"queue"）/
      任意 ``Executor`` 实例；"queue" 与实例语义详见 :func:`run_pipeline` docstring。
    """
    config_path = Path(config).expanduser().resolve()
    load_plugins(
        modules=plugin_modules,
        paths=plugins,
        base_dir=config_path.parent,
        registry=registry,
    )
    spec = PipelineSpec.from_file(config_path)
    return run_pipeline(
        spec,
        registry=registry if registry is not None else default_registry,
        workdir=workdir or config_path.parent,
        overrides=overrides,
        cache_dir=cache_dir,
        max_workers=max_workers,
        fail_fast=fail_fast,
        logger=logger,
        executor=executor,
        handle_store=handle_store,
        run_store=run_store,
        resume_run_id=resume_run_id,
        hooks=hooks,
    )


load_builtin_ops()
