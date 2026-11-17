"""执行引擎：把编译好的计划跑成结果。

关键行为（也是配置化框架最容易含糊的部分，这里明确写死）：

1. **调度**：按依赖就绪驱动，无依赖关系的节点并行（线程池），
   与配置里节点的书写顺序无关。
2. **失败传播**：节点失败后，依赖它的下游节点标记为 ``skipped``，
   并行分支继续跑（除非 ``fail_fast=True``）。
3. **``when`` 条件**：为假时节点跳过，下游同样跳过。
4. **重试**：``retries`` 次指数退避式重试；配置类错误（写错了参数）不重试。
5. **超时**：``timeout`` 秒为软超时——到点标记失败，但底层线程无法强杀，
   这一点在 README 里作为已知限制写明。
6. **缓存**：``cache=True`` 且算子声明确定性时，按内容指纹复用上次输出。
7. **取消**：``cancel_event`` 置位或 :meth:`Engine.cancel` 之后，未调度节点标记
   skipped、在跑节点自然结束，报告状态为 partial。
"""
from __future__ import annotations

import logging
import os
import threading
import time
import traceback
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping

from .cache import CacheStore
from .context import RunContext
from .errors import ConfigError, RefResolutionError
from .expression import evaluate_condition
from .spec import Plan, PlanNode, RefResolver, check_handle_types

log = logging.getLogger("op_pipeline.engine")

SUCCESS = "success"
FAILED = "failed"
SKIPPED = "skipped"

_TRACEBACK_LIMIT = 2000


def _clip_traceback(text: str, limit: int = _TRACEBACK_LIMIT) -> str:
    """截断堆栈：保留末尾（异常抛出点最有排障价值），超出部分以省略标记提示。"""
    if len(text) <= limit:
        return text
    return f"...（前 {len(text) - limit} 字符已省略）...\n{text[-limit:]}"


# --------------------------------------------------------------------------- #
# 结果对象
# --------------------------------------------------------------------------- #
def _describe(value: Any) -> str:
    if isinstance(value, (list, tuple)):
        return f"{type(value).__name__}[{len(value)}]"
    if isinstance(value, dict):
        return f"dict[{len(value)}]"
    if isinstance(value, str):
        text = value if len(value) <= 60 else value[:57] + "..."
        return f"str({text!r})"
    return f"{type(value).__name__}"


@dataclass
class NodeResult:
    node_id: str
    op: str
    status: str = SUCCESS
    started_at: float = 0.0
    duration: float = 0.0
    attempts: int = 0
    cached: bool = False
    error: str | None = None
    outputs: dict[str, str] = field(default_factory=dict)
    note: str = ""
    # 失败时的异常堆栈（截断保留末尾；成功时为空串）
    traceback: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.node_id,
            "op": self.op,
            "status": self.status,
            "duration": round(self.duration, 4),
            "attempts": self.attempts,
            "cached": self.cached,
            "outputs": self.outputs,
            "error": self.error,
            "note": self.note,
            "traceback": self.traceback,
        }


@dataclass
class RunReport:
    pipeline: str
    version: str
    run_id: str
    status: str
    started_at: float
    finished_at: float = 0.0
    nodes: list[NodeResult] = field(default_factory=list)
    outputs: dict[str, str] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)
    params: dict[str, Any] = field(default_factory=dict)
    artifacts_dir: str = ""

    @property
    def duration(self) -> float:
        return max(0.0, self.finished_at - self.started_at)

    @property
    def ok(self) -> bool:
        return self.status == SUCCESS

    def node(self, node_id: str) -> NodeResult:
        for item in self.nodes:
            if item.node_id == node_id:
                return item
        raise KeyError(node_id)

    def to_dict(self) -> dict[str, Any]:
        return {
            "pipeline": self.pipeline,
            "version": self.version,
            "run_id": self.run_id,
            "status": self.status,
            "duration": round(self.duration, 4),
            "started_at": time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(self.started_at)),
            "params": self.params,
            "outputs": self.outputs,
            "warnings": self.warnings,            "artifacts_dir": self.artifacts_dir,
            "nodes": [n.to_dict() for n in self.nodes],
        }

    def to_json(self, path: str | Path) -> Path:
        import json

        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(self.to_dict(), ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        return target

    def summary(self) -> str:
        icon = {"success": "✓", "failed": "✗", "skipped": "·"}
        lines = [
            f"pipeline : {self.pipeline} (v{self.version})",
            f"run_id   : {self.run_id}",
            f"status   : {self.status}   耗时 {self.duration:.2f}s",
            "",
            f"{'节点':<20}{'状态':<10}{'耗时':>9}{'尝试':>6}  算子",
            "-" * 72,
        ]
        for node in self.nodes:
            mark = icon.get(node.status, "?")
            flag = " [cache]" if node.cached else ""
            lines.append(
                f"{node.node_id:<20}{mark} {node.status:<8}{node.duration:>8.2f}s{node.attempts:>6}  {node.op}{flag}"
            )
            if node.error:
                lines.append(f"{'':<20}└─ {node.error.splitlines()[0][:120]}")
            if node.note:
                lines.append(f"{'':<20}└─ {node.note}")
        if self.outputs:
            lines.append("")
            lines.append("输出：")
            for name, value in self.outputs.items():
                lines.append(f"  {name:<16} {_describe(value)}")
        if self.warnings:
            lines.append("")
            lines.append("告警：")
            for warning in self.warnings:
                lines.append(f"  - {warning}")
        return "\n".join(lines)


# --------------------------------------------------------------------------- #
# 引擎
# --------------------------------------------------------------------------- #
class Engine:
    """执行一个 :class:`~op_pipeline.spec.Plan`。

    一个 Engine 实例只负责一次运行（句柄表挂在实例上），
    需要重跑请新建 Engine，否则会复用上一次的中间结果。
    """

    def __init__(
        self,
        plan: Plan,
        context: RunContext,
        *,
        cache_store: CacheStore | None = None,
        max_workers: int | None = None,
        fail_fast: bool = False,
        cancel_event: threading.Event | None = None,
    ) -> None:
        self.plan = plan
        self.context = context
        self.cache = cache_store or CacheStore(None)
        self.max_workers = max_workers or min(8, max(1, (os.cpu_count() or 2)))
        self.fail_fast = fail_fast

        self.handles: dict[str, Any] = {}
        self._lock = threading.Lock()
        self._results: dict[str, NodeResult] = {}
        self._completed: dict[str, str] = {}
        self._aborted = False
        self._warnings: list[str] = []
        # 取消：支持共享外部事件或 Engine.cancel()，语义见模块 docstring 第 7 条
        self._cancel_event = cancel_event if cancel_event is not None else threading.Event()
        self._cancel_reason = "cancel_event 被置位"
        self._cancel_announced = False

    def cancel(self, reason: str = "调用方请求取消") -> None:
        """请求取消本次运行（线程安全，可从其它线程调用）。

        在跑节点自然结束；尚未调度的节点将标记为 skipped，报告状态为 partial。
        也可在构造时传入共享的 ``cancel_event``，由宿主直接 set() 触发同样语义。
        """
        self._cancel_reason = reason
        self._cancel_event.set()

    # ------------------------------------------------------------------ #
    def run(self) -> RunReport:
        started = time.time()
        report = RunReport(
            pipeline=self.plan.spec.name,
            version=self.plan.spec.version,
            run_id=self.context.run_id,
            status=SUCCESS,
            started_at=started,
            params=dict(self.plan.params),
            artifacts_dir=str(self.context.artifacts_dir),
        )

        self.context.logger.info(
            "开始运行 %s（%d 个节点，最大并发 %d）",
            self.plan.spec.name,
            len(self.plan.nodes),
            self.max_workers,
        )

        pending = list(self.plan.order)
        running: dict[Future, str] = {}

        with ThreadPoolExecutor(max_workers=self.max_workers, thread_name_prefix="op") as pool:
            while pending or running:
                progressed = self._dispatch(pending, running, pool)
                if running:
                    done, _ = wait(list(running), return_when=FIRST_COMPLETED)
                    for future in done:
                        node_id = running.pop(future)
                        self._collect(node_id, future)
                        progressed = True
                    continue
                if not progressed:
                    # 既没有可调度节点也没有在跑的节点：依赖成环或全部被跳过
                    for node_id in pending:
                        self._mark_skipped(node_id, "依赖无法满足（上游未执行或已跳过）")
                    pending.clear()

        # 汇总
        for node_id in self.plan.order:
            if node_id in self._results:
                report.nodes.append(self._results[node_id])

        for name, ref in self.plan.pipeline_outputs.items():
            try:
                report.outputs[name] = RefResolver(self.plan.params, self.handles).resolve(ref)
            except RefResolutionError as exc:
                report.warnings.append(f"输出 '{name}' 无法解析：{exc}")

        hard_failures = [n for n in report.nodes if n.status == FAILED and not self._allowed_failure(n.node_id)]
        skipped = [n for n in report.nodes if n.status == SKIPPED]
        really_failed = [n for n in report.nodes if n.status == FAILED]

        if hard_failures:
            report.status = FAILED
        elif really_failed or skipped:
            report.status = "partial"
        else:
            report.status = SUCCESS

        # 汇总阶段：端口软类型告警（值照常传递，只提示不符；缺失的句柄天然跳过）
        self._warnings.extend(check_handle_types(self.plan, self.handles))
        report.warnings.extend(self._warnings)
        report.finished_at = time.time()
        self.context.logger.info(
            "运行结束 %s：%s，耗时 %.2fs",
            report.run_id,
            report.status,
            report.duration,
        )
        return report

    # ------------------------------------------------------------------ #
    # 调度
    # ------------------------------------------------------------------ #
    def _allowed_failure(self, node_id: str) -> bool:
        return self.plan.node(node_id).spec.allow_failure

    def _dispatch(
        self,
        pending: list[str],
        running: dict[Future, str],
        pool: ThreadPoolExecutor,
    ) -> bool:
        progressed = False
        if self._cancel_event.is_set():
            if not self._cancel_announced:
                self._warnings.append(
                    f"运行被取消：{self._cancel_reason}；在跑节点自然结束，未调度节点跳过"
                )
                self._cancel_announced = True
            for node_id in list(pending):
                self._mark_skipped(node_id, f"运行已被取消：{self._cancel_reason}")
                pending.remove(node_id)
            return progressed
        if self._aborted:
            for node_id in list(pending):
                self._mark_skipped(node_id, "运行已被中断（fail_fast）")
                pending.remove(node_id)
            return progressed

        for node_id in list(pending):
            node = self.plan.node(node_id)

            if any(dep not in self._completed for dep in node.depends_on):
                continue

            blocked = [
                dep for dep in node.depends_on
                if self._completed[dep] in (FAILED, SKIPPED)
            ]
            if blocked:
                self._mark_skipped(node_id, f"上游节点 {', '.join(blocked)} 未成功，缺少输入")
                pending.remove(node_id)
                progressed = True
                continue

            if node.spec.when:
                try:
                    if not self._eval_when(node):
                        self._mark_skipped(node_id, f"when 条件为假：{node.spec.when}")
                        pending.remove(node_id)
                        progressed = True
                        continue
                except Exception as exc:
                    self._mark_skipped(node_id, f"when 条件求值失败：{exc}")
                    pending.remove(node_id)
                    progressed = True
                    continue

            if len(running) >= self.max_workers:
                break

            pending.remove(node_id)
            running[pool.submit(self._run_node, node)] = node_id
            progressed = True
        return progressed

    def _eval_when(self, node: PlanNode) -> bool:
        resolver = RefResolver(self.plan.params, self.handles)
        env: dict[str, Any] = {}
        for port, ref in node.spec.inputs.items():
            try:
                env[port] = resolver.resolve(ref)
            except RefResolutionError:
                continue
        for key, value in node.spec.params.items():
            try:
                env[key] = resolver.resolve(value)
            except RefResolutionError:
                continue
        env["inputs"] = {k: env[k] for k in node.spec.inputs if k in env}
        # params 命名空间与扁平参数名同语义（解析后的值，跳过解析失败的键）；
        # 原始配置值（可能仍是 '${p.x}' 串）保留在 raw_params。
        env["params"] = {k: env[k] for k in node.spec.params if k in env}
        env["raw_params"] = dict(node.spec.params)
        return evaluate_condition(str(node.spec.when), env)

    def _collect(self, node_id: str, future: Future) -> None:
        try:
            result: NodeResult = future.result()
        except Exception as exc:  # pragma: no cover - _run_node 内部已兜住
            result = NodeResult(node_id=node_id, op=self.plan.node(node_id).spec.op, status=FAILED, error=str(exc))
            result.duration = 0.0
        self._results[node_id] = result
        self._completed[node_id] = result.status

        if result.status == FAILED and not self._allowed_failure(node_id) and self.fail_fast:
            self._aborted = True

    def _mark_skipped(self, node_id: str, note: str) -> None:
        node = self.plan.node(node_id)
        result = NodeResult(node_id=node_id, op=node.spec.op, status=SKIPPED, note=note)
        self._results[node_id] = result
        self._completed[node_id] = SKIPPED
        self.context.logger.info("节点 %s 跳过：%s", node_id, note)

    # ------------------------------------------------------------------ #
    # 单节点执行
    # ------------------------------------------------------------------ #
    def _run_node(self, node: PlanNode) -> NodeResult:
        ctx = self.context.for_node(node.spec.id, node.spec.op)
        result = NodeResult(node_id=node.spec.id, op=node.spec.op, started_at=time.time())
        handles_snapshot = dict(self.handles)
        resolver = RefResolver(self.plan.params, handles_snapshot)

        try:
            inputs = {
                port: resolver.resolve(ref)
                for port, ref in node.spec.inputs.items()
            }
            params = resolver.resolve(node.spec.params)
            ctx.params = params
            instance = node.operator if node.operator is not None else node.operator_cls(**params)

            cache_key = None
            if (
                node.spec.cache
                and self.cache.enabled
                and getattr(instance, "deterministic", True)
            ):
                cache_key = self.cache.compute_key(
                    node.spec.op, instance.version, params, inputs
                )
                cached = self.cache.load(cache_key)
                if cached is not None:
                    outputs = cached
                    result.cached = True
                    result.status = SUCCESS
                    result.duration = time.time() - result.started_at
                    result.outputs = {k: _describe(v) for k, v in outputs.items()}
                    self._publish(node, outputs)
                    self.context.logger.info("节点 %s 命中缓存", node.spec.id)
                    return result

            outputs = self._execute_with_retry(instance, ctx, inputs, node, result)
            result.status = SUCCESS
            result.outputs = {k: _describe(v) for k, v in outputs.items()}

            if cache_key is not None:
                self.cache.store(cache_key, outputs)

            self._publish(node, outputs)
        except _NodeFailure as failure:
            result.status = FAILED
            result.error = f"{type(failure.cause).__name__}: {failure.cause}"
            result.traceback = _clip_traceback(traceback.format_exc())
        except Exception as exc:
            result.status = FAILED
            result.error = f"{type(exc).__name__}: {exc}"
            result.traceback = _clip_traceback(traceback.format_exc())
        finally:
            result.duration = time.time() - result.started_at
            if result.status == FAILED:
                ctx.logger.error("节点 %s 失败：%s", node.spec.id, result.error)
            else:
                ctx.logger.info(
                    "节点 %s 完成（%.2fs%s）",
                    node.spec.id,
                    result.duration,
                    "，缓存" if result.cached else "",
                )
        return result

    def _publish(self, node: PlanNode, outputs: Mapping[str, Any]) -> None:
        with self._lock:
            for port, value in outputs.items():
                handle = node.handles.get(port)
                if handle:
                    self.handles[handle] = value

    def _execute_with_retry(
        self,
        instance: Any,
        ctx: Any,
        inputs: Mapping[str, Any],
        node: PlanNode,
        result: NodeResult,
    ) -> dict[str, Any]:
        attempts = max(0, node.spec.retries) + 1
        last_error: BaseException | None = None
        for attempt in range(1, attempts + 1):
            result.attempts = attempt
            try:
                return self._execute_once(instance, ctx, inputs, node.spec.timeout)
            except ConfigError as exc:
                # 配置/契约错误重试也没意义，直接抛
                raise _NodeFailure(exc) from exc
            except Exception as exc:
                last_error = exc
                if attempt >= attempts:
                    break
                delay = max(0.0, node.spec.retry_delay) * (2 ** (attempt - 1))
                ctx.logger.warning(
                    "节点 %s 第 %d/%d 次尝试失败：%s；%.2fs 后重试",
                    node.spec.id, attempt, attempts, exc, delay,
                )
                time.sleep(delay)
        # from 显式链接根因：traceback.format_exc() 才能带出算子内原始异常的完整帧
        raise _NodeFailure(last_error) from last_error  # type: ignore[arg-type]

    def _execute_once(
        self,
        instance: Any,
        ctx: Any,
        inputs: Mapping[str, Any],
        timeout: float | None,
    ) -> dict[str, Any]:
        if not timeout:
            return instance.execute(ctx, inputs)

        box: dict[str, Any] = {}

        def target() -> None:
            try:
                box["ok"] = instance.execute(ctx, inputs)
            except BaseException as exc:  # noqa: BLE001 - 需要捕获后回到主线程
                box["err"] = exc

        worker = threading.Thread(target=target, daemon=True, name=f"op-{ctx.node_id}")
        worker.start()
        worker.join(timeout)
        if worker.is_alive():
            raise TimeoutError(f"节点 {ctx.node_id} 执行超过 {timeout}s（软超时，底层线程仍在运行）")
        if "err" in box:
            raise box["err"]
        return box["ok"]


class _NodeFailure(Exception):
    """内部包装，保留原始异常用于展示。"""

    def __init__(self, cause: BaseException) -> None:
        self.cause = cause
        super().__init__(str(cause))


# --------------------------------------------------------------------------- #
# 便捷入口
# --------------------------------------------------------------------------- #
def run_pipeline(
    spec: Any,
    *,
    registry: Any,
    workdir: str | Path = ".",
    overrides: Mapping[str, Any] | None = None,
    cache_dir: str | Path | None = None,
    max_workers: int | None = None,
    fail_fast: bool = False,
    cancel_event: threading.Event | None = None,
    run_id: str | None = None,
    logger: logging.Logger | None = None,
) -> tuple[RunReport, dict[str, Any]]:
    """编译 + 执行一步到位，返回值与句柄表。

    典型的库内调用::

        report, values = run_pipeline(spec, registry=default_registry,
                                      workdir=".", overrides={"threshold": 10})
        values["stat.total"]
    """
    from .spec import build_plan

    plan = build_plan(spec, registry, overrides=overrides)
    context = RunContext.create(
        pipeline=plan.spec.name,
        params=plan.params,
        workdir=workdir,
        run_id=run_id,
        logger=logger,
        cache=None,
    )
    cache_store = CacheStore(cache_dir, base_dir=workdir)
    engine = Engine(plan, context, cache_store=cache_store, max_workers=max_workers,
                    fail_fast=fail_fast, cancel_event=cancel_event)
    report = engine.run()
    return report, dict(engine.handles)
