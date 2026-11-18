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

import asyncio
import functools
import inspect
import logging
import os
import threading
import time
import traceback
from concurrent.futures import Future
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Mapping, Protocol

from .cache import CacheStore
from .context import RunContext
from .errors import ConfigError, RefResolutionError
from .executor import Executor, ProcessExecutor, ThreadExecutor, _child_run_op
from .expression import evaluate_condition
from .handlestore import HandleStore, InMemoryHandleStore, SharedHandleStore
from .remote import QueueWorkerExecutor, _remote_run
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
    # M2-5：命中缓存计算出的 cache_key（供 RunStore 记录与 M2-6 断点续跑比对）
    cache_key: str = ""
    # M2-6：断点续跑命中（缓存条目 + 参考 run 的成功记录 + key 一致）时为 True
    resumed: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.node_id,
            "op": self.op,
            "status": self.status,
            "duration": round(self.duration, 4),
            "attempts": self.attempts,
            "cached": self.cached,
            "resumed": self.resumed,
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
            flag = " [resumed]" if node.resumed else (" [cache]" if node.cached else "")
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
# 执行事件钩子（M2-7）
# --------------------------------------------------------------------------- #
class Hook(Protocol):
    """执行事件钩子协议（M2-7）：实现其中任意一个或多个方法即可挂到
    ``Engine(hooks=[...])`` / ``run_pipeline(hooks=[...])``。

    - ``on_node_start(node_id, op)``：节点开始执行（含命中缓存提前返回前）；
    - ``on_node_finish(node_id, status, duration, cached)``：节点结果确定——
      成功、失败、缓存命中都触发；被跳过的节点（when 为假 / 上游失败 /
      取消）从未开始执行，不触发任何钩子；
    - ``on_run_finish(report)``：运行汇总完毕（报告状态已定），总在最后。

    节点并行执行时钩子可能被多个线程并发调用，实现需自带同步
    （文档进度条示例即用锁）；钩子抛出的异常只记入报告告警，不影响流水线。
    """

    def on_node_start(self, node_id: str, op: str) -> None: ...

    def on_node_finish(self, node_id: str, status: str, duration: float, cached: bool) -> None: ...

    def on_run_finish(self, report: RunReport) -> None: ...


# --------------------------------------------------------------------------- #
# 引擎
# --------------------------------------------------------------------------- #
class Engine:
    """执行一个 :class:`~op_pipeline.spec.Plan`。

    一个 Engine 实例只负责一次运行（句柄存储挂在实例上），
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
        executor: str | Executor | None = None,
        handle_store: HandleStore | None = None,
        run_store: Any = None,
        resume_run_id: str | None = None,
        hooks: list[Any] | None = None,
    ) -> None:
        # M5-1：运行级执行器三元态——字符串选择器 / Executor 实例 / None（默认线程池）。
        if isinstance(executor, str) and executor not in ("thread", "process", "queue"):
            raise ConfigError(f"executor 只能是 thread/process/queue，实际是 {executor!r}")
        if executor is not None and not isinstance(executor, (str, Executor)):
            raise ConfigError(
                "executor 只能是字符串选择器（thread/process/queue）或实现 "
                "Executor 协议（submit/wait/shutdown）的实例，"
                f"实际是 {type(executor).__name__}"
            )
        self.plan = plan
        self.context = context
        self.cache = cache_store or CacheStore(None)
        self.max_workers = max_workers or min(8, max(1, (os.cpu_count() or 2)))
        self.fail_fast = fail_fast
        # 告警先于执行器形态判定初始化：节点级声明被忽略时要在这里留痕
        self._warnings: list[str] = []

        # M5-1：实例路径——引擎直接使用调用方传入的 Executor，不负责其生命周期
        # （归属调用方）；字符串 "queue" 与节点级 executor="queue" 声明由引擎自建
        # 队列执行器（归属引擎，run() 结束时 shutdown）。队列/结果目录固定推导自
        # 本次运行的 workdir（<workdir>/.op_pipeline/remote/ 下，而非随 run 变化的
        # artifacts_dir）——路径跨 run 稳定，worker 可预先启动常驻消费，与 worker
        # CLI 的 --queue/--out 约定一致。
        self.default_executor = executor  # 字符串 / Executor 实例 / None（M2-2 语义）
        self._executor_instance: Executor | None = None
        self._owns_executor = False
        node_level_queue = any(node.spec.executor == "queue" for node in plan.nodes)
        # 节点级 "queue" 声明在运行级为 None/"thread" 时触发自建（运行级显式
        # "process" 优先——用户明确选择了内置执行器，不静默改道，记告警忽略）；
        # 实例模式（含 queue 自建）下全部节点统一经实例分发，节点级声明一律忽略。
        need_queue = executor == "queue" or (
            executor in (None, "thread") and node_level_queue
        )
        if isinstance(executor, str) and executor in ("thread", "process") and node_level_queue:
            self._warnings.append(
                f"存在节点级 executor='queue' 声明，但运行级 executor={executor!r} 优先，"
                "本次运行不会创建队列执行器"
            )
        if need_queue:
            remote_dir = self.context.workdir / ".op_pipeline" / "remote"
            self._executor_instance = QueueWorkerExecutor(
                remote_dir / "jobs.jsonl", remote_dir / "results", poll=0.05
            )
            self._owns_executor = True
        elif isinstance(executor, Executor):
            self._executor_instance = executor
            self._owns_executor = False

        # M5-1：远程（队列）模式标记——主循环据此走 _remote_run 分发与共享句柄读取
        self._remote = isinstance(self._executor_instance, QueueWorkerExecutor)
        # 运行级实例接管节点执行时，节点级 executor 声明被忽略——记告警留痕
        if isinstance(executor, Executor):
            ignored = {"process"} if self._remote else {"process", "queue"}
            for node in plan.nodes:
                if node.spec.executor in ignored:
                    self._warnings.append(
                        f"节点 [{node.spec.id}] 显式 executor={node.spec.executor!r} 被忽略："
                        "运行级传入执行器实例，全部节点统一经该实例执行"
                    )
        elif need_queue:
            for node in plan.nodes:
                if node.spec.executor == "process":
                    self._warnings.append(
                        f"节点 [{node.spec.id}] 显式 executor='process' 被忽略："
                        "本次运行为远程队列模式，不创建进程池"
                    )
        self._remote_shared_dir: Path | None = None

        # M2-3：句柄存储抽象——写收敛在 _publish(put)，读收敛在 RefResolver(snapshot)
        self.store: HandleStore = handle_store or InMemoryHandleStore()
        if self._remote:
            # M5-1：远程模式共享句柄存储——引擎从执行器 out 目录推导共享目录；
            # 调用方显式传 handle_store 则优先（此时跨进程一致性由调用方负责）
            self._remote_shared_dir = Path(self._executor_instance.out) / "handles"
            if handle_store is None:
                self.store = SharedHandleStore(shared_dir=self._remote_shared_dir)
        self._lock = threading.Lock()
        self._results: dict[str, NodeResult] = {}
        self._completed: dict[str, str] = {}
        self._aborted = False
        # 取消：支持共享外部事件或 Engine.cancel()，语义见模块 docstring 第 7 条
        self._cancel_event = cancel_event if cancel_event is not None else threading.Event()
        self._cancel_reason = "cancel_event 被置位"
        self._cancel_announced = False
        # 运行级执行器缺省（M2-2）：节点显式 executor 优先，其次运行级，最后 thread。
        # 进程池只在"有进程节点"时创建：纯线程运行保持零进程开销与行为零变化。
        # M5-1：实例路径（含 "queue"/节点级声明触发的自建队列执行器）下引擎不再
        # 额外创建进程池——实例是唯一执行后端，全部节点经它分发；节点级 "process"
        # 声明被忽略并记告警（见上）。
        if isinstance(executor, Executor) or need_queue:
            need_process = False
        else:
            need_process = executor == "process" or any(
                node.spec.executor == "process" for node in plan.nodes
            )
        self._process_pool = (
            ProcessExecutor(on_fallback=self._warnings.append) if need_process else None
        )
        # M2-4：落盘型句柄存储（如 SpillHandleStore）实现 attach(context) 时，
        # 由引擎把落盘目录挂到本次运行的 artifacts_dir 下；普通实现无此钩子。
        attach = getattr(self.store, "attach", None)
        if callable(attach):
            attach(context)
        # M2-5：可选的 SQLite 运行记录（RunStore）；默认 None = 完全不触碰 SQLite
        self.run_store = run_store
        # M2-6：断点续跑——仅当同时给定了 run_store 与 resume_run_id 才生效；
        # 语义：节点 cache_key 与参考 run 的成功记录一致且缓存可命中时标记 resumed。
        # 参考 run 的节点记录在构造时一次预载进内存（SQLite 连接有线程亲和性，
        # 不能在工作线程里查），worker 线程只读内存 map。
        self.resume_run_id = resume_run_id
        self._resume_map: dict[str, tuple[str, str | None]] = {}
        if self.resume_run_id is not None and self.run_store is not None:
            for row in self.run_store.query(
                "SELECT node_id, status, cache_key FROM nodes WHERE run_id = ?",
                (self.resume_run_id,),
            ):
                self._resume_map[row[0]] = (row[1], row[2])
        # M2-7：执行事件钩子——广播给全部钩子，异常只记告警
        self.hooks = list(hooks or [])

    def _emit(self, event: str, *args: Any) -> None:
        """M2-7：把事件广播给全部钩子；钩子异常只记告警，不影响流水线。"""
        for hook in self.hooks:
            fn = getattr(hook, event, None)
            if callable(fn):
                try:
                    fn(*args)
                except Exception as exc:  # noqa: BLE001 钩子是旁观者，异常必须隔离
                    self._warnings.append(f"hook {event} 异常：{type(exc).__name__}: {exc}")

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

        # M5-1：执行后端形态——
        # - 远程（队列）模式：主循环 pool 仍是引擎内部 ThreadExecutor，远程节点
        #   经内部线程任务包装（_run_remote_node：主进程解析载荷 → 队列 submit →
        #   阻塞等结果），重试/缓存/软超时都在主进程侧——主循环 wait 的
        #   first_completed 语义完全不变（远程节点在主循环看来仍是普通线程任务）；
        # - 其他外部实例直接作为主循环 pool 使用（协议兼容，不 shutdown）；
        # - 自建实例（"queue" / 节点级声明触发）与默认线程池由引擎创建并 shutdown。
        if self._remote:
            pool = ThreadExecutor(max_workers=self.max_workers)
            owned = True
        elif self._executor_instance is not None:
            pool = self._executor_instance
            owned = False
        else:
            pool = ThreadExecutor(max_workers=self.max_workers)
            owned = True
        try:
            while pending or running:
                progressed = self._dispatch(pending, running, pool)
                if running:
                    done = pool.wait(running, first_completed=True)
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
        finally:
            if owned:
                pool.shutdown()
            if self._owns_executor and self._executor_instance is not None:
                # 引擎自建的队列执行器（"queue" / 节点级声明触发）随运行收尾
                self._executor_instance.shutdown()

        if self._process_pool is not None:
            self._process_pool.shutdown()

        # 汇总
        for node_id in self.plan.order:
            if node_id in self._results:
                report.nodes.append(self._results[node_id])

        for name, ref in self.plan.pipeline_outputs.items():
            try:
                report.outputs[name] = RefResolver(self.plan.params, self.store).resolve(ref)
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
        self._warnings.extend(check_handle_types(self.plan, self.store.snapshot()))
        report.warnings.extend(self._warnings)
        report.finished_at = time.time()
        # M2-5：运行级记录落库（run_id 主键，可覆盖同 run_id 的重跑记录）
        if self.run_store is not None:
            self.run_store.record_run(
                report.run_id,
                report.pipeline,
                report.version,
                report.status,
                report.started_at,
                report.finished_at,
                report.params,
            )
        self.context.logger.info(
            "运行结束 %s：%s，耗时 %.2fs",
            report.run_id,
            report.status,
            report.duration,
        )
        # M2-7：运行汇总完毕（报告状态已定），广播 on_run_finish——总在最后
        self._emit("on_run_finish", report)
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
        pool: Executor,
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
            if self._remote:
                # M5-1：远程模式——submit 引擎方法 _run_remote_node（主循环 pool
                # 是内部 ThreadExecutor，无需可 pickle），由它构造载荷并经队列
                # 执行器分发，重试/缓存/软超时都留在主进程侧
                running[pool.submit(self._run_remote_node, node)] = node_id
            else:
                running[pool.submit(self._run_node, node)] = node_id
            progressed = True
        return progressed

    def _eval_when(self, node: PlanNode) -> bool:
        resolver = RefResolver(self.plan.params, self.store)
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
        except Exception as exc:  # pragma: no cover - _run_node/_run_remote_node 内部已兜住
            result = NodeResult(node_id=node_id, op=self.plan.node(node_id).spec.op, status=FAILED, error=str(exc))
            result.duration = 0.0
        self._results[node_id] = result
        self._completed[node_id] = result.status

        # M2-5：节点级记录落库（_collect 是真实执行节点的收口点）
        if self.run_store is not None:
            self._record_node(result)

        if result.status == FAILED and not self._allowed_failure(node_id) and self.fail_fast:
            self._aborted = True

    def _record_node(self, result: NodeResult) -> None:
        """把单个节点结果写入 RunStore（成功/失败/跳过都记录，nodes 表与报告一致）。"""
        self.run_store.record_node(
            self.context.run_id,
            result.node_id,
            result.op,
            result.status,
            result.attempts,
            result.cached,
            result.error,
            result.cache_key or None,
            result.started_at + result.duration,
        )

    def _mark_skipped(self, node_id: str, note: str) -> None:
        node = self.plan.node(node_id)
        result = NodeResult(node_id=node_id, op=node.spec.op, status=SKIPPED, note=note)
        self._results[node_id] = result
        self._completed[node_id] = SKIPPED
        # M2-5：跳过节点同样入库（保证 nodes 表与 RunReport 的节点列表一致）
        if self.run_store is not None:
            self._record_node(result)
        self.context.logger.info("节点 %s 跳过：%s", node_id, note)

    # ------------------------------------------------------------------ #
    # 远程执行（M5-1）
    # ------------------------------------------------------------------ #
    def _remote_payload(
        self, node: PlanNode, params: Mapping[str, Any], inputs: Mapping[str, Any]
    ) -> dict[str, Any]:
        """组装可 pickle 的节点载荷（契约见 ``remote._remote_run`` docstring）。

        ``params``/``inputs`` 是主进程 ``RefResolver`` 解析后的实际值——引用
        解析与快照语义留在主进程（与线程路径 ``_run_node`` 一致），worker 只
        执行算子并把输出写进共享句柄目录；载荷全部为可 pickle 类型。
        """
        return {
            "node_id": node.spec.id,
            "op": node.spec.op,
            "params": dict(params),
            "inputs": dict(inputs),
            "outputs": dict(node.handles),
            "shared_dir": str(self._remote_shared_dir),
            "run": {
                "run_id": self.context.run_id,
                "pipeline": self.plan.spec.name,
                "params": dict(self.plan.params),
                "workdir": str(self.context.workdir),
                "artifacts_dir": str(self.context.artifacts_dir),
            },
        }

    def _remote_once(
        self,
        node: PlanNode,
        params: Mapping[str, Any],
        inputs: Mapping[str, Any],
        timeout: float | None,
    ) -> dict[str, Any]:
        """单次远程执行：载荷提交进队列，阻塞等结果（``timeout`` 即节点软超时）。

        队列任务无法强杀 worker 侧算子——软超时限制不因远程而消失：到点抛
        ``TimeoutError`` 交给重试包装按普通失败处理，worker 侧算子可能仍在
        运行（与线程路径"底层线程仍在跑"的已知限制同语义）。
        """
        payload = self._remote_payload(node, params, inputs)
        assert self._executor_instance is not None  # 仅 _remote 模式进入
        task = self._executor_instance.submit(_remote_run, payload)
        try:
            data = task.result(timeout=timeout)
        except TimeoutError:
            raise TimeoutError(
                f"节点 {node.spec.id} 远程执行超过 {timeout}s"
                "（软超时：队列任务无法强杀，worker 侧算子可能仍在运行）"
            ) from None
        if data.get("status") != "success":
            # 失败已打包进 dict：还原为异常交给重试包装（携带 worker 的 traceback）
            raise _RemoteFailure(data) from None
        return data

    def _run_remote_node(self, node: PlanNode) -> NodeResult:
        """远程节点执行（M5-1）：与 ``_run_node`` 同构，算子改在队列 worker 进程跑。

        - 引用解析在主进程（``RefResolver`` 快照语义与线程路径一致）；worker
          只执行 op.run 并把输出原子发布进共享句柄目录，引擎侧从同一目录读回；
        - 重试/退避沿用引擎包装（主进程侧）；缓存 ``cache_key`` 计算与读写也
          在主进程——命中则根本不发队列任务；``timeout`` 在主进程侧生效（软
          超时，见 ``_remote_once``）；
        - 失败时把 worker 打包的 error/traceback 还原进 ``NodeResult``，下游
          照常跳过。
        """
        ctx = self.context.for_node(node.spec.id, node.spec.op)
        result = NodeResult(node_id=node.spec.id, op=node.spec.op, started_at=time.time())
        self._emit("on_node_start", node.spec.id, node.spec.op)
        resolver = RefResolver(self.plan.params, self.store)
        try:
            inputs = {
                port: resolver.resolve(ref) for port, ref in node.spec.inputs.items()
            }
            params = resolver.resolve(node.spec.params)
            ctx.params = params

            cache_key = None
            if (
                node.spec.cache
                and self.cache.enabled
                and getattr(node.operator_cls, "deterministic", True)
            ):
                source = None
                if getattr(self.cache, "include_source", False):
                    try:
                        source = inspect.getsource(node.operator_cls)
                    except (OSError, TypeError):  # pragma: no cover - 动态类拿不到源码
                        source = None
                cache_key = self.cache.compute_key(
                    node.spec.op,
                    node.operator_cls.version,
                    params,
                    inputs,
                    source=source,
                )
                cached = self.cache.load(cache_key)
                if cached is not None:
                    outputs = cached
                    result.cached = True
                    result.cache_key = cache_key
                    result.status = SUCCESS
                    result.duration = time.time() - result.started_at
                    result.outputs = {k: _describe(v) for k, v in outputs.items()}
                    # M2-6：断点续跑语义与线程路径一致
                    prev = self._resume_map.get(node.spec.id)
                    if prev is not None and prev[0] == SUCCESS and prev[1] == cache_key:
                        result.resumed = True
                    # 缓存命中走引擎写收敛点：值经 store.put 进共享目录（下游可读）
                    self._publish(node, outputs)
                    self.context.logger.info(
                        "节点 %s 命中缓存%s",
                        node.spec.id,
                        f"，续跑自 {self.resume_run_id}" if result.resumed else "",
                    )
                    return result

            result.cache_key = cache_key or ""
            data = self._attempt_loop(
                node,
                ctx,
                result,
                lambda: self._remote_once(node, params, inputs, node.spec.timeout),
            )
            result.status = SUCCESS
            # worker 已把输出原子发布进共享目录；引擎侧从同一目录读回实际值
            raw = {
                port: self.store.get(handle)
                for port, handle in (data.get("outputs") or {}).items()
            }
            result.outputs = {k: _describe(v) for k, v in raw.items()}
            if cache_key is not None:
                self.cache.store(cache_key, raw)
        except _NodeFailure as failure:
            result.status = FAILED
            result.error = f"{type(failure.cause).__name__}: {failure.cause}"
            if isinstance(failure.cause, _RemoteFailure):
                # worker 打包的堆栈：截断保留末尾（与线程路径 format_exc 同语义）
                result.traceback = _clip_traceback(
                    (failure.cause.data or {}).get("traceback") or ""
                )
            else:
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
                    "，续跑" if result.resumed else ("，缓存" if result.cached else ""),
                )
            # M2-7：节点结果确定（成功/失败/缓存命中都触发）
            self._emit(
                "on_node_finish",
                node.spec.id,
                result.status,
                result.duration,
                result.cached,
            )
        return result

    # ------------------------------------------------------------------ #
    # 单节点执行
    # ------------------------------------------------------------------ #
    def _run_node(self, node: PlanNode) -> NodeResult:
        ctx = self.context.for_node(node.spec.id, node.spec.op)
        result = NodeResult(node_id=node.spec.id, op=node.spec.op, started_at=time.time())
        # M2-7：节点开始执行（worker 线程调用，钩子需自行加锁）
        self._emit("on_node_start", node.spec.id, node.spec.op)
        # M2-3：RefResolver 构造时经 store.snapshot() 拍快照，
        # 与原 dict(self.handles) 行为等价（节点开始时的可见句柄集）
        resolver = RefResolver(self.plan.params, self.store)

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
                # M2-5（T7 出债）：可选地把算子类源码摘要纳入 key——
                # 源码变化即缓存失效；默认关闭（include_source=False）保持旧行为
                source = None
                if getattr(self.cache, "include_source", False):
                    try:
                        source = inspect.getsource(type(instance))
                    except (OSError, TypeError):  # pragma: no cover - 动态类拿不到源码
                        source = None
                cache_key = self.cache.compute_key(
                    node.spec.op, instance.version, params, inputs, source=source
                )
                cached = self.cache.load(cache_key)
                if cached is not None:
                    outputs = cached
                    result.cached = True
                    result.cache_key = cache_key
                    result.status = SUCCESS
                    result.duration = time.time() - result.started_at
                    result.outputs = {k: _describe(v) for k, v in outputs.items()}
                    # M2-6：断点续跑——参考 run 里该节点成功且 cache_key 一致才算续跑命中；
                    # 只复用缓存/句柄（产出已在缓存里），不重跑算子
                    prev = self._resume_map.get(node.spec.id)
                    if prev is not None and prev[0] == SUCCESS and prev[1] == cache_key:
                        result.resumed = True
                    self._publish(node, outputs)
                    self.context.logger.info(
                        "节点 %s 命中缓存%s",
                        node.spec.id,
                        f"，续跑自 {self.resume_run_id}" if result.resumed else "",
                    )
                    return result

            result.cache_key = cache_key or ""
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
                    "，续跑" if result.resumed else ("，缓存" if result.cached else ""),
                )
            # M2-7：节点结果确定（成功/失败/缓存命中都触发；finally 保证
            # 缓存命中提前 return 也会发 finish）
            self._emit(
                "on_node_finish",
                node.spec.id,
                result.status,
                result.duration,
                result.cached,
            )
        return result

    def _publish(self, node: PlanNode, outputs: Mapping[str, Any]) -> None:
        # M2-3：写收敛点——句柄写入走 HandleStore（store 自带锁，跨节点并发发布安全）
        for port, value in outputs.items():
            handle = node.handles.get(port)
            if handle:
                self.store.put(handle, value)

    def _attempt_loop(
        self,
        node: PlanNode,
        ctx: Any,
        result: NodeResult,
        once: Callable[[], Any],
    ) -> Any:
        """重试包装（M5-1 自 _execute_with_retry 抽取）：attempts 次指数退避。

        ``once`` 是"单次执行"callable（线程/进程路径传 ``_execute_once``，
        远程路径传 ``_remote_once``）——重试策略与执行方式解耦；
        ``ConfigError``（含引用解析错误）重试也没意义，直接抛。
        """
        attempts = max(0, node.spec.retries) + 1
        last_error: BaseException | None = None
        for attempt in range(1, attempts + 1):
            result.attempts = attempt
            try:
                return once()
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

    def _execute_with_retry(
        self,
        instance: Any,
        ctx: Any,
        inputs: Mapping[str, Any],
        node: PlanNode,
        result: NodeResult,
    ) -> dict[str, Any]:
        return self._attempt_loop(
            node,
            ctx,
            result,
            lambda: self._execute_once(
                instance, ctx, inputs, node.spec.timeout, self._executor_kind(node)
            ),
        )

    def _executor_kind(self, node: PlanNode) -> str:
        return node.spec.executor or self.default_executor or "thread"

    def _execute_in_process(
        self,
        instance: Any,
        ctx: Any,
        inputs: Mapping[str, Any],
        timeout: float | None,
    ) -> dict[str, Any]:
        """进程模式（M2-2）：算子在子进程执行，timeout 由 ProcessExecutor 强制（硬超时）。"""
        run = ctx.run
        ctx_data = (
            ctx.node_id,
            run.run_id,
            run.pipeline,
            dict(ctx.params),
            str(run.workdir),
            str(run.artifacts_dir),
        )
        assert self._process_pool is not None  # 仅在 kind=="process" 且池存在时进入
        task = self._process_pool.submit(
            _child_run_op, instance, ctx_data, dict(inputs), timeout=timeout
        )
        self._process_pool.wait((task,), first_completed=False)
        return task.result()

    def _execute_once(
        self,
        instance: Any,
        ctx: Any,
        inputs: Mapping[str, Any],
        timeout: float | None,
        kind: str = "thread",
    ) -> dict[str, Any]:
        if kind == "process" and self._process_pool is not None:
            return self._execute_in_process(instance, ctx, inputs, timeout)
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


class _RemoteFailure(Exception):
    """远程单次执行失败包装（M5-1）：携带 worker 返回的 error/traceback 文本。

    ``_remote_once`` 把 worker 打包的失败 dict 还原为异常抛给重试包装
    （按普通失败参与重试），重试耗尽后引擎从 ``data`` 里取 traceback 文本
    写进 ``NodeResult.traceback``——不需要跨进程传异常对象。
    """

    def __init__(self, data: dict[str, Any]) -> None:
        self.data = data
        super().__init__(str(data.get("error") or "远程执行失败"))


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
    executor: str | Executor | None = None,
    handle_store: HandleStore | None = None,
    run_store: Any = None,
    resume_run_id: str | None = None,
    hooks: list[Any] | None = None,
    run_id: str | None = None,
    logger: logging.Logger | None = None,
) -> tuple[RunReport, dict[str, Any]]:
    """编译 + 执行一步到位，返回值与句柄表。

    ``handle_store`` 可注入自定义句柄存储（如
    ``SpillHandleStore(threshold=...)``，超阈值落盘）；
    返回的句柄表是全量物化后的 dict（落盘项在此时加载回内存）。
    ``run_store`` 可注入 :class:`~op_pipeline.store.RunStore`（SQLite 运行记录，
    默认 None = 不落库；连接生命周期由调用方管理）。
    ``resume_run_id``（M2-6）配合 ``run_store`` 使用：参考该 run 的 nodes 记录，
    与本次 cache_key 比对，成功段复用缓存并标记 [resumed]，失败段与下游照常重跑。
    ``hooks``（M2-7）执行事件钩子列表：on_node_start / on_node_finish /
    on_run_finish，节点并行时可能被并发调用，异常只记告警不影响流水线。
    ``executor``（M5-1）三种形态：None（默认线程池）；字符串 "thread"/"process"/"queue"
    快捷方式（"queue" 由引擎自建 :class:`~op_pipeline.remote.QueueWorkerExecutor`，
    队列与结果目录固定为 ``<workdir>/.op_pipeline/remote/``——路径跨 run 稳定，
    worker 可预先启动常驻消费）；或任意 ``Executor`` 实例（如
    ``QueueWorkerExecutor(...)`` / ``ThreadExecutor(...)``），引擎把节点执行分发到
    该实例，实例生命周期归调用方、引擎不 shutdown。

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
                    fail_fast=fail_fast, cancel_event=cancel_event, executor=executor,
                    handle_store=handle_store, run_store=run_store,
                    resume_run_id=resume_run_id, hooks=hooks)
    report = engine.run()
    return report, dict(engine.store.snapshot())


async def run_pipeline_async(
    spec: Any,
    *,
    registry: Any,
    workdir: str | Path = ".",
    overrides: Mapping[str, Any] | None = None,
    cache_dir: str | Path | None = None,
    max_workers: int | None = None,
    fail_fast: bool = False,
    cancel_event: threading.Event | None = None,
    executor: str | Executor | None = None,
    handle_store: HandleStore | None = None,
    run_store: Any = None,
    resume_run_id: str | None = None,
    hooks: list[Any] | None = None,
    run_id: str | None = None,
    logger: logging.Logger | None = None,
) -> tuple[RunReport, dict[str, Any]]:
    """async 宿主嵌入（M2-8）：把同步引擎抛到事件循环的默认执行器里跑。

    ``await run_pipeline_async(...)`` 与 :func:`run_pipeline` 的参数、
    返回值完全一致；区别在于不会阻塞事件循环——流水线在
    ``loop.run_in_executor`` 的线程里执行，await 期间同循环的其它
    asyncio 任务照常推进。

    这是"嵌入 async 宿主"的桥接，不是全 asyncio 调度器：调度仍发生在
    线程池内，取消/超时语义与同步版本相同（``asyncio.CancelledError``
    只会放弃 await，不会中途终止正在跑的流水线；需要中途止损仍应使用
    ``cancel_event``）。全 asyncio 调度器不在本里程碑范围。

    典型用法::

        async def main():
            report, values = await run_pipeline_async(
                spec, registry=default_registry, workdir="."
            )
    """
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(
        None,
        functools.partial(
            run_pipeline,
            spec,
            registry=registry,
            workdir=workdir,
            overrides=overrides,
            cache_dir=cache_dir,
            max_workers=max_workers,
            fail_fast=fail_fast,
            cancel_event=cancel_event,
            executor=executor,
            handle_store=handle_store,
            run_store=run_store,
            resume_run_id=resume_run_id,
            hooks=hooks,
            run_id=run_id,
            logger=logger,
        ),
    )
