"""执行引擎：把编译好的计划跑成结果。

关键行为（也是配置化框架最容易含糊的部分，这里明确写死）：

1. **调度**：按依赖就绪驱动，无依赖关系的节点并行（线程池），
   与配置里节点的书写顺序无关。
2. **失败传播**：节点失败后，依赖它的下游节点标记为 ``skipped``，
   并行分支继续跑（除非 ``fail_fast=True``）。
3. **``when`` 条件**：为假时节点跳过，下游同样跳过。
4. **重试**：``retries`` 次指数退避式重试；配置类错误（写错了参数）不重试；
   ``retry_on`` 非空时只重试列出的异常类型名（M6-3）。
5. **超时**：``timeout`` 秒为软超时——到点标记失败，但底层线程无法强杀，
   这一点在 README 里作为已知限制写明。
6. **缓存**：``cache=True`` 且算子声明确定性时，按内容指纹复用上次输出。
7. **取消**：``cancel_event`` 置位或 :meth:`Engine.cancel` 之后，未调度节点标记
   skipped、在跑节点自然结束，报告状态为 partial；远程（队列）节点不再等待
   结果，同样标记 skipped 并写 note（M6-3）。
8. **并发分组**（M6-3）：节点 ``concurrency_group`` 配合运行级
   ``concurrency_limits`` 限制同组节点同时执行数；未声明组或未配置限制时
   不加锁，行为与 v0.6.0 一致。acquire 在工作线程内（缓存判定之后），不阻塞调度。
"""
from __future__ import annotations

import asyncio
import functools
import inspect
import json
import logging
import os
import re
import threading
import time
import traceback
from concurrent.futures import CancelledError, Future
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Mapping, Protocol

from .cache import CacheStore
from .context import RunContext
from .errors import ConfigError, RefResolutionError
from .executor import (
    Executor,
    PooledProcessExecutor,
    ProcessExecutor,
    ThreadExecutor,
    _child_run_op,
)
from .expression import evaluate_condition
from .handlestore import (
    HandleStore,
    InMemoryHandleStore,
    SharedHandleStore,
    SpillHandleStore,
)
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


# M7-2 Task 6：报告参数打码——键名或值命中敏感特征时显示 ***。
_SENSITIVE_PARAM_KEY = re.compile(r"TOKEN|SECRET|PASSWORD|KEY", re.IGNORECASE)


def _masked_params(
    params: Mapping[str, Any], sensitive_values: tuple[str, ...] = ()
) -> dict[str, Any]:
    """构造报告用的参数副本（不影响执行期真实参数与 ``plan.params``）。

    满足任一条件即打码：参数键名匹配 ``TOKEN|SECRET|PASSWORD|KEY``（不区分
    大小写），或值转成字符串后包含解析期记录的非空敏感值。
    """
    secrets = tuple(str(value) for value in sensitive_values if str(value))
    masked: dict[str, Any] = {}
    for key, value in params.items():
        text = str(value)
        sensitive_key = bool(_SENSITIVE_PARAM_KEY.search(str(key)))
        if sensitive_key or any(secret in text for secret in secrets):
            masked[key] = "***"
        else:
            masked[key] = value
    return masked


def _make_preview(value: Any, n: int) -> Any:
    """M9 Task 7：单端口值级预览快照（只影响报告，不影响执行）。

    - list：前 n 行原值（元素原样，不截断）；
    - dict：前 n 个键的子 dict；
    - str：截前 200 字符；
    - 其他类型：repr 截前 200 字符。
    """
    if isinstance(value, list):
        return value[:n]
    if isinstance(value, dict):
        return {key: value[key] for key in list(value)[:n]}
    if isinstance(value, str):
        return value[:200]
    return repr(value)[:200]


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
    # M8-2 Task 5：行数血缘——list 类型端口在输入解析后（含采样截断）与输出
    # 发布时的实际行数；非 list 端口不记录。lineage=False 时不采集。
    rows_in: dict[str, int] = field(default_factory=dict)
    rows_out: dict[str, int] = field(default_factory=dict)
    # M8-2 Task 4：采样预览模式运行时为 True（报告标注；非采样不出现该键）
    sampled: bool = False
    # M9 Task 7：值级预览——输入/输出端口的前 N 行（list）或前 N 键（dict）
    # 原值快照；str 与其他类型截前 200 字符。仅 value_preview 非 None 时采集。
    preview_in: dict[str, Any] = field(default_factory=dict)
    preview_out: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        data = {
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
        if self.rows_in or self.rows_out:
            data["rows_in"] = dict(self.rows_in)
            data["rows_out"] = dict(self.rows_out)
        if self.sampled:
            data["sampled"] = True
        if self.preview_in:
            data["preview_in"] = self.preview_in
        if self.preview_out:
            data["preview_out"] = self.preview_out
        return data


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
    # M8-2 Task 4：采样预览行数；None = 非采样模式（报告不出现该字段）
    sample_rows: int | None = None

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
        data = {
            "pipeline": self.pipeline,
            "version": self.version,
            "run_id": self.run_id,
            "status": self.status,
            "duration": round(self.duration, 4),
            "started_at": time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(self.started_at)),
            "params": self.params,
            "outputs": self.outputs,
            "warnings": self.warnings,
            "artifacts_dir": self.artifacts_dir,
            "nodes": [n.to_dict() for n in self.nodes],
        }
        if self.sample_rows is not None:
            data["sample_rows"] = self.sample_rows
        return data

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
      取消）从未开始执行，不触发任何钩子。例外：远程（队列）节点已进入
      执行阶段、随后因取消而放弃等待时，会以 ``status="skipped"`` 触发
      本钩子（M6-3）；
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
        concurrency_limits: Mapping[str, int] | None = None,
        sample_rows: int | None = None,
        lineage: bool = True,
        value_preview: int | None = None,
    ) -> None:
        # M5-1：运行级执行器三元态——字符串选择器 / Executor 实例 / None（默认线程池）。
        if isinstance(executor, str) and executor not in (
            "thread", "process", "process_pool", "queue"
        ):
            raise ConfigError(
                f"executor 只能是 thread/process/process_pool/queue，实际是 {executor!r}"
            )
        if executor is not None and not isinstance(executor, (str, Executor)):
            raise ConfigError(
                "executor 只能是字符串选择器（thread/process/process_pool/queue）或实现 "
                "Executor 协议（submit/wait/shutdown）的实例，"
                f"实际是 {type(executor).__name__}"
            )
        # M8-2 Task 4：采样预览——sample_rows 必须是 ≥1 的整数（bool 不算）；
        # None = 正常运行（默认行为与 v0.8.0 一致）
        if sample_rows is not None and (
            isinstance(sample_rows, bool)
            or not isinstance(sample_rows, int)
            or sample_rows < 1
        ):
            raise ValueError(f"sample_rows 必须是 ≥1 的整数，实际是 {sample_rows!r}")
        # M9 Task 7：值级预览行数——sample_rows 之外的独立预览开关；只影响报告
        # （preview_in/preview_out），不影响执行（句柄不截断、缓存照常），也不受
        # lineage 开关控制。None = 不采集（默认行为与 v0.9.0 一致）。
        if value_preview is not None and (
            isinstance(value_preview, bool)
            or not isinstance(value_preview, int)
            or value_preview < 1
        ):
            raise ValueError(f"value_preview 必须是 ≥1 的整数，实际是 {value_preview!r}")
        self.value_preview = value_preview
        self.plan = plan
        self.context = context
        # M8-2 Task 5：行数血缘开关（默认开启）；False 时报告节点不含 rows_in/rows_out
        self.lineage = lineage
        self.sample_rows = sample_rows
        # M8-2 Task 4：采样模式下缓存读写全部禁用——预览结果不落缓存，
        # 也不读旧缓存（调用方传入的 cache_store 被替换为禁用实例）
        self.cache = (
            CacheStore(None) if sample_rows is not None else (cache_store or CacheStore(None))
        )
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
        if (
            isinstance(executor, str)
            and executor in ("thread", "process", "process_pool")
            and node_level_queue
        ):
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
            ignored = (
                {"process", "process_pool"}
                if self._remote
                else {"process", "process_pool", "queue"}
            )
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
            use_pooled = False
        else:
            kinds = {"process", "process_pool"}
            need_process = executor in kinds or any(
                node.spec.executor in kinds for node in plan.nodes
            )
            # M9 Task 6：仅当运行级或任一节点声明 process_pool 时启用常驻
            # worker 池；纯 "process" 保持每任务一进程的 M2-2 行为（零变化）。
            use_pooled = executor == "process_pool" or any(
                node.spec.executor == "process_pool" for node in plan.nodes
            )
        self._process_pool = (
            (PooledProcessExecutor if use_pooled else ProcessExecutor)(
                on_fallback=self._warnings.append
            )
            if need_process
            else None
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

        # M6-3 Task 8：节点并发分组限流——组名 -> 正整数上限。
        # 非法配置（0 / 负数 / 非整数）在构造期即报 ConfigError，绝不静默忽略。
        # 信号量按组惰性创建（见 _group_semaphore）：未声明组或未配置限制的节点
        # 完全不加锁，行为与 v0.6.0 一致。
        self._concurrency_limits: dict[str, int] = {}
        for group, limit in (concurrency_limits or {}).items():
            if not isinstance(group, str) or not group:
                raise ConfigError(
                    f"concurrency_limits 的组名必须是非空字符串，实际是 {group!r}"
                )
            if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
                raise ConfigError(
                    f"concurrency_limits[{group!r}] 必须是正整数，实际是 {limit!r}"
                )
            self._concurrency_limits[group] = limit
        self._semaphores: dict[str, threading.Semaphore] = {}
        self._semaphore_lock = threading.Lock()

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

    def _group_semaphore(self, group: str) -> threading.Semaphore | None:
        """取（惰性创建）并发分组信号量（M6-3 Task 8）。

        未声明组（空串）或 ``concurrency_limits`` 里没有该组 → 返回 None，
        调用方不加锁。信号量首次在**工作线程**执行该组节点时创建；创建过程
        用锁保护，避免并发首建出多个信号量而突破上限。
        """
        limit = self._concurrency_limits.get(group) if group else None
        if not limit:
            return None
        with self._semaphore_lock:
            sem = self._semaphores.get(group)
            if sem is None:
                sem = threading.Semaphore(limit)
                self._semaphores[group] = sem
        return sem

    # ------------------------------------------------------------------ #
    def run(self) -> RunReport:
        started = time.time()
        report = RunReport(
            pipeline=self.plan.spec.name,
            version=self.plan.spec.version,
            run_id=self.context.run_id,
            status=SUCCESS,
            started_at=started,
            params=_masked_params(
                dict(self.plan.params), self.plan.spec.sensitive_values
            ),
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

        # M8-2 Task 4：采样预览模式标注——run 级 sample_rows 字段 +
        # 节点级 sampled 标记（to_dict 仅在采样模式输出这两个字段）
        if self.sample_rows is not None:
            report.sample_rows = self.sample_rows
            for node_result in report.nodes:
                node_result.sampled = True

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
        # M7-4 Task 10：运行成功且策略为 on_run_success 时删除本次 run 的 spill 目录；
        # 失败/取消保留，便于排障。放在类型检查之后（检查会读盘取值）。
        self._cleanup_spill_on_success(report.status)
        report.warnings.extend(self._warnings)
        report.finished_at = time.time()
        # M2-5：运行级记录落库（run_id 主键，可覆盖同 run_id 的重跑记录）。
        # M9 Task 11：附带全量报告 JSON——与 --report-out 同一序列化规则
        # （json.dumps(report.to_dict(), ensure_ascii=False, indent=2, default=str)）。
        # 此时报告已定稿（nodes/status/warnings/finished_at 均已汇总完毕）。
        if self.run_store is not None:
            self.run_store.record_run(
                report.run_id,
                report.pipeline,
                report.version,
                report.status,
                report.started_at,
                report.finished_at,
                report.params,
                report_json=json.dumps(
                    report.to_dict(), ensure_ascii=False, indent=2, default=str
                ),
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
    def _cleanup_spill_on_success(self, status: str) -> None:
        """M7-4 Task 10：``on_run_success`` 策略下删除本次 run 的落盘目录。

        仅处理 ``SpillHandleStore``（含其子类，子类默认策略为 never）；
        显式 ``spill_dir`` 归调用方管理，``cleanup_run_dir`` 不会删除。
        删除失败（如 Windows 句柄占用）只记告警，不改变运行结果。
        """
        store = self.store
        if not isinstance(store, SpillHandleStore):
            return
        if status != SUCCESS or store.cleanup != "on_run_success":
            return
        try:
            store.cleanup_run_dir()
        except OSError as exc:
            self._warnings.append(f"清理 spill 落盘目录失败：{exc}")

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
            result.duration,
            # M8 Task 9.1：行数血缘透传——空 dict 视同未采集写 NULL，
            # 与报告"节点无 rows_in/rows_out 键 ↔ DB NULL"语义一致
            rows_in=result.rows_in or None,
            rows_out=result.rows_out or None,
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

        M6-3 Task 10：等待期间轮询运行级 ``cancel_event``（经 ``_QueueTask``
        的取消支持）；置位则抛内部 ``_RemoteCancelled``——该异常不参与重试，
        由 :meth:`_run_remote_node` 把节点标记为 skipped（取消不是失败）。
        """
        payload = self._remote_payload(node, params, inputs)
        assert self._executor_instance is not None  # 仅 _remote 模式进入
        task = self._executor_instance.submit(
            _remote_run, payload, cancel_event=self._cancel_event
        )
        try:
            data = task.result(timeout=timeout)
        except CancelledError:
            raise _RemoteCancelled(node.spec.id) from None
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
            # M8-2 Task 4/5：远程路径同样在输入解析收敛处采样截断并记录行数
            inputs = self._sample_inputs(inputs)
            if self.lineage:
                result.rows_in = self._row_counts(inputs)
            # M9 Task 7：值级预览——远程路径输入快照（与 lineage 独立）
            if self.value_preview is not None:
                result.preview_in = self._previews(inputs)
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
                    return self._finish_cache_hit(node, result, cached, cache_key)

            result.cache_key = cache_key or ""
            # M7-4 Task 11：单飞锁 + 二次查缓存（远程路径与线程路径同语义）
            flight_lock = (
                self.cache.key_lock(cache_key) if cache_key is not None else None
            )
            if flight_lock is not None:
                flight_lock.acquire()
            try:
                if cache_key is not None:
                    cached = self.cache.load(cache_key)
                    if cached is not None:
                        return self._finish_cache_hit(node, result, cached, cache_key)
                # M6-3 Task 8：远程节点同样受并发分组限流（acquire 在工作线程内）
                sem = self._group_semaphore(node.spec.concurrency_group)
                if sem is not None:
                    sem.acquire()
                try:
                    data = self._attempt_loop(
                        node,
                        ctx,
                        result,
                        lambda: self._remote_once(node, params, inputs, node.spec.timeout),
                    )
                finally:
                    if sem is not None:
                        sem.release()
                result.status = SUCCESS
                # worker 已把输出原子发布进共享目录；引擎侧从同一目录读回实际值
                raw = {
                    port: self.store.get(handle)
                    for port, handle in (data.get("outputs") or {}).items()
                }
                result.outputs = {k: _describe(v) for k, v in raw.items()}
                # M8-2 Task 5：行数血缘——远程路径输出读回处记录 list 端口行数
                if self.lineage:
                    result.rows_out = self._row_counts(raw)
                # M9 Task 7：值级预览——远程路径输出快照
                if self.value_preview is not None:
                    result.preview_out = self._previews(raw)
                if cache_key is not None:
                    self.cache.store(cache_key, raw)
            finally:
                if flight_lock is not None:
                    flight_lock.release()
        except _RemoteCancelled:
            # M6-3 Task 10：取消不再等待远程结果——标记 skipped，不算失败
            result.status = SKIPPED
            result.note = f"取消：不再等待远程节点 {node.spec.id}"
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
            elif result.status == SKIPPED:
                ctx.logger.info("节点 %s 跳过：%s", node.spec.id, result.note)
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
    def _sample_inputs(self, inputs: dict[str, Any]) -> dict[str, Any]:
        """M8-2 Task 4：采样模式——list 类型输入句柄截断为前 sample_rows 行。

        在输入解析收敛处统一做：实际传给算子的句柄即截断后的句柄；
        dict/str/其他类型原样不动。非采样模式原样返回。
        """
        if self.sample_rows is None:
            return inputs
        return {
            port: value[: self.sample_rows] if isinstance(value, list) else value
            for port, value in inputs.items()
        }

    def _row_counts(self, values: Mapping[str, Any]) -> dict[str, int]:
        """M8-2 Task 5：行数血缘——仅统计 list 类型端口的行数（键为端口名）。"""
        return {
            port: len(value)
            for port, value in values.items()
            if isinstance(value, list)
        }

    def _previews(self, values: Mapping[str, Any]) -> dict[str, Any]:
        """M9 Task 7：值级预览采集（value_preview 非 None 时调用，规则见
        :func:`_make_preview`；预览不影响句柄本身）。"""
        return {
            port: _make_preview(value, self.value_preview or 1)
            for port, value in values.items()
        }

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
            # M8-2 Task 4：采样截断在输入解析收敛处统一做——传给算子的句柄
            # 即截断后的句柄（缓存 key、行数血缘都基于截断后的输入）
            inputs = self._sample_inputs(inputs)
            # M8-2 Task 5：行数血缘——记录截断后（实际传入规模）的输入行数
            if self.lineage:
                result.rows_in = self._row_counts(inputs)
            # M9 Task 7：值级预览——快照"采样截断后实际传入"的输入（与 lineage 独立）
            if self.value_preview is not None:
                result.preview_in = self._previews(inputs)
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
                    return self._finish_cache_hit(node, result, cached, cache_key)

            result.cache_key = cache_key or ""
            # M7-4 Task 11：单飞锁在工作线程内获取（未命中且准备计算处）；
            # 进入后二次查缓存——同 key 的其它线程可能已写入，命中则直接复用。
            flight_lock = (
                self.cache.key_lock(cache_key) if cache_key is not None else None
            )
            if flight_lock is not None:
                flight_lock.acquire()
            try:
                if cache_key is not None:
                    cached = self.cache.load(cache_key)
                    if cached is not None:
                        return self._finish_cache_hit(node, result, cached, cache_key)
                # M6-3 Task 8：并发分组限流——acquire 在工作线程内、缓存命中判定
                # 之后、实际执行之前（不阻塞调度线程）；异常路径同样释放配额。
                sem = self._group_semaphore(node.spec.concurrency_group)
                if sem is not None:
                    sem.acquire()
                try:
                    outputs = self._execute_with_retry(instance, ctx, inputs, node, result)
                finally:
                    if sem is not None:
                        sem.release()
                result.status = SUCCESS
                result.outputs = {k: _describe(v) for k, v in outputs.items()}
                # M8-2 Task 5：行数血缘——输出发布处记录 list 端口行数
                if self.lineage:
                    result.rows_out = self._row_counts(outputs)
                # M9 Task 7：值级预览——发布前原值快照（句柄本身不截断、缓存照常）
                if self.value_preview is not None:
                    result.preview_out = self._previews(outputs)

                if cache_key is not None:
                    self.cache.store(cache_key, outputs)

                self._publish(node, outputs)
            finally:
                if flight_lock is not None:
                    flight_lock.release()
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

    def _finish_cache_hit(
        self,
        node: PlanNode,
        result: NodeResult,
        outputs: dict[str, Any],
        cache_key: str,
    ) -> NodeResult:
        """命中缓存的收口（M7-4 Task 11 抽出，首次查询与单飞二次查询共用）。

        线程路径与远程路径语义一致：标记 cached/cache_key、判定断点续跑、
        经 ``_publish`` 写回句柄并按需记录日志。
        """
        result.cached = True
        result.cache_key = cache_key
        result.status = SUCCESS
        result.duration = time.time() - result.started_at
        result.outputs = {k: _describe(v) for k, v in outputs.items()}
        # M8-2 Task 5：行数血缘——缓存命中后反序列化的输出若为 list 也记录行数
        if self.lineage:
            result.rows_out = self._row_counts(outputs)
        # M9 Task 7：值级预览——缓存命中同样快照（preview_in 已在输入解析处采集）
        if self.value_preview is not None:
            result.preview_out = self._previews(outputs)
        # M2-6：参考 run 里该节点成功且 cache_key 一致才算续跑命中
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

        M6-3 Task 9：``node.spec.retry_on`` 非空时，仅当 ``type(exc).__name__``
        在该元组内才继续重试；未命中立即失败（attempts 停在当前次）。
        配置类错误（ConfigError/ParamValidationError 等子类）永远先走上面的
        ``except ConfigError`` 分支，不受 ``retry_on`` 影响。
        远程路径的异常在引擎侧是 ``_RemoteFailure``（worker 侧类型名为打包文本），
        需要按类型重试远程异常时应在 ``retry_on`` 里写 ``_RemoteFailure``。
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
            except _RemoteCancelled:
                # M6-3 Task 10：取消不是失败——不重试、不包装，交给远程节点收口
                raise
            except Exception as exc:
                last_error = exc
                # M6-3 Task 9：条件重试——未命中类型立即失败，不再退避
                if node.spec.retry_on and type(exc).__name__ not in node.spec.retry_on:
                    break
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
        """进程模式（M2-2/M9 Task 6）：算子在子进程执行，timeout 硬超时强制。

        池实例按运行级/节点级声明二选一构造：纯 "process" 用每任务一进程的
        ``ProcessExecutor``；涉及 "process_pool" 用常驻 worker 池
        ``PooledProcessExecutor``。两种 kind 共用同一池实例，此处无需区分。
        """
        run = ctx.run
        ctx_data = (
            ctx.node_id,
            run.run_id,
            run.pipeline,
            dict(ctx.params),
            str(run.workdir),
            str(run.artifacts_dir),
        )
        assert self._process_pool is not None  # 仅在 kind 为 process/process_pool 且池存在时进入
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
        if kind in ("process", "process_pool") and self._process_pool is not None:
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


class _RemoteCancelled(Exception):
    """远程等待被取消（M6-3 Task 10）。

    运行级 ``cancel_event`` 置位时由 ``_remote_once`` 抛出；它不参与重试、
    不算失败——``_run_remote_node`` 捕获后把节点标记为 ``skipped`` 并写 note。
    队列中的任务本身不会被撤回（worker 侧可能继续执行，软取消语义）。
    """

    def __init__(self, node_id: str) -> None:
        self.node_id = node_id
        super().__init__(f"取消：不再等待远程节点 {node_id}")


# --------------------------------------------------------------------------- #
# 便捷入口
# --------------------------------------------------------------------------- #
def _dir_usage(path: Path) -> int:
    """目录内文件总字节（递归；目录不存在返回 0）。"""
    if not path.is_dir():
        return 0
    total = 0
    for item in path.rglob("*"):
        try:
            if item.is_file():
                total += item.stat().st_size
        except OSError:  # pragma: no cover - 并发删除等瞬时失败
            continue
    return total


def storage_warnings(
    cache: CacheStore,
    *,
    artifacts_dir: str | Path,
    spill_warn_bytes: int | None = None,
    spill_dir: str | Path | None = None,
) -> list[str]:
    """M7-4 Task 12：运行结束的存储容量告警（只读统计，不改变运行结果）。

    - 配置了 ``cache.max_bytes`` 且缓存目录实际占用超过它 → 中文提示；
    - 配置了 ``spill_warn_bytes`` 且 spill 目录占用超过它 → 中文提示；
    - 未配置阈值时不告警（默认 None，与 v0.7.0 一致）。
    """
    warnings: list[str] = []
    if cache.enabled and cache.max_bytes is not None and cache.directory is not None:
        used = _dir_usage(cache.directory)
        if used > cache.max_bytes:
            warnings.append(
                f"缓存目录 {cache.directory} 实际占用 {used} 字节，"
                f"超过上限 {cache.max_bytes} 字节，建议清理或调整缓存容量参数"
            )
    if spill_warn_bytes is not None and spill_warn_bytes > 0:
        target = (
            Path(spill_dir) if spill_dir is not None
            else Path(artifacts_dir) / "spill"
        )
        used = _dir_usage(target)
        if used > spill_warn_bytes:
            warnings.append(
                f"落盘目录 {target} 实际占用 {used} 字节，"
                f"超过告警阈值 {spill_warn_bytes} 字节"
            )
    return warnings


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
    concurrency_limits: Mapping[str, int] | None = None,
    cache_max_entries: int | None = None,
    cache_max_bytes: int | None = None,
    spill_warn_bytes: int | None = None,
    sample_rows: int | None = None,
    lineage: bool = True,
    value_preview: int | None = None,
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
    ``executor="process_pool"``（M9 Task 6）走常驻 worker 进程池
    :class:`~op_pipeline.executor.PooledProcessExecutor`：N 个 spawn 子进程复用
    执行多个节点任务（默认 ``min(8, cpu_count)`` 个 worker），免去每任务冷启动
    开销；硬超时仍强制——terminate 在跑 worker 并自动补位，worker 崩溃同样补位、
    在跑任务按失败结算。节点级 ``executor="process_pool"`` 声明同样触发建池。
    ``concurrency_limits``（M6-3 Task 8）节点并发分组上限：``{组名: 正整数}``，
    与节点 ``concurrency_group`` 配合限制同组节点同时执行数；未声明组或未配置
    限制的节点不受影响；非法值（<=0 / 非整数）抛 ``ConfigError``。
    ``cache_max_entries`` / ``cache_max_bytes``（M7-4 Task 11）：缓存容量上限，
    写入后按 mtime 淘汰最旧条目；默认 None = 不限（与 v0.7.0 一致）。
    ``spill_warn_bytes``（M7-4 Task 12）：spill 落盘目录占用告警阈值，运行结束
    超过阈值时向 ``report.warnings`` 追加中文提示；默认 None = 不告警。
    ``sample_rows``（M8-2 Task 4）：采样预览行数——非 None 时进入采样模式，
    每个节点解析输入后把 list 类型句柄截断为前 N 行（dict/str 原样），
    缓存读写全部禁用；必须是 ≥1 的整数（bool 不算），非法抛 ``ValueError``。
    ``lineage``（M8-2 Task 5）：行数血缘开关（默认开启）；False 时报告节点
    不含 rows_in/rows_out，其余行为不变。
    ``value_preview``（M9 Task 7）：值级预览行数——非 None 时报告节点附带
    ``preview_in``/``preview_out``（dict[端口, 预览]）：list 取前 N 行原值、
    dict 取前 N 键子 dict、str 截前 200 字符、其他类型 repr 截 200 字符。
    只影响报告不影响执行（句柄不截断、缓存照常），与 ``sample_rows``、
    ``lineage`` 相互独立；非法值抛 ``ValueError``。

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
    # M8-2 Task 4：采样预览模式不构造缓存上下文——cache_dir 传 None，
    # 连目录都不创建；引擎内会再替换为禁用缓存实例（读写全部禁用）
    cache_store = CacheStore(
        cache_dir if sample_rows is None else None,
        base_dir=workdir,
        max_entries=cache_max_entries,
        max_bytes=cache_max_bytes,
    )
    engine = Engine(plan, context, cache_store=cache_store, max_workers=max_workers,
                    fail_fast=fail_fast, cancel_event=cancel_event, executor=executor,
                    handle_store=handle_store, run_store=run_store,
                    resume_run_id=resume_run_id, hooks=hooks,
                    concurrency_limits=concurrency_limits,
                    sample_rows=sample_rows, lineage=lineage, value_preview=value_preview)
    report = engine.run()
    # M7-4 Task 12：运行结束统计缓存/spill 目录占用，超阈值追加中文告警
    report.warnings.extend(
        storage_warnings(
            cache_store,
            artifacts_dir=report.artifacts_dir,
            spill_warn_bytes=spill_warn_bytes,
            spill_dir=getattr(engine.store, "spill_dir", None),
        )
    )
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
    concurrency_limits: Mapping[str, int] | None = None,
    sample_rows: int | None = None,
    lineage: bool = True,
    value_preview: int | None = None,
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
            concurrency_limits=concurrency_limits,
            sample_rows=sample_rows,
            lineage=lineage,
            value_preview=value_preview,
        ),
    )
