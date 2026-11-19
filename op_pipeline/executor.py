"""执行器抽象：把"节点在哪儿跑"与"引擎怎么调度"解耦。

Engine 只依赖 `Executor` 的三个方法（`submit` / `wait` / `shutdown`）；
默认的 `ThreadExecutor` 平移了历史上内联在 Engine 里的
`ThreadPoolExecutor + wait(FIRST_COMPLETED)` 行为（M2-1，行为零变化）。

`ProcessExecutor`（M2-2）实现真正的硬超时：每个任务一个 spawn 子进程，
`timeout` 到点 `terminate()`。刻意不用 `ProcessPoolExecutor`——它无法
强杀单个任务，要杀只能 terminate worker，那会把整个池打成
`BrokenProcessPool`、殃及同时在跑的其它节点。

实现自定义执行器时注意：`_run_node` 依赖引擎内部的句柄表快照与
`NodeContext`，跨进程时载荷必须可序列化（见扩展设计 §4.2）。
"""
from __future__ import annotations

import logging
import multiprocessing
import pickle
import threading
import time
from concurrent.futures import (
    ALL_COMPLETED,
    FIRST_COMPLETED,
    Future,
    ThreadPoolExecutor,
    wait,
)
from pathlib import Path
from typing import Any, Callable, Iterable, Protocol, runtime_checkable

from .context import RunContext


@runtime_checkable
class Executor(Protocol):
    """执行器接口。

    - ``submit(fn, *args)``：提交一个节点任务，返回可等待的 Future；
    - ``wait(futures, first_completed=True)``：等待完成，返回已完成集合；
      ``first_completed=True`` 时首个完成即返回（引擎主循环依赖此语义
      尽快收集结果并调度下游）；
    - ``shutdown()``：收尾，拒绝新提交。
    """

    def submit(self, fn: Callable[..., Any], /, *args: Any) -> Future: ...

    def wait(
        self, futures: Iterable[Future], *, first_completed: bool = True
    ) -> set[Future]: ...

    def shutdown(self) -> None: ...


class ThreadExecutor:
    """线程池执行器：平移原有 ThreadPoolExecutor + FIRST_COMPLETED 行为。"""

    def __init__(self, max_workers: int | None = None) -> None:
        self._pool = ThreadPoolExecutor(
            max_workers=max_workers, thread_name_prefix="op"
        )

    def submit(self, fn: Callable[..., Any], /, *args: Any) -> Future:
        return self._pool.submit(fn, *args)

    def wait(
        self, futures: Iterable[Future], *, first_completed: bool = True
    ) -> set[Future]:
        done, _ = wait(
            list(futures),
            return_when=FIRST_COMPLETED if first_completed else ALL_COMPLETED,
        )
        return done

    def shutdown(self) -> None:
        self._pool.shutdown()

    def __enter__(self) -> "ThreadExecutor":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.shutdown()


# --------------------------------------------------------------------------- #
# 进程执行器（M2-2）
# --------------------------------------------------------------------------- #
def _child_main(blob: bytes, conn: Any) -> None:
    """子进程入口：反序列化载荷并执行，把结果/异常经管道送回父进程。"""
    try:
        fn, args = pickle.loads(blob)
        conn.send(("ok", fn(*args)))
    except BaseException as exc:  # noqa: BLE001 - 需要把任意异常送回父进程
        try:
            conn.send(("error", exc))
        except Exception:
            try:
                conn.send(("error", RuntimeError(f"{type(exc).__name__}: {exc}")))
            except Exception:
                pass
    finally:
        conn.close()


def _child_run_op(
    instance: Any, ctx_data: tuple, inputs: dict[str, Any]
) -> dict[str, Any]:
    """子进程内执行算子：用序列化的运行数据重建轻量 NodeContext。

    子进程日志走独立的 ``op_pipeline`` logger（默认 WARNING 以上经
    lastResort 打到 stderr），产物写回与父进程相同的 artifacts_dir。
    """
    node_id, run_id, pipeline, params, workdir, artifacts_dir = ctx_data
    run = RunContext(
        run_id=run_id,
        pipeline=pipeline,
        params=params,
        workdir=Path(workdir),
        artifacts_dir=Path(artifacts_dir),
        logger=logging.getLogger("op_pipeline"),
    )
    ctx = run.for_node(node_id, getattr(instance, "name", node_id))
    ctx.params = params
    return instance.execute(ctx, inputs)


class _ProcTask:
    """进程任务的等待句柄（duck-type 并发 Future：result/exception/done）。

    与 `concurrent.futures.Future` 不同，`done` 是普通属性——引擎按
    `future.result()` 消费，不依赖其线程设施。
    """

    def __init__(self, proc: Any, conn: Any, timeout: float | None) -> None:
        self.proc = proc
        self.conn = conn
        self.timeout = timeout
        self.deadline = None if not timeout else time.monotonic() + timeout
        self.done = False
        self._result: Any = None
        self._exception: BaseException | None = None

    def _settle_recv(self) -> None:
        try:
            kind, value = self.conn.recv()
            if kind == "ok":
                self._result = value
            else:
                self._exception = value
        except (EOFError, OSError) as exc:
            self._exception = RuntimeError(f"子进程连接中断：{exc}")
        self.done = True
        self._close()

    def kill(self) -> None:
        """硬超时：terminate 子进程并标记失败（唯一能真正终止算子的手段）。"""
        try:
            self.proc.terminate()
            self.proc.join()
        except Exception:
            pass
        self._exception = TimeoutError(
            f"硬超时：子进程执行超过 {self.timeout}s，已强制终止"
        )
        self.done = True
        self._close()

    def _close(self) -> None:
        try:
            self.conn.close()
        except Exception:
            pass

    def result(self) -> Any:
        if not self.done:
            raise RuntimeError("进程任务尚未完成：先调用 wait()")
        if self._exception is not None:
            raise self._exception
        return self._result

    def exception(self) -> BaseException | None:
        return self._exception


class _FallbackTask:
    """不可 pickle 载荷的线程回退句柄：构造时同步执行完（含软超时）。"""

    def __init__(
        self, fn: Callable[..., Any], args: tuple, timeout: float | None
    ) -> None:
        self.done = True
        self.deadline = None
        box: dict[str, Any] = {}

        def runner() -> None:
            try:
                box["ok"] = fn(*args)
            except BaseException as exc:  # noqa: BLE001
                box["err"] = exc

        worker = threading.Thread(target=runner, daemon=True)
        worker.start()
        worker.join(timeout)
        if worker.is_alive():
            self._exception: BaseException | None = TimeoutError(
                f"软超时：回退线程执行超过 {timeout}s，底层线程仍在运行"
            )
        elif "err" in box:
            self._exception = box["err"]
        else:
            self._exception = None
            self._result = box["ok"]

    def result(self) -> Any:
        if self._exception is not None:
            raise self._exception
        return self._result

    def exception(self) -> BaseException | None:
        return self._exception


class ProcessExecutor:
    """进程执行器：每任务一个 spawn 子进程，支持硬超时 terminate（M2-2）。

    - ``submit(fn, *args, timeout=None)``：载荷先 ``pickle.dumps``（该
      序列化结果直接作为子进程输入，不做二次序列化）；不可 pickle 时
      回退线程执行并经 ``on_fallback(message)`` 记录告警；
    - ``wait(...)``：与 `ThreadExecutor` 同语义，并在内部检查
      deadline——到点 terminate 子进程、任务以"硬超时"异常完成；
    - ``shutdown()``：终止仍存活的子进程。

    载荷可 pickle 指：``fn`` 及其 ``args``（算子实例、参数、输入）能被
    pickle 且子进程能 import 其类型定义（模块级类）。
    """

    def __init__(
        self,
        *,
        max_workers: int | None = None,  # 兼容签名：每任务一进程，不做池化上限
        on_fallback: Callable[[str], None] | None = None,
    ) -> None:
        self._ctx = multiprocessing.get_context("spawn")
        self._on_fallback = on_fallback
        self._live: list[_ProcTask] = []
        self._lock = threading.Lock()
        self._closed = False

    def submit(
        self, fn: Callable[..., Any], /, *args: Any, timeout: float | None = None
    ) -> Any:
        if self._closed:
            raise RuntimeError("ProcessExecutor 已 shutdown，拒绝新提交")
        try:
            blob = pickle.dumps((fn, args), protocol=pickle.HIGHEST_PROTOCOL)
        except Exception as exc:
            if self._on_fallback is not None:
                self._on_fallback(
                    f"进程负载不可 pickle（{type(exc).__name__}: {exc}），已回退线程执行"
                )
            return _FallbackTask(fn, args, timeout)
        parent_conn, child_conn = self._ctx.Pipe(duplex=False)
        proc = self._ctx.Process(target=_child_main, args=(blob, child_conn), daemon=True)
        try:
            proc.start()
        finally:
            child_conn.close()
        task = _ProcTask(proc, parent_conn, timeout)
        with self._lock:
            self._live.append(task)
        return task

    def wait(
        self, futures: Iterable[Any], *, first_completed: bool = True
    ) -> set[Any]:
        tasks = list(futures)
        # 先非阻塞收割一轮（含回退任务——它们构造时已完成）
        for t in tasks:
            if t.done:
                continue
            if getattr(t, "conn", None) is not None and t.conn.poll(0):
                t._settle_recv()
        if first_completed and any(t.done for t in tasks):
            return {t for t in tasks if t.done}
        while True:
            pending = [t for t in tasks if not t.done]
            if not pending or (first_completed and any(t.done for t in tasks)):
                break
            now = time.monotonic()
            killed = False
            for t in pending:
                if t.deadline is not None and now >= t.deadline:
                    t.kill()
                    killed = True
            if killed:
                continue
            nearest = min(
                (t.deadline - now for t in pending if t.deadline is not None),
                default=None,
            )
            slice_timeout = 0.05 if nearest is None else min(0.05, max(0.0, nearest))
            for t in pending:
                if t.conn.poll(slice_timeout):
                    t._settle_recv()
                slice_timeout = 0.0  # 首片等待后，其余连接只做非阻塞检查
        return {t for t in tasks if t.done}

    def shutdown(self) -> None:
        self._closed = True
        with self._lock:
            tasks, self._live = self._live, []
        for t in tasks:
            if isinstance(t, _ProcTask):
                if t.proc.is_alive():
                    t.proc.terminate()
                    t.proc.join()
                if not t.done:
                    t.kill()

    def __enter__(self) -> "ProcessExecutor":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.shutdown()
