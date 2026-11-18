"""远程执行示范（M4-4）：基于共享队列文件 + 结果目录的跨进程执行器。

``QueueWorkerExecutor`` 是 ``Executor`` 协议的一个队列版实现：客户端把任务
载荷（函数 + 实参，pickle 后 base64）按行追加进 JSONL 队列文件，远端 worker
进程（``python -m op_pipeline worker``）增量消费并执行，结果/异常以
``<job_id>.json`` 写回共享结果目录；``wait`` 轮询结果目录直到任务完成。

设计取舍：
- 共享文件系统（队列文件 + 结果目录）代替网络 socket：演示场景零额外依赖，
  跨机器可迁移（挂载盘 / NFS / 对象存储前缀均可），且不要求改动引擎一行；
- 载荷走 JSONL 而非逐任务小文件：队列天然有序、可回放、可断点续消费；
- 结果按 job_id 独立成文件、先写 ``.tmp`` 再原子 ``replace``：
  ``wait`` 轮询不会读到半个结果文件；
- 载荷必须可 pickle：submit 时即校验，不可 pickle 直接抛 ``RuntimeError``
  （对比 ``ProcessExecutor`` 的"回退线程"策略，队列模式无法回退，提前报错更诚实）。
"""
from __future__ import annotations

import base64
import json
import logging
import pickle
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Callable, Iterable

from .executor import Executor

log = logging.getLogger("op_pipeline.remote")


def _encode_value(value: Any) -> str:
    """把任意值 pickle + base64 成 JSON 安全字符串。"""
    return base64.b64encode(pickle.dumps(value)).decode("ascii")


def _decode_value(raw: str) -> Any:
    return pickle.loads(base64.b64decode(raw.encode("ascii")))


def _atomic_write(path: Path, data: dict[str, Any]) -> None:
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    tmp.replace(path)


class _QueueTask:
    """队列任务的 Future 鸭子类型：``done`` 是属性，``result``/``exception`` 读结果文件。"""

    def __init__(self, job_id: str, result_path: Path, poll: float) -> None:
        self.job_id = job_id
        self._result_path = result_path
        self._poll = poll
        self._lock = threading.Lock()
        self._result: Any = None
        self._error: BaseException | None = None
        self._resolved = False

    @property
    def done(self) -> bool:
        if not self._resolved and self._result_path.is_file():
            self._read()
        return self._resolved

    def _read(self) -> None:
        with self._lock:
            if self._resolved:
                return
            data = json.loads(self._result_path.read_text(encoding="utf-8"))
            if data["status"] == "ok":
                self._result = _decode_value(data["result"])
            else:
                self._error = RuntimeError(data["error"])
            self._resolved = True

    def _wait_ready(self, timeout: float | None) -> None:
        deadline = None if timeout is None else time.monotonic() + timeout
        while not self._resolved:
            if self._result_path.is_file():
                self._read()
                continue
            if deadline is not None and time.monotonic() >= deadline:
                raise TimeoutError(f"任务 {self.job_id} 超时未完成")
            time.sleep(self._poll)

    def result(self, timeout: float | None = None) -> Any:
        self._wait_ready(timeout)
        if self._error is not None:
            raise self._error
        return self._result

    def exception(self, timeout: float | None = None) -> BaseException | None:
        self._wait_ready(timeout)
        return self._error


class QueueWorkerExecutor:
    """把任务提交到共享 JSONL 队列，由远端 worker 进程消费执行。

    用法（客户端侧）::

        exec_ = QueueWorkerExecutor("jobs.jsonl", "results", poll=0.05)
        f = exec_.submit(operator.add, 1, 2)
        done = exec_.wait([f])
        print(f.result())

    配套 worker 进程::

        python -m op_pipeline worker --queue jobs.jsonl --out results --count 1
    """

    def __init__(self, queue: str | Path, out: str | Path, *, poll: float = 0.1) -> None:
        self._queue = Path(queue)
        self._out = Path(out)
        self._poll = poll
        self._out.mkdir(parents=True, exist_ok=True)
        self._closed = False

    def submit(self, fn: Callable[..., Any], /, *args: Any) -> _QueueTask:
        if self._closed:
            raise RuntimeError("执行器已关闭，拒绝新任务")
        try:
            payload = _encode_value((fn, args))
        except Exception as exc:  # pickle 失败典型：lambda/闭包/不可序列化对象
            raise RuntimeError(f"任务不可 pickle，无法提交到队列：{exc}") from exc
        job_id = uuid.uuid4().hex
        line = json.dumps({"job_id": job_id, "payload": payload}, ensure_ascii=False)
        with self._queue.open("a", encoding="utf-8") as fh:
            fh.write(line + "\n")
        return _QueueTask(job_id, self._out / f"{job_id}.json", self._poll)

    def wait(self, futures: Iterable[Any], *, first_completed: bool = True) -> set[Any]:
        tasks = list(futures)
        if not tasks:
            return set()
        while True:
            done = {t for t in tasks if t.done}
            if done and (first_completed or len(done) == len(tasks)):
                return done
            time.sleep(self._poll)

    def shutdown(self) -> None:
        self._closed = True


def consume_jobs(
    queue: str | Path,
    out: str | Path,
    *,
    count: int | None = None,
    poll: float = 0.1,
    stop_event: threading.Event | None = None,
) -> list[dict[str, Any]]:
    """worker 侧：增量消费 JSONL 队列并把每个任务的结果写回 ``out`` 目录。

    - ``count``：最多消费的任务数；``None`` 表示持续监听（直到 stop_event / Ctrl+C）；
    - 返回已消费任务的 ``[{"job_id", "status"}]`` 列表，供 CLI 打印与测试断言。
    """
    queue = Path(queue)
    out = Path(out)
    out.mkdir(parents=True, exist_ok=True)
    offset = 0
    consumed = 0
    processed: list[dict[str, Any]] = []
    while True:
        if stop_event is not None and stop_event.is_set():
            break
        lines = queue.read_text(encoding="utf-8").splitlines() if queue.exists() else []
        new_lines = lines[offset:]
        if not new_lines:
            if count is not None and consumed >= count:
                break
            time.sleep(poll)
            continue
        for line in new_lines:
            if count is not None and consumed >= count:
                break
            item = json.loads(line)
            job_id = item["job_id"]
            try:
                fn, args = _decode_value(item["payload"])
                value = fn(*args)
                status, result, error = "ok", _encode_value(value), None
            except Exception as exc:
                status, result, error = "error", None, f"{type(exc).__name__}: {exc}"
            _atomic_write(out / f"{job_id}.json", {
                "job_id": job_id, "status": status, "result": result, "error": error,
            })
            offset += 1
            consumed += 1
            processed.append({"job_id": job_id, "status": status})
        if count is not None and consumed >= count:
            break
    return processed
