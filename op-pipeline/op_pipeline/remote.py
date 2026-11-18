"""远程执行（M5-1）：基于共享队列文件 + 结果目录的跨进程执行器。

``QueueWorkerExecutor`` 是 ``Executor`` 协议的一个队列版实现：客户端把任务
载荷（函数 + 实参，pickle 后 base64）按行追加进 JSONL 队列文件，远端 worker
进程（``python -m op_pipeline worker``）增量消费并执行，结果/异常以
``<job_id>.json`` 写回共享结果目录；``wait`` 轮询结果目录直到任务完成。

M5-1 起该执行器已接入引擎节点调度主循环：引擎把每个节点的载荷（纯 dict，
模块级 ``_remote_run`` + payload）提交进队列，worker 侧经 ``default_registry``
按算子名实例化并执行，输出经共享目录句柄存储（``SharedHandleStore``）原子发布
（上游输入值由引擎主进程解析后直接进载荷，见 ``_remote_run`` docstring）。

设计取舍：
- 共享文件系统（队列文件 + 结果目录）代替网络 socket：零额外依赖、跨机器可
  迁移（挂载盘 / NFS / 对象存储前缀均可）；
- 载荷走 JSONL 而非逐任务小文件：队列天然有序、可回放、可断点续消费；
- 结果按 job_id 独立成文件、先写 ``.tmp`` 再原子 ``replace``：
  ``wait`` 轮询不会读到半个结果文件；
- 载荷必须可 pickle：submit 时即校验，不可 pickle 直接抛 ``RuntimeError``
  （对比 ``ProcessExecutor`` 的"回退线程"策略，队列模式无法回退，提前报错更诚实）；
- 句柄必须可 pickle：``SharedHandleStore.put`` 对不可 pickle 的值同样抛
  ``RuntimeError``——跨进程没有内存退化的余地，绝不静默丢失数据。
"""
from __future__ import annotations

import base64
import json
import logging
import pickle
import threading
import time
import traceback
import uuid
from pathlib import Path
from typing import Any, Callable, Iterable

from .context import RunContext
from .executor import Executor
from .handlestore import SharedHandleStore
from .registry import default_registry

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

    @property
    def out(self) -> Path:
        """结果目录（引擎据此推导共享句柄目录，M5-1）。"""
        return self._out

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


def _remote_run(payload: dict[str, Any]) -> dict[str, Any]:
    """远端执行单个节点（M5-1）：worker 只做"实例化算子 → 执行 → 发布输出"。

    载荷契约（由 ``Engine._remote_payload`` 构造）::

        {
          "node_id": 节点 id,
          "op":      算子名（worker 经 ``default_registry`` 按名实例化），
          "params":  解析后的节点参数 dict（主进程已解析），
          "inputs":  解析后的上游输入值 dict（主进程已解析），
          "outputs": 端口 -> 句柄名映射（worker 发布输出用），
          "shared_dir": 共享句柄目录（``SharedHandleStore``），
          "run": {run_id, pipeline, params, workdir, artifacts_dir},
        }

    序列化决策（为什么输入走 payload、输出走共享目录）：

    - **输入值直接进 payload**：引用解析与快照语义（``RefResolver``）留在引擎
      主进程——与线程路径 ``_run_node`` 完全一致，worker 不需要知道引用语法。
      解析结果要么来自共享目录句柄存储（``SharedHandleStore.put`` 已保证句柄值
      可 pickle），要么是配置/参数字面量，天然可 pickle，因此无需降级路径。
      万一调用方经 overrides 注入了不可 pickle 的参数值，
      ``QueueWorkerExecutor.submit`` 会以 ``RuntimeError`` 明确拒绝——该输入
      不适合远程执行，绝不静默退化。
    - **输出恒经共享目录句柄存储**：worker 返回值要经结果文件（pickle+base64）
      回传，若输出值本身不可 pickle，整个返回 dict 都无法落盘、错误路径很糟；
      改为 worker 把输出 ``put`` 进共享目录（原子发布），返回值只携带句柄名
      映射——不可 pickle 的输出在 ``put`` 处即抛 ``RuntimeError`` 并打包进
      返回 dict，引擎侧还原为节点失败。

    返回 ``{"status", "outputs": {port: handle}, "error", "traceback"}``：
    ``status`` 为 "failed" 时 ``outputs`` 为空 dict——失败不发布任何句柄，
    引擎侧按失败节点处理（下游照常跳过）。传输层恒为 "ok"（异常已打包进
    dict），``_QueueTask.result()`` 拿到的就是这个 dict。
    """
    try:
        node_id = payload["node_id"]
        op_name = payload["op"]
        params = payload["params"]
        inputs = payload["inputs"]
        outputs = payload["outputs"]
        shared_dir = payload["shared_dir"]
        run_info = payload["run"]
        run = RunContext(
            run_id=run_info["run_id"],
            pipeline=run_info["pipeline"],
            params=run_info["params"],
            workdir=Path(run_info["workdir"]),
            artifacts_dir=Path(run_info["artifacts_dir"]),
            logger=log,
        )
        ctx = run.for_node(node_id, op_name)
        ctx.params = params
        # 算子按名经 default_registry 实例化：worker 进程须能 import 算子定义
        # （内置算子天然可见；自定义算子经插件机制或随代码部署，见扩展设计 §4.2）
        operator_cls = default_registry.get(op_name)
        instance = operator_cls(**params)
        raw_outputs = instance.execute(ctx, inputs)
        # 输出发布：写进共享目录句柄存储（引擎侧另一实例从同一目录读取）
        store = SharedHandleStore(shared_dir)
        published: dict[str, str] = {}
        for port, value in raw_outputs.items():
            handle = outputs.get(port)
            if handle:  # 只发布有句柄名的端口（与引擎 _publish 一致）
                store.put(handle, value)
                published[port] = handle
        return {
            "status": "success",
            "outputs": published,
            "error": None,
            "traceback": "",
        }
    except Exception as exc:  # noqa: BLE001 - 需要把任意异常打包回引擎侧
        return {
            "status": "failed",
            "outputs": {},
            "error": f"{type(exc).__name__}: {exc}",
            "traceback": traceback.format_exc(),
        }


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
