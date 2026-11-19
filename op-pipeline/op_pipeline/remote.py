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

M6-3 起补充：
- ``worker --workers N`` / ``consume_jobs(workers=N)``：用标准库线程池并发执行
  已认领任务（认领与 offset 递增加锁）；``count`` 语义为"最多认领的任务数"；
- 等待结果可取消：``_QueueTask`` 持有可选的 ``cancel_event``，取消置位后
  ``result()`` 抛 ``concurrent.futures.CancelledError``（任务本身仍留在队列，
  worker 侧可能继续执行——软取消，与线程路径的已知限制同语义）。
"""
from __future__ import annotations

import base64
import json
import logging
import os
import pickle
import socket
import tempfile
import threading
import time
import traceback
import uuid
from concurrent.futures import CancelledError, ThreadPoolExecutor
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


# --------------------------------------------------------------------------- #
# M7-1：租约（claims/<job_id>.json）辅助
#
# 租约是跨进程的墙钟约定（``time.time()``，不能用 monotonic）：不同 worker
# 可能在不同机器/进程上，只有 epoch 秒可比。claim 的创建必须 O_EXCL 原子，
# 保证同一 job 只有一个 worker 认领成功；续租/回收重写用同目录临时文件 +
# ``os.replace``，读取方不会看到写了一半的 JSON。
# --------------------------------------------------------------------------- #
def _claims_dir(out: Path) -> Path:
    return out / "claims"


def _default_owner() -> str:
    try:
        host = socket.gethostname()
    except OSError:  # 极端环境下取不到主机名：仍给出可用标识
        host = "unknown"
    return f"{host}-{os.getpid()}"


def _read_claim(path: Path) -> dict[str, Any] | None:
    """读 claim；文件被删除/写一半/权限异常时当作"未持有"（返回 None）。"""
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def _try_create_claim(path: Path, data: dict[str, Any]) -> bool:
    """``O_CREAT|O_EXCL`` 原子创建 claim；已存在返回 False（绝不覆盖）。"""
    try:
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError:
        return False
    try:
        os.write(fd, json.dumps(data, ensure_ascii=False).encode("utf-8"))
    finally:
        os.close(fd)
    return True


def _replace_claim(path: Path, data: dict[str, Any]) -> None:
    """同目录临时文件 + ``os.replace`` 原子替换（心跳续租用）。"""
    fd, tmp_name = tempfile.mkstemp(
        dir=str(path.parent), prefix=path.name + ".", suffix=".tmp"
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(json.dumps(data, ensure_ascii=False))
        os.replace(tmp_name, path)
    except BaseException:
        try:
            os.unlink(tmp_name)
        except OSError:
            pass
        raise


def _release_claim(path: Path) -> None:
    try:
        path.unlink()
    except FileNotFoundError:
        pass
    except OSError:  # 并发删除/权限问题：租约过期后仍会被回收，不阻断执行
        pass


def _acquire_claim(
    out: Path,
    job_id: str,
    *,
    owner: str,
    lease_seconds: float,
    attempt: int,
) -> tuple[Path, dict[str, Any]] | None:
    """尝试认领任务：成功返回 ``(claim 路径, claim 内容)``，被他人持有返回 None。

    - 首次 ``O_EXCL`` 创建即成功；
    - 已存在且未过期 → 另一个 worker 持有，跳过；
    - 已过期 → unlink 旧 claim 后重试一次（attempt = 旧 attempt+1），
      再失败（其他 worker 抢先）则跳过本轮。
    行内 ``attempt`` 与旧 claim 的 attempt 取较大者，保证 attempt 单调递增。
    """
    claims = _claims_dir(out)
    claims.mkdir(parents=True, exist_ok=True)
    path = claims / f"{job_id}.json"
    attempt = max(1, int(attempt or 1))
    data = {"owner": owner, "deadline": time.time() + lease_seconds, "attempt": attempt}
    if _try_create_claim(path, data):
        return path, data

    existing = _read_claim(path)
    if existing is None:  # 刚被删除或正在写：本轮不碰，等下一轮
        return None
    try:
        deadline = float(existing.get("deadline", 0.0))
    except (TypeError, ValueError):
        return None
    if deadline >= time.time():
        return None  # 未过期：另一 worker 持有

    try:
        old_attempt = int(existing.get("attempt", attempt))
    except (TypeError, ValueError):
        old_attempt = attempt
    try:
        path.unlink()
    except FileNotFoundError:
        pass
    except OSError:
        return None
    attempt = max(attempt, old_attempt + 1)
    data = {"owner": owner, "deadline": time.time() + lease_seconds, "attempt": attempt}
    if _try_create_claim(path, data):
        return path, data
    return None


class _LeaseHeartbeat:
    """执行期间的后台续租线程（daemon）：每 ``lease_seconds/3`` 刷新 deadline。

    claim 若已被回收（文件消失），线程直接退出——绝不"复活"一个已过期的
    claim，否则原 worker 的结果会与重新执行的结果互相覆盖。
    """

    def __init__(self, path: Path, claim: dict[str, Any], lease_seconds: float):
        self._path = path
        self._claim = dict(claim)
        self._lease_seconds = lease_seconds
        self._interval = max(lease_seconds / 3.0, 0.05)
        self._stop = threading.Event()
        self._thread = threading.Thread(
            target=self._run, name="op-lease-heartbeat", daemon=True
        )

    def __enter__(self) -> "_LeaseHeartbeat":
        self._thread.start()
        return self

    def __exit__(self, *_exc: Any) -> bool:
        self._stop.set()
        self._thread.join(timeout=5)
        return False

    def _run(self) -> None:
        while not self._stop.wait(self._interval):
            if not self._path.is_file():
                return
            self._claim["deadline"] = time.time() + self._lease_seconds
            try:
                _replace_claim(self._path, self._claim)
            except OSError:
                # Windows 上其他 worker 正在读 claim 时 os.replace 可能短暂失败
                # （共享冲突）：下一拍重试，不能因此停止续租。
                continue


class _QueueTask:
    """队列任务的 Future 鸭子类型：``done`` 是属性，``result``/``exception`` 读结果文件。

    ``cancel_event``（M6-3 Task 10）可选：置位后在等待循环中立即抛
    ``concurrent.futures.CancelledError``——不再关心结果文件是否出现
    （任务仍留在队列，worker 侧可能继续执行，结果文件稍后仍会原子写出）。
    """

    def __init__(
        self,
        job_id: str,
        result_path: Path,
        poll: float,
        cancel_event: threading.Event | None = None,
    ) -> None:
        self.job_id = job_id
        self._result_path = result_path
        self._poll = poll
        self._cancel_event = cancel_event
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
            # 结果已落盘优先返回：取消只影响"继续等待"，不影响已就绪的结果
            if self._result_path.is_file():
                self._read()
                continue
            if self._cancel_event is not None and self._cancel_event.is_set():
                raise CancelledError(f"任务 {self.job_id} 的等待已被取消")
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

    def __init__(
        self,
        queue: str | Path,
        out: str | Path,
        *,
        poll: float = 0.1,
        lease_seconds: float = 0.0,
        max_attempts: int = 3,
    ) -> None:
        self._queue = Path(queue)
        self._out = Path(out)
        self._poll = poll
        # M7-1：租约配置随队列行下发（0 = 关闭租约，保持 v0.7.0 行为）
        self.lease_seconds = lease_seconds
        self.max_attempts = max_attempts
        self._out.mkdir(parents=True, exist_ok=True)
        self._closed = False

    @property
    def out(self) -> Path:
        """结果目录（引擎据此推导共享句柄目录，M5-1）。"""
        return self._out

    def submit(
        self,
        fn: Callable[..., Any],
        /,
        *args: Any,
        cancel_event: threading.Event | None = None,
    ) -> _QueueTask:
        if self._closed:
            raise RuntimeError("执行器已关闭，拒绝新任务")
        try:
            payload = _encode_value((fn, args))
        except Exception as exc:  # pickle 失败典型：lambda/闭包/不可序列化对象
            raise RuntimeError(f"任务不可 pickle，无法提交到队列：{exc}") from exc
        job_id = uuid.uuid4().hex
        line = json.dumps(
            {
                "job_id": job_id,
                "payload": payload,
                # M7-1：老 worker 读多余键无影响；缺这两个键时按 worker 参数处理
                "lease_seconds": self.lease_seconds,
                "max_attempts": self.max_attempts,
            },
            ensure_ascii=False,
        )
        with self._queue.open("a", encoding="utf-8") as fh:
            fh.write(line + "\n")
        return _QueueTask(
            job_id, self._out / f"{job_id}.json", self._poll, cancel_event=cancel_event
        )

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


def _run_one(line: str, out: Path) -> dict[str, Any]:
    """执行队列中的一行并原子写回结果文件，返回 ``{"job_id", "status"}``。

    任务失败不向调用方抛异常：错误打包进 ``<job_id>.json``（``status="error"``），
    worker 继续消费后续任务。
    """
    item = json.loads(line)
    job_id = item["job_id"]
    try:
        fn, args = _decode_value(item["payload"])
        value = fn(*args)
        status, result, error = "ok", _encode_value(value), None
    except Exception as exc:  # noqa: BLE001 - 任务失败写回结果，worker 不退出
        status, result, error = "error", None, f"{type(exc).__name__}: {exc}"
    _atomic_write(out / f"{job_id}.json", {
        "job_id": job_id, "status": status, "result": result, "error": error,
    })
    return {"job_id": job_id, "status": status}


def _process_line(
    line: str,
    out: Path,
    *,
    owner: str,
    lease_seconds: float,
) -> dict[str, Any] | None:
    """执行一行；租约开启时先互斥认领。返回 None 表示"跳过（不计数）"。

    行内 ``lease_seconds>0`` 覆盖函数参数（老队列行缺该键时按 worker 参数）；
    ``lease_seconds<=0`` 时完全不碰 claims 目录，行为与 v0.7.0 一致。
    """
    try:
        item = json.loads(line)
    except json.JSONDecodeError:
        return _run_one(line, out)  # 坏行保持旧行为：在 _run_one 处报错
    job_id = item.get("job_id")
    if not job_id:
        return _run_one(line, out)
    try:
        job_lease = float(item.get("lease_seconds", lease_seconds) or 0.0)
    except (TypeError, ValueError):
        job_lease = lease_seconds
    if job_lease <= 0:
        return _run_one(line, out)
    # 已完成（结果文件已存在）→ 跳过，即使残留 claim 也不再执行
    if (out / f"{job_id}.json").is_file():
        return None
    claimed = _acquire_claim(
        out,
        job_id,
        owner=owner,
        lease_seconds=job_lease,
        attempt=item.get("attempt") or 1,
    )
    if claimed is None:
        return None  # 另一 worker 持有 / 抢输 / claim 正在写
    claim_path, claim = claimed
    try:
        with _LeaseHeartbeat(claim_path, claim, job_lease):
            return _run_one(line, out)
    finally:
        _release_claim(claim_path)


def _try_reclaim_lock(claims: Path, job_id: str) -> Path | None:
    """``O_EXCL`` 抢占回收锁，避免多个 worker 对同一过期任务重复追加队列行。"""
    lock = claims / f".reclaim-{job_id}.lock"
    try:
        fd = os.open(lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError:
        return None
    except OSError:
        return None
    os.close(fd)
    return lock


def _append_queue_line(queue: Path, item: dict[str, Any]) -> None:
    line = json.dumps(item, ensure_ascii=False)
    with queue.open("a", encoding="utf-8") as fh:
        fh.write(line + "\n")


def _scan_expired_claims(
    queue: Path, out: Path, *, default_max_attempts: int
) -> list[str]:
    """扫描 ``<out>/claims`` 并回收过期租约（空闲时每个 poll 周期最多一次）。

    - 对应 job 已有结果 → 只删除 claim（清理）；
    - attempt < max_attempts → 删除 claim，把原队列行（attempt+1）追加到队列末尾；
    - attempt >= max_attempts → 删除 claim，写 error 结果，不再重投。
    每个 job 的回收处理都先抢 ``.reclaim-<job_id>.lock``，抢不到直接跳过。
    """
    claims = _claims_dir(out)
    if not claims.is_dir():
        return []
    now = time.time()
    expired: list[Path] = []
    for path in claims.glob("*.json"):
        claim = _read_claim(path)
        if claim is None:
            continue
        try:
            deadline = float(claim.get("deadline", 0.0))
        except (TypeError, ValueError):
            continue
        if deadline < now:
            expired.append(path)
    if not expired:
        return []

    lines = queue.read_text(encoding="utf-8").splitlines() if queue.exists() else []
    by_job: dict[str, dict[str, Any]] = {}
    for raw in lines:
        try:
            item = json.loads(raw)
        except json.JSONDecodeError:
            continue
        job_id = item.get("job_id")
        if job_id:
            by_job[job_id] = item  # 同 job 多次重投时以最后一行为准

    reclaimed: list[str] = []
    for path in expired:
        job_id = path.stem
        lock = _try_reclaim_lock(claims, job_id)
        if lock is None:
            continue
        try:
            claim = _read_claim(path)
            if claim is None:
                continue
            try:
                deadline = float(claim.get("deadline", 0.0))
            except (TypeError, ValueError):
                continue
            if deadline >= time.time():
                continue  # 抢锁期间被心跳续租
            if (out / f"{job_id}.json").is_file():
                _release_claim(path)
                continue
            item = by_job.get(job_id)
            if item is None:  # 队列行已被归档/删除：孤儿 claim 直接清理
                _release_claim(path)
                continue
            try:
                attempt = int(claim.get("attempt", item.get("attempt") or 1))
            except (TypeError, ValueError):
                attempt = 1
            try:
                max_attempts = int(item.get("max_attempts", default_max_attempts))
            except (TypeError, ValueError):
                max_attempts = default_max_attempts
            _release_claim(path)
            if attempt >= max_attempts:
                _atomic_write(
                    out / f"{job_id}.json",
                    {
                        "job_id": job_id,
                        "status": "error",
                        "result": None,
                        "error": (
                            f"超过重投上限：attempt={attempt} >= "
                            f"max_attempts={max_attempts}"
                        ),
                    },
                )
                continue
            new_item = dict(item)
            new_item["attempt"] = attempt + 1
            _append_queue_line(queue, new_item)
            reclaimed.append(job_id)
        finally:
            try:
                lock.unlink()
            except OSError:
                pass
    return reclaimed


def _consume_sequential(
    queue: Path,
    out: Path,
    *,
    count: int | None,
    poll: float,
    stop_event: threading.Event | None,
    owner: str,
    lease_seconds: float,
    max_attempts: int,
) -> list[dict[str, Any]]:
    """单 worker 消费（M5-1 原循环）：逐行执行；租约关闭时行为与历史一致。"""
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
            # 空闲：扫描过期租约（重投的行会作为"新增行"被后续 offset 读到）
            _scan_expired_claims(queue, out, default_max_attempts=max_attempts)
            time.sleep(poll)
            continue
        for line in new_lines:
            if count is not None and consumed >= count:
                break
            result = _process_line(
                line,
                out,
                owner=owner,
                lease_seconds=lease_seconds,
            )
            offset += 1
            if result is not None:  # 认领失败/已完成的作品不计数（count 只数实际执行）
                processed.append(result)
                consumed += 1
        if count is not None and consumed >= count:
            break
    return processed


def _consume_parallel(
    queue: Path,
    out: Path,
    *,
    count: int | None,
    poll: float,
    stop_event: threading.Event | None,
    workers: int,
    owner: str,
    lease_seconds: float,
    max_attempts: int,
) -> list[dict[str, Any]]:
    """多 worker 并发消费（M6-3 Task 10）：认领与 offset 递增加锁。

    - 每个 worker 循环"认领一行 -> 执行 -> 写结果文件"；认领到的下标即
      返回列表中的位置，因此返回顺序恒为认领顺序（单队列 FIFO = 提交顺序）；
    - ``count`` 为"最多实际执行的任务数"（M7-1：认领失败/跳过不计数）；
    - ``stop_event`` 置位后不再认领新任务，已认领的任务仍执行完并写结果；
    - 空闲线程每个 ``poll`` 至多触发一次过期租约扫描（共享时间戳去重）。
    """
    lock = threading.Lock()
    scan_lock = threading.Lock()
    offset = 0
    claimed = 0
    inflight = 0
    last_scan = 0.0
    processed: list[dict[str, Any] | None] = []

    def _claim() -> tuple[tuple[int, str] | None, bool]:
        """返回 ``((下标, 行), False)``；无新任务时 ``(None, 是否已达 count 上限)``。"""
        nonlocal offset, inflight
        with lock:
            if count is not None and claimed + inflight >= count:
                return None, True
            lines = (
                queue.read_text(encoding="utf-8").splitlines() if queue.exists() else []
            )
            if offset >= len(lines):
                return None, False
            line = lines[offset]
            offset += 1
            inflight += 1
            index = len(processed)
            processed.append(None)
            return (index, line), False

    def _maybe_scan() -> None:
        nonlocal last_scan
        if not scan_lock.acquire(blocking=False):
            return
        try:
            now = time.monotonic()
            if now - last_scan < poll:
                return
            last_scan = now
            _scan_expired_claims(queue, out, default_max_attempts=max_attempts)
        finally:
            scan_lock.release()

    def _worker() -> None:
        nonlocal claimed, inflight
        while True:
            if stop_event is not None and stop_event.is_set():
                return
            item, exhausted = _claim()
            if item is None:
                if exhausted:
                    return
                _maybe_scan()
                time.sleep(poll)
                continue
            index, line = item
            try:
                result = _process_line(
                    line,
                    out,
                    owner=owner,
                    lease_seconds=lease_seconds,
                )
            finally:
                with lock:
                    inflight -= 1
            if result is not None:
                with lock:
                    claimed += 1
                processed[index] = result

    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="op-worker") as pool:
        for _ in range(workers):
            pool.submit(_worker)
        # 退出 with 时等待全部 worker 结束（与串行版的阻塞语义相同）
    return [item for item in processed if item is not None]


def consume_jobs(
    queue: str | Path,
    out: str | Path,
    *,
    count: int | None = None,
    poll: float = 0.1,
    stop_event: threading.Event | None = None,
    workers: int = 1,
    owner: str | None = None,
    lease_seconds: float = 0.0,
    max_attempts: int = 3,
) -> list[dict[str, Any]]:
    """worker 侧：增量消费 JSONL 队列并把每个任务的结果写回 ``out`` 目录。

    - ``count``：最多消费（认领）的任务数；``None`` 表示持续监听（直到
      stop_event / Ctrl+C）；
    - ``workers``（M6-3 Task 10）：并发执行任务的 worker 数，默认 1（保持原
      串行循环行为）；``<= 0`` 抛 ``ValueError``。并发时认领与 offset 递增
      在同一把锁内完成，结果仍按 ``<job_id>.json`` 原子写，返回顺序按认领顺序；
    - ``owner``/``lease_seconds``/``max_attempts``（M7-1）：任务租约配置；
      ``lease_seconds<=0`` 时完全不碰 ``claims/``，行为与 v0.7.0 一致；
      队列行内的 ``lease_seconds``/``max_attempts`` 优先于这里（行内缺失才用
      这些默认值）；
    - 返回已消费任务的 ``[{"job_id", "status"}]`` 列表，供 CLI 打印与测试断言。
    """
    if workers <= 0:
        raise ValueError(f"workers 必须是正整数，实际是 {workers}")
    queue = Path(queue)
    out = Path(out)
    out.mkdir(parents=True, exist_ok=True)
    owner = owner or _default_owner()
    if workers == 1:
        return _consume_sequential(
            queue,
            out,
            count=count,
            poll=poll,
            stop_event=stop_event,
            owner=owner,
            lease_seconds=lease_seconds,
            max_attempts=max_attempts,
        )
    return _consume_parallel(
        queue,
        out,
        count=count,
        poll=poll,
        stop_event=stop_event,
        workers=workers,
        owner=owner,
        lease_seconds=lease_seconds,
        max_attempts=max_attempts,
    )


def compact_queue(
    queue: str | Path,
    out: str | Path,
    archive: str | Path | None = None,
) -> tuple[list[str], Path]:
    """把已处理条目归档、重写活跃队列（M7-1 Task 3）。

    返回 ``(未完成 job_id 列表, 归档文件路径)``。存在未完成任务时不改动任何
    文件（调用方据此返回非 0 退出码）；全部完成时把原始行追加进归档文件，
    再用同目录临时文件 + ``os.replace`` 把活跃队列原子重写为空。
    """
    queue = Path(queue)
    out = Path(out)
    if not queue.exists():
        raise FileNotFoundError(f"队列文件不存在：{queue}")
    target = (
        Path(archive).expanduser()
        if archive is not None
        else queue.with_name(queue.stem + ".archive.jsonl")
    )
    lines = [raw for raw in queue.read_text(encoding="utf-8").splitlines() if raw.strip()]
    pending: list[str] = []
    for raw in lines:
        try:
            item = json.loads(raw)
        except json.JSONDecodeError:
            pending.append("(无法解析的行)")
            continue
        job_id = item.get("job_id")
        if not job_id or not (out / f"{job_id}.json").is_file():
            pending.append(str(job_id or "(缺少 job_id)"))
    if pending:
        return pending, target
    if lines:
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open("a", encoding="utf-8") as fh:
            for raw in lines:
                fh.write(raw + "\n")
    fd, tmp_name = tempfile.mkstemp(
        dir=str(queue.parent), prefix=queue.name + ".", suffix=".tmp"
    )
    os.close(fd)
    os.replace(tmp_name, queue)  # 空临时文件原子替换活跃队列
    return [], target

