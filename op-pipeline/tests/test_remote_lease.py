"""M7-1：远程队列执行的租约、心跳、过期回收与队列归档。

覆盖：
- Task 1：两个 worker 争抢同一任务只执行一次；未开启租约时行为与 v0.7.0 一致；
  任务结束后 claim 被删除；已完成任务即使残留 claim 也不再执行。
- Task 2：过期 claim 被接管（attempt 递增）；空闲时回收并重新入队（attempt+1）；
  attempt 达到 max_attempts 时写 error 结果且不再执行；短租约 + 慢任务由心跳续租。
- Task 3：``queue compact`` 全部完成时归档并清空队列，存在未完成任务时拒绝且不改动文件。
"""
import json
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from op_pipeline import QueueWorkerExecutor  # noqa: E402
from op_pipeline.cli import main as cli_main  # noqa: E402
from op_pipeline.remote import (  # noqa: E402
    _encode_value,
    compact_queue,
    consume_jobs,
)

# 模块级计数（跨线程共享；任务函数必须模块级才能被 pickle）
_COUNTER: dict[str, int] = {}
_COUNTER_LOCK = threading.Lock()


def _reset_counter() -> None:
    with _COUNTER_LOCK:
        _COUNTER.clear()


def _count(key: str) -> int:
    with _COUNTER_LOCK:
        return _COUNTER.get(key, 0)


def _bump(key: str) -> None:
    with _COUNTER_LOCK:
        _COUNTER[key] = _COUNTER.get(key, 0) + 1


def _counted_task(seconds: float, key: str) -> str:
    """记录执行次数后 sleep，返回 key（验证"只执行一次"用）。"""
    _bump(key)
    if seconds:
        time.sleep(float(seconds))
    return key


def _claim_attempts(claims_dir: str) -> list[int]:
    """读取当前 claims 目录下所有 claim 的 attempt（验证增量用）。"""
    for _ in range(100):
        found: list[int] = []
        for path in Path(claims_dir).glob("*.json"):
            try:
                found.append(
                    int(json.loads(path.read_text(encoding="utf-8"))["attempt"])
                )
            except (OSError, json.JSONDecodeError, KeyError, TypeError, ValueError):
                continue
        if found:
            return found
        time.sleep(0.01)
    return []


def _write_claim(path: Path, *, attempt: int, deadline: float, owner: str = "dead") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps({"owner": owner, "deadline": deadline, "attempt": attempt}),
        encoding="utf-8",
    )


def _wait_for(predicate, timeout: float = 10.0, interval: float = 0.01) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


def _result(out: Path, job_id: str) -> dict | None:
    path = out / f"{job_id}.json"
    if not path.is_file():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None


def _join(thread: threading.Thread, timeout: float = 20.0) -> None:
    thread.join(timeout=timeout)


class LeaseClaimTest(unittest.TestCase):
    """Task 1：互斥认领与兼容性。"""

    def setUp(self) -> None:
        _reset_counter()

    def test_two_workers_execute_once(self):
        """两个 worker 争抢同一队列：只有认领成功的执行，任务只执行一次。"""
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            queue, out = tmp / "jobs.jsonl", tmp / "results"
            ex = QueueWorkerExecutor(queue, out, poll=0.01, lease_seconds=1.0)
            task = ex.submit(_counted_task, 0.3, "race")
            barrier = threading.Barrier(2)
            stop = threading.Event()
            results: list[list[dict]] = []

            def worker(name: str) -> None:
                barrier.wait()
                results.append(
                    consume_jobs(
                        queue, out, count=1, poll=0.01, owner=name,
                        lease_seconds=1.0, stop_event=stop,
                    )
                )

            threads = [
                threading.Thread(target=worker, args=(f"w{i}",)) for i in range(2)
            ]
            watcher = threading.Thread(
                target=lambda: (
                    _wait_for(lambda: (out / f"{task.job_id}.json").is_file(), 10),
                    stop.set(),
                ),
                daemon=True,
            )
            for t in threads:
                t.start()
            watcher.start()
            for t in threads:
                _join(t)
            watcher.join(timeout=5)

            self.assertFalse(any(t.is_alive() for t in threads), "worker 未退出")
            self.assertEqual(_count("race"), 1, "任务被重复执行")
            self.assertEqual(sum(len(r) for r in results), 1, "只应有一个 worker 计数")
            data = _result(out, task.job_id)
            self.assertIsNotNone(data)
            self.assertEqual(data["status"], "ok")
            ex.shutdown()

    def test_no_lease_both_workers_execute(self):
        """未开启租约：两个 worker 都会执行同一行（与 v0.7.0 一致）。"""
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            queue, out = tmp / "jobs.jsonl", tmp / "results"
            ex = QueueWorkerExecutor(queue, out, poll=0.01)
            ex.submit(_counted_task, 0.4, "nolease")
            barrier = threading.Barrier(2)

            def worker() -> None:
                barrier.wait()
                consume_jobs(queue, out, count=1, poll=0.01)

            threads = [threading.Thread(target=worker) for _ in range(2)]
            for t in threads:
                t.start()
            for t in threads:
                _join(t)
            self.assertFalse(any(t.is_alive() for t in threads))
            self.assertEqual(_count("nolease"), 2)
            self.assertFalse((out / "claims").exists(), "租约关闭时不应创建 claims 目录")
            ex.shutdown()

    def test_claim_removed_after_execution(self):
        """认领成功并执行结束后，claim 文件被删除。"""
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            queue, out = tmp / "jobs.jsonl", tmp / "results"
            ex = QueueWorkerExecutor(queue, out, poll=0.01, lease_seconds=1.0)
            task = ex.submit(_counted_task, 0.05, "cleanup")
            processed = consume_jobs(
                queue, out, count=1, poll=0.01, lease_seconds=1.0
            )
            self.assertEqual(len(processed), 1)
            self.assertTrue((out / "claims").is_dir())
            self.assertEqual(list((out / "claims").glob("*.json")), [])
            self.assertEqual(_count("cleanup"), 1)
            self.assertEqual(_result(out, task.job_id)["status"], "ok")
            ex.shutdown()

    def test_completed_job_with_claim_not_reexecuted(self):
        """已有结果文件时，即使 claim 存在也不再执行。"""
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            queue, out = tmp / "jobs.jsonl", tmp / "results"
            ex = QueueWorkerExecutor(queue, out, poll=0.01, lease_seconds=1.0)
            task = ex.submit(_counted_task, 0.0, "done")
            (out / f"{task.job_id}.json").write_text(
                json.dumps(
                    {"job_id": task.job_id, "status": "ok",
                     "result": _encode_value("pre"), "error": None}
                ),
                encoding="utf-8",
            )
            _write_claim(
                out / "claims" / f"{task.job_id}.json",
                attempt=1, deadline=time.time() + 60,
            )
            stop = threading.Event()
            thread = threading.Thread(
                target=consume_jobs,
                args=(queue, out),
                kwargs={"poll": 0.01, "stop_event": stop, "lease_seconds": 1.0},
            )
            thread.start()
            time.sleep(0.4)
            stop.set()
            _join(thread, timeout=10)
            self.assertFalse(thread.is_alive())
            self.assertEqual(_count("done"), 0, "已完成任务不应再次执行")
            self.assertEqual(_result(out, task.job_id)["result"], _encode_value("pre"))
            ex.shutdown()


class LeaseRecoveryTest(unittest.TestCase):
    """Task 2：心跳续租与过期回收。"""

    def setUp(self) -> None:
        _reset_counter()

    def test_expired_claim_taken_over_with_attempt_increment(self):
        """过期 claim 被接管：attempt = 旧 attempt + 1。"""
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            queue, out = tmp / "jobs.jsonl", tmp / "results"
            ex = QueueWorkerExecutor(queue, out, poll=0.01, lease_seconds=0.5)
            task = ex.submit(_claim_attempts, str(out / "claims"))
            _write_claim(
                out / "claims" / f"{task.job_id}.json",
                attempt=7, deadline=time.time() - 1,
            )
            processed = consume_jobs(
                queue, out, count=1, poll=0.01, lease_seconds=0.5
            )
            self.assertEqual(len(processed), 1)
            data = _result(out, task.job_id)
            self.assertEqual(data["status"], "ok")
            self.assertEqual(list(_decode_attempts(data["result"])), [8])
            ex.shutdown()

    def test_idle_scan_reclaims_and_requeues(self):
        """空闲扫描：过期 claim 被删除并把原行（attempt+1）追加到队列末尾。"""
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            queue, out = tmp / "jobs.jsonl", tmp / "results"
            ex = QueueWorkerExecutor(queue, out, poll=0.01, lease_seconds=0.5,
                                     max_attempts=3)
            held = ex.submit(_counted_task, 0.0, "reclaim")
            sentinel = ex.submit(_counted_task, 0.0, "sentinel")
            claim_path = out / "claims" / f"{held.job_id}.json"
            _write_claim(claim_path, attempt=1, deadline=time.time() + 60)
            stop = threading.Event()
            thread = threading.Thread(
                target=consume_jobs,
                args=(queue, out),
                kwargs={"poll": 0.01, "stop_event": stop, "lease_seconds": 0.5},
            )
            thread.start()
            try:
                # sentinel 完成 = worker 已越过被占用的首行（offset >= 2），进入空闲
                self.assertTrue(
                    _wait_for(lambda: (out / f"{sentinel.job_id}.json").is_file(), 10),
                    "worker 未消费 sentinel 任务",
                )
                _write_claim(claim_path, attempt=1, deadline=time.time() - 1)
                self.assertTrue(
                    _wait_for(lambda: (out / f"{held.job_id}.json").is_file(), 10),
                    "过期任务未被回收执行",
                )
            finally:
                stop.set()
                _join(thread)
            self.assertFalse(thread.is_alive())
            self.assertEqual(_count("reclaim"), 1)
            self.assertEqual(_result(out, held.job_id)["status"], "ok")
            lines = [
                json.loads(raw)
                for raw in queue.read_text(encoding="utf-8").splitlines()
                if raw.strip()
            ]
            attempts = [
                item.get("attempt") for item in lines
                if item.get("job_id") == held.job_id
            ]
            self.assertIn(2, attempts, f"应有 attempt=2 的重投行，实际：{attempts}")
            ex.shutdown()

    def test_attempt_limit_writes_error(self):
        """attempt 达到 max_attempts：写 error 结果，不再重投。"""
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            queue, out = tmp / "jobs.jsonl", tmp / "results"
            ex = QueueWorkerExecutor(queue, out, poll=0.01, lease_seconds=0.5,
                                     max_attempts=3)
            held = ex.submit(_counted_task, 0.0, "overlimit")
            sentinel = ex.submit(_counted_task, 0.0, "sentinel2")
            claim_path = out / "claims" / f"{held.job_id}.json"
            _write_claim(claim_path, attempt=3, deadline=time.time() + 60)
            stop = threading.Event()
            thread = threading.Thread(
                target=consume_jobs,
                args=(queue, out),
                kwargs={"poll": 0.01, "stop_event": stop, "lease_seconds": 0.5},
            )
            thread.start()
            try:
                self.assertTrue(
                    _wait_for(lambda: (out / f"{sentinel.job_id}.json").is_file(), 10),
                    "worker 未消费 sentinel 任务",
                )
                _write_claim(claim_path, attempt=3, deadline=time.time() - 1)
                self.assertTrue(
                    _wait_for(lambda: (out / f"{held.job_id}.json").is_file(), 10),
                    "超限任务未写 error 结果",
                )
            finally:
                stop.set()
                _join(thread)
            data = _result(out, held.job_id)
            self.assertEqual(data["status"], "error")
            self.assertIn("超过重投上限", data["error"])
            self.assertIn("attempt=3", data["error"])
            self.assertEqual(_count("overlimit"), 0, "超限任务不应被执行")
            self.assertFalse(claim_path.exists(), "超限后 claim 应被删除")
            lines = [
                json.loads(raw)
                for raw in queue.read_text(encoding="utf-8").splitlines()
                if raw.strip()
            ]
            self.assertEqual(
                [item.get("job_id") for item in lines].count(held.job_id), 1,
                "超限后不应再追加重投行",
            )
            ex.shutdown()

    def test_heartbeat_keeps_long_task_alive(self):
        """短租约（0.3s）+ 任务 sleep 1.2s：心跳续租使任务不被回收，只执行一次。"""
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            queue, out = tmp / "jobs.jsonl", tmp / "results"
            ex = QueueWorkerExecutor(queue, out, poll=0.01, lease_seconds=0.3)
            task = ex.submit(_counted_task, 1.2, "heartbeat")
            stop = threading.Event()
            threads = [
                threading.Thread(
                    target=consume_jobs,
                    args=(queue, out),
                    kwargs={"poll": 0.02, "stop_event": stop,
                            "lease_seconds": 0.3, "owner": name},
                )
                for name in ("hb-a", "hb-b")
            ]
            for t in threads:
                t.start()
            try:
                self.assertTrue(
                    _wait_for(lambda: (out / f"{task.job_id}.json").is_file(), 15),
                    "长任务未完成",
                )
            finally:
                stop.set()
                for t in threads:
                    _join(t)
            self.assertFalse(any(t.is_alive() for t in threads))
            self.assertEqual(_count("heartbeat"), 1, "心跳失败导致任务被重复执行")
            data = _result(out, task.job_id)
            self.assertEqual(data["status"], "ok")
            self.assertIsNone(data["error"])
            ex.shutdown()


def _decode_attempts(raw: str) -> list[int]:
    from op_pipeline.remote import _decode_value

    return list(_decode_value(raw))


class QueueCompactTest(unittest.TestCase):
    """Task 3：队列归档（纯 Python 与 CLI 两条路径）。"""

    def test_compact_all_completed(self):
        """全部完成：归档原条目、清空活跃队列；重复执行不丢数据。"""
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            queue, out = tmp / "jobs.jsonl", tmp / "results"
            ex = QueueWorkerExecutor(queue, out, poll=0.01)
            ex.submit(_counted_task, 0.0, "c1")
            ex.submit(_counted_task, 0.0, "c2")
            consume_jobs(queue, out, count=2, poll=0.01)
            pending, archive = compact_queue(queue, out)
            self.assertEqual(pending, [])
            self.assertEqual(archive, tmp / "jobs.archive.jsonl")
            self.assertEqual(queue.read_text(encoding="utf-8"), "")
            self.assertEqual(
                len(archive.read_text(encoding="utf-8").splitlines()), 2
            )
            # 第二次执行：队列为空 → 直接成功，不重复写入归档
            pending2, archive2 = compact_queue(queue, out)
            self.assertEqual(pending2, [])
            self.assertEqual(archive2, archive)
            self.assertEqual(
                len(archive.read_text(encoding="utf-8").splitlines()), 2
            )
            ex.shutdown()

    def test_compact_cli_refuses_pending(self):
        """有未完成任务：CLI 返回非 0 且队列文件不变。"""
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            queue, out = tmp / "jobs.jsonl", tmp / "results"
            ex = QueueWorkerExecutor(queue, out, poll=0.01)
            first = ex.submit(_counted_task, 0.0, "p1")
            ex.submit(_counted_task, 0.0, "p2")
            consume_jobs(queue, out, count=1, poll=0.01)
            before = queue.read_text(encoding="utf-8")
            code = cli_main(
                ["queue", "compact", "--queue", str(queue), "--out", str(out)]
            )
            self.assertNotEqual(code, 0)
            self.assertEqual(queue.read_text(encoding="utf-8"), before)
            self.assertFalse((tmp / "jobs.archive.jsonl").exists())
            self.assertTrue((out / f"{first.job_id}.json").is_file())
            ex.shutdown()

    def test_compact_cli_success(self):
        """全部完成后 CLI 返回 0，归档文件存在、活跃队列为空。"""
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            queue, out = tmp / "jobs.jsonl", tmp / "results"
            ex = QueueWorkerExecutor(queue, out, poll=0.01)
            ex.submit(_counted_task, 0.0, "s1")
            consume_jobs(queue, out, count=1, poll=0.01)
            code = cli_main(
                [
                    "queue", "compact",
                    "--queue", str(queue),
                    "--out", str(out),
                    "--archive", str(tmp / "custom.jsonl"),
                ]
            )
            self.assertEqual(code, 0)
            self.assertEqual(queue.read_text(encoding="utf-8"), "")
            self.assertEqual(len((tmp / "custom.jsonl").read_text().splitlines()), 1)
            ex.shutdown()

    def test_compact_missing_queue_fails(self):
        """队列文件不存在：CLI 返回非 0。"""
        with tempfile.TemporaryDirectory() as tmp:
            code = cli_main(
                [
                    "queue", "compact",
                    "--queue", str(Path(tmp) / "nope.jsonl"),
                    "--out", str(Path(tmp) / "results"),
                ]
            )
            self.assertNotEqual(code, 0)


if __name__ == "__main__":
    unittest.main()
