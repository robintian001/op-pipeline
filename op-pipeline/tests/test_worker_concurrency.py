"""M6-3 Task 10：队列并发消费（``consume_jobs(workers=N)``）与远程节点取消传播。

语义：
- ``consume_jobs(..., workers=N)``：用标准库 ``ThreadPoolExecutor`` 并发执行已认领
  任务；认领与 offset 递增加锁，结果仍按 ``<job_id>.json`` 原子写；返回顺序按
  认领顺序；``count`` 为最多认领任务数；``workers<=0`` 抛 ``ValueError``；
  ``workers=1`` 保持原有串行循环行为。
- 远程取消：``_QueueTask`` 等待循环检测取消事件后抛 ``CancelledError``；
  引擎把该远程节点标记为 ``skipped``（note 含"取消"），不算失败，运行状态
  ``partial``，且能在 5s 内收尾。
"""
import operator
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from op_pipeline import PipelineSpec, QueueWorkerExecutor, default_registry, run_pipeline
from op_pipeline.remote import consume_jobs


def _slow_add(a, b, delay=0.05):
    """模块级可 pickle 任务：sleep 后相加（并发消费测试用）。"""
    time.sleep(float(delay))
    return a + b


class ConsumeJobsWorkersTest(unittest.TestCase):
    @staticmethod
    def _enqueue(tmp: Path, jobs: int):
        exec_ = QueueWorkerExecutor(tmp / "jobs.jsonl", tmp / "results", poll=0.01)
        tasks = [exec_.submit(_slow_add, i, i, 0.05) for i in range(jobs)]
        return exec_, tasks

    def test_workers_process_all_jobs(self):
        """workers=4 消费 8 个任务：全部 ok，结果正确，返回顺序按认领顺序。"""
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            exec_, tasks = self._enqueue(tmp, 8)
            try:
                processed = consume_jobs(
                    tmp / "jobs.jsonl", tmp / "results",
                    count=8, workers=4, poll=0.01,
                )
                self.assertEqual(len(processed), 8)
                self.assertEqual([p["status"] for p in processed], ["ok"] * 8)
                # 单队列 FIFO：认领顺序 = 提交顺序
                self.assertEqual(
                    [p["job_id"] for p in processed], [t.job_id for t in tasks]
                )
                self.assertEqual(
                    [t.result(timeout=1) for t in tasks], [i + i for i in range(8)]
                )
            finally:
                exec_.shutdown()

    def test_default_workers_single(self):
        """workers 缺省 = 1：串行消费行为不变。"""
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            exec_, tasks = self._enqueue(tmp, 2)
            try:
                processed = consume_jobs(
                    tmp / "jobs.jsonl", tmp / "results", count=2, poll=0.01
                )
                self.assertEqual(len(processed), 2)
                self.assertEqual([p["status"] for p in processed], ["ok", "ok"])
                self.assertEqual([t.result(timeout=1) for t in tasks], [0, 2])
            finally:
                exec_.shutdown()

    def test_invalid_workers_raises(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            for bad in (0, -1):
                with self.assertRaises(ValueError):
                    consume_jobs(
                        tmp / "jobs.jsonl", tmp / "results", count=0, workers=bad
                    )


class QueueTaskCancelTest(unittest.TestCase):
    def test_task_result_raises_cancelled(self):
        """取消置位后 ``_QueueTask.result()`` 抛 concurrent.futures.CancelledError。"""
        from concurrent.futures import CancelledError

        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            exec_ = QueueWorkerExecutor(tmp / "jobs.jsonl", tmp / "results", poll=0.01)
            ev = threading.Event()
            try:
                task = exec_.submit(operator.add, 1, 2, cancel_event=ev)
                threading.Thread(
                    target=lambda: (time.sleep(0.2), ev.set()), daemon=True
                ).start()
                with self.assertRaises(CancelledError):
                    task.result(timeout=5)
            finally:
                exec_.shutdown()


class RemoteCancelTest(unittest.TestCase):
    def test_cancel_marks_remote_node_skipped(self):
        """无 worker 消费时取消：远程节点 5s 内以 skipped 收尾（不算失败）。"""
        spec = PipelineSpec.from_dict(
            {
                "name": "cancel",
                "nodes": [{"id": "a", "op": "control.const",
                           "params": {"value": 1}}],
                "outputs": {"final": "${a.value}"},
            }
        )
        ev = threading.Event()
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            exec_ = QueueWorkerExecutor(tmp / "jobs.jsonl", tmp / "results", poll=0.01)
            threading.Thread(
                target=lambda: (time.sleep(0.5), ev.set()), daemon=True
            ).start()
            started = time.monotonic()
            try:
                report, _ = run_pipeline(
                    spec, registry=default_registry, workdir=tmp, cache_dir=None,
                    executor=exec_, cancel_event=ev,
                )
            finally:
                elapsed = time.monotonic() - started
                exec_.shutdown()
        self.assertLess(elapsed, 5)
        node = report.node("a")
        self.assertEqual(node.status, "skipped")
        self.assertIn("取消", node.note)
        self.assertEqual(report.status, "partial")


class WorkerCliTest(unittest.TestCase):
    def test_cli_workers_process_jobs(self):
        """``worker --workers 4 --count 8``：8 个任务全部 ok（真实 CLI 路径）。"""
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            queue = tmp / "q.jsonl"
            out = tmp / "r"
            exec_ = QueueWorkerExecutor(queue, out, poll=0.01)
            # 子进程 worker 只能 import 标准库函数（自定义测试函数在 tests/ 下
            # 不在其 sys.path），这里用 time.sleep 作为可 pickle 的慢任务
            tasks = [exec_.submit(time.sleep, 0.02) for _ in range(8)]
            try:
                proc = subprocess.run(
                    [
                        sys.executable, "-m", "op_pipeline", "worker",
                        "--queue", str(queue),
                        "--out", str(out),
                        "--count", "8",
                        "--workers", "4",
                        "--poll", "0.01",
                    ],
                    cwd=str(ROOT),
                    capture_output=True,
                    text=True,
                    timeout=60,
                )
                results = [t.result(timeout=1) for t in tasks]
            finally:
                exec_.shutdown()
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(results, [None] * 8)
        self.assertEqual(proc.stdout.count("-> ok"), 8)

    def test_cli_workers_zero_rejected(self):
        """``worker --workers 0``：退出码非 0。"""
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            proc = subprocess.run(
                [
                    sys.executable, "-m", "op_pipeline", "worker",
                    "--queue", str(tmp / "q.jsonl"),
                    "--out", str(tmp / "r"),
                    "--workers", "0",
                ],
                cwd=str(ROOT),
                capture_output=True,
                text=True,
                timeout=30,
            )
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("workers", (proc.stdout + proc.stderr))


if __name__ == "__main__":
    unittest.main()
