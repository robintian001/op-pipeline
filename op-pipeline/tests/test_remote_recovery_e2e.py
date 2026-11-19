"""M7-1 Task 3.2：双进程故障注入 e2e（worker 崩溃后由另一 worker 接管）。

流程：主进程提交一个长任务 → worker A 认领后被 ``terminate()`` → worker B
等待租约过期后接管并产出结果。所有子进程在 ``finally`` 中强制清理；stdout /
stderr 使用 DEVNULL（PIPE 会因日志撑爆缓冲区导致死锁）。
"""
import json
import os
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TESTS = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from op_pipeline import QueueWorkerExecutor  # noqa: E402

TASK_SECONDS = 4.0
LEASE_SECONDS = 1.0
MAX_ATTEMPTS = 3


def _long_task(seconds: float) -> str:
    """模块级可 pickle 任务：worker 子进程经 PYTHONPATH 导入本模块后执行。"""
    time.sleep(float(seconds))
    return "done"


class RemoteRecoveryE2ETest(unittest.TestCase):
    def test_crashed_worker_job_taken_over(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            queue, out = tmp / "jobs.jsonl", tmp / "results"
            ex = QueueWorkerExecutor(
                queue, out, poll=0.02, lease_seconds=LEASE_SECONDS,
                max_attempts=MAX_ATTEMPTS,
            )
            task = ex.submit(_long_task, TASK_SECONDS)

            env = dict(os.environ)
            env["PYTHONPATH"] = (
                str(TESTS)
                + (os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else "")
            )
            procs: list[subprocess.Popen] = []

            def spawn() -> subprocess.Popen:
                proc = subprocess.Popen(
                    [
                        sys.executable, "-m", "op_pipeline", "worker",
                        "--queue", str(queue),
                        "--out", str(out),
                        "--lease-seconds", str(LEASE_SECONDS),
                        "--max-attempts", str(MAX_ATTEMPTS),
                        "--count", "1",
                        "--poll", "0.02",
                    ],
                    cwd=str(ROOT),
                    env=env,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
                procs.append(proc)
                return proc

            started = time.monotonic()
            try:
                worker_a = spawn()
                claims = out / "claims"
                self.assertTrue(
                    self._wait(
                        lambda: claims.is_dir() and any(claims.glob("*.json")),
                        timeout=35,
                    ),
                    "worker A 未创建 claim（任务未被认领）",
                )
                worker_a.terminate()
                worker_a.wait(timeout=10)

                worker_b = spawn()
                result_path = out / f"{task.job_id}.json"
                self.assertTrue(
                    self._wait(lambda: result_path.is_file(), timeout=30),
                    "worker B 未在租约过期后接管并产出结果",
                )
                data = json.loads(result_path.read_text(encoding="utf-8"))
                self.assertEqual(set(data), {"job_id", "status", "result", "error"})
                self.assertEqual(data["status"], "ok")
                self.assertIsNone(data["error"])
                self.assertEqual(worker_b.wait(timeout=30), 0)
            finally:
                for proc in procs:
                    if proc.poll() is None:
                        proc.terminate()
                for proc in procs:
                    try:
                        proc.wait(timeout=10)
                    except subprocess.TimeoutExpired:  # pragma: no cover - 兜底
                        proc.kill()
                        proc.wait(timeout=5)
                ex.shutdown()
            elapsed = time.monotonic() - started
            self.assertLess(elapsed, 60, f"e2e 超过 60s：{elapsed:.1f}s")

    @staticmethod
    def _wait(predicate, timeout: float, interval: float = 0.02) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if predicate():
                return True
            time.sleep(interval)
        return False


if __name__ == "__main__":
    unittest.main()
