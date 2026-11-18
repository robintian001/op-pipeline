"""远程执行示范（M4-4）：队列执行器 + worker 进程 双进程演示。

本脚本扮演"引擎进程"：用 ``QueueWorkerExecutor`` 把一批任务（代表节点工作）
提交进共享 JSONL 队列，另起 ``op_pipeline worker`` 子进程消费执行，
结果/异常写回共享结果目录，客户端 ``wait`` 收集并打印。

用法：``python examples/remote_exec_demo.py``

注意：任务函数必须可 pickle（如 ``operator.add`` / 内置 ``sum``），
worker 子进程按模块名反序列化——定义在脚本里的函数对 worker 不可见。
"""
from __future__ import annotations

import operator
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from op_pipeline import QueueWorkerExecutor  # noqa: E402


def main() -> int:
    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        queue = tmp / "jobs.jsonl"
        out = tmp / "results"
        exec_ = QueueWorkerExecutor(queue, out, poll=0.05)

        # 提交三个"节点任务"：两个成功 + 一个故意失败（演示异常回写）
        f1 = exec_.submit(operator.add, 40, 2)      # 42
        f2 = exec_.submit(sum, [1, 2, 3, 4, 5])      # 15
        f3 = exec_.submit(operator.truediv, 1, 0)    # ZeroDivisionError

        print(f"队列文件：{queue}\n结果目录：{out}")
        print(f"已提交 3 个任务，启动 worker 子进程…")

        proc = subprocess.Popen(
            [sys.executable, "-m", "op_pipeline", "worker",
             "--queue", str(queue), "--out", str(out), "--count", "3", "--poll", "0.05"],
            cwd=str(ROOT),
        )
        try:
            done = exec_.wait([f1, f2, f3], first_completed=False)
            assert len(done) == 3
        finally:
            proc.wait(timeout=60)

        print("结果：")
        for name, f in (("add(40, 2)", f1), ("sum([1..5])", f2)):
            print(f"  {name:<14} -> {f.result()}")
        try:
            f3.result()
        except RuntimeError as exc:
            print(f"  truediv(1, 0) -> 异常回写: {exc}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
