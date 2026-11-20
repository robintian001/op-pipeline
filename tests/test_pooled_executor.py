"""M9 Task 6：PooledProcessExecutor——常驻 worker 进程池执行器测试。

运行：``python -m unittest discover -s tests -k PooledExecutor -v``

与 ``ProcessExecutor``（每任务一进程）不同，池化执行器用 N 个常驻 spawn
子进程复用执行多个节点任务。本文件覆盖：协议契约与结果回传、硬超时
（terminate 在跑 worker + 自动补位 / 排队任务到点失败）、worker 崩溃恢复
（``os._exit`` 模拟）、并发上限、不可 pickle 回退，以及引擎级
``executor="process_pool"`` 全链路接线。

注意：辅助函数必须模块级——子进程按引用 pickle 反序列化（同 M2-2 约定）。
"""
from __future__ import annotations

import os
import sys
import tempfile
import time
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from op_pipeline import PipelineSpec, default_registry, run_pipeline  # noqa: E402
from op_pipeline.executor import Executor, PooledProcessExecutor  # noqa: E402
from op_pipeline.operator import Operator  # noqa: E402
from op_pipeline.params import Param  # noqa: E402
from op_pipeline.registry import op  # noqa: E402


# 引擎级测试算子：testkit.double 与 test_op_pipeline.py 同名注册（replace=True
# 保证单独运行本文件与全量 discover 两种方式都可注册成功）。
@op("testkit.double", replace=True)
class _DoubleOp(Operator):
    inputs = [("value", int)]
    outputs = [("value", int)]
    params = {"times": Param(int, default=2)}

    def run(self, ctx, value, times):
        return {"value": value * times}


# --------------------------------------------------------------------------- #
# 模块级辅助函数（子进程按引用 pickle，必须可从模块顶层导入）
# --------------------------------------------------------------------------- #
def _p_add(a, b):
    return a + b


def _p_spin(seconds: float) -> str:
    deadline = time.time() + seconds
    while time.time() < deadline:  # 忙等：硬超时的靶子
        pass
    return "done"


def _p_boom():
    raise ValueError("pooled-boom")


def _p_die():
    os._exit(1)  # 模拟 worker 硬崩溃：无任何清理、无回传


def _p_sleep_return(seconds: float, value):
    time.sleep(seconds)
    return value


def _wait_worker_count(pool: PooledProcessExecutor, expected: int, timeout: float = 15.0) -> bool:
    """轮询等待池内 worker 数恢复到期望值（terminate/崩溃补位是异步 spawn）。"""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if pool._worker_count() == expected:
            return True
        time.sleep(0.05)
    return False


# --------------------------------------------------------------------------- #
# 契约测试
# --------------------------------------------------------------------------- #
class PooledExecutorContractTest(unittest.TestCase):
    """协议契约、结果回传、first_completed、异常透传、不可 pickle 回退。"""

    def test_satisfies_protocol_and_roundtrip(self):
        with PooledProcessExecutor(max_workers=2) as ex:
            self.assertIsInstance(ex, Executor, "PooledProcessExecutor 应满足 Executor 协议")
            slow = ex.submit(_p_spin, 0.3)
            fast = ex.submit(_p_add, 1, 2)
            done = ex.wait({slow, fast}, first_completed=True)
            self.assertIn(fast, done, "first_completed 应返回先完成的快任务")
            self.assertEqual(fast.result(), 3)
            self.assertEqual(ex.wait({slow}, first_completed=False), {slow})
            self.assertEqual(slow.result(), "done")

    def test_child_exception_roundtrips(self):
        with PooledProcessExecutor(max_workers=2) as ex:
            task = ex.submit(_p_boom)
            ex.wait({task}, first_completed=False)
            exc = task.exception()
            self.assertIsNotNone(exc)
            self.assertIn("pooled-boom", str(exc))

    def test_unpicklable_falls_back_with_warning(self):
        notes: list[str] = []
        with PooledProcessExecutor(max_workers=2, on_fallback=notes.append) as ex:
            task = ex.submit(lambda: "via-thread")
            ex.wait({task}, first_completed=False)
            self.assertEqual(task.result(), "via-thread")
        self.assertTrue(notes, "不可 pickle 应触发 on_fallback 告警")
        self.assertIn("pickle", notes[0])

    def test_shutdown_rejects_new_submits(self):
        ex = PooledProcessExecutor(max_workers=1)
        ex.shutdown()
        with self.assertRaises(RuntimeError):
            ex.submit(_p_add, 1, 2)


class PooledExecutorHardTimeoutTest(unittest.TestCase):
    """硬超时：terminate 在跑 worker 并自动补位；排队任务到点直接失败结算。"""

    def test_hard_timeout_terminates_worker_and_replaces(self):
        with PooledProcessExecutor(max_workers=2) as ex:
            started = time.monotonic()
            task = ex.submit(_p_spin, 30.0, timeout=0.5)
            done = ex.wait({task}, first_completed=False)
            elapsed = time.monotonic() - started
            self.assertIn(task, done)
            self.assertLess(elapsed, 5.0, "到点必须 terminate worker，而不是等它跑满 30s")
            exc = task.exception()
            self.assertIsNotNone(exc)
            self.assertIn("硬超时", str(exc))
            self.assertTrue(
                _wait_worker_count(ex, 2), "worker 被 terminate 后应自动补位回 2 个"
            )
            # 补位 worker 必须可用：再提交一个正常任务应成功
            ok = ex.submit(_p_add, 40, 2)
            ex.wait({ok}, first_completed=False)
            self.assertEqual(ok.result(), 42)

    def test_queued_task_times_out_without_worker(self):
        # 两个 worker 都被长任务占住；第三个任务排队，排队超时直接失败（不杀 worker）
        with PooledProcessExecutor(max_workers=2) as ex:
            a = ex.submit(_p_spin, 2.0)
            b = ex.submit(_p_spin, 2.0)
            queued = ex.submit(_p_add, 1, 2, timeout=0.5)
            done = ex.wait({a, b, queued}, first_completed=False)
            self.assertEqual(done, {a, b, queued})
            self.assertEqual(a.result(), "done")
            self.assertEqual(b.result(), "done")
            exc = queued.exception()
            self.assertIsNotNone(exc, "排队任务到点应以失败结算")
            self.assertIn("硬超时", str(exc))
            self.assertIn("排队", str(exc))


class PooledExecutorCrashRecoveryTest(unittest.TestCase):
    """worker 硬崩溃（os._exit）：在跑任务失败结算 + 自动补位，池继续可用。"""

    def test_worker_crash_fails_task_and_replaces(self):
        with PooledProcessExecutor(max_workers=2) as ex:
            task = ex.submit(_p_die)
            done = ex.wait({task}, first_completed=False)
            self.assertIn(task, done)
            exc = task.exception()
            self.assertIsNotNone(exc)
            self.assertIn("worker", str(exc))
            self.assertTrue(
                _wait_worker_count(ex, 2), "崩溃 worker 应被自动补位回 2 个"
            )
            # 补位后池继续可用
            ok = ex.submit(_p_add, 1, 1)
            ex.wait({ok}, first_completed=False)
            self.assertEqual(ok.result(), 2)


class PooledExecutorConcurrencyTest(unittest.TestCase):
    """并发上限：max_workers=2 时 4 个 0.3s 任务至少串行约 2 轮（≈0.6s）。"""

    def test_max_workers_bounds_parallelism(self):
        with PooledProcessExecutor(max_workers=2) as ex:
            started = time.monotonic()
            futures = [ex.submit(_p_sleep_return, 0.3, i) for i in range(4)]
            ex.wait(futures, first_completed=False)
            elapsed = time.monotonic() - started
            self.assertEqual([f.result() for f in futures], list(range(4)))
            # 无上限时会并行跑满 6 worker（≈0.3s 完成）；2 worker 至少 2 轮 > 0.5s
            self.assertGreaterEqual(elapsed, 0.5, "并发不应超过 max_workers")
            # 上界仅兜底防挂死：Windows spawn + 杀软扫描下进程创建耗时可放大
            # 数倍（实测 4s~22s），断言耗时上限不可靠，只验证结果与下界。
            self.assertLess(elapsed, 60.0, "兜底防挂死")


class PooledExecutorEngineTest(unittest.TestCase):
    """引擎级接线：运行级/节点级 executor="process_pool" 全链路。"""

    def _run(self, nodes, outputs, **kw):
        spec = PipelineSpec.from_dict(
            {"name": "pooled-e2e", "params": {}, "nodes": nodes, "outputs": outputs}
        )
        with tempfile.TemporaryDirectory() as tmp:
            return run_pipeline(
                spec, registry=default_registry, workdir=tmp, cache_dir=None, **kw
            )

    def test_run_level_process_pool(self):
        report, values = self._run(
            [
                {"id": "src", "op": "control.const", "params": {"value": 21}},
                {"id": "dbl", "op": "testkit.double", "inputs": {"value": "${src.value}"}},
            ],
            {"v": "dbl.value"},
            executor="process_pool",
        )
        self.assertTrue(report.ok, report.summary())
        self.assertEqual(values["dbl.value"], 42)

    def test_node_level_process_pool_decl(self):
        # 节点级声明触发建池：声明节点子进程执行，其余节点线程执行，结果一致
        report, values = self._run(
            [
                {"id": "src", "op": "control.const", "params": {"value": 1}},
                {
                    "id": "dbl",
                    "op": "testkit.double",
                    "inputs": {"value": "${src.value}"},
                    "executor": "process_pool",
                },
            ],
            {"v": "dbl.value"},
        )
        self.assertTrue(report.ok, report.summary())
        self.assertEqual(values["dbl.value"], 2)

    def test_node_level_process_with_run_level_pool(self):
        # 运行级 process_pool + 节点级 process：同一池实例服务两种 kind
        report, values = self._run(
            [
                {"id": "src", "op": "control.const", "params": {"value": 3}},
                {
                    "id": "plain",
                    "op": "testkit.double",
                    "inputs": {"value": "${src.value}"},
                    "executor": "process",
                },
                {
                    "id": "pooled",
                    "op": "testkit.double",
                    "inputs": {"value": "${plain.value}"},
                },
            ],
            {"v": "pooled.value"},
            executor="process_pool",
        )
        self.assertTrue(report.ok, report.summary())
        self.assertEqual(values["pooled.value"], 12)


if __name__ == "__main__":
    unittest.main()
