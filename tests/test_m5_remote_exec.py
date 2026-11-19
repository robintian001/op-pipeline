"""M5-1 远程执行落地：executor 参数支持 Executor 实例与 "queue" 字符串。

覆盖三个任务：
- Task 1：`Engine.__init__` / `run_pipeline` / `run()` 的 executor 参数扩为
  ``str | Executor | None``；字符串 "thread"/"process"/"queue" 映射到实例，
  非法字符串与非 Executor 对象报 ``ConfigError``；实例路径下引擎不额外创建
  进程池，节点级 executor 声明被忽略时记 warning；节点级 ``executor="queue"``
  声明同样触发引擎自建队列执行器。
- Task 2：`remote._remote_run(payload)`——引擎在主进程解析实参（RefResolver
  快照语义），payload 携带解析后的输入值与共享句柄目录；worker 侧经
  ``default_registry`` 实例化算子执行、把输出写回共享目录句柄存储；
  不可 pickle 的载荷/输出报 RuntimeError，绝不静默回退。
- Task 3：引擎主循环接入队列执行器（远程节点经内部线程任务包装：重试 /
  缓存 / 软超时都在主进程侧），双进程 e2e 跑 `examples/pipeline_orders.json`
  （18 节点）与 ThreadExecutor 结果一致；无 worker 时节点按超时失败、语义清晰。
"""
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

EXAMPLES = ROOT / "examples"

# 运行报告文本里的时变内容（生成时间 / run_id）：e2e 比对前归一
_TS_PATTERN = re.compile(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}|\d{8}-\d{6}-[0-9a-f]+")


def _normalize(obj):
    """递归把字符串里的时间戳 / run_id 替换为占位符（e2e 结果比对用）。"""
    if isinstance(obj, str):
        return _TS_PATTERN.sub("<ts>", obj)
    if isinstance(obj, dict):
        return {k: _normalize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_normalize(v) for v in obj]
    return obj

# --------------------------------------------------------------------------- #
# 测试算子（模块级注册：线程内 worker 与本进程可见；双进程 e2e 只用内置算子）
# --------------------------------------------------------------------------- #
from op_pipeline.operator import Operator
from op_pipeline.params import Param
from op_pipeline.registry import op as register_op


@register_op("m5.double", replace=True)
class DoubleOp(Operator):
    """把输入值乘以 times（进程内 worker 测试用）。"""

    description = "翻倍"
    inputs = [("value", int)]
    outputs = [("value", int)]
    params = {"times": Param(int, default=2)}

    def run(self, ctx, value, times):
        return {"value": int(value) * times}


@register_op("m5.lambda_out", replace=True)
class LambdaOutOp(Operator):
    """输出不可 pickle 的值（验证共享句柄存储绝不静默回退）。"""

    description = "输出 lambda"
    outputs = [("value", object)]

    def run(self, ctx):
        return {"value": lambda x: x}


@register_op("m5.slow", replace=True)
class SlowOp(Operator):
    """慢算子：sleep 指定秒数后返回（验证远程节点软超时）。"""

    description = "慢算子"
    outputs = [("value", int)]
    params = {"seconds": Param(float, default=0.5)}

    def run(self, ctx, seconds):
        time.sleep(float(seconds))
        return {"value": 1}


def spec_of(nodes, params=None, outputs=None, name="t"):
    from op_pipeline import PipelineSpec

    return PipelineSpec.from_dict(
        {
            "name": name,
            "params": params or {},
            "nodes": nodes,
            "outputs": outputs or {},
        }
    )


def _chain_spec(timeout=None, retries=0):
    """m5.double 三节点链：21 -> 42 -> 84（进程内 worker 测试用）。

    ``timeout``/``retries`` 应用到每个节点（软超时与重试语义测试用）。
    """
    nodes = [
        {"id": "a", "op": "control.const", "params": {"value": 21}},
        {"id": "b", "op": "m5.double", "inputs": {"value": "${a.value}"}},
        {"id": "c", "op": "m5.double", "inputs": {"value": "${b.value}"}},
    ]
    for node in nodes:
        if timeout is not None:
            node["timeout"] = timeout
        if retries:
            node["retries"] = retries
    return spec_of(nodes, outputs={"final": "${c.value}"})


def _builtin_chain_spec():
    """纯内置算子三节点链（Popen worker 进程也能解析）：hello -> 写文件 -> 读长度。

    c 经 ``${b.path}`` 引用显式声明对 b 的依赖（远程路径节点分发是并发的，
    只靠"文件系统隐式依赖"会产生写读竞态）。
    """
    return spec_of(
        [
            {"id": "a", "op": "control.const", "params": {"value": "hello"}},
            {
                "id": "b",
                "op": "io.write_text",
                "inputs": {"text": "${a.value}"},
                "params": {"path": "out.txt"},
            },
            {"id": "c", "op": "io.read_text", "params": {"path": "${b.path}"}},
        ],
        outputs={"final": "${c.length}"},
    )


def _spawn_worker(queue, out, count, poll="0.05"):
    """启动一个独立 worker 进程消费队列（复用 QueueWorkerTest 的双进程模式）。

    stdout/stderr 用 DEVNULL 而非 PIPE：worker 消费整条流水线（十几个任务）
    的日志输出可能超过管道缓冲区（约 8KB），而 ``run_pipeline`` 运行期间没有
    人读管道——缓冲写满后 worker 会阻塞在写日志上、引擎死等结果，形成双端
    死锁（PIPE 只适合任务数极少的消费场景）。worker 退出码仍可经
    ``proc.returncode`` 断言。
    """
    return subprocess.Popen(
        [
            sys.executable,
            "-m",
            "op_pipeline",
            "worker",
            "--queue",
            str(queue),
            "--out",
            str(out),
            "--count",
            str(count),
            "--poll",
            poll,
        ],
        cwd=str(ROOT),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def _build_engine(spec, tmp, **engine_kwargs):
    """直接构造 Engine（白盒断言构造期行为用）。"""
    from op_pipeline.context import RunContext
    from op_pipeline.engine import Engine
    from op_pipeline.registry import default_registry
    from op_pipeline.spec import build_plan

    plan = build_plan(spec, default_registry)
    context = RunContext.create(pipeline="t", params={}, workdir=tmp)
    return Engine(plan, context, **engine_kwargs)


# --------------------------------------------------------------------------- #
# Task 1：executor 参数扩展（实例 / 字符串 / 校验 / 节点级声明）
# --------------------------------------------------------------------------- #
class M5ExecutorParamTest(unittest.TestCase):
    def test_thread_executor_instance_matches_default(self):
        """传入 ThreadExecutor 实例，结果与默认（None）完全一致。"""
        from op_pipeline import ThreadExecutor, default_registry, run_pipeline

        with tempfile.TemporaryDirectory() as tmp:
            report, values = run_pipeline(
                _chain_spec(), registry=default_registry, workdir=tmp, cache_dir=None
            )
        with tempfile.TemporaryDirectory() as tmp:
            exec_ = ThreadExecutor(max_workers=4)
            try:
                report2, values2 = run_pipeline(
                    _chain_spec(),
                    registry=default_registry,
                    workdir=tmp,
                    cache_dir=None,
                    executor=exec_,
                )
            finally:
                exec_.shutdown()
        self.assertEqual(report.status, "success")
        self.assertEqual(report.status, report2.status)
        self.assertEqual(report.outputs, report2.outputs)
        self.assertEqual(values, values2)
        self.assertEqual({n.status for n in report2.nodes}, {"success"})

    def test_thread_string_compat(self):
        """字符串 "thread" 路径行为不变。"""
        from op_pipeline import default_registry, run_pipeline

        with tempfile.TemporaryDirectory() as tmp:
            report, values = run_pipeline(
                _chain_spec(),
                registry=default_registry,
                workdir=tmp,
                cache_dir=None,
                executor="thread",
            )
        self.assertEqual(report.status, "success")
        self.assertEqual(values["c.value"], 84)

    def test_process_string_compat(self):
        """字符串 "process" 路径行为不变（节点走内置进程执行器）。"""
        from op_pipeline import default_registry, run_pipeline

        with tempfile.TemporaryDirectory() as tmp:
            report, values = run_pipeline(
                _chain_spec(),
                registry=default_registry,
                workdir=tmp,
                cache_dir=None,
                executor="process",
            )
        self.assertEqual(report.status, "success")
        self.assertEqual(values["c.value"], 84)

    def test_invalid_executor_string_raises(self):
        """非法字符串仍报 ConfigError。"""
        from op_pipeline import ConfigError, default_registry, run_pipeline

        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(ConfigError):
                run_pipeline(
                    _chain_spec(),
                    registry=default_registry,
                    workdir=tmp,
                    cache_dir=None,
                    executor="bogus",
                )

    def test_non_executor_object_raises(self):
        """传非 Executor 对象（lambda / 整数）报 ConfigError。"""
        from op_pipeline import ConfigError, default_registry, run_pipeline

        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(ConfigError):
                run_pipeline(
                    _chain_spec(),
                    registry=default_registry,
                    workdir=tmp,
                    cache_dir=None,
                    executor=lambda: 1,
                )
            with self.assertRaises(ConfigError):
                run_pipeline(
                    _chain_spec(),
                    registry=default_registry,
                    workdir=tmp,
                    cache_dir=None,
                    executor=42,
                )

    def test_instance_mode_no_process_pool(self):
        """实例模式：引擎直接使用实例、不额外创建进程池。"""
        from op_pipeline import QueueWorkerExecutor, ThreadExecutor

        with tempfile.TemporaryDirectory() as tmp:
            engine = _build_engine(_chain_spec(), tmp, executor=ThreadExecutor())
            self.assertIs(engine._executor_instance.__class__, ThreadExecutor)
            self.assertFalse(engine._owns_executor)
            self.assertIsNone(engine._process_pool)
            self.assertFalse(engine._remote)

            engine = _build_engine(
                _chain_spec(), tmp,
                executor=QueueWorkerExecutor(Path(tmp) / "q.jsonl", Path(tmp) / "r"),
            )
            self.assertFalse(engine._owns_executor)
            self.assertIsNone(engine._process_pool)
            self.assertTrue(engine._remote)

    def test_node_level_process_ignored_with_warning(self):
        """实例模式下节点级 executor="process" 声明被忽略并记 warning。"""
        from op_pipeline import ThreadExecutor

        spec = spec_of(
            [
                {"id": "a", "op": "control.const", "params": {"value": 1}},
                {"id": "b", "op": "m5.double", "inputs": {"value": "${a.value}"},
                 "executor": "process"},
            ]
        )
        with tempfile.TemporaryDirectory() as tmp:
            engine = _build_engine(spec, tmp, executor=ThreadExecutor())
        self.assertIsNone(engine._process_pool)
        self.assertTrue(
            any("b" in w and "process" in w for w in engine._warnings),
            f"应有节点级 process 被忽略的告警，实际：{engine._warnings}",
        )

    def test_node_level_queue_triggers_queue_executor(self):
        """运行级 None + 节点级 executor="queue"：引擎自建队列执行器（路径挂 workdir）。"""
        from op_pipeline import QueueWorkerExecutor

        spec = spec_of(
            [
                {"id": "a", "op": "control.const", "params": {"value": 1}},
                {"id": "b", "op": "m5.double", "inputs": {"value": "${a.value}"},
                 "executor": "queue"},
            ]
        )
        with tempfile.TemporaryDirectory() as tmp:
            engine = _build_engine(spec, tmp)
            try:
                self.assertTrue(engine._remote)
                self.assertTrue(engine._owns_executor)
                self.assertIsInstance(engine._executor_instance, QueueWorkerExecutor)
                self.assertIsNone(engine._process_pool)
                # 队列/结果目录推导自 workdir/.op_pipeline/remote/（构造时已建结果目录）
                self.assertTrue(
                    (Path(tmp) / ".op_pipeline" / "remote" / "results").is_dir()
                )
            finally:
                engine._executor_instance.shutdown()

    def test_queue_string_maps_and_creates_executor(self):
        """字符串 "queue"：引擎自建队列执行器（路径与 workdir 相关）。"""
        from op_pipeline import QueueWorkerExecutor

        with tempfile.TemporaryDirectory() as tmp:
            engine = _build_engine(_chain_spec(), tmp, executor="queue")
            try:
                self.assertIsInstance(engine._executor_instance, QueueWorkerExecutor)
                self.assertTrue(engine._owns_executor)
                self.assertTrue(engine._remote)
            finally:
                engine._executor_instance.shutdown()


# --------------------------------------------------------------------------- #
# Task 2：_remote_run 与共享目录句柄存储
# --------------------------------------------------------------------------- #
class M5RemoteRunTest(unittest.TestCase):
    @staticmethod
    def _payload(tmp, *, op="m5.double", params=None, inputs=None, outputs=None):
        return {
            "node_id": "b",
            "op": op,
            "params": dict(params or {}),
            "inputs": dict(inputs or {}),
            "outputs": dict(outputs or {}),
            "shared_dir": str(Path(tmp) / "handles"),
            "run": {
                "run_id": "r1",
                "pipeline": "t",
                "params": {},
                "workdir": str(tmp),
                "artifacts_dir": str(Path(tmp) / "artifacts"),
            },
        }

    def test_remote_run_payload_roundtrip(self):
        """payload（主进程已解析的输入值）-> worker 执行 -> 输出句柄写回共享目录。"""
        from op_pipeline.handlestore import SharedHandleStore
        from op_pipeline.remote import _remote_run

        with tempfile.TemporaryDirectory() as tmp:
            shared = Path(tmp) / "handles"
            store_a = SharedHandleStore(shared_dir=shared)
            store_a.put("a.value", 21)
            payload = self._payload(
                tmp,
                params={"times": 2},
                inputs={"value": 21},  # 主进程 RefResolver 解析后的实际值
                outputs={"value": "b.value"},
            )
            result = _remote_run(payload)
            self.assertEqual(result["status"], "success")
            self.assertEqual(result["outputs"], {"value": "b.value"})
            self.assertEqual(result["error"], None)
            # 另一个存储实例（模拟引擎侧）从同一共享目录读到下游句柄
            store_b = SharedHandleStore(shared_dir=shared)
            self.assertEqual(store_b.get("b.value"), 42)

    def test_remote_run_builtin_const(self):
        """真实内置算子（control.const）经 payload 执行，输出可从共享目录取回。"""
        from op_pipeline.handlestore import SharedHandleStore
        from op_pipeline.remote import _remote_run

        with tempfile.TemporaryDirectory() as tmp:
            payload = self._payload(
                tmp, op="control.const", params={"value": 7}, outputs={"value": "a.value"}
            )
            result = _remote_run(payload)
            self.assertEqual(result["status"], "success")
            store = SharedHandleStore(shared_dir=Path(tmp) / "handles")
            self.assertEqual(store.get("a.value"), 7)

    def test_remote_run_error_returns_traceback(self):
        """算子不存在 / 执行出错时返回失败 dict，error 与 traceback 均保留。"""
        from op_pipeline.remote import _remote_run

        with tempfile.TemporaryDirectory() as tmp:
            payload = self._payload(tmp, op="no.such_op")
            result = _remote_run(payload)
            self.assertEqual(result["status"], "failed")
            self.assertIn("no.such_op", result["error"])
            self.assertIn("Traceback", result["traceback"])
            self.assertEqual(result["outputs"], {})

    def test_remote_run_unpicklable_output_fails(self):
        """算子输出不可 pickle 时写共享目录抛 RuntimeError，绝不静默回退。"""
        from op_pipeline.handlestore import SharedHandleStore
        from op_pipeline.remote import _remote_run

        with tempfile.TemporaryDirectory() as tmp:
            payload = self._payload(
                tmp, op="m5.lambda_out", outputs={"value": "x.value"}
            )
            result = _remote_run(payload)
            self.assertEqual(result["status"], "failed")
            self.assertIn("不可 pickle", result["error"])
            self.assertNotIn("x.value", SharedHandleStore(shared_dir=Path(tmp) / "handles").snapshot())

    def test_submit_unpicklable_payload_raises(self):
        """submit 时载荷不可 pickle 直接抛 RuntimeError（含 pickle 提示）。"""
        from op_pipeline import QueueWorkerExecutor

        with tempfile.TemporaryDirectory() as tmp:
            exec_ = QueueWorkerExecutor(Path(tmp) / "jobs.jsonl", Path(tmp) / "results")
            with self.assertRaises(RuntimeError) as cm:
                exec_.submit(lambda: 1)
            self.assertIn("pickle", str(cm.exception))


# --------------------------------------------------------------------------- #
# Task 3：引擎主循环接入队列执行器 + 双进程 e2e
# --------------------------------------------------------------------------- #
class M5EngineRemoteTest(unittest.TestCase):
    def test_engine_remote_inprocess(self):
        """Executor 实例路径：引擎直接使用实例，节点全量经队列 worker 执行。"""
        from op_pipeline import QueueWorkerExecutor, default_registry, run_pipeline
        from op_pipeline.remote import consume_jobs

        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            queue = tmp / "jobs.jsonl"
            out = tmp / "results"
            exec_ = QueueWorkerExecutor(queue, out, poll=0.01)
            worker = threading.Thread(
                target=consume_jobs,
                args=(queue, out),
                kwargs={"count": 3, "poll": 0.01},
                daemon=True,
            )
            worker.start()
            report, values = run_pipeline(
                _chain_spec(),
                registry=default_registry,
                workdir=tmp,
                cache_dir=None,
                executor=exec_,
            )
            worker.join(timeout=30)
            exec_.shutdown()
            self.assertFalse(worker.is_alive())
        self.assertEqual(report.status, "success")
        self.assertEqual(report.outputs["final"], 84)
        self.assertEqual(values["a.value"], 21)
        self.assertEqual(values["c.value"], 84)
        self.assertEqual({n.status for n in report.nodes}, {"success"})
        self.assertTrue(all(n.attempts == 1 for n in report.nodes))
        # 与线程路径结果一致
        with tempfile.TemporaryDirectory() as tmp:
            report_t, values_t = run_pipeline(
                _chain_spec(), registry=default_registry, workdir=tmp, cache_dir=None
            )
        self.assertEqual(report_t.status, "success")
        self.assertEqual(values_t["c.value"], values["c.value"])

    def test_queue_string_e2e_with_worker(self):
        """字符串 "queue"：引擎自建队列执行器 + 独立 worker 进程完整跑通。"""
        from op_pipeline import default_registry, run_pipeline

        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            queue = tmp / ".op_pipeline" / "remote" / "jobs.jsonl"
            out = tmp / ".op_pipeline" / "remote" / "results"
            proc = _spawn_worker(queue, out, count=3)
            try:
                report, values = run_pipeline(
                    _builtin_chain_spec(),
                    registry=default_registry,
                    workdir=tmp,
                    cache_dir=None,
                    executor="queue",
                )
            finally:
                proc.communicate(timeout=120)
            self.assertEqual(proc.returncode, 0)
            self.assertEqual(report.status, "success")
            self.assertEqual(report.outputs["final"], 5)
            self.assertEqual(values["c.length"], 5)

    def test_no_worker_fails_nodes_by_timeout(self):
        """无 worker 消费：节点按软超时失败（语义清晰），报告为 partial。"""
        from op_pipeline import QueueWorkerExecutor, default_registry, run_pipeline

        with tempfile.TemporaryDirectory() as tmp:
            exec_ = QueueWorkerExecutor(
                Path(tmp) / "jobs.jsonl", Path(tmp) / "results", poll=0.01
            )
            report, _ = run_pipeline(
                _chain_spec(timeout=0.5),
                registry=default_registry,
                workdir=tmp,
                cache_dir=None,
                executor=exec_,
            )
            exec_.shutdown()
        # 状态归并：存在 hard failure 即整体 failed（skipped 不救，见 run() 汇总）
        self.assertEqual(report.status, "failed")
        # 链式失败传播：a 超时失败，b/c 因上游失败被跳过
        self.assertEqual(report.node("a").status, "failed")
        self.assertEqual({n.status for n in report.nodes}, {"failed", "skipped"})
        self.assertIn("超时", report.node("a").error or "")

    def test_no_worker_queue_task_timeout(self):
        """没有 worker 消费时，_QueueTask.result(timeout=...) 短超时抛 TimeoutError。"""
        import operator

        from op_pipeline import QueueWorkerExecutor
        from op_pipeline.remote import _QueueTask

        with tempfile.TemporaryDirectory() as tmp:
            exec_ = QueueWorkerExecutor(
                Path(tmp) / "jobs.jsonl", Path(tmp) / "results", poll=0.01
            )
            task = exec_.submit(operator.add, 1, 2)
            self.assertIsInstance(task, _QueueTask)
            with self.assertRaises(TimeoutError):
                task.result(timeout=0.2)

    def test_remote_node_retry_attempts(self):
        """远程节点沿用引擎主进程侧重试包装：retries=1 失败后 attempts=2。"""
        from op_pipeline import QueueWorkerExecutor, default_registry, run_pipeline

        spec = spec_of(
            [{"id": "a", "op": "control.const", "params": {"value": 1},
              "retries": 1, "timeout": 0.3}],
            outputs={"final": "${a.value}"},
        )
        with tempfile.TemporaryDirectory() as tmp:
            exec_ = QueueWorkerExecutor(
                Path(tmp) / "jobs.jsonl", Path(tmp) / "results", poll=0.01
            )
            report, _ = run_pipeline(
                spec, registry=default_registry, workdir=tmp, cache_dir=None,
                executor=exec_,
            )
            exec_.shutdown()
        self.assertEqual(report.status, "failed")
        self.assertEqual(report.node("a").attempts, 2)

    def test_remote_node_soft_timeout(self):
        """远程节点软超时在主进程侧生效：timeout 到点标记失败（worker 侧算子可能仍在跑）。"""
        from op_pipeline import QueueWorkerExecutor, default_registry, run_pipeline
        from op_pipeline.remote import consume_jobs

        spec = spec_of(
            [{"id": "a", "op": "m5.slow", "params": {"seconds": 0.6}, "timeout": 0.2}],
            outputs={"final": "${a.value}"},
        )
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            exec_ = QueueWorkerExecutor(
                tmp / "jobs.jsonl", tmp / "results", poll=0.01
            )
            worker = threading.Thread(
                target=consume_jobs, args=(tmp / "jobs.jsonl", tmp / "results"),
                kwargs={"count": 1, "poll": 0.01}, daemon=True,
            )
            worker.start()
            report, _ = run_pipeline(
                spec, registry=default_registry, workdir=tmp, cache_dir=None,
                executor=exec_,
            )
            worker.join(timeout=30)
            exec_.shutdown()
        self.assertEqual(report.status, "failed")
        node = report.node("a")
        self.assertEqual(node.status, "failed")
        self.assertIn("软超时", node.error)

    def test_remote_node_cache_hit(self):
        """远程节点缓存：cache_key 在主进程计算，第二遍命中缓存且不再发队列任务。"""
        from op_pipeline import default_registry, run_pipeline
        from op_pipeline.remote import consume_jobs

        spec = spec_of(
            [{"id": "a", "op": "control.const", "params": {"value": 33}, "cache": True}],
            outputs={"final": "${a.value}"},
        )
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            cache_dir = tmp / "cache"
            queue = tmp / "jobs.jsonl"
            out = tmp / "results"
            from op_pipeline import QueueWorkerExecutor

            # 第一遍：线程内 worker 消费 1 个任务，产出写缓存
            exec_ = QueueWorkerExecutor(queue, out, poll=0.01)
            worker = threading.Thread(
                target=consume_jobs, args=(queue, out),
                kwargs={"count": 1, "poll": 0.01}, daemon=True,
            )
            worker.start()
            report1, values1 = run_pipeline(
                spec, registry=default_registry, workdir=tmp / "run1",
                cache_dir=cache_dir, executor=exec_,
            )
            worker.join(timeout=30)
            exec_.shutdown()
            # 第二遍：不开 worker——缓存命中则不发任何队列任务，不会挂起
            report2, values2 = run_pipeline(
                spec, registry=default_registry, workdir=tmp / "run2",
                cache_dir=cache_dir, executor=QueueWorkerExecutor(
                    queue, out, poll=0.01
                ),
            )
        self.assertEqual(report1.status, "success")
        self.assertFalse(report1.node("a").cached)
        self.assertEqual(report2.status, "success")
        self.assertTrue(report2.node("a").cached)
        self.assertEqual(values1["a.value"], values2["a.value"])
        self.assertEqual(values2["a.value"], 33)

    def test_e2e_pipeline_orders_queue_vs_thread(self):
        """双进程 e2e：pipeline_orders.json（18 节点）经队列全 success，与线程结果一致。"""
        from op_pipeline import (
            PipelineSpec,
            QueueWorkerExecutor,
            default_registry,
            run_pipeline,
        )

        spec = PipelineSpec.from_file(EXAMPLES / "pipeline_orders.json")
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            data_dir = tmp / "data"
            data_dir.mkdir(parents=True)
            shutil.copy(EXAMPLES / "data" / "orders.csv", data_dir / "orders.csv")
            shutil.copy(EXAMPLES / "data" / "regions.csv", data_dir / "regions.csv")
            # 线程基线（同一 workdir，路径类句柄一致可整体比对）
            report_t, values_t = run_pipeline(
                spec, registry=default_registry, workdir=tmp, cache_dir=None
            )
            # 队列执行器 + 独立 worker 进程
            queue = tmp / ".op_pipeline" / "remote" / "jobs.jsonl"
            out = tmp / ".op_pipeline" / "remote" / "results"
            exec_ = QueueWorkerExecutor(queue, out, poll=0.05)
            proc = _spawn_worker(queue, out, count=18)
            try:
                report_q, values_q = run_pipeline(
                    spec,
                    registry=default_registry,
                    workdir=tmp,
                    cache_dir=None,
                    executor=exec_,
                )
            finally:
                proc.communicate(timeout=180)
                exec_.shutdown()
            self.assertEqual(proc.returncode, 0)
            self.assertEqual(report_t.status, "success")
            self.assertEqual(report_q.status, "success")
            self.assertEqual(len(report_q.nodes), 18)
            self.assertEqual(
                {n.node_id: n.status for n in report_q.nodes},
                {n.node_id: n.status for n in report_t.nodes},
            )
            # 报告文本含生成时间 / run_id（时变），比对前归一
            self.assertEqual(
                _normalize(report_q.outputs), _normalize(report_t.outputs)
            )
            self.assertEqual(_normalize(values_q), _normalize(values_t))


if __name__ == "__main__":
    unittest.main()
