"""框架自测（标准库 unittest，零第三方依赖）。

运行：``python -m unittest discover -s tests -v``
"""
from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
import tempfile
import threading
import time
import tomllib
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from op_pipeline import (  # noqa: E402
    Operator,
    Param,
    PipelineSpec,
    SpecValidationError,
    default_registry,
    evaluate,
    load_plugins,
    op,
    register_function,
    run_pipeline,
)
from op_pipeline import expression  # noqa: E402
from op_pipeline.expression import ExpressionError  # noqa: E402
from op_pipeline.spec import build_plan, PlanNode  # noqa: E402
from op_pipeline.errors import ConfigError  # noqa: E402
from op_pipeline.cache import CacheStore  # noqa: E402
from op_pipeline.context import RunContext  # noqa: E402
from op_pipeline.engine import Engine  # noqa: E402
from op_pipeline.registry import Registry  # noqa: E402
from op_pipeline import run  # noqa: E402


# --------------------------------------------------------------------------- #
# 测试用算子
# --------------------------------------------------------------------------- #
@op("testkit.sleep", replace=True)
class SleepOp(Operator):
    inputs = [("value", object)]
    outputs = [("value", object)]
    params = {"seconds": Param(float, default=0.2, minimum=0.0)}

    def run(self, ctx, value, seconds):
        time.sleep(seconds)
        return {"value": value}


@op("testkit.flaky", replace=True)
class FlakyOp(Operator):
    inputs = [("value", object)]
    outputs = [("value", object), ("attempts", int)]
    params = {"fail_times": Param(int, default=1), "state": Param(dict, default=None)}

    def run(self, ctx, value, fail_times, state):
        state = state if state is not None else _STATE
        key = f"{ctx.run_id}:{ctx.node_id}"
        state[key] = state.get(key, 0) + 1
        if state[key] <= fail_times:
            raise RuntimeError("预期失败")
        return {"value": value, "attempts": state[key]}


_STATE: dict[str, int] = {}


@op("testkit.boom", replace=True)
class BoomOp(Operator):
    inputs = [("value", object)]
    outputs = [("value", object)]

    def run(self, ctx, value):
        raise ValueError("故意失败")


@op("testkit.double", replace=True)
class DoubleOp(Operator):
    inputs = [("value", int)]
    outputs = [("value", int)]
    params = {"times": Param(int, default=2)}

    def run(self, ctx, value, times):
        return {"value": value * times}


def spec_of(nodes, params=None, outputs=None, name="t") -> PipelineSpec:
    return PipelineSpec.from_dict(
        {"name": name, "params": params or {}, "nodes": nodes, "outputs": outputs or {}}
    )


class RegistryTest(unittest.TestCase):
    def test_builtin_ops_registered(self):
        self.assertGreaterEqual(len(default_registry), 30)
        for name in ("io.read_csv", "table.filter_rows", "stats.aggregate", "report.markdown"):
            self.assertIn(name, default_registry)

    def test_contract_describe(self):
        info = default_registry.get("table.filter_rows").describe()
        self.assertEqual(info["name"], "table.filter_rows")
        self.assertEqual([p["name"] for p in info["inputs"]], ["table"])
        self.assertIn("where", info["params"])
        self.assertTrue(info["params"]["where"]["required"])

    def test_param_validation(self):
        cls = default_registry.get("io.read_csv")
        with self.assertRaises(Exception):
            cls()  # 缺必填 path
        with self.assertRaises(Exception):
            cls(path="a.csv", nope=1)  # 未声明参数

    def test_param_coercion(self):
        cls = default_registry.get("table.limit")
        instance = cls(n="3", offset="1")
        self.assertEqual(instance.params["n"], 3)
        self.assertEqual(instance.params["offset"], 1)


class ExpressionTest(unittest.TestCase):
    def test_basic(self):
        self.assertEqual(evaluate("1 + 2 * 3", {}), 7)
        self.assertTrue(evaluate("a > 2 and b == 'x'", {"a": 3, "b": "x"}))
        self.assertTrue(evaluate("'a' in row", {"row": ["a", "b"]}))
        self.assertEqual(evaluate("len(items)", {"items": [1, 2, 3]}), 3)

    def test_blocked(self):
        for bad in ("__import__('os')", "x.__class__", "open('a')", "lambda: 1"):
            with self.assertRaises(ExpressionError):
                evaluate(bad, {"x": 1})


class SpecValidationTest(unittest.TestCase):
    def test_unknown_operator(self):
        with self.assertRaises(SpecValidationError) as ctx:
            build_plan(spec_of([{"id": "a", "op": "nope.nope"}]), default_registry)
        self.assertIn("未注册的算子", str(ctx.exception))

    def test_missing_input(self):
        with self.assertRaises(SpecValidationError) as ctx:
            build_plan(spec_of([{"id": "f", "op": "table.filter_rows", "params": {"where": "1 > 0"}}]), default_registry)
        self.assertIn("缺少必填输入端口", str(ctx.exception))

    def test_unknown_handle(self):
        with self.assertRaises(SpecValidationError) as ctx:
            build_plan(
                spec_of([{"id": "f", "op": "table.filter_rows", "inputs": {"table": "${nope.table}"},
                          "params": {"where": "1 > 0"}}]),
                default_registry,
            )
        self.assertIn("未知句柄", str(ctx.exception))

    def test_unknown_param_name_with_ref(self):
        node = {
            "id": "f",
            "op": "table.filter_rows",
            "inputs": {"table": "${src.table}"},
            "params": {"where": "1 > 0", "wrong": "${p.x}"},
        }
        src = {"id": "src", "op": "control.const", "params": {"value": []}}
        with self.assertRaises(SpecValidationError) as ctx:
            build_plan(spec_of([src, node], params={"x": 1}), default_registry)
        self.assertIn("未声明的参数", str(ctx.exception))

    def test_cycle_detected(self):
        nodes = [
            {"id": "a", "op": "control.passthrough", "inputs": {"value": "${b.value}"}},
            {"id": "b", "op": "control.passthrough", "inputs": {"value": "${a.value}"}},
        ]
        with self.assertRaises(SpecValidationError) as ctx:
            build_plan(spec_of(nodes), default_registry)
        self.assertIn("环", str(ctx.exception))


class ExecutionTest(unittest.TestCase):
    def test_end_to_end_with_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            workdir = Path(tmp)
            (workdir / "data.csv").write_text(
                "region,amount\nnorth,10\nnorth,20\nsouth,30\nsouth,40\n", encoding="utf-8"
            )
            spec = spec_of(
                [
                    {"id": "load", "op": "io.read_csv", "params": {"path": "data.csv"}, "cache": True},
                    {"id": "agg", "op": "stats.aggregate", "inputs": {"table": "${load.table}"},
                     "params": {"group_by": "region", "metrics": {"amount": "sum"}}},
                    {"id": "json", "op": "io.write_json", "inputs": {"data": "${agg.table}"},
                     "params": {"path": "out/agg.json"}},
                ],
                outputs={"agg": "${agg.table}"},
            )
            report, values = run_pipeline(spec, registry=default_registry, workdir=workdir, cache_dir=None)
            self.assertEqual(report.status, "success")
            self.assertTrue(report.ok)
            self.assertEqual(values["agg.table"], [{"region": "north", "amount_sum": 30},
                                                   {"region": "south", "amount_sum": 70}])
            written = json.loads((workdir / "out/agg.json").read_text(encoding="utf-8"))
            self.assertEqual(len(written), 2)

    def test_parallel_speed(self):
        nodes = [
            {"id": f"a{i}", "op": "testkit.sleep", "inputs": {"value": "${src.value}"},
             "params": {"seconds": 0.25}}
            for i in range(4)
        ]
        nodes.append({"id": "src", "op": "control.const", "params": {"value": 1}})
        with tempfile.TemporaryDirectory() as tmp:
            report, _ = run_pipeline(spec_of(nodes), registry=default_registry, workdir=tmp,
                                     cache_dir=None, max_workers=4)
        self.assertEqual(report.status, "success")
        # 串行需要 1.0s，4 并发应显著更快
        self.assertLess(report.duration, 0.8)

    def test_retries(self):
        nodes = [
            {"id": "src", "op": "control.const", "params": {"value": 1}},
            {"id": "flaky", "op": "testkit.flaky", "inputs": {"value": "${src.value}"},
             "params": {"fail_times": 2}, "retries": 2, "retry_delay": 0.01},
        ]
        with tempfile.TemporaryDirectory() as tmp:
            report, values = run_pipeline(spec_of(nodes), registry=default_registry, workdir=tmp, cache_dir=None)
        self.assertEqual(report.status, "success")
        self.assertEqual(report.node("flaky").attempts, 3)
        self.assertEqual(values["flaky.attempts"], 3)

    def test_failure_propagation(self):
        nodes = [
            {"id": "src", "op": "control.const", "params": {"value": 1}},
            {"id": "boom", "op": "testkit.boom", "inputs": {"value": "${src.value}"}},
            {"id": "after", "op": "testkit.double", "inputs": {"value": "${boom.value}"}},
            {"id": "solo", "op": "control.const", "params": {"value": "独立分支"}},
        ]
        with tempfile.TemporaryDirectory() as tmp:
            report, _ = run_pipeline(spec_of(nodes), registry=default_registry, workdir=tmp, cache_dir=None)
        self.assertEqual(report.status, "failed")
        self.assertEqual(report.node("boom").status, "failed")
        self.assertEqual(report.node("after").status, "skipped")
        # 无依赖关系的分支仍然执行
        self.assertEqual(report.node("solo").status, "success")

    def test_when_skips_branch(self):
        nodes = [
            {"id": "src", "op": "control.const", "params": {"value": [1, 2, 3]}},
            {"id": "limit", "op": "table.limit", "inputs": {"table": "${src.value}"},
             "params": {"n": "${p.n}"}, "when": "n > 0"},
            {"id": "after", "op": "stats.count_rows", "inputs": {"table": "${limit.table}"}},
        ]
        with tempfile.TemporaryDirectory() as tmp:
            report, _ = run_pipeline(spec_of(nodes, params={"n": 0}), registry=default_registry,
                                     workdir=tmp, cache_dir=None)
        self.assertEqual(report.node("limit").status, "skipped")
        self.assertEqual(report.node("after").status, "skipped")
        self.assertEqual(report.status, "partial")

    def test_cache_reuse(self):
        nodes = [{"id": "load", "op": "io.read_csv", "params": {"path": "d.csv"}, "cache": True}]
        with tempfile.TemporaryDirectory() as tmp:
            workdir = Path(tmp)
            (workdir / "d.csv").write_text("a,b\n1,2\n", encoding="utf-8")
            cache_dir = workdir / "cache"
            first, _ = run_pipeline(spec_of(nodes), registry=default_registry, workdir=workdir,
                                    cache_dir=cache_dir)
            second, _ = run_pipeline(spec_of(nodes), registry=default_registry, workdir=workdir,
                                     cache_dir=cache_dir)
        self.assertFalse(first.node("load").cached)
        self.assertTrue(second.node("load").cached)

    def test_handle_path_drilldown(self):
        nodes = [
            {"id": "stats", "op": "demo.text_stats", "inputs": {"lines": "${src.value}"}},
            {"id": "src", "op": "control.const", "params": {"value": ["a", "bb"]}},
            {"id": "out", "op": "io.write_json", "inputs": {"data": "${stats.stats}"},
             "params": {"path": "stats.json"}},
        ]
        load_plugins(paths=["examples/plugins/demo_ops.py"], base_dir=ROOT)
        with tempfile.TemporaryDirectory() as tmp:
            report, values = run_pipeline(spec_of(nodes), registry=default_registry, workdir=tmp, cache_dir=None)
        self.assertEqual(report.status, "success")
        self.assertEqual(values["stats.stats"]["行数"], 2)


class ConfigFormatTest(unittest.TestCase):
    def test_toml_spec_loads(self):
        spec = PipelineSpec.from_file(ROOT / "examples" / "pipeline_orders.toml")
        self.assertEqual(spec.name, "orders_daily")
        self.assertEqual(len(spec.nodes), 11)
        build_plan(spec, default_registry)

    def test_json_spec_loads(self):
        spec = PipelineSpec.from_file(ROOT / "examples" / "pipeline_orders.json")
        plan = build_plan(spec, default_registry)
        self.assertEqual(len(plan.nodes), 18)
        self.assertTrue(plan.order.index("load_orders") < plan.order.index("clean"))

    def test_plugin_loading(self):
        load_plugins(paths=["examples/plugins/demo_ops.py"], base_dir=ROOT)
        for name in ("demo.word_count", "demo.flaky_probe", "demo.text_stats"):
            self.assertIn(name, default_registry)


# --------------------------------------------------------------------------- #
# M1.5-2 打包完备 / M1.5-3 惰性产物目录 / M1.5-7 取消机制
# --------------------------------------------------------------------------- #
class PackagingTest(unittest.TestCase):
    def test_py_typed_marker_shipped(self):
        import op_pipeline

        pkg_dir = Path(op_pipeline.__file__).parent
        self.assertTrue((pkg_dir / "py.typed").is_file(), "包内缺少 py.typed 标记文件")

    def test_pyproject_metadata_complete(self):
        data = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
        project = data["project"]
        self.assertIn("Apache-2.0", project["license"]["text"])
        classifiers = " ".join(project.get("classifiers", []))
        self.assertIn("License :: OSI Approved :: Apache Software License", classifiers)
        self.assertEqual(
            data["tool"]["setuptools"]["package-data"]["op_pipeline"], ["py.typed"]
        )


class LazyArtifactsTest(unittest.TestCase):
    def test_in_memory_run_leaves_no_hidden_dir(self):
        nodes = [
            {"id": "src", "op": "control.const", "params": {"value": 1}},
            {"id": "dbl", "op": "testkit.double", "inputs": {"value": "${src.value}"}},
        ]
        with tempfile.TemporaryDirectory() as tmp:
            report, _ = run_pipeline(spec_of(nodes), registry=default_registry,
                                     workdir=tmp, cache_dir=None)
            self.assertTrue(report.ok)
            self.assertFalse((Path(tmp) / ".op_pipeline").exists(),
                             "纯内存运行不应在 workdir 留下 .op_pipeline/")

    def test_artifacts_dir_override_lazy_create(self):
        with tempfile.TemporaryDirectory() as tmp:
            custom = Path(tmp).resolve() / "custom-art"
            ctx = RunContext.create(pipeline="t", params={}, workdir=tmp, artifacts_dir=custom)
            self.assertEqual(ctx.artifacts_dir, custom)
            self.assertFalse(custom.exists(), "未使用产物前不应创建目录")
            node = ctx.for_node("n1", "testkit.double")
            path = node.artifact_path("probe.json")
            self.assertTrue(custom.exists(), "首次 artifact_path 应创建目录")
            self.assertEqual(path.parent.name, "n1")

    def test_artifacts_dir_relative_resolves_to_workdir(self):
        with tempfile.TemporaryDirectory() as tmp:
            ctx = RunContext.create(pipeline="t", params={}, workdir=tmp, artifacts_dir="rel-art")
            self.assertEqual(ctx.artifacts_dir, Path(tmp).resolve() / "rel-art")

    def test_save_artifact_writes_to_custom_dir(self):
        with tempfile.TemporaryDirectory() as tmp:
            custom = Path(tmp).resolve() / "art"
            ctx = RunContext.create(pipeline="t", params={}, workdir=tmp, artifacts_dir=custom)
            node = ctx.for_node("n1", "testkit.double")
            path = node.save_artifact("summary.json", {"ok": 1})
            self.assertTrue(path.is_file())
            self.assertEqual(json.loads(path.read_text(encoding="utf-8")), {"ok": 1})


class CancelTest(unittest.TestCase):
    @staticmethod
    def _three_node_spec() -> PipelineSpec:
        # src(瞬时) -> slow(0.5s) -> after(瞬时)：取消窗口在 slow 执行期间
        nodes = [
            {"id": "src", "op": "control.const", "params": {"value": 1}},
            {"id": "slow", "op": "testkit.sleep", "inputs": {"value": "${src.value}"},
             "params": {"seconds": 0.5}},
            {"id": "after", "op": "testkit.double", "inputs": {"value": "${slow.value}"}},
        ]
        return spec_of(nodes)

    def test_cancel_event_pre_set_skips_all(self):
        event = threading.Event()
        event.set()
        with tempfile.TemporaryDirectory() as tmp:
            report, _ = run_pipeline(self._three_node_spec(), registry=default_registry,
                                      workdir=tmp, cache_dir=None, cancel_event=event)
        for node in report.nodes:
            self.assertEqual(node.status, "skipped")
            self.assertIn("取消", node.note)
        self.assertEqual(report.status, "partial")
        self.assertTrue(any("取消" in w for w in report.warnings))

    def test_engine_cancel_mid_run(self):
        plan = build_plan(self._three_node_spec(), default_registry)
        with tempfile.TemporaryDirectory() as tmp:
            context = RunContext.create(pipeline="t", params=plan.params, workdir=tmp)
            engine = Engine(plan, context, cache_store=CacheStore(None))
            timer = threading.Timer(0.15, engine.cancel, kwargs={"reason": "测试取消"})
            timer.start()
            try:
                report = engine.run()
            finally:
                timer.cancel()
        self.assertEqual(report.node("slow").status, "success", "在跑节点应自然结束")
        self.assertEqual(report.node("after").status, "skipped")
        self.assertIn("取消", report.node("after").note)
        self.assertEqual(report.status, "partial")
        self.assertTrue(any("测试取消" in w for w in report.warnings))


class RegistryIsolationTest(unittest.TestCase):
    """M1.5-1：独立注册表——load_plugins/run 支持 registry 注入，互不污染全局表。"""

    PLUGIN = str(ROOT / "examples" / "plugins" / "demo_ops.py")

    @staticmethod
    def _const_cfg(path: Path) -> None:
        path.write_text(
            "[[nodes]]\n"
            "id = \"src\"\n"
            "op = \"control.const\"\n"
            "\n"
            "[nodes.params]\n"
            "value = 42\n",
            encoding="utf-8",
        )

    def test_same_plugin_file_into_two_registries(self):
        before = set(default_registry.names())
        reg_a, reg_b = Registry(), Registry()
        load_plugins(paths=(self.PLUGIN,), registry=reg_a)
        load_plugins(paths=(self.PLUGIN,), registry=reg_b)
        for reg in (reg_a, reg_b):
            self.assertIn("demo.word_count", reg, "插件算子应注册进目标注册表")
            self.assertIn("control.const", reg, "内置算子应补齐到目标注册表")
        self.assertEqual(set(default_registry.names()), before, "全局注册表不应被插件污染")

        @op("m15.only_a", registry=reg_a, replace=True)
        class _OnlyA(Operator):
            def execute(self, ctx, inputs):
                return {"value": inputs.get("value", 1)}

        self.assertIn("m15.only_a", reg_a)
        self.assertNotIn("m15.only_a", reg_b)
        self.assertNotIn("m15.only_a", default_registry)

    def test_run_with_custom_registry(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = Path(tmp) / "pipe.toml"
            self._const_cfg(cfg)
            before = set(default_registry.names())
            reg = Registry()
            load_plugins(paths=(self.PLUGIN,), registry=reg)
            report, _ = run(cfg, registry=reg, cache_dir=None)
            self.assertTrue(report.ok)
            self.assertEqual(report.node("src").status, "success")
            self.assertEqual(set(default_registry.names()), before, "全局注册表不应被污染")

    def test_run_without_registry_backward_compatible(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = Path(tmp) / "pipe.toml"
            self._const_cfg(cfg)
            report, _ = run(cfg, cache_dir=None)
            self.assertTrue(report.ok)

    def test_directed_op_on_empty_registry(self):
        """空注册表（len==0 为 falsy）也必须定向成功——不能被 `or` 短路到全局表。"""
        empty = Registry()

        @op("m15.empty_only", registry=empty)
        class _EmptyOnly(Operator):
            def execute(self, ctx, inputs):
                return {}

        self.assertIn("m15.empty_only", empty)
        self.assertNotIn("m15.empty_only", default_registry)


class RegistryThreadSafetyTest(unittest.TestCase):
    """M1.5-8：注册表线程安全——并发注册/加载不竞态，失败占位可重试。"""

    PLUGIN = str(ROOT / "examples" / "plugins" / "demo_ops.py")

    @staticmethod
    def _dyn_op(k: int) -> type:
        def execute(self, ctx, inputs):
            return {"value": inputs.get("value", 1)}

        return type(f"_DynOp{k}", (Operator,), {"execute": execute})

    def test_failed_plugin_can_retry_same_registry(self):
        """加载失败的插件文件应回滚"同表去重"占位，下次调用能重新执行。"""
        with tempfile.TemporaryDirectory() as tmp:
            bad = Path(tmp) / "bad_plugin.py"
            bad.write_text(
                'raise RuntimeError("插件加载失败（预期内）")\n', encoding="utf-8"
            )
            reg = Registry()
            for _ in range(2):
                with self.assertRaises(RuntimeError, msg="失败后重试应再次真实执行插件文件"):
                    load_plugins(paths=(str(bad),), package=None, registry=reg)

    def test_concurrent_register_and_read(self):
        """多线程并发注册唯一名算子 + 并发读：无异常、注册数量一个不少。"""
        reg = Registry()
        writers, readers, per_writer = 4, 2, 25
        errors: list[BaseException] = []
        stop = threading.Event()
        start = threading.Barrier(writers + readers)

        def register_worker(w: int) -> None:
            try:
                start.wait()
                for i in range(per_writer):
                    reg.register(self._dyn_op(w * per_writer + i), f"m15.w{w}.op{i}")
            except BaseException as exc:  # pragma: no cover - 仅在竞态时触发
                errors.append(exc)

        def read_worker() -> None:
            try:
                start.wait()
                while not stop.is_set():
                    reg.names()
                    "m15.nobody" in reg
                    reg.canonical_name("m15.w0.op0")
            except BaseException as exc:  # pragma: no cover - 仅在竞态时触发
                errors.append(exc)

        threads = [
            threading.Thread(target=register_worker, args=(w,)) for w in range(writers)
        ] + [threading.Thread(target=read_worker) for _ in range(readers)]
        for t in threads:
            t.start()
        for t in threads[:writers]:
            t.join()
        stop.set()
        for t in threads[writers:]:
            t.join()

        self.assertEqual([], errors, "并发注册/读取期间不应有异常")
        self.assertEqual(len(reg), writers * per_writer, "注册数量应一个不少")

    def test_concurrent_load_same_file_same_registry(self):
        """多线程并发把同一插件文件加载进同一注册表：恰好执行一次，无重复注册异常。"""
        reg = Registry()
        workers, rounds = 4, 5
        errors: list[BaseException] = []
        start = threading.Barrier(workers)

        def load_worker() -> None:
            try:
                start.wait()
                for _ in range(rounds):
                    load_plugins(paths=(self.PLUGIN,), registry=reg)
            except BaseException as exc:  # pragma: no cover - 仅在竞态时触发
                errors.append(exc)

        threads = [threading.Thread(target=load_worker) for _ in range(workers)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual([], errors, "并发加载同一文件不应触发重复注册或字典竞态")
        self.assertIn("demo.word_count", reg, "插件算子应恰好注册一次")
        self.assertIn("control.const", reg, "内置算子应补齐到目标注册表")


class UnknownKeyWarningTest(unittest.TestCase):
    """M1.5-4：from_dict 对未知键发 logging.warning，正确配置零告警。"""

    def test_unknown_root_key_warns(self):
        with self.assertLogs("op_pipeline", level="WARNING") as log:
            PipelineSpec.from_dict({"nodes": [], "param": {"a": 1}})  # 拼错：应为 params
        joined = "\n".join(log.output)
        self.assertIn("'param'", joined, "应告警未知根键 'param'")
        self.assertIn("'params'", joined, "应给出近义键提示")

    def test_unknown_node_key_warns(self):
        with self.assertLogs("op_pipeline", level="WARNING") as log:
            PipelineSpec.from_dict(
                {"nodes": [{"id": "src", "op": "control.const", "retry": 2}]}
            )
        joined = "\n".join(log.output)
        self.assertIn("[src]", joined, "告警应定位到节点 id")
        self.assertIn("'retry'", joined, "应告警未知节点键 'retry'")
        self.assertIn("'retries'", joined, "应给出近义键提示")

    def test_valid_config_has_no_warnings(self):
        with self.assertNoLogs("op_pipeline", level="WARNING"):
            PipelineSpec.from_dict(
                {
                    "name": "ok",
                    "description": "全键合法",
                    "version": "1",
                    "params": {"x": 1},
                    "nodes": [
                        {
                            "id": "src",
                            "op": "control.const",
                            "inputs": {},
                            "outputs": {"value": "src.value"},
                            "params": {"value": 1},
                            "when": "value > 0",
                            "retries": 1,
                            "retry_delay": 0.1,
                            "timeout": 5,
                            "cache": True,
                            "allow_failure": False,
                            "description": "d",
                        }
                    ],
                    "outputs": {"v": "src.value"},
                    "defaults": {},
                }
            )

    def test_validate_cli_shows_warning_on_stderr(self):
        """validate 命令对含未知键的配置在 stderr 展示告警（exit code 仍为 0）。"""
        with tempfile.TemporaryDirectory() as tmp:
            cfg = Path(tmp) / "typo.toml"
            cfg.write_text(
                'name = "typo"\n'
                'param = { a = 1 }\n'  # 拼错：应为 params
                "[[nodes]]\n"
                'id = "src"\n'
                'op = "control.const"\n'
                "\n"
                "[nodes.params]\n"
                "value = 42\n",
                encoding="utf-8",
            )
            env = {**os.environ, "PYTHONIOENCODING": "utf-8"}
            proc = subprocess.run(
                [sys.executable, "-m", "op_pipeline", "validate", str(cfg)],
                capture_output=True,
                text=True,
                encoding="utf-8",
                env=env,
                cwd=str(ROOT),
            )
        self.assertEqual(0, proc.returncode, proc.stderr)
        self.assertIn("未知键", proc.stderr)
        self.assertIn("'param'", proc.stderr)


class WhenParamsConsistencyTest(unittest.TestCase):
    """M1.5-5：when 环境的 `params` 用解析后的值（与扁平参数名一致），原值保留在 `raw_params`。"""

    NODES = [
        {"id": "src", "op": "control.const", "params": {"value": [1, 2, 3]}},
        {
            "id": "limit",
            "op": "table.limit",
            "inputs": {"table": "${src.value}"},
            "params": {"n": "${p.n}"},  # 引用流水线参数，两套语义可区分
        },
        {"id": "after", "op": "stats.count_rows", "inputs": {"table": "${limit.table}"}},
    ]

    def _run(self, when: str):
        nodes = [dict(n) for n in self.NODES]
        nodes[1] = {**nodes[1], "when": when}
        with tempfile.TemporaryDirectory() as tmp:
            return run_pipeline(
                spec_of(nodes, params={"n": 2}),
                registry=default_registry,
                workdir=tmp,
                cache_dir=None,
            )

    def test_when_params_namespace_is_resolved(self):
        """`params.n` 与扁平 `n` 应同为解析后的值（2 > 1），节点应执行。"""
        report, _ = self._run("params.n > 1 and n > 1")
        self.assertEqual(
            report.node("limit").status, "success", "params 命名空间应为解析后的值"
        )
        self.assertEqual(report.status, "success")

    def test_when_raw_params_keeps_original(self):
        """原始配置值（可能是 '${p.n}' 串）保留在 `raw_params` 命名空间。"""
        report, _ = self._run("raw_params.n == '${p.n}'")
        self.assertEqual(
            report.node("limit").status, "success", "raw_params 应保留原始配置值"
        )
        self.assertEqual(report.status, "success")


class ExpressionFunctionHookTest(unittest.TestCase):
    """M1.5-6：`register_function` 把领域函数注册进 when 表达式白名单。"""

    NAME = "m156_parse_int"

    def tearDown(self):
        # 注册是全局的：清理本用例注册的函数，避免污染其他用例的白名单
        expression._ALLOWED_FUNCS.pop(self.NAME, None)

    def test_register_then_use_in_when(self):
        register_function(self.NAME, lambda s: int(str(s).replace(",", "")))
        self.assertEqual(evaluate(f"{self.NAME}('1,234')", {}), 1234)

        nodes = [
            {"id": "src", "op": "control.const", "params": {"value": "1,234"}},
            {
                "id": "gate",
                "op": "testkit.double",
                "inputs": {"value": "${src.value}"},
                "params": {"times": 2},
                "when": f"{self.NAME}(value) > 1000",
            },
        ]
        with tempfile.TemporaryDirectory() as tmp:
            report, _ = run_pipeline(
                spec_of(nodes), registry=default_registry, workdir=tmp, cache_dir=None
            )
        self.assertEqual(report.node("gate").status, "success")
        self.assertEqual(report.status, "success")

    def test_override_builtin_rejected(self):
        with self.assertRaises(ExpressionError):
            register_function("len", lambda x: 1)

    def test_non_identifier_name_rejected(self):
        with self.assertRaises(ExpressionError):
            register_function("not-a-name", lambda x: x)

    def test_non_callable_rejected(self):
        with self.assertRaises(ExpressionError):
            register_function(self.NAME, "not-callable")

    def test_ast_interception_unchanged(self):
        register_function(self.NAME, lambda s: 0)
        for bad in ("__import__('os')", "x.__class__"):
            with self.assertRaises(ExpressionError):
                evaluate(bad, {"x": 1})


class PortTypeWarningTest(unittest.TestCase):
    """M1-1：端口软类型不符时进 report.warnings（状态仍 success，值照常传递）。"""

    def test_type_mismatch_warns_but_succeeds(self):
        @op("m11.bad_const", replace=True)
        class _BadConst(Operator):
            description = "声明 int 却返回 str（软类型告警演示）"

            outputs = [("value", int)]

            def run(self, ctx):
                return "42"  # 故意类型不符

        nodes = [
            {"id": "src", "op": "m11.bad_const"},
            {
                "id": "dbl",
                "op": "testkit.double",
                "inputs": {"value": "${src.value}"},
                "params": {"times": 2},
            },
        ]
        with tempfile.TemporaryDirectory() as tmp:
            report, values = run_pipeline(
                spec_of(nodes),
                registry=default_registry,
                workdir=tmp,
                cache_dir=None,
            )
        self.assertEqual(report.status, "success", "软类型不符不应导致失败")
        self.assertEqual(values["dbl.value"], "4242", "str 值应照常向下游传递")
        joined = "\n".join(report.warnings)
        self.assertIn("src", joined, f"应指明类型不符的节点，实际：{report.warnings}")
        self.assertIn("声明", joined, "告警应包含声明类型与实际类型")


class CacheDirResolutionTest(unittest.TestCase):
    """M1-2：相对 cache_dir 以 workdir 为基准（与进程 CWD 无关）。"""

    NODES = [
        {"id": "src", "op": "control.const", "params": {"value": 21}},
        {
            "id": "dbl",
            "op": "testkit.double",
            "inputs": {"value": "${src.value}"},
            "params": {"times": 2},
            "cache": True,
        },
    ]

    def test_relative_cache_dir_anchors_to_workdir(self):
        with tempfile.TemporaryDirectory() as workdir, \
                tempfile.TemporaryDirectory() as cwd_a, \
                tempfile.TemporaryDirectory() as cwd_b:
            old = os.getcwd()
            try:
                os.chdir(cwd_a)
                first, _ = run_pipeline(
                    spec_of(self.NODES),
                    registry=default_registry,
                    workdir=workdir,
                    cache_dir=".pipeline_cache",
                )
                os.chdir(cwd_b)
                second, _ = run_pipeline(
                    spec_of(self.NODES),
                    registry=default_registry,
                    workdir=workdir,
                    cache_dir=".pipeline_cache",
                )
            finally:
                os.chdir(old)
            self.assertFalse(
                first.node("dbl").cached, "首次运行应真实计算"
            )
            self.assertTrue(
                second.node("dbl").cached, "换 CWD 后应命中 workdir 下的同一份缓存"
            )
            self.assertFalse(
                Path(cwd_a, ".pipeline_cache").exists(), "缓存不应落在进程 CWD"
            )

    def test_cli_cache_dir_anchors_to_config_dir(self):
        with tempfile.TemporaryDirectory() as cfg_dir, \
                tempfile.TemporaryDirectory() as cwd:
            cfg = Path(cfg_dir) / "pipe.toml"
            cfg.write_text(
                "[[nodes]]\n"
                "id = \"src\"\n"
                "op = \"control.const\"\n"
                "\n"
                "[nodes.params]\n"
                "value = 42\n",
                encoding="utf-8",
            )
            env = {
                **os.environ,
                "PYTHONIOENCODING": "utf-8",
                # 子进程 CWD 刻意不在仓库根（测试目的），用 PYTHONPATH 定位包
                "PYTHONPATH": str(ROOT),
            }
            for _ in range(2):
                proc = subprocess.run(
                    [sys.executable, "-m", "op_pipeline", "run", str(cfg)],
                    capture_output=True,
                    text=True,
                    encoding="utf-8",
                    env=env,
                    cwd=cwd,
                    timeout=120,
                )
                self.assertEqual(0, proc.returncode, proc.stderr)
            self.assertTrue(
                (Path(cfg_dir) / ".pipeline_cache").exists(),
                "CLI 缓存应落在配置文件目录（workdir 默认值）",
            )
            self.assertFalse(
                Path(cwd, ".pipeline_cache").exists(), "CLI 缓存不应落在进程 CWD"
            )


class FailureTracebackTest(unittest.TestCase):
    """M1-3：失败节点保留（截断的）堆栈，to_dict 带出，summary 不受影响。"""

    def _run_raiser(self, message: str):
        @op("m13.raiser", replace=True)
        class _Raiser(Operator):
            description = "总是抛错（堆栈演示）"
            outputs = [("value", int)]

            def run(self, ctx):
                raise ValueError(message)

        nodes = [{"id": "bad", "op": "m13.raiser"}]
        with tempfile.TemporaryDirectory() as tmp:
            report, _ = run_pipeline(
                spec_of(nodes),
                registry=default_registry,
                workdir=tmp,
                cache_dir=None,
            )
        return report

    def test_failed_node_keeps_traceback(self):
        report = self._run_raiser("boom for traceback")
        node = report.node("bad")
        self.assertEqual(report.status, "failed")
        self.assertIn("Traceback (most recent call last)", node.traceback)
        self.assertIn("ValueError: boom for traceback", node.traceback)
        data = node.to_dict()
        self.assertIn("Traceback (most recent call last)", data["traceback"])

    def test_traceback_truncated_to_about_2000_chars(self):
        report = self._run_raiser("x" * 5000)
        node = report.node("bad")
        self.assertLessEqual(len(node.traceback), 2100, "应截断到约 2000 字符")
        self.assertTrue(
            node.traceback.startswith("..."), "截断应以省略标记开头"
        )
        self.assertIn(
            "x" * 500, node.traceback[-600:],
            "截断应保留末尾（超长异常消息的尾部）",
        )

    def test_successful_node_has_empty_traceback(self):
        nodes = [{"id": "ok", "op": "control.const", "params": {"value": 1}}]
        with tempfile.TemporaryDirectory() as tmp:
            report, _ = run_pipeline(
                spec_of(nodes),
                registry=default_registry,
                workdir=tmp,
                cache_dir=None,
            )
        self.assertEqual("", report.node("ok").traceback)


class RunIdCollisionTest(unittest.TestCase):
    """M1-4：同秒并发的 run_id 不碰撞（产物目录互相隔离）。"""

    def test_two_creates_in_same_second_differ(self):
        with tempfile.TemporaryDirectory() as tmp:
            a = RunContext.create(pipeline="p", params={}, workdir=tmp)
            b = RunContext.create(pipeline="p", params={}, workdir=tmp)
        self.assertNotEqual(a.run_id, b.run_id, "同秒两次 create 应得到不同 run_id")
        self.assertNotEqual(a.artifacts_dir, b.artifacts_dir)
        # 前缀仍是可读的秒级时间戳，后缀为 4 位十六进制
        for run_id in (a.run_id, b.run_id):
            self.assertRegex(run_id, r"^\d{8}-\d{6}-[0-9a-f]{4}$")

    def test_explicit_run_id_still_respected(self):
        with tempfile.TemporaryDirectory() as tmp:
            ctx = RunContext.create(
                pipeline="p", params={}, workdir=tmp, run_id="custom-id"
            )
        self.assertEqual("custom-id", ctx.run_id)


class DefaultsInheritanceTest(unittest.TestCase):
    """M1-5：PipelineSpec.defaults 作为节点运行策略缺省值（节点显式值优先）。"""

    DEFAULTS = {"retries": 2, "cache": True, "timeout": 30}

    def _node_spec(self, **node_overrides):
        node = {"id": "n", "op": "control.const", "params": {"value": 1}}
        node.update(node_overrides)
        spec = PipelineSpec.from_dict(
            {"defaults": self.DEFAULTS, "nodes": [node]}
        )
        return spec.nodes[0]

    def test_unset_node_inherits_defaults(self):
        node = self._node_spec()
        self.assertEqual(2, node.retries, "未设置 retries 应继承 defaults")
        self.assertTrue(node.cache, "未设置 cache 应继承 defaults")
        self.assertEqual(30.0, node.timeout, "未设置 timeout 应继承 defaults")

    def test_explicit_values_win_over_defaults(self):
        node = self._node_spec(retries=0, cache=False)
        self.assertEqual(0, node.retries, "显式 retries=0 应优先于 defaults")
        self.assertFalse(node.cache, "显式 cache=false 应优先于 defaults")

    def test_unknown_default_key_rejected(self):
        with self.assertRaises(ConfigError):
            PipelineSpec.from_dict({"defaults": {"brain": 1}, "nodes": []})


class DeadCodeRemovalTest(unittest.TestCase):
    """M1-6：死代码清理——残留字段与误导性导出必须移除，`__all__` 同步收缩。"""

    def test_module_execution_error_removed(self):
        import op_pipeline

        self.assertFalse(
            hasattr(op_pipeline, "ModuleExecutionError"),
            "引擎从不抛出该类，保留导出会误导排障",
        )
        self.assertNotIn("ModuleExecutionError", op_pipeline.__all__)

    def test_plan_node_has_no_resolved_inputs(self):
        import dataclasses

        fields = {f.name for f in dataclasses.fields(PlanNode)}
        self.assertNotIn(
            "resolved_inputs", fields, "恒为 {} 的残留字段应删除"
        )


class PluginPathDedupTest(unittest.TestCase):
    """M1-7：文件插件按路径去重——不同目录的同名文件都能加载，同文件保持幂等。"""

    OPS_A = """
from op_pipeline import Operator, op

@op("m17a.op_a")
class OpA(Operator):
    outputs = [("value", int)]

    def run(self, ctx):
        return 1
"""

    OPS_B = """
from op_pipeline import Operator, op

@op("m17a.op_b")
class OpB(Operator):
    outputs = [("value", int)]

    def run(self, ctx):
        return 2
"""

    OPS_C = """
from op_pipeline import Operator, op

@op("m17b.op_a")
class OpC(Operator):
    outputs = [("value", int)]

    def run(self, ctx):
        return 3
"""

    def test_same_stem_different_dirs_both_loaded(self):
        with tempfile.TemporaryDirectory() as d1, tempfile.TemporaryDirectory() as d2:
            a = Path(d1) / "my_ops.py"
            b = Path(d2) / "my_ops.py"
            a.write_text(self.OPS_A, encoding="utf-8")
            b.write_text(self.OPS_B, encoding="utf-8")
            load_plugins(paths=[a, b], package=None)
        self.assertIn("m17a.op_a", default_registry, "第一个同名文件应被加载")
        self.assertIn("m17a.op_b", default_registry, "第二个同名文件不应被 stem 去重静默跳过")

    def test_same_file_stays_idempotent(self):
        with tempfile.TemporaryDirectory() as d:
            c = Path(d) / "my_ops.py"
            c.write_text(self.OPS_C, encoding="utf-8")
            load_plugins(paths=[c], package=None)
            # 裸 @op 无 replace：若重复执行会抛 DuplicateOperatorError
            load_plugins(paths=[c], package=None)
        self.assertIn("m17b.op_a", default_registry)


class RunModulePluginsTest(unittest.TestCase):
    """M1-8：run() 支持模块插件——库调用方不必再自行 load_plugins。

    text 示例的 read 节点按 workdir 读 data/notes.txt，因此沿用默认
    workdir（配置文件目录），与 CLI 回归的副作用一致（.gitignore 已忽略）。
    """

    def test_run_accepts_module_plugins(self):
        import sys

        cfg = ROOT / "examples" / "pipeline_text.toml"
        sys.path.insert(0, str(ROOT))  # examples.* 是 ROOT 下的命名空间包
        try:
            report, _ = run(
                cfg,
                cache_dir=None,
                plugin_modules=("examples.plugins.demo_ops",),
                registry=Registry(),  # 独立表：不依赖默认表状态，裸 @op 经 contextvar 落入
            )
        finally:
            sys.path.remove(str(ROOT))
        self.assertEqual("success", report.status)

    def test_run_without_plugin_modules_unchanged(self):
        # 守门：不传 plugin_modules 时与历史行为一致（路径插件照常工作）
        cfg = ROOT / "examples" / "pipeline_text.toml"
        report, _ = run(
            cfg,
            cache_dir=None,
            plugins=(str(cfg.parent / "plugins" / "demo_ops.py"),),
            registry=Registry(),
        )
        self.assertEqual("success", report.status)


class DryRunNoSideEffectsTest(unittest.TestCase):
    """M1-9：--dry-run 提前到副作用创建之前——不建产物/缓存目录；ops --json 契约核对。"""

    ENV = {
        **os.environ,
        "PYTHONIOENCODING": "utf-8",
        "PYTHONPATH": str(ROOT),
    }

    @staticmethod
    def _run_cli(*cli_args: str, cwd: str) -> subprocess.CompletedProcess:
        return subprocess.run(
            [sys.executable, "-m", "op_pipeline", *cli_args],
            capture_output=True,
            text=True,
            encoding="utf-8",
            env=DryRunNoSideEffectsTest.ENV,
            cwd=cwd,
            timeout=120,
        )

    def test_dry_run_creates_no_dirs(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = Path(tmp) / "pipe.toml"
            cfg.write_text(
                "[[nodes]]\n"
                "id = \"src\"\n"
                "op = \"control.const\"\n"
                "\n"
                "[nodes.params]\n"
                "value = 42\n",
                encoding="utf-8",
            )
            proc = self._run_cli("run", str(cfg), "--dry-run", cwd=str(ROOT))
            self.assertEqual(0, proc.returncode, proc.stderr)
            self.assertFalse((Path(tmp) / ".op_pipeline").exists(), "dry-run 不应建产物目录")
            self.assertFalse(
                (Path(tmp) / ".pipeline_cache").exists(), "dry-run 不应建缓存目录"
            )

    def test_ops_json_contract(self):
        proc = self._run_cli("ops", "--json", cwd=str(ROOT))
        self.assertEqual(0, proc.returncode, proc.stderr)
        payload = json.loads(proc.stdout)
        self.assertIsInstance(payload, list)
        self.assertGreater(len(payload), 20)
        names = {item["name"] for item in payload}
        self.assertIn("control.const", names)
        required = {"name", "description", "inputs", "outputs"}
        for item in payload:
            self.assertTrue(required <= set(item), f"{item.get('name')} 缺少契约字段")


class ExecutorContractTest(unittest.TestCase):
    """M2-1：Executor 接口契约——Engine 的执行器扩展点。"""

    def test_thread_executor_satisfies_protocol(self):
        from op_pipeline.executor import Executor, ThreadExecutor

        ex = ThreadExecutor(max_workers=2)
        with ex:  # __enter__/__exit__ 必须可用（Engine 用 with 驱动）
            self.assertIsInstance(ex, Executor, "ThreadExecutor 应满足 Executor 协议")

    def test_submit_wait_first_completed(self):
        import time as _time

        from op_pipeline.executor import ThreadExecutor

        with ThreadExecutor(max_workers=4) as ex:
            started = _time.monotonic()
            futures = {ex.submit(_time.sleep, 0.05) for _ in range(4)}
            # first_completed=True 时 wait 应在约 0.05s 返回（不是等全部）
            done = ex.wait(futures, first_completed=True)
            elapsed = _time.monotonic() - started
            self.assertLess(elapsed, 0.15, "first_completed 应在首个完成即返回")
            self.assertTrue(done)
            rest = futures - done
            if rest:
                all_done = ex.wait(rest, first_completed=False)
                self.assertEqual(rest, all_done, "first_completed=False 应等到全部")

    def test_shutdown_rejects_new_submits(self):
        from op_pipeline.executor import ThreadExecutor

        ex = ThreadExecutor(max_workers=1)
        ex.shutdown()
        with self.assertRaises(RuntimeError):
            ex.submit(sum, [1, 2])


# --------------------------------------------------------------------------- #
# M2-2：进程执行器测试（算子与辅助函数必须模块级——子进程按引用反序列化）
# --------------------------------------------------------------------------- #
@op("testkit.pid", replace=True)
class PidOp(Operator):
    inputs = [("value", object)]
    outputs = [("value", int)]

    def run(self, ctx, value):
        return {"value": os.getpid()}


@op("testkit.spin", replace=True)
class SpinOp(Operator):
    inputs = [("value", object)]
    outputs = [("value", object)]
    params = {"seconds": Param(float, default=30.0, minimum=0.0)}

    def run(self, ctx, value, seconds):
        deadline = time.time() + seconds
        while time.time() < deadline:  # 忙等：硬超时的靶子
            pass
        return {"value": value}


@op("testkit.cpu_burn", replace=True)
class CpuBurnOp(Operator):
    inputs = [("value", object)]
    outputs = [("value", int)]

    def run(self, ctx, value):
        # 计算量须显著大于子进程 spawn 成本（Windows 上约 1s），
        # 否则 spawn 串行化会淹没并行收益（见 M2-2 测试注记）
        return {"value": sum(i * i for i in range(16_000_000))}


@op("testkit.echo", replace=True)
class EchoOp(Operator):
    inputs = [("value", object)]
    outputs = [("value", object)]

    def run(self, ctx, value):
        return {"value": value}


@op("testkit.make_lambda", replace=True)
class LambdaOutOp(Operator):
    outputs = [("value", object)]

    def run(self, ctx):
        return {"value": lambda x: x + 1}


def _spin(seconds: float) -> str:
    deadline = time.time() + seconds
    while time.time() < deadline:
        pass
    return "done"


def _add(a, b):
    return a + b


def _boom():
    raise ValueError("child-boom")


class ProcessExecutorContractTest(unittest.TestCase):
    """M2-2：ProcessExecutor——每任务一进程、硬超时 terminate、不可 pickle 回退线程。"""

    def _executor(self, **kw):
        from op_pipeline.executor import ProcessExecutor

        return ProcessExecutor(**kw)

    def test_satisfies_protocol(self):
        from op_pipeline.executor import Executor

        ex = self._executor()
        try:
            self.assertIsInstance(ex, Executor, "ProcessExecutor 应满足 Executor 协议")
        finally:
            ex.shutdown()

    def test_roundtrip_and_first_completed(self):
        with self._executor() as ex:
            slow = ex.submit(_spin, 0.3)
            fast = ex.submit(_add, 1, 2)
            done = ex.wait({slow, fast}, first_completed=True)
            self.assertEqual({fast}, done, "first_completed 应只返回先完成的快任务")
            self.assertEqual(fast.result(), 3)
            self.assertEqual(ex.wait({slow}, first_completed=False), {slow})
            self.assertEqual(slow.result(), "done")

    def test_hard_timeout_terminates_child(self):
        with self._executor() as ex:
            started = time.monotonic()
            task = ex.submit(_spin, 30.0, timeout=0.5)
            done = ex.wait({task}, first_completed=False)
            elapsed = time.monotonic() - started
            self.assertIn(task, done)
            self.assertLess(elapsed, 3.0, "到点必须 terminate 子进程，而不是等它跑满 30s")
            exc = task.exception()
            self.assertIsNotNone(exc)
            self.assertIn("硬超时", str(exc))

    def test_child_exception_roundtrips(self):
        with self._executor() as ex:
            task = ex.submit(_boom)
            ex.wait({task}, first_completed=False)
            exc = task.exception()
            self.assertIsNotNone(exc)
            self.assertIn("child-boom", str(exc))

    def test_unpicklable_falls_back_with_warning(self):
        notes: list[str] = []
        with self._executor(on_fallback=notes.append) as ex:
            task = ex.submit(lambda: "via-thread")
            ex.wait({task}, first_completed=False)
            self.assertEqual(task.result(), "via-thread")
        self.assertTrue(notes, "不可 pickle 应触发 on_fallback 告警")
        self.assertIn("pickle", notes[0])


class ProcessNodeTest(unittest.TestCase):
    """M2-2：节点级/运行级 executor="process" 的引擎语义。"""

    def _run(self, nodes, params=None, **kw):
        with tempfile.TemporaryDirectory() as tmp:
            return run_pipeline(
                spec_of(nodes, params=params), registry=default_registry,
                workdir=tmp, cache_dir=None, max_workers=kw.pop("max_workers", 4), **kw,
            )

    def test_node_executor_process_runs_in_child(self):
        report, values = self._run([
            {"id": "src", "op": "control.const", "params": {"value": 1}},
            {"id": "pid", "op": "testkit.pid", "inputs": {"value": "${src.value}"},
             "executor": "process"},
        ])
        self.assertEqual(report.status, "success")
        self.assertNotEqual(values["pid.value"], os.getpid(), "节点应运行在子进程")

    def test_run_level_executor_process(self):
        report, values = self._run([
            {"id": "src", "op": "control.const", "params": {"value": 1}},
            {"id": "pid", "op": "testkit.pid", "inputs": {"value": "${src.value}"}},
        ], executor="process")
        self.assertEqual(report.status, "success")
        self.assertNotEqual(values["pid.value"], os.getpid(), "运行级 --executor 应作用于未显式指定的节点")

    def test_hard_timeout_node_fails_fast(self):
        report, _ = self._run([
            {"id": "src", "op": "control.const", "params": {"value": 1}},
            {"id": "spin", "op": "testkit.spin", "inputs": {"value": "${src.value}"},
             "executor": "process", "timeout": 0.5},
        ])
        self.assertEqual(report.node("spin").status, "failed")
        self.assertIn("硬超时", report.node("spin").error)
        self.assertLess(report.duration, 3.0, "死循环节点应被 terminate 而非拖满 30s")
        self.assertEqual(report.status, "failed")

    def test_cpu_parallel_speed(self):
        src = {"id": "src", "op": "control.const", "params": {"value": 1}}
        fan = [src] + [
            {"id": f"b{i}", "op": "testkit.cpu_burn",
             "inputs": {"value": "${src.value}"}, "executor": "process"}
            for i in range(4)
        ]
        chain = [dict(src)]
        prev = "src"
        for i in range(4):
            chain.append({"id": f"c{i}", "op": "testkit.cpu_burn",
                          "inputs": {"value": f"${{{prev}.value}}"}, "executor": "process"})
            prev = f"c{i}"
        report_fan, _ = self._run(fan)
        report_chain, _ = self._run(chain)
        self.assertEqual(report_fan.status, "success")
        self.assertEqual(report_chain.status, "success")
        # 串行链与扇出各自承担相同的 spawn 成本；差异只来自计算并行度。
        # 阈值 0.8：即便 spawn 被环境（杀软扫描）串行化拉长，计算收益也应兑现。
        self.assertLess(
            report_fan.duration, report_chain.duration * 0.8,
            f"4 并行进程节点({report_fan.duration:.2f}s)应显著低于串行链({report_chain.duration:.2f}s)",
        )

    def test_unpicklable_fallback_warns(self):
        report, _ = self._run([
            {"id": "src", "op": "testkit.make_lambda"},
            {"id": "echo", "op": "testkit.echo", "inputs": {"value": "${src.value}"},
             "executor": "process"},
        ])
        self.assertEqual(report.status, "success", "不可 pickle 应回退线程执行，节点不失败")
        self.assertTrue(any("回退" in w for w in report.warnings),
                        f"应记录回退告警：{report.warnings}")

    def test_executor_field_validation(self):
        with self.assertRaises(ConfigError):
            spec_of([{"id": "x", "op": "control.const", "executor": "oops"}])

    def test_defaults_executor_merge(self):
        spec = PipelineSpec.from_dict({
            "name": "t",
            "defaults": {"executor": "process"},
            "nodes": [
                {"id": "x", "op": "control.const", "params": {"value": 1}},
                {"id": "y", "op": "control.const", "executor": "thread",
                 "params": {"value": 2}},
            ],
        })
        self.assertEqual(spec.nodes[0].executor, "process", "未显式指定应继承 defaults")
        self.assertEqual(spec.nodes[1].executor, "thread", "节点显式值优先")

    def test_cli_executor_flag(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = Path(tmp) / "mini.json"
            cfg.write_text(json.dumps({
                "name": "mini",
                "nodes": [{"id": "n", "op": "control.const", "params": {"value": 1}}],
            }), encoding="utf-8")
            proc = DryRunNoSideEffectsTest._run_cli(
                "run", str(cfg), "--executor", "process", "--no-cache", cwd=str(ROOT)
            )
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertIn("success", proc.stdout)


# --------------------------------------------------------------------------- #
# M2-3：句柄存取抽象
# --------------------------------------------------------------------------- #
class HandleStoreTest(unittest.TestCase):
    """M2-3：HandleStore 协议契约 + InMemoryHandleStore 平移行为 + 引擎注入。"""

    def _store(self):
        from op_pipeline.handlestore import InMemoryHandleStore

        return InMemoryHandleStore()

    def test_protocol_and_roundtrip(self):
        from op_pipeline.handlestore import HandleStore
        from op_pipeline import HandleStore as Exported, InMemoryHandleStore

        store = self._store()
        self.assertIsInstance(store, HandleStore, "InMemoryHandleStore 应满足协议")
        self.assertIs(Exported, HandleStore, "协议应从包顶层导出")
        store.put("a.value", 1)
        self.assertEqual(store.get("a.value"), 1)
        with self.assertRaises(KeyError):
            store.get("missing")

    def test_snapshot_isolates_concurrent_writes(self):
        store = self._store()
        store.put("a", [1, 2])
        snap = store.snapshot()
        store.put("b", 3)
        self.assertEqual(snap, {"a": [1, 2]}, "已取快照不受后续写入影响")
        self.assertEqual(dict(store.snapshot()), {"a": [1, 2], "b": 3})
        self.assertEqual(store.items(), [("a", [1, 2]), ("b", 3)])

    def test_thread_safety(self):
        store = self._store()
        errors: list[Exception] = []

        def writer(prefix: str):
            try:
                for i in range(100):
                    store.put(f"{prefix}{i}", i)
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [threading.Thread(target=writer, args=(f"t{t}.",)) for t in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertFalse(errors)
        self.assertEqual(len(store.items()), 400)

    def test_ref_resolver_accepts_store(self):
        from op_pipeline.spec import RefResolver

        store = self._store()
        store.put("src.value", {"x": 10})
        resolver = RefResolver({"n": 5}, store)
        self.assertEqual(resolver.resolve("${p.n}"), 5, "p. 前缀应查 params['n']")
        self.assertEqual(resolver.resolve("${src.value.x}"), 10, "store 应支持句柄下钻解析")
        mirror = RefResolver({"n": 5}, {"src.value": {"x": 10}})
        self.assertEqual(mirror.resolve("${src.value.x}"), 10, "Mapping 用法保持兼容")

    def test_engine_accepts_custom_store(self):
        from op_pipeline.handlestore import InMemoryHandleStore

        store = InMemoryHandleStore()
        with tempfile.TemporaryDirectory() as tmp:
            plan = build_plan(
                spec_of([{"id": "src", "op": "control.const", "params": {"value": 42}}]),
                default_registry,
            )
            context = RunContext.create(pipeline="t", params={}, workdir=tmp)
            engine = Engine(plan, context, handle_store=store)
            report = engine.run()
        self.assertEqual(report.status, "success")
        self.assertEqual(store.get("src.value"), 42, "节点输出应写入注入的 store")


# --------------------------------------------------------------------------- #
# M2-4：超阈值落盘的句柄存储（SpillHandleStore）
# --------------------------------------------------------------------------- #
@op("m24.big_table")
class M24BigTableOp(Operator):
    """产出大表（list[int]）的测试算子；n 够大时输出会超过落盘阈值。"""

    outputs = [("rows", list)]
    params = {"n": Param(int, default=10)}

    def run(self, ctx, n):
        return {"rows": list(range(n))}


@op("m24.sum_two")
class M24SumTwoOp(Operator):
    """把两张表求和，验证落盘句柄能被下游整键引用并按需加载。"""

    inputs = [("a", list), ("b", list)]
    outputs = [("total", int)]

    def run(self, ctx, a, b):
        return sum(a) + sum(b)


class SpillHandleStoreTest(unittest.TestCase):
    """M2-4：SpillHandleStore 超阈值落盘、按需加载、引擎注入与收口。"""

    def _store(self, **kwargs):
        from op_pipeline.handlestore import SpillHandleStore

        return SpillHandleStore(**kwargs)

    @staticmethod
    def _spill_files(store):
        return list(store.spill_dir.glob("*.pkl")) if store.spill_dir else []

    def test_protocol_and_export(self):
        from op_pipeline.handlestore import HandleStore, SpillHandleStore as ModuleSpill
        from op_pipeline import HandleStore as ExportedStore, SpillHandleStore

        store = self._store()
        self.assertIsInstance(store, HandleStore, "SpillHandleStore 应满足协议")
        self.assertIs(ExportedStore, HandleStore, "HandleStore 应从包顶层导出")
        self.assertIs(SpillHandleStore, ModuleSpill, "SpillHandleStore 应从包顶层导出")

    def test_small_values_stay_in_memory(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = self._store(threshold=1_000_000, spill_dir=tmp)
            store.put("a.value", {"x": 1})
            store.put("b", "小字符串")
            self.assertEqual(self._spill_files(store), [], "小值不应产生落盘文件")
            self.assertEqual(store.get("a.value"), {"x": 1})
            self.assertEqual(store.get("b"), "小字符串")
            self.assertEqual(dict(store.snapshot()), {"a.value": {"x": 1}, "b": "小字符串"})

    def test_large_value_spills_to_disk(self):
        big = list(range(2000))
        with tempfile.TemporaryDirectory() as tmp:
            store = self._store(threshold=64, spill_dir=tmp)
            store.put("big.rows", big)
            files = self._spill_files(store)
            self.assertEqual(len(files), 1, "超阈值值应落盘为单个 pkl 文件")
            self.assertEqual(store.get("big.rows"), big, "落盘值按需加载应还原等值对象")
            snap = store.snapshot()
            self.assertEqual(snap["big.rows"], big, "快照按键访问应加载等值对象")
            self.assertEqual(store.items(), [("big.rows", big)])
            with self.assertRaises(KeyError):
                store.get("missing")

    def test_snapshot_membership_skips_disk(self):
        # 惰性快照的核心承诺：`in` / len / 迭代 不触发磁盘加载
        big = list(range(2000))
        with tempfile.TemporaryDirectory() as tmp:
            store = self._store(threshold=64, spill_dir=tmp)
            store.put("big.rows", big)
            snap = store.snapshot()
            for path in self._spill_files(store):
                path.unlink()
            self.assertIn("big.rows", snap, "成员检查不应读盘（文件已删仍应为真）")
            self.assertEqual(len(snap), 1)
            self.assertEqual(list(snap), ["big.rows"])
            with self.assertRaises(FileNotFoundError):
                snap["big.rows"]  # noqa: B018  按键访问应触发读盘并如实报错

    def test_unpickleable_value_falls_back_to_memory(self):
        sentinel = lambda: None  # noqa: E731  lambda 不可 pickle
        with tempfile.TemporaryDirectory() as tmp:
            store = self._store(threshold=64, spill_dir=tmp)
            with self.assertLogs("op_pipeline", level="WARNING") as caught:
                store.put("fn", sentinel)
            self.assertIn("不可 pickle", "\n".join(caught.output))
            self.assertIs(store.get("fn"), sentinel, "不可 pickle 值应原对象留在内存")
            self.assertEqual(self._spill_files(store), [])

    def test_large_value_without_dir_falls_back_to_memory(self):
        big = list(range(2000))
        store = self._store(threshold=64)
        with self.assertLogs("op_pipeline", level="WARNING") as caught:
            store.put("big.rows", big)
        self.assertIn("落盘目录", "\n".join(caught.output))
        self.assertEqual(store.get("big.rows"), big, "无目录时应退化为内存保留")
        self.assertEqual(dict(store.snapshot()), {"big.rows": big})

    def test_engine_attaches_spill_dir_to_artifacts(self):
        store = self._store(threshold=64)
        with tempfile.TemporaryDirectory() as tmp:
            plan = build_plan(
                spec_of([{"id": "src", "op": "m24.big_table", "params": {"n": 2000}}]),
                default_registry,
            )
            context = RunContext.create(pipeline="t", params={}, workdir=tmp)
            engine = Engine(plan, context, handle_store=store)
            report = engine.run()
            self.assertEqual(report.status, "success")
            self.assertEqual(
                store.spill_dir, context.artifacts_dir / "spill",
                "未显式指定目录时应挂到本次运行的 artifacts_dir 下",
            )
            self.assertEqual(len(self._spill_files(store)), 1, "大表输出应落盘")
            self.assertEqual(store.get("src.rows")[:3], [0, 1, 2])

    def test_two_large_tables_pipeline_end_to_end(self):
        # 验收：两个大表节点 + 汇总节点，落盘与整键引用下结果一致；可复跑
        spec = spec_of(
            [
                {"id": "big1", "op": "m24.big_table", "params": {"n": 5000}},
                {"id": "big2", "op": "m24.big_table", "params": {"n": 4000}},
                {
                    "id": "join",
                    "op": "m24.sum_two",
                    "inputs": {"a": "${big1.rows}", "b": "${big2.rows}"},
                },
            ],
            outputs={"total": "${join.total}"},
        )
        expected = 5000 * 4999 // 2 + 4000 * 3999 // 2
        with tempfile.TemporaryDirectory() as tmp:
            for _round in (1, 2):  # 可复跑：同一 workdir 两次运行各自成功
                store = self._store(threshold=100)
                report, values = run_pipeline(
                    spec, registry=default_registry, workdir=tmp, handle_store=store
                )
                self.assertEqual(report.status, "success")
                self.assertEqual(values["join.total"], expected, "落盘句柄经下游求和应与内存一致")
                self.assertEqual(values["big1.rows"][:3], [0, 1, 2])
                self.assertGreaterEqual(
                    len(self._spill_files(store)), 2, "两个大表句柄都应落盘"
                )

    def test_run_toplevel_passes_handle_store(self):
        store = self._store()
        with tempfile.TemporaryDirectory() as tmp:
            cfg = Path(tmp) / "pipeline.toml"
            cfg.write_text(
                'name = "m24"\n'
                "[[nodes]]\n"
                'id = "src"\n'
                'op = "control.const"\n'
                "[nodes.params]\n"
                "value = 7\n",
                encoding="utf-8",
            )
            _report, values = run(str(cfg), handle_store=store)
        self.assertEqual(values["src.value"], 7, "顶层 run() 应透传 handle_store")

    def test_concurrent_puts(self):
        errors: list[Exception] = []

        def writer(tid: int):
            try:
                with tempfile.TemporaryDirectory() as tmp:
                    store = self._store(threshold=64, spill_dir=tmp)
                    for i in range(50):
                        store.put(f"t{tid}.r{i}", list(range(100)))
                    for i in range(50):
                        assert store.get(f"t{tid}.r{i}") == list(range(100))
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [threading.Thread(target=writer, args=(t,)) for t in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(errors, [], "并发写读不应有异常")


# --------------------------------------------------------------------------- #
# M2-5：RunStore + SQLite 运行记录 + cache_key 可选源码摘要（T7 出债）
# --------------------------------------------------------------------------- #
@op("m25.const")
class M25ConstOp(Operator):
    """产出常量，验证运行记录与 RunReport 一致。"""

    outputs = [("value", int)]
    params = {"value": Param(int, default=1)}

    def run(self, ctx, value):
        return {"value": value}


class RunStoreTest(unittest.TestCase):
    """M2-5：跑完后 SQLite 可查询到与 RunReport 一致的节点状态/attempts；不传开关零副作用。"""

    def test_record_query_roundtrip(self):
        from op_pipeline.store import RunStore

        with tempfile.TemporaryDirectory() as tmp:
            store = RunStore(Path(tmp) / "p.db")
            store.record_run("r1", "demo", "1.2.0", "partial", 100.0, 200.0, {"n": 5})
            store.record_node("r1", "a", "control.const", "success", 1, False, None, "key-abc", 150.0)
            runs = store.query("SELECT run_id, params_json, status FROM runs")
            nodes = store.query("SELECT node_id, attempts, cache_key FROM nodes")
            store.close()
            self.assertEqual(runs[0][0], "r1")
            self.assertEqual(runs[0][1], '{"n": 5}', "params 应序列化为 JSON 存表")
            self.assertEqual(runs[0][2], "partial")
            self.assertEqual(nodes[0][0], "a")
            self.assertEqual(nodes[0][1], 1)
            self.assertEqual(nodes[0][2], "key-abc")

    def test_recorded_rows_match_report(self):
        from op_pipeline.store import RunStore

        spec = spec_of(
            [
                {"id": "src", "op": "m25.const", "params": {"value": 3}},
                {
                    "id": "retry",
                    "op": "testkit.flaky",
                    "params": {"fail_times": 1},
                    "inputs": {"value": "${src.value}"},
                    "retries": 1,
                    "retry_delay": 0.01,
                },
            ],
            outputs={"v": "${retry.value}"},
        )
        with tempfile.TemporaryDirectory() as tmp:
            store = RunStore(Path(tmp) / "runs.db")
            report, values = run_pipeline(
                spec, registry=default_registry, workdir=tmp, run_store=store
            )
            runs = store.query("SELECT run_id, pipeline, version, status FROM runs")
            nodes = store.query(
                "SELECT node_id, op, status, attempts FROM nodes ORDER BY node_id"
            )
            store.close()

            self.assertEqual(len(runs), 1, "每次运行应写入一条 runs 记录")
            run_row = runs[0]
            self.assertEqual(run_row[0], report.run_id)
            self.assertEqual(run_row[1], report.pipeline)
            self.assertEqual(run_row[2], report.version)
            self.assertEqual(run_row[3], report.status)

            self.assertEqual(len(nodes), 2, "每个节点应写入一条 nodes 记录")
            by_node = {row[0]: row for row in nodes}
            for result in report.nodes:
                row = by_node[result.node_id]
                self.assertEqual(row[1], result.op, "op 应与报告一致")
                self.assertEqual(row[2], result.status, "状态应与报告一致")
                self.assertEqual(row[3], result.attempts, "attempts 应与报告一致")
            self.assertEqual(values["retry.value"], 3, "重试节点最终应成功产出")

    def test_no_run_store_creates_no_db_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            spec = spec_of([{"id": "src", "op": "m25.const", "params": {"value": 1}}])
            _report, _values = run_pipeline(spec, registry=default_registry, workdir=tmp)
            self.assertEqual(
                list(Path(tmp).glob("*.db")), [], "不传 run_store 不应产生任何 db 文件"
            )

    def test_cli_runs_switch_writes_db(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = Path(tmp) / "pipe.toml"
            cfg.write_text(
                'name = "m25"\n'
                "[[nodes]]\n"
                'id = "src"\n'
                'op = "control.const"\n'
                "[nodes.params]\n"
                "value = 9\n",
                encoding="utf-8",
            )
            db = Path(tmp) / "runs.db"
            proc = DryRunNoSideEffectsTest._run_cli(
                "run", str(cfg), "--runs", str(db), "--no-cache", cwd=str(ROOT)
            )
            self.assertEqual(0, proc.returncode, proc.stderr)
            self.assertTrue(db.exists(), "--runs 开关应写出 SQLite 文件")
            import sqlite3
            conn = sqlite3.connect(db)
            rows = conn.execute("SELECT pipeline, status FROM runs").fetchall()
            conn.close()
            self.assertEqual(rows, [("m25", "success")], "runs 表应含本次运行记录")

            proc2 = DryRunNoSideEffectsTest._run_cli(
                "run", str(cfg), "--no-cache", cwd=str(ROOT)
            )
            self.assertEqual(0, proc2.returncode, proc2.stderr)
            self.assertEqual(
                sorted(p.name for p in Path(tmp).glob("*.db")),
                ["runs.db"],
                "不传 --runs 不应新增任何 db 文件",
            )

    def test_compute_key_source_hash_opt_in(self):
        from op_pipeline.cache import CacheStore

        default_cache = CacheStore(None)
        optin_cache = CacheStore(None, include_source=True)
        src_a = "class A:\n    pass\n"
        src_b = "class B:\n    pass\n"
        self.assertEqual(
            default_cache.compute_key("m25.srcdep", "1.0", {"a": 1}, {}, source=src_a),
            default_cache.compute_key("m25.srcdep", "1.0", {"a": 1}, {}, source=src_b),
            "默认关闭源码摘要时 source 不应影响 key",
        )
        k1 = optin_cache.compute_key("m25.srcdep", "1.0", {"a": 1}, {}, source=src_a)
        k2 = optin_cache.compute_key("m25.srcdep", "1.0", {"a": 1}, {}, source=src_b)
        self.assertNotEqual(k1, k2, "开启源码摘要后不同源码应得到不同 key")
        self.assertEqual(
            optin_cache.compute_key("m25.srcdep", "1.0", {"a": 1}, {}, source=src_a),
            k1,
            "相同源码应稳定复现同一 key",
        )

    def test_engine_feeds_source_when_opted_in(self):
        from op_pipeline.cache import CacheStore

        seen: list[str | None] = []

        class SpyCache(CacheStore):
            def compute_key(self, op_name, op_version, params, inputs, source=None):
                seen.append(source)
                return super().compute_key(op_name, op_version, params, inputs, source=source)

        spec = spec_of([{"id": "src", "op": "m25.const", "params": {"value": 5}, "cache": True}])
        with tempfile.TemporaryDirectory() as tmp:
            plan = build_plan(spec, default_registry)
            context = RunContext.create(pipeline="t", params={}, workdir=tmp)
            engine = Engine(plan, context, cache_store=SpyCache(tmp, include_source=True))
            engine.run()
        self.assertTrue(seen, "有缓存节点应计算 cache_key")
        self.assertTrue(
            all(isinstance(s, str) and "M25ConstOp" in s for s in seen),
            "开启 include_source 后应把算子源码传入 compute_key",
        )

    def test_engine_omits_source_by_default(self):
        from op_pipeline.cache import CacheStore

        seen: list[str | None] = []

        class SpyCache(CacheStore):
            def compute_key(self, op_name, op_version, params, inputs, source=None):
                seen.append(source)
                return super().compute_key(op_name, op_version, params, inputs, source=source)

        spec = spec_of([{"id": "src", "op": "m25.const", "params": {"value": 5}, "cache": True}])
        with tempfile.TemporaryDirectory() as tmp:
            plan = build_plan(spec, default_registry)
            context = RunContext.create(pipeline="t", params={}, workdir=tmp)
            engine = Engine(plan, context, cache_store=SpyCache(tmp))
            engine.run()
        self.assertTrue(seen)
        self.assertEqual(seen, [None], "默认关闭源码摘要时 source 应为 None")


# --------------------------------------------------------------------------- #
# M2-6：断点续跑（--resume <run_id>）——失败段与下游重跑，成功段 [resumed]
# --------------------------------------------------------------------------- #
@op("m26.stage1")
class M26Stage1(Operator):
    """首段：纯计算，产出常量（确定性，可缓存，续跑时复用）。"""

    outputs = [("value", int)]
    params = {"value": Param(int, default=5)}

    def run(self, ctx, value):
        return {"value": value}


@op("m26.stage2")
class M26Stage2(Operator):
    """第二段：门限校验，输入小于门限即失败（模拟"数据校验不过"）。"""

    inputs = [("data", int)]
    outputs = [("value", int)]
    params = {"threshold": Param(int, default=100)}

    def run(self, ctx, data, threshold):
        if data < threshold:
            raise ValueError(f"校验失败：{data} < {threshold}")
        return {"value": data}


@op("m26.sink")
class M26Sink(Operator):
    """第三段：下游，依赖第二段产出。"""

    inputs = [("data", int)]
    outputs = [("value", int)]

    def run(self, ctx, data):
        return {"value": data}


def _resume_spec():
    """三段流水线：s1 → s2（门限校验）→ s3；门限以流水线参数控制。"""
    return spec_of(
        [
            {"id": "s1", "op": "m26.stage1", "cache": True},
            {
                "id": "s2",
                "op": "m26.stage2",
                "params": {"threshold": "${p.threshold}"},
                "inputs": {"data": "${s1.value}"},
                "cache": True,
            },
            {"id": "s3", "op": "m26.sink", "inputs": {"data": "${s2.value}"}, "cache": True},
        ],
        params={"threshold": 100},
        outputs={"v": "${s3.value}"},
    )


class ResumeTest(unittest.TestCase):
    """M2-6：--resume 只重跑上次失败段与下游；成功段以 [resumed] 复用缓存不重跑。"""

    def test_resume_skips_succeeded_segment(self):
        from op_pipeline.store import RunStore

        spec = _resume_spec()
        with tempfile.TemporaryDirectory() as tmp:
            store = RunStore(Path(tmp) / "runs.db")
            cache_dir = str(Path(tmp) / "cache")
            try:
                # 首跑：s2 门限 100 校验失败 → 报告 failed，s3 跳过
                report1, _values1 = run_pipeline(
                    spec, registry=default_registry, workdir=tmp,
                    cache_dir=cache_dir, run_store=store,
                )
                self.assertEqual(report1.status, "failed")
                self.assertEqual(report1.node("s1").status, "success")
                self.assertEqual(report1.node("s2").status, "failed")
                self.assertEqual(report1.node("s3").status, "skipped")
                run_id1 = report1.run_id

                # 修数据（门限降到 4）后以 --resume 语义续跑
                report2, values2 = run_pipeline(
                    spec, registry=default_registry, workdir=tmp,
                    cache_dir=cache_dir, run_store=store,
                    overrides={"threshold": 4}, resume_run_id=run_id1,
                )
                self.assertEqual(report2.status, "success")
                self.assertTrue(report2.node("s1").resumed, "首段应标记 [resumed]")
                self.assertTrue(report2.node("s1").cached, "首段应复用缓存不重跑")
                self.assertEqual(report2.node("s2").status, "success", "失败段应重跑")
                self.assertEqual(report2.node("s3").status, "success", "下游应重跑")
                self.assertEqual(values2["s3.value"], 5)

                # 不带 resume 的普通重跑：缓存命中但不标记 resumed
                report3, _values3 = run_pipeline(
                    spec, registry=default_registry, workdir=tmp,
                    cache_dir=cache_dir, run_store=store,
                    overrides={"threshold": 4},
                )
                self.assertTrue(report3.node("s1").cached, "普通重跑仍应命中缓存")
                self.assertFalse(report3.node("s1").resumed, "未指定 resume 不应标记 [resumed]")
            finally:
                store.close()

    def test_resume_unknown_run_id_runs_normally(self):
        from op_pipeline.store import RunStore

        spec = _resume_spec()
        with tempfile.TemporaryDirectory() as tmp:
            store = RunStore(Path(tmp) / "runs.db")
            cache_dir = str(Path(tmp) / "cache")
            try:
                report1, _v = run_pipeline(
                    spec, registry=default_registry, workdir=tmp,
                    cache_dir=cache_dir, run_store=store,
                )
                self.assertEqual(report1.status, "failed")
                report2, _v = run_pipeline(
                    spec, registry=default_registry, workdir=tmp,
                    cache_dir=cache_dir, run_store=store,
                    overrides={"threshold": 4}, resume_run_id="no-such-run",
                )
                self.assertEqual(report2.status, "success")
                self.assertFalse(
                    report2.node("s1").resumed,
                    "找不到参考 run_id 时应退化为普通运行，不标记 [resumed]",
                )
                self.assertTrue(report2.node("s1").cached, "缓存命中仍是普通缓存语义")
            finally:
                store.close()

    def test_resume_requires_cache_entry(self):
        from op_pipeline.store import RunStore

        spec = _resume_spec()
        with tempfile.TemporaryDirectory() as tmp:
            store = RunStore(Path(tmp) / "runs.db")
            try:
                cache_dir1 = str(Path(tmp) / "cache1")
                report1, _v = run_pipeline(
                    spec, registry=default_registry, workdir=tmp,
                    cache_dir=cache_dir1, run_store=store,
                )
                self.assertEqual(report1.status, "failed")
                run_id1 = report1.run_id
                # 缓存目录更换（模拟缓存被清理）：key 一致但无缓存条目 → 不能 resume，必须重跑
                cache_dir2 = str(Path(tmp) / "cache2")
                report2, _v = run_pipeline(
                    spec, registry=default_registry, workdir=tmp,
                    cache_dir=cache_dir2, run_store=store,
                    overrides={"threshold": 4}, resume_run_id=run_id1,
                )
                self.assertEqual(report2.status, "success")
                self.assertFalse(report2.node("s1").resumed, "无缓存条目时不能 resume")
                self.assertFalse(report2.node("s1").cached, "缓存缺失时应重跑")
            finally:
                store.close()

    def test_cli_resume_requires_runs_db(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = Path(tmp) / "pipe.toml"
            cfg.write_text(
                'name = "m26"\n'
                "[[nodes]]\n"
                'id = "s1"\n'
                'op = "control.const"\n'
                "[nodes.params]\n"
                "value = 1\n",
                encoding="utf-8",
            )
            proc = DryRunNoSideEffectsTest._run_cli(
                "run", str(cfg), "--resume", "some-run-id", "--no-cache", cwd=str(ROOT)
            )
            self.assertNotEqual(0, proc.returncode, "--resume 不配合 --runs 应报错退出")
            self.assertIn("--runs", proc.stderr)


class _RecordingHook:
    """M2-7 测试钩子：按顺序收集事件序列。"""

    def __init__(self) -> None:
        self.events: list[tuple] = []

    def on_node_start(self, node_id, op):
        self.events.append(("start", node_id, op))

    def on_node_finish(self, node_id, status, duration, cached):
        self.events.append(("finish", node_id, status, cached))

    def on_run_finish(self, report):
        self.events.append(("run_finish", report.status))


class HookTest(unittest.TestCase):
    """M2-7：Engine(hooks=[...]) 执行事件钩子。"""

    def _linear_spec(self):
        return spec_of(
            [
                {"id": "src", "op": "control.const", "params": {"value": 1}},
                {"id": "h1", "op": "testkit.double", "params": {"times": 2},
                 "inputs": {"value": "${src.value}"}},
                {"id": "h2", "op": "testkit.double", "params": {"times": 3},
                 "inputs": {"value": "${h1.value}"}},
                {"id": "h3", "op": "testkit.double", "params": {"times": 4},
                 "inputs": {"value": "${h2.value}"}},
            ],
            outputs={"v": "${h3.value}"},
        )

    def test_hooks_follow_schedule_order(self):
        hook = _RecordingHook()
        report, values = run_pipeline(
            self._linear_spec(), registry=default_registry, hooks=[hook]
        )
        self.assertEqual(report.status, "success")
        self.assertEqual(values["h3.value"], 24)
        events = hook.events
        # on_run_finish 总是最后一个事件
        self.assertEqual(events[-1][0], "run_finish")
        self.assertEqual(events[-1][1], "success")
        # 每个真实执行的节点：start 先于 finish，且各恰好一次
        for node_id in ("src", "h1", "h2", "h3"):
            starts = [i for i, e in enumerate(events) if e[0] == "start" and e[1] == node_id]
            finishes = [i for i, e in enumerate(events) if e[0] == "finish" and e[1] == node_id]
            self.assertEqual(len(starts), 1, f"{node_id} 应恰好一次 start")
            self.assertEqual(len(finishes), 1, f"{node_id} 应恰好一次 finish")
            self.assertLess(starts[0], finishes[0], f"{node_id} 的 start 应先于 finish")
        # 调度顺序：上游 finish 先于下游 start（线性链 src→h1→h2→h3）
        for up, down in (("src", "h1"), ("h1", "h2"), ("h2", "h3")):
            up_finish = next(i for i, e in enumerate(events) if e[0] == "finish" and e[1] == up)
            down_start = next(i for i, e in enumerate(events) if e[0] == "start" and e[1] == down)
            self.assertLess(up_finish, down_start, f"{up} 的 finish 应先于 {down} 的 start")

    def test_hook_on_node_finish_fires_on_failure(self):
        hook = _RecordingHook()
        spec = spec_of(
            [
                {"id": "src", "op": "control.const", "params": {"value": 1}},
                {"id": "h1", "op": "testkit.boom", "inputs": {"value": "${src.value}"}},
                {"id": "h2", "op": "testkit.double", "params": {"times": 2},
                 "inputs": {"value": "${h1.value}"}},
            ],
            outputs={"v": "${h2.value}"},
        )
        report, _v = run_pipeline(spec, registry=default_registry, hooks=[hook])
        self.assertEqual(report.status, "failed")
        # 失败节点也触发 on_node_finish（status=failed）
        self.assertIn(("finish", "h1", "failed", False), hook.events)
        # 被跳过的下游不触发任何钩子（它从未开始执行）
        self.assertNotIn(("start", "h2", "testkit.double"), hook.events)
        self.assertNotIn(("finish", "h2", "skipped", False), hook.events)
        # 运行级事件仍在最后，且状态为 failed
        self.assertEqual(hook.events[-1][0], "run_finish")
        self.assertEqual(hook.events[-1][1], "failed")

    def test_hook_cached_node_reports_cached(self):
        hook = _RecordingHook()
        spec = spec_of(
            [
                {"id": "src", "op": "control.const", "params": {"value": 1}},
                {"id": "h1", "op": "testkit.double", "params": {"times": 2},
                 "inputs": {"value": "${src.value}"}, "cache": True},
            ],
            outputs={"v": "${h1.value}"},
        )
        with tempfile.TemporaryDirectory() as tmp:
            run_pipeline(spec, registry=default_registry, workdir=tmp,
                         cache_dir=str(Path(tmp) / "cache"), hooks=[hook])
            report, _v = run_pipeline(spec, registry=default_registry, workdir=tmp,
                                      cache_dir=str(Path(tmp) / "cache"), hooks=[hook])
        self.assertEqual(report.status, "success")
        self.assertTrue(report.node("h1").cached, "第二次运行应命中缓存")
        # 同一 hook 跨两次运行复用，取最后一次 finish（即第二次运行的）
        finish = [e for e in hook.events if e[0] == "finish" and e[1] == "h1"][-1]
        self.assertEqual(finish[2], "success")
        self.assertTrue(finish[3], "命中缓存的节点 on_node_finish 应带 cached=True")

    def test_hook_exception_does_not_break_pipeline(self):
        class BadHook:
            def on_node_finish(self, node_id, status, duration, cached):
                raise RuntimeError("钩子炸了")

        report, values = run_pipeline(
            self._linear_spec(), registry=default_registry, hooks=[BadHook()]
        )
        self.assertEqual(report.status, "success", "钩子异常不应影响流水线")
        self.assertEqual(values["h3.value"], 24)
        self.assertTrue(
            any("钩子炸了" in w for w in report.warnings),
            "钩子异常应记入报告告警",
        )

    def test_top_level_run_accepts_hooks(self):
        hook = _RecordingHook()
        with tempfile.TemporaryDirectory() as tmp:
            cfg = Path(tmp) / "pipe.toml"
            cfg.write_text(
                'name = "m27"\n'
                "[[nodes]]\n"
                'id = "s1"\n'
                'op = "control.const"\n'
                "[nodes.params]\n"
                "value = 7\n",
                encoding="utf-8",
            )
            report, values = run(str(cfg), hooks=[hook])
        self.assertEqual(report.status, "success")
        self.assertEqual(values["s1.value"], 7)
        self.assertIn(("run_finish", "success"), hook.events)


class AsyncHostTest(unittest.TestCase):
    """M2-8：run_pipeline_async 在 async 宿主内 await，且不阻塞事件循环。"""

    @staticmethod
    def _slow_spec():
        return spec_of(
            [
                {"id": "src", "op": "control.const", "params": {"value": 1}},
                {"id": "slow", "op": "testkit.sleep", "params": {"seconds": 0.3},
                 "inputs": {"value": "${src.value}"}},
                {"id": "out", "op": "testkit.double", "params": {"times": 24},
                 "inputs": {"value": "${slow.value}"}},
            ],
            outputs={"v": "${out.value}"},
        )

    def test_await_in_running_loop_returns_report(self):
        from op_pipeline import run_pipeline_async

        async def main():
            report, values = await run_pipeline_async(
                self._slow_spec(), registry=default_registry
            )
            return report, values

        report, values = asyncio.run(main())
        self.assertEqual(report.status, "success")
        self.assertEqual(values["out.value"], 24)
        self.assertEqual(report.outputs["v"], 24)

    def test_other_async_tasks_advance_while_pipeline_runs(self):
        from op_pipeline import run_pipeline_async

        async def main():
            ticks = 0

            async def ticker():
                nonlocal ticks
                while True:
                    await asyncio.sleep(0.01)
                    ticks += 1

            task = asyncio.create_task(ticker())
            try:
                report, _v = await run_pipeline_async(
                    self._slow_spec(), registry=default_registry
                )
            finally:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
            return report, ticks

        report, ticks = asyncio.run(main())
        self.assertEqual(report.status, "success")
        self.assertGreaterEqual(ticks, 3, "流水线运行期间事件循环应持续推进")


if __name__ == "__main__":
    unittest.main(verbosity=2)
