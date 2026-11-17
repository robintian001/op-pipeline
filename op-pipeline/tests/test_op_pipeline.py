"""框架自测（标准库 unittest，零第三方依赖）。

运行：``python -m unittest discover -s tests -v``
"""
from __future__ import annotations

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
from op_pipeline.spec import build_plan  # noqa: E402
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


if __name__ == "__main__":
    unittest.main(verbosity=2)
