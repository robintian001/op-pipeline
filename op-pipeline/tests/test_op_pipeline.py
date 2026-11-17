"""框架自测（标准库 unittest，零第三方依赖）。

运行：``python -m unittest discover -s tests -v``
"""
from __future__ import annotations

import json
import sys
import tempfile
import time
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
    run_pipeline,
)
from op_pipeline.expression import ExpressionError  # noqa: E402
from op_pipeline.spec import build_plan  # noqa: E402


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


if __name__ == "__main__":
    unittest.main(verbosity=2)
