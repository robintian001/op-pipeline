# -*- coding: utf-8 -*-
"""M5-4 体验与技术债收口（Task 9 / Task 10）测试。

- Task 9：``CacheStore.include_source`` 默认开启——算子源码变更后
  以默认参数重跑会自动缓存失效（``cached=false``）；显式传
  ``include_source=False`` 仍保持旧行为（兼容不变）。
- Task 10：CLI ``run`` 失败节点在输出中展示截断堆栈；``ops --json``
  输出契约完整性核对。

全部用例不依赖改磁盘文件、不依赖网络与环境变量，只使用临时目录。
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from op_pipeline import Operator, Param, default_registry, op, run_pipeline
from op_pipeline.cache import CacheStore
from op_pipeline.spec import PipelineSpec, build_plan

ROOT = Path(__file__).resolve().parents[1]


# --------------------------------------------------------------------------- #
# Task 9 测试算子：同名两种源码实现（模块级定义，保证 inspect.getsource 可区分）
# --------------------------------------------------------------------------- #
@op("m5.srcdep", replace=True)
class M5SrcDepV1(Operator):
    """源码 V1：产出常量 1（确定性、可缓存）。"""

    outputs = [("value", int)]
    params = {"value": Param(int, default=1)}

    def run(self, ctx, value):
        return {"value": value}


@op("m5.srcdep", replace=True)
class M5SrcDepV2(Operator):
    """源码 V2：产出常量 2（与 V1 类源码不同，模拟算子源码变更）。"""

    outputs = [("value", int)]
    params = {"value": Param(int, default=2)}

    def run(self, ctx, value):
        return {"value": value + 100}


def _srcdep_spec() -> PipelineSpec:
    """单节点确定性流水线：节点开缓存，参数用默认值。"""
    return PipelineSpec.from_dict(
        {
            "name": "m5-srcdep",
            "params": {},
            "nodes": [{"id": "src", "op": "m5.srcdep", "cache": True}],
            "outputs": {},
        }
    )


class M5CacheExperienceTest(unittest.TestCase):
    """Task 9：include_source 默认化——源码变更后默认参数重跑自动缓存失效。"""

    def test_default_include_source_enabled(self):
        from op_pipeline.cache import CacheStore as CS

        self.assertTrue(CS(None).include_source, "默认应开启算子源码指纹")
        self.assertFalse(
            CS(None, include_source=False).include_source,
            "显式 include_source=False 仍可关闭（兼容不变）",
        )

    def test_default_cache_invalidates_on_source_change(self):
        """确定性算子默认参数跑两次第二次 cached=true；源码变更后重跑 cached=false。"""
        default_registry.register(M5SrcDepV1, "m5.srcdep", replace=True)
        spec = _srcdep_spec()
        with tempfile.TemporaryDirectory() as tmp:
            cache_dir = Path(tmp) / "cache"
            report1, _ = run_pipeline(
                spec, registry=default_registry, workdir=tmp, cache_dir=cache_dir
            )
            self.assertEqual("success", report1.status)
            self.assertFalse(report1.node("src").cached, "首次运行应执行并写入缓存")

            report2, _ = run_pipeline(
                spec, registry=default_registry, workdir=tmp, cache_dir=cache_dir
            )
            self.assertTrue(report2.node("src").cached, "默认参数重跑应命中缓存")

            # 模拟算子源码变更：同名字重新注册不同实现（模块级类源码不同）
            default_registry.register(M5SrcDepV2, "m5.srcdep", replace=True)
            report3, _ = run_pipeline(
                spec, registry=default_registry, workdir=tmp, cache_dir=cache_dir
            )
            self.assertFalse(
                report3.node("src").cached, "源码变更后默认参数重跑应缓存失效"
            )

    def test_explicit_false_keeps_old_behavior(self):
        """显式 include_source=False：源码变更不影响缓存命中（旧默认行为兼容）。"""
        from op_pipeline.context import RunContext
        from op_pipeline.engine import Engine

        default_registry.register(M5SrcDepV1, "m5.srcdep", replace=True)
        spec = _srcdep_spec()
        with tempfile.TemporaryDirectory() as tmp:
            cache = CacheStore(Path(tmp) / "cache", include_source=False)
            plan1 = build_plan(spec, default_registry)
            ctx1 = RunContext.create(pipeline="t", params=plan1.params, workdir=tmp)
            Engine(plan1, ctx1, cache_store=cache).run()

            # 源码变更后重跑：显式关闭源码指纹时仍命中缓存
            default_registry.register(M5SrcDepV2, "m5.srcdep", replace=True)
            plan2 = build_plan(spec, default_registry)
            ctx2 = RunContext.create(pipeline="t", params=plan2.params, workdir=tmp)
            report = Engine(plan2, ctx2, cache_store=cache).run()
            self.assertTrue(
                report.node("src").cached, "显式关闭源码指纹后源码变化不应失效"
            )


class M5CliExperienceTest(unittest.TestCase):
    """Task 10：CLI 打磨——失败节点 traceback 展示与 ops --json 完整性。"""

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
            env=M5CliExperienceTest.ENV,
            cwd=cwd,
            timeout=120,
        )

    def test_ops_json_contract_complete(self):
        """ops --json：包含全部内置关键算子且每条契约字段结构完整。"""
        proc = self._run_cli("ops", "--json", cwd=str(ROOT))
        self.assertEqual(0, proc.returncode, proc.stderr)
        payload = json.loads(proc.stdout)
        self.assertIsInstance(payload, list)
        self.assertGreater(len(payload), 20, "内置算子数量应超过 20 个")
        names = {item["name"] for item in payload}
        for op_name in (
            "db.sqlite_exec",
            "db.sqlite_query",
            "io.read_csv",
            "report.markdown",
            "control.const",
            "stats.aggregate",
        ):
            self.assertIn(op_name, names, f"内置算子 {op_name} 应在 ops --json 中列出")
        required_fields = {
            "name",
            "version",
            "description",
            "module",
            "deterministic",
            "inputs",
            "outputs",
            "params",
        }
        for item in payload:
            self.assertTrue(
                required_fields <= set(item), f"{item.get('name')} 缺少契约字段"
            )
            for port_key in ("inputs", "outputs"):
                self.assertIsInstance(item[port_key], list, "端口应为列表")
                for port in item[port_key]:
                    self.assertIn("name", port, "端口应有 name")
                    self.assertIn("type", port, "端口应有 type")
                    self.assertIn("optional", port, "端口应有 optional")
            self.assertIsInstance(item["params"], dict, "params 应为映射")
            for pname, pmeta in item["params"].items():
                self.assertIn("type", pmeta, f"参数 {pname} 应有 type")
                self.assertIn("required", pmeta, f"参数 {pname} 应有 required")

    def test_cli_run_shows_failed_node_traceback(self):
        """run 失败节点：CLI 输出展示截断堆栈（含 Traceback 标记与异常类型）。"""
        with tempfile.TemporaryDirectory() as tmp:
            cfg = Path(tmp) / "fail.toml"
            cfg.write_text(
                'name = "m5-fail"\n'
                "[[nodes]]\n"
                'id = "gate"\n'
                'op = "control.const"\n'
                "[nodes.params]\n"
                "value = 1\n"
                "\n"
                "[[nodes]]\n"
                'id = "check"\n'
                'op = "control.assert"\n'
                'inputs = { value = "${gate.value}" }\n'
                "[nodes.params]\n"
                'that = "value > 10"\n',
                encoding="utf-8",
            )
            proc = self._run_cli("run", str(cfg), "--no-cache", cwd=str(ROOT))
            self.assertEqual(1, proc.returncode, proc.stderr)
            self.assertIn(
                "Traceback (most recent call last)",
                proc.stdout,
                "失败节点截断堆栈应展示在 CLI 输出",
            )
            self.assertIn("AssertionError", proc.stdout, "堆栈应含异常类型")
            self.assertIn("[check]", proc.stdout, "堆栈块应标注失败节点")


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
