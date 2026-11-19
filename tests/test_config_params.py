"""M8-2 Task 1：``${param:...}`` 运行级参数注入。

覆盖：有值 / 默认值 / 缺失报错（含变量名与节点 id）/ 同一字符串多占位符 /
语法错误（未闭合、空名）/ CLI ``--param`` 透传 / 敏感键打码 /
``${param:}`` 与 ``${env:}`` 混用互不干扰 / 替换发生在解析期。
"""
from __future__ import annotations

import contextlib
import io
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from op_pipeline import (  # noqa: E402
    PipelineSpec,
    SpecValidationError,
    default_registry,
    run_pipeline,
)
from op_pipeline.cli import main as cli_main  # noqa: E402


def _load(nodes, params=None, injected=None):
    """构造最小 PipelineSpec（param 占位符写在节点 params 里）。"""
    return PipelineSpec.from_dict(
        {
            "name": "param_test",
            "params": params or {},
            "nodes": nodes,
        },
        params=injected,
    )


class ParamPlaceholderTests(unittest.TestCase):
    """解析期替换语义。"""

    def test_injected_value_replaces_placeholder(self):
        """有值：注入的运行级参数在解析期替换占位符。"""
        spec = _load(
            [{"id": "src", "op": "control.const",
              "params": {"value": "${param:env_name}"}}],
            injected={"env_name": "prod"},
        )
        self.assertEqual(spec.nodes[0].params["value"], "prod")

    def test_whole_placeholder_preserves_type(self):
        """整串恰好一个占位符时保留参数值原类型。"""
        spec = _load(
            [{"id": "src", "op": "control.const",
              "params": {"value": "${param:n}"}}],
            injected={"n": 5},
        )
        self.assertEqual(spec.nodes[0].params["value"], 5)
        self.assertIsInstance(spec.nodes[0].params["value"], int)

    def test_config_params_section_serves_as_lookup(self):
        """未注入时可用配置 [params] 段的值（注入优先）。"""
        spec = _load(
            [{"id": "src", "op": "control.const",
              "params": {"value": "${param:env_name}"}}],
            params={"env_name": "qa"},
        )
        self.assertEqual(spec.nodes[0].params["value"], "qa")
        spec = _load(
            [{"id": "src", "op": "control.const",
              "params": {"value": "${param:env_name}"}}],
            params={"env_name": "qa"},
            injected={"env_name": "prod"},
        )
        self.assertEqual(spec.nodes[0].params["value"], "prod")

    def test_default_used_when_missing(self):
        """默认值：``${param:NAME:-default}`` 缺失注入时用默认值。"""
        spec = _load(
            [{"id": "src", "op": "control.const",
              "params": {"value": "${param:env:-dev}"}}],
        )
        self.assertEqual(spec.nodes[0].params["value"], "dev")
        spec = _load(
            [{"id": "src", "op": "control.const",
              "params": {"value": "${param:env:-}"}}],
        )
        self.assertEqual(spec.nodes[0].params["value"], "")

    def test_missing_without_default_raises_with_name_and_node_id(self):
        """缺失且无默认：SpecValidationError 含变量名与节点 id。"""
        with self.assertRaises(SpecValidationError) as ctx:
            _load(
                [{"id": "load", "op": "control.const",
                  "params": {"value": "${param:env_name}"}}],
            )
        message = "\n".join(ctx.exception.problems)
        self.assertIn("env_name", message)
        self.assertIn("[load]", message)

    def test_missing_in_top_level_params_reports_param_key(self):
        """顶层 params 段占位符缺失时报错含变量名与参数键。"""
        with self.assertRaises(SpecValidationError) as ctx:
            _load(
                [{"id": "src", "op": "control.const", "params": {"value": 1}}],
                params={"path": "${param:nowhere}"},
            )
        message = "\n".join(ctx.exception.problems)
        self.assertIn("nowhere", message)
        self.assertIn("流水线参数", message)

    def test_multiple_placeholders_in_one_string(self):
        """同一字符串可混合多个占位符。"""
        spec = _load(
            [{"id": "src", "op": "control.const",
              "params": {"value": "pre-${param:a}-${param:b}-post"}}],
            injected={"a": 1, "b": 2},
        )
        self.assertEqual(spec.nodes[0].params["value"], "pre-1-2-post")

    def test_nested_structure_replaced_recursively(self):
        """嵌套结构（dict/list）递归替换。"""
        spec = _load(
            [{"id": "src", "op": "control.const",
              "params": {"value": {
                  "items": ["${param:a}", 3],
                  "inner": {"deep": "${param:b:-none}"},
              }}}],
            injected={"a": "x"},
        )
        value = spec.nodes[0].params["value"]
        self.assertEqual(value["items"][0], "x")
        self.assertEqual(value["inner"]["deep"], "none")

    def test_unclosed_placeholder_raises(self):
        """语法错误：未闭合占位符报 SpecValidationError。"""
        with self.assertRaises(SpecValidationError):
            _load(
                [{"id": "src", "op": "control.const",
                  "params": {"value": "${param:a"}}],
                injected={"a": 1},
            )

    def test_empty_name_raises(self):
        """语法错误：空变量名（含只写默认值的 ``${param:-x}``）报错。"""
        for bad in ("${param:}", "${param:-x}"):
            with self.assertRaises(SpecValidationError):
                _load(
                    [{"id": "src", "op": "control.const",
                      "params": {"value": bad}}],
                    injected={"a": 1},
                )

    def test_env_and_param_mix_in_one_string(self):
        """``${env:}`` 与 ``${param:}`` 混用互不干扰。"""
        with mock.patch.dict(os.environ, {"M82_MIX_ENV": "e"}):
            spec = _load(
                [{"id": "src", "op": "control.const",
                  "params": {"value": "pre-${env:M82_MIX_ENV}-${param:p}-post"}}],
                injected={"p": "v"},
            )
        self.assertEqual(spec.nodes[0].params["value"], "pre-e-v-post")

    def test_substitution_happens_at_parse_time(self):
        """替换发生在解析期：spec 构造完即生效，且注入键合并进顶层 params。"""
        spec = _load(
            [{"id": "src", "op": "control.const",
              "params": {"value": "${param:date}"}}],
            params={"env": "qa"},
            injected={"date": "2026-11-19"},
        )
        # 未运行任何节点，占位符已消失
        self.assertEqual(spec.nodes[0].params["value"], "2026-11-19")
        # 注入参数合并进顶层 params（注入优先），build_plan/报告可见
        self.assertEqual(spec.params["date"], "2026-11-19")
        self.assertEqual(spec.params["env"], "qa")


class ParamRunTests(unittest.TestCase):
    """运行路径（run_pipeline / CLI）上的注入行为。"""

    def test_run_pipeline_uses_injected_value(self):
        """注入值进入数据流与报告 params。"""
        report, values = run_pipeline(
            _load(
                [{"id": "src", "op": "control.const",
                  "params": {"value": "${param:greeting}"}}],
                injected={"greeting": "你好"},
            ),
            registry=default_registry,
            cache_dir=None,
        )
        self.assertEqual(report.status, "success")
        self.assertEqual(values["src.value"], "你好")
        self.assertEqual(report.params["greeting"], "你好")

    def test_sensitive_key_masked_in_report(self):
        """敏感键打码沿用 M7-2 机制：报告中显示 ***。"""
        report, values = run_pipeline(
            _load(
                [{"id": "src", "op": "control.const",
                  "params": {"value": "${param:MY_TOKEN}"}}],
                injected={"MY_TOKEN": "s3cr3t-value"},
            ),
            registry=default_registry,
            cache_dir=None,
        )
        self.assertEqual(values["src.value"], "s3cr3t-value")
        self.assertEqual(report.params["MY_TOKEN"], "***")

    def test_cli_run_passes_param(self):
        """CLI 透传：``run --param KEY=VALUE`` 注入 ``${param:KEY}``。"""
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            cfg = tmp_path / "pipeline.toml"
            cfg.write_text(
                "\n".join(
                    [
                        'name = "cli_param"',
                        "[[nodes]]",
                        'id = "src"',
                        'op = "control.const"',
                        "[nodes.params]",
                        'value = "${param:sample_text}"',
                        "[outputs]",
                        'out = "${src.value}"',
                    ]
                ),
                encoding="utf-8",
            )
            report_path = tmp_path / "report.json"
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                code = cli_main(
                    [
                        "run", str(cfg),
                        "--param", "sample_text=hello",
                        "--json", str(report_path),
                        "--no-cache",
                    ]
                )
            self.assertEqual(code, 0)
            data = json.loads(report_path.read_text(encoding="utf-8"))
            self.assertEqual(data["status"], "success")
            self.assertEqual(data["outputs"]["out"], "hello")
            self.assertEqual(data["params"]["sample_text"], "hello")

    def test_cli_param_defaults_to_raw_string(self):
        """CLI 值先尝试 json.loads，失败按原字符串。"""
        spec = PipelineSpec.from_dict(
            {"nodes": [{"id": "src", "op": "control.const",
                        "params": {"value": "${param:v}"}}]},
            params={"v": "not-json{"},
        )
        self.assertEqual(spec.nodes[0].params["value"], "not-json{")


if __name__ == "__main__":
    unittest.main()
