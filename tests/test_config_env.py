"""M7-2 Task 6：``${env:...}`` 解析期注入与运行报告打码。

覆盖：有值 / 默认值 / 缺失无默认（SpecValidationError 含变量名与节点 id）/
多占位符 / 未闭合与非法变量名 / 旧 ``${env.NAME}`` 不受影响 / 敏感值打码。
环境变量用 ``mock.patch.dict`` 临时注入，结束后自动还原。
"""
from __future__ import annotations

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
    build_plan,
    default_registry,
    run_pipeline,
)


def _spec(params=None, nodes=None):
    return PipelineSpec.from_dict(
        {
            "name": "env_test",
            "params": params or {},
            "nodes": nodes
            if nodes is not None
            else [{"id": "src", "op": "control.const", "params": {"value": 1}}],
        }
    )


class ConfigEnvSubstitutionTest(unittest.TestCase):
    def test_value_replaced_in_top_level_param(self):
        with mock.patch.dict(os.environ, {"M72_DIR": "/data"}, clear=False):
            spec = _spec(params={"path": "${env:M72_DIR}/in.csv"})
        self.assertEqual("/data/in.csv", spec.params["path"])
        self.assertEqual((), spec.sensitive_values)

    def test_default_used_when_missing(self):
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("M72_MISSING", None)
            spec = _spec(
                params={
                    "path": "${env:M72_MISSING:-fallback.csv}",
                    "empty": "${env:M72_MISSING:-}",
                }
            )
        self.assertEqual("fallback.csv", spec.params["path"])
        self.assertEqual("", spec.params["empty"])

    def test_missing_without_default_raises_with_var_and_node_id(self):
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("M72_SECRET_DIR", None)
            with self.assertRaises(SpecValidationError) as ctx:
                _spec(
                    nodes=[
                        {
                            "id": "load",
                            "op": "control.const",
                            "params": {"value": "${env:M72_SECRET_DIR}"},
                        }
                    ]
                )
        message = str(ctx.exception)
        self.assertIn("M72_SECRET_DIR", message)
        self.assertIn("[load]", message)

    def test_multiple_placeholders_in_one_string(self):
        with mock.patch.dict(
            os.environ, {"M72_A": "alpha", "M72_B": "beta"}, clear=False
        ):
            spec = _spec(
                params={"text": "pre-${env:M72_A}-${env:M72_B}-post"}
            )
        self.assertEqual("pre-alpha-beta-post", spec.params["text"])

    def test_unclosed_placeholder_raises(self):
        with mock.patch.dict(os.environ, {"M72_A": "a"}, clear=False):
            with self.assertRaises(SpecValidationError) as ctx:
                _spec(params={"text": "${env:M72_A"})
        message = str(ctx.exception)
        self.assertTrue("语法错误" in message or "未闭合" in message, message)

    def test_invalid_variable_name_raises(self):
        with mock.patch.dict(os.environ, {}, clear=False):
            with self.assertRaises(SpecValidationError) as ctx:
                _spec(params={"text": "${env:1BAD}"})
        self.assertIn("1BAD", str(ctx.exception))

    def test_nested_and_non_string_values(self):
        with mock.patch.dict(os.environ, {"M72_NESTED": "x"}, clear=False):
            spec = _spec(
                nodes=[
                    {
                        "id": "src",
                        "op": "control.const",
                        "params": {
                            "value": {
                                "items": ["${env:M72_NESTED}", 3],
                                "flag": True,
                            }
                        },
                    }
                ]
            )
        value = spec.nodes[0].params["value"]
        self.assertEqual(["x", 3], value["items"])
        self.assertIs(True, value["flag"])

    def test_old_dot_syntax_untouched_by_spec(self):
        """既有 ``${env.NAME}`` 仍是运行期引用，解析期不替换。"""
        with mock.patch.dict(os.environ, {"M72_OLD": "old-value"}, clear=False):
            spec = _spec(params={"v": "${env.M72_OLD}"})
            self.assertEqual("${env.M72_OLD}", spec.params["v"])
            plan = build_plan(
                spec, default_registry, env={"M72_OLD": "old-value"}
            )
        self.assertEqual("old-value", plan.params["v"])

    def test_old_dot_syntax_resolves_at_run_time(self):
        with mock.patch.dict(os.environ, {"M72_OLD": "runtime"}, clear=False):
            spec = _spec(
                params={"v": "${env.M72_OLD}"},
                nodes=[
                    {
                        "id": "src",
                        "op": "control.const",
                        "params": {"value": "${p.v}"},
                    }
                ],
            )
            with tempfile.TemporaryDirectory() as tmp:
                report, values = run_pipeline(
                    spec,
                    registry=default_registry,
                    workdir=tmp,
                    cache_dir=None,
                )
        self.assertEqual("success", report.status)
        self.assertEqual("runtime", values["src.value"])


class ReportMaskingTest(unittest.TestCase):
    def test_sensitive_env_value_recorded_and_masked(self):
        with mock.patch.dict(
            os.environ, {"M72_API_TOKEN": "s3cr3t-value"}, clear=False
        ):
            spec = _spec(
                params={"auth": "${env:M72_API_TOKEN}", "user": "bob"},
                nodes=[
                    {
                        "id": "src",
                        "op": "control.const",
                        "params": {"value": "${p.auth}"},
                    }
                ],
            )
        self.assertEqual(("s3cr3t-value",), spec.sensitive_values)

        plan = build_plan(spec, default_registry)
        # 执行期真实参数不受影响
        self.assertEqual("s3cr3t-value", plan.params["auth"])

        with tempfile.TemporaryDirectory() as tmp:
            report, values = run_pipeline(
                spec, registry=default_registry, workdir=tmp, cache_dir=None
            )
        self.assertEqual("***", report.params["auth"])
        self.assertEqual("bob", report.params["user"])
        self.assertEqual("s3cr3t-value", values["src.value"])

    def test_sensitive_key_name_masked(self):
        spec = _spec(params={"API_KEY": "plain-value", "user": "bob"})
        with tempfile.TemporaryDirectory() as tmp:
            report, _values = run_pipeline(
                spec, registry=default_registry, workdir=tmp, cache_dir=None
            )
        self.assertEqual("***", report.params["API_KEY"])
        self.assertEqual("bob", report.params["user"])

    def test_sensitive_default_value_masked(self):
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("M72_DB_PASSWORD", None)
            spec = _spec(params={"auth": "${env:M72_DB_PASSWORD:-fallback-pass}"})
        self.assertEqual(("fallback-pass",), spec.sensitive_values)
        with tempfile.TemporaryDirectory() as tmp:
            report, _values = run_pipeline(
                spec, registry=default_registry, workdir=tmp, cache_dir=None
            )
        self.assertEqual("***", report.params["auth"])


if __name__ == "__main__":
    unittest.main()
