"""M6-3 Task 9：按异常类型重试 ``retry_on``。

语义：
- ``retry_on`` 为空（默认）→ 现状：普通异常按 ``retries`` 重试；
- ``retry_on`` 非空 → 仅当 ``type(exc).__name__`` 在该集合内才重试，
  未命中立即失败（attempts=1，error/traceback 照常保留）；
- 配置类错误（``ConfigError`` 及其子类，如 ``ParamValidationError``）永不重试
  ——即使 ``retry_on`` 显式包含其类型名也一样。
"""
import sys
import tempfile
import threading
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from op_pipeline import ConfigError, Operator, Param, PipelineSpec, default_registry, run_pipeline
from op_pipeline.registry import op as register_op

# --------------------------------------------------------------------------- #
# 不稳定算子：前 fail_times 次抛指定异常，之后成功
# --------------------------------------------------------------------------- #
_ATTEMPT_LOCK = threading.Lock()
_ATTEMPTS: dict[str, int] = {}


def _reset_attempts() -> None:
    with _ATTEMPT_LOCK:
        _ATTEMPTS.clear()


@register_op("m6.flaky", replace=True)
class FlakyOp(Operator):
    """第 1..fail_times 次调用抛 ``error_type`` 指定异常，之后返回尝试次数。"""

    description = "按次数失败的测试算子"
    outputs = [("value", int)]
    params = {
        "fail_times": Param(int, default=1),
        "error_type": Param(str, default="ValueError"),
    }

    def run(self, ctx, fail_times, error_type):
        with _ATTEMPT_LOCK:
            attempt = _ATTEMPTS.get(ctx.node_id, 0) + 1
            _ATTEMPTS[ctx.node_id] = attempt
        if attempt <= int(fail_times):
            if error_type == "ConfigError":
                raise ConfigError("故意配置错误")
            raise ValueError("故意值错误")
        return {"value": attempt}


def _spec(node_extra=None, *, retry_on=None, retries=2, fail_times=1,
          error_type="ValueError"):
    node = {"id": "flaky", "op": "m6.flaky", "retries": retries,
            "params": {"fail_times": fail_times, "error_type": error_type}}
    if retry_on is not None:
        node["retry_on"] = list(retry_on)
    node.update(node_extra or {})
    return PipelineSpec.from_dict(
        {"name": "retry", "params": {}, "nodes": [node],
         "outputs": {"final": "${flaky.value}"}}
    )


def _run(cfg, tmp):
    return run_pipeline(
        cfg, registry=default_registry, workdir=tmp, cache_dir=None
    )


class RetryOnEngineTest(unittest.TestCase):
    def setUp(self):
        _reset_attempts()

    def test_matching_type_retries_then_succeeds(self):
        """命中 ValueError 且 retries=2：第 2 次成功，attempts=2。"""
        with tempfile.TemporaryDirectory() as tmp:
            report, values = _run(_spec(retry_on=["ValueError"]), tmp)
        self.assertEqual(report.status, "success")
        self.assertEqual(report.node("flaky").attempts, 2)
        self.assertEqual(values["flaky.value"], 2)

    def test_non_matching_type_fails_immediately(self):
        """未命中（配置 KeyError，实际抛 ValueError）：不重试，attempts=1。"""
        with tempfile.TemporaryDirectory() as tmp:
            report, _ = _run(_spec(retry_on=["KeyError"]), tmp)
        node = report.node("flaky")
        self.assertEqual(report.status, "failed")
        self.assertEqual(node.attempts, 1)
        self.assertIn("ValueError", node.error or "")
        self.assertTrue(node.traceback)

    def test_empty_retry_on_keeps_legacy_behavior(self):
        """retry_on 为空：与现状一致（ValueError 重试到成功）。"""
        with tempfile.TemporaryDirectory() as tmp:
            report, _ = _run(_spec(retry_on=None), tmp)
        self.assertEqual(report.status, "success")
        self.assertEqual(report.node("flaky").attempts, 2)

    def test_config_error_never_retried(self):
        """配置类错误永不重试——即使 retry_on 显式包含 ConfigError。"""
        with tempfile.TemporaryDirectory() as tmp:
            report, _ = _run(
                _spec(retry_on=["ConfigError"], error_type="ConfigError"), tmp
            )
        node = report.node("flaky")
        self.assertEqual(report.status, "failed")
        self.assertEqual(node.attempts, 1)
        self.assertIn("ConfigError", node.error or "")

    def test_retry_on_exhausted(self):
        """命中但重试耗尽：attempts = retries+1，状态 failed。"""
        with tempfile.TemporaryDirectory() as tmp:
            report, _ = _run(
                _spec(retry_on=["ValueError"], fail_times=99), tmp
            )
        self.assertEqual(report.status, "failed")
        self.assertEqual(report.node("flaky").attempts, 3)


class RetryOnSpecTest(unittest.TestCase):
    def test_parse_and_serialize(self):
        """list/tuple of str 均可解析；序列化为 list；空元组序列化为 []。"""
        cfg = _spec(retry_on=["ValueError", "TimeoutError"])
        self.assertEqual(
            cfg.nodes[0].retry_on, ("ValueError", "TimeoutError")
        )
        self.assertEqual(
            cfg.to_dict()["nodes"][0]["retry_on"], ["ValueError", "TimeoutError"]
        )
        self.assertEqual(_spec() .nodes[0].retry_on, ())
        self.assertEqual(_spec().to_dict()["nodes"][0]["retry_on"], [])

        tuple_cfg = PipelineSpec.from_dict(
            {"name": "t", "nodes": [
                {"id": "a", "op": "m6.flaky", "retry_on": ("ValueError",)}
            ]}
        )
        self.assertEqual(tuple_cfg.nodes[0].retry_on, ("ValueError",))

    def test_single_string_rejected(self):
        """单个字符串不是合法形态（要求明确的列表），报 ConfigError。"""
        with self.assertRaises(ConfigError):
            PipelineSpec.from_dict(
                {"name": "t", "nodes": [
                    {"id": "a", "op": "m6.flaky", "retry_on": "ValueError"}
                ]}
            )

    def test_non_string_element_rejected(self):
        with self.assertRaises(ConfigError):
            PipelineSpec.from_dict(
                {"name": "t", "nodes": [
                    {"id": "a", "op": "m6.flaky", "retry_on": ["ValueError", 3]}
                ]}
            )


if __name__ == "__main__":
    unittest.main()
