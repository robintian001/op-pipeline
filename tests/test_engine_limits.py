"""M6-3 Task 8：节点并发分组限流（``concurrency_group`` + ``concurrency_limits``）。

语义：
- 节点声明 ``concurrency_group``（非空字符串）且运行配置了该组的正整数上限时，
  同一组内的节点最多只有 N 个真正在执行；acquire 发生在工作线程内（缓存判定
  之后、实际执行之前），因此不会阻塞调度线程；
- 未声明组、或声明了组但未配置限制 → 不加锁，行为与 v0.6.0 完全一致；
- 非法限制值（0 / 负数 / 非整数）在 ``Engine`` 构造期抛 ``ConfigError``。
"""
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from op_pipeline import ConfigError, Operator, Param, PipelineSpec, default_registry, run_pipeline
from op_pipeline.registry import op as register_op

# --------------------------------------------------------------------------- #
# 并发探针算子：模块级计数器记录"同时执行"的最大值
# --------------------------------------------------------------------------- #
_COUNTER_LOCK = threading.Lock()
_current = 0
_peak = 0


def _reset_counter() -> None:
    global _current, _peak
    with _COUNTER_LOCK:
        _current = 0
        _peak = 0


@register_op("m6.concurrency_probe", replace=True)
class ConcurrencyProbe(Operator):
    """记录并发峰值；``fail=True`` 时先计数再抛错（验证异常路径也会释放配额）。"""

    description = "并发探针"
    outputs = [("value", int)]
    params = {
        "seconds": Param(float, default=0.2),
        "fail": Param(bool, default=False),
    }

    def run(self, ctx, seconds, fail):
        global _current, _peak
        with _COUNTER_LOCK:
            _current += 1
            _peak = max(_peak, _current)
        try:
            time.sleep(float(seconds))
            if fail:
                raise RuntimeError("探针故意失败")
        finally:
            with _COUNTER_LOCK:
                _current -= 1
        return {"value": 1}


def _spec(nodes, outputs=None):
    return PipelineSpec.from_dict(
        {"name": "limits", "params": {}, "nodes": nodes, "outputs": outputs or {}}
    )


def _probe_nodes(groups, **extra):
    """按组名列表生成无依赖的探针节点（组名空串 = 不声明组）。"""
    nodes = []
    for index, group in enumerate(groups):
        node = {"id": f"n{index}", "op": "m6.concurrency_probe"}
        if group:
            node["concurrency_group"] = group
        node.update(extra)
        nodes.append(node)
    return nodes


class ConcurrencyLimitsTest(unittest.TestCase):
    def setUp(self):
        _reset_counter()

    def test_same_group_serialized(self):
        """3 个同组节点限 1：峰值并发必须为 1，且全部成功。"""
        with tempfile.TemporaryDirectory() as tmp:
            report, _ = run_pipeline(
                _spec(_probe_nodes(["gpu"] * 3)),
                registry=default_registry,
                workdir=tmp,
                cache_dir=None,
                max_workers=4,
                concurrency_limits={"gpu": 1},
            )
        self.assertEqual(report.status, "success")
        self.assertEqual(_peak, 1)

    def test_different_groups_run_in_parallel(self):
        """两组各限 1：组内串行、组间并行，峰值并发 >= 2。"""
        with tempfile.TemporaryDirectory() as tmp:
            report, _ = run_pipeline(
                _spec(_probe_nodes(["a", "a", "b", "b"])),
                registry=default_registry,
                workdir=tmp,
                cache_dir=None,
                max_workers=4,
                concurrency_limits={"a": 1, "b": 1},
            )
        self.assertEqual(report.status, "success")
        self.assertGreaterEqual(_peak, 2)

    def test_undeclared_or_unconfigured_group_unlimited(self):
        """未声明组 / 声明了组但未配置限制：不加锁（峰值并发 >= 2）。"""
        with tempfile.TemporaryDirectory() as tmp:
            report, _ = run_pipeline(
                _spec(_probe_nodes(["", "", ""])),
                registry=default_registry,
                workdir=tmp,
                cache_dir=None,
                max_workers=4,
                concurrency_limits={"gpu": 1},
            )
        self.assertEqual(report.status, "success")
        self.assertGreaterEqual(_peak, 2)

        _reset_counter()
        with tempfile.TemporaryDirectory() as tmp:
            report, _ = run_pipeline(
                _spec(_probe_nodes(["other"] * 3)),
                registry=default_registry,
                workdir=tmp,
                cache_dir=None,
                max_workers=4,
                concurrency_limits={"gpu": 1},
            )
        self.assertEqual(report.status, "success")
        self.assertGreaterEqual(_peak, 2)

    def test_failed_node_releases_slot(self):
        """异常路径同样释放配额：同组一失败一成功，运行正常收尾。"""
        nodes = _probe_nodes(["gpu", "gpu"], seconds=0.05)
        nodes[0]["params"] = {"fail": True, "seconds": 0.05}
        with tempfile.TemporaryDirectory() as tmp:
            report, _ = run_pipeline(
                _spec(nodes),
                registry=default_registry,
                workdir=tmp,
                cache_dir=None,
                max_workers=2,
                concurrency_limits={"gpu": 1},
            )
        self.assertEqual({n.status for n in report.nodes}, {"failed", "success"})
        self.assertEqual(_peak, 1)

    def test_invalid_limits_raise_config_error(self):
        """0 / 负数限制在 Engine 构造期抛 ConfigError。"""
        with tempfile.TemporaryDirectory() as tmp:
            for bad in ({"gpu": 0}, {"gpu": -1}, {"gpu": "2"}):
                with self.assertRaises(ConfigError):
                    run_pipeline(
                        _spec(_probe_nodes(["gpu"])),
                        registry=default_registry,
                        workdir=tmp,
                        cache_dir=None,
                        concurrency_limits=bad,
                    )


class ConcurrencyGroupSpecTest(unittest.TestCase):
    def test_from_dict_and_to_dict(self):
        """concurrency_group 可解析、可序列化；空串 = 未声明。"""
        spec = _spec(
            [
                {"id": "a", "op": "m6.concurrency_probe", "concurrency_group": "gpu"},
                {"id": "b", "op": "m6.concurrency_probe"},
            ]
        )
        self.assertEqual(spec.nodes[0].concurrency_group, "gpu")
        self.assertEqual(spec.nodes[1].concurrency_group, "")
        data = spec.to_dict()
        self.assertEqual(data["nodes"][0]["concurrency_group"], "gpu")
        self.assertEqual(data["nodes"][1]["concurrency_group"], "")

    def test_non_string_group_raises(self):
        with self.assertRaises(ConfigError):
            _spec([{"id": "a", "op": "m6.concurrency_probe", "concurrency_group": 3}])


if __name__ == "__main__":
    unittest.main()
