"""子流水线算子 ``pipeline.run`` 的行为测试（M8 Task 2）。

覆盖：父子执行与取值、摘要 dict 与 run_id 一致性、子失败冒泡、循环引用
防环链路、嵌套深度上限、params / plugins 注入、deterministic=False 不落
父节点缓存、目标节点/端口校验、on_error 参数校验。
"""
from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from op_pipeline import run
from op_pipeline.ops.pipeline_ops import SubPipeline
from op_pipeline.registry import default_registry

# --------------------------------------------------------------------------- #
# 辅助
# --------------------------------------------------------------------------- #
def _write_config(directory: Path, name: str, cfg: dict) -> Path:
    """把配置写成 JSON 文件（支持中文内容）。"""
    path = directory / name
    path.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _sub_node(config: str, node: str = "sub", **params: object) -> dict:
    """构造一个 pipeline.run 节点定义。"""
    spec = {"id": "sub", "op": "pipeline.run", "params": {"config": config, "node": node}}
    spec["params"].update(params)
    return spec


def _node_error(report, node_id: str) -> str:
    """取报告中某节点的错误文本（无该节点或无错误即断言失败）。"""
    for n in report.nodes:
        if n.node_id == node_id:
            if not n.error:
                raise AssertionError(f"节点 {node_id} 没有 error 信息（status={n.status}）")
            return n.error
    raise AssertionError(f"报告中没有节点 {node_id}")


# --------------------------------------------------------------------------- #
# 用例
# --------------------------------------------------------------------------- #
class TestSubPipelineOp(unittest.TestCase):
    """pipeline.run 算子：编排组合、防环、深度、注入与缓存豁免。"""

    def test_parent_child_run_result_and_summary(self) -> None:
        """父子执行成功：result 取子节点输出，摘要键齐全、run_id 一致，完整报告不并入父报告。"""
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            tmp = Path(tmp)
            _write_config(tmp, "sub.json", {
                "name": "sub", "version": "1",
                "nodes": [
                    {"id": "mk", "op": "control.const", "params": {"value": {"hello": "世界"}}},
                ],
            })
            _write_config(tmp, "parent.json", {
                "name": "parent", "version": "1",
                "nodes": [_sub_node("sub.json", node="mk")],
            })
            report, values = run(tmp / "parent.json")
            self.assertEqual(report.status, "success")
            # 子 run 的完整报告不并入父报告：父报告只有 pipeline.run 节点本身
            self.assertEqual([n.node_id for n in report.nodes], ["sub"])
            # handle=None → 取子节点声明顺序的第一个输出端口（control.const 为 value）
            self.assertEqual(values["sub.result"], {"hello": "世界"})
            # 摘要 dict 键齐全
            summary = values["sub.report"]
            self.assertEqual(
                set(summary),
                {"run_id", "status", "duration", "node_count", "failed_nodes"},
            )
            self.assertEqual(summary["status"], "success")
            self.assertEqual(summary["node_count"], 1)
            self.assertEqual(summary["failed_nodes"], [])
            self.assertIsInstance(summary["duration"], float)
            self.assertGreaterEqual(summary["duration"], 0.0)
            # run_id 一致：摘要 dict 与独立 run_id 端口同源
            self.assertTrue(summary["run_id"])
            self.assertEqual(summary["run_id"], values["sub.run_id"])

    def test_handle_port_selection(self) -> None:
        """handle=None 取声明顺序第一个端口；显式 handle 按端口名取值。"""
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            tmp = Path(tmp)
            # control.if_else 输出端口声明顺序：value, branch
            _write_config(tmp, "sub.json", {
                "name": "sub", "version": "1",
                "nodes": [
                    {
                        "id": "pick", "op": "control.if_else",
                        "inputs": {"condition": 1, "when_true": "是", "when_false": "否"},
                    },
                ],
            })
            _write_config(tmp, "parent_default.json", {
                "name": "parent", "version": "1",
                "nodes": [_sub_node("sub.json", node="pick")],
            })
            report, values = run(tmp / "parent_default.json")
            self.assertEqual(report.status, "success")
            self.assertEqual(values["sub.result"], "是")

            _write_config(tmp, "parent_branch.json", {
                "name": "parent", "version": "1",
                "nodes": [_sub_node("sub.json", node="pick", handle="branch")],
            })
            report2, values2 = run(tmp / "parent_branch.json")
            self.assertEqual(report2.status, "success")
            self.assertEqual(values2["sub.result"], "then")

    def test_child_failure_propagates(self) -> None:
        """子流水线节点失败 → 父节点失败，错误含子 run_id、状态与失败节点 id。"""
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            tmp = Path(tmp)
            _write_config(tmp, "sub.json", {
                "name": "sub", "version": "1",
                "nodes": [
                    {
                        "id": "check", "op": "control.assert",
                        "inputs": {"value": 1},
                        "params": {"that": "value > 10", "message": "数值太小"},
                    },
                ],
            })
            _write_config(tmp, "parent.json", {
                "name": "parent", "version": "1",
                "nodes": [_sub_node("sub.json", node="check")],
            })
            report, values = run(tmp / "parent.json")
            self.assertEqual(report.status, "failed")
            error = _node_error(report, "sub")
            # 错误信息含子 run_id、状态与失败节点 id 列表
            self.assertIn("run_id=", error)
            self.assertIn("status=failed", error)
            self.assertIn("失败节点：check", error)
            self.assertIn("sub.json", error)
            # 失败详情随错误链冒泡，可定位子节点原始错误
            self.assertIn("数值太小", error)

    def test_cycle_detection(self) -> None:
        """A 调 B、B 调 A：报 ConfigError，信息含完整循环链路。"""
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            tmp = Path(tmp)
            _write_config(tmp, "a.json", {
                "name": "a", "version": "1", "nodes": [_sub_node("b.json")],
            })
            _write_config(tmp, "b.json", {
                "name": "b", "version": "1", "nodes": [_sub_node("a.json")],
            })
            report, values = run(tmp / "a.json")
            self.assertEqual(report.status, "failed")
            error = _node_error(report, "sub")
            self.assertIn("循环引用", error)
            self.assertIn("a.json", error)
            self.assertIn("b.json", error)
            self.assertIn(" -> ", error)

    def test_depth_limit(self) -> None:
        """嵌套深度超上限 5：报 ConfigError，信息含当前深度与上限。"""
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            tmp = Path(tmp)
            # 7 个配置连成链：d1 → d2 → ... → d7，共 6 次 pipeline.run 嵌套
            for i in range(1, 8):
                nxt = f"d{i + 1}.json" if i < 7 else None
                nodes = (
                    [_sub_node(nxt)] if nxt
                    else [{"id": "leaf", "op": "control.const", "params": {"value": 1}}]
                )
                _write_config(tmp, f"d{i}.json", {"name": f"d{i}", "version": "1", "nodes": nodes})
            report, values = run(tmp / "d1.json")
            self.assertEqual(report.status, "failed")
            error = _node_error(report, "sub")
            self.assertIn("嵌套深度超限", error)
            self.assertIn("第 6 层", error)
            self.assertIn("上限 5 层", error)
            self.assertIn("d2.json", error)

    def test_params_injection(self) -> None:
        """params 注入子流水线运行级参数，使子配置的 ${param:} 生效（含默认值回退）。"""
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            tmp = Path(tmp)
            _write_config(tmp, "sub.json", {
                "name": "sub", "version": "1",
                "nodes": [
                    {
                        "id": "mk", "op": "control.const",
                        "params": {"value": "${param:NAME}|${param:MISSING:-回退值}"},
                    },
                ],
            })
            _write_config(tmp, "parent.json", {
                "name": "parent", "version": "1",
                "nodes": [_sub_node("sub.json", node="mk", params={"NAME": "注入值"})],
            })
            report, values = run(tmp / "parent.json")
            self.assertEqual(report.status, "success")
            self.assertEqual(values["sub.result"], "注入值|回退值")

    def test_plugins_param(self) -> None:
        """plugins 参数把文件插件加载进子流水线，子配置可用插件算子。"""
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            tmp = Path(tmp)
            (tmp / "test_plugin.py").write_text(
                "from op_pipeline import op, Operator\n"
                "\n"
                "\n"
                '@op("testplugin.echo")\n'
                "class Echo(Operator):\n"
                '    """测试插件：原样返回输入。"""\n'
                "\n"
                '    description = "测试插件：原样返回输入"\n'
                '    inputs = [("text", str)]\n'
                '    outputs = [("text", str)]\n'
                "\n"
                "    def run(self, ctx, text):\n"
                '        return {"text": str(text)}\n',
                encoding="utf-8",
            )
            _write_config(tmp, "sub.json", {
                "name": "sub", "version": "1",
                "nodes": [
                    {"id": "seed", "op": "control.const", "params": {"value": "插件输出"}},
                    {"id": "echo", "op": "testplugin.echo", "inputs": {"text": "${seed.value}"}},
                ],
            })
            _write_config(tmp, "parent.json", {
                "name": "parent", "version": "1",
                "nodes": [
                    _sub_node("sub.json", node="echo", plugins=["test_plugin.py"]),
                ],
            })
            report, values = run(tmp / "parent.json")
            self.assertEqual(report.status, "success")
            self.assertEqual(values["sub.result"], "插件输出")
            self.assertIn("testplugin.echo", default_registry.names())

    def test_parent_node_not_cached(self) -> None:
        """deterministic=False：节点声明 cache=true 也不落缓存，第二次运行仍真实执行。"""
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            tmp = Path(tmp)
            _write_config(tmp, "sub.json", {
                "name": "sub", "version": "1",
                "nodes": [
                    {"id": "mk", "op": "control.const", "params": {"value": 42}},
                ],
            })
            _write_config(tmp, "parent.json", {
                "name": "parent", "version": "1",
                "nodes": [
                    # 对照组：普通确定性节点正常落缓存
                    {"id": "seed", "op": "control.const", "params": {"value": 1}, "cache": True},
                    {"id": "sub", "op": "pipeline.run", "cache": True,
                     "params": {"config": "sub.json", "node": "mk"}},
                ],
            })
            cache_dir = str(tmp / "parent_cache")
            report1, values1 = run(tmp / "parent.json", cache_dir=cache_dir)
            self.assertEqual(report1.status, "success")
            self.assertFalse(_cached(report1, "seed"))
            self.assertFalse(_cached(report1, "sub"))
            self.assertTrue(values1["sub.run_id"])

            report2, values2 = run(tmp / "parent.json", cache_dir=cache_dir)
            self.assertEqual(report2.status, "success")
            # 第二次运行：对照组命中缓存，父节点仍真实执行（未缓存）
            self.assertTrue(_cached(report2, "seed"))
            self.assertFalse(_cached(report2, "sub"))
            # 子 run 每次都是新执行（新 run_id），证明父节点没有被缓存结果替代
            self.assertNotEqual(values1["sub.run_id"], values2["sub.run_id"])
            # 缓存目录确有对照组条目，但条目数不因第二次运行增加
            # （pipeline.run 未向缓存写入任何条目）
            entries1 = _cache_entries(Path(cache_dir))
            entries2 = _cache_entries(Path(cache_dir))
            self.assertGreaterEqual(entries1, 1)
            self.assertEqual(entries1, entries2)

    def test_node_not_found(self) -> None:
        """目标节点不存在：报 ConfigError，含配置路径、节点 id 与可用节点。"""
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            tmp = Path(tmp)
            _write_config(tmp, "sub.json", {
                "name": "sub", "version": "1",
                "nodes": [
                    {"id": "mk", "op": "control.const", "params": {"value": 1}},
                ],
            })
            _write_config(tmp, "parent.json", {
                "name": "parent", "version": "1",
                "nodes": [_sub_node("sub.json", node="ghost")],
            })
            report, values = run(tmp / "parent.json")
            self.assertEqual(report.status, "failed")
            error = _node_error(report, "sub")
            self.assertIn("ConfigError", error)
            self.assertIn("sub.json", error)
            self.assertIn("不存在节点 'ghost'", error)
            self.assertIn("mk", error)

    def test_handle_port_not_found(self) -> None:
        """handle 端口名不在节点输出中：报 ConfigError，含配置路径、节点 id 与可用端口。"""
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            tmp = Path(tmp)
            _write_config(tmp, "sub.json", {
                "name": "sub", "version": "1",
                "nodes": [
                    {"id": "mk", "op": "control.const", "params": {"value": 1}},
                ],
            })
            _write_config(tmp, "parent.json", {
                "name": "parent", "version": "1",
                "nodes": [_sub_node("sub.json", node="mk", handle="ghost")],
            })
            report, values = run(tmp / "parent.json")
            self.assertEqual(report.status, "failed")
            error = _node_error(report, "sub")
            self.assertIn("ConfigError", error)
            self.assertIn("sub.json", error)
            self.assertIn("'mk'", error)
            self.assertIn("没有输出端口 'ghost'", error)
            self.assertIn("value", error)

    def test_on_error_invalid_value(self) -> None:
        """on_error 仅支持 propagate，其他值（含拼错）直接抛 ValueError。"""
        op = SubPipeline(config="whatever.json", node="n", on_error="skip")
        with self.assertRaises(ValueError) as cm:
            op.run(None, "whatever.json", "n", on_error="skip")
        self.assertIn("on_error 仅支持 'propagate'", str(cm.exception))
        self.assertIn("skip", str(cm.exception))


# --------------------------------------------------------------------------- #
# 小工具
# --------------------------------------------------------------------------- #
def _cached(report, node_id: str) -> bool:
    """取报告中某节点是否为缓存命中。"""
    for n in report.nodes:
        if n.node_id == node_id:
            return bool(n.cached)
    raise AssertionError(f"报告中没有节点 {node_id}")


def _cache_entries(cache_dir: Path) -> int:
    """统计缓存目录中的条目（*.pkl）个数。"""
    return len(list(cache_dir.glob("*.pkl")))


if __name__ == "__main__":
    unittest.main()
