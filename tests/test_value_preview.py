"""M9 Task 7：value_preview 值级预览测试。

运行：``python -m unittest discover -s tests -k ValuePreview -v``

语义：``run_pipeline(value_preview=N)`` 非 None 时报告节点附带
``preview_in``/``preview_out``（dict[端口, 预览]）：list 取前 N 行原值、dict
取前 N 键子 dict、str 截前 200 字符、其他类型 repr 截 200 字符。只影响报告
不影响执行（句柄不截断、缓存照常），与 ``sample_rows``/``lineage`` 相互独立；
非法值抛 ``ValueError``；默认 None 时报告不含这两个键。
"""
from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from op_pipeline import PipelineSpec, default_registry, run_pipeline  # noqa: E402


def _const(value):
    """单节点 control.const 配置（value 端口类型 object，任意值可注入）。"""
    return {"id": "src", "op": "control.const", "params": {"value": value}}


class ValuePreviewTest(unittest.TestCase):
    def _run(self, nodes, **kw):
        spec = PipelineSpec.from_dict(
            {"name": "vp", "params": {}, "nodes": nodes, "outputs": {}}
        )
        with tempfile.TemporaryDirectory() as tmp:
            return run_pipeline(
                spec, registry=default_registry, workdir=tmp, cache_dir=None, **kw
            )

    # ------------------------------------------------------------------ #
    # 预览规则
    # ------------------------------------------------------------------ #
    def test_list_preview_first_n_rows_handle_untruncated(self):
        report, values = self._run([_const(list(range(20)))], value_preview=3)
        node = report.node("src")
        self.assertEqual(node.preview_out["value"], [0, 1, 2])
        # 只影响报告不影响执行：句柄值完整
        self.assertEqual(values["src.value"], list(range(20)))

    def test_dict_preview_first_n_keys(self):
        report, _ = self._run(
            [_const({"a": 1, "b": 2, "c": 3, "d": 4})], value_preview=2
        )
        self.assertEqual(report.node("src").preview_out["value"], {"a": 1, "b": 2})

    def test_str_truncated_to_200_chars(self):
        report, _ = self._run([_const("x" * 500)], value_preview=5)
        self.assertEqual(report.node("src").preview_out["value"], "x" * 200)


def _const_name_fix():
    """第二个 control.const 节点（float 值），节点 id 与第一个错开。"""
    return {"id": "src2", "op": "control.const", "params": {"value": 1.5}}


class ValuePreviewMiscTest(unittest.TestCase):
    def _run(self, nodes, **kw):
        spec = PipelineSpec.from_dict(
            {"name": "vp", "params": {}, "nodes": nodes, "outputs": {}}
        )
        with tempfile.TemporaryDirectory() as tmp:
            return run_pipeline(
                spec, registry=default_registry, workdir=tmp, cache_dir=None, **kw
            )

    def test_other_types_use_repr_200(self):
        report, _ = self._run([_const(42), _const_name_fix()], value_preview=1)
        # int → repr "42"；float → repr "1.5"
        self.assertEqual(report.node("src").preview_out["value"], "42")
        self.assertEqual(report.node("src2").preview_out["value"], "1.5")

    def test_preview_in_snapshot_of_downstream_inputs(self):
        # 串联：下游 preview_in 快照采样截断后实际传入的输入
        nodes = [
            _const(list(range(10))),
            {
                "id": "down",
                "op": "table.limit",
                "inputs": {"table": "${src.value}"},
                "params": {"n": 5},
            },
        ]
        report, values = self._run(nodes, value_preview=2)
        self.assertEqual(report.node("down").preview_in["table"], [0, 1])
        # 下游输出 5 行，但预览只取前 2
        self.assertEqual(report.node("down").preview_out["table"], [0, 1])
        self.assertEqual(values["down.table"], list(range(5)))

    def test_preview_with_sample_rows_sees_truncated_input(self):
        # sample_rows=3 截断句柄后，preview 快照的是"截断后实际传入"（≤3 行）
        nodes = [
            _const(list(range(10))),
            {
                "id": "down",
                "op": "table.limit",
                "inputs": {"table": "${src.value}"},
                "params": {"n": 100},
            },
        ]
        report, values = self._run(nodes, sample_rows=3, value_preview=5)
        self.assertEqual(report.node("down").preview_in["table"], [0, 1, 2])
        self.assertEqual(report.node("down").sampled, True)
        self.assertEqual(values["down.table"], [0, 1, 2])

    def test_lineage_false_does_not_disable_preview(self):
        # lineage 开关独立：rows_* 不采集，preview_* 照常
        report, _ = self._run([_const(list(range(5)))], lineage=False, value_preview=2)
        node = report.node("src")
        self.assertNotIn("rows_in", node.to_dict())
        self.assertNotIn("rows_out", node.to_dict())
        self.assertEqual(node.preview_out["value"], [0, 1])

    def test_disabled_by_default_no_keys_in_report(self):
        report, _ = self._run([_const([1, 2])])
        d = report.node("src").to_dict()
        self.assertNotIn("preview_in", d)
        self.assertNotIn("preview_out", d)

    def test_invalid_value_preview_rejected(self):
        for bad in (0, -1, True, 2.5, "3"):
            with self.assertRaises(ValueError):
                self._run([_const(1)], value_preview=bad)

    def test_cli_preview_flag_writes_report(self):
        # CLI 链：--preview 2 → --json 报告含 preview_in/preview_out
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            config = {
                "name": "vp-cli",
                "params": {},
                "nodes": [
                    _const(list(range(10))),
                    {
                        "id": "down",
                        "op": "table.limit",
                        "inputs": {"table": "${src.value}"},
                        "params": {"n": 4},
                    },
                ],
                "outputs": {},
            }
            cfg = tmp / "vp.json"
            cfg.write_text(json.dumps(config), encoding="utf-8")
            report_path = tmp / "report.json"
            proc = subprocess.run(
                [
                    sys.executable, "-m", "op_pipeline", "run", str(cfg),
                    "--workdir", str(tmp), "--preview", "2",
                    "--json", str(report_path),
                ],
                capture_output=True, text=True, timeout=120,
            )
            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            data = json.loads(report_path.read_text(encoding="utf-8"))
            down = next(n for n in data["nodes"] if n["id"] == "down")
            self.assertEqual(down["preview_in"]["table"], [0, 1])
            self.assertEqual(down["preview_out"]["table"], [0, 1])
            # CLI 不带 --preview 时报告不含预览键
            report_path2 = tmp / "report2.json"
            proc2 = subprocess.run(
                [
                    sys.executable, "-m", "op_pipeline", "run", str(cfg),
                    "--workdir", str(tmp), "--json", str(report_path2),
                ],
                capture_output=True, text=True, timeout=120,
            )
            self.assertEqual(proc2.returncode, 0, proc2.stdout + proc2.stderr)
            data2 = json.loads(report_path2.read_text(encoding="utf-8"))
            down2 = next(n for n in data2["nodes"] if n["id"] == "down")
            self.assertNotIn("preview_in", down2)
            self.assertNotIn("preview_out", down2)


if __name__ == "__main__":
    unittest.main()
