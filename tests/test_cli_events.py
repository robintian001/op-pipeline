"""M8-3 Task 8：``events`` 事件流查询子命令。

覆盖：单独/组合过滤（--run-id/--node/--event，AND 语义）、``--last`` 截取、
``--summary`` 聚合（次数/总耗时/失败数，含同一节点多 run 累加）、
损坏行（非 JSON/非 dict/缺 event）warn 后跳过、退出码 0/1/2、
明细输出保持原始行文本与顺序、以及用真实 ``JsonlHook`` 产出文件查询。

测试数据全部使用 ``op_pipeline.observe.JsonlHook`` 的真实字段名：
node 事件键为 ``node_id``（无 run_id），``run_finish`` 行才携带 ``run_id``；
``duration`` 为秒（float）。
"""
from __future__ import annotations

import contextlib
import io
import json
import logging
import sys
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from op_pipeline.cli import main as cli_main  # noqa: E402
from op_pipeline.observe import JsonlHook  # noqa: E402

_T0 = "2026-11-19T10:00:00"
_T1 = "2026-11-19T10:00:01"

# 多 run（run-a 成功 / run-b 失败）、多节点（clean / agg）事件序列；
# 字段与 JsonlHook._write 完全一致。
_EVENTS = [
    {"event": "node_start", "ts": _T0, "node_id": "clean", "op": "table.filter_rows"},
    {
        "event": "node_finish", "ts": _T0, "node_id": "clean",
        "status": "success", "duration": 0.1, "cached": False,
    },
    {"event": "node_start", "ts": _T0, "node_id": "agg", "op": "stats.aggregate"},
    {
        "event": "node_finish", "ts": _T0, "node_id": "agg",
        "status": "success", "duration": 0.2, "cached": False,
    },
    {
        "event": "run_finish", "ts": _T1, "pipeline": "demo", "version": "1",
        "run_id": "run-a", "status": "success", "duration": 0.3, "ok": True,
    },
    {"event": "node_start", "ts": _T1, "node_id": "clean", "op": "table.filter_rows"},
    {
        "event": "node_finish", "ts": _T1, "node_id": "clean",
        "status": "failed", "duration": 0.05, "cached": False,
    },
    {"event": "node_start", "ts": _T1, "node_id": "agg", "op": "stats.aggregate"},
    {
        "event": "node_finish", "ts": _T1, "node_id": "agg",
        "status": "success", "duration": 0.15, "cached": True,
    },
    {
        "event": "run_finish", "ts": _T1, "pipeline": "demo", "version": "1",
        "run_id": "run-b", "status": "failed", "duration": 0.2, "ok": False,
    },
]
_LINES = [json.dumps(e, ensure_ascii=False) for e in _EVENTS]

# 汇总期望（node_finish 计数 / duration 求和 / status=failed 计数）：
# clean 在 run-a 与 run-b 各执行一次 → 同一节点多 run 累加。
_SUMMARY_CLEAN = ["clean", "2", "0.1500", "1"]
_SUMMARY_AGG = ["agg", "2", "0.3500", "0"]


class EventsCommandTests(unittest.TestCase):
    """events 过滤/截取/聚合/容错/退出码。"""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.path = Path(self._tmp.name) / "events.jsonl"
        self.path.write_text("\n".join(_LINES) + "\n", encoding="utf-8")

    def tearDown(self):
        self._tmp.cleanup()

    def _run(self, *extra, path=None):
        """调用 cli.main 并捕获 stdout/stderr。

        先清空 root handlers 再运行（仿 test_cli_validate 的做法），
        保证 main 里 basicConfig 把日志 handler 绑到当前重定向的 stderr，
        warn 断言不依赖同进程内其他测试文件的执行顺序。
        """
        root = logging.getLogger()
        saved = list(root.handlers)
        out, err = io.StringIO(), io.StringIO()
        try:
            root.handlers[:] = []
            with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
                code = cli_main(["events", str(path or self.path), *extra])
        finally:
            root.handlers[:] = saved
        return code, out.getvalue(), err.getvalue()

    def _out_lines(self, out):
        return out.splitlines()

    def _row_tokens(self, out, node_id):
        """取汇总表格中某节点行的字段（按空白切分）。"""
        rows = [ln.split() for ln in out.splitlines() if ln.split()]
        matched = [toks for toks in rows if toks[0] == node_id]
        self.assertEqual(1, len(matched), f"节点 {node_id} 应恰好出现一行汇总：\n{out}")
        return matched[0]

    # ------------------------------------------------------------------ #
    def test_no_filter_outputs_original_lines_in_order(self):
        """无过滤时输出全部行，且保持原始文本与文件顺序。"""
        code, out, err = self._run()
        self.assertEqual(0, code)
        self.assertEqual(_LINES, self._out_lines(out))
        self.assertEqual("", err)

    def test_filter_event(self):
        """--event 单独过滤命中全部对应行。"""
        code, out, _ = self._run("--event", "node_finish")
        self.assertEqual(0, code)
        self.assertEqual([_LINES[1], _LINES[3], _LINES[6], _LINES[8]], self._out_lines(out))

    def test_filter_node(self):
        """--node 单独过滤命中该节点全部 node_start/node_finish 行。"""
        code, out, _ = self._run("--node", "clean")
        self.assertEqual(0, code)
        self.assertEqual([_LINES[0], _LINES[1], _LINES[5], _LINES[6]], self._out_lines(out))

    def test_filter_run_id_only_matches_run_finish(self):
        """--run-id 过滤：run_id 只存在于 run_finish 行，节点行被 AND 排除。"""
        code, out, _ = self._run("--run-id", "run-a")
        self.assertEqual(0, code)
        self.assertEqual([_LINES[4]], self._out_lines(out))
        code, out, _ = self._run("--run-id", "run-b")
        self.assertEqual(0, code)
        self.assertEqual([_LINES[9]], self._out_lines(out))

    def test_filter_combined_and_semantics(self):
        """组合过滤为 AND 语义：--node 与 --event 同时成立才命中。"""
        code, out, _ = self._run("--node", "clean", "--event", "node_finish")
        self.assertEqual(0, code)
        self.assertEqual([_LINES[1], _LINES[6]], self._out_lines(out))
        # clean 节点不可能有 run_finish 行（run_finish 无 node_id）→ 无匹配
        code, out, _ = self._run("--node", "clean", "--event", "run_finish")
        self.assertEqual(0, code)
        self.assertEqual("", out)

    def test_last_keeps_tail_of_filtered_lines(self):
        """--last N 保留过滤后最后 N 行（先过滤后截取）。"""
        code, out, _ = self._run("--event", "node_finish", "--last", "2")
        self.assertEqual(0, code)
        self.assertEqual([_LINES[6], _LINES[8]], self._out_lines(out))
        code, out, _ = self._run("--last", "3")
        self.assertEqual(0, code)
        self.assertEqual(_LINES[-3:], self._out_lines(out))
        code, out, _ = self._run("--last", "100")
        self.assertEqual(0, code)
        self.assertEqual(_LINES, self._out_lines(out))

    def test_last_zero_outputs_nothing(self):
        """--last 0 合法：输出为空且退出码 0。"""
        code, out, _ = self._run("--last", "0")
        self.assertEqual(0, code)
        self.assertEqual("", out)

    def test_last_invalid_negative_reports_error(self):
        """--last 非法（负数）报错，退出码非 0。"""
        code, _, err = self._run("--last", "-1")
        self.assertEqual(2, code)
        self.assertIn("--last", err)

    def test_summary_aggregates_and_accumulates_across_runs(self):
        """--summary 聚合：次数/总耗时/失败数正确，同一节点多 run 累加。"""
        code, out, _ = self._run("--summary")
        self.assertEqual(0, code)
        self.assertEqual(_SUMMARY_CLEAN, self._row_tokens(out, "clean"))
        self.assertEqual(_SUMMARY_AGG, self._row_tokens(out, "agg"))
        # --summary 与明细输出互斥：不输出任何原始 JSONL 行
        self.assertNotIn("{", out)

    def test_summary_with_node_filter(self):
        """spec 场景：--node clean --summary 只输出 clean 节点的聚合。"""
        code, out, _ = self._run("--node", "clean", "--summary")
        self.assertEqual(0, code)
        self.assertEqual(_SUMMARY_CLEAN, self._row_tokens(out, "clean"))
        self.assertNotIn("agg", out)

    def test_summary_empty_when_no_node_finish_matched(self):
        """过滤后无 node_finish 事件时汇总为空，退出码 0。"""
        code, out, _ = self._run("--event", "run_finish", "--summary")
        self.assertEqual(0, code)
        self.assertIn("汇总为空", out)

    def test_damaged_lines_warned_and_skipped(self):
        """夹带损坏行：warn 到 stderr 后跳过，其余事件正常处理。"""
        damaged = self.path.parent / "damaged.jsonl"
        content = "\n".join(
            [
                _LINES[0],
                "this is not json",  # 非 JSON
                "[1, 2, 3]",  # 合法 JSON 但不是 dict
                json.dumps({"node_id": "x"}, ensure_ascii=False),  # 缺 event 字段
                "",  # 空行：静默跳过（不计 warn）
                _LINES[1],
            ]
        ) + "\n"
        damaged.write_text(content, encoding="utf-8")
        code, out, err = self._run(path=damaged)
        self.assertEqual(0, code)
        self.assertEqual([_LINES[0], _LINES[1]], self._out_lines(out))
        self.assertEqual(3, err.count("已跳过"), err)
        self.assertIn("this is not json", err)

    def test_missing_file_exit_code_1(self):
        """文件不存在：退出码 1，中文错误信息。"""
        code, _, err = self._run(path=Path(self._tmp.name) / "nope.jsonl")
        self.assertEqual(1, code)
        self.assertIn("不存在", err)

    def test_empty_file_exit_code_0(self):
        """空文件：退出码 0，无输出。"""
        empty = self.path.parent / "empty.jsonl"
        empty.write_text("", encoding="utf-8")
        code, out, _ = self._run(path=empty)
        self.assertEqual(0, code)
        self.assertEqual("", out)

    def test_jsonl_hook_output_queryable(self):
        """集成：真实 JsonlHook 产出的事件文件可被 events 正确过滤/聚合。"""
        hook_path = self.path.parent / "hook_events.jsonl"
        hook = JsonlHook(hook_path, mode="w")
        try:
            hook.on_node_start("src", "control.const")
            hook.on_node_finish("src", "success", 0.25, False)
            hook.on_run_finish(
                SimpleNamespace(
                    finished_at=time.time(),
                    pipeline="hook_demo",
                    version="1",
                    run_id="run-hook",
                    status="success",
                    duration=0.4,
                    ok=True,
                )
            )
        finally:
            hook.close()
        code, out, _ = self._run("--run-id", "run-hook", path=hook_path)
        self.assertEqual(0, code)
        self.assertEqual(1, len(self._out_lines(out)))
        self.assertIn("run-hook", out)
        code, out, _ = self._run("--summary", path=hook_path)
        self.assertEqual(0, code)
        toks = self._row_tokens(out, "src")
        self.assertEqual("1", toks[1])  # 执行次数
        self.assertEqual("0.2500", toks[2])  # JsonlHook 对 duration 4 位舍入
        self.assertEqual("0", toks[3])  # 失败数


if __name__ == "__main__":
    unittest.main()
