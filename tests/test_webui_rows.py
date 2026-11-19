"""Web UI 行数展示测试（M8-3 Task 9.2）：详情页 In/Out 列 + /compare 行数差异列。

运行：``python -m unittest discover -s tests -k test_webui_rows -v``

覆盖：
- 详情页：表头含 In/Out；单元格 ``端口=行数`` 紧凑展示（按端口名排序）；
- 详情页旧行 NULL → 对应单元格为空；
- 详情页坏数据容错（手工 UPDATE 非法 JSON / 非 dict / 非整数值）→
  显示为空或跳过坏值，页面不 500；
- 对比页：同节点 A/B 行数不同 → +N（绿色标注）/ -N（普通减号）；
  多端口按 dict 值求和；相同 → 0；
- 对比页：节点仅 A / 仅 B → 「仅 A」「仅 B」；
- 对比页：单侧行数 NULL（旧行）→ 差异列为空，不猜 0。

启动方式沿用 test_webui_compare.py：ThreadingHTTPServer(port=0) + 后台线程。
"""
from __future__ import annotations

import sqlite3
import sys
import tempfile
import threading
import unittest
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def _seed_detail(db: Path) -> None:
    """三节点 run：rows_in/rows_out 各不相同（多端口 / 单端口 / 旧行 NULL）。"""
    from op_pipeline import RunStore

    with RunStore(db) as store:
        store.record_run("run-rows", "demo", "0.9", "success", 1000.0, 1010.0, {})
        # 多端口行数：展示为 "path=1, table=120"（按端口名排序）
        store.record_node(
            "run-rows", "load", "io.read_csv", "success", 1, False, None, "k1",
            1002.0, duration=2.0,
            rows_in={"table": 120, "path": 1},
            rows_out={"clean": 100, "stats": 1},
        )
        store.record_node(
            "run-rows", "train", "ml.fit", "success", 1, False, None, "k2",
            1008.0, duration=6.0,
            rows_in={"rows": 100}, rows_out={"rows": 88},
        )
        # 旧行：未采集行数（NULL），In/Out 单元格应为空
        store.record_node(
            "run-rows", "legacy", "control.const", "success", 1, False, None, "k3",
            1010.0, duration=2.0,
        )


def _seed_compare(db: Path) -> None:
    """两 run：差值正/负/零、多端口求和、单侧节点、单侧 NULL 全场景。"""
    from op_pipeline import RunStore

    with RunStore(db) as store:
        store.record_run("cmp-a", "demo", "0.9", "success", 1000.0, 1010.0, {})
        store.record_node("cmp-a", "up", "io.read_csv", "success", 1, False,
                          None, "ka1", 1001.0, duration=1.0,
                          rows_in={"table": 100}, rows_out={"table": 120})
        store.record_node("cmp-a", "down", "ml.fit", "success", 1, False,
                          None, "ka2", 1003.0, duration=2.0,
                          rows_in={"rows": 100}, rows_out={"rows": 50})
        store.record_node("cmp-a", "same", "control.const", "success", 1, False,
                          None, "ka3", 1005.0, duration=1.0,
                          rows_in={"v": 1}, rows_out={"table": 10})
        store.record_node("cmp-a", "multi", "table.split", "success", 1, False,
                          None, "ka4", 1007.0, duration=1.0,
                          rows_in={"a": 15}, rows_out={"a": 10, "b": 5})  # 总 15
        store.record_node("cmp-a", "null_b", "io.write_text", "success", 1, False,
                          None, "ka5", 1008.5, duration=0.5,
                          rows_in={"rows": 30}, rows_out={"rows": 30})
        store.record_node("cmp-a", "null_a", "control.const", "success", 1, False,
                          None, "ka6", 1009.5, duration=0.5)
        store.record_node("cmp-a", "only_a", "control.const", "success", 1, False,
                          None, "ka7", 1010.0, duration=0.5,
                          rows_in={"v": 2}, rows_out={"v": 7})
        store.record_run("cmp-b", "demo", "0.9", "success", 2000.0, 2010.0, {})
        store.record_node("cmp-b", "up", "io.read_csv", "success", 1, True,
                          None, "kb1", 2001.0, duration=1.0,
                          rows_in={"table": 100}, rows_out={"table": 100})
        store.record_node("cmp-b", "down", "ml.fit", "success", 1, False,
                          None, "kb2", 2003.0, duration=2.0,
                          rows_in={"rows": 100}, rows_out={"rows": 90})
        store.record_node("cmp-b", "same", "control.const", "success", 1, False,
                          None, "kb3", 2005.0, duration=1.0,
                          rows_in={"v": 1}, rows_out={"table": 10})
        store.record_node("cmp-b", "multi", "table.split", "success", 1, False,
                          None, "kb4", 2007.0, duration=1.0,
                          rows_in={"a": 13}, rows_out={"a": 12, "b": 1})  # 总 13
        # null_b：B 侧 rows_out 未采集（NULL）→ 差异列为空
        store.record_node("cmp-b", "null_b", "io.write_text", "success", 1, False,
                          None, "kb5", 2008.5, duration=0.5,
                          rows_in={"rows": 30})
        store.record_node("cmp-b", "null_a", "control.const", "success", 1, False,
                          None, "kb6", 2009.5, duration=0.5,
                          rows_in={"v": 1}, rows_out={"v": 5})
        store.record_node("cmp-b", "only_b", "control.const", "success", 1, False,
                          None, "kb7", 2010.0, duration=0.5,
                          rows_in={"v": 1}, rows_out={"v": 3})


def _seed_bad(db: Path) -> None:
    """四节点 run，随后手工 UPDATE 注入坏数据与未排序 JSON。"""
    from op_pipeline import RunStore

    with RunStore(db) as store:
        store.record_run("run-bad", "demo", "0.9", "success", 1000.0, 1004.0, {})
        for i, node_id in enumerate(("bad_json", "not_dict", "weird_values",
                                     "unsorted"), start=1):
            store.record_node("run-bad", node_id, "control.const", "success",
                              1, False, None, f"k{i}", 1000.0 + i, duration=1.0)
    conn = sqlite3.connect(db)
    # 非法 JSON + 空串：均应显示为空
    conn.execute(
        "UPDATE nodes SET rows_in = 'not-json{', rows_out = '' "
        "WHERE node_id = 'bad_json'"
    )
    # 合法 JSON 但非 dict：显示为空
    conn.execute(
        "UPDATE nodes SET rows_in = '[1, 2, 3]' WHERE node_id = 'not_dict'"
    )
    # dict 中混入非整数值：坏值跳过，仅展示可用的整数端口
    conn.execute(
        'UPDATE nodes SET rows_out = \'{"table": "x", "ok": 5}\' '
        "WHERE node_id = 'weird_values'"
    )
    # 未按端口名排序的 JSON 文本：展示层应重排为 "aport=3, zport=7"
    conn.execute(
        'UPDATE nodes SET rows_in = \'{"zport": 7, "aport": 3}\' '
        "WHERE node_id = 'unsorted'"
    )
    conn.commit()
    conn.close()


class _ServerCase(unittest.TestCase):
    """启动临时 Web 服务；``db`` 由子类在 setUp 中准备。"""

    db: Path
    server = None
    thread: threading.Thread | None = None

    def setUp(self) -> None:
        from scheduler_app import create_server

        self.server = create_server(str(self.db), port=0)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.base = f"http://127.0.0.1:{self.server.server_address[1]}"

    def tearDown(self) -> None:
        if self.server is not None:
            self.server.shutdown()
            self.server.server_close()
        if self.thread is not None:
            self.thread.join(timeout=5)

    def _get(self, path: str) -> str:
        with urllib.request.urlopen(self.base + path, timeout=5) as resp:
            self.assertEqual(resp.status, 200)
            return resp.read().decode("utf-8")

    def _row_segment(self, page: str, node_id: str) -> str:
        """截取某节点所在 ``<tr>`` 行片段（从节点单元格起至 ``</tr>`` 止）。"""
        start = page.index(f"<td>{node_id}</td>")
        end = page.index("</tr>", start) + len("</tr>")
        return page[start:end]


class DetailRowsTest(_ServerCase):
    """Task 9.2：详情页 In/Out 行数列。"""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.db = Path(self._tmp.name) / "runs.db"
        _seed_detail(self.db)
        super().setUp()

    def tearDown(self) -> None:
        super().tearDown()
        self._tmp.cleanup()

    def test_header_has_in_out_columns(self):
        page = self._get("/run/run-rows")
        self.assertIn("<th>In</th>", page)
        self.assertIn("<th>Out</th>", page)

    def test_rows_cells_render_sorted_ports(self):
        page = self._get("/run/run-rows")
        load_row = self._row_segment(page, "load")
        # In：{"table": 120, "path": 1} → 按端口名排序
        self.assertIn("path=1, table=120", load_row)
        # Out：{"clean": 100, "stats": 1}
        self.assertIn("clean=100, stats=1", load_row)
        train_row = self._row_segment(page, "train")
        self.assertIn("rows=100", train_row)
        self.assertIn("rows=88", train_row)

    def test_legacy_null_rows_cells_empty(self):
        """旧行 NULL：In/Out 单元格为空（无 ``端口=行数`` 文本），页面正常。"""
        page = self._get("/run/run-rows")
        row = self._row_segment(page, "legacy")
        self.assertTrue(
            row.endswith("<td></td><td></td><td></td></tr>"),
            "旧行 In/Out（及错误摘要）单元格应为空",
        )


class DetailBadDataTest(_ServerCase):
    """Task 9.2：坏数据容错——手工 UPDATE 注入非法 JSON，页面不 500。"""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.db = Path(self._tmp.name) / "runs.db"
        _seed_bad(self.db)
        super().setUp()

    def tearDown(self) -> None:
        super().tearDown()
        self._tmp.cleanup()

    def test_bad_json_and_non_dict_render_empty(self):
        page = self._get("/run/run-bad")
        bad_row = self._row_segment(page, "bad_json")
        self.assertTrue(
            bad_row.endswith("<td></td><td></td><td></td></tr>"),
            "非法 JSON / 空串的 In/Out（及错误摘要）单元格应为空",
        )
        not_dict_row = self._row_segment(page, "not_dict")
        self.assertNotIn("path=", not_dict_row, "非 dict JSON 应显示为空")
        self.assertTrue(
            not_dict_row.endswith("<td></td><td></td></tr>"),
            "非 dict JSON 的 In 单元格应为空",
        )

    def test_non_int_values_skipped(self):
        """dict 混入非整数值：坏值跳过，仅展示 ``ok=5``。"""
        page = self._get("/run/run-bad")
        row = self._row_segment(page, "weird_values")
        self.assertIn("ok=5", row)
        self.assertNotIn("table=", row)

    def test_ports_sorted_in_display(self):
        """展示层按端口名排序（不依赖库中文本顺序）。"""
        page = self._get("/run/run-bad")
        row = self._row_segment(page, "unsorted")
        self.assertIn("aport=3, zport=7", row)


class CompareRowsDiffTest(_ServerCase):
    """Task 9.2：对比页行数差异列。"""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.db = Path(self._tmp.name) / "runs.db"
        _seed_compare(self.db)
        super().setUp()

    def tearDown(self) -> None:
        super().tearDown()
        self._tmp.cleanup()

    def test_header_has_rows_diff_column(self):
        page = self._get("/compare?a=cmp-a&b=cmp-b")
        self.assertIn("<th>行数差（A-B）</th>", page)

    def test_positive_diff_annotated_green_plus(self):
        """A 多：+N（rows-up 绿色标注）；up 节点 120-100=+20。"""
        page = self._get("/compare?a=cmp-a&b=cmp-b")
        row = self._row_segment(page, "up")
        self.assertIn("<span class='rows-up'>+20</span>", row)

    def test_negative_diff_plain_minus(self):
        """B 多：普通减号 -N；down 节点 50-90=-40。"""
        page = self._get("/compare?a=cmp-a&b=cmp-b")
        row = self._row_segment(page, "down")
        self.assertIn("<td>-40</td>", row)
        self.assertNotIn("rows-up", row)

    def test_multi_port_diff_is_summed(self):
        """多端口按 dict 值求和：multi 节点 (10+5)-(12+1)=+2。"""
        page = self._get("/compare?a=cmp-a&b=cmp-b")
        row = self._row_segment(page, "multi")
        self.assertIn("<span class='rows-up'>+2</span>", row)

    def test_equal_diff_is_zero(self):
        """两侧行数相同：same 节点 10-10=0。"""
        page = self._get("/compare?a=cmp-a&b=cmp-b")
        row = self._row_segment(page, "same")
        self.assertIn("<td>0</td>", row)

    def test_single_side_nodes_annotated(self):
        """节点仅一侧存在：行数差列标注 仅 A / 仅 B（与状态列口径一致）。"""
        page = self._get("/compare?a=cmp-a&b=cmp-b")
        only_a_row = self._row_segment(page, "only_a")
        self.assertEqual(only_a_row.count("仅 A"), 2, "状态列与行数差列各标一次")
        self.assertNotIn("仅 B", only_a_row)
        only_b_row = self._row_segment(page, "only_b")
        self.assertEqual(only_b_row.count("仅 B"), 2, "状态列与行数差列各标一次")
        self.assertNotIn("仅 A", only_b_row)

    def test_null_side_diff_cell_empty(self):
        """任一侧 rows_out 为 NULL（旧行）：差异列为空，不猜 0。"""
        page = self._get("/compare?a=cmp-a&b=cmp-b")
        for node_id in ("null_a", "null_b"):
            row = self._row_segment(page, node_id)
            self.assertTrue(
                row.endswith("<td></td></tr>"),
                f"{node_id} 行数差单元格应为空（旧行不猜 0）",
            )


if __name__ == "__main__":
    unittest.main()
