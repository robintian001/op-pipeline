"""Web UI /stats 聚合页测试（M9 Task 13）：按 pipeline 汇总 + 失败节点 Top10。

运行：``python -m unittest discover -s tests -k Stats -v``

覆盖：
- 聚合数值正确：运行总数 / 成功率（成功/总数，百分比）/ 平均耗时（已知数据）；
- 失败节点 Top10：按 node_id 跨运行聚合、标注 op、按失败次数降序；
- 空窗口渲染：窗口内无记录 → 两表占位文案，页面仍 200；
- days 参数：非法（非整数/≤0）与超上限（>90）回退默认 7，合法值生效。

窗口口径与实现一致：``started_at >= now - days*86400``（seed 数据用相对
``time.time()`` 的时间戳，保证落在默认 7 天窗口内）。

启动方式沿用 test_webui_m6.py：ThreadingHTTPServer(port=0) + 后台线程。
"""
from __future__ import annotations

import sys
import tempfile
import threading
import time
import unittest
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def _seed(db: Path) -> None:
    """已知数据：etl 3 次（2 成功 1 失败，耗时 10/20/30 → 均 20.0s）、
    ml 2 次（1 成功 1 失败，耗时 5/15 → 均 10.0s）；失败节点 load×2、save×1。"""
    from op_pipeline import RunStore

    now = time.time()
    with RunStore(db) as store:
        store.record_run("etl-1", "etl", "1.0", "success",
                         now - 3600, now - 3600 + 10, {})
        store.record_run("etl-2", "etl", "1.0", "success",
                         now - 7200, now - 7200 + 20, {})
        store.record_run("etl-3", "etl", "1.0", "failed",
                         now - 86400, now - 86400 + 30, {})
        store.record_run("ml-1", "ml", "1.0", "success",
                         now - 1800, now - 1800 + 5, {})
        store.record_run("ml-2", "ml", "1.0", "failed",
                         now - 5400, now - 5400 + 15, {})
        store.record_node("etl-3", "load", "io.read_csv", "failed", 2, False,
                          "文件不存在", "k1", now - 86400 + 10, duration=5.0)
        store.record_node("ml-2", "load", "io.read_csv", "failed", 1, False,
                          "网络错误", "k2", now - 5400 + 5, duration=3.0)
        store.record_node("ml-2", "save", "io.write_text", "failed", 1, False,
                          "写盘失败", "k3", now - 5400 + 12, duration=2.0)
        # 成功节点不参与失败聚合
        store.record_node("etl-1", "load", "io.read_csv", "success", 1, False,
                          None, "k4", now - 3600 + 5, duration=2.0)


def _seed_empty_window(db: Path) -> None:
    """唯一运行在 100 天前：默认 7 天窗口内无记录。"""
    from op_pipeline import RunStore

    now = time.time()
    with RunStore(db) as store:
        store.record_run("old-1", "etl", "1.0", "success",
                         now - 100 * 86400, now - 100 * 86400 + 10, {})


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


class StatsAggregationTest(_ServerCase):
    """Task 13.1：聚合数值正确性（构造已知数据）。"""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.db = Path(self._tmp.name) / "runs.db"
        _seed(self.db)
        super().setUp()

    def tearDown(self) -> None:
        super().tearDown()
        self._tmp.cleanup()

    def test_default_window_is_seven_days(self):
        page = self._get("/stats")
        self.assertIn("统计窗口：最近 7 天", page)
        self.assertIn("共 5 条运行", page)

    def test_pipeline_summary_numbers(self):
        page = self._get("/stats")
        self.assertIn("<tr><td>etl</td><td>3</td><td>2/3 （66.7%）</td>"
                      "<td>20.00s</td></tr>", page)
        self.assertIn("<tr><td>ml</td><td>2</td><td>1/2 （50.0%）</td>"
                      "<td>10.00s</td></tr>", page)

    def test_failed_nodes_top10_aggregated(self):
        """load 跨 etl/ml 两 run 失败 2 次排第一，save 1 次排第二。"""
        page = self._get("/stats")
        self.assertIn("<tr><td>1</td><td>load</td><td>io.read_csv</td>"
                      "<td>2</td></tr>", page)
        self.assertIn("<tr><td>2</td><td>save</td><td>io.write_text</td>"
                      "<td>1</td></tr>", page)

    def test_pipeline_order_by_run_count(self):
        """按运行总数降序：etl（3）在 ml（2）之前。"""
        page = self._get("/stats")
        self.assertLess(page.index("<td>etl</td>"), page.index("<td>ml</td>"))


class StatsEmptyWindowTest(_ServerCase):
    """Task 13.2：窗口内无记录 → 占位文案，页面仍 200。"""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.db = Path(self._tmp.name) / "runs.db"
        _seed_empty_window(self.db)
        super().setUp()

    def tearDown(self) -> None:
        super().tearDown()
        self._tmp.cleanup()

    def test_empty_window_placeholders(self):
        page = self._get("/stats")
        self.assertIn("窗口内无运行记录", page)
        self.assertIn("窗口内无失败节点", page)
        self.assertIn("共 0 条运行", page)


class StatsDaysFallbackTest(_ServerCase):
    """Task 13.2：days 非法 / 超上限回退默认 7，合法值生效。"""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.db = Path(self._tmp.name) / "runs.db"
        _seed(self.db)
        super().setUp()

    def tearDown(self) -> None:
        super().tearDown()
        self._tmp.cleanup()

    def test_invalid_days_falls_back_to_default(self):
        for bad in ("abc", "0", "-3", "200"):
            page = self._get(f"/stats?days={bad}")
            self.assertIn("统计窗口：最近 7 天", page, f"days={bad} 应回退默认 7")

    def test_valid_days_applies(self):
        page = self._get("/stats?days=30")
        self.assertIn("统计窗口：最近 30 天", page)

    def test_max_days_is_allowed(self):
        page = self._get("/stats?days=90")
        self.assertIn("统计窗口：最近 90 天", page)


if __name__ == "__main__":
    unittest.main()
