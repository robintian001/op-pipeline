"""Web UI 增强测试（M6-2 Task 7）：分页、状态筛选、节点耗时列。

运行：``python -m unittest discover -s tests -k test_webui_m6 -v``

覆盖：
- 列表页 ``?page=N&size=M`` 分页（SQL LIMIT/OFFSET）与非法参数回落；
- 上一页/下一页链接保留 status 参数；"第 x/y 页，共 n 条"统计；
- ``?status=`` 只接受 success/failed/partial/skipped，非法值忽略；
- 详情页节点表含"耗时"列（1 位小数 + s）；
- 旧库（nodes 无 duration 列）详情页仍 200 且显示 0.0s；
- 用户数据 HTML 转义。

启动方式沿用 test_scheduler_app.py：ThreadingHTTPServer(port=0) + 后台线程。
"""
from __future__ import annotations

import sqlite3
import sys
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

_STATUSES = ("success", "failed", "partial", "skipped")


def _seed_runs(db: Path, count: int = 120) -> None:
    """写入 count 条 runs（状态轮换），供分页/筛选断言。"""
    from op_pipeline import RunStore

    with RunStore(db) as store:
        for i in range(count):
            store.record_run(
                f"run-{i:03d}", f"pipeline-{i}", "1.0", _STATUSES[i % 4],
                1000.0 + i, 1000.5 + i, {"i": i},
            )


class _ServerCase(unittest.TestCase):
    """启动一个临时 Web 服务，测试结束回收。"""

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

    @staticmethod
    def _row_count(page: str) -> int:
        """列表页 run 链接数量（每行一个 /run/ 链接）。"""
        return page.count("href='/run/")


class PaginationTest(_ServerCase):
    """Task 7：分页与统计。"""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.db = Path(self._tmp.name) / "runs.db"
        _seed_runs(self.db)
        super().setUp()

    def tearDown(self) -> None:
        super().tearDown()
        self._tmp.cleanup()

    def test_default_page_is_one_with_50_rows(self):
        page = self._get("/")
        self.assertEqual(self._row_count(page), 50)
        self.assertIn("第 1/3 页", page)
        self.assertIn("共 120 条", page)
        self.assertIn("page=2", page, "应有下一页链接")

    def test_last_page_has_remainder_rows(self):
        page = self._get("/?page=3&size=50")
        self.assertEqual(self._row_count(page), 20)
        self.assertIn("第 3/3 页", page)

    def test_invalid_page_falls_back(self):
        self.assertIn("第 1/3 页", self._get("/?page=0"))
        self.assertIn("第 1/3 页", self._get("/?page=abc"))
        # 超出末页：回落到最后一页而非空页
        self.assertIn("第 3/3 页", self._get("/?page=999"))

    def test_size_is_clamped(self):
        page = self._get("/?size=1000")
        self.assertEqual(self._row_count(page), 120, "size 上限 200，120 条应一页显示")
        self.assertIn("第 1/1 页", page)

    def test_pager_keeps_status_filter(self):
        # failed 共 30 条，每页 10 条 → 翻页链接必须保留 status=failed
        page = self._get("/?status=failed&size=10")
        self.assertIn("page=2", page)
        self.assertIn("status=failed", page)


class StatusFilterTest(_ServerCase):
    """Task 7：status 筛选（SQL 参数化）。"""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.db = Path(self._tmp.name) / "runs.db"
        _seed_runs(self.db, count=40)
        super().setUp()

    def tearDown(self) -> None:
        super().tearDown()
        self._tmp.cleanup()

    def test_filter_returns_only_matching_rows(self):
        page = self._get("/?status=failed")
        self.assertEqual(self._row_count(page), 10, "40 条中 failed 应 10 条")
        self.assertIn("共 10 条", page)
        self.assertNotIn(">success</span>", page)

    def test_skipped_status_is_allowed(self):
        page = self._get("/?status=skipped")
        self.assertEqual(self._row_count(page), 10)
        self.assertIn(">skipped</span>", page)

    def test_invalid_status_is_ignored(self):
        page = self._get("/?status=bogus")
        self.assertEqual(self._row_count(page), 40, "非法 status 应忽略")
        self.assertIn("共 40 条", page)


class DetailDurationTest(_ServerCase):
    """Task 7：详情页节点耗时列 + 旧库兼容。"""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.db = Path(self._tmp.name) / "runs.db"
        from op_pipeline import RunStore

        with RunStore(self.db) as store:
            store.record_run("run-1", "demo", "1.0", "success", 1000.0, 1010.0, {})
            store.record_node(
                "run-1", "load", "io.read_csv", "success", 1, False, None, "k1",
                1002.0, duration=2.5,
            )
            store.record_node(
                "run-1", "save", "io.write_text", "success", 1, False, None, "k2",
                1009.0, duration=7.0,
            )
        super().setUp()

    def tearDown(self) -> None:
        super().tearDown()
        self._tmp.cleanup()

    def test_detail_shows_node_duration(self):
        page = self._get("/run/run-1")
        self.assertIn("耗时", page)
        self.assertIn("2.5s", page)
        self.assertIn("7.0s", page)

    def test_old_db_without_duration_column(self):
        """外部旧库 nodes 无 duration：详情页 200 且显示 0.0s。"""
        db2 = Path(self._tmp.name) / "old.db"
        conn = sqlite3.connect(db2)
        conn.executescript(
            "CREATE TABLE runs (run_id TEXT PRIMARY KEY, pipeline TEXT NOT NULL,"
            " version TEXT NOT NULL, status TEXT NOT NULL, started_at REAL NOT NULL,"
            " finished_at REAL NOT NULL, params_json TEXT NOT NULL);"
            "CREATE TABLE nodes (run_id TEXT NOT NULL, node_id TEXT NOT NULL,"
            " op TEXT NOT NULL, status TEXT NOT NULL, attempts INTEGER NOT NULL,"
            " cached INTEGER NOT NULL, error TEXT, cache_key TEXT,"
            " finished_at REAL NOT NULL, PRIMARY KEY (run_id, node_id));"
        )
        conn.execute(
            "INSERT INTO runs VALUES ('old-1', 'legacy', '0.6', 'success', 1.0, 2.0, '{}')"
        )
        conn.execute(
            "INSERT INTO nodes VALUES ('old-1', 'n1', 'control.const', 'success',"
            " 1, 0, NULL, NULL, 1.5)"
        )
        conn.commit()
        conn.close()

        from scheduler_app import create_server

        server = create_server(str(db2), port=0)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with urllib.request.urlopen(
                f"http://127.0.0.1:{server.server_address[1]}/run/old-1", timeout=5
            ) as resp:
                self.assertEqual(resp.status, 200)
                body = resp.read().decode("utf-8")
            self.assertIn("0.0s", body)
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)


class EscapeTest(_ServerCase):
    """Task 7：用户数据仍须 HTML 转义。"""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.db = Path(self._tmp.name) / "runs.db"
        from op_pipeline import RunStore

        with RunStore(self.db) as store:
            store.record_run(
                "run-x", "<script>alert(1)</script>", "1.0", "success", 0.0, 1.0, {},
            )
        super().setUp()

    def tearDown(self) -> None:
        super().tearDown()
        self._tmp.cleanup()

    def test_pipeline_name_is_escaped(self):
        page = self._get("/")
        self.assertNotIn("<script>", page)
        self.assertIn("&lt;script&gt;", page)


if __name__ == "__main__":
    unittest.main()
