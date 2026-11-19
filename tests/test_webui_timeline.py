"""Web UI 节点执行时间线测试（M8-3 Task 7）：详情页甘特式条形。

运行：``python -m unittest discover -s tests -k test_webui_timeline -v``

覆盖：
- 并行窗口：三条时间条按各自 left/width 百分比渲染；
- 状态颜色：success / failed / cached / skipped 四类样式类齐备；
- duration=0 走最小可见宽度（min-width），窗口防除零不报错；
- 全失败 run 与无节点 run 正常渲染；
- 节点 id 含 HTML 特殊字符被转义；
- 旧数据：nodes 缺 duration 列、finished_at/duration 为 NULL 时不报错。

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


def _seed_parallel(db: Path) -> None:
    """三个并行窗口节点：窗口 10s，各条 left/width 百分比可精确断言。

    load:  开始 0.0s / 耗时 2.5s → left 0%    width 25%
    train: 开始 2.5s / 耗时 6.0s → left 25%   width 60%
    save:  开始 8.5s / 耗时 1.5s → left 85%   width 15%
    """
    from op_pipeline import RunStore

    with RunStore(db) as store:
        store.record_run("run-par", "demo", "1.0", "success", 1000.0, 1010.0, {})
        store.record_node("run-par", "load", "io.read_csv", "success", 1, False,
                          None, "k1", 1002.5, duration=2.5)
        store.record_node("run-par", "train", "ml.fit", "success", 1, False,
                          None, "k2", 1008.5, duration=6.0)
        store.record_node("run-par", "save", "io.write_text", "success", 1, False,
                          None, "k3", 1010.0, duration=1.5)


def _seed_statuses(db: Path) -> None:
    """四类状态节点：普通成功 / 失败 / 缓存命中（cached=1）/ skipped。"""
    from op_pipeline import RunStore

    with RunStore(db) as store:
        store.record_run("run-st", "demo", "1.0", "partial", 2000.0, 2010.0, {})
        store.record_node("run-st", "load", "io.read_csv", "success", 1, False,
                          None, "k1", 2002.0, duration=2.0)
        store.record_node("run-st", "train", "ml.fit", "failed", 2, False,
                          "ValueError: 故意失败", "k2", 2006.0, duration=4.0)
        store.record_node("run-st", "cache_hit", "control.const", "success", 1,
                          True, None, "k3", 2008.0, duration=1.0)
        store.record_node("run-st", "skip_me", "control.const", "skipped", 0,
                          False, None, "k4", 2009.0, duration=0.0)


def _seed_zero_duration(db: Path) -> None:
    """started_at == finished_at 且节点 duration=0：窗口取最小值防除零。"""
    from op_pipeline import RunStore

    with RunStore(db) as store:
        store.record_run("run-zero", "demo", "1.0", "success", 3000.0, 3000.0, {})
        store.record_node("run-zero", "instant", "control.const", "success", 1,
                          False, None, "k1", 3000.0, duration=0.0)


def _seed_all_failed(db: Path) -> None:
    """全失败 run：只有 failed 节点。"""
    from op_pipeline import RunStore

    with RunStore(db) as store:
        store.record_run("run-fail", "demo", "1.0", "failed", 4000.0, 4005.0, {})
        store.record_node("run-fail", "load", "io.read_csv", "failed", 3, False,
                          "boom", "k1", 4002.0, duration=2.0)
        store.record_node("run-fail", "train", "ml.fit", "failed", 1, False,
                          "boom", "k2", 4005.0, duration=3.0)


def _seed_empty(db: Path) -> None:
    """只有 runs 记录、没有任何节点行。"""
    from op_pipeline import RunStore

    with RunStore(db) as store:
        store.record_run("run-empty", "demo", "1.0", "failed", 5000.0, 5001.0, {})


def _seed_escape(db: Path) -> None:
    """节点 id 含 HTML 特殊字符（标签 + 引号）。"""
    from op_pipeline import RunStore

    with RunStore(db) as store:
        store.record_run("run-esc", "demo", "1.0", "success", 6000.0, 6001.0, {})
        store.record_node('run-esc', '<a href="x">', "control.const", "success",
                          1, False, None, "k1", 6000.5, duration=0.5)


def _create_legacy_db(db: Path, *, with_duration: bool) -> None:
    """手工建外部旧库：runs + nodes（可选是否带 duration 列，列均可空）。"""
    conn = sqlite3.connect(db)
    duration_col = " duration REAL," if with_duration else ""
    conn.executescript(
        "CREATE TABLE runs (run_id TEXT PRIMARY KEY, pipeline TEXT NOT NULL,"
        " version TEXT NOT NULL, status TEXT NOT NULL, started_at REAL NOT NULL,"
        " finished_at REAL NOT NULL, params_json TEXT NOT NULL);"
        "CREATE TABLE nodes (run_id TEXT NOT NULL, node_id TEXT NOT NULL,"
        " op TEXT NOT NULL, status TEXT NOT NULL, attempts INTEGER NOT NULL,"
        f" cached INTEGER NOT NULL, error TEXT, cache_key TEXT,"
        f" finished_at REAL,{duration_col} PRIMARY KEY (run_id, node_id));"
    )
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


class ParallelWindowTest(_ServerCase):
    """Task 7.2：并行窗口节点的偏移与条长。"""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.db = Path(self._tmp.name) / "runs.db"
        _seed_parallel(self.db)
        super().setUp()

    def tearDown(self) -> None:
        super().tearDown()
        self._tmp.cleanup()

    def test_bars_render_at_offsets_and_widths(self):
        page = self._get("/run/run-par")
        self.assertIn("节点执行时间线", page)
        self.assertIn("left:0.000%;width:25.000%", page)
        self.assertIn("left:25.000%;width:60.000%", page)
        self.assertIn("left:85.000%;width:15.000%", page)

    def test_scale_ruler_present(self):
        page = self._get("/run/run-par")
        for mark in (">0%</span>", ">25%</span>", ">50%</span>",
                     ">75%</span>", ">100%</span>"):
            self.assertIn(mark, page, "相对刻度标尺应包含 0/25/50/75/100%")

    def test_one_row_per_node_in_order(self):
        page = self._get("/run/run-par")
        # 3 个时间条（CSS 中的 .tl-row 不带 class='tl-row' 属性，不重复计数）
        self.assertEqual(page.count("class='tl-row'"), 3)
        self.assertIn("load", page)
        self.assertIn("train", page)
        self.assertIn("save", page)


class StatusColorTest(_ServerCase):
    """Task 7.2：状态颜色语义。"""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.db = Path(self._tmp.name) / "runs.db"
        _seed_statuses(self.db)
        super().setUp()

    def tearDown(self) -> None:
        super().tearDown()
        self._tmp.cleanup()

    def test_all_status_classes_present(self):
        page = self._get("/run/run-st")
        for cls in ("tl-success", "tl-failed", "tl-cached", "tl-skipped"):
            self.assertIn(cls, page, f"应出现 {cls} 状态色")

    def test_cached_takes_priority_over_success(self):
        """cached=1 且 success → 灰色（tl-cached），不落在 tl-success。"""
        page = self._get("/run/run-st")
        self.assertIn("tl-cached", page)


class ZeroDurationTest(_ServerCase):
    """Task 7.2：异常数据——duration=0 与窗口防除零。"""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.db = Path(self._tmp.name) / "runs.db"
        _seed_zero_duration(self.db)
        super().setUp()

    def tearDown(self) -> None:
        super().tearDown()
        self._tmp.cleanup()

    def test_zero_duration_gets_minimum_width(self):
        page = self._get("/run/run-zero")
        self.assertIn("width:0.000%", page, "duration=0 宽度按 0% 渲染")
        self.assertIn("min-width: 2px", page, "CSS 最小可见宽度兜底")


class AllFailedTest(_ServerCase):
    """Task 7.2：全失败 run 正常渲染。"""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.db = Path(self._tmp.name) / "runs.db"
        _seed_all_failed(self.db)
        super().setUp()

    def tearDown(self) -> None:
        super().tearDown()
        self._tmp.cleanup()

    def test_all_failed_run_renders_timeline(self):
        page = self._get("/run/run-fail")
        self.assertIn("节点执行时间线", page)
        self.assertIn("tl-failed", page)
        # load 开始 0.0s / 耗时 2.0s；train 开始 2.0s / 耗时 3.0s（窗口 5s）
        self.assertIn("left:0.000%;width:40.000%", page)
        self.assertIn("left:40.000%;width:60.000%", page)


class EmptyRunTest(_ServerCase):
    """Task 7.2：无节点 run 正常渲染。"""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.db = Path(self._tmp.name) / "runs.db"
        _seed_empty(self.db)
        super().setUp()

    def tearDown(self) -> None:
        super().tearDown()
        self._tmp.cleanup()

    def test_run_without_nodes_renders_empty_note(self):
        page = self._get("/run/run-empty")
        self.assertIn("节点执行时间线", page)
        self.assertIn("该运行无可用时间线数据", page)
        self.assertNotIn("class='tl-row'", page)


class EscapeTest(_ServerCase):
    """Task 7.2：节点 id HTML 转义。"""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.db = Path(self._tmp.name) / "runs.db"
        _seed_escape(self.db)
        super().setUp()

    def tearDown(self) -> None:
        super().tearDown()
        self._tmp.cleanup()

    def test_node_id_is_escaped(self):
        page = self._get("/run/run-esc")
        self.assertNotIn('<a href="x"', page, "原始标签不得出现")
        self.assertIn("&lt;a href=&quot;x&quot;&gt;", page)


class LegacyNoDurationColumnTest(_ServerCase):
    """Task 7.2：旧数据健壮性——nodes 缺 duration 列。"""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.db = Path(self._tmp.name) / "old.db"
        _create_legacy_db(self.db, with_duration=False)
        conn = sqlite3.connect(self.db)
        conn.execute(
            "INSERT INTO runs VALUES ('old-1', 'legacy', '0.8', 'success',"
            " 7000.0, 7010.0, '{}')"
        )
        conn.execute(
            "INSERT INTO nodes (run_id, node_id, op, status, attempts, cached,"
            " error, cache_key, finished_at) VALUES"
            " ('old-1', 'n1', 'control.const', 'success', 1, 0, NULL, NULL, 7002.5),"
            " ('old-1', 'n2', 'control.const', 'success', 1, 0, NULL, NULL, 7005.0)"
        )
        conn.commit()
        conn.close()
        super().setUp()

    def tearDown(self) -> None:
        super().tearDown()
        self._tmp.cleanup()

    def test_missing_duration_column(self):
        """nodes 缺 duration 列：回退 0.0，时间条按最小宽度渲染。"""
        page = self._get("/run/old-1")
        self.assertIn("节点执行时间线", page)
        self.assertIn("width:0.000%", page)
        # 窗口由节点完成偏移决定（n2 → 5s）：n1 开始偏移 2.5s → left 50%
        self.assertIn("left:50.000%", page)


class LegacyNullRowTest(_ServerCase):
    """Task 7.2：旧数据健壮性——finished_at / duration 为 NULL 的行跳过。"""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.db = Path(self._tmp.name) / "old.db"
        _create_legacy_db(self.db, with_duration=True)
        conn = sqlite3.connect(self.db)
        conn.execute(
            "INSERT INTO runs VALUES ('old-2', 'legacy', '0.8', 'success',"
            " 7000.0, 7010.0, '{}')"
        )
        conn.execute(
            "INSERT INTO nodes (run_id, node_id, op, status, attempts, cached,"
            " error, cache_key, finished_at, duration) VALUES"
            " ('old-2', 'n_null', 'control.const', 'success', 1, 0, NULL,"
            " NULL, NULL, NULL)"
        )
        conn.execute(
            "INSERT INTO nodes (run_id, node_id, op, status, attempts, cached,"
            " error, cache_key, finished_at, duration) VALUES"
            " ('old-2', 'n_ok', 'control.const', 'success', 1, 0, NULL,"
            " NULL, 7005.0, 3.0)"
        )
        conn.commit()
        conn.close()
        super().setUp()

    def tearDown(self) -> None:
        super().tearDown()
        self._tmp.cleanup()

    def test_null_rows_skipped_and_valid_row_rendered(self):
        """NULL 行不进时间线，其余节点正常渲染、页面不报错。"""
        page = self._get("/run/old-2")
        self.assertIn("节点执行时间线", page)
        # 仅 n_ok 有时间条（窗口 5s：开始偏移 2s → left 40%，宽 3s → 60%）
        self.assertEqual(page.count("class='tl-row'"), 1)
        self.assertIn("left:40.000%;width:60.000%", page)


if __name__ == "__main__":
    unittest.main()
