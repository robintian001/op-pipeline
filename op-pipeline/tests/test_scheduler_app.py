"""scheduler_app 独立外围子包测试（M5-2：调度守护进程 + 轻量 Web UI）。

运行：``python -m unittest discover -s tests -k test_scheduler_app -v``

覆盖：
- Task 5：短间隔定时触发、RunStore 记录随之增长、stop_event 干净退出、CLI --help；
- Task 6：列表页 / 与详情页 /run/<run_id> 关键字段、HTML 转义、server 关停；
- 内核隔离：op_pipeline 下所有 .py 不得出现 scheduler_app 引用。

遵循现有测试风格：文件顶部只做路径准备，op_pipeline / scheduler_app
的 import 放在测试方法内（方法级 import）。
"""
from __future__ import annotations

import json
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

# 小型临时流水线：3 个轻量节点（常量 -> 透传 -> 写文本），
# 无外部数据依赖、执行毫秒级，适合调度循环反复触发。
MINI_CFG = {
    "name": "sched_demo",
    "version": "1.0",
    "nodes": [
        {"id": "seed", "op": "control.const", "params": {"value": {"x": 1}}},
        {"id": "pass", "op": "control.passthrough", "inputs": {"value": "${seed.value}"}},
        {"id": "save", "op": "io.write_text", "inputs": {"text": "${pass.value}"},
         "params": {"path": "out.txt"}},
    ],
    "outputs": {"out": "${pass.value}"},
}


def _write_cfg(directory: Path) -> Path:
    """在临时目录写入小型流水线配置，返回配置文件路径。"""
    cfg = directory / "mini.json"
    cfg.write_text(json.dumps(MINI_CFG), encoding="utf-8")
    return cfg


# --------------------------------------------------------------------------- #
# Task 5：调度守护进程
# --------------------------------------------------------------------------- #
class SchedulerTest(unittest.TestCase):
    """M5-2 Task 5：定时触发 + RunStore 落库 + stop_event 干净退出。"""

    def test_scheduler_repeats_and_records_then_stops(self):
        from op_pipeline import RunStore
        from scheduler_app import run_scheduler

        with tempfile.TemporaryDirectory() as d:
            d = Path(d)
            cfg = _write_cfg(d)
            db = d / "runs.db"
            stop = threading.Event()
            stats: list[int] = []

            def worker() -> None:
                stats.append(run_scheduler(
                    str(cfg), every=0.05, db=str(db), cache_dir=None, stop_event=stop,
                ))

            thread = threading.Thread(target=worker)
            thread.start()
            # 轮询等待 RunStore 记录 ≥2 条再置位停止事件，对系统负载不敏感：
            # 全量回归时多进程测试并行、磁盘 fsync 变慢，固定 sleep 窗口可能
            # 只够 1 轮；上限 20s 保证高负载下也能凑满 2 轮。
            deadline = time.time() + 20.0
            while time.time() < deadline:
                with RunStore(db) as probe:
                    rows = probe.query("SELECT COUNT(*) FROM runs")
                if rows[0][0] >= 2:
                    break
                time.sleep(0.1)
            stop.set()
            thread.join(timeout=10)
            self.assertFalse(thread.is_alive(), "stop_event 置位后调度线程应退出")

            count = stats[0]
            self.assertGreaterEqual(count, 2, f"短间隔下至少触发 2 次，实际 {count}")

            # RunStore 记录随触发次数同步增长，且全部成功
            with RunStore(db) as store:
                rows = store.query("SELECT run_id, status FROM runs ORDER BY started_at")
                self.assertEqual(len(rows), count, "每次触发都应产生一条 runs 记录")
                self.assertTrue(
                    all(status == "success" for _, status in rows),
                    f"小型流水线应全部成功：{rows}",
                )
                node_rows = store.query(
                    "SELECT COUNT(*) FROM nodes WHERE run_id = ?", (rows[0][0],)
                )
                self.assertEqual(node_rows[0][0], 3, "每次运行应记录 3 个节点")

    def test_scheduler_without_db_runs_and_counts(self):
        """未指定 db 时照常循环触发（零副作用），返回触发次数。"""
        from scheduler_app import run_scheduler

        with tempfile.TemporaryDirectory() as d:
            d = Path(d)
            cfg = _write_cfg(d)
            stop = threading.Event()
            stats: list[int] = []

            def worker() -> None:
                stats.append(run_scheduler(
                    str(cfg), every=0.05, cache_dir=None, stop_event=stop,
                ))

            thread = threading.Thread(target=worker)
            thread.start()
            # 不落库时每轮仅 run() 开销（~0.1s），1.5s 窗口足够覆盖 ≥1 轮，
            # 同时在高负载（全量回归并行）下留足余量。
            time.sleep(1.5)
            stop.set()
            thread.join(timeout=10)
            self.assertFalse(thread.is_alive(), "stop_event 置位后调度线程应退出")
            self.assertGreaterEqual(stats[0], 1, "至少触发 1 次")

    def test_scheduler_cli_help(self):
        """``python -m scheduler_app.scheduler --help`` 可用且列出关键选项。"""
        proc = subprocess.run(
            [sys.executable, "-m", "scheduler_app.scheduler", "--help"],
            cwd=str(ROOT), capture_output=True, text=True, timeout=30,
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)
        for option in ("--pipeline", "--every", "--registry", "--plugin",
                       "--workdir", "--cache-dir", "--db"):
            self.assertIn(option, proc.stdout, f"--help 应列出 {option}")


# --------------------------------------------------------------------------- #
# Task 6：轻量 Web UI
# --------------------------------------------------------------------------- #
class WebUiTest(unittest.TestCase):
    """M5-2 Task 6：http.server 列表页 / 与详情页 /run/<run_id>。"""

    @staticmethod
    def _seed_db(db: Path) -> None:
        """写入两条运行记录：run-0001 成功（2 节点）、run-0002 失败（1 节点带错误）。"""
        from op_pipeline import RunStore

        with RunStore(db) as store:
            store.record_run(
                "run-0001", "orders_daily", "1.0", "success",
                1000.0, 1010.0, {"n": 1},
            )
            store.record_node("run-0001", "load_orders", "io.read_csv",
                              "success", 1, False, None, "k1", 1005.0)
            store.record_node("run-0001", "clean", "table.filter_rows",
                              "success", 1, False, None, "k2", 1009.0)
            store.record_run(
                "run-0002", "broken", "1.0", "failed",
                2000.0, 2005.0, {"n": 2},
            )
            store.record_node("run-0002", "load_orders", "io.read_csv",
                              "failed", 2, False,
                              "ValueError: 故意失败，用于详情页错误摘要",
                              None, 2004.0)

    def test_list_and_detail_pages(self):
        from scheduler_app import create_server

        with tempfile.TemporaryDirectory() as d:
            db = Path(d) / "runs.db"
            self._seed_db(db)
            server = create_server(str(db), port=0)
            port = server.server_address[1]
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                # 列表页：run_id / status / 节点数
                index = urllib.request.urlopen(
                    f"http://127.0.0.1:{port}/", timeout=5
                ).read().decode("utf-8")
                self.assertIn("run-0001", index)
                self.assertIn("run-0002", index)
                self.assertIn("success", index)
                self.assertIn("failed", index)
                # 节点数（2 / 1）应出现在列表页
                self.assertIn("2", index)
                self.assertIn("1", index)

                # 详情页：节点状态 / 错误摘要
                detail = urllib.request.urlopen(
                    f"http://127.0.0.1:{port}/run/run-0002", timeout=5
                ).read().decode("utf-8")
                self.assertIn("load_orders", detail)
                self.assertIn("failed", detail)
                self.assertIn("故意失败", detail, "错误摘要应展示")
                self.assertIn("ValueError", detail)
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)

    def test_detail_404_for_unknown_run(self):
        from scheduler_app import create_server

        with tempfile.TemporaryDirectory() as d:
            db = Path(d) / "runs.db"
            self._seed_db(db)
            server = create_server(str(db), port=0)
            port = server.server_address[1]
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                try:
                    urllib.request.urlopen(
                        f"http://127.0.0.1:{port}/run/nope", timeout=5
                    )
                    self.fail("未知 run_id 应返回 404")
                except urllib.error.HTTPError as exc:
                    self.assertEqual(exc.code, 404, "未知 run_id 应返回 404")
                    body = exc.read().decode("utf-8")
                    self.assertIn("nope", body, "404 页应提示未知 run_id")
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)

    def test_html_escapes_user_data(self):
        """列表页转义用户数据，防止 HTML 注入。"""
        import sqlite3

        from scheduler_app import create_server

        with tempfile.TemporaryDirectory() as d:
            db = Path(d) / "runs.db"
            conn = sqlite3.connect(db)
            conn.executescript(
                "CREATE TABLE IF NOT EXISTS runs ("
                " run_id TEXT PRIMARY KEY, pipeline TEXT NOT NULL, version TEXT NOT NULL,"
                " status TEXT NOT NULL, started_at REAL NOT NULL, finished_at REAL NOT NULL,"
                " params_json TEXT NOT NULL);"
            )
            conn.execute(
                "INSERT INTO runs VALUES (?, ?, ?, ?, ?, ?, ?)",
                ("run-x", "<script>alert(1)</script>", "1.0", "success",
                 0.0, 1.0, "{}"),
            )
            conn.commit()
            conn.close()
            server = create_server(str(db), port=0)
            port = server.server_address[1]
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                index = urllib.request.urlopen(
                    f"http://127.0.0.1:{port}/", timeout=5
                ).read().decode("utf-8")
                self.assertNotIn("<script>", index, "原始 HTML 不应原样出现")
                self.assertIn("&lt;script&gt;", index, "应转义为实体")
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)


# --------------------------------------------------------------------------- #
# 内核隔离：scheduler_app 不得被 op_pipeline 引用
# --------------------------------------------------------------------------- #
class KernelIsolationTest(unittest.TestCase):
    """op_pipeline 内核保持"零依赖、零守护进程"承诺。"""

    def test_kernel_has_no_scheduler_app_reference(self):
        import op_pipeline

        pkg_dir = Path(op_pipeline.__file__).parent
        offenders = [
            py for py in pkg_dir.rglob("*.py")
            if "scheduler_app" in py.read_text(encoding="utf-8")
        ]
        self.assertEqual(
            offenders, [],
            f"op_pipeline 内核不得引用 scheduler_app：{offenders}",
        )


if __name__ == "__main__":
    unittest.main()
