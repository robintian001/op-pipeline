"""运行对比页测试（M7-3 Task 7）：``/compare?a=<run_id>&b=<run_id>``。

运行：``python -m unittest discover -s tests -k test_webui_compare -v``

覆盖：
- 正常对比：两 run 的流水线/状态/总耗时、节点状态与耗时差异；
- 节点按 id 取并集，单侧节点标注"仅 A"/"仅 B"；
- 缺少 ``a``/``b`` 参数 → 400；run 不存在 → 404；
- 动态文本（流水线名、节点 id）HTML 转义。

启动方式沿用 test_webui_m6.py：ThreadingHTTPServer(port=0) + 后台线程。
"""
from __future__ import annotations

import sys
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def _seed(db: Path) -> None:
    """写入两份可对比的运行：A 成功（12s）、B 部分成功（10s）。"""
    from op_pipeline import RunStore

    with RunStore(db) as store:
        store.record_run("run-a", "demo", "1.0", "success", 1000.0, 1012.0, {})
        store.record_node("run-a", "load", "io.read_csv", "success", 1, False,
                          None, "ka1", 1002.5, duration=2.5)
        store.record_node("run-a", "train", "ml.fit", "success", 1, False,
                          None, "ka2", 1008.5, duration=6.0)
        store.record_node("run-a", "only_a", "control.const", "success", 1, True,
                          None, "ka3", 1010.0, duration=1.5)
        store.record_run("run-b", "demo", "1.0", "partial", 2000.0, 2010.0, {})
        store.record_node("run-b", "load", "io.read_csv", "success", 1, True,
                          None, "kb1", 2001.5, duration=1.5)
        store.record_node("run-b", "train", "ml.fit", "failed", 2, False,
                          "ValueError: 故意失败", "kb2", 2008.0, duration=8.0)
        store.record_node("run-b", "only_b", "control.const", "success", 1, False,
                          None, "kb3", 2009.5, duration=0.5)


class _ServerCase(unittest.TestCase):
    """启动临时 Web 服务；记录 ``db`` 由子类在 setUp 中准备。"""

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

    def _status(self, path: str) -> tuple[int, str]:
        try:
            with urllib.request.urlopen(self.base + path, timeout=5) as resp:
                return resp.status, resp.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            return exc.code, exc.read().decode("utf-8")


class ComparePageTest(_ServerCase):
    """Task 7：正常对比内容。"""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.db = Path(self._tmp.name) / "runs.db"
        _seed(self.db)
        super().setUp()

    def tearDown(self) -> None:
        super().tearDown()
        self._tmp.cleanup()

    def test_compare_shows_runs_and_totals(self):
        page = self._get("/compare?a=run-a&b=run-b")
        for text in ("run-a", "run-b", "demo", "success", "partial"):
            self.assertIn(text, page)
        self.assertIn("12.00s", page, "A 总耗时")
        self.assertIn("10.00s", page, "B 总耗时")

    def test_compare_shows_node_diff(self):
        page = self._get("/compare?a=run-a&b=run-b")
        for node_id in ("load", "train", "only_a", "only_b"):
            self.assertIn(node_id, page)
        # A 侧耗时 / B 侧耗时
        self.assertIn("2.5s", page)
        self.assertIn("1.5s", page)
        self.assertIn("6.0s", page)
        self.assertIn("8.0s", page)
        # 耗时差带符号：load A-B=+1.0s，train A-B=-2.0s
        self.assertIn("+1.0s", page)
        self.assertIn("-2.0s", page)
        # 缓存标记
        self.assertIn("缓存", page)

    def test_single_side_nodes_are_annotated(self):
        page = self._get("/compare?a=run-a&b=run-b")
        self.assertIn("仅 A", page)
        self.assertIn("仅 B", page)


class CompareErrorsTest(_ServerCase):
    """Task 7：参数缺失 400、run 不存在 404。"""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.db = Path(self._tmp.name) / "runs.db"
        _seed(self.db)
        super().setUp()

    def tearDown(self) -> None:
        super().tearDown()
        self._tmp.cleanup()

    def test_missing_param_returns_400(self):
        self.assertEqual(self._status("/compare")[0], 400)
        self.assertEqual(self._status("/compare?a=run-a")[0], 400)
        self.assertEqual(self._status("/compare?b=run-b")[0], 400)
        self.assertEqual(self._status("/compare?a=&b=run-b")[0], 400)

    def test_unknown_run_returns_404(self):
        code, body = self._status("/compare?a=run-a&b=nope")
        self.assertEqual(code, 404)
        self.assertIn("nope", body, "404 应提示缺失的 run_id")
        self.assertEqual(self._status("/compare?a=nope&b=run-b")[0], 404)


class CompareEscapeTest(_ServerCase):
    """Task 7：动态文本 HTML 转义。"""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.db = Path(self._tmp.name) / "runs.db"
        from op_pipeline import RunStore

        with RunStore(self.db) as store:
            store.record_run("run-a", "<script>alert(1)</script>", "1.0",
                             "success", 1000.0, 1001.0, {})
            store.record_node("run-a", "<b>n1</b>", "control.const", "success",
                              1, False, None, "k1", 1000.5, duration=0.5)
            store.record_run("run-b", "demo", "1.0", "success", 2000.0, 2002.0, {})
            store.record_node("run-b", "<b>n1</b>", "control.const", "success",
                              1, False, None, "k2", 2001.5, duration=1.5)
        super().setUp()

    def tearDown(self) -> None:
        super().tearDown()
        self._tmp.cleanup()

    def test_dynamic_text_is_escaped(self):
        page = self._get("/compare?a=run-a&b=run-b")
        self.assertNotIn("<script>", page)
        self.assertNotIn("<b>n1</b>", page)
        self.assertIn("&lt;script&gt;", page)
        self.assertIn("&lt;b&gt;n1&lt;/b&gt;", page)


if __name__ == "__main__":
    unittest.main()
