"""只读 JSON API 与指标测试（M7-3 Task 8）。

运行：``python -m unittest discover -s tests -k test_webui_api -v``

覆盖：
- ``/api/runs?page&size&status``：items/total/page/size 与列表页分页/筛选语义一致；
- ``/api/runs/<id>``：返回 run + nodes；不存在 → 404 JSON ``{"error": ...}``；
- ``/metrics``：``text/plain; version=0.0.4``，各状态计数与耗时总量可正则断言；
- Content-Type 与末尾空行。

启动方式沿用 test_webui_m6.py。
"""
from __future__ import annotations

import json
import re
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


def _seed(db: Path, count: int = 40) -> None:
    """40 条运行（状态轮换、每条 0.5s），并为 run-000 写两个节点。"""
    from op_pipeline import RunStore

    with RunStore(db) as store:
        for i in range(count):
            store.record_run(
                f"run-{i:03d}", f"pipeline-{i}", "1.0", _STATUSES[i % 4],
                1000.0 + i, 1000.5 + i, {"i": i},
            )
        store.record_node("run-000", "load", "io.read_csv", "success", 1, False,
                          None, "k1", 1000.2, duration=0.2)
        store.record_node("run-000", "save", "io.write_text", "success", 1, True,
                          None, "k2", 1000.4, duration=0.2)


class _ServerCase(unittest.TestCase):
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

    def _get(self, path: str) -> tuple[int, str, str]:
        """返回 (状态码, Content-Type, 文本)；4xx 也返回而不抛异常。"""
        try:
            with urllib.request.urlopen(self.base + path, timeout=5) as resp:
                return (resp.status, resp.headers.get("Content-Type", ""),
                        resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            return (exc.code, exc.headers.get("Content-Type", ""),
                    exc.read().decode("utf-8"))


class ApiListTest(_ServerCase):
    """Task 8：分页、筛选与字段。"""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.db = Path(self._tmp.name) / "runs.db"
        _seed(self.db)
        super().setUp()

    def tearDown(self) -> None:
        super().tearDown()
        self._tmp.cleanup()

    def test_pagination_metadata(self):
        code, ctype, body = self._get("/api/runs?page=2&size=10")
        self.assertEqual(code, 200)
        self.assertIn("application/json", ctype)
        data = json.loads(body)
        self.assertEqual(data["total"], 40)
        self.assertEqual(data["page"], 2)
        self.assertEqual(data["size"], 10)
        self.assertEqual(len(data["items"]), 10)
        item = data["items"][0]
        for key in ("run_id", "pipeline", "status", "started_at",
                    "duration", "node_count"):
            self.assertIn(key, item, f"列表项应包含 {key}")

    def test_status_filter(self):
        data = json.loads(self._get("/api/runs?status=failed&size=5")[2])
        self.assertEqual(data["total"], 10, "40 条中 failed 应 10 条")
        self.assertEqual(len(data["items"]), 5)
        self.assertTrue(all(i["status"] == "failed" for i in data["items"]))

    def test_invalid_status_is_ignored(self):
        data = json.loads(self._get("/api/runs?status=bogus")[2])
        self.assertEqual(data["total"], 40, "非法 status 与列表页一致应忽略")

    def test_page_is_clamped_like_list_page(self):
        data = json.loads(self._get("/api/runs?page=999&size=10")[2])
        self.assertEqual(data["page"], 4, "越界页回落到最后一页")

    def test_node_count_matches_store(self):
        data = json.loads(self._get("/api/runs?size=200")[2])
        by_id = {item["run_id"]: item for item in data["items"]}
        self.assertEqual(by_id["run-000"]["node_count"], 2)
        self.assertEqual(by_id["run-001"]["node_count"], 0)


class ApiDetailTest(_ServerCase):
    """Task 8：``/api/runs/<id>`` 详情与 404。"""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.db = Path(self._tmp.name) / "runs.db"
        _seed(self.db)
        super().setUp()

    def tearDown(self) -> None:
        super().tearDown()
        self._tmp.cleanup()

    def test_detail_returns_run_and_nodes(self):
        code, ctype, body = self._get("/api/runs/run-000")
        self.assertEqual(code, 200)
        self.assertIn("application/json", ctype)
        data = json.loads(body)
        self.assertEqual(data["run"]["run_id"], "run-000")
        self.assertEqual(data["run"]["status"], "success")
        self.assertEqual(len(data["nodes"]), 2)
        node = data["nodes"][0]
        for key in ("node_id", "op", "status", "cached", "duration"):
            self.assertIn(key, node)

    def test_unknown_run_returns_404_json(self):
        code, ctype, body = self._get("/api/runs/nope")
        self.assertEqual(code, 404)
        self.assertIn("application/json", ctype)
        data = json.loads(body)
        self.assertIn("error", data)
        self.assertIn("nope", data["error"])


class MetricsTest(_ServerCase):
    """Task 8：Prometheus 文本指标。"""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.db = Path(self._tmp.name) / "runs.db"
        _seed(self.db)
        super().setUp()

    def tearDown(self) -> None:
        super().tearDown()
        self._tmp.cleanup()

    def test_metrics_content_type(self):
        code, ctype, _ = self._get("/metrics")
        self.assertEqual(code, 200)
        self.assertIn("text/plain", ctype)
        self.assertIn("version=0.0.4", ctype)

    def test_metrics_counts_and_duration(self):
        body = self._get("/metrics")[2]
        for status in _STATUSES:
            pattern = rf'^op_pipeline_runs_total\{{status="{status}"\}} 10$'
            self.assertRegex(body, re.compile(pattern, re.MULTILINE),
                             f"{status} 计数应为 10")
        match = re.search(
            r"^op_pipeline_runs_duration_seconds (\d+\.\d+)$", body, re.MULTILINE
        )
        self.assertIsNotNone(match, "应输出耗时总量")
        self.assertAlmostEqual(float(match.group(1)), 20.0, places=3)
        self.assertTrue(body.endswith("\n\n"), "指标文本应以空行结束")


if __name__ == "__main__":
    unittest.main()
