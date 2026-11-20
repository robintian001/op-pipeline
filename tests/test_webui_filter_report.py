"""Web UI 筛选与报告测试（M9 Task 12）：列表页 pipeline/since/until 筛选、报告区块、报告 API。

运行：``python -m unittest discover -s tests -k WebFilter -v``

覆盖：
- 列表页 ``?pipeline=`` 子串匹配（不区分大小写），与 status/since/until 任意组合；
- ``?since=``/``?until=`` ISO 日期筛选（含该天边界）；非法日期忽略；翻页链接保留筛选；
- 详情页报告折叠区块：params（JSON 缩进转义）/ warnings / 失败节点完整 traceback，
  traceback 含 ``<script>`` 等需转义；report_json 为 NULL 显示"报告未入库"；
- ``/api/runs/<id>/report``：200 返回完整对象；404 记录不存在；NULL 返回 ``{"report": null}``。

启动方式沿用 test_webui_m6.py：ThreadingHTTPServer(port=0) + 后台线程。
"""
from __future__ import annotations

import json
import sys
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def _seed_filter(db: Path) -> None:
    """六条运行覆盖三个流水线、三天区间（固定时间戳，便于日期筛选断言）。

    时间线（以 2026-11-18 00:00:00 为基准 base）：
    - 11-18：r1 demo-etl success / r2 demo-etl failed / r3 other success
    - 11-19：r4 demo-export failed
    - 11-20：r5 demo-etl success
    - 11-21：r6 demo-etl success
    """
    from op_pipeline import RunStore

    base = time.mktime(time.strptime("2026-11-18", "%Y-%m-%d"))
    with RunStore(db) as store:
        store.record_run("r1", "demo-etl", "1.0", "success", base, base + 10, {})
        store.record_run("r2", "demo-etl", "1.0", "failed", base + 100, base + 110, {})
        store.record_run("r3", "other", "1.0", "success", base + 200, base + 210, {})
        store.record_run("r4", "demo-export", "1.0", "failed",
                         base + 86400, base + 86400 + 10, {})
        store.record_run("r5", "demo-etl", "1.0", "success",
                         base + 2 * 86400, base + 2 * 86400 + 10, {})
        store.record_run("r6", "demo-etl", "1.0", "success",
                         base + 3 * 86400, base + 3 * 86400 + 10, {})


def _report_dict() -> dict:
    """全量报告 dict：params/warnings/失败节点（traceback 含 <script> 验证转义）。"""
    return {
        "pipeline": "demo", "version": "1.0", "run_id": "rep-1", "status": "failed",
        "duration": 5.0, "started_at": "2026-11-20T10:00:00",
        "params": {"n": 3, "path": "/tmp/x"},
        "outputs": {}, "warnings": ["磁盘空间不足", "警告 <b>粗体</b>"],
        "artifacts_dir": "",
        "nodes": [
            {"id": "load", "op": "io.read_csv", "status": "failed", "duration": 1.0,
             "attempts": 2, "cached": False, "resumed": False, "outputs": {},
             "error": "文件不存在", "note": "",
             "traceback": "Traceback (most recent call last):\n"
                          '  File "x.py", line 1, in boom\n    raise ValueError\n'
                          "<script>alert(1)</script>"},
            {"id": "ok", "op": "control.const", "status": "success", "duration": 0.5,
             "attempts": 1, "cached": False, "resumed": False, "outputs": {},
             "error": None, "note": "", "traceback": ""},
        ],
    }


def _seed_report(db: Path) -> None:
    """rep-1 带完整报告（失败节点含 <script> traceback）；rep-null 报告未入库。"""
    from op_pipeline import RunStore

    with RunStore(db) as store:
        store.record_run("rep-1", "demo", "1.0", "failed", 1000.0, 1005.0,
                         {"n": 3},
                         report_json=json.dumps(_report_dict(), ensure_ascii=False))
        store.record_run("rep-null", "demo", "1.0", "success", 2000.0, 2005.0, {})


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

    def _get_any(self, path: str) -> tuple[int, str, str]:
        """返回 (状态码, Content-Type, 文本)；4xx 也返回而不抛异常。"""
        try:
            with urllib.request.urlopen(self.base + path, timeout=5) as resp:
                return (resp.status, resp.headers.get("Content-Type", ""),
                        resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            return (exc.code, exc.headers.get("Content-Type", ""),
                    exc.read().decode("utf-8"))

    @staticmethod
    def _row_count(page: str) -> int:
        """列表页 run 链接数量（每行一个 /run/ 链接）。"""
        return page.count("href='/run/")


class WebFilterListTest(_ServerCase):
    """Task 12.1：pipeline/since/until 筛选及其组合。"""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.db = Path(self._tmp.name) / "runs.db"
        _seed_filter(self.db)
        super().setUp()

    def tearDown(self) -> None:
        super().tearDown()
        self._tmp.cleanup()

    def test_pipeline_substring_match(self):
        page = self._get("/?pipeline=demo")
        self.assertEqual(self._row_count(page), 5, "demo-etl 4 条 + demo-export 1 条")
        self.assertIn("共 5 条", page)
        self.assertIn("demo-etl", page)
        self.assertIn("demo-export", page)
        self.assertNotIn(">other</td>", page)

    def test_pipeline_case_insensitive(self):
        page = self._get("/?pipeline=DEMO")
        self.assertEqual(self._row_count(page), 5, "大小写不敏感")

    def test_pipeline_literal_wildcards(self):
        """% 与 _ 按字面匹配（LIKE 通配符转义）：库里无此类流水线 → 0 条。"""
        page = self._get("/?pipeline=demo_etl")
        self.assertEqual(self._row_count(page), 0, "_ 应转义为字面量，不匹配 demo-etl")

    def test_since_until_inclusive(self):
        page = self._get("/?since=2026-11-19&until=2026-11-20")
        self.assertEqual(self._row_count(page), 2, "含 11-19（r4）与 11-20（r5）两天")
        self.assertIn("href='/run/r4'", page)
        self.assertIn("href='/run/r5'", page)
        self.assertNotIn("href='/run/r1'", page)
        self.assertNotIn("href='/run/r6'", page)
        self.assertIn("起始 2026-11-19", page)
        self.assertIn("截止 2026-11-20", page)

    def test_since_only(self):
        page = self._get("/?since=2026-11-20")
        self.assertEqual(self._row_count(page), 2, "r5（11-20）与 r6（11-21）")

    def test_invalid_dates_ignored(self):
        page = self._get("/?since=abc&until=2026-13-40")
        self.assertEqual(self._row_count(page), 6, "非法日期应忽略")

    def test_pipeline_plus_status_plus_dates(self):
        page = self._get("/?pipeline=demo&since=2026-11-19&until=2026-11-20"
                         "&status=failed")
        self.assertEqual(self._row_count(page), 1, "仅 r4（demo-export failed 11-19）")
        self.assertIn("href='/run/r4'", page)
        self.assertIn("状态 failed", page)
        self.assertIn("流水线 demo", page)

    def test_pager_keeps_pipeline_filter(self):
        page = self._get("/?pipeline=demo&size=2")
        self.assertIn("page=2", page)
        self.assertIn("pipeline=demo", page)


class WebFilterReportTest(_ServerCase):
    """Task 12.2：详情页报告折叠区块（渲染 / 占位 / HTML 转义）。"""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.db = Path(self._tmp.name) / "runs.db"
        _seed_report(self.db)
        super().setUp()

    def tearDown(self) -> None:
        super().tearDown()
        self._tmp.cleanup()

    def test_report_section_present(self):
        page = self._get("/run/rep-1")
        self.assertIn("运行报告", page)
        self.assertIn("报告（参数 / 告警 / 失败节点）", page)

    def test_params_rendered_escaped(self):
        page = self._get("/run/rep-1")
        self.assertIn("&quot;n&quot;: 3", page)
        self.assertIn("&quot;path&quot;", page)

    def test_warnings_rendered_escaped(self):
        page = self._get("/run/rep-1")
        self.assertIn("磁盘空间不足", page)
        self.assertIn("&lt;b&gt;粗体&lt;/b&gt;", page, "warnings 动态文本须转义")
        self.assertNotIn("<b>粗体</b>", page)

    def test_failed_node_traceback_escaped(self):
        page = self._get("/run/rep-1")
        self.assertIn("节点 load（io.read_csv）", page)
        self.assertIn("文件不存在", page)
        self.assertIn("&lt;script&gt;alert(1)&lt;/script&gt;", page,
                      "traceback 中 <script> 须转义")
        self.assertNotIn("<script>alert(1)</script>", page)

    def test_null_report_shows_placeholder(self):
        page = self._get("/run/rep-null")
        self.assertIn("报告未入库", page)


class WebFilterReportApiTest(_ServerCase):
    """Task 12.3：``/api/runs/<id>/report`` 只读 JSON 与 404。"""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.db = Path(self._tmp.name) / "runs.db"
        _seed_report(self.db)
        super().setUp()

    def tearDown(self) -> None:
        super().tearDown()
        self._tmp.cleanup()

    def test_report_api_returns_full_object(self):
        code, ctype, body = self._get_any("/api/runs/rep-1/report")
        self.assertEqual(code, 200)
        self.assertIn("application/json", ctype)
        report = json.loads(body)["report"]
        self.assertEqual(report["run_id"], "rep-1")
        self.assertEqual(report["params"], {"n": 3, "path": "/tmp/x"})
        self.assertEqual(report["warnings"], ["磁盘空间不足", "警告 <b>粗体</b>"])
        nodes = {n["id"]: n for n in report["nodes"]}
        self.assertIn("<script>", nodes["load"]["traceback"],
                      "JSON 原文不转义（转义只发生在 HTML 渲染层）")

    def test_report_api_null_report(self):
        code, _, body = self._get_any("/api/runs/rep-null/report")
        self.assertEqual(code, 200)
        self.assertIsNone(json.loads(body)["report"], "未入库 → report 为 null")

    def test_report_api_unknown_run_404(self):
        code, ctype, body = self._get_any("/api/runs/nope/report")
        self.assertEqual(code, 404)
        self.assertIn("application/json", ctype)
        self.assertIn("nope", json.loads(body)["error"])


if __name__ == "__main__":
    unittest.main()
