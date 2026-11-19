"""告警静默窗口测试（M7-3 Task 9）：``run_scheduler(alert_silence=...)``。

运行：``python -m unittest discover -s tests -k test_scheduler_silence -v``

覆盖：
- 同一失败 pipeline 在窗口内连续多轮只 POST 一次；
- 窗口过期后再次失败重新 POST；
- 两条不同 pipeline 各自计数、互不抑制；
- 未配置 ``alert_silence`` 时行为与 M6 一致（每轮都发）；
- CLI ``--alert-silence`` 解析与默认值。

用本地 ``http.server`` 收包计数，``stop_event`` 控制调度循环退出，
``finally`` 中停服务/线程，保证测试 < 30s。
"""
from __future__ import annotations

import json
import sys
import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

FAIL_CFG = {
    "name": "silence_fail",
    "version": "1.0",
    "nodes": [
        {"id": "load", "op": "io.read_csv", "params": {"path": "nope.csv"}},
    ],
    "outputs": {},
}


def _write_cfg(directory: Path, name: str, pipeline_name: str) -> Path:
    path = directory / f"{name}.json"
    path.write_text(
        json.dumps({**FAIL_CFG, "name": pipeline_name}, ensure_ascii=False),
        encoding="utf-8",
    )
    return path


def _wait(predicate, timeout: float = 20.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(0.02)
    return False


class _AlertServer:
    """本地告警接收端：按 pipeline 统计 POST 次数。"""

    def __init__(self) -> None:
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:  # noqa: N802 - http.server 协议命名
                length = int(self.headers.get("Content-Length") or 0)
                body = self.rfile.read(length)
                outer.payloads.append(json.loads(body.decode("utf-8")))
                self.send_response(204)
                self.end_headers()

            def log_message(self, fmt: str, *args: object) -> None:
                pass

        self.payloads: list[dict] = []
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}/alert"
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    def count(self, pipeline: str | None = None) -> int:
        if pipeline is None:
            return len(self.payloads)
        return sum(1 for p in self.payloads if p.get("pipeline") == pipeline)

    def __enter__(self) -> "_AlertServer":
        self.thread.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)


def _start_scheduler(pipeline, **kwargs):
    """后台线程跑调度器，返回 (线程, stop_event, 结果列表, 异常列表)。"""
    from scheduler_app import run_scheduler

    stop = threading.Event()
    results: list[int] = []
    errors: list[BaseException] = []

    def worker() -> None:
        try:
            results.append(run_scheduler(pipeline, every=0.05, stop_event=stop, **kwargs))
        except BaseException as exc:  # noqa: BLE001 - 测试需捕获所有异常
            errors.append(exc)

    thread = threading.Thread(target=worker)
    thread.start()
    return thread, stop, results, errors


def _run_count(db: Path) -> int:
    from op_pipeline import RunStore

    with RunStore(db) as store:
        return store.query("SELECT COUNT(*) FROM runs")[0][0]


class SilenceWindowTest(unittest.TestCase):
    """Task 9：窗口内只告警一次。"""

    def test_same_pipeline_alerts_once_within_window(self):
        with tempfile.TemporaryDirectory() as d:
            d = Path(d)
            cfg = _write_cfg(d, "fail", "silence_fail")
            db = d / "runs.db"
            with _AlertServer() as alert:
                thread, stop, results, errors = _start_scheduler(
                    str(cfg), db=str(db), cache_dir=None,
                    alert_webhook=alert.url, alert_silence=60.0,
                )
                try:
                    self.assertTrue(
                        _wait(lambda: _run_count(db) >= 3),
                        "窗口内应至少跑满 3 轮失败",
                    )
                    time.sleep(0.1)  # 留出告警线程收包时间
                finally:
                    stop.set()
                    thread.join(timeout=10)
            self.assertFalse(thread.is_alive())
            self.assertEqual(errors, [])
            self.assertEqual(alert.count(), 1, "窗口内同 pipeline 只应发送 1 次告警")

    def test_alert_resumes_after_window(self):
        with tempfile.TemporaryDirectory() as d:
            d = Path(d)
            cfg = _write_cfg(d, "fail", "silence_fail")
            db = d / "runs.db"
            with _AlertServer() as alert:
                thread, stop, results, errors = _start_scheduler(
                    str(cfg), db=str(db), cache_dir=None,
                    alert_webhook=alert.url, alert_silence=0.15,
                )
                try:
                    self.assertTrue(_wait(lambda: alert.count() >= 1), "首轮应告警")
                    time.sleep(0.3)  # 越过静默窗口，后续轮次应重新发送
                    self.assertTrue(
                        _wait(lambda: alert.count() >= 2), "窗口外应重新告警"
                    )
                finally:
                    stop.set()
                    thread.join(timeout=10)
            self.assertFalse(thread.is_alive())
            self.assertEqual(errors, [])

    def test_different_pipelines_are_independent(self):
        with tempfile.TemporaryDirectory() as d:
            d = Path(d)
            cfg_a = _write_cfg(d, "a", "silence_a")
            cfg_b = _write_cfg(d, "b", "silence_b")
            db = d / "runs.db"
            with _AlertServer() as alert:
                thread, stop, results, errors = _start_scheduler(
                    [str(cfg_a), str(cfg_b)], db=str(db), cache_dir=None,
                    alert_webhook=alert.url, alert_silence=60.0,
                )
                try:
                    self.assertTrue(
                        _wait(lambda: _run_count(db) >= 6),
                        "两条 pipeline 应各跑满 3 轮",
                    )
                    time.sleep(0.1)
                finally:
                    stop.set()
                    thread.join(timeout=10)
            self.assertFalse(thread.is_alive())
            self.assertEqual(errors, [])
            self.assertEqual(alert.count(), 2, "两条 pipeline 各告警 1 次")
            self.assertEqual(alert.count(str(cfg_a)), 1)
            self.assertEqual(alert.count(str(cfg_b)), 1)

    def test_without_silence_alerts_every_round(self):
        with tempfile.TemporaryDirectory() as d:
            d = Path(d)
            cfg = _write_cfg(d, "fail", "silence_fail")
            db = d / "runs.db"
            with _AlertServer() as alert:
                thread, stop, results, errors = _start_scheduler(
                    str(cfg), db=str(db), cache_dir=None,
                    alert_webhook=alert.url,
                )
                try:
                    self.assertTrue(
                        _wait(lambda: alert.count() >= 2),
                        "未配置静默时每轮失败都应告警",
                    )
                finally:
                    stop.set()
                    thread.join(timeout=10)
            self.assertFalse(thread.is_alive())
            self.assertEqual(errors, [])
            self.assertGreaterEqual(alert.count(), 2)


class CliAlertSilenceTest(unittest.TestCase):
    """Task 9：CLI ``--alert-silence`` 解析与默认值。"""

    def test_parse_args_with_silence(self):
        from scheduler_app.scheduler import parse_args

        args = parse_args([
            "--pipeline", "a.json", "--every", "5", "--alert-silence", "30.5",
        ])
        self.assertAlmostEqual(args.alert_silence, 30.5)

    def test_parse_args_default_is_none(self):
        from scheduler_app.scheduler import parse_args

        args = parse_args(["--pipeline", "a.json", "--every", "1"])
        self.assertIsNone(args.alert_silence)


if __name__ == "__main__":
    unittest.main()
