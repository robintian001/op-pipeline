"""调度器增强测试（M6-2 Task 6）：多流水线、告警 Webhook、历史保留。

运行：``python -m unittest discover -s tests -k test_scheduler_m6 -v``

覆盖：
- 序列入参每轮按顺序触发全部流水线；单字符串入参保持 v0.6 兼容；
- 运行失败时向本地 ``http.server`` POST JSON 告警（字段校验）；
- 告警地址不可达只记 warning，调度循环继续；
- ``keep_runs`` 每轮结束后调用 ``RunStore.prune``；
- CLI ``--pipeline`` 可重复、``--alert-webhook``/``--keep-runs`` 解析正确。

遵循现有测试风格：仅文件顶部做路径准备，业务 import 放在测试方法内。
"""
from __future__ import annotations

import json
import socket
import sys
import tempfile
import threading
import time
import unittest
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

MINI_CFG = {
    "name": "sched_multi",
    "version": "1.0",
    "nodes": [
        {"id": "seed", "op": "control.const", "params": {"value": {"x": 1}}},
        {"id": "pass", "op": "control.passthrough", "inputs": {"value": "${seed.value}"}},
    ],
    "outputs": {"out": "${pass.value}"},
}

FAIL_CFG = {
    "name": "sched_fail",
    "version": "1.0",
    "nodes": [
        {"id": "load", "op": "io.read_csv", "params": {"path": "nope.csv"}},
    ],
    "outputs": {},
}


def _write_cfg(directory: Path, name: str, cfg: dict) -> Path:
    path = directory / f"{name}.json"
    path.write_text(json.dumps(cfg, ensure_ascii=False), encoding="utf-8")
    return path


def _closed_port() -> int:
    """取一个刚释放的本地端口，连接时预期被拒绝。"""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _wait(predicate, timeout: float = 20.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(0.05)
    return False


class _AlertServer:
    """本地告警接收端：收集 POST JSON 请求体。"""

    def __init__(self) -> None:
        outer = self
        self.payloads: list[dict] = []
        self.received = threading.Event()

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:  # noqa: N802 - http.server 协议命名
                length = int(self.headers.get("Content-Length") or 0)
                body = self.rfile.read(length)
                outer.payloads.append(json.loads(body.decode("utf-8")))
                outer.received.set()
                self.send_response(204)
                self.end_headers()

            def log_message(self, fmt: str, *args: object) -> None:
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}/alert"
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    def __enter__(self) -> "_AlertServer":
        self.thread.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)


def _start_scheduler(pipeline, **kwargs) -> tuple[threading.Thread, list, threading.Event, list]:
    """后台线程跑调度器，返回 (线程, 结果列表, stop_event, 异常列表)。"""
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
    return thread, results, stop, errors


class MultiPipelineTest(unittest.TestCase):
    """Task 6：多流水线按顺序触发 + 单值兼容。"""

    def test_sequence_triggers_all_pipelines_each_cycle(self):
        from op_pipeline import RunStore

        with tempfile.TemporaryDirectory() as d:
            d = Path(d)
            cfg_a = _write_cfg(d, "a", MINI_CFG)
            cfg_b = _write_cfg(d, "b", {**MINI_CFG, "name": "sched_multi_b"})
            db = d / "runs.db"
            thread, results, stop, errors = _start_scheduler(
                [str(cfg_a), str(cfg_b)], db=str(db), cache_dir=None,
            )
            try:
                def _two_runs() -> bool:
                    with RunStore(db) as probe:
                        return probe.query("SELECT COUNT(*) FROM runs")[0][0] >= 2

                self.assertTrue(_wait(_two_runs), "一轮应触发两条流水线")
            finally:
                stop.set()
                thread.join(timeout=10)
            self.assertFalse(thread.is_alive())
            self.assertEqual(errors, [], f"调度线程不应抛异常：{errors}")
            self.assertGreaterEqual(results[0], 2)

            with RunStore(db) as store:
                names = {row[0] for row in store.query("SELECT DISTINCT pipeline FROM runs")}
            self.assertEqual(len(names), 2, f"两条流水线都应落库：{names}")

    def test_single_string_pipeline_backward_compatible(self):
        from op_pipeline import RunStore

        with tempfile.TemporaryDirectory() as d:
            d = Path(d)
            cfg = _write_cfg(d, "solo", MINI_CFG)
            db = d / "runs.db"
            thread, results, stop, errors = _start_scheduler(
                str(cfg), db=str(db), cache_dir=None,
            )
            try:
                def _one_run() -> bool:
                    with RunStore(db) as probe:
                        return probe.query("SELECT COUNT(*) FROM runs")[0][0] >= 1

                self.assertTrue(_wait(_one_run))
            finally:
                stop.set()
                thread.join(timeout=10)
            self.assertEqual(errors, [])
            self.assertGreaterEqual(results[0], 1)


class AlertTest(unittest.TestCase):
    """Task 6：失败告警 POST JSON；告警异常隔离。"""

    def test_failed_run_posts_alert_payload(self):
        with tempfile.TemporaryDirectory() as d:
            d = Path(d)
            cfg = _write_cfg(d, "fail", FAIL_CFG)
            with _AlertServer() as alert:
                thread, results, stop, errors = _start_scheduler(
                    str(cfg), db=str(d / "runs.db"), cache_dir=None,
                    alert_webhook=alert.url,
                )
                try:
                    self.assertTrue(alert.received.wait(timeout=20), "失败运行应发送告警")
                finally:
                    stop.set()
                    thread.join(timeout=10)
            self.assertEqual(errors, [])
            self.assertGreaterEqual(results[0], 1)
            payload = alert.payloads[0]
            self.assertEqual(set(payload), {"pipeline", "run_id", "status", "error"})
            self.assertEqual(payload["pipeline"], str(cfg))
            self.assertTrue(payload["run_id"], "失败运行应有 run_id")
            self.assertEqual(payload["status"], "failed")
            self.assertIn("FileNotFoundError", payload["error"])

    def test_alert_unreachable_does_not_stop_scheduler(self):
        with tempfile.TemporaryDirectory() as d:
            d = Path(d)
            cfg = _write_cfg(d, "fail", FAIL_CFG)
            url = f"http://127.0.0.1:{_closed_port()}/alert"
            thread, results, stop, errors = _start_scheduler(
                str(cfg), db=str(d / "runs.db"), cache_dir=None,
                alert_webhook=url,
            )
            try:
                time.sleep(1.0)  # 至少完整跑过 1~2 轮
            finally:
                stop.set()
                thread.join(timeout=10)
            self.assertFalse(thread.is_alive())
            self.assertEqual(errors, [], "告警不可达不得抛出异常")
            self.assertGreaterEqual(results[0], 1, "告警失败后调度应继续")


class KeepRunsTest(unittest.TestCase):
    """Task 6：每轮结束后按 keep_runs 清理历史。"""

    def test_keep_runs_prunes_old_history(self):
        from op_pipeline import RunStore

        with tempfile.TemporaryDirectory() as d:
            d = Path(d)
            cfg = _write_cfg(d, "mini", MINI_CFG)
            db = d / "runs.db"
            with RunStore(db) as store:
                for i in range(5):
                    store.record_run(
                        f"old-{i}", "history", "1.0", "success",
                        1000.0 + i, 1000.5 + i, {},
                    )
            thread, results, stop, errors = _start_scheduler(
                str(cfg), db=str(db), cache_dir=None, keep_runs=2,
            )
            try:
                def _new_run_recorded() -> bool:
                    with RunStore(db) as probe:
                        return probe.query(
                            "SELECT COUNT(*) FROM runs WHERE pipeline != 'history'"
                        )[0][0] >= 1

                self.assertTrue(_wait(_new_run_recorded))
                # 等本轮 prune 生效（新记录已写入后同一轮内立即清理）
                self.assertTrue(_wait(lambda: _count(db) <= 2))
            finally:
                stop.set()
                thread.join(timeout=10)
            self.assertEqual(errors, [])
            self.assertLessEqual(_count(db), 2, "keep_runs=2 应只保留最近 2 条")
            with RunStore(db) as store:
                names = [row[0] for row in store.query("SELECT pipeline FROM runs")]
            self.assertIn(MINI_CFG["name"], names, "最新一轮记录应保留")

    def test_keep_runs_non_positive_means_no_prune(self):
        from op_pipeline import RunStore

        with tempfile.TemporaryDirectory() as d:
            d = Path(d)
            cfg = _write_cfg(d, "mini", MINI_CFG)
            db = d / "runs.db"
            with RunStore(db) as store:
                for i in range(3):
                    store.record_run(
                        f"old-{i}", "history", "1.0", "success",
                        1000.0 + i, 1000.5 + i, {},
                    )
            thread, results, stop, errors = _start_scheduler(
                str(cfg), db=str(db), cache_dir=None, keep_runs=0,
            )
            try:
                self.assertTrue(_wait(lambda: _count_pipeline(db, MINI_CFG["name"]) >= 1))
            finally:
                stop.set()
                thread.join(timeout=10)
            self.assertEqual(errors, [])
            self.assertGreaterEqual(_count(db), 4, "keep_runs<=0 不应清理历史")


def _count(db: Path) -> int:
    from op_pipeline import RunStore

    with RunStore(db) as store:
        return store.query("SELECT COUNT(*) FROM runs")[0][0]


def _count_pipeline(db: Path, name: str) -> int:
    from op_pipeline import RunStore

    with RunStore(db) as store:
        return store.query(
            "SELECT COUNT(*) FROM runs WHERE pipeline = ?", (name,)
        )[0][0]


class CliArgsTest(unittest.TestCase):
    """Task 6：CLI 多 --pipeline 与告警/保留参数。"""

    def test_parse_args_multi_pipeline(self):
        from scheduler_app.scheduler import parse_args

        args = parse_args([
            "--pipeline", "a.json", "--pipeline", "b.json", "--every", "5",
            "--alert-webhook", "http://127.0.0.1:1/hook", "--keep-runs", "3",
        ])
        self.assertEqual(args.pipeline, ["a.json", "b.json"])
        self.assertEqual(args.alert_webhook, "http://127.0.0.1:1/hook")
        self.assertEqual(args.keep_runs, 3)

    def test_parse_args_defaults(self):
        from scheduler_app.scheduler import parse_args

        args = parse_args(["--pipeline", "one.json", "--every", "1"])
        self.assertEqual(args.pipeline, ["one.json"], "单值仍兼容")
        self.assertIsNone(args.alert_webhook)
        self.assertIsNone(args.keep_runs)


if __name__ == "__main__":
    unittest.main()
