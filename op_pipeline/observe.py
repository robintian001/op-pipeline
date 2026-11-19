"""观测性（M3-6）：基于 M2-7 钩子的结构化事件导出。

把一次运行的事件流导出为 JSON lines（NDJSON），每行一个 JSON 对象，
便于下游按行消费（grep / jq / 日志收集器）。同时提供可选的
OTel bridge（``OtelHook``，extras ``otel``），未安装 ``opentelemetry-api``
时只告警、不影响流水线（延迟导入，与 df.* 算子同一原则）。

用法::

    from op_pipeline import run_pipeline
    from op_pipeline.observe import JsonlHook

    hook = JsonlHook("runs/events.jsonl")
    report, values = run_pipeline(spec, hooks=[hook])
    hook.close()

事件行示例::

    {"event": "node_start", "ts": "2026-11-18T11:32:24", "node_id": "load", "op": "io.read_csv"}
    {"event": "node_finish", "ts": "2026-11-18T11:32:24", "node_id": "load", "status": "success",
     "duration": 0.07, "cached": false}
    {"event": "run_finish", "ts": "2026-11-18T11:32:24", "pipeline": "orders_daily",
     "run_id": "...", "status": "success", "duration": 0.17, "ok": true}

事件语义与 ``RunReport`` / ``NodeResult`` 一致（同一份事实的两个投影），
因此"导出的 JSONL 与报告一致"是自然成立的结构性保证。
"""
from __future__ import annotations

import json
import logging
import threading
import time
from pathlib import Path

logger = logging.getLogger("op_pipeline.observe")


def _iso_ts(epoch: float) -> str:
    """秒级 epoch -> ISO 8601 字符串（与 RunReport.to_dict 的格式一致）。"""
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(epoch))


class JsonlHook:
    """把 Hook 事件逐行追加写入 JSONL 文件。

    - 线程安全：节点并行执行时 ``on_node_finish`` 可能被并发调用，
      内部以锁保护单次写入（单行 json.dumps 原子性足够，行不交错）；
    - 事件与报告同源：``run_finish`` 行直接取自 ``RunReport``，
      ``node_finish`` 行的 status/cached 与报告节点一致；
    - 用完调用 :meth:`close` 关闭文件句柄（或依赖 GC / 进程退出）。
    """

    def __init__(self, path: str | Path, mode: str = "a") -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._file = self.path.open(mode, encoding="utf-8")
        self._lock = threading.Lock()

    def _write(self, event: dict) -> None:
        line = json.dumps(event, ensure_ascii=False, default=str)
        with self._lock:
            self._file.write(line + "\n")
            self._file.flush()

    def on_node_start(self, node_id: str, op: str) -> None:
        self._write(
            {
                "event": "node_start",
                "ts": _iso_ts(time.time()),
                "node_id": node_id,
                "op": op,
            }
        )

    def on_node_finish(self, node_id: str, status: str, duration: float, cached: bool) -> None:
        self._write(
            {
                "event": "node_finish",
                "ts": _iso_ts(time.time()),
                "node_id": node_id,
                "status": status,
                "duration": round(duration, 4),
                "cached": cached,
            }
        )

    def on_run_finish(self, report) -> None:
        self._write(
            {
                "event": "run_finish",
                "ts": _iso_ts(report.finished_at),
                "pipeline": report.pipeline,
                "version": report.version,
                "run_id": report.run_id,
                "status": report.status,
                "duration": round(report.duration, 4),
                "ok": report.ok,
            }
        )

    def close(self) -> None:
        with self._lock:
            if not self._file.closed:
                self._file.close()


class OtelHook:
    """OTel bridge（可选，extras ``otel``）：运行结束时把报告摘要记为 span。

    - ``opentelemetry-api`` 在事件触发时延迟导入；未安装只告警并跳过，
      绝不拖垮流水线（与 df.* 算子"未装 extras 不报错"同一原则）；
    - 通过 ``opentelemetry.trace.get_tracer("op_pipeline")`` 取 tracer，
      以 ``op_pipeline.run`` span 记录报告关键字段（pipeline/run_id/status/duration）。
    """

    def on_run_finish(self, report) -> None:
        try:
            from opentelemetry import trace as otel_trace  # type: ignore
        except ImportError:
            logger.warning(
                "opentelemetry-api 未安装：OtelHook 跳过 "
                "（pip install op-pipeline[otel] 后生效）"
            )
            return
        tracer = otel_trace.get_tracer("op_pipeline")
        span = tracer.start_span("op_pipeline.run")
        try:
            span.set_attribute("pipeline", report.pipeline)
            span.set_attribute("run_id", report.run_id)
            span.set_attribute("status", report.status)
            span.set_attribute("duration", round(report.duration, 4))
        finally:
            span.end()
