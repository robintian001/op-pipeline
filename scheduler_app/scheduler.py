"""调度守护进程（M5-2 Task 5 / M6-2 Task 6）：定时触发 ``op_pipeline.run``，结果写 RunStore。

- 命令行：``python -m scheduler_app.scheduler --pipeline <cfg> --every <秒>``，
  可选 ``--registry/--plugin/--workdir/--cache-dir/--db``；
- M6：``--pipeline`` 可重复（多流水线每轮按顺序全部触发）；``--alert-webhook``
  在运行失败时 POST JSON 告警；``--keep-runs`` 每轮结束后清理历史运行记录；
- M7-3：``--alert-silence <秒>`` 告警静默窗口：同一 pipeline 在窗口内失败只发送
  一次告警（内存记录上次发送时刻，进程内有效，多 pipeline 互不影响）；
- 启动后立即执行一次，之后每 ``--every`` 秒执行一次；
- 每次运行结果写入 RunStore（复用 M2-5 SQLite 运行记录库）；
- Ctrl+C / KeyboardInterrupt 干净退出：打印退出信息，退出码 0；
- 内部用 ``threading.Event`` 提供可测试的停止机制；调度循环在主线程串行执行，
  可编程入口 :func:`run_scheduler` 供测试直接调用，CLI 只是薄封装。

仅使用标准库（argparse / json / threading / time / urllib.request）。
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import threading
import time
import urllib.request
from pathlib import Path
from typing import Any, Sequence

log = logging.getLogger("scheduler_app.scheduler")

_ALERT_TIMEOUT = 5.0  # 告警 HTTP 超时（秒）


def run_scheduler(
    pipeline: str | Path | Sequence[str | Path],
    every: float,
    *,
    registry: Any = None,
    plugin: tuple[str, ...] = (),
    plugin_modules: tuple[str, ...] = (),
    workdir: str | Path | None = None,
    cache_dir: str | Path | None = ".pipeline_cache",
    db: str | Path | None = None,
    alert_webhook: str | None = None,
    alert_silence: float | None = None,
    keep_runs: int | None = None,
    stop_event: threading.Event | None = None,
    logger: logging.Logger | None = None,
) -> int:
    """按固定间隔反复执行流水线，返回**累计触发次数**（每执行一条流水线计 1 次）。

    参数（全部透传给 :func:`op_pipeline.run`）：
    - ``pipeline``：流水线配置文件路径；可为路径序列（每轮按顺序全部触发），
      单个 ``str`` / ``Path`` 等价于只含一条的序列（v0.6 兼容）；
    - ``every``：触发间隔（秒），启动后立即执行一次；
    - ``registry``：``Registry`` 实例（None = 使用全局默认算子表）；
    - ``plugin``：插件**文件路径**元组（相对配置文件所在目录解析）；
    - ``plugin_modules``：插件**模块路径**元组（CLI ``--registry`` 映射到此）；
    - ``workdir`` / ``cache_dir``：工作目录与缓存目录；
    - ``db``：RunStore SQLite 路径；给定则每次运行结果落库，None = 不落库；
    - ``alert_webhook``：告警 Webhook URL；运行状态非 success 或抛异常时 POST
      ``{"pipeline","run_id","status","error"}``，发送失败只记 warning；
    - ``alert_silence``：告警静默窗口（秒）；同一 pipeline 距上次实际发送不足该
      时长时跳过告警，``None`` 或 ``<= 0`` 表示不静默（与 M6 行为一致）；
    - ``keep_runs``：每轮结束后保留最近 N 条运行记录（``RunStore.prune``）；
      ``None`` 或 ``<= 0`` 表示不清理；
    - ``stop_event``：置位后当前轮结束即退出循环（默认内部自建）；
    - ``logger``：日志器（默认 ``scheduler_app.scheduler``）。

    返回累计触发次数。单次运行失败不中断循环：异常记入日志，等待下一轮。
    调度循环在主线程串行执行；``KeyboardInterrupt`` 会在循环层被捕获，
    以当前累计次数正常返回（配合 CLI 打印退出信息）。
    """
    if every <= 0:
        raise ValueError(f"every 必须是正数，实际是 {every!r}")
    pipelines = _normalize_pipelines(pipeline)
    logger = logger or log
    stop = stop_event if stop_event is not None else threading.Event()
    # 复用同一个 SQLite 连接（调度循环串行，连接只在主线程使用）；
    # op_pipeline 延迟导入：CLI --help 等轻量场景不加载内核
    from op_pipeline import RunStore

    store = RunStore(db) if db is not None else None
    # M7-3：告警静默窗口的内存记录（pipeline_key -> 上次实际发送的 monotonic）
    last_alert_at: dict[str, float] = {}
    count = 0
    try:
        while not stop.is_set():
            for cfg in pipelines:
                ok, info = _run_once(
                    cfg,
                    registry=registry,
                    plugin=plugin,
                    plugin_modules=plugin_modules,
                    workdir=workdir,
                    cache_dir=cache_dir,
                    store=store,
                    logger=logger,
                )
                count += 1
                if not ok and alert_webhook:
                    _maybe_alert(
                        alert_webhook, cfg, info, alert_silence,
                        last_alert_at, logger,
                    )
                if stop.is_set():
                    break
            # M6：每轮结束（全部流水线跑完）后按保留策略清理历史
            if keep_runs is not None and keep_runs > 0 and store is not None:
                try:
                    deleted = store.prune(keep_runs)
                except Exception as exc:  # noqa: BLE001 - 清理失败不中断守护进程
                    logger.warning("历史清理失败：%s: %s", type(exc).__name__, exc)
                else:
                    if deleted:
                        logger.info("历史清理：删除 %d 条较早的运行记录", deleted)
            # 间隔从本轮结束开始计时；stop 置位时 wait 立即返回并退出
            if stop.wait(every):
                break
    except KeyboardInterrupt:
        logger.info("收到 KeyboardInterrupt，调度循环结束")
    finally:
        if store is not None:
            store.close()
    logger.info("调度器退出：共触发 %d 次", count)
    return count


def _normalize_pipelines(
    pipeline: str | Path | Sequence[str | Path],
) -> list[str | Path]:
    """把单路径或序列统一成列表（``str``/``Path`` 视为单条，v0.6 兼容）。"""
    if isinstance(pipeline, (str, Path)):
        return [pipeline]
    items = list(pipeline)
    if not items:
        raise ValueError("pipeline 不能为空序列")
    return items


def _maybe_alert(
    webhook: str,
    pipeline: str | Path,
    info: dict[str, str],
    alert_silence: float | None,
    last_alert_at: dict[str, float],
    logger: logging.Logger,
) -> None:
    """M7-3：按静默窗口决定是否发送告警。

    ``alert_silence`` 为 ``None``/``<= 0`` 时行为与 M6 完全一致（每次失败都发）；
    否则同一 pipeline 距上次实际发送不足 ``alert_silence`` 秒时跳过并记 info 日志。
    多 pipeline 以配置路径为 key，互不影响。
    """
    key = str(pipeline)
    silence = alert_silence if alert_silence and alert_silence > 0 else None
    if silence is not None:
        last = last_alert_at.get(key)
        now = time.monotonic()
        if last is not None and (now - last) < silence:
            logger.info(
                "告警静默：pipeline=%s 距上次告警 %.2fs < %.2fs，跳过本轮",
                key, now - last, silence,
            )
            return
    _send_alert(webhook, pipeline, info, logger)
    last_alert_at[key] = time.monotonic()


def _send_alert(
    webhook: str,
    pipeline: str | Path,
    info: dict[str, str],
    logger: logging.Logger,
) -> None:
    """POST JSON 告警；任何异常只记 warning，绝不影响调度循环。"""
    payload = {
        "pipeline": str(pipeline),
        "run_id": info.get("run_id") or "",
        "status": info.get("status") or "error",
        "error": info.get("error") or "",
    }
    try:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        request = urllib.request.Request(
            webhook,
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=_ALERT_TIMEOUT) as response:
            response.read()
    except Exception as exc:  # noqa: BLE001 - 告警失败必须隔离
        logger.warning(
            "告警发送失败（不影响调度）：%s: %s", type(exc).__name__, exc
        )


def _run_once(
    pipeline: str | Path,
    *,
    registry: Any,
    plugin: tuple[str, ...],
    plugin_modules: tuple[str, ...],
    workdir: str | Path | None,
    cache_dir: str | Path | None,
    store: RunStore | None,
    logger: logging.Logger,
) -> tuple[bool, dict[str, str]]:
    """执行一次流水线并落库，返回 ``(是否成功, 运行摘要)``。

    失败只记日志，不中断调度循环；摘要供告警使用
    （``run_id`` / ``status`` / ``error``）。``KeyboardInterrupt`` 原样向上传播
    （由循环层统一收口）。
    """
    try:
        from op_pipeline import run

        report, _values = run(
            pipeline,
            registry=registry,
            workdir=workdir,
            cache_dir=cache_dir,
            plugins=plugin,
            plugin_modules=plugin_modules,
            run_store=store,
            logger=logger,
        )
        logger.info(
            "触发成功：run_id=%s status=%s 耗时 %.2fs（%d 个节点）",
            report.run_id, report.status, report.duration, len(report.nodes),
        )
        if report.status == "success":
            return True, {"run_id": report.run_id, "status": report.status, "error": ""}
        error = next(
            (n.error for n in report.nodes if n.status == "failed" and n.error), ""
        )
        return False, {
            "run_id": report.run_id,
            "status": report.status,
            "error": error,
        }
    except KeyboardInterrupt:
        raise
    except Exception as exc:  # noqa: BLE001 - 单轮失败必须隔离，守护进程照常运转
        logger.error("本轮运行失败：%s: %s", type(exc).__name__, exc)
        logger.debug("失败详情：", exc_info=True)
        return False, {
            "run_id": "",
            "status": "error",
            "error": f"{type(exc).__name__}: {exc}",
        }


# --------------------------------------------------------------------------- #
# 命令行薄封装
# --------------------------------------------------------------------------- #
def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="scheduler_app.scheduler",
        description="op_pipeline 调度守护进程：定时执行流水线并把结果写入 RunStore。",
    )
    parser.add_argument(
        "--pipeline", action="append", required=True, metavar="PATH",
        help="流水线配置文件路径（可重复，每轮按顺序全部触发）",
    )
    parser.add_argument(
        "--every", type=float, required=True, help="触发间隔（秒），启动后立即执行一次"
    )
    parser.add_argument(
        "--registry", action="append", default=[], metavar="MODULE",
        help="模块路径插件（可重复），加载进算子注册表",
    )
    parser.add_argument(
        "--plugin", action="append", default=[], metavar="PATH",
        help="插件文件路径（可重复），相对配置文件所在目录解析",
    )
    parser.add_argument(
        "--workdir", default=None, help="工作目录（默认取配置文件所在目录）"
    )
    parser.add_argument(
        "--cache-dir", default=".pipeline_cache", help="缓存目录（默认 .pipeline_cache）"
    )
    parser.add_argument(
        "--db", default=None, help="RunStore SQLite 路径（默认不落库）"
    )
    parser.add_argument(
        "--alert-webhook", default=None, metavar="URL",
        help="告警 Webhook：运行失败时 POST JSON 到该地址（默认不告警）",
    )
    parser.add_argument(
        "--alert-silence", type=float, default=None, metavar="SECONDS",
        help="告警静默窗口（秒）：同 pipeline 窗口内失败只告警一次（默认不静默）",
    )
    parser.add_argument(
        "--keep-runs", type=int, default=None, metavar="N",
        help="每轮结束后仅保留最近 N 条运行记录（N<=0 表示不清理）",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """CLI 入口：``python -m scheduler_app.scheduler ...``，返回退出码。"""
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-7s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    stop = threading.Event()
    count = run_scheduler(
        args.pipeline,
        args.every,
        plugin=tuple(args.plugin),
        plugin_modules=tuple(args.registry),
        workdir=args.workdir,
        cache_dir=args.cache_dir,
        db=args.db,
        alert_webhook=args.alert_webhook,
        alert_silence=args.alert_silence,
        keep_runs=args.keep_runs,
        stop_event=stop,
    )
    print(f"调度器已停止，共触发 {count} 次（退出码 0）。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
