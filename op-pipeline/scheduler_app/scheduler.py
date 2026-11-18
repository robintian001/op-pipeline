"""调度守护进程（M5-2 Task 5）：定时触发 ``op_pipeline.run``，结果写 RunStore。

- 命令行：``python -m scheduler_app.scheduler --pipeline <cfg> --every <秒>``，
  可选 ``--registry/--plugin/--workdir/--cache-dir/--db``；
- 启动后立即执行一次，之后每 ``--every`` 秒执行一次；
- 每次运行结果写入 RunStore（复用 M2-5 SQLite 运行记录库）；
- Ctrl+C / KeyboardInterrupt 干净退出：打印退出信息，退出码 0；
- 内部用 ``threading.Event`` 提供可测试的停止机制；调度循环在主线程串行执行，
  可编程入口 :func:`run_scheduler` 供测试直接调用，CLI 只是薄封装。

仅使用标准库（argparse / threading / time）。
"""
from __future__ import annotations

import argparse
import logging
import sys
import threading
import time
from pathlib import Path
from typing import Any

log = logging.getLogger("scheduler_app.scheduler")


def run_scheduler(
    pipeline: str | Path,
    every: float,
    *,
    registry: Any = None,
    plugin: tuple[str, ...] = (),
    plugin_modules: tuple[str, ...] = (),
    workdir: str | Path | None = None,
    cache_dir: str | Path | None = ".pipeline_cache",
    db: str | Path | None = None,
    stop_event: threading.Event | None = None,
    logger: logging.Logger | None = None,
) -> int:
    """按固定间隔反复执行流水线，返回**触发次数**。

    参数（全部透传给 :func:`op_pipeline.run`）：
    - ``pipeline``：流水线配置文件路径；
    - ``every``：触发间隔（秒），启动后立即执行一次；
    - ``registry``：``Registry`` 实例（None = 使用全局默认算子表）；
    - ``plugin``：插件**文件路径**元组（相对配置文件所在目录解析）；
    - ``plugin_modules``：插件**模块路径**元组（CLI ``--registry`` 映射到此）；
    - ``workdir`` / ``cache_dir``：工作目录与缓存目录；
    - ``db``：RunStore SQLite 路径；给定则每次运行结果落库，None = 不落库；
    - ``stop_event``：置位后当前轮结束即退出循环（默认内部自建）；
    - ``logger``：日志器（默认 ``scheduler_app.scheduler``）。

    返回累计触发次数。单次运行失败不中断循环：异常记入日志，等待下一轮。
    调度循环在主线程串行执行；``KeyboardInterrupt`` 会在循环层被捕获，
    以当前累计次数正常返回（配合 CLI 打印退出信息）。
    """
    if every <= 0:
        raise ValueError(f"every 必须是正数，实际是 {every!r}")
    logger = logger or log
    stop = stop_event if stop_event is not None else threading.Event()
    # 复用同一个 SQLite 连接（调度循环串行，连接只在主线程使用）；
    # op_pipeline 延迟导入：CLI --help 等轻量场景不加载内核
    from op_pipeline import RunStore

    store = RunStore(db) if db is not None else None
    count = 0
    try:
        while not stop.is_set():
            _run_once(
                pipeline,
                registry=registry,
                plugin=plugin,
                plugin_modules=plugin_modules,
                workdir=workdir,
                cache_dir=cache_dir,
                store=store,
                logger=logger,
            )
            count += 1
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
) -> None:
    """执行一次流水线并落库；失败只记日志，不中断调度循环。

    ``KeyboardInterrupt`` 原样向上传播（由循环层统一收口）。
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
    except KeyboardInterrupt:
        raise
    except Exception as exc:  # noqa: BLE001 - 单轮失败必须隔离，守护进程照常运转
        logger.error("本轮运行失败：%s: %s", type(exc).__name__, exc)
        logger.debug("失败详情：", exc_info=True)


# --------------------------------------------------------------------------- #
# 命令行薄封装
# --------------------------------------------------------------------------- #
def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="scheduler_app.scheduler",
        description="op_pipeline 调度守护进程：定时执行流水线并把结果写入 RunStore。",
    )
    parser.add_argument("--pipeline", required=True, help="流水线配置文件路径")
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
        stop_event=stop,
    )
    print(f"调度器已停止，共触发 {count} 次（退出码 0）。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
