"""调度守护进程（M5-2 Task 5 / M6-2 Task 6）：定时触发 ``op_pipeline.run``，结果写 RunStore。

- 命令行：``python -m scheduler_app.scheduler --pipeline <cfg> --every <秒>``，
  可选 ``--registry/--plugin/--workdir/--cache-dir/--db``；
- M6：``--pipeline`` 可重复（多流水线每轮按顺序全部触发）；``--alert-webhook``
  在运行失败时 POST JSON 告警；``--keep-runs`` 每轮结束后清理历史运行记录；
- M7-3：``--alert-silence <秒>`` 告警静默窗口：同一 pipeline 在窗口内失败只发送
  一次告警（内存记录上次发送时刻，进程内有效，多 pipeline 互不影响）；
- M8-1：``--pipeline`` 条目支持 ``{"path": ..., "after": [...]}`` 依赖声明（pipeline
  名 = 配置文件去扩展名的 stem），CLI ``--after name=dep1,dep2``（可重复）追加依赖；
  依赖本轮全部运行且 success 才触发，否则跳过本轮并记 info 日志；依赖名未知启动期
  记 warning（不 fail），``after`` 成环抛 ``ConfigError``；无 ``after`` 的条目行为
  与 v0.8.0 完全一致；
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
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

log = logging.getLogger("scheduler_app.scheduler")

_ALERT_TIMEOUT = 5.0  # 告警 HTTP 超时（秒）


@dataclass(frozen=True)
class _PipelineEntry:
    """单条调度条目（M8-1）：配置路径 + 依赖的 pipeline 名列表。

    ``path`` 保留调用方传入的 ``str``/``Path`` 原样（透传给 :func:`op_pipeline.run`）；
    pipeline 名 = 配置文件去扩展名的 stem（如 ``a.toml`` → ``a``）。
    """

    path: str | Path
    after: tuple[str, ...] = ()

    @property
    def name(self) -> str:
        """pipeline 名：配置文件去扩展名的 stem。"""
        return Path(self.path).stem


def run_scheduler(
    pipeline: str | Path | Sequence[str | Path | dict[str, Any]],
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
    after: Mapping[str, Sequence[str]] | None = None,
    stop_event: threading.Event | None = None,
    logger: logging.Logger | None = None,
) -> int:
    """按固定间隔反复执行流水线，返回**累计触发次数**（每执行一条流水线计 1 次）。

    参数（全部透传给 :func:`op_pipeline.run`）：
    - ``pipeline``：流水线配置文件路径；可为**条目序列**（每轮按顺序全部触发），
      单个 ``str`` / ``Path`` 等价于只含一条的序列（v0.6 兼容）。条目（M8-1）支持
      ``str | Path``（无依赖）与 ``dict{"path": ..., "after": [...]}``：
      ``after`` 为依赖的 pipeline 名列表（名 = 配置文件去扩展名的 stem），
      仅当所有依赖**本轮已运行且 status == success** 才触发，否则跳过本轮并记
      info 日志；无 ``after`` 的条目每轮照常触发，行为与 v0.8.0 完全一致；
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
    - ``after``（M8-1）：CLI ``--after`` 的解析结果（名 -> 依赖列表），按 stem
      匹配追加到对应条目的依赖（排在条目自身声明之后）；默认 None = 不追加；
    - ``stop_event``：置位后当前轮结束即退出循环（默认内部自建）；
    - ``logger``：日志器（默认 ``scheduler_app.scheduler``）。

    启动期校验（M8-1）：``after`` 引用的依赖名不在当前流水线集合时记 warning
    （不 fail，向后兼容）；依赖声明成环抛 ``ConfigError``（信息含环链路）。
    每轮按拓扑顺序触发：无依赖条目按声明序先行，有依赖条目按拓扑序随后；
    依赖条目被跳过时，依赖它的条目连锁跳过。

    返回累计触发次数。单次运行失败不中断循环：异常记入日志，等待下一轮。
    调度循环在主线程串行执行；``KeyboardInterrupt`` 会在循环层被捕获，
    以当前累计次数正常返回（配合 CLI 打印退出信息）。
    """
    if every <= 0:
        raise ValueError(f"every 必须是正数，实际是 {every!r}")
    logger = logger or log
    entries = _normalize_pipelines(pipeline, after=after)
    _warn_unknown_deps(entries, logger)
    order = _resolve_order(entries)  # 依赖成环在此抛 ConfigError
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
            # M8-1：本轮各流水线的触发状态（名 -> success/failed/error/skipped），
            # 供 after 依赖判断；无依赖条目照常触发，行为与 v0.8.0 一致。
            ran_status: dict[str, str] = {}
            for entry in order:
                unmet = _unmet_deps(entry, ran_status)
                if unmet:
                    logger.info(
                        "跳过 pipeline=%s：依赖未满足（%s）",
                        entry.name, "；".join(unmet),
                    )
                    ran_status[entry.name] = "skipped"
                    continue
                ok, info = _run_once(
                    entry.path,
                    registry=registry,
                    plugin=plugin,
                    plugin_modules=plugin_modules,
                    workdir=workdir,
                    cache_dir=cache_dir,
                    store=store,
                    logger=logger,
                )
                count += 1
                ran_status[entry.name] = info["status"]
                if not ok and alert_webhook:
                    _maybe_alert(
                        alert_webhook, entry.path, info, alert_silence,
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
    pipeline: str | Path | Sequence[str | Path | dict[str, Any]],
    *,
    after: Mapping[str, Sequence[str]] | None = None,
) -> list[_PipelineEntry]:
    """把单路径或条目序列统一成调度条目列表（``str``/``Path`` 视为单条，v0.6 兼容）。

    条目（M8-1）支持 ``str | Path``（等价于无依赖的 dict 条目）与
    ``dict{"path": ..., "after": [...]}``（``after`` 为依赖的 pipeline 名列表）。
    ``after``（CLI ``--after`` 的解析结果，名 -> 依赖列表）按 stem 匹配追加到
    对应条目的依赖，且排在条目自身声明的依赖之后。
    """
    if isinstance(pipeline, (str, Path)):
        items: list[str | Path | dict[str, Any]] = [pipeline]
    else:
        items = list(pipeline)
        if not items:
            raise ValueError("pipeline 不能为空序列")
    # CLI 追加的依赖（按 pipeline 名匹配，名 = 配置文件 stem）
    extra: dict[str, tuple[str, ...]] = {
        name: tuple(deps) for name, deps in (after or {}).items()
    }
    entries: list[_PipelineEntry] = []
    for item in items:
        if isinstance(item, (str, Path)):
            path: str | Path = item
            declared: tuple[str, ...] = ()
        elif isinstance(item, Mapping):
            path = item.get("path")
            if not isinstance(path, (str, Path)):
                raise ValueError(f"dict 条目缺少合法的 path 字段，实际是 {item!r}")
            deps = item.get("after") or ()
            if isinstance(deps, str) or not isinstance(deps, Sequence):
                raise ValueError(f"dict 条目的 after 必须是字符串数组，实际是 {deps!r}")
            declared = tuple(deps)
        else:
            raise ValueError(f"pipeline 条目必须是 str/Path/dict，实际是 {item!r}")
        name = Path(path).stem
        entries.append(
            _PipelineEntry(path=path, after=declared + extra.get(name, ()))
        )
    return entries


def _warn_unknown_deps(entries: list[_PipelineEntry], logger: logging.Logger) -> None:
    """M8-1 启动期校验：依赖名不在当前流水线集合时 warning（不 fail，向后兼容）。"""
    names = {entry.name for entry in entries}
    for entry in entries:
        for dep in entry.after:
            if dep not in names:
                logger.warning(
                    "流水线 %s 声明的依赖 %r 不在当前流水线集合中，"
                    "该依赖永远无法满足（将每轮跳过）",
                    entry.name, dep,
                )


def _resolve_order(entries: list[_PipelineEntry]) -> list[_PipelineEntry]:
    """计算单轮触发顺序：无依赖条目按声明序在前，有依赖条目按拓扑序随后。

    仅**已知条目名**的依赖参与拓扑排序（未知依赖启动期已 warning，永远无法
    满足，不参与排序也不构成环）；依赖声明成环抛 :class:`ConfigError`
    （信息含环链路，与内核 includes 防环风格一致）。
    """
    # op_pipeline 延迟导入：CLI --help 等轻量场景不加载内核
    from op_pipeline import ConfigError

    plain = [entry for entry in entries if not entry.after]
    dep_entries = [entry for entry in entries if entry.after]
    by_name: dict[str, _PipelineEntry] = {entry.name: entry for entry in entries}
    dep_names = {entry.name for entry in dep_entries}
    graph: dict[str, list[str]] = {
        entry.name: [d for d in entry.after if d in by_name] for entry in dep_entries
    }
    order: list[_PipelineEntry] = []
    state: dict[str, int] = {}  # 0=未访问 1=访问中 2=已完成
    visiting: list[str] = []

    def visit(entry: _PipelineEntry) -> None:
        state[entry.name] = 1
        visiting.append(entry.name)
        for dep in graph[entry.name]:
            if dep not in dep_names:  # 依赖无 after 的条目：已在 plain 段先行触发
                continue
            mark = state.get(dep, 0)
            if mark == 1:
                chain = " -> ".join(visiting[visiting.index(dep):] + [dep])
                raise ConfigError(f"调度依赖存在循环引用：{chain}")
            if mark == 0:
                visit(by_name[dep])
        visiting.pop()
        state[entry.name] = 2
        order.append(entry)

    for entry in dep_entries:
        if state.get(entry.name, 0) == 0:
            visit(entry)
    return plain + order


def _unmet_deps(entry: _PipelineEntry, ran_status: dict[str, str]) -> list[str]:
    """返回本轮未满足的依赖描述列表（空列表 = 全部满足，可以触发）。"""
    reasons: list[str] = []
    for dep in entry.after:
        if dep not in ran_status:
            reasons.append(f"{dep}(本轮未运行)")
        elif ran_status[dep] != "success":
            reasons.append(f"{dep}(状态 {ran_status[dep]})")
    return reasons


def _parse_after_specs(specs: Sequence[str]) -> dict[str, list[str]]:
    """解析 CLI ``--after NAME=dep1,dep2``（可重复）为名 -> 依赖列表的映射。

    同名多次声明按出现顺序追加；逗号分隔的空项忽略；缺 ``=`` 或名为空抛
    ``ValueError``。
    """
    result: dict[str, list[str]] = {}
    for spec in specs:
        name, sep, deps = spec.partition("=")
        if not sep or not name.strip():
            raise ValueError(f"--after 参数格式必须是 NAME=dep1,dep2，实际是 {spec!r}")
        result.setdefault(name.strip(), []).extend(
            d.strip() for d in deps.split(",") if d.strip()
        )
    return result


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
        "--after", action="append", default=[], metavar="NAME=DEPS",
        help="流水线依赖（可重复）：NAME=dep1,dep2 表示名为 NAME 的流水线（按配置"
             "文件名去扩展名匹配）须在 dep1/dep2 本轮全部成功后才触发，否则跳过；"
             "与 dict 条目已声明的 after 合并（CLI 追加在后）",
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
        after=_parse_after_specs(args.after),
        stop_event=stop,
    )
    print(f"调度器已停止，共触发 {count} 次（退出码 0）。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
