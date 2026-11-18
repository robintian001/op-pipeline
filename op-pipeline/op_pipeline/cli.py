"""命令行入口：``python -m op_pipeline <command>``。"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any

from .cache import CacheStore
from .context import RunContext
from .engine import Engine
from .graph import to_ascii, to_dot, to_mermaid
from .registry import default_registry, load_plugins
from .spec import PipelineSpec, build_plan
from .store import RunStore

log = logging.getLogger("op_pipeline.cli")


def _parse_value(raw: str) -> Any:
    for parser in (json.loads,):
        try:
            return parser(raw)
        except Exception:
            continue
    low = raw.lower()
    if low in ("true", "false"):
        return low == "true"
    return raw


def _parse_overrides(items: list[str] | None) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for item in items or []:
        if "=" not in item:
            raise SystemExit(f"--param 需要 KEY=VALUE 形式，收到：{item}")
        key, raw = item.split("=", 1)
        out[key.strip()] = _parse_value(raw)
    return out


def _load(path: str, plugins: list[str] | None) -> PipelineSpec:
    spec = PipelineSpec.from_file(path)
    base_dir = Path(path).expanduser().resolve().parent
    load_plugins(paths=plugins or (), base_dir=base_dir)
    return spec


def _setup_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s  %(levelname)-7s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    if not verbose:
        logging.getLogger("op_pipeline").setLevel(logging.INFO)


# --------------------------------------------------------------------------- #
def cmd_run(args: argparse.Namespace) -> int:
    spec = _load(args.config, args.plugin)
    overrides = _parse_overrides(args.param)
    plan = build_plan(spec, default_registry, overrides=overrides)

    if args.dry_run:
        # 在创建 RunContext/CacheStore 之前返回：不建产物目录与缓存目录
        print(to_ascii(plan))
        return 0

    workdir = args.workdir or str(Path(args.config).expanduser().resolve().parent)
    context = RunContext.create(
        pipeline=plan.spec.name,
        params=plan.params,
        workdir=workdir,
        logger=logging.getLogger("op_pipeline"),
    )
    cache_store = CacheStore(
        None if args.no_cache else args.cache_dir, base_dir=workdir
    )
    # M2-5：--runs <db> 开关，默认关闭 = 零副作用（不产生任何记录文件）
    run_store = None
    if args.runs:
        runs_path = Path(args.runs).expanduser()
        if not runs_path.is_absolute():
            runs_path = Path(workdir) / runs_path
        runs_path.parent.mkdir(parents=True, exist_ok=True)
        run_store = RunStore(runs_path)
    # M2-6：--resume <run_id> 必须配合 --runs（参考库），否则无从比对
    if args.resume and run_store is None:
        print("--resume 需要同时指定 --runs <db>（参考运行记录库）", file=sys.stderr)
        return 2
    engine = Engine(plan, context, cache_store=cache_store, max_workers=args.workers,
                    fail_fast=args.fail_fast, executor=args.executor, run_store=run_store,
                    resume_run_id=args.resume)

    report = engine.run()
    if run_store is not None:
        run_store.close()
    print(report.summary())
    # M5-4（Task 10）：失败节点在 CLI 报告追加截断堆栈——
    # NodeResult.traceback 已由引擎 _clip_traceback 截断（保留末尾约 2000 字符），
    # 这里只负责展示；summary() 保持不含堆栈的既有约定
    for node in report.nodes:
        if node.status == "failed" and node.traceback:
            print(f"\n── 节点 [{node.node_id}]（{node.op}）失败堆栈 ──")
            print(node.traceback)
    if args.json:
        path = report.to_json(args.json)
        print(f"\n报告已写入：{path}")
    return 0 if report.status in ("success", "partial") else 1


def cmd_validate(args: argparse.Namespace) -> int:
    spec = _load(args.config, args.plugin)
    try:
        plan = build_plan(spec, default_registry, overrides=_parse_overrides(args.param))
    except Exception as exc:
        print(f"✗ 校验失败\n{exc}", file=sys.stderr)
        return 2
    print(f"✓ 校验通过：{plan.spec.name}，{len(plan.nodes)} 个节点")
    print(to_ascii(plan))
    return 0


def cmd_graph(args: argparse.Namespace) -> int:
    spec = _load(args.config, args.plugin)
    plan = build_plan(spec, default_registry, overrides=_parse_overrides(args.param))
    renderer = {"mermaid": to_mermaid, "dot": to_dot, "ascii": to_ascii}[args.format]
    print(renderer(plan))
    return 0


def cmd_ops(args: argparse.Namespace) -> int:
    describe = default_registry.describe()
    if args.json:
        print(json.dumps(describe, ensure_ascii=False, indent=2))
        return 0
    for item in describe:
        inputs = ", ".join(f"{p['name']}:{p['type']}" for p in item["inputs"]) or "-"
        outputs = ", ".join(f"{p['name']}:{p['type']}" for p in item["outputs"]) or "-"
        print(f"{item['name']:<28} {item['description']}")
        print(f"{'':<28} in  <- {inputs}")
        print(f"{'':<28} out -> {outputs}")
    print(f"\n共 {len(describe)} 个算子")
    return 0


def cmd_describe(args: argparse.Namespace) -> int:
    cls = default_registry.get(args.name)
    print(json.dumps(cls.describe(), ensure_ascii=False, indent=2))
    return 0


def cmd_worker(args: argparse.Namespace) -> int:
    """M5-1 远程执行：消费共享队列文件并执行任务（配合 QueueWorkerExecutor）。"""
    from .remote import consume_jobs

    out = args.out or str(Path(args.queue).expanduser().resolve().parent / "results")
    print(f"worker 监听队列：{args.queue}\n结果目录：{out}")
    try:
        processed = consume_jobs(args.queue, out, count=args.count, poll=args.poll)
    except KeyboardInterrupt:
        print("\nworker 已停止（Ctrl+C）")
        return 130
    print(f"共消费 {len(processed)} 个任务：")
    for item in processed:
        print(f"  {item['job_id']} -> {item['status']}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="op_pipeline", description="算子级可配置流水线框架")
    parser.add_argument("-v", "--verbose", action="store_true", help="打印调试日志")
    sub = parser.add_subparsers(dest="command", required=True)

    def common(p: argparse.ArgumentParser) -> None:
        p.add_argument("config", help="流水线配置文件（.json/.toml/.yaml）")
        p.add_argument("--plugin", action="append", help="额外插件文件，可多次指定")
        p.add_argument("--param", action="append", help="覆盖流水线参数 KEY=VALUE，可多次指定")

    run = sub.add_parser("run", help="执行流水线")
    common(run)
    run.add_argument("--workdir", default=None, help="工作目录（相对路径基准），默认取配置文件所在目录")
    run.add_argument("--workers", type=int, default=None, help="最大并发节点数")
    run.add_argument(
        "--cache-dir", default=".pipeline_cache",
        help="节点结果缓存目录（相对路径按工作目录解析）",
    )
    run.add_argument("--no-cache", action="store_true", help="关闭缓存")
    run.add_argument("--fail-fast", action="store_true", help="任一节点失败即停止调度新节点")
    run.add_argument(
        "--executor", choices=("thread", "process", "queue"), default=None,
        help="运行级执行器：process 时未显式指定的节点在子进程执行（硬超时）；"
             "queue 时引擎自建队列执行器——队列与结果目录推导自工作目录下的 "
             ".op_pipeline/remote/（jobs.jsonl 与 results/），需另启 worker 消费："
             "python -m op_pipeline worker --queue <工作目录>/.op_pipeline/remote/jobs.jsonl "
             "--out <工作目录>/.op_pipeline/remote/results",
    )
    run.add_argument("--dry-run", action="store_true", help="只打印执行计划不执行")
    run.add_argument(
        "--runs", default=None,
        help="SQLite 运行记录库路径（相对路径按工作目录解析；默认关闭，不产生任何文件）",
    )
    run.add_argument(
        "--resume", default=None,
        help="M2-6 断点续跑：参考 run_id 的 nodes 记录与本次 cache_key 比对，"
             "成功段复用缓存并标记 [resumed]（需同时指定 --runs）",
    )
    run.add_argument("--json", default=None, help="把运行报告写成 JSON")
    run.set_defaults(func=cmd_run)

    validate = sub.add_parser("validate", help="只校验配置，不执行")
    common(validate)
    validate.set_defaults(func=cmd_validate)

    graph = sub.add_parser("graph", help="输出依赖图")
    common(graph)
    graph.add_argument("--format", choices=("mermaid", "dot", "ascii"), default="mermaid")
    graph.set_defaults(func=cmd_graph)

    ops = sub.add_parser("ops", help="列出已注册算子")
    ops.add_argument("--json", action="store_true")
    ops.set_defaults(func=cmd_ops)

    describe = sub.add_parser("describe", help="查看单个算子契约")
    describe.add_argument("name")
    describe.set_defaults(func=cmd_describe)

    worker = sub.add_parser("worker", help="远程执行：消费共享队列文件并执行任务（M5-1）")
    worker.add_argument("--queue", required=True, help="JSONL 任务队列文件路径")
    worker.add_argument("--out", default=None, help="结果目录（默认取队列同目录下的 results/）")
    worker.add_argument("--count", type=int, default=None, help="最多消费任务数（默认不限，持续监听）")
    worker.add_argument("--poll", type=float, default=0.1, help="队列轮询间隔（秒）")
    worker.set_defaults(func=cmd_worker)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    _setup_logging(getattr(args, "verbose", False))
    load_plugins(entry_points=True)  # 内置算子 + 已安装插件包入口点（M3-4，坏插件只告警）
    return int(args.func(args) or 0)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
