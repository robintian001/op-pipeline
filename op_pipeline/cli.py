"""命令行入口：``python -m op_pipeline <command>``。"""
from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from pathlib import Path
from typing import Any

from .cache import CacheStore
from .context import RunContext
from .engine import Engine, storage_warnings
from .errors import SpecValidationError
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


class _WarningCollector(logging.Handler):
    """临时收集 ``op_pipeline`` 的 WARNING 记录（不改变终端输出）。

    只挂载在 ``op_pipeline`` 日志器上，子日志器（spec/engine 等）的 WARNING
    照常传播到这里；既有 root handler 仍在，stderr 行为保持不变。
    """

    def __init__(self) -> None:
        super().__init__(level=logging.WARNING)
        self.messages: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.messages.append(record.getMessage())


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
        None if args.no_cache else args.cache_dir,
        base_dir=workdir,
        max_entries=args.cache_max_entries,
        max_bytes=args.cache_max_bytes,
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
    # M7-4 Task 12：运行结束统计缓存/spill 目录占用，超阈值追加中文告警
    report.warnings.extend(
        storage_warnings(
            cache_store,
            artifacts_dir=report.artifacts_dir,
            spill_warn_bytes=args.spill_warn_bytes,
        )
    )
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
    """M7-2 Task 4：只校验配置，不执行节点、不产生任何输出文件。

    退出码：成功 0，配置非法 1。``--json`` 输出单个 JSON 对象，
    含 ok 布尔、错误/告警清单与节点清单。
    """
    result: dict[str, Any] = {"ok": True, "errors": [], "warnings": [], "nodes": []}
    collector = _WarningCollector()
    pipeline_logger = logging.getLogger("op_pipeline")
    pipeline_logger.addHandler(collector)
    plan = None
    try:
        try:
            spec = _load(args.config, args.plugin)
            result["nodes"] = [{"id": n.id, "op": n.op} for n in spec.nodes]
            try:
                plan = build_plan(
                    spec, default_registry, overrides=_parse_overrides(args.param)
                )
            except SpecValidationError as exc:
                result["errors"].extend(exc.problems)
            except Exception as exc:  # noqa: BLE001 配置类错误统一转为校验失败
                result["errors"].append(str(exc))
            else:
                result["nodes"] = [
                    {"id": n.spec.id, "op": n.spec.op} for n in plan.nodes
                ]
        except SpecValidationError as exc:
            result["errors"].extend(exc.problems)
        except Exception as exc:  # noqa: BLE001 加载/解析失败同样是校验失败
            result["errors"].append(str(exc))
    finally:
        pipeline_logger.removeHandler(collector)

    result["warnings"] = list(collector.messages)
    result["ok"] = not result["errors"]

    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    elif result["ok"]:
        print(f"✓ 校验通过：{plan.spec.name}，{len(plan.nodes)} 个节点")
        print(to_ascii(plan))
    else:
        print("✗ 校验失败", file=sys.stderr)
        for problem in result["errors"]:
            print(f"  - {problem}", file=sys.stderr)
    return 0 if result["ok"] else 1


# --------------------------------------------------------------------------- #
# M7-2 Task 5：init 脚手架模板（全部只使用内置算子，validate 无需 --plugin）
# --------------------------------------------------------------------------- #
_MINIMAL_PIPELINE = """\
name = "my_pipeline"
version = "1"
description = "最小示例：常量 -> Markdown 报告（仅内置算子）"

[params]
greeting = "hello"

# 自定义算子示例见 plugins/my_ops.py；需要时显式加载：
#   python -m op_pipeline validate pipeline.toml --plugin plugins/my_ops.py

[[nodes]]
id = "greet"
op = "control.const"

  [nodes.params]
  value = "${p.greeting}"

[[nodes]]
id = "report"
op = "report.markdown"

  [nodes.params]
  title = "最小流水线报告"
  blocks = [
    { heading = "结果", level = 2 },
    { text = "问候：${greet.value}" },
  ]

[outputs]
report = "${report.text}"
"""

_DATA_PIPELINE = """\
name = "sales_summary"
version = "1"
description = "数据示例：内联数据 -> 清洗 -> 派生列 -> 聚合 -> Markdown 报告"

# 数据用 control.const 内联注入，不依赖任何外部文件，validate 可直接通过。
# 自定义算子示例见 plugins/my_ops.py；需要时显式加载：
#   python -m op_pipeline run pipeline.toml --plugin plugins/my_ops.py

[[nodes]]
id = "source"
op = "control.const"

  [nodes.params]
  value = [
    { region = "north", amount = 120, qty = 2 },
    { region = "south", amount = 80, qty = 1 },
    { region = "north", amount = -5, qty = 1 },
    { region = "east", amount = 200, qty = 4 },
  ]

[[nodes]]
id = "clean"
op = "table.filter_rows"

  [nodes.inputs]
  table = "${source.value}"

  [nodes.params]
  where = "amount > 0"

[[nodes]]
id = "enrich"
op = "table.derive_column"

  [nodes.inputs]
  table = "${clean.table}"

  [nodes.params]
  name = "total"
  expr = "amount * qty"

[[nodes]]
id = "agg"
op = "stats.aggregate"

  [nodes.inputs]
  table = "${enrich.table}"

  [nodes.params]
  group_by = "region"
  metrics = { amount = "sum", total = "sum" }

[[nodes]]
id = "report"
op = "report.markdown"

  [nodes.params]
  title = "销售汇总"
  blocks = [
    { heading = "按区域汇总", level = 2 },
    { table = "${agg.table}", title = "汇总表" },
    { text = "过滤掉的行数：${clean.dropped}" },
  ]

[outputs]
report = "${report.text}"
"""

_SCHEDULER_TOML = """\
# scheduler 模板：定时执行同目录的 pipeline.toml
#
# 本文件只描述定时参数；实际触发交给系统调度器（cron / Windows 任务计划），
# 每轮调用：
#   python -m op_pipeline run pipeline.toml --runs runs.db
#
# 仓库外围另提供调度与 Web UI 子包，用法见 docs/guides/operations.md。

[scheduler]
pipeline = "pipeline.toml"
every = 60
db = "runs.db"
"""

_INIT_PLUGIN = '''\
"""脚手架生成的示例插件算子（注册名 my_ops.uppercase）。

插件在 import 时通过 ``@op`` 自注册；单独使用时显式加载：
    python -m op_pipeline run pipeline.toml --plugin plugins/my_ops.py
"""
from __future__ import annotations

from op_pipeline import Operator, Param, op


@op("my_ops.uppercase")
class Uppercase(Operator):
    """把输入文本转成大写（示例插件算子）。"""

    description = "转大写（示例插件）"
    inputs = [("text", str)]
    outputs = [("text", str)]
    params = {"prefix": Param(str, default="", doc="输出前缀")}

    def run(self, ctx, text, prefix):
        return {"text": f"{prefix}{str(text).upper()}"}
'''

_INIT_PLUGIN_TEST = '''\
"""脚手架生成：示例插件算子的最小单测骨架。"""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from op_pipeline import Registry, load_plugins  # noqa: E402

PLUGIN = ROOT / "plugins" / "my_ops.py"


class _FakeContext:
    run_id = "test"
    node_id = "demo"

    def log(self, *_args, **_kwargs):
        pass


class MyOpsTest(unittest.TestCase):
    def _registry(self):
        registry = Registry()
        load_plugins(paths=[PLUGIN], registry=registry)
        return registry

    def test_plugin_registers(self):
        self.assertIn("my_ops.uppercase", self._registry())

    def test_uppercase_run(self):
        cls = self._registry().get("my_ops.uppercase")
        result = cls(prefix=">> ").execute(_FakeContext(), {"text": "hi"})
        self.assertEqual(">> HI", result["text"])


if __name__ == "__main__":
    unittest.main()
'''

_OP_NAME_PATTERN = re.compile(r"[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*\Z")

_SCAFFOLD_OP_MODULE = '''\
"""自动生成的算子骨架：__NAME__。"""
from __future__ import annotations

from typing import Any

from op_pipeline import Operator, Param, op


@op("__NAME__")
class __CLASS__(Operator):
    """__NAME__：请补充算子说明。"""

    description = "__NAME__"
    inputs = [("value", Any)]
    outputs = [("value", Any)]
    params = {
        "scale": Param(int, default=1, doc="示例参数：缩放倍数"),
    }

    def run(self, ctx, value, scale):
        # TODO: 实现 __NAME__ 的业务逻辑
        return {"value": value}
'''

_SCAFFOLD_OP_TEST = '''\
"""自动生成：__NAME__ 算子契约测试骨架。"""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from __LAST___op import __CLASS__  # noqa: E402


class _FakeContext:
    run_id = "test"
    node_id = "demo"

    def log(self, *_args, **_kwargs):
        pass


class __CLASS__Test(unittest.TestCase):
    def test_contract(self):
        self.assertEqual("__NAME__", __CLASS__.name)
        self.assertTrue(__CLASS__.inputs)
        self.assertTrue(__CLASS__.outputs)
        self.assertIn("scale", __CLASS__.param_table)

    def test_run(self):
        instance = __CLASS__(scale=2)
        result = instance.execute(_FakeContext(), {"value": 21})
        self.assertEqual(21, result["value"])


if __name__ == "__main__":
    unittest.main()
'''


def cmd_init(args: argparse.Namespace) -> int:
    target = Path(args.dir).expanduser()
    if target.exists():
        if not target.is_dir():
            print(f"目标路径不是目录：{target}", file=sys.stderr)
            return 2
        if any(target.iterdir()):
            print(
                f"目标目录已存在且非空：{target}（请先清空或换一个目录）",
                file=sys.stderr,
            )
            return 2
    target.mkdir(parents=True, exist_ok=True)

    files = {
        "pipeline.toml": _DATA_PIPELINE if args.template == "data" else _MINIMAL_PIPELINE,
        "plugins/my_ops.py": _INIT_PLUGIN,
        "tests/test_my_ops.py": _INIT_PLUGIN_TEST,
    }
    if args.template == "scheduler":
        files["scheduler.toml"] = _SCHEDULER_TOML

    for rel, content in files.items():
        path = target / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
        print(f"已生成：{path}")

    print(f"\n下一步：cd {target} && python -m op_pipeline run pipeline.toml")
    return 0


def cmd_scaffold_op(args: argparse.Namespace) -> int:
    name = args.name
    if not _OP_NAME_PATTERN.fullmatch(name):
        print(
            f"非法算子名：{name!r}（应形如 'etl.dedupe'，每段匹配 "
            "[A-Za-z_][A-Za-z0-9_]*，不能以点开头/结尾或包含空格）",
            file=sys.stderr,
        )
        return 2

    last = name.rsplit(".", 1)[-1]
    class_base = "".join(
        part.capitalize() for part in re.split(r"[^0-9A-Za-z]+", last) if part
    )
    class_name = f"{class_base}Op"
    base = Path(args.dir).expanduser()
    module_path = base / f"{last}_op.py"
    test_path = base / "tests" / f"test_{last}_op.py"
    if module_path.exists() or test_path.exists():
        print(
            f"目标文件已存在：{module_path if module_path.exists() else test_path}",
            file=sys.stderr,
        )
        return 2

    module_path.parent.mkdir(parents=True, exist_ok=True)
    test_path.parent.mkdir(parents=True, exist_ok=True)
    module_path.write_text(
        _SCAFFOLD_OP_MODULE.replace("__NAME__", name).replace("__CLASS__", class_name),
        encoding="utf-8",
    )
    test_path.write_text(
        _SCAFFOLD_OP_TEST.replace("__NAME__", name)
        .replace("__CLASS__", class_name)
        .replace("__LAST__", last),
        encoding="utf-8",
    )
    print(f"已生成：{module_path}")
    print(f"已生成：{test_path}")
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
    """M5-1 远程执行：消费共享队列文件并执行任务（配合 QueueWorkerExecutor）。

    M6-3 Task 10：``--workers N`` 并发消费（默认 1 = 原串行行为），N<=0 报错。
    M7-1：``--lease-seconds``/``--max-attempts``/``--owner`` 任务租约（默认关闭，
    行为与 v0.7.0 一致）。
    """
    from .remote import consume_jobs

    if args.workers <= 0:
        print(f"--workers 必须是正整数，实际是 {args.workers}", file=sys.stderr)
        return 2
    out = args.out or str(Path(args.queue).expanduser().resolve().parent / "results")
    print(f"worker 监听队列：{args.queue}\n结果目录：{out}")
    try:
        processed = consume_jobs(
            args.queue,
            out,
            count=args.count,
            poll=args.poll,
            workers=args.workers,
            owner=args.owner,
            lease_seconds=args.lease_seconds,
            max_attempts=args.max_attempts,
        )
    except KeyboardInterrupt:
        print("\nworker 已停止（Ctrl+C）")
        return 130
    print(f"共消费 {len(processed)} 个任务：")
    for item in processed:
        print(f"  {item['job_id']} -> {item['status']}")
    return 0


def cmd_queue_compact(args: argparse.Namespace) -> int:
    """M7-1 Task 3：把已处理条目归档并重写活跃队列（无未完成任务时才动文件）。"""
    from .remote import compact_queue

    queue = Path(args.queue).expanduser()
    if not queue.is_file():
        print(f"队列文件不存在：{queue}", file=sys.stderr)
        return 1
    try:
        pending, archive = compact_queue(queue, args.out, args.archive)
    except OSError as exc:
        print(f"队列归档失败：{exc}", file=sys.stderr)
        return 1
    if pending:
        print("存在未完成任务，未改动任何文件：", file=sys.stderr)
        for job_id in pending:
            print(f"  - {job_id}", file=sys.stderr)
        return 1
    print(f"已完成条目已归档：{archive}")
    print("活跃队列已清空")
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
    run.add_argument(
        "--cache-max-entries", type=int, default=None,
        help="M7-4 缓存条目上限：写入后按 mtime 淘汰最旧条目（默认不限）",
    )
    run.add_argument(
        "--cache-max-bytes", type=int, default=None,
        help="M7-4 缓存总字节上限：写入后按 mtime 淘汰最旧条目（默认不限）",
    )
    run.add_argument(
        "--spill-warn-bytes", type=int, default=None,
        help="M7-4 spill 落盘目录占用告警阈值（字节；默认不告警）",
    )
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
    validate.add_argument(
        "--json", action="store_true",
        help="输出单个 JSON 对象：{ok, errors, warnings, nodes}",
    )
    validate.set_defaults(func=cmd_validate)

    init = sub.add_parser("init", help="生成脚手架项目（M7-2）")
    init.add_argument("dir", help="目标目录（已存在且非空时拒绝）")
    init.add_argument(
        "--template", choices=("minimal", "data", "scheduler"), default="minimal",
        help="模板类型：minimal / data / scheduler（默认 minimal）",
    )
    init.set_defaults(func=cmd_init)

    scaffold = sub.add_parser("scaffold", help="生成代码骨架（M7-2）")
    scaffold_sub = scaffold.add_subparsers(dest="scaffold_command", required=True)
    scaffold_op = scaffold_sub.add_parser("op", help="生成自定义算子骨架")
    scaffold_op.add_argument("name", help="算子注册名，如 etl.dedupe")
    scaffold_op.add_argument(
        "--dir", default=".", help="基准目录（默认当前目录）"
    )
    scaffold_op.set_defaults(func=cmd_scaffold_op)

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
    worker.add_argument(
        "--workers", type=int, default=1,
        help="并发消费任务的 worker 数（默认 1；必须为正整数，M6-3）",
    )
    worker.add_argument(
        "--lease-seconds", type=float, default=0.0,
        help="任务租约秒数（默认 0 = 关闭租约，M7-1；行内配置优先）",
    )
    worker.add_argument(
        "--max-attempts", type=int, default=3,
        help="租约过期后的最大 attempt（默认 3，M7-1；行内配置优先）",
    )
    worker.add_argument(
        "--owner", default=None,
        help="租约持有者标识（默认 <hostname>-<pid>，M7-1）",
    )
    worker.set_defaults(func=cmd_worker)

    queue = sub.add_parser("queue", help="远程队列维护（M7-1）")
    queue_sub = queue.add_subparsers(dest="queue_command", required=True)
    compact = queue_sub.add_parser(
        "compact", help="全部任务完成后归档已处理条目并清空活跃队列"
    )
    compact.add_argument("--queue", required=True, help="JSONL 任务队列文件路径")
    compact.add_argument("--out", required=True, help="结果目录（判断任务是否完成）")
    compact.add_argument(
        "--archive", default=None,
        help="归档文件路径（默认 <队列同目录>/<队列 stem>.archive.jsonl）",
    )
    compact.set_defaults(func=cmd_queue_compact)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    _setup_logging(getattr(args, "verbose", False))
    load_plugins(entry_points=True)  # 内置算子 + 已安装插件包入口点（M3-4，坏插件只告警）
    return int(args.func(args) or 0)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
