"""命令行入口：``python -m op_pipeline <command>``。"""
from __future__ import annotations

import argparse
import json
import logging
import re
import sys
import time
from pathlib import Path
from typing import Any

from .cache import CacheStore
from .context import RunContext
from .engine import Engine, RunReport, _masked_params, storage_warnings
from .errors import RefResolutionError, SpecValidationError
from .graph import to_ascii, to_dot, to_mermaid
from .registry import default_registry, load_plugins
from .spec import PipelineSpec, RefResolver, build_plan
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


def _load_param_files(paths: list[str] | None) -> dict[str, Any]:
    """M9 Task 8：逐文件加载 ``--param-file`` 的 JSON 参数（顶层必须是 object）。

    文件不存在 / 不是合法 JSON / 顶层不是 dict 都视为用法错误：打印中文错误
    并 ``SystemExit(2)``（与 ``--param`` 非法值的处理风格一致）。
    """
    out: dict[str, Any] = {}
    for item in paths or []:
        path = Path(item).expanduser()
        try:
            raw = path.read_text(encoding="utf-8")
        except OSError as exc:
            print(f"--param-file 无法读取：{path}（{exc}）", file=sys.stderr)
            raise SystemExit(2) from None
        try:
            data = json.loads(raw)
        except ValueError as exc:
            print(f"--param-file 不是合法 JSON：{path}（{exc}）", file=sys.stderr)
            raise SystemExit(2) from None
        if not isinstance(data, dict):
            print(f"--param-file 顶层必须是 JSON 对象（dict）：{path}", file=sys.stderr)
            raise SystemExit(2) from None
        out.update(data)
    return out


def _merged_params(
    param: list[str] | None, param_files: list[str] | None
) -> dict[str, Any]:
    """M9 Task 8：合并 ``--param-file``（JSON 文件）与 ``--param``（KEY=VALUE）。

    各文件按顺序累加；命令行 ``--param`` 最后合并，覆盖文件中的同名键。
    合并结果即运行级参数注入 map（``${param:...}`` 解析期替换，语义同 --param）。
    """
    params = _load_param_files(param_files)
    params.update(_parse_overrides(param))
    return params


def _load(
    path: str,
    plugins: list[str] | None,
    params: dict[str, Any] | None = None,
) -> PipelineSpec:
    spec = PipelineSpec.from_file(path, params=params)
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
def _write_report_out(path: str, report: RunReport, workdir: str) -> None:
    """M9 Task 8：把全量运行报告 JSON 写到 ``--report-out`` 指定路径。

    与 ``RunReport.to_json`` 同规则（ensure_ascii=False、indent=2、default=str）；
    相对路径按工作目录解析，父目录自动创建。任何写失败只记 ``logger.error``，
    调用方照常返回运行结果与退出码，绝不掩盖运行状态。
    """
    target = Path(path).expanduser()
    if not target.is_absolute():
        target = Path(workdir) / target
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(
            json.dumps(report.to_dict(), ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )
    except OSError as exc:
        log.error("运行报告写入失败（不影响运行结果与退出码）：%s（%s）", target, exc)


def cmd_run(args: argparse.Namespace) -> int:
    # M9 Task 8：--param-file 与 --param 合并（命令行 --param 覆盖文件值）
    overrides = _merged_params(args.param, args.param_file)
    # M8-2 Task 1：--param 值同时作为运行级参数注入（${param:...} 解析期替换）
    spec = _load(args.config, args.plugin, params=overrides)
    plan = build_plan(spec, default_registry, overrides=overrides)

    if args.dry_run:
        # 在创建 RunContext/CacheStore 之前返回：不建产物目录与缓存目录
        print(to_ascii(plan))
        return 0

    # M8-2 Task 4：--sample N 采样预览；非法值在创建上下文之前就拒绝
    if args.sample is not None and args.sample < 1:
        print("--sample 必须是 ≥1 的整数", file=sys.stderr)
        return 2
    # M9 Task 7：--preview N 值级预览；非法值在创建上下文之前就拒绝
    if args.preview is not None and args.preview < 1:
        print("--preview 必须是 ≥1 的整数", file=sys.stderr)
        return 2

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
                    resume_run_id=args.resume, sample_rows=args.sample,
                    value_preview=args.preview)

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
    # M9 Task 8：--report-out 全量报告落盘（不传参数不写文件）
    if args.report_out:
        _write_report_out(args.report_out, report, workdir)
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
            # M8-2 Task 1：--param 值同时作为运行级参数注入（${param:...} 解析期替换）
            overrides = _parse_overrides(args.param)
            spec = _load(args.config, args.plugin, params=overrides)
            result["nodes"] = [{"id": n.id, "op": n.op} for n in spec.nodes]
            try:
                plan = build_plan(spec, default_registry, overrides=overrides)
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

_QUALITY_PIPELINE = """\
name = "quality_gate"
version = "1"
description = "质量示例：schema_check + null_check 双门禁（warn/error 模式 + allow_failure 语义）"

# 数据用 control.const 内联注入，不依赖任何外部文件，validate 可直接通过。
# 自定义算子示例见 plugins/my_ops.py；需要时显式加载：
#   python -m op_pipeline run pipeline.toml --plugin plugins/my_ops.py

[[nodes]]
id = "source"
op = "control.const"

  [nodes.params]
  value = [
    { id = 1, name = "alice", region = "north", amount = 120 },
    { id = 2, name = "bob", region = "south", amount = 80 },
    { id = 3, name = "", region = "north", amount = 5 },
  ]

# 第一道门禁：类型检查（mode = "error" —— 违例即节点失败，硬门禁）
[[nodes]]
id = "schema_gate"
op = "quality.schema_check"

  [nodes.inputs]
  table = "${source.value}"

  [nodes.params]
  expected = { id = "int", name = "str", region = "str", amount = "int" }
  strict_columns = true
  mode = "error"

# 第二道门禁：空值率检查（mode = "warn" —— 违例只出明细不中断，
# 明细交给 report 节点展示；name = "" 即为一条空值违例）
[[nodes]]
id = "null_gate"
op = "quality.null_check"

  [nodes.inputs]
  table = "${schema_gate.table}"

  [nodes.params]
  columns = ["name", "region"]
  max_null_ratio = 0.0
  mode = "warn"

# allow_failure 语义示例：本节点（error 门禁）会因 region 重复而失败，
# 但 allow_failure = true 时失败只影响自身，不中断整条流水线。
[[nodes]]
id = "audit"
op = "quality.unique"
allow_failure = true

  [nodes.inputs]
  table = "${null_gate.table}"

  [nodes.params]
  keys = ["region"]
  mode = "error"

[[nodes]]
id = "report"
op = "report.markdown"

  [nodes.params]
  title = "质量检查报告"
  blocks = [
    { heading = "数据", level = 2 },
    { table = "${null_gate.table}", title = "通过门禁的数据" },
    { text = "空值违例数：${null_gate.violations}" },
  ]

[outputs]
report = "${report.text}"
"""

_SUBPIPELINE_PARENT = """\
name = "subpipeline_demo"
version = "1"
description = "编排示例：父配置调用子流水线（pipeline.run）并把 ${param:} 注入子配置"

# 父配置通过内置算子 pipeline.run 执行同目录的子配置 child.toml，
# 并从子流水线取回节点输出；运行级参数经 params 注入子流水线：
#   python -m op_pipeline run pipeline.toml --param CITY=上海

[params]
CITY = "北京"

[[nodes]]
id = "sub"
op = "pipeline.run"

  [nodes.params]
  config = "child.toml"
  node = "greet"
  params = { CITY = "${param:CITY}" }

[[nodes]]
id = "wrap"
op = "control.passthrough"

  [nodes.inputs]
  value = "${sub.result}"

[outputs]
out = "${wrap.value}"
"""

_SUBPIPELINE_CHILD = """\
name = "child"
version = "1"
description = "子流水线：被 pipeline.toml 调用，接收父配置注入的 ${param:} 运行级参数"

# ${param:CITY:-北京} 的 :- 默认值保证 child.toml 可被独立 validate；
# 经父配置 pipeline.run 注入时使用注入值。
[[nodes]]
id = "greet"
op = "control.const"

  [nodes.params]
  value = "你好，${param:CITY:-北京}！"

[outputs]
greeting = "${greet.value}"
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
        "pipeline.toml": _QUALITY_PIPELINE
        if args.template == "quality"
        else (_DATA_PIPELINE if args.template == "data" else _MINIMAL_PIPELINE),
        "plugins/my_ops.py": _INIT_PLUGIN,
        "tests/test_my_ops.py": _INIT_PLUGIN_TEST,
    }
    if args.template == "scheduler":
        files["scheduler.toml"] = _SCHEDULER_TOML
    elif args.template == "subpipeline":
        # M9 Task 9：subpipeline 模板只生成两个配置文件（父配置 + 子配置）
        files = {"pipeline.toml": _SUBPIPELINE_PARENT, "child.toml": _SUBPIPELINE_CHILD}

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
    overrides = _parse_overrides(args.param)
    spec = _load(args.config, args.plugin, params=overrides)
    plan = build_plan(spec, default_registry, overrides=overrides)
    renderer = {"mermaid": to_mermaid, "dot": to_dot, "ascii": to_ascii}[args.format]
    print(renderer(plan))
    return 0


# --------------------------------------------------------------------------- #
# M8-2 Task 6：explain —— 输出解析后的最终生效配置（不执行节点、零文件产出）
# --------------------------------------------------------------------------- #
def _explain_value(value: Any, resolver: RefResolver) -> Any:
    """尽力就地解析节点参数里的 ``${p.x}`` / ``${env.X}`` 引用。

    句柄引用（``${node.port}``，运行期才能求值）解析失败时保持原样。
    """
    if isinstance(value, str):
        try:
            return resolver.resolve(value)
        except RefResolutionError:
            return value
    if isinstance(value, list):
        return [_explain_value(item, resolver) for item in value]
    if isinstance(value, dict):
        return {k: _explain_value(v, resolver) for k, v in value.items()}
    return value


def _explain_node(node: Any, resolver: RefResolver) -> dict[str, Any]:
    """单个节点的生效配置片段（与 --json 结构一致）。"""
    spec = node.spec
    return {
        "id": spec.id,
        "op": spec.op,
        "params": _explain_value(dict(spec.params), resolver),
        "inputs": dict(spec.inputs),
        "when": spec.when,
        "policy": {
            "retries": spec.retries,
            "timeout": spec.timeout,
            "executor": spec.executor,
            "cache": spec.cache,
            "concurrency_group": spec.concurrency_group,
            "allow_failure": spec.allow_failure,
        },
        "depends_on": list(node.depends_on),
    }


def _fmt_value(value: Any) -> str:
    """explain 文本输出里的参数值格式化（配置值来自 JSON/TOML/YAML）。"""
    try:
        return json.dumps(value, ensure_ascii=False)
    except TypeError:
        return repr(value)


def _print_explain_text(result: dict[str, Any]) -> None:
    """explain 文本输出：每节点一段，人读友好。"""
    print(f"生效配置：{result['pipeline']} (v{result['version']})")
    print("\n参数：")
    if result["params"]:
        for key, value in result["params"].items():
            print(f"  {key} = {_fmt_value(value)}")
    else:
        print("  （无）")
    for node in result["nodes"]:
        print(f"\n节点 [{node['id']}]  op={node['op']}")
        print("  inputs:")
        if node["inputs"]:
            for port, ref in node["inputs"].items():
                print(f"    {port} = {ref}")
        else:
            print("    （无）")
        print("  params:")
        if node["params"]:
            for key, value in node["params"].items():
                print(f"    {key} = {_fmt_value(value)}")
        else:
            print("    （无）")
        print(f"  when: {node['when'] or '（无条件）'}")
        policy = " ".join(f"{k}={node['policy'][k]!r}" for k in node["policy"])
        print(f"  policy: {policy}")
        deps = ", ".join(node["depends_on"]) or "（无上游依赖）"
        print(f"  depends_on: {deps}")


def cmd_explain(args: argparse.Namespace) -> int:
    """M8-2 Task 6：输出解析后的最终生效配置，不执行节点、不产生任何输出文件。

    加载阶段完成 includes 合并、defaults 合并、``${env:...}`` 与
    ``${param:...}`` 占位符替换（与 validate 共用同一套加载路径与
    ``_WarningCollector`` 模式）；``${p.x}`` 参数引用尽力就地解析，
    句柄引用保持原样。退出码：成功 0；配置非法或 ``--node`` 不存在为 1。
    """
    result: dict[str, Any] = {
        "pipeline": "",
        "version": "",
        "params": {},
        "nodes": [],
        "warnings": [],
    }
    errors: list[str] = []
    collector = _WarningCollector()
    pipeline_logger = logging.getLogger("op_pipeline")
    pipeline_logger.addHandler(collector)
    plan = None
    try:
        try:
            # M9 Task 8：--param-file 与 --param 合并（命令行 --param 覆盖文件值）
            overrides = _merged_params(args.param, args.param_file)
            spec = _load(args.config, args.plugin, params=overrides)
            plan = build_plan(spec, default_registry)
        except SpecValidationError as exc:
            errors.extend(exc.problems)
        except Exception as exc:  # noqa: BLE001 加载/解析失败统一按配置非法处理
            errors.append(str(exc))
        else:
            resolver = RefResolver(plan.params, {})
            selected = plan.nodes
            if args.node is not None:
                selected = [n for n in plan.nodes if n.spec.id == args.node]
                if not selected:
                    errors.append(f"节点不存在：{args.node}")
            if not errors:
                result["pipeline"] = plan.spec.name
                result["version"] = plan.spec.version
                # 敏感打码沿用 M7-2 机制
                result["params"] = _masked_params(plan.params, plan.spec.sensitive_values)
                result["nodes"] = [_explain_node(n, resolver) for n in selected]
    finally:
        pipeline_logger.removeHandler(collector)

    result["warnings"] = list(collector.messages)
    if errors:
        if args.json:
            result["errors"] = errors
            print(json.dumps(result, ensure_ascii=False, indent=2))
        else:
            print("✗ 无法生成生效配置", file=sys.stderr)
            for problem in errors:
                print(f"  - {problem}", file=sys.stderr)
        return 1
    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        _print_explain_text(result)
    return 0


# --------------------------------------------------------------------------- #
# M8-3 Task 8：events —— 查询/过滤/聚合 JsonlHook 事件流（零第三方依赖）
#
# JsonlHook（op_pipeline/observe.py）实际事件字段：
#   node_start  : {event, ts(ISO 字符串), node_id, op}
#   node_finish : {event, ts, node_id, status, duration(秒，float，4 位舍入), cached}
#   run_finish  : {event, ts, pipeline, version, run_id, status, duration, ok}
# 即 node_id 只在节点事件行出现、run_id 只在 run_finish 行出现。
# --------------------------------------------------------------------------- #
def _event_matches(
    ev: dict[str, Any],
    run_id: str | None,
    node: str | None,
    event_type: str | None,
) -> bool:
    """AND 语义过滤：三个条件全部满足才命中（未指定的条件视为通过）。

    注意：``node_start``/``node_finish`` 行不携带 ``run_id`` 字段，
    因此 ``--run-id`` 只能命中 ``run_finish`` 行（严格 AND 语义）。
    """
    if run_id is not None and ev.get("run_id") != run_id:
        return False
    if node is not None and ev.get("node_id") != node:
        return False
    if event_type is not None and ev.get("event") != event_type:
        return False
    return True


def _print_events_summary(kept: list[tuple[str, dict[str, Any]]]) -> None:
    """--summary 文本表格：按节点聚合执行次数/总耗时/失败数（取自 node_finish 事件）。"""
    stats: dict[str, dict[str, Any]] = {}
    for _text, ev in kept:
        if ev.get("event") != "node_finish":
            continue
        node_id = str(ev.get("node_id", "（缺失）"))
        row = stats.setdefault(node_id, {"count": 0, "duration": 0.0, "failed": 0})
        row["count"] += 1
        duration = ev.get("duration")
        if isinstance(duration, (int, float)):
            row["duration"] += duration
        if ev.get("status") == "failed":
            row["failed"] += 1
    if not stats:
        print("（无 node_finish 事件，汇总为空）")
        return
    width = max(len("节点"), *(len(k) for k in stats))
    print(f"{'节点':<{width}}  {'执行次数':>6}  {'总耗时(s)':>10}  {'失败数':>6}")
    for node_id, row in stats.items():
        print(
            f"{node_id:<{width}}  {row['count']:>6}  "
            f"{row['duration']:>10.4f}  {row['failed']:>6}"
        )


def cmd_events(args: argparse.Namespace) -> int:
    """M8-3 Task 8：查询/过滤/聚合 ``JsonlHook`` 导出的事件 JSONL 文件。

    逐行读取并按 ``--run-id`` / ``--node`` / ``--event``（AND 语义）过滤，
    命中行按**原文本**输出（不做重排序）；``--last N`` 只保留过滤后最后 N 行；
    ``--summary`` 输出每节点聚合表格（与明细输出互斥）。损坏行（非 JSON /
    非 dict / 缺 event 字段）记 warn 后跳过，不中断。
    退出码：正常（含空文件、过滤后无匹配）0；文件不存在/无法读取 1。
    """
    if args.last is not None and args.last < 0:
        print("--last 必须是 ≥0 的整数", file=sys.stderr)
        return 2
    path = Path(args.file).expanduser()
    if not path.is_file():
        print(f"事件文件不存在：{path}", file=sys.stderr)
        return 1
    kept: list[tuple[str, dict[str, Any]]] = []  # (原始行文本, 解析后事件)
    try:
        with path.open("r", encoding="utf-8") as fh:
            for lineno, raw in enumerate(fh, 1):
                text = raw.strip()
                if not text:  # 空行静默跳过（常见于文件末尾多余换行）
                    continue
                try:
                    ev = json.loads(text)
                except ValueError:
                    log.warning(
                        "第 %d 行不是合法 JSON，已跳过：%s", lineno, text[:80]
                    )
                    continue
                if not isinstance(ev, dict) or not ev.get("event"):
                    log.warning(
                        "第 %d 行不是事件对象或缺少 event 字段，已跳过", lineno
                    )
                    continue
                if not _event_matches(ev, args.run_id, args.node, args.event):
                    continue
                kept.append((raw.rstrip("\n"), ev))
    except (OSError, UnicodeDecodeError) as exc:
        print(f"事件文件无法读取：{exc}", file=sys.stderr)
        return 1

    if args.last is not None:
        kept = kept[-args.last:] if args.last > 0 else []
    if args.summary:
        _print_events_summary(kept)
    else:
        for text, _ev in kept:
            print(text)
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


# --------------------------------------------------------------------------- #
# M9 Task 10：runs —— 查询 RunStore 运行历史（列表/详情，复用 store 查询 API）
# --------------------------------------------------------------------------- #
def _fmt_ts(epoch: Any) -> str:
    """时间戳（秒）格式化为本地时间字符串；无记录返回 '（无）'。"""
    if not isinstance(epoch, (int, float)):
        return "（无）"
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(epoch))


def _row_total(raw: Any) -> int | None:
    """把 nodes.rows_in/rows_out 的 JSON 文本求和为总行数（无记录返回 None）。"""
    if not raw:
        return None
    try:
        data = json.loads(raw)
    except (TypeError, ValueError):
        return None
    if not isinstance(data, dict):
        return None
    total = sum(v for v in data.values() if isinstance(v, (int, float)))
    return int(total) if total else None


def _runs_list(store: RunStore, args: argparse.Namespace) -> int:
    """列表模式：--status 精确过滤、--pipeline 子串过滤、--last 截取全部下推到 SQL。"""
    sql = "SELECT run_id, pipeline, version, status, started_at, finished_at FROM runs"
    conds: list[str] = []
    params: list[Any] = []
    if args.status:
        conds.append("status = ?")
        params.append(args.status)
    if args.pipeline:
        conds.append("pipeline LIKE ?")
        params.append(f"%{args.pipeline}%")
    if conds:
        sql += " WHERE " + " AND ".join(conds)
    sql += " ORDER BY started_at DESC, run_id DESC"
    if args.last is not None:
        sql += " LIMIT ?"
        params.append(args.last)
    rows = store.query(sql, tuple(params))

    items = [
        {
            "run_id": r[0],
            "pipeline": r[1],
            "version": r[2],
            "status": r[3],
            "started_at": _fmt_ts(r[4]),
            "finished_at": _fmt_ts(r[5]),
        }
        for r in rows
    ]
    if args.json:
        print(json.dumps(items, ensure_ascii=False, indent=2))
        return 0
    if not items:
        print("（无匹配的运行记录）")
    for item in items:
        print(
            f"{item['run_id']}  {item['pipeline']} v{item['version']}  "
            f"{item['status']}  {item['started_at']}"
        )
    return 0


def _runs_detail(store: RunStore, run_id: str) -> int:
    """详情模式：run 摘要 + 节点状态/尝试次数/错误摘要/行数。"""
    rows = store.query(
        "SELECT run_id, pipeline, version, status, started_at, finished_at "
        "FROM runs WHERE run_id = ?",
        (run_id,),
    )
    if not rows:
        print(f"运行记录不存在：{run_id}", file=sys.stderr)
        return 0
    run = rows[0]
    print(f"run_id   : {run[0]}")
    print(f"pipeline : {run[1]} (v{run[2]})")
    print(f"status   : {run[3]}")
    print(f"时间     : {_fmt_ts(run[4])} → {_fmt_ts(run[5])}")

    nodes = store.query(
        "SELECT node_id, op, status, attempts, cached, error, duration, "
        "rows_in, rows_out FROM nodes WHERE run_id = ? "
        "ORDER BY finished_at, node_id",
        (run_id,),
    )
    if not nodes:
        print("节点     ：（无记录）")
        return 0
    print(f"\n节点（{len(nodes)} 个）：")
    print(f"{'节点':<24}{'状态':<10}{'尝试':>6}{'耗时':>9}{'行数 in/out':>16}  算子")
    for node in nodes:
        node_id, op, status, attempts, cached, error, duration, rows_in, rows_out = node
        total_in, total_out = _row_total(rows_in), _row_total(rows_out)
        rows_text = "、".join(
            f"{name}={value}"
            for name, value in (("in", total_in), ("out", total_out))
            if value is not None
        ) or "—"
        mark = " [cache]" if cached else ""
        print(
            f"{node_id:<24}{status:<10}{attempts:>6}{duration:>9.2f}s"
            f"{rows_text:>16}  {op}{mark}"
        )
        if error:
            print(f"{'':<24}└─ {str(error).splitlines()[0][:120]}")
    return 0


def cmd_runs(args: argparse.Namespace) -> int:
    """M9 Task 10：查询 RunStore 运行历史。

    ``runs [--db] [--status] [--pipeline] [--last] [--json]`` 列表模式；
    ``runs <run_id> [--db]`` 详情模式。库不存在退出码 1；库存在但无匹配退出码 0。
    """
    if args.last is not None and args.last < 1:
        print("--last 必须是 ≥1 的整数", file=sys.stderr)
        return 2
    db = Path(args.db).expanduser()
    if not db.is_file():
        print(f"运行记录库不存在：{db}", file=sys.stderr)
        return 1
    with RunStore(db) as store:
        if args.run_id is not None:
            return _runs_detail(store, args.run_id)
        return _runs_list(store, args)


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
    run.add_argument(
        "--param-file", action="append", default=None,
        help="M9 从 JSON 文件加载流水线参数（顶层必须是对象，可多次指定；"
             "命令行 --param 覆盖文件值）",
    )
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
        "--executor", choices=("thread", "process", "process_pool", "queue"), default=None,
        help="运行级执行器：process 时未显式指定的节点在子进程执行（硬超时）；"
             "process_pool 时用常驻 worker 进程池复用执行（默认 min(8, CPU) 个 worker，"
             "硬超时 terminate 在跑 worker 并自动补位）；"
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
    run.add_argument(
        "--sample", type=int, default=None,
        help="M8-2 采样预览：每个节点解析输入后把 list 类型句柄截断为前 N 行，"
             "缓存读写全部禁用；必须是 ≥1 的整数（默认不采样）",
    )
    run.add_argument(
        "--preview", type=int, default=None,
        help="M9 值级预览：报告节点附带 preview_in/preview_out（list 取前 N 行、"
             "dict 取前 N 键、str 截 200 字符）；只影响报告不影响执行，"
             "必须是 ≥1 的整数（默认不采集）",
    )
    run.add_argument(
        "--report-out", default=None,
        help="M9 运行结束后把全量报告 JSON 落盘到指定路径"
             "（相对路径按工作目录解析，父目录自动创建；不传则默认不写文件）",
    )
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
        "--template",
        choices=("minimal", "data", "scheduler", "quality", "subpipeline"),
        default="minimal",
        help="模板类型：minimal / data / scheduler / quality / subpipeline"
             "（默认 minimal；quality=质量双门禁，subpipeline=父子编排）",
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

    explain = sub.add_parser(
        "explain", help="M8-2：输出解析后的最终生效配置（不执行节点、零文件产出）"
    )
    common(explain)
    explain.add_argument(
        "--param-file", action="append", default=None,
        help="M9 从 JSON 文件加载流水线参数（顶层必须是对象，可多次指定；"
             "命令行 --param 覆盖文件值）",
    )
    explain.add_argument("--node", default=None, help="只输出指定 id 的节点")
    explain.add_argument(
        "--json", action="store_true",
        help="输出 JSON：{pipeline, version, params, nodes, warnings}",
    )
    explain.set_defaults(func=cmd_explain)

    events = sub.add_parser(
        "events", help="M8-3：查询/过滤/聚合 JsonlHook 事件流（JSONL 文件）"
    )
    events.add_argument("file", help="JsonlHook 导出的事件 JSONL 文件路径")
    events.add_argument(
        "--run-id", default=None,
        help="只输出 run_id 匹配的事件（仅 run_finish 行携带 run_id，AND 语义）",
    )
    events.add_argument(
        "--node", default=None,
        help="只输出 node_id 匹配的节点事件（对应 node_start/node_finish 行）",
    )
    events.add_argument(
        "--event", choices=("node_start", "node_finish", "run_finish"), default=None,
        help="只输出指定类型的事件",
    )
    events.add_argument(
        "--last", type=int, default=None,
        help="过滤后只保留最后 N 行（必须是 ≥0 的整数）",
    )
    events.add_argument(
        "--summary", action="store_true",
        help="输出每节点聚合文本表格（执行次数/总耗时/失败数），不输出明细",
    )
    events.set_defaults(func=cmd_events)

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

    runs = sub.add_parser("runs", help="M9：查询运行历史记录（RunStore SQLite 库）")
    runs.add_argument(
        "run_id", nargs="?", default=None,
        help="运行记录 run_id；省略则列出运行记录（可配 --status/--pipeline/--last 过滤）",
    )
    runs.add_argument("--db", default="runs.db", help="运行记录库路径（默认 runs.db）")
    runs.add_argument(
        "--status", default=None,
        help="按状态过滤（如 success / failed / partial）",
    )
    runs.add_argument(
        "--pipeline", default=None,
        help="按流水线名过滤（子串匹配）",
    )
    runs.add_argument(
        "--last", type=int, default=None,
        help="最多列出最近 N 条（必须是 ≥1 的整数；默认全部）",
    )
    runs.add_argument("--json", action="store_true", help="列表以 JSON 数组输出")
    runs.set_defaults(func=cmd_runs)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    _setup_logging(getattr(args, "verbose", False))
    load_plugins(entry_points=True)  # 内置算子 + 已安装插件包入口点（M3-4，坏插件只告警）
    return int(args.func(args) or 0)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
