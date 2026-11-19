# op_pipeline —— 算子级、配置驱动的 Python 流水线框架

> 一句话：**算子是一个自描述的可复用单元，流水线只是算子之间的一份配置。**
> 零第三方依赖（纯标准库，Python ≥ 3.11），可单独嵌入任何 Python 项目。

## 文档导航

| 文档 | 内容 |
| --- | --- |
| [快速上手](docs/guides/quickstart.md) | 安装、第一个流水线、Python 内嵌调用、自定义算子、常见错误 |
| [运维指南](docs/guides/operations.md) | scheduler_app 调度/告警/保留策略、Web UI、RunStore、远程 worker 与故障排查 |
| [API 参考](docs/reference/api.md) | 包顶层稳定 API 清单与稳定性承诺 |
| [设计文档索引](docs/README.md) | 架构、运行时语义、扩展设计、配置规范与算子清单 |

---

## 1. 设计目标与取舍

| 目标 | 做法 | 明确的取舍 |
| --- | --- | --- |
| 算子级封装 | `Operator` 基类统一声明 `inputs / outputs / params`，注册表按名字解耦编排层 | 不做 schema 强校验（类型只做软校验），因为数据形态变化快 |
| 完全可配置 | JSON / TOML / YAML 描述 DAG、参数、连线、运行策略 | 不把 Python 代码写进配置（表达式走 AST 白名单，非 `eval`） |
| 可单测 | 算子不依赖引擎内部状态，`Operator.execute(ctx, inputs)` 可被单独调用 | — |
| 可增量扩展 | `@op` 装饰器 + 插件加载（模块路径/文件路径），不改编排层 | 不做算子市场/版本仓库 |
| 轻量可嵌入 | 无调度器、无数据库、无服务端；`run_pipeline()` 一行调用 | 不做分布式调度、不做多租户（见第 9 节） |

与 Airflow / Prefect / Dagster 的定位差异：那三者是**平台**（调度、元数据、UI、重试策略中心化）；
本项目是**库**——适合"脚本级 ETL/数据处理/报告生成"要长期维护、又想保留可嵌入性的场景。

---

## 2. 分层架构

```
┌──────────────────────────────────────────────────────────────────────┐
│  配置层  config            JSON / TOML / YAML  →  PipelineSpec       │
│          spec.py           节点、参数、输出、引用解析、DAG 编译        │
├──────────────────────────────────────────────────────────────────────┤
│  编排层  engine.py         依赖就绪调度、并行、重试、超时、失败传播     │
│          cache.py          内容指纹缓存（可关）                       │
├──────────────────────────────────────────────────────────────────────┤
│  契约层  operator.py       算子基类：声明 + 执行 + 输出归一            │
│          ports.py / params.py   端口与参数的类型/默认值/校验          │
│          registry.py       @op 注册表 + 插件发现                      │
├──────────────────────────────────────────────────────────────────────┤
│  算子层  ops/              io / table / stats / text / report / control │
│          （业务算子以插件形式平级加入，不与框架耦合）                   │
└──────────────────────────────────────────────────────────────────────┘
```

**数据流**：算子之间不直接调用，只通过"句柄"交换数据。
`node.port` 默认就是句柄名（可在配置里改名），引擎在节点成功后把输出写进句柄表，
下游按 `${handle}` 取用——因此**任意两个算子之间都可以替换、旁路、并行**。

```
load_orders ──▶ clean ──▶ unit_price ──▶ agg ──┐
load_regions ────────────────────────────────▶ joined ──▶ scored ──▶ ranked ──▶ top ──┬─▶ report ─▶ write_md
                                                                        (when: n>0)   ├─▶ html_report ─▶ write_html
                                                                                      ├─▶ write_csv
                                                                                      └─▶ summary ─▶ write_json
```

---

## 3. 目录结构

```
op_pipeline/
├── __init__.py        公开 API（Operator / op / Param / run_pipeline / run ...）
├── operator.py        算子基类与执行契约
├── ports.py           端口声明与软类型检查
├── params.py          参数声明、类型归一、校验
├── registry.py        注册表 + @op + 插件加载
├── spec.py            PipelineSpec / NodeSpec / 引用解析 / 计划编译 / 拓扑排序
├── engine.py          调度执行 + 运行报告 + 失败传播
├── cache.py           内容指纹缓存
├── context.py         RunContext / NodeContext（日志、产物路径、参数）
├── expression.py      安全表达式求值（AST 白名单）
├── graph.py           Mermaid / Graphviz / ASCII 渲染
├── cli.py             python -m op_pipeline
└── ops/               内置算子（34 个）
examples/
├── pipeline_orders.json / .toml    订单汇总（18 节点，含 fan-out 并行）
├── pipeline_text.toml              文本摘要（插件算子 + 重试 + 句柄下钻）
├── plugins/demo_ops.py             自定义算子示例
└── data/*.csv, *.txt               示例数据
tests/test_op_pipeline.py           21 个用例（unittest，零依赖）
```

---

## 4. 快速开始

```powershell
# 校验配置（不执行）
python -m op_pipeline validate examples/pipeline_orders.json

# 执行，并输出 JSON 运行报告
python -m op_pipeline run examples/pipeline_orders.json --json examples/output/run_report.json

# 覆盖流水线参数、看依赖图、看执行计划
python -m op_pipeline run examples/pipeline_orders.json --param min_amount=50 --param top_n=3
python -m op_pipeline graph examples/pipeline_orders.json --format mermaid
python -m op_pipeline run examples/pipeline_orders.json --dry-run
python -m op_pipeline ops
python -m op_pipeline describe table.join
```

库内调用：

```python
from op_pipeline import run

report, values = run("examples/pipeline_orders.json", overrides={"min_amount": 50})
print(report.summary())
print(values["top.table"][0]["region"])
```

---

## 5. 配置参考

支持 **JSON / TOML / YAML**（内容同构），`.yaml` 需 `pip install pyyaml`；扩展格式经
`PipelineSpec.register_loader(suffix, fn)` 注册（M3-8，如 `.ini`）。顶层字段见下表，
`includes`（M3-7）可按相对路径拆分复用配置。

### 5.1 顶层字段

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `spec_version` | str | 配置格式版本（可选，缺省 `"1"`）：高于框架当前版本报错；旧版本配置按 `MIGRATIONS` 迁移器逐级升级 |
| `name` / `version` / `description` | str | 流水线标识，进报告 |
| `params` | dict | 流水线参数，可被 `--param` 覆盖，参数内可用 `${env.X}` |
| `nodes` | list | 节点数组，顺序无关（执行由依赖决定） |
| `outputs` | dict | 流水线对外产物：名字 → 引用 |
| `includes` | list[str] | 配置组合（可选）：相对配置文件目录的子配置路径，按顺序合并（params/defaults/outputs 覆盖、nodes 拼接），主配置优先；节点 id 重复报错；循环引用报错（`from_file` 下自动按目录解析） |
| `defaults` | dict | 预留的默认运行策略（当前由节点字段直接表达） |

### 5.2 节点字段

| 字段 | 默认 | 说明 |
| --- | --- | --- |
| `id` | 必填 | 节点唯一标识，默认句柄前缀 |
| `op` | 必填 | 注册表中的算子名 |
| `inputs` | `{}` | 端口 → 引用，`"${upstream.port}"` |
| `params` | `{}` | 算子参数，值可含引用；未声明参数直接报错 |
| `outputs` | `{}` | 端口 → 句柄名（改名以便下游引用，或避免句柄冲突） |
| `when` | 无 | 布尔表达式，假则跳过（下游连带跳过） |
| `retries` / `retry_delay` | 0 / 0 | 失败重试次数与指数退避基数；配置类错误不重试 |
| `timeout` | 无 | 秒级**软超时**（到点标失败，底层线程不可强杀） |
| `cache` | false | 是否参与内容指纹缓存（算子须声明 `deterministic=True`） |
| `allow_failure` | false | 失败不视为致命（报告标 `partial`，下游仍会因缺输入跳过） |
| `description` | "" | 备注 |

### 5.3 引用语法

| 写法 | 含义 |
| --- | --- |
| `${load.table}` | 上游节点输出（默认句柄 `id.port`） |
| `${raw}` | 用 `outputs` 改过名的句柄 |
| `${stats.stats.最长行}` | 句柄下钻（dict 键 / list 下标，最长前缀优先） |
| `${p.min_amount}` | 流水线参数 |
| `${env.HOME}` | 环境变量 |
| `"${p.dir}/report.md"` | 字符串内混排（整串是单一引用时保留原始类型） |

`when` 表达式的变量来自：本节点输入端口值、本节点参数值、`inputs` / `params` 两个字典。
支持比较、布尔、算术、`in`、下标和少量白名单函数（`len/str/int/float/round/min/max/sum/sorted/any/all`），
**禁止属性访问、禁止 `eval`/`open`/`__import__` 一类逃逸**。

---

## 6. 执行语义（写死在引擎里，不靠约定）

1. **调度**：依赖就绪即调度，互不依赖的节点并行（线程池，默认 `min(8, CPU)`；算子为同步函数）。
2. **失败传播**：节点失败 → 下游 `skipped`；**无依赖关系的并行分支继续执行**（这是配置化流水线应有的行为）。
   `fail_fast=True` 时停止调度新节点。
3. **跳过**：`when` 为假 → 该节点 `skipped`，下游同因缺输入而 `skipped`。
4. **重试**：`retries` 次、`retry_delay * 2^(n-1)` 退避；参数/契约类错误（`ConfigError`）不重试。
5. **缓存**：key = 算子名 + 版本 + 解析后参数指纹 + 上游输入指纹。改上游数据自动失效，
   改下游逻辑不影响上游复用。命中时报告里标 `[cache]`。
6. **报告状态**：`success` / `partial`（有跳过或有 `allow_failure` 失败）/ `failed`（有硬失败）。

---

## 7. 写一个算子（算子级封装的标准姿势）

```python
from op_pipeline import Operator, Param, op

@op("demo.conversion_rate")                       # 注册名即配置里 op 的取值
class ConversionRate(Operator):
    description = "计算转化率并做阈值标记"

    inputs  = [("orders", int), ("visits", int)]  # 端口声明（也可用 dict 写法）
    outputs = [("rate", float), ("flagged", bool)]
    params  = {
        "target": Param(float, default=0.05, minimum=0.0, maximum=1.0),
        "label":  Param(str, default="conversion"),
    }

    def run(self, ctx, orders, visits, target, label):
        rate = (orders / visits) if visits else 0.0
        ctx.log(f"{label} = {rate:.2%}")
        return {"rate": rate, "flagged": rate >= target}
```

规则就三条：

1. **只依赖 `ctx`**：日志走 `ctx.log`，落盘走 `ctx.artifact_path()`，不要在算子里拼全局路径；
2. **返回 dict 按端口名**（单输出可返回裸值），返回未声明的键会报错——契约不会被悄悄破坏；
3. **参数声明即校验**：未声明参数报错、必填缺失报错、类型可归一（`"3"` → `3`）。

单测示例（不需要跑引擎）：

```python
op_ = default_registry.get("demo.conversion_rate")(target=0.1)
out = op_.execute(ctx=None, inputs={"orders": 3, "visits": 100})
assert out["flagged"] is False
```

**加载插件**：

```powershell
python -m op_pipeline run my_pipeline.toml --plugin plugins/my_ops.py
```

```python
from op_pipeline import load_plugins
load_plugins(modules=["my_company.ops"], paths=["plugins/my_ops.py"], base_dir=".")
```

插件信任（M4-1）：`allowlist` 白名单限制可加载来源（入口点名/模块名/文件路径，清单外
告警跳过，`strict=True` 时拒绝）；`verify_manifest=True` 用 `<base_dir>/OP_PLUGINS.sha256`
（sha256sum 格式）校验文件插件哈希。详见 [扩展设计 §3.5](docs/design/extension-points.md#35-插件信任白名单与哈希清单校验m4-1)。

**入口点自动发现（M3-4）**：插件包在自己的 `pyproject.toml` 声明
`[project.entry-points."op_pipeline.ops"]` 后 pip 安装，CLI（`ops`/`run` 等，默认开启）
与库调用 `load_plugins(entry_points=True)` 即可免 `--plugin` 直接使用：

```python
from op_pipeline import load_plugins
load_plugins(entry_points=True)  # 扫描已安装插件包入口点
```

发布插件包的完整指南见 [扩展设计 §3.4](../op-pipeline/docs/design/extension-points.md#34-插件包发布指南pip-分发m3-4)。

---

## 8. 内置算子（50 个）

| 分组 | 算子 | 说明 |
| --- | --- | --- |
| IO | `io.read_csv` `io.write_csv` `io.read_json` `io.write_json` `io.read_text` `io.write_text` | 表格以 `list[dict]` 为通用交换格式 |
| 表 | `table.filter_rows` `select_columns` `rename_columns` `drop_columns` `derive_column` `sort_rows` `dedupe` `limit` `join` `union` `take_column` | 哈希连接、表达式派生列；列判定按全表并集 |
| 统计 | `stats.aggregate` `count_rows` `value_counts` `describe` | `sum/mean/count/min/max/median/std/nunique/first/last` |
| 文本 | `text.template` `split_lines` `join_lines` `regex_extract` `replace_regex` | 模板用 `{{key}}`，避开 `${}` |
| 报告 | `report.markdown` `report.html` | 单文件 HTML，无前端依赖 |
| 控制 | `control.const` `if_else` `switch` `assert` `passthrough` `merge_dicts` | 常量注入、分支、数据质量门禁 |
| HTTP | `http.request` | GET/POST、json/text 双视图、超时与重试 |
| DB | `db.sqlite_exec` `db.sqlite_query` `db.read_mysql` `db.write_mysql` `db.read_postgres` `db.write_postgres` | SQLite 用标准库 sqlite3；MySQL/PostgreSQL 走 DSN（可选 extras），均参数化绑定，写后读须接 `gate` 排序 |
| DataFrame | `df.from_table` `df.to_table` `df.to_csv` | pandas 可选依赖（`pip install op-pipeline[dataframe]`），重计算段交给 pandas |
| Arrow/Parquet | `df.from_arrow` `df.to_arrow` `io.read_parquet` `io.write_parquet` | pyarrow 可选依赖（`pip install op-pipeline[parquet]`），行表 ↔ pyarrow Table / Parquet 文件（M4-2） |
| Excel | `io.read_excel` `io.write_excel` | openpyxl 可选依赖（`pip install op-pipeline[excel]`），行表 ↔ .xlsx、多 sheet 读写（M4-3） |

> extras 说明：内核 import 路径保持零第三方依赖，可选功能做成 extras 延迟导入——
> `dataframe`（pandas）、`parquet`（pyarrow）、`excel`（openpyxl）、
> `mysql`（pymysql，`db.read_mysql`/`db.write_mysql`，M5-3）、
> `postgres`（psycopg2，`db.read_postgres`/`db.write_postgres`，M5-3）、
> `yaml`（pyyaml，`.yaml` 配置格式）、`otel`（opentelemetry-api，
> `observe.OtelHook` 的可选 OTel bridge，未安装只告警跳过）。

---

## 8. 观测性（M3-6）

`run_pipeline(..., hooks=[...])` 之外，`op_pipeline.observe.JsonlHook` 把一次运行的事件流
导出为 JSON lines（NDJSON），每行一个事件：`node_start` / `node_finish` / `run_finish`，
与运行报告同一事实源（`run_finish` 行直接取自 `RunReport`，字段一致）。线程安全，节点
并行执行不丢行。

```python
from op_pipeline import run_pipeline
from op_pipeline.observe import JsonlHook

hook = JsonlHook("runs/events.jsonl")
report, values = run_pipeline(spec, hooks=[hook])
hook.close()
```

可选 OTel bridge：`observe.OtelHook`（`pip install op-pipeline[otel]`）在运行结束时把报告
摘要记为 `op_pipeline.run` span；未安装 `opentelemetry-api` 时只告警、不影响流水线。

---

## 9. 远程执行示范（M4-4）

`QueueWorkerExecutor`（`from op_pipeline import QueueWorkerExecutor`）实现
`Executor` 协议，但把任务分发给**独立 worker 进程**执行，客户端与 worker 之间
只交换共享文件：

- 客户端 `exec_.submit(fn, *args)`：载荷 `pickle → base64` 追加进 JSONL 队列
  文件；载荷不可 pickle 时直接抛 `RuntimeError`；
- worker：`python -m op_pipeline worker --queue jobs.jsonl --out results [--count N]`
  增量消费队列并执行，结果/异常写回 `results/<job_id>.json`（先写 `.tmp`
  再原子改名，`wait` 不会读到半个结果）；
- 客户端 `exec_.wait([...])` 轮询结果文件，`f.result()` 取结果或抛回远端异常。

```python
import operator
from op_pipeline import QueueWorkerExecutor

exec_ = QueueWorkerExecutor("jobs.jsonl", "results", poll=0.05)
f = exec_.submit(operator.add, 40, 2)
exec_.wait([f])
print(f.result())  # 42
```

双进程端到端示范见 [examples/remote_exec_demo.py](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/examples/remote_exec_demo.py)
（脚本扮演引擎进程提交 3 个任务、另起 worker 子进程消费，含异常回写演示）。
数据生态示例 [examples/pipeline_data_ecosystem.toml](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/examples/pipeline_data_ecosystem.toml)
（CSV → Parquet → Excel 往返读写）：

```bash
python -m op_pipeline run examples/pipeline_data_ecosystem.toml --no-cache
```

---

## 10. 调度与 Web UI 外围服务（M5-2）

独立子包 `scheduler_app/`（纯标准库，**不进内核**——`op_pipeline` 不引用它，保持
"零依赖、零守护进程"承诺）：

```powershell
# 调度守护进程：启动立即执行一次，之后每 60 秒一轮，运行记录写 RunStore SQLite
python -m scheduler_app.scheduler --pipeline examples/pipeline_orders.json --every 60 --db runs.db

# 轻量 Web UI（http.server）：运行历史列表 + 单次运行详情（节点状态/耗时/traceback 摘要）
python -c "from scheduler_app import create_server; create_server('runs.db', 8000).serve_forever()"
```

---

## 11. 已知限制与后续演进

| 限制 | 现状 | 演进方向 |
| --- | --- | --- |
| 超时是软超时 | 线程无法被强杀，超时后底层仍可能继续跑 | 引入进程池/子进程执行器做硬超时 |
| 并发模型 | 同步算子 + 线程池；`async def run` 会被自动放进事件循环 | 换成 asyncio 调度器以支撑高并发 IO |
| 无持久化元数据 | 只有 JSON 运行报告 + 文件缓存 | 接 SQLite 记录 run/node 状态，支持断点续跑 |
| 类型只做软校验 | 端口类型不符仅告警 | 引入可选 schema（pydantic/dataclass）做深校验 |
| 无分布式 | 单机进程内 | 抽出 `Executor` 接口，接队列/集群 |
| 大对象 | 句柄常驻内存，缓存走 pickle 文件 | 接对象存储/Arrow 表，句柄传引用 |

---

## 12. 自测

```powershell
python -m unittest discover -s tests -v
```

覆盖：算子契约与参数校验、表达式白名单拦截、未知算子/未知句柄/缺输入/环检测、
端到端文件读写、4 路并行耗时、重试与尝试次数、失败传播与旁路分支继续、
`when` 跳过、缓存复用、句柄下钻、JSON/TOML 解析、插件加载。
