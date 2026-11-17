# 扩展设计

本文说明**在不改编排层的前提下**可以扩展哪些东西、怎么扩；以及要实现 README §9 中的演进方向时，
应该落在哪些文件的哪些位置。

---

## 1. 扩展点总览

| 扩展点 | 机制 | 需要改动的代码 | 稳定性 |
| --- | --- | --- | --- |
| 新增算子 | `@op` 装饰器 + `Operator` 子类 | 只新增文件，不改框架 | 稳定 |
| 算子插件 | 模块路径 / 文件路径加载 | 配置或调用方传参 | 稳定 |
| 新配置格式 | `PipelineSpec.from_file` 的分支 | `spec.py` | 内部接口 |
| 算子别名 | `Registry.register(aliases=...)` | 算子声明 | 稳定 |
| 自定义缓存 | 传入 `CacheStore` 兼容对象 | `run_pipeline` / `Engine` | 弱契约（鸭子类型） |
| 替换执行器 | 目前无 `Executor` 抽象 | 需先抽接口 | **未实现** |
| 新渲染格式 | `graph.py` 增加函数 + CLI choices | `graph.py` / `cli.py` | 内部接口 |

---

## 2. 写一个算子

### 2.1 最小骨架

```python
from op_pipeline import Operator, Param, op

@op("domain.conversion_rate")
class ConversionRate(Operator):
    description = "计算转化率并做阈值标记"
    version = "1"                                  # 逻辑变更时递增，用于缓存失效

    inputs  = [("orders", int), ("visits", int)]
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

### 2.2 必须遵守的六条约束

1. **签名即契约**：`run(self, ctx, <输入端口...>, <参数...>)`，名字必须与声明完全一致；
   端口名是合法标识符且不能是关键字，**不能与参数同名**（注册期校验）。
2. **返回值**：多输出必须返回 dict；单输出可返回裸值；不允许返回未声明的键。
3. **optional 输入**：未接线时收到 `None`，实现里必须自行兜底。
4. **副作用只走 ctx**：`ctx.log()`、`ctx.artifact_path()`、`ctx.save_artifact()`、`ctx.resolve_path()`；
   不要 `os.chdir`、不要写全局路径、不要改环境变量。
5. **确定性标注**：有随机性、依赖当前时间、依赖外部可变状态或**有落盘副作用**的算子，
   必须 `deterministic = False`，否则 `cache=true` 会跳过落盘。
6. **异常语义**：普通异常会被重试（按 `retries`）；只有 `ConfigError` 及其子类不重试。
   数据内容错误（列不存在、模板缺 key）用 `ValueError`/`KeyError`，不要用 `ConfigError`。

### 2.3 单测方式（不需要跑引擎）

```python
from op_pipeline import default_registry

op_ = default_registry.get("domain.conversion_rate")(target=0.1)
out = op_.execute(ctx=None, inputs={"orders": 3, "visits": 100})
assert out["flagged"] is False
```

`execute()` 会做输入/输出契约校验，因此这类测试同时覆盖契约正确性。
需要日志或产物路径时，构造一个带 `log / artifact_path / resolve_path` 的假 ctx 即可。

---

## 3. 插件加载

### 3.1 两种加载方式

```powershell
# 文件路径（相对配置文件所在目录解析）
python -m op_pipeline run my_pipeline.toml --plugin plugins/my_ops.py
```

```python
from op_pipeline import load_plugins
load_plugins(modules=["my_company.ops"], paths=["plugins/my_ops.py"], base_dir=".")
```

加载顺序（[registry.py#L119-L166](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/registry.py#L119-L166)）：
内置 `op_pipeline.ops` → `modules` → `paths`。文件加载的模块名是 `_op_plugin_<stem>`。

### 3.2 约束与坑

| 约束 | 说明 |
| --- | --- |
| 导入即注册 | 插件模块必须在 import 时执行 `@op`；仅定义类不注册 |
| 同名去重 | 以 `stem` 去重，两个不同目录的同名文件只有第一个生效 |
| 默认注册表 | 插件注册到 `default_registry`；如需隔离，用 `op(registry=...)` 自建 `Registry` |
| 覆盖 | 重新注册同名算子需 `replace=True`（测试中大量使用） |
| 依赖方向 | 插件只能 import `op_pipeline` 的公开符号，不得 import `op_pipeline.engine` 内部实现 |
| 入口限制 | `run()` 辅助函数只接受文件路径插件，不接受模块名；需要模块插件时直接调 `load_plugins()` |

---

## 4. 替换/扩展外部能力

### 4.1 缓存

`Engine` 只依赖 `CacheStore` 的鸭子类型：

```python
class CacheStore:
    enabled: bool
    def compute_key(self, op_name, op_version, params, inputs) -> str
    def load(self, key) -> dict | None
    def store(self, key, outputs) -> None
```

因此可以把 `CacheStore` 换成 Redis/S3 实现，只要保持四个成员。
注意 `Engine.__init__` 用 `cache_store or CacheStore(None)` 判定，空实现的布尔值为真即可（普通对象默认为真）。

### 4.2 执行器（尚未抽象）

当前执行器写死在 `Engine`：`ThreadPoolExecutor` + `wait(FIRST_COMPLETED)` + `_run_node`。
若要接进程池、队列或集群，需要先抽出如下接口（建议放在 `engine.py` 或新 `executor.py`）：

```python
class Executor(Protocol):
    def submit(self, plan_node, inputs) -> Future   # 返回可 wait 的句柄
    def wait(self, futures, first_completed=True) -> set[Future]
```

需要一起调整的点：

1. `_run_node` 里依赖 `self.handles` 快照与 `ctx`，跨进程时句柄必须可序列化或改为引用传递；
2. `NodeContext` 需要能序列化（logger 不可序列化，要换成日志转发）；
3. 超时策略从"线程 join"改为"进程终止/任务取消"；
4. `_publish` 的锁需要换成协调层（队列/共享存储）。

### 4.3 报告

`RunReport` 是纯数据类，`to_dict()` / `to_json()` 可直接用于外部系统。
扩展方向是加字段而不是改结构，避免破坏现有调用方（例如 `status`、`nodes[].attempts` 已被 README 与测试依赖）。

---

## 5. 已知技术债与实现缺口

下表是**代码已有但未接入**或**语义有缺口**的地方。这些不是文档推测，均可在源码中验证。

| # | 缺口 | 证据 | 影响 | 建议落点 |
| --- | --- | --- | --- | --- |
| T1 | 端口软类型检查未接入 | `check_handle_types()`、`port_warnings()` 无调用方 | README 宣称"类型不符仅告警"实际不发生 | `Engine.run()` 结束前调用并写入 `report.warnings` |
| T2 | `PlanNode.resolved_inputs` 恒为 `{}` | [spec.py#L455](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/spec.py#L455) | 无功能影响，属于残留字段 | 删除字段或改为运行期回填 |
| T3 | `ModuleExecutionError` 定义了但没抛 | [errors.py#L54-L67](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/errors.py#L54-L67) | 调用方无法按异常类型识别节点失败 | 引擎包装异常时改用该类，或删除 |
| T4 | `PipelineSpec.defaults` 解析但未生效 | 只在 `from_dict/to_dict` 出现 | 用户以为能配置默认 retries/cache | 在 `build_plan` 合并 `defaults`，或删除该字段 |
| T5 | `Engine._warnings` 无写入方 | [engine.py#L193](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/engine.py#L193) | 除输出解析失败外没有引擎级告警 | 配合 T1 写入 |
| T6 | `cache-dir` 相对进程 CWD | [cli.py#L142](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/cli.py#L142) | 在不同目录启动 CLI 会产生多份缓存 | 改为相对 `workdir` 解析 |
| T7 | 缓存不感知代码变更 | key 仅含 `version` 字符串 | 忘记递增 version 会命中旧结果 | 可选：把算子源码哈希或 git rev 纳入 key |
| T8 | 软超时无法终止算子 | [engine.py#L459-L484](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/engine.py#L459-L484) | 超时后线程仍可能写文件/占连接 | 换进程执行器，或文档要求算子自身支持取消 |
| T9 | 失败不保留堆栈 | 只存 `type: message` | 线上排障依赖日志 | `NodeResult` 增加 `traceback`（截断） |
| T10 | `run_id` 秒级精度 | [context.py#L94](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/context.py#L94) | 同秒并发会共用产物目录 | 加随机后缀或纳秒 |
| T11 | 插件按 stem 去重 | [registry.py#L155-L158](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/registry.py#L155-L158) | 同名文件静默不加载 | 改用路径哈希作模块名 |
| T12 | `run()` 不支持模块插件 | `plugins` 参数只传 `paths` | 库调用方需自行 `load_plugins` | 增加 `plugin_modules` 参数 |
| T13 | `strict=False` 可返回残缺 Plan | [spec.py#L477-L487](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/spec.py#L477-L487) | 无调用方使用，属预留 | 保持现状并在文档说明 |
| T14 | `to_mermaid` 中 `style` 死变量 | [graph.py#L16-L18](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/graph.py#L16-L18) | 无影响 | 清理 |
| T15 | 表算子假设首行代表全表列 | 多处使用 `rows[0].keys()` | 列不全的行会丢字段或抛错 | 统一列集合并集，或要求 schema 显式声明 |

> 修复建议的优先级：T1/T5（文档承诺与实现不一致）> T6（隐性正确性问题）> T8/T9（可运维性）> 其余。

---

## 6. 演进路线与落点

对应 README §9 的六项限制：

| 演进方向 | 具体设计 | 落点 |
| --- | --- | --- |
| 硬超时 | 引入进程池执行器；`timeout` 到点 `terminate()`；返回码 + 序列化结果/异常 | 新 `executor.py`；`Engine._execute_once` |
| asyncio 调度 | 算子支持 `async def run`（已有）；调度器改为任务集合 + `asyncio.wait(FIRST_COMPLETED)`；线程池仅用于同步算子 | `Engine.run/_dispatch` |
| 持久化元数据 | SQLite 表 `runs / nodes`，记录 status/attempts/error/时间；据此实现断点续跑 | 新 `store.py`；`Engine.run` 汇总处 |
| 可选 schema | 端口支持 `dataclass`/`pydantic` 类型；在 `_prepare_inputs` 前做深校验 | `ports.value_matches` + `Operator._prepare_inputs` |
| 分布式 | 抽出 `Executor` 协议；句柄改引用（对象存储 key / Arrow 表） | `engine.py` + `context.py` |
| 大对象 | 句柄表存引用而非值；`RefResolver` 解析时按需加载 | `Engine._publish` / `RefResolver` |

这些演进有一个共同前置条件：**句柄访问必须收敛到少数几个函数**。
目前是 `Engine._publish`（写）与 `RefResolver._lookup`（读）两处，改造时应保持这一点，
不要在算子内部直接读写句柄表。

---

## 7. 新增能力时的回归清单

1. 跑 `python -m unittest discover -s tests -v`（21 个用例）；
2. 用 `examples/pipeline_orders.json` 做端到端（覆盖并行 fan-out、when、断言、4 种输出）；
3. 用 `examples/pipeline_text.toml --plugin examples/plugins/demo_ops.py` 验证插件与重试；
4. `python -m op_pipeline validate` 与 `graph` 必须仍然可用；
5. 若改变节点契约（字段/语义），同步更新 [配置规范](../reference/config-schema.md) 与
   [运行时语义](../architecture/runtime-semantics.md)。
