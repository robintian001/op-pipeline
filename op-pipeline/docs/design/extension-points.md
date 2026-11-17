# 扩展设计

本文说明**在不改编排层的前提下**可以扩展哪些东西、怎么扩；以及要实现 README §9 中的演进方向时，
应该落在哪些文件的哪些位置。

---

## 1. 扩展点总览

| 扩展点 | 机制 | 需要改动的代码 | 稳定性 |
| --- | --- | --- | --- |
| 新增算子 | `@op` 装饰器 + `Operator` 子类 | 只新增文件，不改框架 | 稳定 |
| 算子插件 | 模块路径 / 文件路径加载 | 配置或调用方传参 | 稳定 |
| 表达式函数 | `register_function(name, fn)`（纯函数、禁覆盖内置名） | 只新增调用，不改框架 | 稳定 |
| 新配置格式 | `PipelineSpec.from_file` 的分支 | `spec.py` | 内部接口 |
| 算子别名 | `Registry.register(aliases=...)` | 算子声明 | 稳定 |
| 自定义缓存 | 传入 `CacheStore` 兼容对象 | `run_pipeline` / `Engine` | 弱契约（鸭子类型） |
| 替换执行器 | 实现 `Executor` 协议（submit/wait/shutdown）；内置 `ThreadExecutor`（默认）与 `ProcessExecutor`（硬超时，M2-2） | [executor.py](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/executor.py) | 稳定（M2-1） |
| 替换句柄存储 | 实现 `HandleStore` 协议（get/put/snapshot/items）；内置 `InMemoryHandleStore`（默认） | [handlestore.py](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/handlestore.py) | 稳定（M2-3） |
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

加载顺序（[registry.py#L177-L253](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/registry.py#L177-L253)）：
内置 `op_pipeline.ops` → `modules` → `paths`。文件加载的模块名是
`_op_plugin_<路径 blake2b 摘要>`（同一文件重复加载幂等跳过；指定 `registry` 时为
`_op_plugin_<stem>_<序号>`，跨注册表各执行一次）。

### 3.2 约束与坑

| 约束 | 说明 |
| --- | --- |
| 导入即注册 | 插件模块必须在 import 时执行 `@op`；仅定义类不注册 |
| 同名去重 | 按**路径**去重（模块名取路径摘要）：不同目录的同名文件都会加载；同一文件重复加载幂等跳过 |
| 默认注册表 | 不指定 `registry` 时一切注册落到 `default_registry`；隔离方案见 §3.3 |
| 覆盖 | 重新注册同名算子需 `replace=True`（测试中大量使用） |
| 依赖方向 | 插件只能 import `op_pipeline` 的公开符号，不得 import `op_pipeline.engine` 内部实现 |
| 入口限制 | 模块插件受 Python import 幂等性限制：已导入过的模块不会重新执行；同一算子库不要同时以文件和模块两种方式加载进同一注册表（裸 `@op` 会撞重名，需 `replace=True`） |

### 3.3 独立注册表（隔离运行）

适合多流水线算子集隔离、测试隔离、同进程并发跑不同算子集的场景：

```python
from op_pipeline import Registry, load_plugins, run

reg = Registry()                                        # 全新注册表
load_plugins(paths=["plugins/my_ops.py"], registry=reg)  # 插件进 reg，内置算子自动补齐
report, values = run("my_pipeline.toml", registry=reg)   # default_registry 全程不受影响
```

要点（[registry.py#L106-L143](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/registry.py#L106-L143)）：

- 裸 `@op("x.y")` 写法的插件文件也能定向：`load_plugins(..., registry=...)` 加载期间
  `@op` 落到目标表（contextvar 实现，线程/任务隔离）；
- 内置算子自动补齐到新表（按 `op_pipeline.ops` 模块前缀识别，已存在的名字不覆盖）；
- 同一插件文件可在多个注册表各执行一次（模块级状态互不影响）；同一注册表重复加载则跳过；
- `Registry` 内部由 `RLock` 保护：多线程并发注册/并发 `load_plugins` 同一注册表是安全的，
  同一插件文件并发加载恰好执行一次，**加载失败不占去重名额**（占位回滚，下次可重试）；
- 类定义处可直接写 `@op("x.y", registry=reg)`；
- `modules` 方式受 Python import 幂等性限制：已导入过的模块不会重新执行。

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

### 4.2 执行器

引擎已抽象出 `Executor` 协议（[executor.py](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/executor.py)，M2-1），
默认 `ThreadExecutor` 平移原线程池行为：

```python
class Executor(Protocol):
    def submit(self, fn, /, *args) -> Future
    def wait(self, futures, *, first_completed: bool = True) -> set[Future]
    def shutdown(self) -> None
```

实现进程池、队列或集群执行器时需要注意：

1. `_run_node` 依赖 `Engine.store`（HandleStore）快照与 `NodeContext`，跨进程时句柄必须可序列化或改为引用传递；
2. `NodeContext` 需要能序列化（logger 不可序列化，要换成日志转发）；
3. 超时策略从"线程 join"改为"进程终止/任务取消"（内置 `ProcessExecutor` 已按此实现，见下）；
4. `_publish` 的锁需要换成协调层（队列/共享存储）；
5. `wait(first_completed=True)` 的"首个完成即返回"语义被主循环依赖，自定义实现必须保持。

内置 `ProcessExecutor`（M2-2）即按上述约束实现：每任务一个 `spawn` 子进程，
载荷（算子实例 + 运行数据 + 输入）先 `pickle.dumps`——该序列化结果直接传给
子进程，不做二次序列化；不可 pickle 时回退线程执行（软超时）并经
`on_fallback` 回调记入 `report.warnings`；`submit(..., timeout=)` 携带节点
超时，`wait` 内到点 `terminate()` 子进程——**硬超时**。刻意不用
`ProcessPoolExecutor`：它无法强杀单个任务，terminate worker 会把整个池打成
`BrokenProcessPool`、殃及同时在跑的其它节点。跨进程载荷要求算子类、参数、
输入全部可 pickle 且子进程能按模块名 import 其定义（插件文件里的算子
需要注意子进程的可见性）。

### 4.3 报告

`RunReport` 是纯数据类，`to_dict()` / `to_json()` 可直接用于外部系统。
扩展方向是加字段而不是改结构，避免破坏现有调用方（例如 `status`、`nodes[].attempts` 已被 README 与测试依赖）。

---

## 5. 已知技术债与实现缺口

下表是**代码已有但未接入**或**语义有缺口**的地方。这些不是文档推测，均可在源码中验证。

| # | 缺口 | 证据 | 影响 | 建议落点 |
| --- | --- | --- | --- | --- |
| T7 | 缓存不感知代码变更 | key 仅含 `version` 字符串 | 忘记递增 version 会命中旧结果 | 可选：把算子源码哈希或 git rev 纳入 key |
| T13 | `strict=False` 可返回残缺 Plan | [spec.py#L477-L487](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/spec.py#L477-L487) | 无调用方使用，属预留 | 保持现状并在文档说明 |
| T15 | 表算子假设首行代表全表列 | 多处使用 `rows[0].keys()` | 列不全的行会丢字段或抛错 | 统一列集合并集，或要求 schema 显式声明 |

> 剩余按需处理；T13 保持"预留，不处理"。
> 已消除：T1/T5（M1-1，软类型告警接入）、T6（M1-2，缓存目录相对 workdir 解析）、
> T9（M1-3，`NodeResult.traceback` 截断保留堆栈，重试耗尽路径 `from last_error` 链根因）、
> T10（M1-4，`run_id` 追加 4 位随机十六进制后缀，同秒并发产物目录隔离）、
> T4（M1-5，`defaults` 在 `NodeSpec.from_dict` 合并为节点策略缺省，显式值优先）、
> T2/T3/T14（M1-6，删除 `PlanNode.resolved_inputs` 残留字段、删除 `ModuleExecutionError`
> 及其导出、清理 `to_mermaid` 死变量——属破坏性删除但无内部调用方）、
> T11（M1-7，文件插件模块名取路径 blake2b 摘要，同名不同目录都能加载，同文件幂等）、
> T12（M1-8，`run()` 新增 `plugin_modules` 参数，支持模块路径插件）、
> **T8（M2-2，`ProcessExecutor` 硬超时——节点 `executor="process"` 或运行级
> `--executor process` 时，`timeout` 到点 terminate 子进程；线程模式仍为软超时）**。

---

## 6. 演进路线与落点

对应 README §9 的六项限制：

| 演进方向 | 具体设计 | 落点 |
| --- | --- | --- |
| 硬超时 | **已实现（M2-2）**：`ProcessExecutor` 每任务一子进程，`timeout` 到点 `terminate()`；结果/异常经管道序列化回传；不可 pickle 回退线程 | [executor.py](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/executor.py)；`Engine._execute_in_process` |
| asyncio 调度 | 算子支持 `async def run`（已有）；调度器改为任务集合 + `asyncio.wait(FIRST_COMPLETED)`；线程池仅用于同步算子 | `Engine.run/_dispatch` |
| 持久化元数据 | SQLite 表 `runs / nodes`，记录 status/attempts/error/时间；据此实现断点续跑 | 新 `store.py`；`Engine.run` 汇总处 |
| 可选 schema | 端口支持 `dataclass`/`pydantic` 类型；在 `_prepare_inputs` 前做深校验 | `ports.value_matches` + `Operator._prepare_inputs` |
| 分布式 | 抽出 `Executor` 协议；句柄改引用（对象存储 key / Arrow 表） | `engine.py` + `context.py` |
| 大对象 | 句柄表存引用而非值；`RefResolver` 解析时按需加载 | `Engine._publish` / `RefResolver` |

这些演进有一个共同前置条件：**句柄访问必须收敛到少数几个函数**。
M2-3 起收敛点已落在 `HandleStore` 接口（[handlestore.py](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/handlestore.py)）：
写 = `Engine._publish` → `store.put`，读 = `RefResolver._lookup` → `store.snapshot()` 后查表
（引擎经 `Engine(handle_store=...)` 注入自定义实现即可）。
不要在算子内部直接读写句柄表。

---

## 7. 新增能力时的回归清单

1. 跑 `python -m unittest discover -s tests -v`（87 个用例）；
2. 用 `examples/pipeline_orders.json` 做端到端（覆盖并行 fan-out、when、断言、4 种输出）；
3. 用 `examples/pipeline_text.toml --plugin plugins/demo_ops.py`（`--plugin` 相对配置文件所在目录解析）验证插件与重试；
4. `python -m op_pipeline validate` 与 `graph` 必须仍然可用；
5. 若改变节点契约（字段/语义），同步更新 [配置规范](../reference/config-schema.md) 与
   [运行时语义](../architecture/runtime-semantics.md)。
