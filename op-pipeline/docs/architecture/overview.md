# 架构设计总览

## 1. 项目定位

`op_pipeline` 是一个**算子级封装、配置驱动**的 Python 流水线框架，形态是**可嵌入的库**而不是平台：

| 维度 | 本项目 | Airflow / Prefect / Dagster |
| --- | --- | --- |
| 交付形态 | Python 包，`run_pipeline()` 一行调用 | 常驻服务 + 调度器 + Web UI |
| 依赖 | 纯标准库（Python ≥ 3.11），YAML 需可选 PyYAML | 较重的外部依赖与基础设施 |
| 执行单元 | 算子（`Operator` 子类），自描述契约 | Task / Op / Asset，通常依附平台元数据 |
| 状态与调度 | 进程内线程池，无持久化元数据 | 中心化调度、元数据库、重试策略管理 |
| 适用场景 | 脚本级 ETL、数据处理、报告生成的长期维护 | 跨团队、跨集群的编排 |

核心命题只有一句：**算子是一个自描述的可复用单元，流水线只是算子之间的一份配置。**

## 2. 分层架构

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ 配置层  (spec.py)                                                           │
│   JSON / TOML / YAML  →  PipelineSpec  →  NodeSpec  →  build_plan() → Plan  │
│   职责：配置解析、引用语法、端口/参数静态校验、句柄归属、依赖推导、拓扑排序 │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │ Plan（已校验的可执行图）
┌─────────────────────────────────────────────────────────────────────────────┐
│ 编排层  (engine.py + executor.py + handlestore.py + cache.py + context.py)      │
│   职责：依赖就绪调度、线程池并行、重试与退避、软超时、失败传播、              │
│         句柄发布、内容指纹缓存、运行报告聚合、运行上下文与产物目录           │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │ 已解析的输入 + 参数
┌─────────────────────────────────────────────────────────────────────────────┐
│ 契约层  (operator.py / ports.py / params.py / registry.py / expression.py)  │
│   职责：算子基类与执行契约、端口与参数声明及校验、注册表与插件发现、         │
│         安全表达式求值（when / 行级表达式）                                  │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │ @op 注册
┌─────────────────────────────────────────────────────────────────────────────┐
│ 算子层  (ops/) + 外部插件                                                    │
│   io / table / stats / text / report / control 六组，共 34 个内置算子        │
│   业务算子以插件形式平级加入，不与框架耦合                                   │
└─────────────────────────────────────────────────────────────────────────────┘
```

依赖方向是单向的：`spec → engine → operator/registry → ports/params/expression`。
算子只依赖 `Operator`、`Param`、`op` 等公开符号，永远看不到 `Plan` 和 `Engine`，
这是"算子可被单独测试、可被任意流水线复用"的结构性保证。

## 3. 模块职责矩阵

| 模块 | 主要符号 | 职责 | 不负责 |
| --- | --- | --- | --- |
| [spec.py](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/spec.py) | `NodeSpec` `PipelineSpec` `RefResolver` `PlanNode` `Plan` `build_plan` `topological_order` | 配置解析、引用解析、静态校验、依赖推导与拓扑排序 | 不做实际执行、不加载算子实现 |
| [engine.py](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/engine.py) | `Engine` `NodeResult` `RunReport` `run_pipeline` | 调度、并行、重试、超时、失败传播、缓存接入、报告 | 不解析配置、不做参数声明校验 |
| [handlestore.py](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/handlestore.py) | `HandleStore` `InMemoryHandleStore` | 句柄存取抽象（get/put/snapshot/items）与线程安全内存实现（M2-3） | 不做引用解析（RefResolver 的事）、不感知节点语义 |
| [executor.py](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/executor.py) | `Executor` `ThreadExecutor` `ProcessExecutor` | 执行器抽象（`submit`/`wait`/`shutdown` 协议）、线程池默认实现与硬超时进程执行器（M2-2） | 不做调度决策、不做重试/缓存（仍属 engine） |
| [operator.py](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/operator.py) | `Operator` `normalize_operator_class` | 算子声明归一化、输入/输出运行时校验、异步支持 | 不关心 DAG 与调度 |
| [ports.py](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/ports.py) | `Port` `as_ports` `value_matches` `port_warnings` | 端口声明归一化、软类型检查 | 不阻断执行（软校验） |
| [params.py](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/params.py) | `Param` `as_params` `coerce` `validate_params` | 参数声明、类型归一、范围/枚举校验 | 不做深层结构校验 |
| [registry.py](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/registry.py) | `Registry` `default_registry` `op` `load_plugins` | 名字 → 算子类映射、别名、插件导入 | 不管理算子版本仓库 |
| [expression.py](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/expression.py) | `evaluate` `evaluate_condition` | AST 白名单安全求值 | 不支持语句、属性访问、任意函数 |
| [cache.py](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/cache.py) | `fingerprint` `CacheStore` | 内容指纹与 pickle 文件缓存 | 不做跨机缓存、不做缓存淘汰 |
| [context.py](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/context.py) | `RunContext` `NodeContext` | 运行 ID、日志、工作目录、产物路径、参数注入 | 不持有句柄表（句柄在 `Engine`） |
| [graph.py](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/graph.py) | `to_mermaid` `to_dot` `to_ascii` | 依赖图 / 执行计划渲染 | 不做布局算法 |
| [cli.py](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/cli.py) | `main` `build_parser` | 命令行入口与退出码 | 不提供业务算子 |
| [ops/](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/ops/__init__.py) | 34 个 `@op` 类 | 内置算子库，导入即注册 | 不依赖引擎内部实现 |

## 4. 核心抽象与不变量

### 4.1 句柄（handle）是唯一的数据交换介质

- 每个输出端口默认映射到句柄 `"<node_id>.<port>"`，可在节点 `outputs` 中改名。
- 引擎在节点成功后把输出写入句柄存储 `Engine.store`（`HandleStore.put`，M2-3 起可注入自定义实现）。
- 下游只能通过 `${...}` 引用取数，**算子之间没有直接调用**，因此任意两个算子之间都可以替换、旁路或并行。

不变量：一个句柄在全图中只能有一个生产者。冲突在编译期由 `handle_owner` 检查并报错。

### 4.2 计划（Plan）是静态校验与动态执行的唯一交界

`build_plan()` 产出的 `Plan` 包含：节点列表、拓扑序、句柄归属表、流水线输出映射和合并后的参数。
引擎只消费 `Plan`，不再回看原始配置文本。好处是配置错误全部在"编译期"暴露，
坏处是运行期不再做深校验（见 §5 的取舍）。

### 4.3 声明即契约

| 声明 | 校验时机 | 失败后果 |
| --- | --- | --- |
| 算子存在（`op`） | 编译期 | `SpecValidationError` 汇总报错 |
| 参数名与参数值 | 编译期（无引用时）/ 运行期（含 `${}` 引用时） | `ParamValidationError` |
| 输入端口名与必填性 | 编译期 | `SpecValidationError` |
| 输出端口名与必填性 | 运行期（`_normalize_output`） | `PipelineError`，节点失败 |
| 端口类型 | 仅软校验（`value_matches`），当前**未接入调度流程** | 告警函数存在但未被调用 |

### 4.4 运行上下文是副作用出口

算子不允许自己拼全局路径：文件读取走 `ctx.resolve_path()`，产物走 `ctx.artifact_path()` /
`ctx.save_artifact()`，日志走 `ctx.log()`。这一层把"可测试性"和"未来可替换执行环境"同时保住了。

## 5. 关键设计决策

| # | 决策 | 理由 | 明确的代价 |
| --- | --- | --- | --- |
| D1 | 算子间只通过句柄交换数据 | 解耦与可替换性是框架的第一价值 | 中间结果常驻内存，大对象会吃内存 |
| D2 | 配置即 DAG，先 `build_plan` 再执行 | 配置错误一次性汇总，避免"跑到一半才发现" | 计划编译期与实际执行参数可能不同（含引用时延迟到运行期） |
| D3 | 声明式契约 + 软类型校验 | 数据形态变化快，硬 schema 会频繁改代码 | 类型错误要到算子内部才炸，定位成本高 |
| D4 | 表达式用 AST 白名单而非 `eval` | 配置可由非受信人员编写，必须防注入 | 只支持子集语法，复杂逻辑仍需写算子 |
| D5 | 同步算子 + `ThreadPoolExecutor` | 实现简单、可嵌入、GIL 之外的文件/网络 IO 仍能并行 | CPU 密集无收益；线程无法强杀（软超时） |
| D6 | 内容指纹缓存（pickle 文件） | 零依赖、天然按上下游自动失效 | 仅单机、无淘汰策略、pickle 只可读可信目录 |
| D7 | 零第三方依赖 | 便于嵌入到任意项目与受限环境 | YAML 支持是可选依赖；无 DataFrame 原生支持 |
| D8 | 失败只向下游传播，独立分支继续 | 配置化流水线的默认预期行为 | 需要 `fail_fast` 才能快速止损 |
| D9 | 报告是返回值 + 可选 JSON，不做持久化 | 保持"库"的形态 | 无断点续跑、无历史查询能力 |

## 6. 一次运行的对象协作

```
调用方
  │  PipelineSpec.from_file()          配置 → 规格
  │  build_plan(spec, registry)        规格 → Plan（含校验/依赖/拓扑序）
  │  RunContext.create(...)            运行上下文 + 产物目录
  │  CacheStore(dir)                   缓存开关
  ▼
Engine.run()
  ├─ 调度循环：dispatch → wait(FIRST_COMPLETED) → collect
  ├─ 每节点：解析引用 → 实例化/复用算子 → 缓存查询 → 执行(重试/超时) → 发布句柄
  └─ 汇总：RunReport（状态、耗时、尝试次数、缓存标记、告警）
  ▼
(RunReport, handles)
```

更细的调度与状态语义见 [运行时语义设计](runtime-semantics.md)。

## 7. 与 README 的关系

根目录 [README.md](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/README.md) 是**使用者手册**（快速开始、配置速查、算子概览）；
本目录是**设计与实现说明**，额外覆盖：

- 每个校验与执行语义对应的源码位置；
- 实现中已存在但未接入主流程的能力（详见 [扩展设计 §5](../design/extension-points.md#5-已知技术债与实现缺口)）；
- 修改框架时需要同步维护的不变量。
