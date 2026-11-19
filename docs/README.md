# op_pipeline 设计文档

本目录是从**代码实现**反向整理出的设计文档集合，描述 `op-pipeline`（包名 `op_pipeline`）的架构、
运行时语义、模块契约、扩展方式与配置规范。文档内容以当前源码为准，凡是与 README 描述不一致之处，
以本文档标注的"实现现状"为准。

## 文档索引

| 文档 | 类型 | 面向读者 | 内容概要 |
| --- | --- | --- | --- |
| [architecture/overview.md](architecture/overview.md) | 架构设计 | 框架维护者、接入方 | 定位、分层、模块职责矩阵、核心抽象、关键设计决策（含取舍与代价） |
| [architecture/runtime-semantics.md](architecture/runtime-semantics.md) | 运行时设计 | 框架维护者、排障者 | 两阶段执行模型、编译期校验、调度算法、失败传播、重试、软超时、缓存、报告聚合 |
| [architecture/roadmap.md](architecture/roadmap.md) | 演进规划 | 维护者、决策者 | 能力边界与场景适配评估（强适配/补插件可用/不适合）、演进原则、三阶段路线、不建议方向 |
| [design/core-modules.md](design/core-modules.md) | 详细设计 | 贡献者 | 逐模块的职责、关键类型与函数、校验规则、实现细节与坑位 |
| [design/extension-points.md](design/extension-points.md) | 扩展设计 | 算子作者、二次开发者 | 自定义算子、插件加载、替换执行器/缓存、演进路线落点、已知技术债 |
| [design/development-plan.md](design/development-plan.md) | 开发计划 | 贡献者、维护者 | 里程碑（M1 正确性 / M1.5 库通用性 / M2 三抽象 / M3 生态）任务分解：改动落点、验收标准、技术债映射、DoD 与工程约定 |
| [reference/config-schema.md](reference/config-schema.md) | 参考手册 | 流水线配置编写者 | 顶层/节点字段、引用语法、表达式子集、参数归一规则、CLI 与退出码 |
| [reference/operator-catalog.md](reference/operator-catalog.md) | 参考手册 | 流水线配置编写者 | 59 个内置算子的输入/输出/参数契约与行为要点 |
| [reference/api.md](reference/api.md) | 参考手册 | 二次开发者、维护者 | 包顶层 `__all__` 公开 API、分组语义与稳定性承诺 |
| [guides/quickstart.md](guides/quickstart.md) | 使用指南 | 首次使用者 | 安装、第一个流水线、Python 内嵌、自定义算子、常见错误 |
| [guides/operations.md](guides/operations.md) | 使用指南 | 运维/接入方 | scheduler_app 调度与告警、Web UI、RunStore、远程 worker、故障排查 |

## 阅读路径建议

- **只想写流水线**：先读根目录 README 的快速开始，再查 [配置规范](reference/config-schema.md) 与 [算子清单](reference/operator-catalog.md)。
- **要写新算子**：读 [扩展设计](design/extension-points.md) + [核心模块设计](design/core-modules.md) 中 `operator.py / params.py / ports.py` 三节。
- **要改引擎或排障**：读 [运行时语义](architecture/runtime-semantics.md)，其中标注了每条语义对应的源码位置。
- **要评估替换/演进**：读 [架构总览](architecture/overview.md) 的关键决策表 + [演进路线](architecture/roadmap.md) 的场景评估，动手时按 [开发计划](design/development-plan.md) 的任务编号执行。

## 代码位置约定

本目录与包源码同处一个仓库根：包源码位于 `op_pipeline/`，示例与插件位于 `examples/`，
自测位于 `tests/test_op_pipeline.py`。文档中的源码链接统一使用 `file:///` 绝对路径，可直接跳转。

示例一览：

- `examples/pipeline_orders.json` / `.toml`：CLI 模式（并行 fan-out、when、断言、4 种输出）；
- `examples/pipeline_text.toml` + `plugins/demo_ops.py`：插件加载与重试（`--plugin` 相对配置文件目录解析）；
- `examples/embedded_lib_demo.py`：**库嵌入模式**（独立注册表 + 取消事件 + 纯内存运行三合一，
  直接 `python examples/embedded_lib_demo.py` 运行，无需安装）。

## 文档维护原则

1. 文档描述的是**实现**而非愿景；新增能力时同步更新对应章节。
2. "设计决策"必须写出**代价**，否则无法判断该决策是否仍然成立。
3. 涉及语义的结论尽量附源码行号链接，避免文档与代码漂移。
