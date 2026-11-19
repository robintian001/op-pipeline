# 开发计划

> 本文是 [演进路线](../architecture/roadmap.md) 的任务级分解：
> 每个任务给出**改动落点**与**可验证的验收标准**，按里程碑组织。
> 任务编号规则：`M<里程碑>-<序号>`。技术债编号（T*）见
> [扩展设计 §5](extension-points.md#5-已知技术债与实现缺口)，映射关系见本文 §8。

---

## 1. 计划约定

| 约定项 | 内容 |
| --- | --- |
| 版本节奏 | M1 → `0.2.0`；M1.5 各任务独立发 `0.2.x` 小版本（一任务一 PR 一发布，小步快跑）；M2 → `0.3`；M3 → `0.4`；M4 → `0.5`；M5 → `0.6`；M6 → `0.7` |
| 合入门槛 | `python -m unittest discover -s tests -v` 全绿 + 新任务自带测试 + 两个示例流水线端到端通过 |
| 回归基线 | 每个任务合入前先跑基线；M2 的抽象类任务要求"平移后行为零变化"（现有 21 用例不改断言） |
| 文档同步 | 改动涉及节点字段/执行语义/CLI 的，同一变更内更新 [运行时语义](../architecture/runtime-semantics.md) 与 [配置规范](../reference/config-schema.md)；新增算子更新 [算子清单](../reference/operator-catalog.md) |
| 不兼容变更 | 报告 schema / 配置 schema 的破坏性改动必须升版本并在 changelog 标注；M1 阶段尽量只做加法 |
| 任务内不做的事 | 见 [roadmap §7](../architecture/roadmap.md#7-明确不建议的方向)，评审时据此否决范围蔓延 |

---

## 2. M1：正确性与承诺补齐（v0.2.0）

目标：实现与文档一致，消除隐性正确性问题。全部为小改动，可按任意顺序执行，
建议按下列顺序（先修"承诺未兑现"，再做清理）。

| 编号 | 任务 | 改动落点 | 验收标准 |
| --- | --- | --- | --- |
| M1-1 | 接入端口软类型告警：引擎在 `run()` 汇总阶段调用 `check_handle_types(plan, self.handles)`，结果写入 `report.warnings`；`Engine._warnings` 同步启用 | [engine.py](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/engine.py) `run()`、[spec.py](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/spec.py) `check_handle_types` | 新增单测：声明 `int` 实际给 `str` 的两节点流水线，报告 `warnings` 含告警且状态仍为 `success`；消除 T1/T5 |
| M1-2 | 缓存目录按 `workdir` 解析：CLI `--cache-dir` 与 `run(cache_dir=...)` 的相对路径基准从进程 CWD 改为工作目录 | [cli.py](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/cli.py) `cmd_run`、[__init__.py](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/__init__.py) `run()` | 单测：同一 workdir、不同进程 CWD 下两次运行命中同一缓存（第二次 `cached=true`）；更新 [配置规范 §8](../reference/config-schema.md) |
| M1-3 | 失败保留堆栈：`NodeResult` 增加 `traceback: str = ""` 字段（截断至约 2000 字符），`_run_node` 的 except 分支用 `traceback.format_exc()` 填充；`to_dict` 带出 | [engine.py](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/engine.py) `NodeResult` / `_run_node` | 单测：失败节点的 JSON 报告含 `traceback` 首行；`summary()` 不变（避免刷屏）；消除 T9 |
| M1-4 | `run_id` 防碰撞：秒级时间戳后追加 4 位随机十六进制后缀（如 `20261117-093012-a3f1`） | [context.py](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/context.py) `RunContext.create` | 单测：同一秒内两次 `create` 得到不同 `artifacts_dir`；消除 T10 |
| M1-5 | `defaults` 生效：`build_plan` 将 `spec.defaults` 中的 `retries/retry_delay/timeout/cache/allow_failure` 作为节点未显式设置时的缺省值（合并发生在 `NodeSpec.from_dict` 或 `build_plan`，节点显式值优先） | [spec.py](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/spec.py) | 单测：`defaults = {"retries": 2}` 时未设置节点继承、显式 `retries=0` 不继承；更新 [配置规范 §1](../reference/config-schema.md)；消除 T4 |
| M1-6 | 死代码清理：删除 `PlanNode.resolved_inputs`（恒为 `{}`）；**删除** `ModuleExecutionError` 及其导出（引擎实际不抛、也无调用方，保留会误导排障）；删除 `to_mermaid` 中只写不读的 `style` 变量 | [spec.py](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/spec.py)、[errors.py](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/errors.py)、[__init__.py](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/__init__.py)、[graph.py](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/graph.py) | 全部测试通过且 `__all__` 同步收缩；在 changelog 标注移除项（属破坏性删除，但无内部调用方）；消除 T2/T3/T14 |
| M1-7 | 插件按路径去重：`load_plugins` 的文件插件模块名改为 `"_op_plugin_" + blake2b(路径)` 摘要，消除同名不同目录被静默跳过 | [registry.py](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/registry.py) `load_plugins` | 单测：两个不同目录的同名 `my_ops.py` 均被加载（注册表含两者的算子）；消除 T11 |
| M1-8 | `run()` 支持模块插件：`run(..., plugin_modules: tuple[str, ...] = ())`，转发给 `load_plugins(modules=...)` | [__init__.py](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/__init__.py) `run()` | 单测：`run(cfg, plugin_modules=("examples.plugins.demo_ops",))` 可解析 `demo.*` 算子；消除 T12 |
| M1-9 | CLI 冒烟与文档收口：`--dry-run` 提前到创建 `RunContext` 之前（避免无谓建产物目录）；`op_pipeline ops --json` 输出与新字段核对 | [cli.py](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/cli.py) `cmd_run` | `--dry-run` 后无 `artifacts_dir` 产生；两个示例 + validate/graph 回归通过 |

> **状态**：M1-1 ~ M1-9 已全部完成，M1 里程碑关闭（2026-11-17，TDD 红→绿各一轮，66 个用例全绿）。
> M1-1：软类型告警接入（T1/T5 出债）。M1-2：相对 `cache_dir` 以 workdir 为基准，
> `CacheStore` 新增 `base_dir`（T6 出债）。M1-3：`NodeResult.traceback` 截断保留堆栈
> （约 2000 字符、保留末尾），重试耗尽路径补 `from last_error` 链根因（T9 出债）。
> M1-4：`run_id` 追加 4 位随机十六进制后缀，同秒并发产物目录隔离（T10 出债）。
> M1-5：`defaults` 在 `NodeSpec.from_dict` 合并为节点策略缺省（显式值 > defaults >
> 框架默认，未知策略键报错；T4 出债）。
> **M1-6（变更记录）**：破坏性删除——`PlanNode.resolved_inputs` 残留字段、
> `ModuleExecutionError` 异常类及其 `__all__` 导出、`to_mermaid` 死变量（T2/T3/T14 出债；
> 均无内部调用方，删除不会影响现有配置）。
> M1-7：默认注册表的文件插件模块名取路径 blake2b 摘要——不同目录的同名文件都能加载，
> 同一文件重复加载保持幂等（T11 出债）。
> M1-8：`run()` 新增 `plugin_modules` 参数（转发 `load_plugins(modules=...)`），
> 库调用方不再需要为模块插件自行 `load_plugins`（T12 出债）。
> M1-9：CLI `--dry-run` 提前到 `RunContext`/`CacheStore` 创建之前（不建产物/缓存目录）；
> `ops --json` 输出契约有测试核对（name/description/inputs/outputs 必含）。
>
> **M1 关闭（v0.3.0，2026-11-17）**：66 个用例全绿；含破坏性变更（删除
> `ModuleExecutionError` 导出、相对 `cache_dir` 改按 workdir 解析），minor 升位。
> 剩余技术债：T7（缓存不感知代码变更）、T8（软超时无法终止算子）、T13（strict=False
> 预留）、T15（表算子首行列假设）。下一站 M2（三个抽象）。

**M1 完成定义（DoD）**：测试全绿；`examples/pipeline_orders.json` 与
`pipeline_text.toml --plugin ...` 端到端通过；`extension-points.md` §5 的 T1–T6、T9–T12、T14 标记为已解决（T13 保持"预留，不处理"，T15 移交 M3）。

---

## 3. M1.5：库通用性补齐（v0.2.x）

> 背景：项目的差异化定位是"**可嵌入库**"（[roadmap §1](../architecture/roadmap.md)）。
> 本里程碑专门消除"嵌入到任意宿主（服务、测试、async 宿主、多流水线并发进程）时的惊讶行为"。
> 任务全部小粒度、相互独立，**每个任务可单独发一个 `0.2.x` 小版本**（小步快跑），可与 M1 交错执行。

| 编号 | 任务 | 消除的通用性缺口 | 改动落点 | 验收标准 |
| --- | --- | --- | --- | --- |
| M1.5-1 | 独立注册表收尾：`load_plugins(..., registry=None)` 注入；`run(..., registry=None)`（当前写死 `default_registry`）；文档化既有的 `@op(registry=...)` 用法 | `@op` 已支持注入但插件加载/快捷入口不支持：全局单例导致多流水线共享一张表、测试互相污染（现有测试被迫用 `replace=True` 即此症状） | [registry.py](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/registry.py) `load_plugins`、[__init__.py](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/__init__.py) `run()` | 单测：两个独立 `Registry` 经 `run(..., registry=...)` 各自跑通且算子集隔离；不传参行为完全向后兼容 |
| M1.5-2 | 打包完备：新增 `op_pipeline/py.typed`；补 `license`/`classifiers`/`[tool.setuptools.package-data]` | 外部消费者拿到的是"盲类型"（无 py.typed，mypy/pyright 忽略全部内联标注）；PyPI 元数据不完整 | [pyproject.toml](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/pyproject.toml)、新文件 `op_pipeline/py.typed` | `pip install -e .` 后第三方项目类型检查无"缺 py.typed"提示；构建 wheel 含 py.typed |
| M1.5-3 | 无惊讶副作用：`artifacts_dir` 惰性创建（首次 `artifact_path` 才 mkdir）；`RunContext.create` 增加位置覆盖参数 | 嵌入方只想做纯内存计算，workdir 却被强制留下 `.op_pipeline/`（`create` 当前无条件 mkdir） | [context.py](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/context.py) `RunContext.create` | 单测：纯内存流水线跑完后 workdir 无 `.op_pipeline/`；用到产物的运行写盘行为不变 |
| M1.5-4 | 未知配置键告警：`PipelineSpec.from_dict` / `NodeSpec.from_dict` 对未知键发 `logging.warning` | `[nodes]` 拼错成 `[Nodes]` 会得到"0 节点运行成功"，笔误静默通过 | [spec.py](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/spec.py) | 单测：assertLogs 捕获未知键告警，正确配置零告警；`validate` 命令展示告警 |
| M1.5-5 | `when` 参数一致性：`env["params"]` 改为解析后的值，原值保留为 `env["raw_params"]` | 当前 `params` 是原始配置串（可能是 `${p.x}`），与平铺名（解析值）混排在同一环境，配置作者易踩坑（运行时语义已标注该坑） | [engine.py](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/engine.py) `_eval_when` + [运行时语义](../architecture/runtime-semantics.md) | 单测：`when = "params.n > 0"` 与 `when = "n > 0"` 均按预期命中；现有 when 用例通过；文档同步 |
| M1.5-6 | 表达式函数扩展钩子：公开 `expression.register_function(name, fn)`（约束：纯函数、禁止覆盖内置名） | 白名单仅 14 个内建函数；配置作者无法使用领域函数（日期解析、正则辅助），复杂条件被迫写算子，"配置驱动"价值打折 | [expression.py](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/expression.py) | 单测：注册自定义函数后 `when` 可用；覆盖内置名被拒绝；AST 拦截规则不变 |
| M1.5-7 | 取消机制：`Engine.cancel(reason)` + `run_pipeline(..., cancel_event: threading.Event)`；取消后待调度节点标记 skipped、在跑节点自然结束、报告照常产出 | 运行一旦启动无法中断——嵌入服务后请求取消/超时，流水线仍跑完浪费资源；Ctrl+C 同样没有干净出口 | [engine.py](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/engine.py) `_dispatch/collect` | 单测：并行流水线运行中 set 事件，未调度节点 skipped（note 含原因）、报告 status=partial；不 set 行为不变 |
| M1.5-8 | 注册表线程安全：`Registry.register`、`load_plugins` 加 `RLock` | 单进程多线程并发调用 `run()`（内部 load_plugins）存在 dict 写竞态 | [registry.py](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/registry.py) | 单测：N 线程并发注册/加载无异常、注册数量正确 |

**M1.5 完成定义（DoD）**：各任务独立合入、各自可发版；收尾时在 `examples/` 新增
"库嵌入模式"综合示例（独立注册表 + 取消事件 + 纯内存运行三合一）；现有测试与新增单测全绿。

> **状态**：**M1.5 全部完成并关闭（v0.2.1，2026-11-17）**——M1.5-1 ~ M1.5-8 交付 + DoD 综合示例
> `examples/embedded_lib_demo.py`（独立注册表 + 取消事件 + 纯内存三合一），47 个用例全绿。
> 各任务要点：注册表线程安全（并发加载同一文件恰好执行一次、失败占位可重试）、
> 未知配置键告警（含近义键提示，顺手修复 `to_ascii` 单节点崩溃）、
> `when` 环境统一为解析值并新增 `raw_params`（点语法开放映射下钻）、
> `register_function` 表达式扩展钩子（纯函数约束、禁覆盖内置名）、
> 收尾时修复空注册表被 `or` 短路进全局表的定向 bug（`Registry.__len__` 使空表 falsy），
> wheel 含 py.typed，两个示例端到端通过，文档已同步。

---

## 4. M2：三个抽象（v0.3）

目标：解锁硬超时、CPU 并行、断点续跑、可观测性。任务有依赖关系：M2-1 → M2-2，M2-5 → M2-6；M2-8 独立，可先行合入。

| 编号 | 任务 | 改动落点 | 验收标准 |
| --- | --- | --- | --- |
| M2-1 | 抽出 `Executor` 接口（`submit(node, payload) -> Future`、`wait(futures, first_completed) -> set`、`shutdown()`）；`ThreadExecutor` 平移现有 `ThreadPoolExecutor + wait(FIRST_COMPLETED)` 行为；`Engine` 只依赖接口 | 新增 [executor.py](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/executor.py)，改 [engine.py](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/engine.py) `_dispatch/collect` | **行为零变化**：现有 21 用例不改断言全绿；`test_parallel_speed` 仍 <0.8s |
| M2-2 | `ProcessExecutor`：`ProcessPoolExecutor` 实现；超时改为 terminate（**硬超时**）；payload（算子类+参数+输入）需可 pickle，不可 pickle 时回退线程执行并记录告警；节点级选择 `executor="process"` 或运行级 `--executor process` | [executor.py](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/executor.py)、[engine.py](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/engine.py) `_execute_once`、[spec.py](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/spec.py) `NodeSpec` | 单测①：CPU 密集 4 节点并行耗时显著低于串行；单测②：`timeout=0.5` 的死循环算子在进程模式下被终止且节点 `failed`（错误信息含"硬超时"）；消除 T8 |
| M2-3 | 抽出 `HandleStore` 接口（`get/put/snapshot/items`）；默认 `InMemoryHandleStore` 平移现有字典行为；`Engine._publish` 与 `RefResolver._lookup` 改为走该接口（保持两处收敛点不变） | 新增 handlestore 模块或并入 executor.py 命名空间，改 [engine.py](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/engine.py)、[spec.py](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/spec.py) `RefResolver` | 行为零变化（21 用例全绿）；`run_pipeline` 返回值仍为句柄值 dict |
| M2-4 | `SpillHandleStore`：值大小超过阈值（按 `fingerprint` 时的 repr 长度或专用估算）时 pickle 落盘（目录挂 `artifacts_dir`），句柄表存路径并在读取时按需加载 | handlestore 模块 | 单测：含两个大表（构造 >阈值）的流水线跑通且结果一致；可复跑（临时文件复用已有产物目录机制） |
| M2-5 | `RunStore` + SQLite：新模块定义 `record_run/record_node/query`；表结构 `runs(run_id, pipeline, version, status, started_at, finished_at, params_json)`、`nodes(run_id, node_id, op, status, attempts, cached, error, cache_key, finished_at)`；`Engine` 在 `_collect` 时写入；`--runs <db>` 开关（默认关闭，保持零副作用）；同时把算子源码摘要纳入 cache_key 的可选开关（缓解 T7） | 新增 [store.py](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/store.py)，改 [engine.py](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/engine.py)、[cli.py](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/cli.py) | 单测：跑完后 SQLite 可查询到与 `RunReport` 一致的节点状态/attempts；不传开关时不产生任何文件 |
| M2-6 | 断点续跑：`--resume <run_id>`——从 `RunStore` 读取上次成功节点的 `cache_key`，与本次编译后的 key 比对，一致则直接复用缓存/句柄并标记 `[resumed]` | [cli.py](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/cli.py)、[engine.py](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/engine.py) `_run_node` 缓存查询处 | 单测：三段流水线首跑到第二段失败；修数据后 `--resume` 只重跑失败段及下游，首段 `[resumed]`；依赖 M2-5 |
| M2-7 | 执行事件钩子：`Engine(hooks=[Hook])`，Hook 协议 `on_node_start(node_id, op)`、`on_node_finish(node_id, status, duration, cached)`、`on_run_finish(report)` | [engine.py](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/engine.py) `_run_node/_collect/run` | 单测：钩子按调度顺序收到事件；失败节点也触发 `on_node_finish`；文档给出进度条示例 |
| M2-8 | async 宿主嵌入：新增 `run_pipeline_async(...)`（以 `loop.run_in_executor` 包装同步引擎；缺口：`run_pipeline` 是同步阻塞函数，在 async 宿主内调用会卡死事件循环），文档说明它与全 asyncio 调度器的区别（后者不在本里程碑范围） | [__init__.py](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/__init__.py)、[engine.py](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/engine.py) | 单测：在运行中的事件循环内 `await run_pipeline_async(...)` 得到正确报告；流水线运行期间同循环的其它 asyncio 任务可推进（证明未阻塞） |

> **状态**：M2-1 已完成（2026-11-17，TDD 红→绿一轮，69 个用例全绿，**行为零变化**——
> 66 个既有用例含 `test_parallel_speed` 不改断言通过，两个示例端到端正常）。
> `Executor` 协议（`submit`/`wait(first_completed=)`/`shutdown`）与 `ThreadExecutor`
> 落在 [executor.py](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/executor.py)
> 并从包顶层导出；主循环改经 `pool.wait(running, first_completed=True)` 收集结果；
> [扩展设计 §4.2](extension-points.md#42-替换执行器) 已更新为稳定扩展点。
> 下一步 M2-2：`ProcessExecutor`（硬超时，消除 T8）。

> **状态**：M2-2 已完成（2026-11-17，TDD 红→绿一轮，82 个用例全绿；两示例 +
> orders `--executor process` 端到端通过）。`ProcessExecutor` 落在
> [executor.py](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/executor.py)
> 并从包顶层导出：每任务一个 `spawn` 子进程 + 管道回传，`submit` 先 `pickle.dumps`
> （序列化结果直传子进程，不做二次序列化），不可 pickle 回退线程执行并记
> `report.warnings` 告警；`wait` 内 deadline 到点 `terminate()`——**硬超时**，T8 出债。
> **有意偏离"ProcessPoolExecutor 实现"**：它无法强杀单个任务，terminate worker 会把
> 整池打成 `BrokenProcessPool` 殃及其它节点。节点字段 `executor` 进 `defaults` 策略键；
> `run(..., executor=)` 与 CLI `--executor {thread,process}` 提供运行级缺省
> （节点显式值优先）。CPU 并行验收改用"扇出 vs 串行链"对比（阈值 0.8，
> 计算量校准至压过子进程 spawn 成本，抗杀软串行化噪声）。
> 下一步 M2-3：HandleStore 接口。

> **状态**：M2-3 已完成（2026-11-17，TDD 红→绿，87 个用例全绿，**行为零变化**——
> 82 个既有用例不改断言通过，两示例端到端正常）。`HandleStore` 协议
> （`get`/`put`/`snapshot`/`items`）与 `InMemoryHandleStore`（RLock + dict）落在
> [handlestore.py](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/handlestore.py)
> 并从包顶层导出；写收敛在 `_publish → store.put`，读收敛在
> `RefResolver._lookup`（`as_mapping` 同时接受 store 与 Mapping，公共符号向后兼容）；
> `Engine(handle_store=...)` 支持注入自定义实现（`run_pipeline` 透传与 M2-4
> SpillHandleStore 一起收口）。TDD 顺带纠正了两处对 `RefResolver` 查表语义
> （`p.` 前缀整键路由、下钻仅在句柄上）的测试误写。
> 下一步 M2-4：SpillHandleStore（超阈值落盘）。

> **状态**：M2-4 已完成（2026-11-18，TDD 红→绿，97 个用例全绿——87 个既有用例
> 不改断言通过，两示例端到端正常）。`SpillHandleStore`（默认阈值 1MB，按
> `pickle.dumps` 后字节量判定）落在
> [handlestore.py](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/handlestore.py)：
> 超阈值值写盘为 `<句柄名>.<blake2b 8 字节摘要>.pkl`（防 Windows 大小写不敏感
> 文件系统名碰撞），表内只存 `_SpilledRef(path, size)` 占位；`snapshot()` 返回
> 惰性视图 `_LazySnapshot`（成员检查 / len / 迭代零 IO——引用解析高频走 `in` 与
> 迭代，仅按键访问读盘）。三层退化保证"落盘是内存优化、不承担正确性"：值不可
> pickle / 未指定落盘目录 → 回退内存并记 warning。目录默认挂 `artifacts_dir/spill`，
> 引擎在构造末尾经 `attach(context)` 钩子注入（普通实现无此钩子，零影响）；
> `run_pipeline(handle_store=...)` 与顶层 `run(handle_store=...)` 透传收口（M2-3 遗留）。
> 下一步 M2-5：RunStore + SQLite。

> **状态**：M2-5 已完成（2026-11-18，TDD 红→绿一轮，104 个用例全绿——97 个既有用例
> 不改断言通过，两示例端到端 + `--runs` 落库可查询正常）。`RunStore`（`record_run`
> /`record_node`/`query`，`runs`/`nodes` 两表惰性建表，连接 `close`/`with`）落在
> [store.py](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/store.py)
> 并从包顶层导出；`Engine(run_store=...)` 在 `_collect`（含缓存命中早退）与
> `_mark_skipped` 写入节点记录（nodes 表与 `RunReport` 节点列表一致，跳过节点也入
> 库），运行末尾写入 runs 记录；CLI 新增 `--runs <db>`（相对路径按工作目录解析），
> **默认关闭 = 零副作用**（不传开关不产生任何文件，`test_no_run_store_creates_no_db_file`
> 与 CLI 双跑验证）。**T7 出债**：`CacheStore(include_source=True)` 可选地把算子类
> 源码 blake2b 摘要纳入 cache_key（`compute_key` 新增可选 `source` 参数，默认关闭
> 保持旧行为，引擎 `_run_node` 经 `inspect.getsource` 取源码），`NodeResult` 新增
> `cache_key` 字段供 nodes 表记录与 M2-6 比对。
> 下一步 M2-6：断点续跑（`--resume`，依赖 M2-5 的 cache_key 记录）。

> **状态**：M2-6 已完成（2026-11-18，TDD 红→绿一轮，108 个用例全绿——104 个既有用例
> 不改断言通过，两示例端到端 + `--runs`+`--resume` 续跑验证 `[resumed]` 标记正常）。
> CLI 新增 `--resume <run_id>`（必须配合 `--runs`，否则 exit 2 提示）；引擎
> `Engine(resume_run_id=...)` 在构造时把参考 run 的 nodes 记录（node_id/status/cache_key）
> 一次预载进内存 `_resume_map`（**SQLite 连接有线程亲和性，不能在 worker 线程查询**，
> 这是首轮绿灯踩到的坑），`_run_node` 缓存命中处比对：参考记录为 success 且 cache_key
> 与本次一致 → 复用缓存并标记 `NodeResult.resumed`，报告/日志显示 `[resumed]`；参考段
> 失败或缓存缺失则照常重跑（失败段与下游不受影响）。不带 resume 的普通重跑只标记
> `[cache]` 不标记 `[resumed]`；未知 run_id 退化为普通运行（均有用例覆盖）。
> `RunStore.prev_node(run_id, node_id)` 作为公共 helper 一并提供（引擎用内存 map，不依赖它）。
> 下一步 M2-7：执行钩子。

> **状态**：M2-7 已完成（2026-11-18，TDD 红→绿一轮，113 个用例全绿——108 个既有用例
> 不改断言通过，两示例端到端正常）。执行事件钩子：`Engine(hooks=[Hook])` /
> `run_pipeline(hooks=[...])` / 顶层 `run(hooks=[...])`，`Hook` 协议（engine.py，
> 包顶层导出）含 `on_node_start(node_id, op)`（`_run_node` 开头）、
> `on_node_finish(node_id, status, duration, cached)`（`_run_node` finally，成功/
> 失败/缓存命中都触发；缓存命中提前 return 也经 finally 发 finish）、
> `on_run_finish(report)`（`run()` 末尾，总在最后）。被跳过的节点（when 为假 /
> 上游失败 / 取消）从不触发。钩子异常只记入报告告警、不影响流水线；节点并行时
> 钩子可能被并发调用（文档示例用锁）。进度条示例见
> [runtime-semantics §6.2](../architecture/runtime-semantics.md#62-执行事件钩子m2-7)。
> 下一步 M2-8（按计划表待办项续写）。

> **状态**：M2-8 已完成（2026-11-18，TDD 红→绿一轮，115 个用例全绿——113 个既有用例
> 不改断言通过，两示例端到端正常）。`run_pipeline_async(...)`（engine.py，包顶层导出）
> 以 `loop.run_in_executor(None, functools.partial(run_pipeline, ...))` 包装同步引擎，
> 参数与返回值与 `run_pipeline` 完全一致；async 宿主内 `await` 不阻塞事件循环
> （`AsyncHostTest`：运行中的事件循环内 await 得到正确报告；流水线运行期间同循环的
> ticker 任务持续推进 ≥3 次，证明未卡死循环）。取消语义不变（`asyncio.CancelledError`
> 只放弃 await，中途止损仍用 `cancel_event`）。这是"嵌入 async 宿主"的桥接，
> **不是全 asyncio 调度器**（引擎内 `asyncio.wait(FIRST_COMPLETED)` 不在本里程碑范围），
> 见 [runtime-semantics §6.3](../architecture/runtime-semantics.md#63-async-宿主嵌入m2-8)。
> M2 DoD 已全部满足（执行器/续跑两节 + async 宿主单测与文档示例已落地）。
> 下一步：进入下一里程碑（M3，按需启动）。

**M2 完成定义（DoD）**：全部现有测试不改断言通过；新增 Executor/HandleStore/RunStore 单测；
`--executor process` + `--resume` 各有一个端到端示例；`run_pipeline_async` 有 async 宿主单测与文档示例；[运行时语义](../architecture/runtime-semantics.md)
新增"执行器/续跑"两节。

---

## 5. M3：生态与规模化（v0.4+，按需启动）

目标：扩大 B 类场景覆盖面；全部走"算子/extras/外围"，不动内核语义。

| 编号 | 任务 | 改动落点 | 验收标准 |
| --- | --- | --- | --- |
| M3-1 | `http.request` 算子（GET/POST、headers、timeout、json/text 双视图），仅用标准库 `urllib` | 新增 `ops/http_ops.py` | 单测（本地 `http.server` 假服务）；示例流水线调用公开 API 一次通过；`deterministic=False`（外部状态） |
| M3-2 | `db.sqlite_query` / `db.sqlite_exec`（标准库 `sqlite3`） | 新增 `ops/db_ops.py` | 单测：内存库建表-查询-写回闭环；写操作算子 `deterministic=False` |
| M3-3 | DataFrame adapter：`df.from_table` / `df.to_table`（pandas 可选 extras，`pip install op-pipeline[dataframe]`）；同时治理表算子"首行代表全表列"假设（T15：显式 `columns` 参数或列集合并集策略，行为变更须在文档标注） | 新增 `ops/dataframe_ops.py`、[pyproject.toml](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/pyproject.toml) extras、`table_ops.py` | 单测：`list[dict]` ↔ DataFrame 往返一致；未装 pandas 时 import 不报错；消除 T15 |
| M3-4 | 插件入口点发现：`pyproject` 支持 `[project.entry-points."op_pipeline.ops"]`；`load_plugins(entry_points=True)` 扫描注册 | [registry.py](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/registry.py) | 单测：安装一个玩具插件包后 `op_pipeline ops` 列出其算子；文档新增插件包发布指南 |
| M3-5 | 配置版本化：`spec_version` 顶层字段（缺省 `"1"`）+ 迁移器框架（`MIGRATIONS: dict[version, callable]`） | [spec.py](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/spec.py) `from_dict` | 单测：旧配置在新版本下仍可解析；未知更高版本报清晰错误 |
| M3-6 | 观测性：基于 M2-7 钩子的结构化事件导出（JSON lines / 可选 OTel bridge） | 新模块 `observe.py`（extras `otel`） | 单测：一次运行导出的 JSONL 与报告一致 |
| M3-7 | 配置组合（第一步）：顶层 `includes`——合并 params/nodes/outputs，节点 id 重复即报错，环检测在合并后进行；节点组/嵌套流水线留作后续迭代（缺口：大型配置重复度高，外部 jinja 预渲染会绕过校验层） | [spec.py](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/spec.py) `from_dict` | 单测：includes 合并结果与手写大配置语义一致（节点数、依赖图相同）；文件缺失/循环 include 报错清晰 |
| M3-8 | 自定义配置格式：`PipelineSpec.register_loader(suffix, fn)` 注册表，内建 json/toml/yaml 改为注册项（缺口：`from_file` 目前是固定 if/elif 分发，第三方无法扩展 .ini 等格式） | [spec.py](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/spec.py) `from_file` | 单测：注册 `.fake` 后缀 loader 后正确分发；未知后缀报错并列出已注册格式 |

**M3 完成定义（DoD）**：`op_pipeline ops` 算子数 ≥ 40；每个 extras 有独立的依赖声明与 README 说明；
内核 import 路径保持零第三方依赖（extras 模块延迟导入）；`examples/` 新增 includes 组合示例配置。

> **M3 状态**：M3-1 已完成（HttpOpTest 6 用例绿；全量回归 121 tests OK；e2e 示例调用公开 API 一次通过）。
> M3-1 落地 `ops/http_ops.py`：`http.request` 仅用 `urllib`，GET/POST、headers/timeout、
> `json_body`/`body` 双请求体，json/text 双视图（Content-Type 含 json 或文本以 `{`/`[` 开头即给 json 视图），
> HTTPError 返回 status/headers/text 且 `ok=False`、URLError 抛 `RuntimeError` 走重试、非 GET/POST 抛 `ConfigError`，
> `deterministic=False`，算子数 34→35（[算子清单](../reference/operator-catalog.md) 新增 §7 HTTP 组）。
> M3 DoD 的"算子数 ≥ 40"由 M3-2（+2）、M3-3（df.from_table/to_table/to_csv，+3）补齐至 41，
> 超出原 5 个新增算子的目标（偏差：M3-3 额外提供 `df.to_csv` 保证 ≥40 达成）。
>
> M3-2 已完成（SqliteDbOpTest 6 用例绿；全量回归 127 tests OK）。落地 `ops/db_ops.py`：
> `db.sqlite_exec`（rowcount/lastrowid）与 `db.sqlite_query`（rows/count/columns，行字典表）
> 仅用标准库 `sqlite3`；`params` 位置参数按 `?` 占位符绑定；`db` 相对 workdir 解析、
> `:memory:` 支持单节点临时查询；两算子均 `deterministic=False`。重要发现：引擎按依赖边调度，
> 多节点写后读必须显式接线——两算子新增**可选输入端口 `gate`**（接上游任意手柄即排序），
> 否则 db 节点并行执行互相踩踏（单测闭环均经 `gate` 接线）。`setup.rowcount` 断言按 sqlite3
> DDL 约定取 `-1`。算子数 35→37（[算子清单](../reference/operator-catalog.md) 新增 §8 SQLite 组，
> 示例插件节顺延为 §9）。
>
> M3-3 已完成（DataFrameOpTest 3 用例 + TableColumnsUnionTest 2 用例全绿；全量回归通过）。
> 落地 `ops/dataframe_ops.py`：`df.from_table`（行表→DataFrame，`shape` 输出）、
> `df.to_table`（DataFrame→行表，NaN/NaT 还原 `None`、numpy 标量转原生类型，保证往返逐值一致）、
> `df.to_csv`（pandas 实现，可写行索引列）。pandas 只在 `run()` 内延迟导入——
> 未安装时模块照常导入、算子照常注册（单测用 `sys.modules['pandas']=None` 子进程验证），
> 调用才报 `RuntimeError` 提示 `pip install op-pipeline[dataframe]`；pyproject 新增
> `dataframe` extras（README §8 同步 extras 说明）。
> **T15 治理（行为变更，已标注 config-schema.md §2）**：`table_ops` 新增 `table_columns`
> （全表列集合并集，按首次出现顺序），select_columns/sort_rows/derive_column/join 四处
> 弃用 `rows[0].keys()`——列只在非首行出现的参差行不再丢字段或误报，列齐整的表行为不变。
> 算子数 37→40，达成 M3 DoD 的 "≥ 40" 目标。
>
> M3-4 已完成（EntryPointPluginTest 5 用例全绿；全量回归 137 tests OK）。落地入口点发现：
> `load_plugins(entry_points=True)`（[registry.py#L188-L281](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/registry.py#L188-L281)）
> 用 `importlib.metadata` 扫描 `[project.entry-points."op_pipeline.ops"]` 组并 import 各模块，
> 加载顺序：内置包 → 入口点 → `modules` → `paths`；单个入口点模块导入失败或算子名撞重
> 只告警跳过（自动发现场景以稳为先）。库调用默认关闭（向后兼容）、CLI 默认开启——
> 验收单测以"玩具插件包"（临时目录模块 + `.dist-info/entry_points.txt`，等价 pip 安装）验证
> `op_pipeline ops` 直接列出插件算子，无需 `--plugin`。文档：extension-points.md 新增
> §3.4 插件包发布指南（pip 分发三步 + 要点），README §7 同步入口点用法。算子数保持 40。
>
> M3-5 已完成（SpecVersionMigrationTest 6 用例全绿；全量回归 148 tests OK；两示例 e2e 通过）。
> 落地配置格式版本化：顶层 `spec_version` 字段（缺省 `"1"`，首个发布格式，不兼容变更必须
> 升号并注册迁移器）；模块级 `CURRENT_SPEC_VERSION` 与 `MIGRATIONS: dict[目标版本, callable]`
> 迁移器注册表（[spec.py](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/spec.py)）。
> `from_dict` 开头先 `_migrate(raw)`：非数字版本号报 `ConfigError`；配置版本高于当前（未来文件）
> 报清晰错误提示升级框架；低于当前则按 1→2→…→CURRENT 逐级调用 `MIGRATIONS`，缺某级迁移器报
> "缺少配置迁移器：X -> Y"。迁移后统一写回 `spec_version=CURRENT_SPEC_VERSION` 再解析。
> 单测用 `patch.object(CURRENT_SPEC_VERSION, "2"/"3")` + `patch.dict(MIGRATIONS, …)` 模拟未来
> 版本升级与缺迁移器场景；`spec_version` 已入 `_KNOWN_KEYS`（不触发未知键告警），`to_dict`
> 输出带 `spec_version`。旧配置（无 `spec_version`）照常解析，零破坏。算子数保持 40。
>
> M3-6 已完成（ObserveJsonlTest 5 用例全绿；全量回归 148 tests OK）。落地新模块
> [observe.py](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/observe.py)：
> `JsonlHook`（JSON lines 事件流 node_start/node_finish/run_finish，与报告同源——`run_finish`
> 直接取自 `RunReport` 字段、`node_finish` 的 status/cached 与报告节点一一对应，写文件带锁、
> 线程安全，并行运行下事件数与报告节点数一致）与 `OtelHook`（`on_run_finish` 内延迟 import
> opentelemetry，未安装时告警跳过、已安装记录 `op_pipeline.run` span 并带
> pipeline/run_id/status/duration 属性）。pyproject 新增 `otel` extras（`opentelemetry-api`），
> 内核 import 路径保持零第三方依赖（单测用 `sys.modules` 模拟缺依赖场景）。README §8 新增
> "观测性"节（JsonlHook 用法 + OtelHook 说明）。算子数保持 40。
>
> M3-7 已完成（IncludesConfigTest 7 用例全绿；全量回归 155 tests OK；两示例 e2e 通过）。
> 落地顶层 `includes` 配置组合（[spec.py](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/spec.py)
> `_resolve_includes`）：`from_dict(raw, base_dir=None)` 新增 `base_dir` 参数，`from_file`
> 自动以配置所在目录传入；递归加载子配置（子配置自身的 `includes` 按它的所在目录解析），
> 沿加载栈做环检测（A→B→A 报"includes 存在循环引用"，含链条）；合并语义主配置优先——
> `params`/`defaults`/`outputs` 逐键覆盖、`nodes` 按 include 顺序拼接且跨文件/与主配置重复
> 节点 id 即在解析期报 `ConfigError`（含双方来源文件）、`name`/`description`/`version`/
> `spec_version` 主配置优先；无 `base_dir` 时使用 `includes` 报"需要文件上下文"。
> 验收：includes 合并结果与手写大配置的节点数、参数、输出及依赖图（build_plan 拓扑序）
> 完全一致（单测直接比对两者 Plan）。examples 新增 includes 组合示例（M3 DoD）。
> 算子数保持 40。
>
> M3-8 已完成（RegisterLoaderTest 5 用例全绿；全量回归 160 tests OK；三示例 e2e 通过）。
> 落地自定义配置格式注册表 `PipelineSpec.register_loader(suffix, fn)`（[spec.py](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/spec.py)）：
> 模块级 `_LOADERS` 注册表，内建 json/toml/yaml 亦为注册项（无后缀文件按历史行为走 JSON）；
> 后缀自动归一化（`"fake"`/`".fake"` 等价、大小写不敏感），`fn(path) -> dict` 返回映射，
> 非 Mapping 报错；未知后缀报 `ConfigError` 并列出已注册格式；非可调用 / 非法后缀报错。
> `includes`（M3-7）递归加载与自定义 loader 正交——子配置文件同样按注册表分发。
> 扩展点文档（extension-points.md §1）同步：新配置格式由"内部接口"升级为"稳定"。
> 算子数保持 40。
>
> **M3 关闭（v0.4，2026-11-18）**：M3-1 ~ M3-8 全部完成，全量回归 160 tests OK，三示例
> （orders JSON / text TOML / includes 组合）端到端通过。M3 DoD 达成——`op_pipeline ops`
> 共 40 个算子（≥ 40）；每个 extras（dataframe/yaml/otel/dev）独立依赖声明且 README 有说明；
> 内核 `dependencies = []` 零第三方依赖（extras 模块延迟导入）；`examples/includes/`
> 新增配置组合示例（跨 JSON/TOML 拆分 + 主配置聚合）。T15（表算子首行列假设）由 M3-3 消除。
> 剩余技术债见 §8。

---

## 6. M4：插件信任、数据生态扩展与远程执行示范（v0.5，按需启动）

目标：补全 [roadmap 阶段 3](../architecture/roadmap.md#5-路线图阶段-1--15--2--3) 剩余方向中
"保持内核可嵌入"的部分——插件生态的信任边界（白名单 + 清单校验，M3-4 入口点发现的配套）、
数据生态的 Excel/Parquet/Arrow 读写（全部 extras，默认交换格式保持 `list[dict]` 不变）、
远程执行示范（Executor 接队列 + worker，句柄走共享目录，复用 M2 的 Executor/HandleStore 抽象）。
调度与 UI 无真实需求，按 roadmap 定位保持"独立外围服务、不进内核"方向**暂缓**（触发条件：出现
真实的调度/可视化诉求），本次不做。

| 编号 | 任务 | 改动落点 | 验收标准 |
| --- | --- | --- | --- |
| M4-1 | 插件信任：`load_plugins` 新增 `allowlist`（入口点名 / 模块名 / 文件路径三类来源的信任清单）与 `strict` 开关——清单外来源默认告警跳过、`strict=True` 报错拒绝；可选清单校验 `verify_manifest`（插件包 `.dist-info` 提供 `OP_PLUGINS.sha256` 清单，hashlib 校验文件哈希，零第三方） | [registry.py](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/registry.py) `load_plugins` | 单测①：allowlist 外插件不加载且告警；② `strict=True` 报错拒绝；③ 篡改插件文件后 `verify_manifest` 拒绝加载；④ 不传新参数行为完全向后兼容（160 用例不改断言全绿）；文档：extension-points.md 新增插件信任小节 |
| M4-2 | 数据生态（Arrow/Parquet）：`df.from_arrow` / `df.to_arrow`（`list[dict]` ↔ pyarrow Table）与 `io.read_parquet` / `io.write_parquet`（行表 ↔ parquet 文件）；pyarrow 可选 extras `parquet`，延迟导入模式同 `df._pandas()` | 新增 `ops/parquet_ops.py`、[pyproject.toml](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/pyproject.toml) extras | 单测：行表 ↔ Arrow/parquet 往返逐值一致；未装 pyarrow 时模块照常导入、调用报错提示 `pip install op-pipeline[parquet]`；extras 独立依赖声明 + README 说明；算子数 40→44 |
| M4-3 | 数据生态（Excel）：`io.read_excel` / `io.write_excel`（openpyxl 可选 extras `excel`，`sheet` 参数指定/多 sheet 写盘），行表交换格式不变 | 新增 `ops/excel_ops.py`、[pyproject.toml](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/pyproject.toml) extras | 单测：行表 ↔ .xlsx 往返一致；多 sheet 读写；未装 openpyxl 时延迟导入报错；extras 独立声明；算子数 44→46 |
| M4-4 | 远程执行示范：`QueueWorkerExecutor`（[executor.py](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/executor.py) 协议实现）——`submit` 把载荷 pickle 写入任务队列文件（JSONL），独立 worker 进程（新 CLI 入口 `op_pipeline worker --queue <path> [--out <dir>]`）消费执行、结果与异常写回共享目录（对象存储替代，复用 M2-4 SpillHandleStore 落盘语义），`wait` 轮询完成标记；载荷不可 pickle 直接报错不静默回退 | 新增 [remote.py](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/remote.py)（或并入 executor.py）、[cli.py](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/cli.py) | 单测①：另起 worker 进程消费队列，`run_pipeline(executor=QueueWorkerExecutor(...))` 与 ThreadExecutor 结果一致；② 无 worker 时超时/失败语义清晰；③ 不可 pickle 载荷报错；④ 文档：extension-points.md §4.2 更新远程执行器示例 + [运行时语义](../architecture/runtime-semantics.md) 新节 |

**M4 完成定义（DoD）**：`op_pipeline ops` 算子数 ≥ 46；每个 extras（excel/parquet）独立依赖声明
且 README 有说明；内核 `dependencies = []` 零第三方依赖保持（新模块延迟导入）；远程执行有端到端
示范（worker 进程 + 引擎进程双进程演示）；examples 新增数据生态示例（Excel/parquet 读写流水线）；
全量回归全绿。

> **M4 状态**：M4-1 已完成（2026-11-18，TDD 红→绿 8 新用例，全量回归 168 tests OK）。
> M4-1：`load_plugins` 新增 `allowlist` 白名单（入口点名/模块名/文件路径，清单外告警跳过、
> `strict=True` 拒绝）与 `verify_manifest` 哈希清单校验（hashlib，默认 `<base_dir>/OP_PLUGINS.sha256`，
> 仅文件插件，缺清单/未列入/不匹配均 `ConfigError`）。文档：extension-points §1 新行 + §3.5 新增。
> M4-2 已完成（2026-11-18，TDD 红→绿 5 新用例）：新增 `ops/parquet_ops.py` 四个算子——
> `df.from_arrow` / `df.to_arrow`（行表 ↔ pyarrow Table）、`io.read_parquet` / `io.write_parquet`
> （行表 ↔ Parquet 文件）；pyarrow 可选 extras `parquet`（延迟导入，未安装调用报错提示安装）。
> 算子数 40→44，README 内置算子表与 extras 说明同步。
> M4-3 已完成（2026-11-18，TDD 红→绿 4 新用例）：新增 `ops/excel_ops.py` 两个算子——
> `io.read_excel` / `io.write_excel`（openpyxl 可选 extras `excel`，延迟导入）；
> `io.write_excel` 支持 `sheets` 可选输入一次写多 sheet，`io.read_excel` 用 `sheet` 参数
> 指定工作表（None 读活动表）。算子数 44→46。
> M4-4 已完成（2026-11-18，TDD 红→绿 5 新用例）：新增 `remote.py` ——
> `QueueWorkerExecutor`（Executor 协议实现）把载荷 `pickle→base64` 追加进 JSONL 队列，
> `consume_jobs()` 增量消费执行并把结果/异常以 `<job_id>.json` 写回（`.tmp`→原子 `replace`），
> 客户端 `wait` 轮询完成标记；载荷不可 pickle 时 submit 直接 `RuntimeError`。
> 新 CLI：`op_pipeline worker --queue <path> [--out <dir>] [--count N] [--poll 秒]`。
> `__init__` 导出 `QueueWorkerExecutor`；双进程 e2e 用例（worker 子进程）通过。
>
> **M4 已完成（v0.5.0，2026-11-18）**。DoD 复核：算子数 46 ≥ 46 ✓；`parquet`/`excel` extras
> 独立声明 + README 说明 ✓；内核零第三方依赖保持（新模块均延迟导入，remote.py 纯标准库）✓；
> 远程执行双进程示范（[examples/remote_exec_demo.py](../../examples/remote_exec_demo.py)，
> 引擎进程提交 + worker 子进程消费，含异常回写）✓；examples 数据生态示例
> （[pipeline_data_ecosystem.toml](../../examples/pipeline_data_ecosystem.toml)，
> CSV → Parquet → Excel 往返）✓；全量回归 **182 tests 全绿** ✓。
> 文档：README §9（远程执行示范）、extension-points §4.2（队列执行器示例）、
> runtime-semantics §9（运行时序要点）。

---

## 7. M5：远程执行落地、调度与 UI 外围、数据生态补齐与体验打磨（v0.6，按需启动）

目标：把 M4 的远程示范推进为引擎正式能力；补齐数据生态的 MySQL/PostgreSQL；
调度与 UI 以独立外围子包落地（内核不引用）；顺手收口体验与技术债。

| 编号 | 方向 | 关键交付 |
| --- | --- | --- |
| M5-1 | 远程执行落地 | `run_pipeline`/CLI `--executor` 接受 `Executor` 实例或字符串（`"thread"`/`"process"`/`"queue"`）；[remote.py](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/remote.py) `_remote_run` payload 序列化（解决 bound method 不可 pickle）+ `SharedHandleStore` 共享目录句柄发布；引擎节点调度接入队列执行器（18 节点双进程 e2e）；extension-points §4.2、runtime-semantics §9.1 同步 |
| M5-2 | 调度与 UI 外围 | 独立子包 `scheduler_app/`：`scheduler.py` 守护进程（定时触发 `run`，结果写 RunStore）+ `web.py` 轻量 Web UI（http.server，运行历史/运行详情）；内核零引用有测试断言 |
| M5-3 | 数据生态 MySQL/PG | `db.read_mysql` / `db.write_mysql`（extras `mysql`，pymysql）、`db.read_postgres` / `db.write_postgres`（extras `postgres`，psycopg2），延迟导入；算子数 46→50 |
| M5-4 | 体验与技术债 | `CacheStore.include_source` 默认 `True`（确定性算子源码变更后缓存失效）；T13 `strict=False` 文档收口；CLI 失败节点展示截断 traceback；`ops --json` 契约完整性测试 |

**M5 完成定义（DoD）**：全量回归全绿；两示例端到端 + 远程双进程示范通过；
`pyproject.toml` 升 `0.6.0`；关闭声明与 roadmap 阶段 3 条目同步落地。

> **M5 已完成（v0.6.0，2026-11-18）**。
> M5-1：引擎 `executor` 参数支持 `str | Executor | None`（实例直接使用、字符串映射、
> 非法串报错）；节点任务打包为可 pickle payload，上游句柄值经共享目录
> `SharedHandleStore` 取用、输出原子发布；队列执行器接入引擎主循环，
> 双进程 e2e（`pipeline_orders.json` 18 节点全 success 且与线程执行器结果一致）通过。
> 新增 22 个用例（test_m5_remote_exec.py）；文档：extension-points §4.2 改写为
> "已接入"、runtime-semantics §9.1 远程执行语义。
> M5-2：`scheduler_app/` 落于项目根独立子包（标准库实现），调度守护进程
> `--pipeline <cfg> --every <秒>` 定时触发并把结果写 `RunStore`（复用 M2-5 SQLite），
> Web UI 提供运行历史列表与单次运行详情（节点状态/耗时/traceback 摘要）；
> 新增 7 个用例（test_scheduler_app.py），含"内核 import 路径无 `scheduler_app` 引用"断言。
> M5-3：新增 `db.read_mysql` / `db.write_mysql` / `db.read_postgres` / `db.write_postgres`
> 四个算子（延迟导入 pymysql / psycopg2，pyproject extras `mysql` / `postgres`，
> README / 算子清单同步），算子数 46→50；新增 14 个用例（test_db_mysql_pg.py）。
> M5-4：`CacheStore.include_source` 默认 `True`——确定性算子源码变更后默认参数重跑
> 缓存失效（`cached=false`）；CLI `run` 报告展示失败节点截断堆栈；`ops --json`
> 契约完整性有测试核对；新增 5 个用例（test_m5_experience.py）。
> 全量回归 **230 用例全绿** ✓；两示例端到端 + 远程双进程示范通过 ✓。

---

## 7.5 M6：工程质量与运维能力增强（v0.7）

目标：不做新能力扩张，只做**工程化收口**——把发布链路、调度外围、执行可靠性与
文档一致性补齐；内核语义、默认数据模型与零第三方依赖保持不变。

| 编号 | 方向 | 关键交付 |
| --- | --- | --- |
| M6-1 | 发布工程 | GitHub Actions 矩阵（Python 3.11/3.12/3.13，最小依赖档 + 全 extras 档）；CHANGELOG 按版本归档；wheel 内容与临时目录安装运行纳入发版前校验 |
| M6-2 | 调度与运维 | 调度器多流水线（`--pipeline` 可重复）、失败告警 `--alert-webhook`、`--keep-runs` + `RunStore.prune` 历史清理、`nodes.duration` 耗时列、Web UI 分页/状态筛选/节点耗时 |
| M6-3 | 执行可靠性 | 节点 `concurrency_group` + 运行级 `concurrency_limits`、按异常类型重试 `retry_on`、`worker --workers` 并发消费、远程节点取消传播（取消后标记 skipped、不再等待结果） |
| M6-4 | 文档与开发者体验 | 算子清单与注册表数量一致性测试；新增快速上手、运维指南、稳定 API 清单三份文档；配置规范/运行时语义补充并发分组、条件重试、远程取消 |

**M6 完成定义（DoD）**：全量回归全绿；主示例、插件示例与远程双进程示范端到端通过；
`pyproject.toml` 升 `0.7.0`；`CHANGELOG` 归档 0.7.0；文档与实现对齐核查通过。

> **M6 已完成（v0.7.0，2026-11-19）**：新增 52 个用例（7 个 M6 测试模块），全量回归
> **282 个用例全绿**；`examples/pipeline_orders.json`（18 节点全 success）、
> `pipeline_text.toml --plugin plugins/demo_ops.py`、`examples/remote_exec_demo.py`
> 三个端到端均退出码 0；wheel 元数据 0.7.0、含 `py.typed`，临时目录安装后
> `op_pipeline ops --json` 得 50 个算子。

---

## 8. 技术债映射

[扩展设计 §5](extension-points.md#5-已知技术债与实现缺口) 的 T1–T15 与本计划的对应关系：

| 技术债 | 任务 | 里程碑 |
| --- | --- | --- |
| T1 类型检查未接入 / T5 `_warnings` 无写入 | M1-1 | M1 |
| T2 `resolved_inputs` 残留 / T3 `ModuleExecutionError` 未用 / T14 死变量 | M1-6 | M1 |
| T4 `defaults` 未生效 | M1-5 | M1 |
| T6 缓存目录相对 CWD | M1-2 | M1 |
| T7 缓存不感知代码变更 | M2-5（可选开关，**已出债**：`CacheStore(include_source=True)`） | M2 |
| T8 软超时不可强杀 | M2-2 | M2 |
| T9 失败无堆栈 | M1-3 | M1 |
| T10 `run_id` 秒级精度 | M1-4 | M1 |
| T11 插件按 stem 去重 | M1-7 | M1 |
| T12 `run()` 不支持模块插件 | M1-8 | M1 |
| T13 `strict=False` 预留 | 不处理（保持现状，文档已说明） | — |
| T15 首行列假设 | M3-3 | M3 |

> 通用性任务（M1.5-1…8、M2-8、M3-7/M3-8）不来自技术债清单，而来自库通用性评估，
> 见 §3 与 [roadmap 阶段 1.5](../architecture/roadmap.md)。

---

## 9. 里程碑级验收清单

每个里程碑收尾时执行（也是发版前检查单）：

```powershell
# 1. 全量单测
python -m unittest discover -s tests -v

# 2. 端到端：18 节点主示例（含并行 fan-out / when / 断言 / 四路输出）
python -m op_pipeline run examples/pipeline_orders.json --no-cache

# 3. 端到端：插件 + 重试 + 句柄下钻
python -m op_pipeline run examples/pipeline_text.toml --plugin plugins/demo_ops.py --no-cache

# 4. 静态入口
python -m op_pipeline validate examples/pipeline_orders.json
python -m op_pipeline graph examples/pipeline_orders.json --format mermaid
python -m op_pipeline ops
```

另需人工核对：

- [ ] 报告 JSON 的 schema 变更已记录（新增字段加法 vs 破坏性变更）；
- [ ] [配置规范](../reference/config-schema.md) 与实际字段一一对应；
- [ ] [运行时语义](../architecture/runtime-semantics.md) 的"不变量清单"仍然成立；
- [ ] [技术债清单](extension-points.md#5-已知技术债与实现缺口) 已随任务完成更新状态。

---

## 10. 工程实践约定

1. **一个任务一个 PR**，PR 描述引用任务编号（如 `M1-2`），便于回溯验收标准；
2. **语义变更先改文档再改代码**：涉及 [运行时语义](../architecture/runtime-semantics.md)
   的改动，PR 里先提交语义描述，评审通过后落实现；
3. **测试先行**：每个任务先写失败的单测（复现问题或表达预期），再实现；
4. **评审否决线**：PR 引入新强依赖、改默认交换格式、在算子内部直读写句柄表——三者一律否决（依据 [roadmap §7](../architecture/roadmap.md#7-明确不建议的方向)）；
5. **发版**：每个任务合入即发一个小版本（`0.2.x`），M1/M1.5/M2/M3 收尾打 tag，`CHANGELOG` 按任务编号列出改动与影响面。
