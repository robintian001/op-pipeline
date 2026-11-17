# 开发计划

> 本文是 [演进路线](../architecture/roadmap.md) 的任务级分解：
> 每个任务给出**改动落点**与**可验证的验收标准**，按里程碑组织。
> 任务编号规则：`M<里程碑>-<序号>`。技术债编号（T*）见
> [扩展设计 §5](extension-points.md#5-已知技术债与实现缺口)，映射关系见本文 §6。

---

## 1. 计划约定

| 约定项 | 内容 |
| --- | --- |
| 版本节奏 | M1 → `0.2.0`；M1.5 各任务独立发 `0.2.x` 小版本（一任务一 PR 一发布，小步快跑）；M2 → `0.3`；M3 → `0.4+` |
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

---

## 6. 技术债映射

[扩展设计 §5](extension-points.md#5-已知技术债与实现缺口) 的 T1–T15 与本计划的对应关系：

| 技术债 | 任务 | 里程碑 |
| --- | --- | --- |
| T1 类型检查未接入 / T5 `_warnings` 无写入 | M1-1 | M1 |
| T2 `resolved_inputs` 残留 / T3 `ModuleExecutionError` 未用 / T14 死变量 | M1-6 | M1 |
| T4 `defaults` 未生效 | M1-5 | M1 |
| T6 缓存目录相对 CWD | M1-2 | M1 |
| T7 缓存不感知代码变更 | M2-5（可选开关） | M2 |
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

## 7. 里程碑级验收清单

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

## 8. 工程实践约定

1. **一个任务一个 PR**，PR 描述引用任务编号（如 `M1-2`），便于回溯验收标准；
2. **语义变更先改文档再改代码**：涉及 [运行时语义](../architecture/runtime-semantics.md)
   的改动，PR 里先提交语义描述，评审通过后落实现；
3. **测试先行**：每个任务先写失败的单测（复现问题或表达预期），再实现；
4. **评审否决线**：PR 引入新强依赖、改默认交换格式、在算子内部直读写句柄表——三者一律否决（依据 [roadmap §7](../architecture/roadmap.md#7-明确不建议的方向)）；
5. **发版**：每个任务合入即发一个小版本（`0.2.x`），M1/M1.5/M2/M3 收尾打 tag，`CHANGELOG` 按任务编号列出改动与影响面。
