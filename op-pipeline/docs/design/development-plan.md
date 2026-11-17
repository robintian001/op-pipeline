# 开发计划

> 本文是 [演进路线](../architecture/roadmap.md) 的任务级分解：
> 每个任务给出**改动落点**与**可验证的验收标准**，按里程碑组织。
> 任务编号规则：`M<里程碑>-<序号>`。技术债编号（T*）见
> [扩展设计 §5](extension-points.md#5-已知技术债与实现缺口)，映射关系见本文 §5。

---

## 1. 计划约定

| 约定项 | 内容 |
| --- | --- |
| 版本节奏 | M1 → `0.2.x`，M2 → `0.3.x`，M3 → `0.4+`（一个里程碑一个 minor 版本） |
| 合入门槛 | `python -m unittest discover -s tests -v` 全绿 + 新任务自带测试 + 两个示例流水线端到端通过 |
| 回归基线 | 每个任务合入前先跑基线；M2 的抽象类任务要求"平移后行为零变化"（现有 21 用例不改断言） |
| 文档同步 | 改动涉及节点字段/执行语义/CLI 的，同一变更内更新 [运行时语义](../architecture/runtime-semantics.md) 与 [配置规范](../reference/config-schema.md)；新增算子更新 [算子清单](../reference/operator-catalog.md) |
| 不兼容变更 | 报告 schema / 配置 schema 的破坏性改动必须升版本并在 changelog 标注；M1 阶段尽量只做加法 |
| 任务内不做的事 | 见 [roadmap §7](../architecture/roadmap.md#7-明确不建议的方向)，评审时据此否决范围蔓延 |

---

## 2. M1：正确性与承诺补齐（v0.2）

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

**M1 完成定义（DoD）**：测试全绿；`examples/pipeline_orders.json` 与
`pipeline_text.toml --plugin ...` 端到端通过；`extension-points.md` §5 的 T1–T6、T9–T12、T14 标记为已解决（T13 保持"预留，不处理"，T15 移交 M3）。

---

## 3. M2：三个抽象（v0.3）

目标：解锁硬超时、CPU 并行、断点续跑、可观测性。任务有依赖关系：M2-1 → M2-2，M2-5 → M2-6。

| 编号 | 任务 | 改动落点 | 验收标准 |
| --- | --- | --- | --- |
| M2-1 | 抽出 `Executor` 接口（`submit(node, payload) -> Future`、`wait(futures, first_completed) -> set`、`shutdown()`）；`ThreadExecutor` 平移现有 `ThreadPoolExecutor + wait(FIRST_COMPLETED)` 行为；`Engine` 只依赖接口 | 新增 [executor.py](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/executor.py)，改 [engine.py](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/engine.py) `_dispatch/collect` | **行为零变化**：现有 21 用例不改断言全绿；`test_parallel_speed` 仍 <0.8s |
| M2-2 | `ProcessExecutor`：`ProcessPoolExecutor` 实现；超时改为 terminate（**硬超时**）；payload（算子类+参数+输入）需可 pickle，不可 pickle 时回退线程执行并记录告警；节点级选择 `executor="process"` 或运行级 `--executor process` | [executor.py](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/executor.py)、[engine.py](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/engine.py) `_execute_once`、[spec.py](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/spec.py) `NodeSpec` | 单测①：CPU 密集 4 节点并行耗时显著低于串行；单测②：`timeout=0.5` 的死循环算子在进程模式下被终止且节点 `failed`（错误信息含"硬超时"）；消除 T8 |
| M2-3 | 抽出 `HandleStore` 接口（`get/put/snapshot/items`）；默认 `InMemoryHandleStore` 平移现有字典行为；`Engine._publish` 与 `RefResolver._lookup` 改为走该接口（保持两处收敛点不变） | 新增 handlestore 模块或并入 executor.py 命名空间，改 [engine.py](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/engine.py)、[spec.py](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/spec.py) `RefResolver` | 行为零变化（21 用例全绿）；`run_pipeline` 返回值仍为句柄值 dict |
| M2-4 | `SpillHandleStore`：值大小超过阈值（按 `fingerprint` 时的 repr 长度或专用估算）时 pickle 落盘（目录挂 `artifacts_dir`），句柄表存路径并在读取时按需加载 | handlestore 模块 | 单测：含两个大表（构造 >阈值）的流水线跑通且结果一致；可复跑（临时文件复用已有产物目录机制） |
| M2-5 | `RunStore` + SQLite：新模块定义 `record_run/record_node/query`；表结构 `runs(run_id, pipeline, version, status, started_at, finished_at, params_json)`、`nodes(run_id, node_id, op, status, attempts, cached, error, cache_key, finished_at)`；`Engine` 在 `_collect` 时写入；`--runs <db>` 开关（默认关闭，保持零副作用）；同时把算子源码摘要纳入 cache_key 的可选开关（缓解 T7） | 新增 [store.py](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/store.py)，改 [engine.py](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/engine.py)、[cli.py](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/cli.py) | 单测：跑完后 SQLite 可查询到与 `RunReport` 一致的节点状态/attempts；不传开关时不产生任何文件 |
| M2-6 | 断点续跑：`--resume <run_id>`——从 `RunStore` 读取上次成功节点的 `cache_key`，与本次编译后的 key 比对，一致则直接复用缓存/句柄并标记 `[resumed]` | [cli.py](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/cli.py)、[engine.py](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/engine.py) `_run_node` 缓存查询处 | 单测：三段流水线首跑到第二段失败；修数据后 `--resume` 只重跑失败段及下游，首段 `[resumed]`；依赖 M2-5 |
| M2-7 | 执行事件钩子：`Engine(hooks=[Hook])`，Hook 协议 `on_node_start(node_id, op)`、`on_node_finish(node_id, status, duration, cached)`、`on_run_finish(report)` | [engine.py](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/engine.py) `_run_node/_collect/run` | 单测：钩子按调度顺序收到事件；失败节点也触发 `on_node_finish`；文档给出进度条示例 |

**M2 完成定义（DoD）**：全部现有测试不改断言通过；新增 Executor/HandleStore/RunStore 单测；
`--executor process` + `--resume` 各有一个端到端示例；[运行时语义](../architecture/runtime-semantics.md)
新增"执行器/续跑"两节。

---

## 4. M3：生态与规模化（v0.4+，按需启动）

目标：扩大 B 类场景覆盖面；全部走"算子/extras/外围"，不动内核语义。

| 编号 | 任务 | 改动落点 | 验收标准 |
| --- | --- | --- | --- |
| M3-1 | `http.request` 算子（GET/POST、headers、timeout、json/text 双视图），仅用标准库 `urllib` | 新增 `ops/http_ops.py` | 单测（本地 `http.server` 假服务）；示例流水线调用公开 API 一次通过；`deterministic=False`（外部状态） |
| M3-2 | `db.sqlite_query` / `db.sqlite_exec`（标准库 `sqlite3`） | 新增 `ops/db_ops.py` | 单测：内存库建表-查询-写回闭环；写操作算子 `deterministic=False` |
| M3-3 | DataFrame adapter：`df.from_table` / `df.to_table`（pandas 可选 extras，`pip install op-pipeline[dataframe]`）；同时治理表算子"首行代表全表列"假设（T15：显式 `columns` 参数或列集合并集策略，行为变更须在文档标注） | 新增 `ops/dataframe_ops.py`、[pyproject.toml](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/pyproject.toml) extras、`table_ops.py` | 单测：`list[dict]` ↔ DataFrame 往返一致；未装 pandas 时 import 不报错；消除 T15 |
| M3-4 | 插件入口点发现：`pyproject` 支持 `[project.entry-points."op_pipeline.ops"]`；`load_plugins(entry_points=True)` 扫描注册 | [registry.py](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/registry.py) | 单测：安装一个玩具插件包后 `op_pipeline ops` 列出其算子；文档新增插件包发布指南 |
| M3-5 | 配置版本化：`spec_version` 顶层字段（缺省 `"1"`）+ 迁移器框架（`MIGRATIONS: dict[version, callable]`） | [spec.py](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/spec.py) `from_dict` | 单测：旧配置在新版本下仍可解析；未知更高版本报清晰错误 |
| M3-6 | 观测性：基于 M2-7 钩子的结构化事件导出（JSON lines / 可选 OTel bridge） | 新模块 `observe.py`（extras `otel`） | 单测：一次运行导出的 JSONL 与报告一致 |

**M3 完成定义（DoD）**：`op_pipeline ops` 算子数 ≥ 40；每个 extras 有独立的依赖声明与 README 说明；
内核 import 路径保持零第三方依赖（extras 模块延迟导入）。

---

## 5. 技术债映射

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

---

## 6. 里程碑级验收清单

每个里程碑收尾时执行（也是发版前检查单）：

```powershell
# 1. 全量单测
python -m unittest discover -s tests -v

# 2. 端到端：18 节点主示例（含并行 fan-out / when / 断言 / 四路输出）
python -m op_pipeline run examples/pipeline_orders.json --no-cache

# 3. 端到端：插件 + 重试 + 句柄下钻
python -m op_pipeline run examples/pipeline_text.toml --plugin examples/plugins/demo_ops.py --no-cache

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

## 7. 工程实践约定

1. **一个任务一个 PR**，PR 描述引用任务编号（如 `M1-2`），便于回溯验收标准；
2. **语义变更先改文档再改代码**：涉及 [运行时语义](../architecture/runtime-semantics.md)
   的改动，PR 里先提交语义描述，评审通过后落实现；
3. **测试先行**：每个任务先写失败的单测（复现问题或表达预期），再实现；
4. **评审否决线**：PR 引入新强依赖、改默认交换格式、在算子内部直读写句柄表——三者一律否决（依据 [roadmap §7](../architecture/roadmap.md#7-明确不建议的方向)）；
5. **发版**：M1/M2 收尾打 tag，`CHANGELOG` 按里程碑列出任务编号与影响面。
