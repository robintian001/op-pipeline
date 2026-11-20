# API 参考

本文按 `op_pipeline/__init__.py` 的 `__all__` 列出包顶层公开 API（共 **45 个**符号），
按用途分组，每个条目只说明一句话语义与稳定性承诺。

两类稳定性承诺：

- **稳定 API，破坏性变更升 minor**：已有调用方式在同一 major 版本内保持兼容；
- **内部/实验**：已导出便于高级用法，但接口细节可能随版本调整，不承诺兼容。

> 便捷函数 `op_pipeline.run(config, ...)` 未列入 `__all__`，但属于稳定 API：
> 从配置文件路径直接执行（自动按配置目录加载插件），等价于"加载 + `run_pipeline`"；
> v0.9 起新增可选参数 `params`（运行级参数注入，解析期 `${param:...}` 替换）、
> `sample_rows`（采样预览行数）、`lineage`（节点行数血缘开关，默认 True）。
> v0.9.1 起新增可选参数 `value_preview`（值级预览行数，报告节点附带
> `preview_in`/`preview_out`，只影响报告不影响执行）。
> 观测扩展 `op_pipeline.observe.JsonlHook` / `OtelHook` 见 README §8 与运行时语义。

---

## 1. 运行入口

| 符号 | 用途 | 稳定性 |
| --- | --- | --- |
| `run_pipeline` | 执行一个已解析的 `PipelineSpec`，返回 `(RunReport, dict[str, Any])`（报告 + 全部句柄值）；支持缓存、重试、并发分组限流、钩子、运行记录与各类执行器；v0.9 起新增可选 `params` / `sample_rows` / `lineage`，v0.9.1 起新增可选 `value_preview`（与 `run()` 语义一致）。 | 稳定 API，破坏性变更升 minor |
| `run_pipeline_async` | `run_pipeline` 的 async 宿主桥接：把同步引擎放到事件循环默认线程执行器，`await` 期间不阻塞循环，参数/返回值一致。 | 稳定 API，破坏性变更升 minor |
| `Engine` | 单次运行的执行器，接收编译后的 `Plan`；需要自定义句柄存储、执行器或直接驱动调度时使用（一个实例只跑一次）；v0.9.1 起新增可选 `value_preview`（值级预览）参数。 | 稳定 API，破坏性变更升 minor |
| `CacheStore` | 内容指纹缓存实现；`cache_dir=None` 即禁用；v0.8 起可选 `max_entries`/`max_bytes` 容量上限（按 mtime 淘汰、命中刷新 mtime）与 `key_lock(key)` 同 key 单飞锁，也可替换为同鸭子类型对象。 | 稳定 API（弱契约，可替换） |
| `fingerprint` | 计算任意值的内容指纹，是缓存 key（算子名 + 版本 + 参数 + 输入 + 源码摘要）的基础。 | 内部/实验 |

错误类型（同属运行入口的异常契约，全部派生自 `PipelineError`）：

| 符号 | 用途 | 稳定性 |
| --- | --- | --- |
| `PipelineError` | 框架异常基类。 | 稳定 API，破坏性变更升 minor |
| `ConfigError` | 配置/契约类错误基类；**永不参与重试**，下游按配置错误处理。 | 稳定 API，破坏性变更升 minor |
| `SpecValidationError` | 编译期校验失败（未知算子、端口/句柄/依赖/环等问题，异常信息一次列出全部）。 | 稳定 API，破坏性变更升 minor |
| `ParamValidationError` | 参数缺失、未声明、类型归一失败或超出 `minimum/maximum`。 | 稳定 API，破坏性变更升 minor |
| `CycleError` | 依赖图存在环。 | 稳定 API，破坏性变更升 minor |
| `OperatorNotFoundError` | 配置引用了未注册的算子（可能附带近似名建议）。 | 稳定 API，破坏性变更升 minor |
| `DuplicateOperatorError` | 注册名已被占用且未声明 `replace=True`。 | 稳定 API，破坏性变更升 minor |
| `ExpressionError` | `when` / 行级表达式超出 AST 白名单或求值失败。 | 稳定 API，破坏性变更升 minor |

---

## 2. 规格与计划

| 符号 | 用途 | 稳定性 |
| --- | --- | --- |
| `PipelineSpec` | 流水线配置的解析结果（参数、节点、输出、`includes`、`defaults`）；`from_file()` 支持 JSON/TOML/YAML；v0.9 起 `from_dict` / `from_file` 新增 `params=` 运行级参数注入（解析期与顶层 `[params]` 合并、注入优先，供 `${param:...}` 占位符解析）。 | 稳定 API，破坏性变更升 minor |
| `NodeSpec` | 单个节点规格（`op`、`inputs`/`outputs`、`params`、`when`、重试/超时/缓存/执行器策略）。 | 稳定 API，破坏性变更升 minor |
| `Plan` | `build_plan()` 产出的已校验执行计划（节点、拓扑序、句柄归属、合并后的参数）。 | 稳定 API，破坏性变更升 minor |
| `PlanNode` | 计划中的节点（节点规格 + 依赖 + 句柄映射）；引擎按它调度。 | 稳定 API，破坏性变更升 minor |
| `build_plan` | 把 `PipelineSpec` 编译为 `Plan`：参数合并、引用/端口/句柄校验、依赖推导、拓扑排序。 | 稳定 API，破坏性变更升 minor |
| `RefResolver` | 解析 `${...}` 引用（句柄/参数/环境变量，支持下钻）；高级用法可直接使用。 | 内部/实验 |
| `Port` | 算子输入/输出端口的声明类型（名字、类型、optional、doc）。 | 稳定 API，破坏性变更升 minor |
| `type_label` | 把 Python 类型转成端口/参数展示用的短标签。 | 内部/实验 |

---

## 3. 算子扩展

| 符号 | 用途 | 稳定性 |
| --- | --- | --- |
| `Operator` | 算子基类：声明 `inputs/outputs/params`，实现 `run(ctx, ...)`；提供 `execute()` 与 `describe()`。 | 稳定 API，破坏性变更升 minor |
| `op` | 注册装饰器：`@op("namespace.name")` 把算子类注册进当前/全局注册表（支持别名）。 | 稳定 API，破坏性变更升 minor |
| `Param` | 参数声明：类型、默认值、必填、`choices`、`minimum/maximum` 与文档。 | 稳定 API，破坏性变更升 minor |
| `Registry` | 算子注册表：注册、按名/别名查找、`describe()` 与独立注册表隔离运行。 | 稳定 API，破坏性变更升 minor |
| `default_registry` | 全局默认注册表（导入 `op_pipeline` 时自动加载 68 个内置算子）。 | 稳定 API，破坏性变更升 minor |
| `load_plugins` | 加载算子插件：模块路径、文件路径、已安装包入口点，支持白名单与哈希清单校验。 | 稳定 API，破坏性变更升 minor |
| `register_function` | 向安全表达式白名单注册纯函数，供 `when`、表算子行级表达式、`control.assert` 使用。 | 稳定 API，破坏性变更升 minor |
| `evaluate` | 按 AST 白名单求值单个表达式，返回结果。 | 内部/实验 |
| `evaluate_condition` | 按 AST 白名单求值布尔条件（`when` / `control.assert` 内部使用）。 | 内部/实验 |

### 3.1 v0.9 新增算子（9 个，一句话契约）

完整输入/输出/参数契约见[算子清单](operator-catalog.md)；此处仅一句话语义：

| 算子 | 用途 |
| --- | --- |
| `time.parse` | 解析时间文本（缺省 ISO 8601，可给 strftime `format`），输出 datetime ISO 文本与时间戳 |
| `time.shift` | 时间偏移：ISO 文本或时间戳输入按 days/hours/minutes/seconds 平移（可为负） |
| `time.now` | 输出当前时间的 datetime / 时间戳 / 日期三视图（`deterministic=False` 永不缓存） |
| `json.flatten` | 嵌套 dict 扁平化：键按路径拼接（`a.b.c`）展开为一层，list 值作为叶子 |
| `table.explode` | 把 list 列展开成多行（每个元素一行、其余列复制；`keep_empty` 控制空列表丢弃/保留，`index_column` 记录原始行索引） |
| `quality.null_check` | 空值率检查：None/空白占比超过 `max_null_ratio` 即违例（error 抛错 / warn 出明细） |
| `quality.unique` | 按 `keys` 列组合查重复行，重复即违例（error 抛错 / warn 出明细） |
| `quality.schema_check` | 逐行类型检查：按 `expected` 列名→类型名声明校验（error 抛错 / warn 出明细） |
| `pipeline.run` | 子流水线编排：运行另一份配置文件并取其某个输出句柄（防环、深度上限 5、永不缓存） |

### 3.2 v0.9.1 新增算子（9 个，一句话契约）

完整输入/输出/参数契约见[算子清单](operator-catalog.md)；此处仅一句话语义：

| 算子 | 用途 |
| --- | --- |
| `json.parse` | JSON 文本 -> Python 结构（`json.loads` 语义；非法输入抛 ValueError，dict / list / 标量均可） |
| `json.stringify` | Python 结构 -> JSON 文本（默认 indent=2、ensure_ascii=False、不排序键；输出文本与 UTF-8 字节数） |
| `table.fill_null` | 填充 None 单元格：`value` 填充值、`columns` 限目标列（None=全部列），输出 `filled` 填充数，不原地修改 |
| `table.replace` | 按映射替换单元格值：`mapping` 旧值→新值（`regex=true` 时按正则全量替换，仅对 str 生效），输出 `replaced` 计数 |
| `table.pivot` | 长表转宽表：`index` 行标识列、`columns` 取值展开为新列名、`values` 值列；`agg`（first/last/sum/count/mean）可选，None 表示不允许重复组合 |
| `table.unpivot` | 宽表转长表（逆透视）：`columns` 展开列、`index` 保留 id 列（None=除展开列外的全部列）、`name_to`/`value_to` 新列名 |
| `text.hash` | 计算文本哈希摘要（hexdigest）：`algorithm` 白名单 md5/sha1/sha256/sha512/blake2b（默认 sha256），输出 digest 与字符数 |
| `compress.gzip` | 文本 gzip 压缩写出为文件（`path`/`encoding`/`mtime`；`deterministic=False`，有落盘副作用、结果不可缓存） |
| `compress.ungzip` | 读取 gzip 文件解压为文本（`path`/`encoding`，输出 text 与字符数；文件不存在抛 FileNotFoundError） |

---

## 4. 执行器与句柄

| 符号 | 用途 | 稳定性 |
| --- | --- | --- |
| `Executor` | 执行器协议：`submit(fn, *args)` / `wait(futures, first_completed=...)` / `shutdown()`。 | 稳定 API（协议） |
| `ThreadExecutor` | 默认线程池执行器：同步算子并行执行，支持软超时。 | 稳定 API，破坏性变更升 minor |
| `ProcessExecutor` | 子进程执行器：节点在 `spawn` 子进程中执行，超时为硬终止；载荷不可 pickle 时回退线程。 | 稳定 API，破坏性变更升 minor |
| `PooledProcessExecutor` | **v0.9.1 新增**：池化进程执行器——N 个常驻 spawn worker 消费任务队列；硬超时 terminate 对应 worker 并自动补位，崩溃自愈；载荷不可 pickle 时回退线程。 | 稳定 API，破坏性变更升 minor |
| `QueueWorkerExecutor` | 队列执行器：任务写入共享 JSONL，由独立 worker 进程消费，结果目录原子回写；v0.8 起构造参数新增 `lease_seconds`/`max_attempts`（租约与重投上限）。 | 稳定 API，破坏性变更升 minor |
| `consume_jobs` | worker 侧消费入口（`op_pipeline.remote` 模块级）；v0.8 起新增 `owner`/`lease_seconds`/`max_attempts` 参数，用于租约认领与过期回收。 | 稳定 API（模块级） |
| `compact_queue` | 队列归档与清空（`op_pipeline.remote` 模块级，CLI `queue compact`）：仅当全部任务已有结果时归档并原子清空，返回 `(未完成 job_id 列表, 归档路径)`。 | 稳定 API（模块级） |
| `HandleStore` | 句柄存储协议：`get/put/snapshot/items`；节点间只通过句柄交换数据。 | 稳定 API（协议） |
| `InMemoryHandleStore` | 默认句柄存储：值常驻内存。 | 稳定 API，破坏性变更升 minor |
| `SpillHandleStore` | 大对象按阈值落盘的句柄存储（默认 1MB），读时按需加载；v0.8 起 `cleanup="on_run_success"` 在运行成功后清理本次 run 的落盘目录（失败/取消保留，显式 `spill_dir` 不删）。 | 稳定 API，破坏性变更升 minor |
| `SharedHandleStore` | 跨进程共享目录的句柄存储，远程 worker 与引擎用它交换节点输出。 | 稳定 API，破坏性变更升 minor |

---

## 5. 运行记录

| 符号 | 用途 | 稳定性 |
| --- | --- | --- |
| `RunStore` | 可选 SQLite 运行记录：`record_run` / `record_node` / `query` / `prev_node`；v0.7 起节点记录含 `duration`，并提供 `prune(keep_last)` 历史清理；v0.9 起 `record_node` 新增 `rows_in`/`rows_out`（端口行数血缘 JSON，缺省 NULL），nodes 表缺列时构造即幂等自动迁移；v0.9.1 起 `record_run` 新增 `report_json`（全量报告 JSON，缺省 NULL），runs 表缺列时同样自动补列。 | 稳定 API，破坏性变更升 minor |

---

## 6. 钩子与观测

| 符号 | 用途 | 稳定性 |
| --- | --- | --- |
| `Hook` | 执行事件协议：`on_node_start` / `on_node_finish` / `on_run_finish`，可实现其一；节点并行时可能并发调用。 | 稳定 API，破坏性变更升 minor |
| `RunContext` | 一次运行的上下文：`run_id`、参数、工作目录、产物目录、`for_node()`。 | 稳定 API，破坏性变更升 minor |
| `NodeContext` | 单节点上下文：日志、`params` 自省、`artifact_path()` / `resolve_path()`；算子只依赖它。 | 稳定 API，破坏性变更升 minor |
| `RunReport` | 运行报告：状态、耗时、参数、输出、告警与节点结果列表；提供 `summary()` / `to_dict()` / `to_json()`。 | 稳定 API，破坏性变更升 minor |
| `NodeResult` | 单节点结果：状态、耗时、尝试次数、缓存/续跑标记、输出类型摘要、错误与截断堆栈。 | 稳定 API，破坏性变更升 minor |

---

## 7. 外围服务与 CLI

### 7.1 外围服务 API（`scheduler_app`）

| 符号 | 用途 | 稳定性 |
| --- | --- | --- |
| `scheduler_app.scheduler.run_scheduler` | 调度循环；v0.8 起新增 `alert_silence`（秒，同 pipeline 窗口内失败只告警一次）；v0.9 起新增 `after`（名 -> 依赖列表映射，本轮依赖全部 success 才触发，否则跳过记 info），条目支持 `{"path": ..., "after": [...]}`。 | 稳定 API，破坏性变更升 minor |
| `scheduler_app.web.create_server` | 只读 Web UI；v0.8 起新增 `/compare`、`/api/runs`、`/api/runs/<id>` 与 `/metrics` 路由；v0.9 起详情页新增节点执行时间线与 In/Out 行数列，`/compare` 新增行数差列；v0.9.1 起新增 `/api/runs/<id>/report` 与 `/stats` 路由，列表页与 `/api/runs` 支持 `pipeline=`/`since=`/`until=` 筛选，详情页新增报告折叠区块。 | 稳定 API，破坏性变更升 minor |

### 7.2 CLI 子命令

| 子命令 | 用途 |
| --- | --- |
| `run <cfg>` | 执行流水线；v0.8 新增 `--cache-max-entries` / `--cache-max-bytes` / `--spill-warn-bytes`（均可选）；v0.9 新增 `--param KEY=VALUE`（运行级参数注入，可重复）与 `--sample N`（采样预览）；v0.9.1 新增 `--param-file PATH`（可重复，与 `--param` 合并、命令行覆盖文件值）、`--report-out PATH`（全量报告 JSON 落盘，父目录自动创建、写失败不掩盖运行结果）与 `--preview N`（值级预览） |
| `validate <cfg> [--json]` | 只加载/编译/契约校验；`--json` 输出 `{"ok","errors","warnings","nodes"}` |
| `init <dir> [--template minimal\|data\|scheduler\|quality\|subpipeline]` | 生成脚手架项目；目标目录非空时拒绝；v0.9.1 新增 `quality` 与 `subpipeline` 模板 |
| `scaffold op <name> [--dir PATH]` | 生成算子与测试骨架；非法名或文件已存在时拒绝 |
| `graph <cfg> [--format mermaid\|dot\|ascii]` | 输出依赖图 |
| `ops [--json]` / `describe <op_name>` | 列出全部算子 / 查看单算子契约 |
| `explain <cfg> [--param KEY=VALUE ...] [--node ID] [--json]` | **v0.9 新增**：输出解析后的最终生效配置（不执行节点、零文件产出） |
| `runs [--db PATH] [--status s] [--pipeline p] [--last N] [--json]` / `runs <run_id>` | **v0.9.1 新增**：查询 RunStore 运行记录列表 / 详情（缺省库 `runs.db`；库不存在退出码 1、无匹配退出码 0） |
| `events <file.jsonl> [--run-id ID] [--node ID] [--event TYPE] [--last N] [--summary]` | **v0.9 新增**：查询/过滤/聚合 JsonlHook 事件流（损坏行容错） |
| `worker --queue <file>` | 消费共享队列；v0.8 新增 `--lease-seconds` / `--max-attempts` / `--owner` |
| `queue compact --queue <file> --out <dir> [--archive <file>]` | 全部任务完成后归档历史行并清空活跃队列 |

> 以上均为加法，向后兼容：所有新参数可选，缺省行为与上一版本一致。v0.9 的报告
> schema 变更为纯加法（run 级 `sample_rows`、节点级 `sampled`/`rows_in`/`rows_out`），
> RunStore nodes 表补 `rows_in`/`rows_out` 列（自动迁移），算子数量 50 → 59
> （53 个零依赖内置 + 6 个可选依赖）；v0.9.1 再增 9 个零依赖算子（json/table/text/
> compress 组）至 68（62 个零依赖内置 + 6 个可选依赖），报告 schema 再增 run 级
> `value_preview` 与节点级 `preview_in`/`preview_out`，runs 表补 `report_json` 列
> （自动迁移），全部为纯加法，不构成破坏性变更。

---

## 8. 相关文档

- [快速上手](../guides/quickstart.md)：安装、首个流水线、自定义算子与常见错误；
- [运维指南](../guides/operations.md)：调度、Web UI、RunStore、远程 worker 与排障；
- [配置规范](config-schema.md)：顶层/节点字段、引用语法、表达式子集与 CLI；
- [算子清单](operator-catalog.md)：内置算子的输入/输出/参数契约；
- [运行时语义](../architecture/runtime-semantics.md)：调度、重试、并发分组、取消与缓存的完整语义。
