# 演进路线与场景评估

> 本文是项目的中长期规划基线：先回答"这个框架现在适合什么、不适合什么"，
> 再给出"往哪里演进、不往哪里演进"以及理由。
> 具体到任务级的执行计划见 [开发计划](../design/development-plan.md)。

---

## 1. 总体判断

`op_pipeline` 目前是一个**小而完整的单机嵌入式流水线内核**：配置编译、契约校验、
并行调度、重试、缓存、报告这条链路已经闭环，且零第三方依赖。

它的价值不在功能数量，而在三点结构性资产：

1. **语义被写死**——失败传播、重试、缓存失效等行为有明确规则（见 [运行时语义](runtime-semantics.md)）；
2. **边界清晰**——算子与编排层通过句柄解耦，算子可脱离引擎单测；
3. **可嵌入**——`run_pipeline()` 一行调用，无服务、无数据库、无守护进程。

因此它的定位是"**长期维护的数据处理脚本与内部工具的底座**"，而不是调度平台。

---

## 2. 能力边界（评估的事实基础）

| 维度 | 现状 | 直接推论 |
| --- | --- | --- |
| 数据处理 | `list[dict]` 行表 + 68 个算子（62 个零依赖 + 6 个可选依赖），覆盖文件→清洗→质量→聚合→报告 | 文件型、批式、中等数据量最顺手 |
| 数据规模 | 句柄默认全内存驻留，超阈值可落盘（`SpillHandleStore`）；缓存走 pickle 文件，v0.8 起可选容量上限（`max_entries`/`max_bytes`，按 mtime 淘汰） | 适合 MB～几百 MB 级；GB 级以上仍不适合 |
| 计算模型 | 同步算子 + 线程池（默认 `min(8, CPU)`）；async 宿主经 `run_pipeline_async` 桥接（M2-8，`run_in_executor`） | IO 并行有效，CPU 密集无收益；桥接保证事件循环不被阻塞 |
| 可靠性 | 重试/退避、`allow_failure`、`fail_fast` 完整；元数据持久化与断点续跑（M2-5/M2-6，`--runs` + `--resume`）可选开启；远程队列支持租约认领与过期重投（M7-1，`--lease-seconds`） | 可接受"失败重跑"，也可 `--resume` 只重跑失败段；远程 worker 崩溃后任务可回收 |
| 容错 | 软超时（线程不可强杀）、失败不保留堆栈 | 不适合强实时、强隔离任务 |
| 安全 | 表达式 AST 白名单；插件是任意代码执行、缓存是 pickle 反序列化 | 信任边界=本机可信用户；不能接多租户/不可信配置 |
| 可嵌入性 | 零依赖、一行调用、算子可单测 | 最难被替代的优势 |
| 生态 | HTTP/SQLite 已内置，DataFrame 走 pandas extras，插件机制完备 | 缺的是"库存"而非"能力"，扩展成本低 |

---

## 3. 场景适配

### 3.1 强适配（现在即可直接用）

| 场景 | 依托能力 |
| --- | --- |
| 文件型批处理 ETL（CSV/JSON/日志的清洗、关联、聚合、落盘） | io/table/stats 算子组；`examples/pipeline_orders.json` 即模板 |
| 周期性报表生成（经营日报、周报、对账） | `report.markdown/html` 自包含输出 + 外部 cron / 任务计划触发 |
| 数据质量门禁（发布前校验关卡） | `control.assert` + `allow_failure` + 报告状态与退出码 |
| 替代越写越乱的长脚本 | 算子可单测 + 配置可评审，性价比最高的用法 |
| 受限/离线环境（内网、无依赖环境、客户交付包） | 零第三方依赖是硬优势 |
| 作为大型系统的内嵌子组件（Web 后端、桌面程序、CI 步骤） | 库形态，`run_pipeline()` 一行调用 |

### 3.2 补插件后可用（框架不用改，缺算子库存）

| 场景 | 需要补的算子 | 对内核的改动 | 计划落点 |
| --- | --- | --- | --- |
| API 数据采集 | `http.request`（标准库 `urllib` 即可，不破零依赖） | 无 | M3（**已落地**） |
| 数据库读写 | `db.sqlite_*`（标准库）；MySQL/PG 走可选 extras | 无 | M3 sqlite / M5-3 MySQL、PG（**均已落地**） |
| Excel/Parquet | 可选依赖算子，做成 extras 不污染内核 | 无 | M4（**已落地**） |
| 较大数据量 | `list[dict]` ↔ DataFrame adapter（df.from_table/to_table/to_csv，pandas extras），重计算段交给 pandas | 无（编排层零改动） | M3（**已落地**） |
| 插件包分发 | `[project.entry-points."op_pipeline.ops"]` 入口点自动发现（M3-4），pip 安装即用 | 无（`load_plugins` 加开关，CLI 默认开启） | M3（**已落地**） |
| 定时调度 | 库内嵌定时触发由外围子包 `scheduler_app` 提供（M5-2，**已落地**）；平台化调度仍不做，交给 cron / 任务计划 / CI schedule | 无（内核不引用该包） | M5-2 |

这一节成立的前提是"算子级封装"核心决策有效——目前所有补齐都不需要动引擎，说明该决策成立。

### 3.3 明确不适合（不建议硬上）

- 分布式大数据处理、实时流计算（内存句柄模型与批式 DAG 语义均不匹配）；
- 需要硬超时、任务强杀、故障隔离的场景（线程模型做不到，M2 进程执行器后才部分解决）；
- 多租户、权限、审计、Web UI 的企业级调度平台（另一个产品形态）；
- 不可信第三方提交配置/插件的环境（插件任意代码执行 + pickle 缓存）；
- CPU/GPU 密集并行计算（GIL + 无进程池，M2 后 CPU 密集可用进程执行器缓解）。

---

## 4. 演进原则

在谈路线之前先定四条约束，避免"演进死"：

1. **内核保持零依赖、保持小**。新能力优先走"可选 extras + 外围服务"，而不是塞进内核。
2. **所有中长期演进有三个共同前置抽象**，先做它们，后面每步都便宜：
   - `Executor`（执行器写死在 `Engine`：线程池 + `wait(FIRST_COMPLETED)`）；
   - `HandleStore`（句柄写死为 `Engine.handles` 内存字典，访问收敛在
     `_publish` 与 `RefResolver._lookup` 两处，这为抽象提供了天然切面）；
   - `RunStore`（只有内存中的 `RunReport`，无持久化元数据）。
3. **契约版本化**：配置 schema、报告 schema、算子 `version` 三者是外部承诺，
   演进优先做加法而非改语义；不兼容变更必须升 `spec_version`。
4. **嵌入视角优先**：新能力设计时必须回答"嵌入到服务、async 宿主或多流水线并发进程时会怎样"——
   全局状态、惊讶副作用、阻塞、不可取消这四类问题在设计期消除，而非事后补丁。

---

## 5. 路线图（阶段 1 → 1.5 → 2 → 3 → 4 → 5 → 6）

### 阶段 1：承诺补齐 + 正确性 — **已完成（v0.3.0，2026-11-17）**

目标：让实现与文档一致，消除隐性正确性问题。全部是小改动，但其中两项是"文档说了但没做到"。

| 事项 | 价值 | 落点 |
| --- | --- | --- |
| 接入端口软类型告警到 `report.warnings` | README 宣称的能力实际不发生（`check_handle_types` 无调用方） | `engine.py` |
| 缓存目录相对 `workdir` 而非进程 CWD | 消除"换个目录启动就多一份缓存" | `cli.py` / `__init__.run()` |
| `NodeResult` 增加截断堆栈 | 排障成本大幅下降 | `engine.py` |
| `run_id` 加随机后缀 | 避免同秒并发共用产物目录 | `context.py` |
| `defaults` 真正生效（节点级运行策略缺省） | 配置体验 + 消除死字段 | `spec.py` |
| 死代码清理（`resolved_inputs`、`ModuleExecutionError`、`to_mermaid` 死变量） | 降低维护者误读成本 | `spec.py` / `errors.py` / `graph.py` |
| 插件按路径哈希去重、`run()` 支持模块插件 | 消除静默不加载；补全库调用入口 | `registry.py` / `__init__.py` |

### 阶段 1.5：库通用性补齐（对应版本 0.2.x）— **已完成（v0.2.1，2026-11-17）**

目标：消除作为"库"嵌入任意宿主（服务、测试、async 宿主、多流水线并发进程）时的惊讶行为。
任务小粒度、每个可独立发版，与阶段 1 交错执行；任务级分解见 [开发计划 §3](../design/development-plan.md)。
综合验收示例：`examples/embedded_lib_demo.py`（独立注册表 + 取消事件 + 纯内存三合一）。

| 事项 | 消除的通用性缺口 | 任务 |
| --- | --- | --- |
| 独立注册表（`load_plugins`/`run` 补 registry 注入） | 全局单例 → 测试互相污染、无算子集隔离 | M1.5-1 |
| `py.typed` + 完整打包元数据 | 外部消费者类型是盲的 | M1.5-2 |
| artifacts 惰性创建 + 位置覆盖 | 嵌入方 workdir 被留隐藏目录 | M1.5-3 |
| 未知配置键告警 | 拼错键静默通过（"0 节点成功"） | M1.5-4 |
| `when` 环境 `params` 一致性 | 原始串与解析值混排，配置陷阱 | M1.5-5 |
| 表达式函数扩展钩子 | 领域函数不可用，复杂条件被迫写算子 | M1.5-6 |
| 取消机制（`cancel_event`） | 服务请求取消后流水线仍跑完 | M1.5-7 |
| 注册表/插件加载线程安全 | 单进程并发 `run()` 的竞态 | M1.5-8 |
| `run_pipeline_async` | 同步阻塞卡死 async 宿主的事件循环 | M2-8 |

### 阶段 2：三个抽象（对应版本 0.3.x）

目标：解锁硬超时、CPU 并行、断点续跑、可观测性。这是性价比最高的一次架构投入。

1. **`Executor` 抽象**：先平移出 `ThreadExecutor`（行为不变），再加 `ProcessExecutor`——
   一次性解决**硬超时**（进程可 terminate）、**CPU 密集并行**、**故障隔离**三个天花板。
2. **`HandleStore` 抽象**：默认仍是内存字典；增加 `SpillHandleStore`（超阈值落盘、句柄存引用），
   把数据规模上限从"内存"抬到"磁盘"。
3. **`RunStore` + SQLite**：记录 run/node 状态、attempts、error、cache_key、产物路径，
   直接支撑**断点续跑**（跳过已成功且 cache_key 一致的节点）与历史查询。
4. **执行事件钩子**（`on_node_start/finish`、`on_run_finish`）：进度条、日志聚合、
   OpenTelemetry、未来 UI 都从这里取数，成本极低但别晚做。

> 进度（2026-11-18）：1–4 已在 M2 里程碑落地——`Executor`（M2-1/M2-2）、
> `HandleStore`（M2-3/M2-4）、`RunStore`+`--runs` 落库（M2-5）与
> `--resume` 断点续跑（M2-6）、执行事件钩子 `Hook`（M2-7，含进度条示例，
> 见 [runtime-semantics §6.2](../architecture/runtime-semantics.md#62-执行事件钩子m2-7)）。
> 库通用性轨道 `run_pipeline_async`（M2-8）也已落地（见
> [runtime-semantics §6.3](../architecture/runtime-semantics.md#63-async-宿主嵌入m2-8)）；
> 全 asyncio 调度器仍明确不在范围。

### 阶段 3：生态与规模化（对应版本 0.4～0.6，按需触发）

- **数据生态**：pandas/Arrow adapter 算子；默认交换格式保持 `list[dict]` 不变——
  **已落地**（M3 pandas adapter；M4-2 Arrow/Parquet 算子 + extras `parquet`；
  M4-3 Excel 算子 + extras `excel`；M5-3 `db.read/write_mysql` + `db.read/write_postgres`
  + extras `mysql`/`postgres`；examples 数据生态往返示例；算子数 50）；
- **远程执行**：`Executor` 接队列 + worker，句柄走对象存储（前提：阶段 2 抽象已稳定）——
  **已接入引擎（M5-1；M4-4 队列执行器示范）**：引擎 `executor` 参数接受
  `Executor` 实例或字符串 `"queue"`，节点任务以可 pickle payload 提交、
  输出经共享目录 `SharedHandleStore` 发布（18 节点双进程 e2e）；
- **调度与 UI**：如确有需要，做成**独立外围服务**（读 `RunStore`、调 `run_pipeline`），
  内核仍是库——这是与 Airflow 的差异化定位，不要在包里长出调度守护进程；——
  **已落地（M5-2，独立外围子包 `scheduler_app/`）**：调度守护进程（定时触发
  `run_pipeline`，结果写 `RunStore`）+ `http.server` 轻量 Web UI（运行历史/运行详情）；
  内核不引用该包，"外围服务、内核仍是库"的定位保持；
- **插件生态**：独立插件包 + `entry_points` 发现 + 签名/白名单机制，缓解插件任意代码执行风险
  ——**已落地**（M3-4 entry_points 发现；M4-1 allowlist 白名单 + verify_manifest 清单校验）；
- **配置演进**：`spec_version` 字段 + 迁移器，保证老配置长期可跑——**已落地（M3-5）**；
  配置组合 `includes`（跨文件拆分/复用，环检测 + 节点 id 查重）——**已落地（M3-7）**；
- **观测性**：基于事件钩子的结构化日志 / Trace 导出——**已落地（M3-6）**：`observe.JsonlHook`
  事件流（JSON lines，与报告同源）+ `OtelHook`（extras `otel`，延迟导入）。

### 阶段 4：工程质量与运维（对应版本 0.7）— **已完成（v0.7.0，2026-11-19）**

定位：**工程化收口，而非新能力扩张**。本阶段不改变内核语义与默认数据模型，只把
"能跑"的库补齐为"可持续交付、可运维"的库：发布链路可重复、调度外围可运营、
执行可靠性可配置、文档与实现可自动核对。四条已落地能力：

- **CI / CHANGELOG / 构建校验**（M6-1）：GitHub Actions 覆盖 Python 3.11–3.13 的
  「最小依赖档 + 全 extras 档」双流水线；CHANGELOG 按版本归档；wheel 内容与临时目录
  安装运行（`ops --json` 算子数）纳入发版前校验；
- **调度多流水线 + 告警 + 保留 + UI 分页**（M6-2）：`scheduler_app` 支持 `--pipeline`
  重复、失败 `--alert-webhook`、`--keep-runs`/`RunStore.prune` 历史清理，Web UI 支持
  `?page&size&status` 分页筛选与节点耗时展示；
- **并发分组 / 条件重试 / worker 并发 / 远程取消**（M6-3）：节点 `concurrency_group` +
  运行级 `concurrency_limits`、`retry_on` 按异常类型重试、`worker --workers` 并发消费、
  远程节点取消后不再等待并标记 `skipped`；
- **文档一致性测试 + 三份新文档**（M6-4）：算子清单与注册表数量一致性测试；
  新增快速上手、运维指南、稳定 API 清单。

原则仍是"外围与可选配置进、内核语义不动"：以上能力均默认关闭或保持旧行为，
零第三方依赖与可嵌入定位不变。

### 阶段 5：可靠性、配置体验与资源治理（对应版本 0.8）— **已完成（v0.8.0，2026-11-19）**

定位：**在既有抽象上补可靠性缺口与使用体验，不扩张内核边界**。四条方向：

- **远程执行可靠性**（M7-1）：队列执行从"投递后听天由命"升级为**租约制**——
  任务认领以 `claims/<job_id>.json` 原子创建互斥，执行期心跳续租，空闲 worker
  扫描过期租约并按 attempt 重投，超过上限写 error 结果；`queue compact` 在全部
  任务完成后归档历史行并原子清空活跃队列；
- **配置体验与脚手架**（M7-2）：`validate --json`、`init --template` 与
  `scaffold op` 三个入口；配置字符串支持解析期 `${env:NAME}` / `${env:NAME:-default}`；
  运行报告对敏感参数打码；
- **运维外围**（M7-3）：Web UI 增加 `/compare` 运行对比、`/api/runs` 只读 JSON API
  与 Prometheus `/metrics`；调度器 `--alert-silence` 提供同 pipeline 告警静默窗口；
- **数据与缓存治理**（M7-4）：`CacheStore(max_entries/max_bytes)` 按 mtime 做 LRU
  容量淘汰并支持同 key 单飞；`SpillHandleStore(cleanup="on_run_success")` 成功后
  清理本次 run 的落盘目录；CLI 增加缓存容量与 spill 占用告警参数。

原则仍是"加法、可选、默认不变"：所有新增参数缺省时行为与 v0.7.0 一致，
RunStore schema 与算子数（50）不变，零第三方依赖与可嵌入定位保持。

### 阶段 6：编排组合、开发者体验与观测性（对应版本 0.9）— **已完成（v0.9.0，2026-11-19）**

定位：**把"一份配置自上而下跑一遍"补成"可组合、可预览、可观测"**，内核语义与默认
行为不变，全部为可选加法。四条方向：

- **编排组合与参数化**（M8-1）：配置字符串解析期 `${param:NAME}` /
  `${param:NAME:-默认值}` 运行级注入（注入优先于顶层 params，CLI `--param`）；
  `pipeline.run` 子流水线算子——把一份子配置当"大算子"执行（thread-local 防环、
  嵌套深度上限 5、永不缓存，子失败按 `on_error` 传播含摘要）；调度器 `pipeline`
  条目支持 `after` 依赖声明（拓扑触发、依赖本轮全部 success 才执行，否则跳过记
  info）+ CLI `--after`；
- **开发者体验**（M8-2）：`--sample N` 采样预览（输入解析处 list 截断、采样模式
  缓存读写禁用、报告 run 级 `sample_rows` + 节点级 `sampled`）；节点行数血缘
  `rows_in`/`rows_out`（`lineage` 开关默认开，关闭即回到 v0.8.0 报告）；`explain`
  子命令输出解析后的最终生效配置（不执行节点、零文件产出）；
- **观测性深化**（M8-3）：Web 详情页甘特式节点执行时间线（纯 CSS 零外部资源）与
  In/Out 行数列、`/compare` 行数差异列；`events` 事件流查询 CLI（过滤 / `--last` /
  `--summary`、损坏行容错）；RunStore nodes 表补 `rows_in`/`rows_out` 列（幂等迁移）；
- **算子库存扩展**（M8-4）：时间 `time.parse`/`time.shift`/`time.now`、结构
  `json.flatten`/`table.explode`、质量 `quality.null_check`/`quality.unique`/
  `quality.schema_check`、编排 `pipeline.run` 共 9 个新算子，全部零第三方依赖，
  算子数 50→59（53 零依赖内置 + 6 可选依赖）。

原则仍是"加法、可选、默认不变"：所有新增参数可选且缺省行为与 v0.8.0 一致，
报告 schema 变更为纯加法（run 级 `sample_rows`、节点级 `sampled`/`rows_in`/`rows_out`），
零第三方依赖与可嵌入定位保持。

---

### 阶段 7：池化执行、报告落地与观测深化（对应版本 0.9.1）— **已完成（v0.9.1，2026-11-20）**

定位：**v0.9 的第二波增量**——算子库存补上压缩/文本哈希/表整形方向，运行参数与
全量报告落地，值级预览与运行记录查询/聚合的观测深化，以及池化进程执行器。四条方向：

- **算子库存扩展（第二波）**（M9-1）：结构 `json.parse`/`json.stringify`、表整形
  `table.fill_null`/`table.replace`/`table.pivot`/`table.unpivot`、文本 `text.hash`、
  压缩 `compress.gzip`/`compress.ungzip`（`compress_ops.py` 新模块，标准库 gzip、
  写文件不落缓存）共 9 个新算子，全部零第三方依赖，算子数 59→68（62 零依赖内置 +
  6 可选依赖）；
- **运行参数与报告落地**（M9-2）：`--param-file PATH`（可重复，与 `--param` 合并、
  命令行覆盖文件值）；`--report-out PATH`（全量报告 JSON 落盘、父目录自动创建、
  写失败只告警不改变运行结果与退出码）；`init --template` 新增 `quality`/`subpipeline`；
- **观测性深化（值级与聚合）**（M9-3）：`--preview N` / `value_preview` 值级预览
  （报告节点附带 `preview_in`/`preview_out`：list 前 N 行 / dict 前 N 键 / str 截
  200 字符，只影响报告不影响执行）；`runs` CLI 子命令（列表/详情，`--db` 缺省
  `runs.db`、库不存在退出码 1、无匹配退出码 0）；RunStore runs 表补 `report_json`
  列（幂等迁移）；Web 列表页 `pipeline=`/`since=`/`until=` 筛选、详情页报告折叠区块、
  `GET /api/runs/<id>/report`、`GET /stats?days=7`（默认 7 上限 90）聚合页；
- **池化进程执行器**（M9-4）：`executor="process_pool"` →
  `PooledProcessExecutor`——N 个常驻 spawn worker 消费任务队列，硬超时 terminate
  对应 worker 并自动补位、崩溃自愈，载荷不可 pickle 时回退线程。

原则仍是"加法、可选、默认不变"：所有新增参数可选且缺省行为与 v0.9.0 一致，
报告 schema 继续纯加法（run 级 `value_preview`、节点级 `preview_in`/`preview_out`），
零第三方依赖与可嵌入定位保持。

---

## 6. 优先级结论

如果只做三件事，按这个顺序：

1. **阶段 1 正确性补丁**——成本最低，且两项属"承诺未兑现"；
2. **`Executor` 抽象 + 进程执行器**——一次解决硬超时、CPU 并行、故障隔离三个天花板；
3. **`RunStore` + SQLite**——从"能跑"升级到"可运维"，断点续跑是用户感知最强的能力。

一句话：**短期做承诺补齐，中期做三个抽象，长期只在外围做平台化，内核永远保持可嵌入。**

库通用性轨道（阶段 1.5）与上述优先级并行：任务粒度小、相互独立，任意时点可插入执行，不被三个抽象阻塞。

---

## 7. 明确不建议的方向

| 不做 | 理由 |
| --- | --- |
| 为支持大数据而重写默认数据模型 | 用 adapter 算子；`list[dict]` 是零依赖与可序列化的根 |
| 在配置里放开 Python 代码执行 | 表达式白名单是核心安全资产 |
| 无真实需求前做流式/微批 | 语义差异太大，等于另起项目 |
| 把 Web UI、权限、多租户做进内核 | 会摧毁"可嵌入库"的定位 |
| 内核引入强依赖（DB、消息队列、Web 框架） | 信任边界与部署成本立即恶化 |

任何会侵蚀"零依赖、语义明确、算子可单测"三点的演进都应被否决。

---

## 8. 文档关系

- 任务级执行计划（编号、改动落点、验收标准）：[开发计划](../design/development-plan.md)
- 现存技术债清单（T1–T15，与阶段 1/2 任务存在映射）：[扩展设计 §5](../design/extension-points.md#5-已知技术债与实现缺口)
- 各阶段语义改动需要同步的手册：[运行时语义](runtime-semantics.md)、[配置规范](../reference/config-schema.md)
