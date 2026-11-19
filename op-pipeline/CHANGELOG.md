# 变更日志

本文档记录 op-pipeline 的重要变更。格式参考 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)，
版本号遵循[语义化版本](https://semver.org/lang/zh-CN/)。

## [Unreleased]

## [0.8.0] - 2026-11-19

### Added

- 远程执行可靠性（M7-1）：`QueueWorkerExecutor` 新增 `lease_seconds`/`max_attempts`，worker
  以 `claims/<job_id>.json` 原子认领、心跳线程定期续租，空闲时扫描过期租约并按 attempt
  重投或写 error；CLI `worker` 新增 `--lease-seconds`/`--max-attempts`/`--owner`，并新增
  `queue compact` 归档已处理条目、清空活跃队列。
- 配置体验与脚手架（M7-2）：新增 `validate --json`、`init --template minimal|data|scheduler`
  与 `scaffold op` 三个 CLI 入口；配置字符串参数支持解析期 `${env:NAME}` /
  `${env:NAME:-default}` 替换，缺失且无默认值时抛出 `SpecValidationError`。
- 运维外围（M7-3）：Web UI 新增运行对比页 `/compare`、只读 JSON API `/api/runs` 与
  `/api/runs/<id>`、Prometheus 文本指标 `/metrics`；`run_scheduler` 新增 `alert_silence`
  （CLI `--alert-silence`），同一 pipeline 在窗口内失败只告警一次。
- 数据与缓存治理（M7-4）：`SpillHandleStore(cleanup="on_run_success")` 在运行成功后清理本次
  run 的落盘目录；`CacheStore(max_entries/max_bytes)` 按 mtime 淘汰最旧条目并在命中时刷新，
  同时提供同 key 进程内单飞锁；CLI `run` 新增 `--cache-max-entries`/`--cache-max-bytes`/
  `--spill-warn-bytes`，运行结束超阈值写入中文告警。

### Changed

- 运行报告 `params` 对敏感参数打码：键名匹配 `TOKEN|SECRET|PASSWORD|KEY`（不区分大小写）
  或值来自敏感环境变量时显示 `***`，执行期真实值不受影响。
- 兼容性：新增参数全部可选、默认行为与 v0.7.0 一致；RunStore schema 无变化，
  算子总数仍为 50（无新增算子）。

## [0.7.0] - 2026-11-19

### Added

- CI 与发布工程（M6-1）：新增 GitHub Actions 工作流（Python 3.11/3.12/3.13 矩阵），
  分「最小依赖档」与「全 extras 档」两条流水线；最小依赖档必须全绿，全 extras 档用于验证可选算子。
- CHANGELOG 与发布校验（M6-1）：补充本文件；`dev` extras 增加 `build`，wheel 内容与
  临时目录安装运行纳入发布前校验。
- 调度与运维（M6-2）：`RunStore` 节点表新增 `duration` 列（旧库自动补列）与 `prune`
  历史清理；调度器支持多流水线、失败告警 webhook 与运行保留策略；Web UI 支持分页、
  状态筛选与节点耗时展示。
- 执行与可靠性（M6-3）：节点并发分组限流（`concurrency_group`）、按异常类型重试
  （`retry_on`）、worker 并发消费（`--workers`）与远程节点取消传播。
- 文档与开发者体验（M6-4）：新增算子清单与注册表一致性的文档测试；新增快速上手、
  运维指南与稳定 API 清单文档；配置规范补充 `concurrency_group`/`retry_on`，
  运行时语义补充并发分组、条件重试、远程取消三节。

### Fixed

- 未知配置键的近义提示排序：`retry` 现在优先提示最短近义键 `retries`，
  不再被同前缀的 `retry_on` 抢占（前缀候选 + difflib 候选按长度、比率排序）。

## [0.6.0] - 2026-11-18

### Added

- 远程执行正式落地：`executor` 参数接受 `str` / `Executor` 实例 / `None`，节点任务打包为
  可 pickle 载荷，上游句柄经共享目录 `SharedHandleStore` 取用、输出原子发布；队列执行器
  接入引擎主循环（18 节点双进程 e2e 与线程执行器结果一致）。
- 独立外围子包 `scheduler_app/`：调度守护进程按间隔触发 `run` 并写入 `RunStore`，
  轻量 Web UI 展示运行历史与单次运行详情（内核零引用，有测试断言）。
- 数据生态补 MySQL/PostgreSQL：新增 `db.read_mysql` / `db.write_mysql` /
  `db.read_postgres` / `db.write_postgres` 四个算子（extras `mysql` / `postgres`，延迟导入），
  算子数 46 → 50。

### Changed

- `CacheStore.include_source` 默认改为 `True`：确定性算子源码变更后缓存自动失效。

### Fixed

- CLI 运行报告展示失败节点的截断堆栈；`ops --json` 输出契约完整性有测试核对。

## [0.5.0] - 2026-11-18

### Added

- 插件信任：`load_plugins` 新增 `allowlist` 白名单与 `strict` 开关，并提供基于
  `OP_PLUGINS.sha256` 清单的 `verify_manifest` 哈希校验。
- 数据生态（Arrow/Parquet）：新增 `df.from_arrow` / `df.to_arrow` /
  `io.read_parquet` / `io.write_parquet`（extras `parquet`）。
- 数据生态（Excel）：新增 `io.read_excel` / `io.write_excel`（extras `excel`，支持多 sheet）。
- 远程执行示范：`QueueWorkerExecutor` 与 `op_pipeline worker` 子命令，实现队列消费与
  双进程端到端示例（含异常回写）。

### Changed

- 算子数 40 → 46；内核保持零第三方依赖（新模块均延迟导入）。

## [0.4.0] - 2026-11-18

### Added

- HTTP 算子 `http.request`（仅标准库 `urllib`，GET/POST、headers/timeout、json/text 双视图）。
- SQLite 算子 `db.sqlite_exec` / `db.sqlite_query`（标准库 `sqlite3`）。
- DataFrame 适配：`df.from_table` / `df.to_table` / `df.to_csv`（extras `dataframe`，延迟导入）。
- 插件入口点发现（`[project.entry-points."op_pipeline.ops"]`）。
- 配置版本化：顶层 `spec_version` 与迁移器框架 `MIGRATIONS`。
- 观测性：`JsonlHook` 与可选 `OtelHook`（extras `otel`）。
- 配置组合：顶层 `includes` 合并（含环检测与重复节点 id 报错）。
- 自定义配置格式：`PipelineSpec.register_loader(suffix, fn)`。

### Changed

- 表算子改为全表列集合并集（消除「首行代表全表列」假设），列齐整的表行为不变。
- 算子数达到 40（DoD 要求 ≥ 40）；全量回归 160 用例全绿，三示例端到端通过。

## [0.3.0] - 2026-11-17

### Added

- 端口软类型告警接入 `report.warnings`。
- `NodeResult.traceback`（截断约 2000 字符）保留失败堆栈。
- `run_id` 追加 4 位随机十六进制后缀，避免同秒并发共用产物目录。
- `defaults` 真正生效：节点未显式设置时继承 `retries`/`retry_delay`/`timeout`/`cache`/
  `allow_failure` 等策略缺省（节点显式值优先）。
- `run(plugin_modules=...)` 支持模块插件；文件插件模块名按路径 blake2b 摘要去重。

### Changed

- 相对 `cache_dir` 改以 `workdir` 为基准解析（不再依赖进程 CWD）。
- CLI `--dry-run` 提前到创建 `RunContext`/缓存之前，不再产生空产物目录。

### Removed

- 破坏性删除：`PlanNode.resolved_inputs` 残留字段、`ModuleExecutionError` 异常类及其导出、
  `to_mermaid` 死变量（均无内部调用方）。

## [0.2.1] - 2026-11-17

### Added

- 独立注册表注入：`load_plugins(..., registry=...)` 与 `run(..., registry=...)`，多流水线算子集隔离。
- `op_pipeline/py.typed` 与完整打包元数据（license/classifiers/package-data）。
- 未知配置键告警（含近义键提示）。
- `expression.register_function(name, fn)` 表达式扩展钩子（纯函数约束、禁覆盖内置名）。
- 取消机制：`Engine.cancel(reason)` 与 `run_pipeline(..., cancel_event=...)`。
- 库嵌入综合示例 `examples/embedded_lib_demo.py`。

### Changed

- `artifacts_dir` 惰性创建，纯内存运行不再在 workdir 留下隐藏目录。
- `when` 环境中的 `params` 改为解析后的值，原值保留为 `raw_params`。
- 注册表注册与插件加载加锁，支持单进程多线程并发 `run()`。

### Fixed

- 修复空注册表被 `or` 短路进全局注册表的问题。

## [0.2.0] - 2026-11-17

### Changed

- 该版本为 M1「正确性与承诺补齐」的计划版本；因包含破坏性变更（移除 `ModuleExecutionError`
  导出、相对 `cache_dir` 改按 workdir 解析），实际以 `0.3.0` 发布，内容见 [0.3.0]。
