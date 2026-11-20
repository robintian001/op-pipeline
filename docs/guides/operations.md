# 运维指南

本文面向"把 op_pipeline 跑在服务器/定时任务里"的使用者，覆盖外围服务
`scheduler_app`（调度守护进程 + Web UI）、RunStore 运行记录、远程执行 worker
与常见故障排查。

`scheduler_app` 是仓库里的**独立子包**（纯标准库），只调用 `op_pipeline` 的公开 API；
内核不引用它，因此"零依赖、零守护进程"的库定位不变。

---

## 1. 定时调度：`scheduler_app.scheduler`

```powershell
python -m scheduler_app.scheduler --pipeline examples/pipeline_orders.json --every 60 --db runs.db
```

行为（与源码一致）：

- 启动后**立即执行一次**，之后每 `--every` 秒执行一轮；间隔从上一轮**结束**开始计时；
- 每轮调用 `op_pipeline.run(...)`，`--db` 给定时把本次运行与节点记录写入 RunStore；
- 单轮失败只记 `ERROR` 日志，**不中断**调度循环；`Ctrl+C` 干净退出（退出码 0）；
- 调度循环在主线程串行执行，同一时刻只有一轮流水线。

参数：

| 参数 | 说明 |
| --- | --- |
| `--pipeline` | 流水线配置文件路径（必填）；**v0.7 起可重复**，每轮按给定顺序依次触发全部流水线；**v0.9 起条目支持 `{"path": ..., "after": [...]}`**（见 §1.5） |
| `--after` | **v0.9 新增**：`NAME=dep1,dep2`（可重复）——给名为 `NAME` 的流水线（按配置文件名去扩展名匹配）追加依赖，与 dict 条目声明的 `after` 合并（CLI 追加在后） |
| `--every` | 触发间隔秒数（必填，必须为正数） |
| `--db` | RunStore SQLite 路径；不传则不落库（零副作用） |
| `--plugin` | 插件**文件路径**，可重复；相对配置文件所在目录解析 |
| `--registry` | 插件**模块路径**，可重复（对应 `run(plugin_modules=...)`） |
| `--workdir` | 工作目录（默认配置文件所在目录），相对路径基准 |
| `--cache-dir` | 节点缓存目录（默认 `.pipeline_cache`，相对工作目录） |

### 1.1 多流水线（v0.7）

```powershell
python -m scheduler_app.scheduler `
  --pipeline examples/pipeline_orders.json `
  --pipeline examples/pipeline_text.toml `
  --every 300 --db runs.db
```

- **v0.7 起支持** `--pipeline` 重复：`run_scheduler` 的 `pipeline` 参数同时接受
  单个路径或路径序列；每轮按顺序触发全部流水线；
- 兼容性：只传一个 `--pipeline` 时行为与旧版完全一致；
- 程序化调用：`run_scheduler(["a.json", "b.toml"], every=300, db="runs.db")`。

### 1.2 失败告警：`--alert-webhook`（v0.7）

```powershell
python -m scheduler_app.scheduler --pipeline pipeline.json --every 60 --db runs.db `
  --alert-webhook https://example.internal/hooks/pipeline-alert
```

- **v0.7 起支持**：某次运行状态**非 `success`**（`failed` / `partial`），或本轮直接
  抛异常时，向该 URL POST JSON：

  ```json
  {"pipeline": "examples/pipeline_orders.json", "run_id": "20260719-101500-ab12", "status": "failed", "error": "ValueError: ..."}
  ```

  载荷字段固定为 `pipeline`（配置文件路径原文）、`run_id`（未跑起来时为空串）、
  `status`（报告状态，异常路径为 `"error"`）、`error`（首个失败节点错误或异常摘要）。

- 使用标准库 `urllib.request`，超时 5 秒；告警请求失败只记 `WARNING`，
  **不影响后续调度**，也不会改变退出码；
- 需要接收端自行鉴权/去重；本框架不做重试队列。

### 1.3 运行记录保留：`--keep-runs`（v0.7）

```powershell
python -m scheduler_app.scheduler --pipeline pipeline.json --every 60 --db runs.db --keep-runs 100
```

- **v0.7 起支持**（需同时给 `--db`，无 RunStore 时不做清理）：每轮结束后调用
  `RunStore.prune(N)`，只保留最近 N 条运行
  （排序 `started_at DESC, run_id DESC`，同一时间戳用 `run_id` 兜底保证确定性）及其节点记录；
- `N <= 0` 视为**不清理**（调度器侧跳过，不调用 `prune`）；
- 不传该参数时不做任何清理，历史记录只增不减。

### 1.4 告警静默：`--alert-silence`（v0.8）

```powershell
python -m scheduler_app.scheduler --pipeline pipeline.json --every 60 --db runs.db `
  --alert-webhook https://example.internal/hooks/pipeline-alert `
  --alert-silence 1800
```

- 同一 pipeline 距**上次实际发送**告警不足 `--alert-silence` 秒时跳过本次告警（记 info 日志）；
- 不传或 `<= 0` 时行为与 v0.7 一致（每次失败都发）；
- 静默记录只存在调度进程内存中，重启后重置；程序化调用为
  `run_scheduler(..., alert_webhook=..., alert_silence=1800)`。

### 1.5 依赖触发：`after` 声明与 `--after`（v0.9）

```powershell
python -m scheduler_app.scheduler `
  --pipeline examples/etl_load.json `
  --pipeline examples/etl_report.json `
  --after etl_report=etl_load `
  --every 300 --db runs.db
```

- pipeline 名 = 配置文件去扩展名的 stem（`etl_load.json` → `etl_load`）；
  条目可用 `--pipeline '{"path": "etl_report.json", "after": ["etl_load"]}'`
  直接声明依赖，也可用 `--after NAME=dep1,dep2`（可重复）追加；
- **触发规则**：每轮按拓扑顺序触发——依赖**本轮已运行且全部 `success`** 才执行，
  否则跳过本轮并记 info 日志（下游连锁跳过）；依赖本轮 `failed` 只影响本轮，
  下一轮按各自实际结果重新判定；
- **启动期校验**：依赖名不在当前流水线集合记 warning（不 fail）；`after` 成环
  启动即抛 `ConfigError`；
- 兼容性：不使用 `after` 的条目每轮照常触发，行为与 v0.8.0 完全一致；
  完整触发语义见 [运行时语义 §10](../architecture/runtime-semantics.md)。

---

## 2. Web UI：`scheduler_app.web`

Web UI 用标准库 `http.server` 读取 RunStore SQLite，**没有 `__main__` CLI 入口**。
以 `create_server()` 编程方式启动（绑定 `127.0.0.1`）：

```powershell
python -c "from scheduler_app import create_server; create_server('runs.db', 8000).serve_forever()"
```

- `create_server(db_path, port=0)` 返回**已绑定但未启动**的 `ThreadingHTTPServer`；
  `port=0` 由系统分配空闲端口，实际端口见 `server.server_address[1]`；
- 路由：`/` 为运行历史列表，`/run/<run_id>` 为单次运行详情；未知路径/记录返回 404；
- 每次请求独立打开 SQLite 连接（`sqlite3` 有线程亲和性），只读查询；
- **无认证、只监听本机**：需要对外暴露时请放在带鉴权的反向代理之后。

v0.7 起的能力（M6 Task 7）：

| 能力 | 用法 |
| --- | --- |
| 分页 | `/?page=2&size=20`（默认 `page=1`、`size=50`，非法参数回落默认；底部有上一页/下一页） |
| 状态筛选 | `/?status=success`（可选 `success` / `failed` / `partial` / `skipped`，SQL 参数化） |
| 节点耗时 | 详情页节点表新增"耗时"列（`nodes.duration`，1 位小数 + `s`），头部保留总耗时 |

旧库没有 `nodes.duration` 列时页面照常渲染，不会报错；该列由 v0.7 的 RunStore
在建表/补列时写入，新产生的运行才有耗时可展示。

v0.9 起的能力（M8-3）：

| 能力 | 说明 |
| --- | --- |
| 节点执行时间线 | 详情页新增甘特式时间线（纯 CSS，零外部资源）：每节点一行，条形位置 = `finished_at − duration` 相对运行开始的偏移，条长 ∝ 耗时；顶部 0%–100% 相对刻度；绿=success、红=failed、灰=缓存命中、淡灰=skipped；旧行缺 `finished_at` 自动跳过，全部异常数据都不会让页面报错 |
| In/Out 行数列 | 详情页节点表新增 In / Out 两列（`端口=行数` 紧凑展示，端口名排序）：来自 v0.9 的 `nodes.rows_in` / `rows_out` 血缘列，旧行 NULL 显示为空 |

v0.9.1 起的能力（M9）：

| 能力 | 说明 |
| --- | --- |
| 列表页筛选 | 列表页与 `/api/runs` 支持 `/?pipeline=xx&since=YYYY-MM-DD&until=YYYY-MM-DD`：按流水线名称与时间段筛选 |
| 详情页报告折叠区块 | 详情页新增报告 JSON 折叠区块（默认收起，可展开/复制），来自 v0.9.1 的 `runs.report_json` 列 |
| 聚合统计页 | `GET /stats?days=7`（默认 7 天、上限 90）：按天聚合运行状态/耗时统计 |

### 2.1 运行对比页：`/compare`（v0.8）

```text
GET /compare?a=<run_id>&b=<run_id>
```

- 缺任一参数返回 **400** 纯文本；run 不存在返回 **404** 并列出缺失的 run_id；
- 页面展示两次运行的状态、总耗时，以及节点级并集差异：仅一侧存在的节点标注"仅 A/仅 B"，
  共有节点展示各自耗时与差值（`+` / `-`）；
- **v0.9 新增行数差（A−B）列**：基于两侧节点 `rows_out` 各端口求和之差，
  `+` / `-` 标注变化量，仅一侧存在时同样标注"仅 A/仅 B"；
- `nodes` 表没有 `resumed` 列，因此对比页只按 `cached` 展示「缓存/—」；
  旧库没有 `rows_in`/`rows_out` 列时行数差展示为空。

### 2.2 只读 JSON API 与指标（v0.8）

| 路径 | 返回 |
| --- | --- |
| `GET /api/runs?page=1&size=20&status=failed` | `{"items": [...], "total": N, "page": 1, "size": 20}`；分页/筛选语义与列表页一致（**v0.9.1 起支持 `pipeline=` / `since=` / `until=` 筛选**） |
| `GET /api/runs/<run_id>` | `{"run": {...}, "nodes": [...]}`；run 不存在返回 404 JSON `{"error": "..."}` |
| `GET /api/runs/<run_id>/report` | **v0.9.1 新增**：该 run 的全量报告 JSON（取自 `runs.report_json` 列；旧行/未落库返回 404） |
| `GET /stats?days=7` | **v0.9.1 新增**：按天聚合的统计页/JSON（默认 7 天、上限 90，非法参数回落默认） |
| `GET /metrics` | Prometheus 文本，`Content-Type: text/plain; version=0.0.4; charset=utf-8` |

```bash
curl -s 'http://127.0.0.1:8000/api/runs?status=failed&size=5'
curl -s 'http://127.0.0.1:8000/api/runs/20260719-101500-ab12'
curl -s 'http://127.0.0.1:8000/metrics'
```

`/metrics` 输出片段：

```text
# HELP op_pipeline_runs_total 按状态统计的运行次数
# TYPE op_pipeline_runs_total counter
op_pipeline_runs_total{status="success"} 12
op_pipeline_runs_total{status="failed"} 2
op_pipeline_runs_total{status="partial"} 1
op_pipeline_runs_total{status="skipped"} 0
# HELP op_pipeline_runs_duration_seconds 全部运行耗时总和（秒）
# TYPE op_pipeline_runs_duration_seconds counter
op_pipeline_runs_duration_seconds 183.420000
```

四个状态始终输出（无记录为 0）；耗时取 `SUM(finished_at - started_at)`。

---

## 3. RunStore 运维

RunStore 是**可选**的 SQLite 运行记录（`--db` / `--runs` / `run_store=...`），
不启用时框架完全不触碰 SQLite。

### 3.1 表结构与耗时

| 表 | 关键字段 |
| --- | --- |
| `runs` | `run_id`(PK)、`pipeline`、`version`、`status`、`started_at`、`finished_at`、`params_json`、`report_json`（v0.9.1 新增，全量报告 JSON） |
| `nodes` | `run_id`+`node_id`(PK)、`op`、`status`、`attempts`、`cached`、`error`、`cache_key`、`finished_at`、`duration`（v0.7 新增）、`rows_in`/`rows_out`（v0.9 新增，JSON 文本；未采集/关闭血缘写 NULL） |

- `nodes.duration` 为单节点执行耗时（秒，`REAL DEFAULT 0`）；**v0.7 起**写入，
  供 Web UI 详情页展示与事后分析；
- 旧库在首次打开/写入时用 `PRAGMA table_info(nodes)` 检测并
  `ALTER TABLE ... ADD COLUMN` 补列，幂等，不需要手工迁移；
- 既有 `query(sql)`、`prev_node(run_id, node_id)` 调用不受影响；断点续跑
  （`--resume`）仍按 `cache_key` 比对。

### 3.2 历史清理：`RunStore.prune(keep_last)`

```python
from op_pipeline import RunStore

with RunStore("runs.db") as store:
    removed = store.prune(100)   # 只保留最近 100 条运行
    print("删除运行数：", removed)
```

- **v0.7 起**提供：按 `started_at DESC` 保留最近 `keep_last` 条，
  删除更早的 `runs` 行**及其 `nodes` 行**，返回删除的运行数；
- `keep_last <= 0` 抛 `ValueError`（调度器 `--keep-runs N <= 0` 会先跳过，不触发异常）；
- 清理只动运行记录，不删除流水线产物（`.op_pipeline/artifacts/`）与缓存
  （`.pipeline_cache/`），这两者需另行清理。

### 3.3 备份与清理建议

- **备份**：SQLite 是单文件数据库。低峰期停止调度器与 Web UI 的写入后直接复制
  `runs.db`；或在有 `sqlite3` CLI 的环境执行
  `sqlite3 runs.db ".backup 'runs-backup.db'"`；
- **清理**：优先用 `prune(N)` 保留窗口；数据库体积仍大时，在停止写入后执行
  `VACUUM` 回收空间；不要手工 `DELETE` 一半 `nodes` 而行未删，否则详情页节点数会缺；
- **磁盘句柄**：调度器复用同一连接（主线程串行）；Web UI 每请求独立连接并在
  `finally` 关闭；外部脚本读库时请用 `with RunStore(...)` 或显式 `close()`；
- **归档**：需要长期保留时，把旧 `runs.db` 改名归档，再让调度器新建空库即可。

### 3.4 常用查询

```python
rows = store.query(
    "SELECT run_id, pipeline, status, started_at FROM runs ORDER BY started_at DESC LIMIT 20"
)
slow = store.query(
    "SELECT node_id, op, duration FROM nodes WHERE run_id = ? ORDER BY duration DESC",
    ("20260719-101500-ab12",),
)
```

### 3.5 `runs` 子命令：CLI 查询运行记录（v0.9.1）

```powershell
python -m op_pipeline runs                     # 最近运行记录（缺省库 runs.db）
python -m op_pipeline runs --status failed --pipeline etl --last 10 --json
python -m op_pipeline runs 20261120-090000-ab12   # 单条详情
```

- 列表模式：`--db <path>`（缺省 `runs.db`）、`--status s`（按状态筛选）、
  `--pipeline p`（按流水线筛选）、`--last N`（只取最近 N 条）、`--json`（JSON 输出），
  参数可组合；缺省库文件不存在/无法打开退出码 1，正常查询（含无匹配）退出码 0；
- 直接传 `run_id` 进入详情模式：展示该次运行的状态、耗时与节点明细
  （含缓存命中、血缘行数、预览字段）。

---

## 4. 远程执行 worker

`QueueWorkerExecutor` 把节点任务写进共享 JSONL 队列，由**独立 worker 进程**消费。
两进程之间只交换文件（队列 + 结果目录），适合"另起 worker / 跨机器共享盘"。

### 4.1 worker 命令

```powershell
python -m op_pipeline worker --queue jobs.jsonl --out results
python -m op_pipeline worker --queue jobs.jsonl --out results --count 8 --poll 0.1
```

| 参数 | 说明 |
| --- | --- |
| `--queue` | JSONL 任务队列文件路径（必填） |
| `--out` | 结果目录；缺省为队列同目录下的 `results/` |
| `--count N` | 最多消费 N 个任务后正常退出；不传则持续监听直到 `Ctrl+C`（exit 130） |
| `--poll` | 队列轮询间隔秒数（默认 0.1） |
| `--workers N` | **v0.7 起支持**：并发执行任务的 worker 线程数（默认 1；`N <= 0` 报错）。`--count` 的语义是"最多认领的任务数" |
| `--lease-seconds S` | **v0.8 起支持**：任务租约秒数（默认 0 = 关闭租约）；队列行内值优先 |
| `--max-attempts N` | **v0.8 起支持**：租约过期后的最大 attempt（默认 3）；队列行内值优先 |
| `--owner ID` | **v0.8 起支持**：租约持有者标识（默认 `<hostname>-<pid>`） |

结果按 `job_id` 独立写成 `<job_id>.json`（先写 `.tmp` 再原子改名），并发消费时
不同任务的结果文件互不覆盖。

### 4.2 与引擎配合

引擎侧用运行级 `--executor queue`（或 `executor="queue"`）时，队列与结果目录固定为
`<workdir>/.op_pipeline/remote/`：

```powershell
# 终端 A：引擎提交远程节点（队列不存在会自动创建）
python -m op_pipeline run pipeline.json --executor queue

# 终端 B：worker 常驻消费
python -m op_pipeline worker `
  --queue .op_pipeline/remote/jobs.jsonl `
  --out .op_pipeline/remote/results
```

worker 通过 `default_registry` 按算子名实例化：**内置算子天然可见**；自定义算子需
随部署代码可导入，或做成安装后的插件包（入口点自动发现）。仅放在某个
`--plugin` 文件里的算子，worker 侧看不到，会以"未注册的算子"失败。

### 4.3 队列压缩：`queue compact`（v0.8）

```powershell
python -m op_pipeline queue compact `
  --queue .op_pipeline/remote/jobs.jsonl `
  --out .op_pipeline/remote/results
```

- **前置条件**：队列中**全部** job_id 都已有 `<out>/<job_id>.json` 结果文件；
- 满足时把原始队列行追加进归档文件（默认 `<队列 stem>.archive.jsonl`，可用
  `--archive <file>` 指定），再用同目录临时文件 + `os.replace` 原子清空活跃队列；
- 存在未完成任务或无法解析的行时**拒绝改动任何文件**，打印未完成 job_id 并返回非 0；
- 建议在 worker 停机（或确认队列不再被追加）后执行；压缩不删除 `results/` 与 `claims/`。

---

## 5. 只读观测工具：`explain` 与 `events`（v0.9）

### 5.1 解析预览：`explain`

```powershell
python -m op_pipeline explain examples/pipeline_orders.json
python -m op_pipeline explain examples/pipeline_subpipeline.json --param LIMIT=3 --json
python -m op_pipeline explain pipeline.json --node clean
```

- **只读**：执行完整加载链（includes 合并 → defaults 合并 → `${env:}` /
  `${param:}` 替换 → 编译 `build_plan`），但**不执行任何节点、零文件产出**
  （不建 RunContext、不建缓存目录）——适合发布前核对参数注入结果；
- 默认输出**最终生效配置**：`params`（敏感值打码）与每节点的 op / 输入 /
  参数摘要——`${p.x}` 参数引用尽力就地解析，句柄引用保持原样；
  `--node <id>` 只看单个节点（不存在报错退出码 1）；`--json` 输出
  `{pipeline, version, params, nodes, warnings}`；
- `--param KEY=VALUE` 与 `run` 语义一致（可重复、后者覆盖、值先尝试 JSON 解析），
  配置非法（缺参/语法错/编译错）以退出码 1 打印中文问题列表。

### 5.2 事件流查询：`events`

```powershell
python -m op_pipeline events .op_pipeline/events/20260101-120000-ab12.jsonl
python -m op_pipeline events events.jsonl --run-id 20260101-120000-ab12 --node clean
python -m op_pipeline events events.jsonl --event node_finish --last 10
python -m op_pipeline events events.jsonl --summary
```

- 输入为 JSONL 事件流文件（每行一个事件 dict，含 `event` 字段）；
- 过滤参数可组合：`--run-id` / `--node` / `--event`（`node_start` / `node_finish` /
  `run_finish`）；`--last N` 只保留末尾 N 条（`N < 0` 报错退出码 2）；
- `--summary` 输出按节点聚合表格（执行次数 / 总耗时 / 失败数，取自
  `node_finish` 事件），与明细输出互斥；
- **容错**：损坏行（非 JSON / 非 dict / 缺 `event` 字段）记 warn 后跳过，
  不中断；文件不存在/无法读取退出码 1，正常（含空文件、过滤后无匹配）退出码 0。

### 5.3 运行辅助：`--param-file` / `--report-out` / `--preview`（v0.9.1）

```powershell
python -m op_pipeline run pipeline.json --param LIMIT=3 --param-file params.json
python -m op_pipeline run pipeline.json --report-out reports/last.json
python -m op_pipeline run pipeline.json --preview 5
```

- `--param-file <path>`（**可重复**）：从 JSON 文件注入运行参数，与 `--param KEY=VALUE`
  合并（文件值并入 `[params]` 段，命令行 `--param` 优先级更高、覆盖文件里的同名键）；
- `--report-out <path>`：把**全量报告 JSON** 落盘到指定文件（父目录不存在自动创建）；
  报告写盘失败只记告警，**不改变运行结果与退出码**；
- `--preview N`：值级预览——报告节点附带 `preview_in` / `preview_out`：
  list 取前 N 行、dict 取前 N 键、str 截前 200 字符；**只影响报告，不影响执行**
  （节点真实结果、缓存与下游都不受影响）。

---

## 6. 故障排查清单

| 症状 | 排查方向 | 处理 |
| --- | --- | --- |
| 远程节点长时间 `running` / 最终超时 | 队列文件在涨但没人消费：worker 未启动、路径不是同一共享目录、worker 导入算子失败 | 确认 `jobs.jsonl` 与 `results/` 路径两边一致；启动 worker 并看其日志；`--count` 用完后 worker 会退出，常驻请去掉 `--count` |
| 结果文件一直不出现 | worker 异常退出；队列行损坏；跨机器文件系统未同步 | 逐个 job 检查 `results/<job_id>.json`；重新启动 worker（按 offset 增量消费，可续跑）；检查共享盘挂载 |
| 改了算子在跑还是旧结果 | 命中缓存；缓存目录不是预期位置 | 用 `--no-cache` 复跑对比；缓存目录默认相对**工作目录**，不是进程 CWD；算子源码变更默认会自动失效；有副作用的算子需 `deterministic = False` |
| `database is locked`（SQLite） | 多个写者同时写 `runs.db`；外部脚本长事务 | 调度器本身串行写；Web UI 只读；备份/清理放在低峰且避开写入；必要时缩短外部脚本事务 |
| 文件被占用 / 无法覆盖输出 | 上一次超时线程仍在写文件；Excel/Parquet 句柄未释放；下游读取中 | 软超时不会强杀线程，对可能卡死的算子用 `executor="process"`（硬超时）；输出路径按 `run_id` 隔离；确保读取方先 `close()` |
| `nodes.duration` 全是 0 | 旧库尚未补列，或运行的是 v0.6 及更早版本 | 用 v0.7 打开旧库会自动补列；新产生的运行才有耗时 |
| Web UI 页面 404 | `run_id` 不存在或库路径不是同一个 | 核对 `create_server(db_path)` 指向的库；列表页确认 run_id |
| 同一任务被执行两次 | 租约过期后重投，原 worker 可能仍存活 | 远程算子应保持幂等；用 `--owner` 区分实例，必要时核对 `results/<job_id>.json` 的写入时间 |
| `queue compact` 拒绝执行 | 队列仍有未完成或不可解析的任务 | 按提示的 job_id 查 `results/`；确认 worker 停机且任务全部完成后重试 |
| 调度器单轮失败但进程还在 | 设计如此：单轮异常被隔离 | 查 `ERROR` 日志与 RunStore `runs.status/error`；需要外部通知时配 `--alert-webhook` |

---

## 7. 相关文档

- [快速上手](quickstart.md)：安装、首个流水线、自定义算子、常见配置错误；
- [API 参考](../reference/api.md)：`RunStore` / `run_pipeline` / `QueueWorkerExecutor` 等公开 API；
- [配置规范](../reference/config-schema.md)：节点运行策略与 CLI 选项；
- [运行时语义](../architecture/runtime-semantics.md)：远程执行、取消、重试与缓存的完整语义。
