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
| `--pipeline` | 流水线配置文件路径（必填）；**v0.7 起可重复**，每轮按给定顺序依次触发全部流水线 |
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

---

## 3. RunStore 运维

RunStore 是**可选**的 SQLite 运行记录（`--db` / `--runs` / `run_store=...`），
不启用时框架完全不触碰 SQLite。

### 3.1 表结构与耗时

| 表 | 关键字段 |
| --- | --- |
| `runs` | `run_id`(PK)、`pipeline`、`version`、`status`、`started_at`、`finished_at`、`params_json` |
| `nodes` | `run_id`+`node_id`(PK)、`op`、`status`、`attempts`、`cached`、`error`、`cache_key`、`finished_at`、`duration`（v0.7 新增） |

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

---

## 5. 故障排查清单

| 症状 | 排查方向 | 处理 |
| --- | --- | --- |
| 远程节点长时间 `running` / 最终超时 | 队列文件在涨但没人消费：worker 未启动、路径不是同一共享目录、worker 导入算子失败 | 确认 `jobs.jsonl` 与 `results/` 路径两边一致；启动 worker 并看其日志；`--count` 用完后 worker 会退出，常驻请去掉 `--count` |
| 结果文件一直不出现 | worker 异常退出；队列行损坏；跨机器文件系统未同步 | 逐个 job 检查 `results/<job_id>.json`；重新启动 worker（按 offset 增量消费，可续跑）；检查共享盘挂载 |
| 改了算子在跑还是旧结果 | 命中缓存；缓存目录不是预期位置 | 用 `--no-cache` 复跑对比；缓存目录默认相对**工作目录**，不是进程 CWD；算子源码变更默认会自动失效；有副作用的算子需 `deterministic = False` |
| `database is locked`（SQLite） | 多个写者同时写 `runs.db`；外部脚本长事务 | 调度器本身串行写；Web UI 只读；备份/清理放在低峰且避开写入；必要时缩短外部脚本事务 |
| 文件被占用 / 无法覆盖输出 | 上一次超时线程仍在写文件；Excel/Parquet 句柄未释放；下游读取中 | 软超时不会强杀线程，对可能卡死的算子用 `executor="process"`（硬超时）；输出路径按 `run_id` 隔离；确保读取方先 `close()` |
| `nodes.duration` 全是 0 | 旧库尚未补列，或运行的是 v0.6 及更早版本 | 用 v0.7 打开旧库会自动补列；新产生的运行才有耗时 |
| Web UI 页面 404 | `run_id` 不存在或库路径不是同一个 | 核对 `create_server(db_path)` 指向的库；列表页确认 run_id |
| 调度器单轮失败但进程还在 | 设计如此：单轮异常被隔离 | 查 `ERROR` 日志与 RunStore `runs.status/error`；需要外部通知时配 `--alert-webhook` |

---

## 6. 相关文档

- [快速上手](quickstart.md)：安装、首个流水线、自定义算子、常见配置错误；
- [API 参考](../reference/api.md)：`RunStore` / `run_pipeline` / `QueueWorkerExecutor` 等公开 API；
- [配置规范](../reference/config-schema.md)：节点运行策略与 CLI 选项；
- [运行时语义](../architecture/runtime-semantics.md)：远程执行、取消、重试与缓存的完整语义。
