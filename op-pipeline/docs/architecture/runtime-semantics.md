# 运行时语义设计

本文回答一个问题：**一份配置从文本到结果，中间到底发生了什么，哪些行为是被写死的。**

整体是严格的两阶段模型：

```
阶段 A：编译（纯静态，可离线校验）
  配置文本 ──► PipelineSpec ──► build_plan() ──► Plan（nodes / order / handle_owner / params）

阶段 B：执行（有副作用）
  Plan + RunContext + CacheStore ──► Engine.run() ──► RunReport + handles
```

编译入口见 [build_plan()](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/spec.py#L328-L487)，
执行入口见 [Engine.run()](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/engine.py#L196-L264)。

---

## 1. 阶段 A：编译期的五类校验

`build_plan` 以"收集所有问题再一次抛出"的方式工作：`problems` 列表累积到函数末尾，
若 `strict=True`（默认）则抛 `SpecValidationError`，异常体里带完整清单。这是刻意设计——
配置文件写错时，用户应该一次看到全部错误，而不是修一个报一个。

### 1.1 流水线参数合并与环境变量展开

```
params = spec.params
for k, v in overrides:
    k 未定义 → problems（防止 --param 拼写错误静默失效）
    params[k] = v
params = RefResolver(params, {}, os.environ).resolve(params)   # 展开 ${env.X}
```

源码：[spec.py#L339-L351](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/spec.py#L339-L351)。
注意参数之间**不保证**能互相引用：`RefResolver` 用的是同一个 `params` 字典，因此
`a = "${p.b}"` 只在 `b` 已存在于字典时才能解析成功，不存在则报 `RefResolutionError`。

### 1.2 节点 id 唯一性

统计出现次数，重复即问题项（[spec.py#L353-L359](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/spec.py#L353-L359)）。

### 1.3 算子存在性、参数合法性、句柄归属

对每个节点：

1. `registry.get(node.op)` 失败（含未注册、别名不存在）→ 问题项；
2. 若 `node.params` **不含**任何 `${}` 引用，直接 `cls(**node.params)` 实例化，
   把参数校验前移到编译期；若含引用，则只做"参数名是否声明"的静态检查，
   实例化推迟到运行期（[spec.py#L370-L387](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/spec.py#L370-L387)）；
3. 为每个输出端口计算句柄：`node.outputs.get(port, f"{id}.{port}")`，
   重复占用直接报错（[spec.py#L389-L403](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/spec.py#L389-L403)）。

> 设计要点：句柄归属表是在**遍历所有节点之后**才用于引用检查的，
> 因此配置里节点的书写顺序无关紧要，前向引用（引用写在后面的节点）天然合法。

### 1.4 引用可达性、输入端口完备性、依赖推导

- `RefResolver.static_target()` 递归提取 `inputs` 与 `params` 中所有 `${...}` 路径；
  以 `p.` / `params.` / `env.` 开头的跳过（参数与环境变量不产生依赖）。
- 其余引用用 `match_handle()` 做**最长前缀匹配**（支持 `${node.port.field}` 下钻），
  匹配到的句柄所属节点即成为依赖（[spec.py#L421-L437](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/spec.py#L421-L437)）。
- 自引用即自环，报错并剔除该依赖。
- 算子声明的非 optional 输入端口若未接线，报错
  （[spec.py#L439-L448](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/spec.py#L439-L448)）。

> 依赖是**由引用推导**出来的，配置里没有独立的 `depends_on` 字段。
> 这保证了"图"与"数据流"永远一致，不可能出现声明依赖但没接线的情况。

### 1.5 流水线输出引用与环检测

- `spec.outputs` 里的引用同样做句柄存在性校验；
- 仅当此前没有任何问题时才做 Kahn 拓扑排序（同层按名字排序，保证结果稳定），
  否则环检测的噪音会掩盖真正的配置错误
  （[spec.py#L469-L515](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/spec.py#L469-L515)）。

`Plan` 一旦产出，执行阶段不再回看配置文本。

---

## 2. 阶段 B：执行期的调度算法

### 2.1 状态机

每个节点在整次运行中只会经历一个终态：

```
pending ──┬─► running ──► success ──► 下游可调度
          │              failed  ──► 下游 skipped（除非 allow_failure 仅影响汇总状态）
          │              skipped ←── when 为假 / 上游未成功 / fail_fast 中断
          └─► skipped（依赖无法满足，兜底分支）
```

引擎内部用两个并发字典跟踪：`_results[node_id] = NodeResult`、`_completed[node_id] = status`
（[engine.py#L188-L193](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/engine.py#L188-L193)）。

### 2.2 主循环

```
pending = list(plan.order)      # 拓扑序，仅用于稳定遍历顺序
running = {}
while pending or running:
    progressed = dispatch(pending, running, pool)
    if running:
        done = wait(running, FIRST_COMPLETED)
        for f in done: collect(running.pop(f))
        continue
    if not progressed:
        # 既无可调度节点、也无在跑节点：只可能是环或全部无法满足
        mark_all_pending_skipped("依赖无法满足")
```

源码：[engine.py#L215-L233](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/engine.py#L215-L233)。

### 2.3 dispatch 的决策顺序

对 `pending` 中的每个节点（按拓扑序）依次判断
（[engine.py#L288-L346](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/engine.py#L288-L346)）：

1. **取消已触发**（`cancel_event` 被置位或 `Engine.cancel()` 被调用）→ 全部标记 skipped，
   注明"运行已被取消：\<原因\>"，并向报告 `warnings` 追加一条取消告警；
2. **fail_fast 已触发** → 全部标记 skipped，注明"运行已被中断（fail_fast）"；
3. **依赖未全部完成** → 跳过本轮（不阻塞其他可运行节点）；
4. **依赖中有 failed / skipped** → 本节点 skipped，注明缺失的上游；
5. **`when` 条件**：在调度前求值，为假则 skipped，求值异常也 skipped（不失败）；
6. **并发已满** → `break`（保留剩余节点到下一轮）；
7. 否则 `pool.submit(self._run_node, node)`。

这七步的顺序是有意义的：取消与 fail_fast 的短路必须最先；条件判断必须在上游完成之后、
提交之前，否则 `when` 拿不到输入值；而并发上限放在最后，保证"能跳过的先跳过"，不占线程。

### 2.4 `when` 的求值环境

```
env = {
  <输入端口名>: 解析后的上游值,          # 解析失败则忽略该键
  <参数名>: 解析后的参数值,              # 同上
  "inputs": {已成功解析的输入端口名: 值},
  "params": {已成功解析的参数名: 值},     # 与扁平参数名同语义（M1.5-5 起生效）
  "raw_params": {原始（未解析）的参数表}, # 原值逃生门：仍是 '${p.x}' 串
}
```

源码：[engine.py#L348-L366](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/engine.py#L348-L366)。
求值走 [evaluate_condition()](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/expression.py#L172-L174)，
即 AST 白名单（见 [核心模块设计 §5](../design/core-modules.md#5-expressionpy安全表达式)）。

> `when = "params.n > 0"` 与 `when = "n > 0"` 完全等价；点语法对映射做键下钻
> （`inputs.table.rows` 这类嵌套 dict 逐层下钻），目标不是映射或键不存在即报错——
> 因此 `x.__class__` 之类的属性逃逸天然被挡住。

### 2.5 单节点执行流水线

```
_run_node(node):
  ctx = run_context.for_node(node_id, op)        # 每个节点独立 logger
  handles_snapshot = dict(self.handles)          # 快照，避免跨节点可见性抖动
  resolver = RefResolver(plan.params, snapshot)

  inputs = {port: resolver.resolve(ref) ...}     # 解析 ${...}
  params = resolver.resolve(node.spec.params)
  ctx.params = params                            # 供算子内自省（如 control.assert）
  instance = node.operator or node.operator_cls(**params)

  # 缓存查询（仅当 node.cache && store.enabled && instance.deterministic）
  key = cache.compute_key(op, version, params, inputs)
  if hit: publish(outputs); return success(cached=True, attempts=0)

  outputs = execute_with_retry(...)
  cache.store(key, outputs)
  publish(node, outputs)
except: status = failed, error = "Type: message"
finally: duration = now - started_at
```

源码：[engine.py#L361-L421](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/engine.py#L361-L421)。
`_publish` 在锁内写共享句柄表（[engine.py#L423-L428](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/engine.py#L423-L428)）。

> 并发正确性说明：节点只能引用其 `depends_on` 中已完成的句柄，而 dispatch 要求依赖全部
> `_completed` 才提交，所以在"只引用声明依赖"的前提下，快照读是安全的。
> 这是一条必须保持的不变量——若将来允许"引用即依赖"之外的隐式读取，需要加锁或改成按需读取。

---

## 3. 失败语义

### 3.1 传播规则

| 情形 | 当前节点 | 下游 | 并行分支 | 报告状态 |
| --- | --- | --- | --- | --- |
| 算子抛异常（重试耗尽） | failed | skipped | 继续执行 | `failed` |
| 算子失败且 `allow_failure=true` | failed | skipped（缺输入） | 继续执行 | `partial` |
| `when` 为假 | skipped | skipped | 继续执行 | `partial` |
| 上游 failed/skipped | skipped | 递归 skipped | 继续执行 | 视上游而定 |
| `fail_fast=true` 且出现硬失败 | failed | skipped | **停止调度** | `failed` |

硬失败判定：`status == failed && not allow_failure`
（[engine.py#L245-L254](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/engine.py#L245-L254)）。

### 3.2 重试与退避

```
attempts = max(0, retries) + 1
for attempt in 1..attempts:
    try: return execute_once(...)
    except ConfigError: 立刻放弃（配置/契约错误重试无意义）
    except Exception:
        if attempt == attempts: break
        sleep(max(0, retry_delay) * 2 ** (attempt - 1))
```

源码：[engine.py#L430-L457](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/engine.py#L430-L457)。

`ConfigError` 覆盖其全部子类：`SpecValidationError`、`ParamValidationError`、
`RefResolutionError`、`CycleError`、`OperatorNotFoundError`、`DuplicateOperatorError`。
因此"配置写错"绝不会被重试掩盖。

### 3.3 软超时

`timeout` 不为空时，`_execute_once` 为本次执行**再开一个守护线程**，主线程 `join(timeout)`：

- 执行按时结束 → 取结果或抛出捕获到的异常；
- 超时 → 抛 `TimeoutError("…（软超时，底层线程仍在运行）")`。

源码：[engine.py#L459-L484](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/engine.py#L459-L484)。

代价：Python 无法强杀线程，超时只是"不再等待"，算子可能继续写文件/占用连接。
超时失败会进入重试逻辑，因此超时 + 重试可能导致同一算子并发多份实例执行。

### 3.4 异常信息的保留

节点失败时 `NodeResult.error = f"{type(exc).__name__}: {exc}"`，报告与 JSON 都保留异常类型名，
但**不保留堆栈**；堆栈只走 logger（`ctx.logger.error`）。需要完整堆栈时应开启 `-v`。

### 3.5 取消

宿主通过 `run_pipeline(..., cancel_event=threading.Event)` 传入共享事件（置位即触发），
或从其它线程调用 `Engine.cancel(reason)`。取消后：

- **在跑节点自然结束**（线程无法强杀，与软超时同理由），结果照常计入报告；
- **未调度节点**在下一轮 `_dispatch` 中全部标记 skipped（note 含取消原因）；
- 报告 `warnings` 追加一条取消告警，状态聚合为 `partial`（存在 skipped 时见 §5.1）。

取消检查位于 `_dispatch` 决策顺序的第一步（§2.3），优先于 fail_fast 与依赖判断。

---

## 4. 缓存语义

### 4.1 key 的组成

```
key = blake2b(20 bytes, json.dumps({
  "op": <算子注册名（配置里写的那个）>,
  "version": <算子 version 类属性，默认 "1">,
  "params": fingerprint(解析后的参数),
  "inputs": {端口名: fingerprint(上游值)}
}, sort_keys=True))
```

源码：[cache.py#L57-L73](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/cache.py#L57-L73)，
指纹算法见 [fingerprint()](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/cache.py#L19-L42)：
标量用 `类型:repr`，容器递归并按 key 排序，类 DataFrame 对象用 `shape + to_json()[:2000]`，其余退化为 `repr`。

由此得到两个期望性质：

- **上游数据变化 ⇒ 自动失效**（inputs 指纹变）；
- **下游逻辑变化 ⇒ 不影响上游复用**（上游 key 不含下游信息）。

### 4.2 生效条件

只有同时满足以下三点才读写缓存
（[engine.py#L376-L401](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/engine.py#L376-L401)）：

1. 节点 `cache = true`；
2. `CacheStore.enabled`（目录非空）；
3. 算子实例 `deterministic` 为真（默认 `True`，有随机/时间/外部 IO 副作用的算子应声明 `deterministic=False`）。

### 4.3 命中与写入

- 命中：直接 `_publish` 缓存值，`result.cached = True`、`attempts = 0`，日志与报告标记 `[cache]`；
- 写入：仅成功执行后写入，采用临时文件 + `os.replace` 原子替换；
- 文件损坏：读取异常只告警并当作未命中，不影响运行。

### 4.4 需要注意的边界

| 边界 | 现状 |
| --- | --- |
| 缓存目录解析 | `--cache-dir` 默认 `.pipeline_cache`，相对**进程 CWD**，而非 `--workdir` |
| 淘汰策略 | 无，只有 `CacheStore.clear()` 手动清理 |
| 算子在执行中写文件但被缓存命中 | 文件写入不会重放（副作用丢失），因此带副作用的算子应关闭缓存 |
| pickle 反序列化 | 只应指向可信目录；不要使用他人提供的缓存目录 |
| 版本失效 | 靠算子类 `version` 手动递增，不会感知代码变更 |
| 别名 | key 使用配置里的名字，`read_csv` 与 `io.read_csv` 会产生不同 key |

---

## 5. 报告与产物

### 5.1 状态聚合

```
hard_failures = failed 且非 allow_failure
really_failed = 所有 failed
skipped       = 所有 skipped

status = "failed"   if hard_failures
       = "partial"  if really_failed or skipped
       = "success"  否则
```

### 5.2 `RunReport` 字段

| 字段 | 含义 |
| --- | --- |
| `pipeline` / `version` / `run_id` | 标识，`run_id` 默认 `YYYYmmdd-HHMMSS` |
| `status` | `success` / `partial` / `failed` |
| `duration` | `finished_at - started_at` |
| `params` | 合并后的流水线参数（含 overrides 与 env 展开） |
| `outputs` | 流水线级输出（引用解析结果，失败则进 `warnings`） |
| `warnings` | 输出解析告警 + 引擎告警（如取消告警） |
| `artifacts_dir` | 本次运行产物根目录 |
| `nodes[]` | 每节点 `status / duration / attempts / cached / outputs（类型摘要）/ error / note` |

`outputs` 在节点表中是**类型摘要**（`_describe`，例如 `list[18]`、`str('...')`），
不是完整数据；完整数据通过 `run_pipeline()` 返回的 `handles` 获取。

### 5.3 运行上下文与目录约定

```
workdir/                                  # 默认 = 配置文件所在目录
├── .op_pipeline/artifacts/<run_id>/      # RunContext.artifacts_dir，惰性创建
│   └── <node_id>/<artifact_name>         # ctx.artifact_path() 首次调用才建目录
└── <算子自己写的相对路径>                 # ctx.resolve_path() 的基准
```

`run_id` 精度到秒，同一秒内并发两次运行会复用同一产物目录（[context.py#L94](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/context.py#L94)）。
产物位置可用 `RunContext.create(..., artifacts_dir=...)` 覆盖（相对路径按 workdir 解析），
便于嵌入方把产物收敛到自己的目录约定。

### 5.4 退出码（CLI）

| 场景 | 退出码 |
| --- | --- |
| `success` / `partial` | 0 |
| `failed` | 1 |
| `validate` 校验失败 | 2 |

---

## 6. 一次完整运行时序

```
CLI/库调用
  │ from_file()               解析 JSON/TOML/YAML
  │ build_plan()              §1 全部校验，产出 Plan
  │ RunContext.create()       计算 artifacts_dir（惰性，首次使用才建目录）
  │ CacheStore(cache_dir)     目录为空即禁用
  ▼
Engine.run()
  │ dispatch  ├─ 取消 / fail_fast 已触发 → 全部 skipped
  │           ├─ 无依赖节点并行提交（≤ max_workers）
  │           ├─ when 为假 / 上游失败 → skipped
  │           └─ 依赖未完成 → 留到下一轮
  │ wait(FIRST_COMPLETED)
  │ collect   ├─ 写 _results/_completed
  │           └─ fail_fast 且硬失败 → _aborted
  │ ...循环直到 pending 与 running 均为空
  │ 解析 pipeline outputs
  │ 聚合 status / warnings
  ▼
RunReport（summary() / to_json()）+ handles
```

---

## 7. 修改引擎时的检查清单

改动 [engine.py](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/engine.py) 或
[spec.py](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/spec.py) 时，下面这些不变量必须仍然成立：

1. 节点只在其全部 `depends_on` 完成后才被提交；
2. 任何 failed/skipped 状态都必须写入 `_completed`，否则调度会永久卡住；
3. `_publish` 必须持锁，且在节点判定 success 之后；
4. `ConfigError` 永远不重试；
5. 报告状态三分类的判定顺序（hard → partial → success）不能被短路；
6. 新的校验若依赖运行期值，必须同时在 `build_plan` 里做静态版本，否则配置错误会延迟暴露。
