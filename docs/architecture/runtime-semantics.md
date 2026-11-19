# 运行时语义设计

本文回答一个问题：**一份配置从文本到结果，中间到底发生了什么，哪些行为是被写死的。**

整体是严格的两阶段模型：

```
阶段 A：编译（纯静态，可离线校验）
  配置文本 ──► PipelineSpec ──► build_plan() ──► Plan（nodes / order / handle_owner / params）

阶段 B：执行（有副作用）
  Plan + RunContext + CacheStore ──► Engine.run() ──► RunReport + handles
```

编译入口见 [build_plan()](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op_pipeline/spec.py#L328-L487)，
执行入口见 [Engine.run()](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op_pipeline/engine.py#L252-L336)。

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

源码：[spec.py#L339-L351](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op_pipeline/spec.py#L339-L351)。
注意参数之间**不保证**能互相引用：`RefResolver` 用的是同一个 `params` 字典，因此
`a = "${p.b}"` 只在 `b` 已存在于字典时才能解析成功，不存在则报 `RefResolutionError`。

### 1.2 节点 id 唯一性

统计出现次数，重复即问题项（[spec.py#L353-L359](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op_pipeline/spec.py#L353-L359)）。

### 1.3 算子存在性、参数合法性、句柄归属

对每个节点：

1. `registry.get(node.op)` 失败（含未注册、别名不存在）→ 问题项；
2. 若 `node.params` **不含**任何 `${}` 引用，直接 `cls(**node.params)` 实例化，
   把参数校验前移到编译期；若含引用，则只做"参数名是否声明"的静态检查，
   实例化推迟到运行期（[spec.py#L370-L387](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op_pipeline/spec.py#L370-L387)）；
3. 为每个输出端口计算句柄：`node.outputs.get(port, f"{id}.{port}")`，
   重复占用直接报错（[spec.py#L389-L403](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op_pipeline/spec.py#L389-L403)）。

> 设计要点：句柄归属表是在**遍历所有节点之后**才用于引用检查的，
> 因此配置里节点的书写顺序无关紧要，前向引用（引用写在后面的节点）天然合法。

### 1.4 引用可达性、输入端口完备性、依赖推导

- `RefResolver.static_target()` 递归提取 `inputs` 与 `params` 中所有 `${...}` 路径；
  以 `p.` / `params.` / `env.` 开头的跳过（参数与环境变量不产生依赖）。
- 其余引用用 `match_handle()` 做**最长前缀匹配**（支持 `${node.port.field}` 下钻），
  匹配到的句柄所属节点即成为依赖（[spec.py#L421-L437](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op_pipeline/spec.py#L421-L437)）。
- 自引用即自环，报错并剔除该依赖。
- 算子声明的非 optional 输入端口若未接线，报错
  （[spec.py#L439-L448](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op_pipeline/spec.py#L439-L448)）。

> 依赖是**由引用推导**出来的，配置里没有独立的 `depends_on` 字段。
> 这保证了"图"与"数据流"永远一致，不可能出现声明依赖但没接线的情况。

### 1.5 流水线输出引用与环检测

- `spec.outputs` 里的引用同样做句柄存在性校验；
- 仅当此前没有任何问题时才做 Kahn 拓扑排序（同层按名字排序，保证结果稳定），
  否则环检测的噪音会掩盖真正的配置错误
  （[spec.py#L469-L515](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op_pipeline/spec.py#L469-L515)）。

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
（[engine.py#L217-L218](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op_pipeline/engine.py#L217-L218)）。

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

源码：[engine.py#L275-L288](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op_pipeline/engine.py#L275-L288)。

### 2.3 dispatch 的决策顺序

对 `pending` 中的每个节点（按拓扑序）依次判断
（[engine.py#L344-L402](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op_pipeline/engine.py#L344-L402)）：

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

源码：[engine.py#L404-L422](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op_pipeline/engine.py#L404-L422)。
求值走 [evaluate_condition()](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op_pipeline/expression.py#L172-L174)，
即 AST 白名单（见 [核心模块设计 §5](../design/core-modules.md#5-expressionpy安全表达式)）。

> `when = "params.n > 0"` 与 `when = "n > 0"` 完全等价；点语法对映射做键下钻
> （`inputs.table.rows` 这类嵌套 dict 逐层下钻），目标不是映射或键不存在即报错——
> 因此 `x.__class__` 之类的属性逃逸天然被挡住。

### 2.5 单节点执行流水线

```
_run_node(node):
  ctx = run_context.for_node(node_id, op)        # 每个节点独立 logger
  resolver = RefResolver(plan.params, self.store) # M2-3：构造时经 store.snapshot() 拍快照，
                                                  # 避免跨节点可见性抖动（行为同 dict 快照）

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
except: status = failed, error = "Type: message", traceback = format_exc()（截断至约 2000 字符、保留末尾）
finally: duration = now - started_at
```

源码：[engine.py#L467-L540](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op_pipeline/engine.py#L467-L540)。
`_publish` 在锁内写共享句柄表（[engine.py#L542-L547](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op_pipeline/engine.py#L542-L547)）。

> M2-4 `SpillHandleStore` 的落盘语义：`put` 时按 `pickle.dumps` 结果量大小，
> 超过阈值（默认 1MB）的值写盘（目录默认 `artifacts_dir/spill`，引擎在构造时经
> `attach(context)` 钩子注入），表内只存路径占位；读收敛点不变——`get` /
> 快照按键访问按需 `pickle.loads`，成员检查与迭代经惰性快照零 IO。

> 并发正确性说明：节点只能引用其 `depends_on` 中已完成的句柄，而 dispatch 要求依赖全部
> `_completed` 才提交，所以在"只引用声明依赖"的前提下，快照读是安全的。
> 这是一条必须保持的不变量——若将来允许"引用即依赖"之外的隐式读取，需要加锁或改成按需读取。

### 2.6 采样预览（v0.9）

`run(config, sample_rows=N)` / CLI `run --sample N` 进入采样模式，用于"不上全量数据
先看链路通不通"：

- **截断点**：只发生在**输入解析收敛处**（`_sample_inputs`，线程与远程路径一致）——
  解析后的输入值若为 `list` 且长度超过 `N`，截断为**前 N 行**；`dict` / `str` /
  其他类型原样透传。节点自身的输出不截断——下游拿到的是"上游被截断后的真实产物"，
  因此报告里的行数血缘（§2.7）能如实反映每一步实际流过的行数；
- **参数不截断**：`params` 不做采样，`${param:...}` 注入值原样生效；
- **缓存整体禁用**：采样模式下引擎用 `CacheStore(None)` 替换缓存存储（读写全部关闭）——
  采样结果不代表全量数据，落缓存会污染后续全量运行；
- **校验**：`sample_rows` 必须是 `≥1` 的整数（`bool` 不算整数），否则构造期抛
  `ValueError`；
- **报告标注**：run 级 `RunReport.sample_rows = N`；节点级每个 `NodeResult.sampled = True`
  （采样模式下全量标注，`to_dict()`/`to_json()` 仅在采样模式输出这两个字段）。
  未采样（`None`）时报告不含 `sample_rows`，节点不含 `sampled`，与 v0.8.0 输出一致。

### 2.7 节点行数血缘（v0.9）

`lineage=True`（默认）时，每个节点的 `NodeResult` 记录两个 dict：

- **`rows_in`**：输入端口名 → 该端口 list 句柄的行数。采集点在**输入解析与采样截断
  之后**——采样模式下记录的是截断后的实际行数；
- **`rows_out`**：输出端口名 → 发布句柄的行数。采集点在 **`_publish`**（含缓存命中
  的发布路径 `_finish_cache_hit`）。非 list 端口不记录（dict/str/标量不计行数）；
- **空值即缺席**：`lineage=False` 或端口不是 list 时对应 dict 为空，
  `to_dict()`/`to_json()` 不输出 `rows_in`/`rows_out` 键，SQLite nodes 表写 `NULL`
  （报告"无键 ↔ DB NULL"语义一致）；
- **默认行为**：`lineage` 默认开启，报告节点平铺这两个键；`run(config, lineage=False)`
  可关——关闭后报告与 v0.8.0 完全一致（纯加法回退）。

### 2.8 子流水线编排（v0.9）：`pipeline.run`

`pipeline.run` 算子把"运行另一份配置文件"封装成普通节点，语义边界如下：

- **执行位置**：在父引擎的工作线程内同步调用 `op_pipeline.run()`——子流水线拥有
  自己的 `RunContext` / `CacheStore` / 报告与 run_id，其完整报告与 artifacts
  **不并入**父运行；父节点只保留三个输出端口：`result`（子节点取到的句柄值）、
  `report`（子 run 摘要：`run_id` / `status` / `duration` / `node_count` /
  `failed_nodes`）、`run_id`；
- **防环**：`pipeline.run` 维护 thread-local 执行栈（当前线程已进入而未退出的子
  配置路径集合），同一栈内重复出现同一配置即循环引用，报 `ConfigError` 且消息含
  完整引用链路（如 `a → b → a`）；
- **深度上限**：嵌套层数最多 **5** 层（`_MAX_SUB_DEPTH`，第 6 层拒绝执行），超出抛
  `ConfigError`——防止配置互相套娃失控；
- **预检先行**：子配置缺失、目标节点/端口不存在在执行前预检报错（读取子配置并编译
  校验），不产生子 run 副作用；
- **永不缓存**：`deterministic=False`——子流水线可能含时间/外部 IO，且子 run 自身
  落自己的缓存，父层不重复落；
- **参数传递**：`params` dict 经 `run(config, params=...)` 注入子流水线（等价于子
  配置的 `--param`），值可含父层的 `${param:...}` 占位符，实现"父参数下传"；
- **失败传播**：子 run 非 success 时按 `on_error` 处理（当前仅 `propagate`）——
  父节点 failed，错误文本含子 run_id / 子状态 / 失败节点列表，下游照常 skipped。

完整示例：`examples/pipeline_subpipeline.json`（父，`--param LIMIT=3`）+
`examples/pipeline_subpipeline_child.json`（子，`${param:LIMIT:-5}` 默认值）。

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
（[engine.py#L341-L343](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op_pipeline/engine.py#L341-L343)）。

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

源码：[engine.py#L549-L580](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op_pipeline/engine.py#L549-L580)。

`ConfigError` 覆盖其全部子类：`SpecValidationError`、`ParamValidationError`、
`RefResolutionError`、`CycleError`、`OperatorNotFoundError`、`DuplicateOperatorError`。
因此"配置写错"绝不会被重试掩盖。

### 3.3 超时：线程软超时 / 进程硬超时

`timeout` 不为空时，按节点执行器分两种语义（M2-2 起）：

- **线程执行（默认）**：`_execute_once` 为本次执行**再开一个守护线程**，
  主线程 `join(timeout)`——超时抛 `TimeoutError("…（软超时，底层线程仍在运行）")`。
  Python 无法强杀线程，超时只是"不再等待"，算子可能继续写文件/占用连接；
- **进程执行（节点 `executor = "process"` 或运行级 `--executor process`）**：
  算子在 `spawn` 子进程里执行，超时到点 `terminate()` 子进程——
  `TimeoutError("硬超时：…已强制终止")`。真正的终止，不留残余线程。
  载荷（算子实例 + 参数 + 输入）不可 pickle 时自动回退线程执行
  （退化为软超时），并在 `report.warnings` 记录回退告警。

两种模式的超时失败都会进入重试逻辑，因此超时 + 重试可能导致同一算子并发多份实例执行。

源码：[engine.py](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op_pipeline/engine.py) `_execute_once` / `_execute_in_process`、
[executor.py](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op_pipeline/executor.py) `ProcessExecutor.wait`。

### 3.4 异常信息的保留

节点失败时 `NodeResult.error = f"{type(exc).__name__}: {exc}"`；M1-3 起
`NodeResult.traceback` 保留 `traceback.format_exc()`（**截断至约 2000 字符、保留末尾**——
异常抛出点最有排障价值）。重试耗尽路径以 `raise _NodeFailure(last_error) from last_error`
显式链接根因，因此堆栈含算子内原始异常的完整帧。`summary()` 不含堆栈（避免刷屏），
完整堆栈走 `to_dict()` / `to_json()` 与 `ctx.logger.error`。

### 3.5 取消

宿主通过 `run_pipeline(..., cancel_event=threading.Event)` 传入共享事件（置位即触发），
或从其它线程调用 `Engine.cancel(reason)`。取消后：

- **在跑节点自然结束**（线程无法强杀，与软超时同理由），结果照常计入报告；
- **未调度节点**在下一轮 `_dispatch` 中全部标记 skipped（note 含取消原因）；
- 报告 `warnings` 追加一条取消告警，状态聚合为 `partial`（存在 skipped 时见 §5.1）。

取消检查位于 `_dispatch` 决策顺序的第一步（§2.3），优先于 fail_fast 与依赖判断。

### 3.6 并发分组限流（v0.7）

节点可用 `concurrency_group` 声明所属分组，运行级 `concurrency_limits` 给每个组
配置同时执行上限（`{组名: 最大并行数}`）。语义：

- **惰性建信号量**：引擎按组名在首次遇到该组节点时创建
  `threading.Semaphore(limit)`；同一份 `Plan` 一次运行的节点共享这些信号量；
- **acquire / release 时机**：输入与参数解析、缓存查询完成后，节点**真正开始执行前**
  acquire，执行结束（成功/失败/超时）在 `finally` 中 release，异常路径不会泄漏配额；
- **只限制实际执行**：缓存命中的节点直接发布结果并返回，**不 acquire**，因此不占用
  分组名额；
- **未声明/未配置不限**：节点 `concurrency_group` 为空，或该组不在
  `concurrency_limits` 中时，行为与旧版完全一致；
- **非法配置报错**：组名必须是非空字符串、上限必须是**正整数**，否则在构造引擎时
  抛 `ConfigError`（注意 `0` 不是"不限制"，而是非法值；不限制请直接不配置该组）；
- **作用域是单次运行**：限流只在当前引擎/进程内生效，不跨 worker 进程或跨机器聚合。

### 3.7 按异常类型重试（v0.7）

`retry_on`（字符串列表）把重试从"所有普通异常"收窄为"指定异常类型"：

```
for attempt in 1..retries+1:
    try: return execute_once(...)
    except ConfigError: 直接失败（永不重试）
    except Exception as exc:
        if retry_on and type(exc).__name__ not in retry_on: 直接失败
        if attempt == 最后一次: 失败
        sleep(retry_delay * 2 ** (attempt - 1))
```

- **匹配方式**：精确匹配 `type(exc).__name__`（异常类名，如 `"TimeoutError"`），
  **不做继承关系匹配**——想重试某个子类需显式写出子类名；
- **与次数/退避的关系**：`retry_on` 只决定"这次失败要不要继续尝试"，总尝试次数仍是
  `retries + 1`，退避仍是 `retry_delay * 2^(n-1)`；未命中的异常立即失败，
  `attempts` 停在当次尝试；
- **配置类错误优先级最高**：`ConfigError` 及其子类（含 `ParamValidationError`、
  `RefResolutionError` 等）即使被写进 `retry_on` 也**永不重试**——配置写错不应被
  重试掩盖；
- **远程节点**：远程失败在引擎侧重试包装处表现为 `_RemoteFailure`（worker 打包的
  原始类型名在错误文本里）；需要按类型重试远程异常时，`retry_on` 应写
  `"_RemoteFailure"`，写 worker 侧原始类型名不会命中；
- **缺省不变**：`retry_on` 为空（默认）时所有普通异常按 `retries` 重试，与旧版一致。

### 3.8 远程节点取消传播（v0.7）

队列执行器（`executor="queue"` / `QueueWorkerExecutor`）的节点在主进程中表现为
"等待结果文件的线程任务"。取消语义分两种路径：

- **远程节点：不再等待远端**。取消事件置位后，等待循环停止轮询结果文件并结束，
  节点直接标记 `skipped`（note 记录取消原因），报告按 `partial` 聚合；
  该节点已经过 `on_node_start`，因此 `on_node_finish` 会以 `status="skipped"` 触发
  （与 `_dispatch` 中从未调度、不发钩子的 skipped 不同）；
  队列任务本身**无法撤销**，worker 可能仍在执行并写出结果文件，引擎会忽略它，
  该结果文件可按运维策略清理；
- **本地线程节点：软取消**。已经在跑的节点自然结束（线程无法强杀，与软超时同理由），
  其结果照常计入报告；尚未调度的节点在下一轮 `_dispatch` 开头标记 `skipped`。

取消检查仍在 `_dispatch` 第一步（§2.3），远程与本地路径共享同一取消事件；
远程模式下取消只影响"是否继续等"，不改变重试/缓存等其它语义。

---

## 4. 缓存语义

> 小节索引：§4.1 key 的组成 → §4.2 生效条件 → §4.3 命中与写入 → §4.4 边界 →
> §4.5 缓存 LRU 与单飞（v0.8）。

### 4.1 key 的组成

```
key = blake2b(20 bytes, json.dumps({
  "op": <算子注册名（配置里写的那个）>,
  "version": <算子 version 类属性，默认 "1">,
  "params": fingerprint(解析后的参数),
  "inputs": {端口名: fingerprint(上游值)},
  "source": blake2b(算子类源码)   ← 默认纳入（M2-5 起可选；M5-4 起默认开启）
}, sort_keys=True))
```

源码：[cache.py#L70-L91](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op_pipeline/cache.py#L70-L91)，
指纹算法见 [fingerprint()](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op_pipeline/cache.py#L19-L42)：
标量用 `类型:repr`，容器递归并按 key 排序，类 DataFrame 对象用 `shape + to_json()[:2000]`，其余退化为 `repr`。

由此得到三个期望性质：

- **上游数据变化 ⇒ 自动失效**（inputs 指纹变）；
- **下游逻辑变化 ⇒ 不影响上游复用**（上游 key 不含下游信息）；
- **源码变化 ⇒ 自动失效（默认）**：`CacheStore` 默认 `include_source=True`
  （M5-4/Task 9 起），把算子类源码 blake2b 摘要纳入 key——算子源码变更后
  以默认参数重跑会自动缓存失效（`cached=false`）；显式
  `include_source=False` 可关闭，退回只按版本字符串的旧行为。

### 4.2 生效条件

只有同时满足以下三点才读写缓存
（[engine.py#L483-L513](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op_pipeline/engine.py#L483-L513)）：

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
| 缓存目录解析 | `--cache-dir` 默认 `.pipeline_cache`，相对**工作目录**（M1-2 起，与进程 CWD 无关） |
| 淘汰策略 | v0.8 起支持 `max_entries` / `max_bytes` 按 mtime 自动淘汰；未配置容量时只有 `CacheStore.clear()` 手动清理 |
| 算子在执行中写文件但被缓存命中 | 文件写入不会重放（副作用丢失），因此带副作用的算子应关闭缓存 |
| pickle 反序列化 | 只应指向可信目录；不要使用他人提供的缓存目录 |
| 源码失效 | 默认（M5-4 起）按算子源码指纹失效——源码变更自动缓存失效；显式 `include_source=False` 后仅靠 `version` 手动递增 |
| 别名 | key 使用配置里的名字，`read_csv` 与 `io.read_csv` 会产生不同 key |

### 4.5 缓存 LRU 与单飞（v0.8）

**容量淘汰**：`CacheStore(directory, max_entries=None, max_bytes=None)` 的容量上限可选，
`None` 表示不限。启用后每次 `store()` 写入完成即调用 `_evict()`：只扫描 `*.pkl`
（正在写的 `<key>.tmp` 不参与），按 mtime 升序删除最旧条目，直到条目数与总字节
同时满足上限；Windows 上因句柄占用删除失败时跳过，下次写入再试。`max_entries <= 0`
或 `max_bytes <= 0` 在构造时即抛 `ValueError`。

**命中刷新（LRU）**：仅当启用了任一容量上限时，`load()` 命中后调用 `os.utime` 刷新
mtime；未配置容量时保持 v0.7.0 行为（不触碰文件时间）。

**同 key 单飞**：`CacheStore.key_lock(key)` 返回该 `cache_key` 对应的进程内
`threading.Lock`（锁表由元锁保护）。引擎在缓存未命中后持锁二次查缓存，仍未命中才
执行算子并写入——并发线程遇到同一 key 时只有一个真正计算，其余等待后直接读缓存。
锁只在当前进程内生效，跨进程（如多个 worker）不共享。

**边界**：容量上限是"写入后收敛"，不是硬配额——单次写入大对象时目录可能瞬时超过上限；
淘汰只在写入路径触发，纯命中的运行不会主动收缩目录。运行结束由 `storage_warnings()`
统计缓存目录与 spill 目录实际占用，超过 `cache.max_bytes` / `spill_warn_bytes` 时只向
`report.warnings` 追加中文提示，**不改变** `status` 与 `outputs`。

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
| `pipeline` / `version` / `run_id` | 标识，`run_id` 默认 `YYYYmmdd-HHMMSS-xxxx`（4 位随机十六进制后缀防同秒碰撞） |
| `status` | `success` / `partial` / `failed` |
| `duration` | `finished_at - started_at` |
| `params` | 合并后的流水线参数（含 overrides 与 env 展开） |
| `outputs` | 流水线级输出（引用解析结果，失败则进 `warnings`） |
| `warnings` | 输出解析告警 + 引擎告警（取消告警、端口软类型告警） |
| `artifacts_dir` | 本次运行产物根目录 |
| `sample_rows` | **v0.9 新增**：采样预览行数（`--sample N` / `run(sample_rows=N)`）；未采样时无此字段 |
| `nodes[]` | 每节点 `status / duration / attempts / cached / outputs（类型摘要）/ error / note / traceback（失败时截断堆栈）`；**v0.9 加法**：采样模式下每节点增 `sampled: true`，`lineage` 开启（默认）时增 `rows_in` / `rows_out`（端口名 → list 行数，见 §2.7），未采样/关闭时不输出这些键 |

`outputs` 在节点表中是**类型摘要**（`_describe`，例如 `list[18]`、`str('...')`），
不是完整数据；完整数据通过 `run_pipeline()` 返回的 `handles` 获取。

**端口软类型告警（M1-1 起）**：`run()` 汇总阶段调用
[check_handle_types()](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op_pipeline/spec.py#L553-L580)
对每个已产出句柄做声明类型与实际类型的核对，不符时写入 `warnings`
（如 `节点 [src] 句柄 'src.value' 声明 int，实际 str`）。值**照常向下游传递**，
运行状态不受影响——类型声明是文档与排障信息，不是执行门禁；缺失的句柄
（skipped/failed 节点）天然跳过。

### 5.3 运行上下文与目录约定

```
workdir/                                  # 默认 = 配置文件所在目录
├── .op_pipeline/artifacts/<run_id>/      # RunContext.artifacts_dir，惰性创建
│   └── <node_id>/<artifact_name>         # ctx.artifact_path() 首次调用才建目录
└── <算子自己写的相对路径>                 # ctx.resolve_path() 的基准
```

`run_id` 精度到秒并带 4 位随机十六进制后缀（M1-4 起），同一秒内并发两次运行
产物目录互相隔离；显式传 `run_id` 参数时以传入值为准。
产物位置可用 `RunContext.create(..., artifacts_dir=...)` 覆盖（相对路径按 workdir 解析），
便于嵌入方把产物收敛到自己的目录约定。

### 5.4 退出码（CLI）

| 场景 | 退出码 |
| --- | --- |
| `success` / `partial` | 0 |
| `failed` | 1 |
| `validate` 校验失败 | 1（通过为 0） |
| CLI 用法错误（如 `--resume` 未同时指定 `--runs`） | 2 |

---

## 6. 运行记录（M2-5）：SQLite 落库

可选的运行元数据持久化：CLI 传 `--runs <db>`、库调用传 `run_store=RunStore(path)`。

- **默认关闭、零副作用**：不传开关时引擎完全不触碰 SQLite，不产生任何文件；
- 表结构（首次写入惰性建表，标准库 `sqlite3`）：

  | 表 | 字段 |
  | --- | --- |
  | `runs` | `run_id`(PK), `pipeline`, `version`, `status`, `started_at`, `finished_at`, `params_json` |
  | `nodes` | `run_id`+`node_id`(PK), `op`, `status`, `attempts`, `cached`, `error`, `cache_key`, `finished_at`, `duration`（v0.7）, `rows_in`/`rows_out`（v0.9，JSON 文本；无血缘写 NULL） |

- **写入点**：
  - 节点：`Engine._collect`（真实执行收口，**含缓存命中早退**）与 `_mark_skipped`
    （跳过节点也入库）——nodes 表与 `RunReport` 节点列表一一对应；
  - 运行：`run()` 末尾一条 `runs`（`INSERT OR REPLACE`，同 `run_id` 重跑可覆盖）；
- `NodeResult.cache_key`：命中缓存时记录算出的 key；未启用缓存或未命中为
  `NULL`。这是 M2-6 断点续跑比对"上次成功节点是否可复用"的依据；
- 连接生命周期由调用方管理（`close()` / `with`）；`query(sql)` 返回原始行；
- `params` 以 `json.dumps(default=str)` 存 JSON 文本；
- **列自动迁移**：旧库缺列时构造 `RunStore` 即幂等 `ALTER TABLE` 补齐（v0.7 补
  `duration`，v0.9 补 `rows_in`/`rows_out`，旧行取默认值/NULL），无需手工迁移。

### 6.1 断点续跑（M2-6）：`--resume <run_id>`

依赖 M2-5 的 nodes 记录（尤其 `cache_key`）。语义与边界：

- **开关**：CLI `run --runs <db> --resume <run_id>`；库调用
  `run_pipeline(..., run_store=..., resume_run_id=...)`。`--resume` 必须配合
  `--runs`（参考库），否则 CLI 报错 exit 2；
- **机制**：引擎构造时把参考 run 的全部 `(node_id, status, cache_key)` 一次预载进
  内存 `_resume_map`（**SQLite 连接有线程亲和性——不能在 worker 线程里查询**，
  预载避免了跨线程访问）；`_run_node` 在缓存命中处比对：
  参考记录 `status == success` 且 `cache_key` 与本次一致 → 复用缓存并标记
  `NodeResult.resumed`，报告与日志显示 `[resumed]`；
- **不会 resume 的情形**：参考段失败/skipped/无记录 → 照常重跑（失败段与下游
  因此被完整修复）；缓存条目缺失（如被清理）→ 必须重跑，不标 `[resumed]`；
  未知 `run_id` → 退化为普通运行；不带 resume 的普通重跑只标 `[cache]`
  （三者均有单测覆盖）；
- **报告**：`[resumed]` 与 `[cache]` 互斥展示（resumed 优先），
  `RunReport.to_dict()`/`to_json()` 含 `resumed` 布尔字段。

### 6.2 执行事件钩子（M2-7）：`Engine(hooks=[Hook])`

进度条、日志聚合、OpenTelemetry、未来 UI 都从这里取数。协议与语义：

- **协议**（`Hook`，包顶层导出）：`on_node_start(node_id, op)`、
  `on_node_finish(node_id, status, duration, cached)`、
  `on_run_finish(report)`；实现任意一个或多个方法即可；
- **触发点**：`on_node_start` 在 `_run_node` 开头（节点开始执行）；
  `on_node_finish` 在 `_run_node` 的 `finally`（成功/失败/缓存命中都触发，
  缓存命中提前 `return` 也会经 finally 发 finish，cached=True 一并传入）；
  `on_run_finish` 在 `run()` 汇总完毕（报告状态已定），总在最后；
- **被跳过的节点不触发**：when 为假 / 上游失败 / 取消的节点从未开始执行，
  不产生任何钩子事件；
- **并发与容错**：节点并行执行时钩子可能被多个线程并发调用，实现需自带
  同步（进度条示例即用锁）；钩子抛出的异常只记入报告告警，不影响流水线；
- **进度条示例**（线程安全，节点数来自 `plan.nodes`）：

```python
import sys
import threading

from op_pipeline import Hook, run_pipeline, default_registry

class ProgressBar(Hook):
    def __init__(self, total: int):
        self.total = total
        self.done = 0
        self._lock = threading.Lock()

    def _redraw(self):
        done = self.done
        width = 30
        filled = int(width * done / self.total)
        sys.stdout.write(f"\r[{'#' * filled}{'-' * (width - filled)}] {done}/{self.total}")
        sys.stdout.flush()

    def on_node_finish(self, node_id, status, duration, cached):
        with self._lock:
            self.done += 1
        self._redraw()

    def on_run_finish(self, report):
        sys.stdout.write(f"\n{report.pipeline} {report.status}（{report.duration:.2f}s）\n")

# plan 编译后可取节点数；或简单传 len(spec.nodes)
report, values = run_pipeline(spec, registry=default_registry,
                              hooks=[ProgressBar(len(spec.nodes))])
```

### 6.3 async 宿主嵌入（M2-8）：`run_pipeline_async`

`run_pipeline` 是同步阻塞调用，在 async 宿主（Web 服务、脚本主协程）里直接
调用会**卡死事件循环**。`run_pipeline_async`（包顶层导出）用
`loop.run_in_executor` 把同步引擎抛到事件循环的默认线程执行器，参数与返回值
与 `run_pipeline` 完全一致：

```python
import asyncio

from op_pipeline import default_registry, run_pipeline_async

async def main():
    report, values = await run_pipeline_async(
        spec, registry=default_registry, workdir="."
    )
    print(report.status, report.run_id)

asyncio.run(main())
```

- **非阻塞**：await 期间同循环的其它 asyncio 任务照常推进（有单测验证）；
- **语义不变**：调度仍在线程池内，重试/缓存/钩子/续跑与同步版本完全一致；
- **取消语义**：`asyncio.CancelledError` 只放弃 await，不会中途终止正在跑的
  流水线；需要中途止损仍用 `cancel_event`；
- **边界**：这是"嵌入 async 宿主"的桥接，**不是全 asyncio 调度器**——后者意味着
  引擎内部改用 `asyncio.wait(FIRST_COMPLETED)` 调度同步与异步算子，不在本里程碑范围。

---

## 7. 一次完整运行时序

```
CLI/库调用
  │ from_file()               解析 JSON/TOML/YAML
  │ await run_pipeline_async() M2-8：run_in_executor 抛到线程执行器，事件循环不被阻塞
  │ build_plan()              §1 全部校验，产出 Plan
  │ RunContext.create()       计算 artifacts_dir（惰性，首次使用才建目录）
  │ CacheStore(cache_dir)     目录为空即禁用
  │ RunStore(db)              仅 --runs / run_store 时构造（否则零副作用）
  ▼
Engine.run()
  │ 构造时 └─ --resume：预载参考 run 的 nodes(status/cache_key) → _resume_map
  │ dispatch  ├─ 取消 / fail_fast 已触发 → 全部 skipped
  │           ├─ 无依赖节点并行提交（≤ max_workers）
  │           ├─ when 为假 / 上游失败 → skipped
  │           └─ 依赖未完成 → 留到下一轮
  │ wait(FIRST_COMPLETED)
  │ collect   ├─ 写 _results/_completed
  │           ├─ 缓存命中且 _resume_map 匹配 → 标记 resumed（[resumed]）
  │           ├─ RunStore.record_node（成功/失败/跳过，含 cache_key）
  │           └─ fail_fast 且硬失败 → _aborted
  │ 每节点     └─ on_node_start（_run_node 开头）→ on_node_finish（finally，成功/失败/缓存命中）
  │ run 末尾 └─ on_run_finish(report)（总在最后）
  │ ...循环直到 pending 与 running 均为空
  │ 解析 pipeline outputs
  │ 聚合 status / warnings
  │ RunStore.record_run（runs 表一条）
  ▼
RunReport（summary() / to_json()）+ handles
```

---

## 8. 修改引擎时的检查清单

改动 [engine.py](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op_pipeline/engine.py) 或
[spec.py](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op_pipeline/spec.py) 时，下面这些不变量必须仍然成立：

1. 节点只在其全部 `depends_on` 完成后才被提交；
2. 任何 failed/skipped 状态都必须写入 `_completed`，否则调度会永久卡住；
3. `_publish` 必须持锁，且在节点判定 success 之后；
4. `ConfigError` 永远不重试；
5. 报告状态三分类的判定顺序（hard → partial → success）不能被短路；
6. 新的校验若依赖运行期值，必须同时在 `build_plan` 里做静态版本，否则配置错误会延迟暴露。

---

## 9. 远程执行示范（M4-4）：队列执行器与 worker CLI

> 小节索引：§9 队列执行器示范（M4-4）→ §9.1 引擎接入（M5-1）→ §9.2 租约与故障回收（v0.8）。

`QueueWorkerExecutor`（[remote.py](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op_pipeline/remote.py)）
实现 `Executor` 协议（submit/wait/shutdown），但把任务分发给**独立 worker 进程**执行，
客户端与 worker 之间只通过共享文件系统交换：

```
客户端进程（引擎进程）                worker 进程（可不同机器）
─────────────────────────            ─────────────────────────────
exec_.submit(fn, *args)              python -m op_pipeline worker
  └─ pickle→base64 追加进               └─ 按 offset 增量读队列文件
      jobs.jsonl（JSONL 队列）           └─ fn(*args) 执行
exec_.wait([f1, ...])                    └─ 结果/异常写回
  └─ 轮询 results/<job_id>.json          results/<job_id>.json
      （存在即完成）                      （先写 .tmp 再原子 replace）
```

运行时序要点：

1. **载荷**：`(fn, args)` 先 `pickle.dumps` 再 base64，写入队列文件的一行
   `{"job_id": ..., "payload": ...}`；worker 按 `offset` 增量消费（可断点续消费、
   可回放）。载荷不可 pickle 时 `submit` 直接抛 `RuntimeError`——队列模式没有
   "回退线程"选项，提前报错比静默丢失任务诚实。
2. **结果回写**：worker 把 `{"job_id", "status", "result"(base64), "error"}` 写为
   `<job_id>.json`，先写 `.tmp` 再 `replace()` 原子改名——客户端 `wait` 轮询
   `is_file()` 永远读到完整文件，不会读到半个结果。
3. **wait 语义**：与引擎主循环对 `Executor.wait` 的约定一致——`first_completed=True`
   时首个完成即返回，`False` 时等全部完成；轮询间隔由 `poll` 控制（默认 0.1s）。
4. **异常语义**：任务执行异常不会让 worker 进程退出，而是以 `status="error"` +
   `"ZeroDivisionError: division by zero"` 形式的字符串回写；客户端 `f.result()`
   时以 `RuntimeError` 抛出（与 `ThreadExecutor` 的 Future 行为一致）。
5. **worker CLI**：`op_pipeline worker --queue <path> [--out <dir>] [--count N]
   [--poll 秒]`。`--count` 限定消费条数后正常退出；不传则持续监听直到
   `Ctrl+C`（exit 130）。`--out` 缺省为队列同目录下的 `results/`。

与内置执行器的差异：`ThreadExecutor`/`ProcessExecutor` 由引擎内部创建并驱动，
服务**同一进程内**的节点调度；`QueueWorkerExecutor` 面向"另起 worker 进程 /
跨机器共享盘"的任务分发。端到端双进程示范：`python examples/remote_exec_demo.py`。

### 9.1 远程执行语义（M5-1）：引擎接入队列执行器

M5-1 起 `run_pipeline(executor=...)` / `Engine` 的 executor 参数接受
`str | Executor | None`：字符串 `"thread"`/`"process"`/`"queue"` 仍是快捷方式
（`"queue"` 由引擎自建 `QueueWorkerExecutor`，队列与结果目录固定为
`<workdir>/.op_pipeline/remote/`——路径跨 run 稳定，worker 可预先启动常驻消费）；
也可以直接传任意 `Executor` 实例（如 `QueueWorkerExecutor(...)` /
`ThreadExecutor(...)`），引擎把节点执行分发到该实例，实例生命周期归调用方、
引擎不 shutdown。节点级 `executor="process"`/`"queue"` 声明在运行级实例模式下
被忽略并记 warning（实例是唯一执行后端）。

远程节点的执行语义（`Engine._run_remote_node`，与线程路径 `_run_node` 同构）：

1. **载荷打包**：引用解析与快照语义留在**主进程**（`RefResolver` 对
   `self.store` 快照，与线程路径一致）——worker 只做"按 `default_registry`
   实例化算子 → 执行 → 发布输出"。载荷（`remote._remote_run` 的 payload）携带
   **解析后的**输入值与参数：输入值要么来自共享句柄存储（`SharedHandleStore.put`
   已保证句柄值可 pickle），要么是配置/参数字面量，天然可 pickle，无需降级
   路径；调用方经 overrides 注入不可 pickle 的参数值时 `submit` 即抛
   `RuntimeError`——该输入不适合远程执行，绝不静默退化。
2. **输出经共享目录对象存储**：worker 把输出 `put` 进共享句柄目录（原子发布），
   返回值只携带句柄名映射——输出值不可 pickle 时在 `put` 处即报错并打包进
   返回 dict，引擎侧还原为节点失败（结果文件必须整体 pickle 回传，输出直接进
   返回值的错误路径很糟）；引擎侧从同一共享目录读回输出值与描述，主循环结束
   后 `run_pipeline` 返回的句柄表经 `SharedHandleStore.snapshot` 即可读全。
3. **调度形态**：远程模式下主循环 pool 仍是引擎内部 `ThreadExecutor`，远程节点
   经内部线程任务包装（主进程构造载荷 → 队列 `submit` → 阻塞等结果）——主循环
   `wait(first_completed=True)` 语义完全不变（远程节点在主循环看来仍是普通
   线程任务）。
4. **重试 / 缓存 / 超时都在主进程侧**：重试退避沿用引擎包装（`_attempt_loop`，
   与线程/进程路径同一套）；缓存 `cache_key` 计算与读写也在主进程，命中则根本
   不发队列任务（命中路径走引擎 `_publish` 写收敛点，值经 `store.put` 进共享
   目录）；`timeout` 在主进程侧生效——**软超时限制不因远程而消失**：到点抛
   `TimeoutError` 按普通失败参与重试，worker 侧算子可能仍在运行（队列任务无法
   强杀，与线程路径"底层线程仍在跑"的已知限制同语义）。
5. **失败还原**：worker 把异常打包为 `{"status": "failed", "error",
   "traceback"}` 返回（传输层恒为 ok）；引擎重试耗尽后把 error 与截断的
   traceback 还原进 `NodeResult`，下游节点照常跳过，报告状态归并
   （hard → partial → success）与线程路径一致。
6. **worker 侧算子可见性**：worker 经 `default_registry` 按算子名实例化——
   内置算子天然可见；自定义算子需经插件机制或随代码部署（见
   [扩展设计 §4.2](../design/extension-points.md#42-执行器)）。

### 9.2 远程队列租约与故障回收（v0.8）

v0.7 及以前的队列投递是"至少一次"：worker 认领后崩溃，任务就此丢失。M7-1 引入
**租约（lease）**，把"认领"变成可过期、可回收的状态：

```
客户端 submit                              worker 认领
─────────────                              ──────────
队列行携带 lease_seconds / max_attempts     原子创建 claims/<job_id>.json
                                            {owner, deadline=now+lease, attempt}
```

- **开关**：`QueueWorkerExecutor(queue, out, lease_seconds=S, max_attempts=N)` 在 `submit`
  时把两个值写进队列行；worker 侧 `consume_jobs(..., lease_seconds=..., max_attempts=...,
  owner=...)` 与 CLI `--lease-seconds`/`--max-attempts`/`--owner` 提供同参数。**队列行内值
  优先**，行内缺失才用 worker 参数；`lease_seconds <= 0`（默认）时完全不创建 `claims/`，
  行为与 v0.7.0 一致。
- **互斥认领**：worker 以 `O_CREAT|O_EXCL` 原子创建 `<out>/claims/<job_id>.json`，内容含
  `owner`（默认 `<hostname>-<pid>`）、`deadline`、`attempt`；文件已存在说明已被其他 worker
  认领，当前 worker 跳过该行。原子创建是跨进程互斥的唯一依据，不依赖锁文件或数据库。
- **心跳续租**：执行期间启动 daemon 线程，每 `max(lease_seconds/3, 0.05)` 秒把 `deadline`
  推到 `now + lease_seconds`；任务结束（成功/失败）删除 claim。
- **过期回收**：worker 空闲时每个 `poll` 周期最多扫描一次 `claims/*.json`，发现
  `deadline < now` 的 claim 时：
  - `attempt < max_attempts` → 删除 claim，把原队列行（`attempt + 1`）追加到队尾重投；
  - `attempt >= max_attempts` → 删除 claim，直接写 `status="error"` 的结果文件，
    错误信息含"超过重投上限"，任务不再重投。

  回收前先尝试创建 `.reclaim-<job_id>.lock`，避免多个 worker 同时回收同一任务；队列行
  已被归档/删除时，孤儿 claim 直接清理。
- **语义边界**：租约保证的是"worker 崩溃后任务不会永久卡住"，不是"恰好一次"——
  原 worker 若只是失联但仍存活，重投会导致同一任务执行两次；远程算子应保持幂等，
  或自行在输出侧去重。deadline 基于 `time.time()`，跨机器使用需时钟大致同步。
- **队列压缩**：`python -m op_pipeline queue compact --queue <file> --out <dir>
  [--archive <file>]` 仅当**全部** job_id 都已有结果文件时，把原始队列行追加到归档
  （默认 `<队列 stem>.archive.jsonl`），再用同目录临时文件 + `os.replace` 原子清空
  活跃队列；存在未完成任务（或无法解析的行）时拒绝改动任何文件并返回非 0 退出码。

---

## 10. 调度器依赖触发（v0.9）：`--pipeline` 的 `after` 声明

`scheduler_app.scheduler.run_scheduler` 的多流水线条目（v0.8 起支持
`--pipeline` 可重复）自 v0.9 起支持两种形态：

```
--pipeline a.toml                          # 等价 {"path": "a.toml"}，无依赖
--pipeline '{"path": "b.toml", "after": ["a"]}'   # dict 条目，依赖 pipeline 名
--after b=a                                # CLI 追加依赖（可重复，与条目声明合并）
```

pipeline 名 = 配置文件去扩展名的 stem（`a.toml` → `a`）；CLI `--after` 按名匹配
追加到对应条目，排在条目自身 `after` 声明之后。每轮触发语义：

- **单轮拓扑顺序**：启动时对条目做一次拓扑排序——无依赖条目按声明序在前，
  有依赖条目按拓扑序随后；依赖成环启动即抛 `ConfigError`（信息含环链路）；
  仅**已知条目名**的依赖参与排序（未知依赖不构成环）；
- **触发条件**：轮内维护"本轮各流水线状态表"（`success` / `failed` / `error` /
  `skipped`），条目仅当**全部依赖本轮已运行且 status == success** 才触发；
  依赖本轮未运行、非 success、或被连锁跳过时，本条目跳过本轮并记
  **info** 日志（"跳过 pipeline=x：依赖未满足（…）"），自身记 `skipped`；
- **失败不中断**：依赖本轮 `failed` 时下游只跳过本轮，下一轮重新按各自实际
  结果判定——上一轮的失败不会跨轮累积；
- **启动期校验**：依赖名不在当前流水线集合时记 **warning**（不 fail，向后兼容；
  该依赖永远无法满足，条目将每轮跳过）；
- **兼容性**：无 `after` 的条目每轮照常触发（无依赖条目不受本轮状态表影响），
  行为与 v0.8.0 完全一致；告警/静默/keep-runs/落库等其余语义不变。

操作手册见 [operations.md §调度器](../guides/operations.md)。
