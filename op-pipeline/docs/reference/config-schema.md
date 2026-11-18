# 配置规范参考

配置格式支持 **JSON / TOML / YAML**（内容同构），此外可按需注册自定义格式。`.json` 与无扩展名走 `json`，
`.toml` 走标准库 `tomllib`，`.yaml/.yml` 需可选依赖 `pyyaml`。解析入口
[PipelineSpec.from_file()](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/spec.py#L402-L406)。

格式分发走 loader 注册表（M3-8）：内建 json/toml/yaml 即注册项；第三方格式通过
`PipelineSpec.register_loader(suffix, fn)` 扩展——`suffix` 为扩展名（可省略前导点、大小写不敏感），
`fn(path) -> dict` 返回配置映射。未知后缀报错并列出已注册格式；子配置文件（`includes`）同样按注册表分发。

---

## 1. 顶层字段

| 字段 | 类型 | 默认 | 说明 |
| --- | --- | --- | --- |
| `spec_version` | str | `"1"` | **配置格式版本**（M3-5）：不兼容变更必须升号并注册迁移器（`MIGRATIONS`）；缺省按 `"1"` 解析；高于框架当前版本报错提示升级；非数字报错 |
| `name` | str | `"pipeline"` | 流水线名，进入报告 |
| `version` | str | `"1"` | 流水线版本，进入报告 |
| `description` | str | `""` | 备注 |
| `params` | object | `{}` | 流水线参数；可被 `--param` 覆盖；值中可用 `${env.X}` |
| `nodes` | array | 必填 | 节点列表，**顺序无关**（执行由引用推导的依赖决定） |
| `outputs` | object | `{}` | 流水线对外产物：名字 → `${...}` 引用 |
| `includes` | array\<str\> | `[]` | **配置组合**（M3-7）：相对本文件所在目录的配置文件路径数组，按顺序加载并合并（`params`/`defaults`/`outputs` 逐键覆盖、`nodes` 拼接），主配置优先；节点 id 跨文件重复报错；子配置自身也可 `includes`（按自身目录解析）；循环引用报错；仅 `from_file` 或显式 `base_dir` 可用 |
| `defaults` | object | `{}` | 节点运行策略缺省值（M1-5 起生效）：可用键 `retries/retry_delay/timeout/cache/allow_failure/executor`，未知键报错；节点显式值优先 |

---

## 2. 节点字段

| 字段 | 类型 | 默认 | 说明 |
| --- | --- | --- | --- |
| `id` | str | 必填（也接受 `name`） | 节点唯一标识，同时是默认句柄前缀 |
| `op` | str | 必填 | 注册表中的算子名或别名 |
| `inputs` | object | `{}` | 端口名 → 引用，如 `{"table": "${load.table}"}` |
| `outputs` | object | `{}` | 端口名 → 自定义句柄名（避免冲突或简化引用） |
| `params` | object | `{}` | 算子参数，值可含 `${...}` 引用 |
| `when` | str | 无 | 布尔表达式，为假则跳过（下游连带跳过） |
| `retries` | int | `0` | 失败重试次数（总尝试 = retries + 1） |
| `retry_delay` | float | `0.0` | 退避基数：第 n 次重试前等待 `retry_delay * 2^(n-1)` 秒 |
| `timeout` | float | 无（`0` 视为无） | 秒级超时：线程模式为**软超时**（到点标记失败、不强杀）；`executor="process"` 时为**硬超时**（terminate 子进程） |
| `cache` | bool | `false` | 是否参与内容指纹缓存（算子须 `deterministic`） |
| `allow_failure` | bool | `false` | 失败不视为硬失败；报告为 `partial`，下游仍因缺输入跳过 |
| `executor` | `"thread"` / `"process"` | 无（先继承运行级，最终 `thread`） | 执行器选择（M2-2）：`process` 时算子在子进程执行（载荷须可 pickle，超时为硬超时）；未指定时沿用 `run(executor=...)` / CLI `--executor` |
| `description` | str | `""` | 备注（不参与执行） |

> **行为变更（M3-3，T15 治理）**：`table.select_columns` / `table.sort_rows` /
> `table.derive_column` / `table.join` 的列判定从"首行代表全表列"改为**全表列集合并集**。
> 影响：列只在非首行出现的参差行不再丢字段或误报——例如
> `select_columns(missing="error")` 不再因目标列"不在首行"而报错，会保留该列并给缺失行补 `None`；
> `sort_rows` 的排序键出现在任何一行即有效。正常（列齐整）表的执行结果不变。

---

## 3. 引用语法

| 写法 | 含义 | 解析时机 |
| --- | --- | --- |
| `${load.table}` | 上游节点输出（默认句柄 `id.port`） | 运行期 |
| `${raw}` | 在节点 `outputs` 里改过名的句柄 | 运行期 |
| `${stats.stats.最长行}` | 句柄下钻：dict 键 / list 下标，最长前缀优先 | 运行期 |
| `${p.min_amount}` / `${params.x}` | 流水线参数 | 编译期（参数表）/ 运行期 |
| `${env.HOME}` | 环境变量 | 编译期（参数表）/ 运行期 |
| `"${p.dir}/report.md"` | 字符串混排，逐段替换并转字符串 | 取决于所在字段 |

补充规则：

- 整个字符串只有**一个**引用时保留原始类型（`"${p.top_n}"` 得到 int），否则结果是 str；
- 正则 `\$\{([^{}]+)\}`，**不支持嵌套花括号**；
- 参数与环境变量引用**不产生依赖**，只有句柄引用才产生依赖；
- 引用解析失败在编译期表现为 `SpecValidationError`（静态目标不存在），运行期为 `RefResolutionError`。

---

## 4. 表达式子集

`when`、`table.filter_rows.where`、`table.derive_column.expr`、`control.assert.that` 共用同一求值器。

**支持**：字面量、变量名、`+ - * / // % **`、比较（含链式）、`and/or/not`、`in/not in`、`is/is not`、
条件表达式 `a if c else b`、列表/元组/字典/集合字面量、下标与切片、
映射点语法（`params.n`、`inputs.table.rows`——仅对映射做键下钻，键不存在即报错）、
函数 `len str int float bool abs min max sum sorted any all round isinstance`。

**扩展函数**：嵌入方可用 `register_function(name, fn)` 把领域函数（日期解析、正则辅助等）
加进白名单，`when`/表算子行级表达式/`control.assert` 均可用；约束：必须是纯函数
（表达式可能被多次求值）、名字须为合法标识符且**禁止覆盖内置名**、
同名的自定义函数重复注册视为替换。

**禁止**：任意对象属性访问（`x.__class__`；点语法仅限映射键下钻）、lambda、推导式、
赋值/语句、任何非白名单函数（含 `eval`/`open`/`__import__`）。

各处的环境变量：

| 场景 | 可用变量 |
| --- | --- |
| `when` | 输入端口名、参数名、`inputs`（仅成功解析的端口）、`params`（解析后的值）、`raw_params`（原始配置值） |
| 表算子行级表达式 | 行字段名（标识符形式）、`row`（整行 dict）、`i`（行号） |
| `control.assert` | `value`、节点参数名、`value` 为 dict 时其顶层键 |

---

## 5. 参数与参数归一

算子参数由 `Param` 声明，未声明的参数**直接报错**（防拼写错误静默失效）。类型归一规则：

| 声明类型 | 归一 |
| --- | --- |
| `bool` | `"true/yes/1/on"`、`"false/no/0/off"`、0/1 → bool |
| `int` | 数字字符串（含 `"3.0"`）→ int；bool 拒绝 |
| `float` | 数值或数字字符串 → float；bool 拒绝 |
| `str` | 数值/布尔 → str |
| `(str, list)` 等 tuple | 属于其一即通过 |
| `Any` | 不校验 |

之后检查 `choices`、`minimum`、`maximum`。完整实现见
[params.coerce()](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/params.py#L64-L137)。

---

## 6. 校验与错误

**未知键告警**：`from_dict` 对顶层与节点里不认识的键发 `logging.warning`（不报错，
保证前向兼容），消息附带近义键建议——例如把 `params` 拼成 `param` 会看到
`流水线配置 含未知键 'param'（将被忽略）；是否想写 'params'？`。合法键集合见
[spec.py](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/spec.py) 中 `NodeSpec._KNOWN_KEYS` 与 `PipelineSpec._KNOWN_KEYS`；
CLI 下 WARNING 会经 stderr 直接展示（`validate` 同样可见）。

编译期（`build_plan`）会一次性汇总以下问题，抛 `SpecValidationError`：

1. 覆盖了未定义的流水线参数；
2. 节点 id 重复；
3. 算子未注册（附带近似名建议）；
4. 参数名未声明 / 参数值不合法（无引用时在编译期校验）；
5. 重命名了未声明的输出端口；
6. 句柄被两个节点同时占用；
7. 连接了未声明的输入端口 / 缺少必填输入端口；
8. 引用了未知句柄 / 自引用；
9. 流水线输出引用了未知句柄；
10. 依赖图存在环。

运行期错误：

| 异常 | 触发 |
| --- | --- |
| `RefResolutionError` | 运行期引用无法解析（参数/环境变量缺失、下钻路径不通） |
| `ParamValidationError` | 含引用的参数在运行期解析后类型不合法 |
| `PipelineError` | 算子输入/输出契约被破坏 |
| 其他异常 | 算子内部错误，按 `retries` 重试，最终标记节点失败 |

---

## 7. CLI

```
python -m op_pipeline run      <config> [选项]
python -m op_pipeline validate <config>
python -m op_pipeline graph    <config> [--format mermaid|dot|ascii]
python -m op_pipeline ops      [--json]
python -m op_pipeline describe <op_name>
```

`run` 选项：`--workdir`、`--workers`、`--cache-dir`（默认 `.pipeline_cache`）、`--no-cache`、
`--fail-fast`、`--dry-run`、`--json PATH`、`--plugin PATH`（可多次，相对配置文件所在目录解析）、`--param KEY=VALUE`（可多次）、`-v`。

| 退出码 | 含义 |
| --- | --- |
| 0 | `success` / `partial` |
| 1 | `failed` |
| 2 | `validate` 校验失败 |

`--param` 的值先尝试 `json.loads`（因此 `n=3` 是 int、`flag=true` 是 bool、`x=[1,2]` 是 list），
失败后按字符串处理。

---

## 8. 目录与产物

| 路径 | 来源 | 说明 |
| --- | --- | --- |
| 工作目录 | `--workdir`，默认配置文件所在目录 | `ctx.resolve_path()` 的相对基准 |
| `.op_pipeline/artifacts/<run_id>/<node_id>/` | `RunContext.create()` | 惰性创建（算子首次使用产物才建目录；可用 `artifacts_dir` 参数覆盖位置） |
| `.pipeline_cache/*.pkl` | `--cache-dir` / `run(cache_dir=...)` | **相对工作目录解析**（M1-2 起，与进程 CWD 无关）；绝对路径原样使用；`--no-cache` / `cache_dir=None` 即禁用 |
| `--json PATH` | `RunReport.to_json()` | 自动创建父目录 |

---

## 9. 完整示例（TOML）

```toml
name = "orders_daily"
version = "1.0"

[params]
input_path = "data/orders.csv"
min_amount = 20
top_n = 8

[[nodes]]
id = "load_orders"
op = "io.read_csv"
cache = true
  [nodes.params]
  path = "${p.input_path}"

[[nodes]]
id = "clean"
op = "table.filter_rows"
cache = true
  [nodes.inputs]
  table = "${load_orders.table}"
  [nodes.params]
  where = "status == 'paid' and amount >= ${p.min_amount}"

[[nodes]]
id = "agg"
op = "stats.aggregate"
  [nodes.inputs]
  table = "${clean.table}"
  [nodes.params]
  group_by = "region"
  [nodes.params.metrics]
  amount = ["sum", "mean"]
  order_id = "count"

[[nodes]]
id = "top"
op = "table.limit"
when = "n > 0"
  [nodes.inputs]
  table = "${agg.table}"
  [nodes.params]
  n = "${p.top_n}"

[outputs]
groups = "${agg.groups}"
```

更完整的示例见仓库 `examples/` 目录（`pipeline_orders.json` 18 节点含 fan-out，
`pipeline_orders.toml` 11 节点主链路，`pipeline_text.toml` 插件 + 重试 + 句柄下钻）。
