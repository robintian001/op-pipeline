# 核心模块详细设计

本文按模块给出：**职责 → 关键类型 → 关键逻辑 → 契约/校验 → 坑位**。
运行时行为的总览见 [运行时语义设计](../architecture/runtime-semantics.md)。

---

## 1. operator.py：算子基类与执行契约

### 1.1 声明侧

`Operator` 类属性即契约（[operator.py#L59-L66](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/operator.py#L59-L66)）：

| 类属性 | 默认 | 说明 |
| --- | --- | --- |
| `name` | `""` | 注册时由 `@op` 写入 |
| `version` | `"1"` | 参与缓存 key，逻辑变更时应手动递增 |
| `description` | `""` | 缺省取类 docstring 首行 |
| `inputs` / `outputs` | `()` | 支持 `Port` / `str` / `dict` / `tuple` 多种写法，注册时归一 |
| `params` | `{}` | 支持 `Param` 与裸值混写 |
| `param_table` | 由 `params` 归一得到 | 运行期真正使用的参数声明表 |
| `deterministic` | `True` | 决定是否允许参与缓存 |

`normalize_operator_class()`（[operator.py#L20-L44](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/operator.py#L20-L44)）
在**注册时**执行一次归一化，并做两项硬校验：

1. 端口名必须是合法 Python 标识符且非关键字（端口名会成为 `run()` 的关键字参数）；
2. 输入端口与参数**不能同名**（二者都会以关键字参数传给 `run()`）。

这两条把"注册算子"变成一个自检点，错误在导入期就暴露，而不是首次运行时。

### 1.2 执行侧

`execute()` 是框架唯一调用入口（[operator.py#L118-L127](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/operator.py#L118-L127)）：

```
execute(ctx, inputs):
    prepared = _prepare_inputs(inputs)         # 未声明/缺失端口 → PipelineError
    kwargs   = {**self.params, **prepared}     # 参数先、输入后（端口不能与参数同名）
    result   = self.run(ctx, **kwargs)         # async def run → asyncio.run(...)
    return _normalize_output(result)
```

- `_prepare_inputs`：未声明的输入直接拒绝；非 optional 且未接线的端口报缺失；
  optional 端口补 `None`（[operator.py#L130-L146](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/operator.py#L130-L146)）。
- `_normalize_output`：`None` 视为空 dict；非 Mapping 且只有一个输出端口时视为该端口裸值；
  未声明的输出拒绝；非 optional 输出缺失拒绝（[operator.py#L148-L170](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/operator.py#L148-L170)）。

**设计取舍**：契约校验放在 `execute()` 而不是 `run()` 内部，保证"任何调用路径（引擎或单测）都受同一约束"，
而算子作者只需专注纯函数式实现。

### 1.3 坑位

- `asyncio.run()` 在每个节点内部新建事件循环；若算子内部已有运行中的 loop（例如被嵌入 async 服务），会抛错。
- 参数与输入合并时输入优先，但归一化已禁止同名，因此顺序只是防御性写法。
- 算子不应直接读 `os.environ`，而应通过流水线参数 `${env.X}` 注入，否则配置不可复现。

---

## 2. ports.py / params.py：契约的两种数据

### 2.1 Port（软类型）

`Port(name, type=Any, optional=False, doc="")` 是冻结 dataclass。`as_ports()` 支持四种写法
（[ports.py#L39-L74](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/ports.py#L39-L74)）：

```python
"table"                                  # 单个简写
["table", "count"]                       # 多个简写
{"table": list, "count": int}            # 名字 → 类型
[Port("table", list), ("count", int)]    # 混写，(name, type[, optional])
```

类型检查是**软校验**：`value_matches()` 只做 `isinstance`，`None` 与 `Any` 永远通过。
`port_warnings()` 与 [check_handle_types()](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/spec.py#L518-L546)
已实现，但**当前未被引擎或 CLI 调用**（见 [技术债 §5](../design/extension-points.md#5-已知技术债与实现缺口)）。

### 2.2 Param（参数声明）

```python
Param(type=Any, default=_UNSET, doc="", choices=(), minimum=None, maximum=None)
```

- `default is _UNSET` ⇒ 必填；给了默认值 ⇒ 可选并自动 `deepcopy` 补齐；
- 裸值声明（如 `{"limit": 10}`）等价于 `Param(type(10), default=10)`。

### 2.3 参数归一规则（coerce）

| 声明类型 | 接受的输入 | 转换 |
| --- | --- | --- |
| `bool` | bool / `"true|yes|1|on"` / `"false|no|0|off"` / 0,1 | 转 bool；其他报错 |
| `int` | int / 整数 float / 数字字符串（`"3"`、`"3.0"`） | 转 int；bool 明确拒绝 |
| `float` | int / float / 数字字符串 | 转 float；bool 明确拒绝 |
| `str` | str / int / float / bool | `str(value)` |
| `tuple`（联合类型） | 实例属于该 tuple | 原样 |
| 其他 `type` | `isinstance` | 原样 |
| `Any` / `None` | 任意 | 原样 |

随后统一检查 `choices`（成员）与 `minimum` / `maximum`（仅数值）。
来源：[params.py#L64-L137](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/params.py#L64-L137)。

**为什么这么设计**：配置来自 TOML/JSON/CLI，天然是字符串或宽类型；有限且明确的归一
（尤其 `"3" -> 3`）比引入 pydantic 更符合"零依赖、可嵌入"的目标。

**坑位**：`int` 的字符串转换对 `"3.9"` 会走 `int(float(...))` 截断为 3（不报错）；
`float` 不接受 `"nan"` 之外的非法串；`list`/`dict` 参数不做元素级校验。

---

## 3. registry.py：注册表与插件发现

### 3.1 Registry

```python
Registry.register(cls, name=None, *, aliases=(), replace=False)
Registry.get(name) -> type        # 别名先行归一；未命中给 difflib 近似建议
Registry.describe() -> list[dict] # 供 `ops --json` 使用
```

- 注册即归一化（调用 `normalize_operator_class`）；
- 重名默认报 `DuplicateOperatorError`，测试与覆盖场景显式 `replace=True`；
- 别名只做单向映射到 canonical name，`__contains__` / `get` 均归一。

### 3.2 `@op` 装饰器

```python
@op("table.filter_rows", aliases=["filter"])   # 显式命名
@op()                                          # 用类名作为注册名
@op                                            # 直接作用于类（name 参数是类本身）
```

### 3.3 插件加载

`load_plugins(modules=(), paths=(), base_dir=None, package="op_pipeline.ops")`
（[registry.py#L119-L166](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/registry.py#L119-L166)）：

1. 先 `import_module(package)` 加载内置算子包（找不到时静默跳过，不阻断自定义插件）；
2. `modules` 走标准 import；
3. `paths` 支持相对 `base_dir` 的文件，模块名固定为 `_op_plugin_<stem>`：

```python
mod_name = f"_op_plugin_{path.stem}"
if mod_name in sys.modules:      # 以 stem 去重，而不是按绝对路径
    continue                    # 同名不同目录的第二个插件会被静默跳过
```

**风险**：不同目录下同名 `.py` 插件（如两个 `my_ops.py`）只会加载第一个。
插件应使用唯一文件名，或改用 `modules=` 走正规包路径。

内置算子通过 [ops/\_\_init\_\_.py](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/ops/__init__.py) 的 import 副作用注册；
[`__init__.py`](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/__init__.py#L90-L92) 在包导入时调用一次 `load_builtin_ops()`。

---

## 4. spec.py：配置到计划的编译器

### 4.1 数据模型

| 类型 | 字段 | 说明 |
| --- | --- | --- |
| `NodeSpec` | `id/op/inputs/outputs/params/when/retries/retry_delay/timeout/cache/allow_failure/description` | 单节点的全部静态信息；`timeout=0` 视为未设置 |
| `PipelineSpec` | `name/description/version/params/nodes/outputs/defaults` | `defaults` 目前仅解析并透传，**未参与执行** |
| `PlanNode` | `spec/operator_cls/operator/resolved_inputs/handles/depends_on` | `operator` 为编译期实例（含引用时为 `None`）；`resolved_inputs` 恒为 `{}` |
| `Plan` | `spec/params/nodes/order/handle_owner/pipeline_outputs` | 引擎唯一输入；`node(id)` 线性查找 |

`defaults` 与 `resolved_inputs` 属于预留字段，当前无消费方，修改时不要假设它们有效。

### 4.2 RefResolver

支持三类引用与两种形态：

| 写法 | 解析规则 |
| --- | --- |
| `${p.name}` / `${params.name}` | 流水线参数，未定义即报错 |
| `${env.NAME}` | 环境变量，未设置即报错 |
| `${node.port}` | 句柄精确命中 |
| `${node.port.field.0}` | 最长前缀命中句柄后逐段下钻（dict 键 / 列表下标，负下标合法） |
| `"${p.dir}/x.md"` | 字符串内混排，逐段替换为 `str()` |
| 整串是单一引用 | 保留原始类型（不转字符串） |

源码：[spec.py#L187-L284](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/spec.py#L187-L284)。

**已知限制**：引用正则 `\$\{([^{}]+)\}` 不支持嵌套花括号，因此无法在 `${}` 内写 dict 字面量。

### 4.3 编译主流程

见 [运行时语义 §1](../architecture/runtime-semantics.md#1-阶段-a编译期的五类校验)，六个阶段依次是：
参数合并 → id 唯一 → 算子/参数/句柄 → 引用与依赖 → 流水线输出 → 环与拓扑。

### 4.4 拓扑排序

Kahn 算法，`ready` 列表每轮排序，保证"同层按名字"的确定性输出
（[spec.py#L490-L515](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/spec.py#L490-L515)）。
确定性对快照测试与 `graph` 输出很重要。

---

## 5. expression.py：安全表达式

### 5.1 白名单

**允许的语法节点**：`Expression`、`BoolOp(And/Or)`、`UnaryOp(Not/USub/UAdd)`、
`BinOp(+, -, *, /, //, %, **)`、`Compare(Eq/NotEq/Lt/LtE/Gt/GtE/In/NotIn/Is/IsNot)`、
`Name`、`Constant`、`Call` + `keyword`、`IfExp`、`List/Tuple/Dict/Set`、`Subscript`、`Slice`、
`Attribute`（仅映射键下钻：`params.n` / `inputs.table.rows`，目标不是映射或键不存在即报错）。

**允许的函数**：`len str int float bool abs min max sum sorted any all round isinstance`；
另可通过 `register_function(name, fn)` 注册领域函数（纯函数、合法标识符、禁止覆盖内置名，
全局生效需在加载配置前调用，见 [expression.py#L179-L206](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/expression.py#L179-L206)）。

**被明确阻断**：任意对象属性访问（`x.__class__`——点语法只做映射下钻，无属性逃逸面）、
`Lambda`、推导式、`eval/open/__import__` 等任意调用、
赋值与语句（只解析 `mode="eval"`）。测试用例见
[tests/test_op_pipeline.py#L116-L126](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/tests/test_op_pipeline.py#L116-L126)。

### 5.2 求值语义细节

- `Name` 未在 env 中即报错（不会静默变 `None`）；
- `BoolOp` 实现短路并返回**最后一个求值结果**（Python 语义），不是强制 bool；
- 比较链 `a < b < c` 按 Python 语义逐段求值；
- `evaluate_condition()` 才做 `bool()` 转换。

### 5.3 使用方

| 使用方 | 环境变量来源 |
| --- | --- |
| 节点 `when` | 输入端口值、参数值、`inputs`、`params`（解析后）、`raw_params`（原始值）（见 [engine.py#L348-L366](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/engine.py#L348-L366)） |
| `table.*` 行级表达式 | `row_env()`：字段名（标识符形式）+ `row` + `i` |
| `control.assert` | `value` + 节点参数 + dict 值展平 |

---

## 6. cache.py：内容指纹缓存

`fingerprint(value)` 是对任意值做稳定摘要的纯函数（不依赖 pickle，逐类型构造字符串后 blake2b）；
`CacheStore` 是薄封装：`compute_key / load / store / clear / enabled`。

设计要点与边界见 [运行时语义 §4](../architecture/runtime-semantics.md#4-缓存语义)，此处只补充实现细节：

- 递归深度上限 6（`_depth > 6` 直接 `repr` 截断 200 字符），防止环状/超深结构爆栈；
- 集合排序后再指纹，保证 `set` 顺序无关；
- 字典按 `str(key)` 排序，保证跨进程稳定；
- 写入用 `tmp + os.replace`，读取异常降级为未命中。

---

## 7. context.py：运行上下文

```python
RunContext.create(pipeline, params, workdir, run_id=None, logger=None, cache=None)
  → run_id 默认 "%Y%m%d-%H%M%S"
  → artifacts_dir = workdir/.op_pipeline/artifacts/<run_id>（惰性创建，首次使用产物才建目录）

NodeContext（由 for_node 创建）
  ctx.log / ctx.artifact_path / ctx.save_artifact / ctx.resolve_path
  ctx.params  ← 引擎在解析参数后回填
  ctx.run_id / ctx.pipeline_params
```

`save_artifact` 只支持 `dict/list/str/bytes`（[context.py#L51-L62](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/context.py#L51-L62)）。
`NodeContext.logger` 通过 `logger.getChild(node_id)` 派生，日志天然带节点名。

**为什么算子必须走 ctx**：单测时可用假 ctx 替代，避免真实文件系统与全局日志；
将来换成进程池/远程执行器时，只需替换 ctx 实现，算子代码不动。

---

## 8. graph.py / cli.py：外围

### 8.1 graph

三种渲染：`to_mermaid`（`flowchart LR`，缓存节点加 class）、`to_dot`（Graphviz）、
`to_ascii`（编号 + 依赖 + 运行策略标记）。
`to_mermaid` 中 `style` 变量只写不读（[graph.py#L15-L22](file:///C:/Users/ZTIAN12/PycharmProjects/op-pipeline/op-pipeline/op_pipeline/graph.py#L15-L22)），属于无害死代码。

### 8.2 cli

```
op_pipeline run      <config> [--workdir] [--workers] [--cache-dir] [--no-cache]
                              [--fail-fast] [--dry-run] [--json PATH] [--plugin P] [--param K=V]
op_pipeline validate <config>
op_pipeline graph    <config> --format mermaid|dot|ascii
op_pipeline ops      [--json]
op_pipeline describe <op_name>
```

- `--param` 取值先尝试 `json.loads`，失败再判断 `true/false`，否则按字符串
  （因此 `--param n=3` 得到 int，`--param name=abc` 得到 str）；
- `--dry-run` 打印 ASCII 计划后返回 0，不执行任何节点（注意：运行上下文与产物目录在此之前已创建）；
- 插件在**解析配置之后、编译之前**加载，因此插件注册的算子可被编译期识别。

---

## 9. ops/：内置算子库的组织约定

| 文件 | 分组 | 算子数 |
| --- | --- | --- |
| `io_ops.py` | 文件读写 | 6 |
| `table_ops.py` | 行字典表处理 | 11 |
| `stats_ops.py` | 聚合与描述统计 | 4 |
| `text_ops.py` | 文本处理 | 5 |
| `report_ops.py` | Markdown / HTML 报告 | 2 |
| `control_ops.py` | 常量、分支、断言、合并 | 6 |

约定：

1. **表的通用交换格式是 `list[dict]`**。选择它而非 DataFrame，是为了零依赖、可 pickle、可 JSON 序列化；
   需要 pandas 时写 adapter 算子，编排层不变。
2. 每个算子的 `ctx.log()` 只打印规模/关键指标，不打印数据本身。
3. 参数尽可能用 `choices` / `minimum` 表达合法域，把错误前移到参数校验。
4. 算子内部抛出的异常只需是普通异常（引擎统一包装），但"配置语义错误"应抛 `ValueError`/`KeyError`
   而不是 `ConfigError`，以免被误判为不可重试。

各算子的输入/输出/参数契约见 [算子清单](../reference/operator-catalog.md)。
