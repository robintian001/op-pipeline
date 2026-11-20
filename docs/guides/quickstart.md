# 快速上手

本文用最短路径跑通第一个流水线，然后写一个自定义算子。所有命令都在项目根目录
（`pyproject.toml` 所在目录）执行，Python 版本要求 **≥ 3.11**。

> 只想先看概念：算子是一个自描述的可复用单元，流水线只是算子之间的一份配置。
> 配置支持 JSON / TOML / YAML（YAML 需可选依赖 `pyyaml`），执行由引用推导的 DAG 决定，
> 与节点书写顺序无关。

---

## 1. 安装

```powershell
# 包本体零第三方依赖，可编辑安装到当前环境
python -m pip install -e .

# 需要哪些可选能力就装哪些 extras
python -m pip install -e ".[dataframe]"          # pandas：df.* 算子
python -m pip install -e ".[parquet]"            # pyarrow：Parquet / Arrow 算子
python -m pip install -e ".[excel]"              # openpyxl：Excel 算子
python -m pip install -e ".[mysql]"              # pymysql：db.read_mysql / db.write_mysql
python -m pip install -e ".[postgres]"           # psycopg2：db.read_postgres / db.write_postgres
python -m pip install -e ".[yaml]"               # pyyaml：.yaml / .yml 配置
python -m pip install -e ".[otel]"               # opentelemetry-api：observe.OtelHook
```

安装后两种入口等价：`op-pipeline <命令>` 与 `python -m op_pipeline <命令>`，本文统一用后者。
可选依赖未安装时算子**仍会注册**，只有真正调用该算子才报带安装提示的错误。

---

## 2. 第一个流水线

新建 `pipeline.toml`：

```toml
name = "hello"
version = "1.0"

[params]
who = "world"
out_dir = "out"

[[nodes]]
id = "greet"
op = "text.template"
  [nodes.params]
  template = "Hello, {{name}}!"
  [nodes.params.values]
  name = "${p.who}"

[[nodes]]
id = "write"
op = "io.write_text"
  [nodes.inputs]
  text = "${greet.text}"
  [nodes.params]
  path = "${p.out_dir}/hello.txt"

[outputs]
greeting = "${greet.text}"
```

等价的 JSON 写法（`pipeline.json`，内容同构）：

```json
{
  "name": "hello",
  "version": "1.0",
  "params": {"who": "world", "out_dir": "out"},
  "nodes": [
    {
      "id": "greet",
      "op": "text.template",
      "params": {
        "template": "Hello, {{name}}!",
        "values": {"name": "${p.who}"}
      }
    },
    {
      "id": "write",
      "op": "io.write_text",
      "inputs": {"text": "${greet.text}"},
      "params": {"path": "${p.out_dir}/hello.txt"}
    }
  ],
  "outputs": {"greeting": "${greet.text}"}
}
```

先校验再执行：

```powershell
python -m op_pipeline validate pipeline.toml
python -m op_pipeline run pipeline.toml
```

预期输出（`run_id`、耗时每次不同）：

```
pipeline : hello (v1.0)
run_id   : 20260719-101500-ab12
status   : success   耗时 0.02s

节点                  状态               耗时    尝试  算子
------------------------------------------------------------------------
greet               ✓ success     0.00s     1  text.template
write               ✓ success     0.01s     1  io.write_text

输出：
  greeting         str('Hello, world!')
```

同时会写出 `out/hello.txt`，内容为 `Hello, world!`。`greet.text` 是节点 `greet`
输出端口 `text` 的默认句柄名（`<节点 id>.<端口名>`），下游用 `${greet.text}` 取用。

常用开关：

```powershell
# 覆盖流水线参数；值先按 JSON 解析（3 是 int、true 是 bool）
python -m op_pipeline run pipeline.toml --param who="op_pipeline"

# 看依赖图与执行计划（不执行、不建产物目录）
python -m op_pipeline graph pipeline.toml --format ascii
python -m op_pipeline run pipeline.toml --dry-run

# 写 JSON 运行报告；关闭缓存
python -m op_pipeline run pipeline.toml --json run_report.json --no-cache

# 查已注册算子及其契约
python -m op_pipeline ops
python -m op_pipeline describe text.template
```

退出码：`success` / `partial` 为 0，`failed` 为 1，`validate` 校验失败为 2。

---

## 3. Python 内嵌调用

```python
from op_pipeline import PipelineSpec, default_registry, run_pipeline

spec = PipelineSpec.from_file("pipeline.toml")
report, values = run_pipeline(
    spec,
    registry=default_registry,
    workdir=".",             # 相对路径基准
    cache_dir=".pipeline_cache",  # 省略则为 None = 不启用缓存
)

print(report.summary())
print(values["greeting"])    # 已解析的流水线输出值
```

`run_pipeline(spec, ...)` 接收已解析的 `PipelineSpec`；若手上只有配置路径，
用便捷入口 `run()`（它同时负责按配置目录加载插件）：

```python
from op_pipeline import run

report, values = run("pipeline.toml", overrides={"who": "op_pipeline"})
print(report.status, report.run_id)
```

两条约定：

- `run_pipeline` 的 `cache_dir` 默认 `None`（库调用默认不碰磁盘缓存）；
  CLI 的 `--cache-dir` 默认 `.pipeline_cache`，`run()` 同样默认 `.pipeline_cache`；
- 返回值 `values` 是全部句柄值，`report` 是运行报告（状态、节点耗时/尝试/缓存命中、
  错误摘要）；产物目录默认在 `<workdir>/.op_pipeline/artifacts/<run_id>/`，惰性创建。

---

## 4. 写一个自定义算子

新建 `ops_extra.py`：

```python
from op_pipeline import Operator, Param, op


@op("demo.uppercase")                    # 注册名 = 配置里的 op 取值
class Uppercase(Operator):
    description = "转大写并按需截断"
    inputs = [("text", str)]             # 端口声明
    outputs = [("text", str)]            # 输出端口
    params = {
        "max_len": Param(int, default=0, minimum=0),  # 0 = 不截断
    }

    def run(self, ctx, text, max_len):
        value = text.upper()
        if max_len:
            value = value[:max_len]
        ctx.log(f"输出 {len(value)} 个字符")
        return {"text": value}
```

在配置里引用它（把这段追加到现有 `nodes` 列表末尾、`[outputs]` 之前；
`--plugin` 路径**相对配置文件所在目录**解析）：

```toml
[[nodes]]
id = "loud"
op = "demo.uppercase"
  [nodes.inputs]
  text = "${greet.text}"
  [nodes.params]
  max_len = 20
```

```powershell
python -m op_pipeline run pipeline.toml --plugin ops_extra.py
```

三条规则：

1. **签名即契约**：`run(self, ctx, <输入端口...>, <参数...>)`，名字必须与声明一致；
   多输出返回 dict（按端口名），单输出可返回裸值，返回未声明的键会报错；
2. **参数声明即校验**：未声明参数报错、必填缺失报错、类型可归一（如 `"3"` → `3`）；
3. **副作用只走 `ctx`**：日志用 `ctx.log()`，落盘路径用 `ctx.resolve_path()` /
   `ctx.artifact_path()`；有落盘副作用的算子应声明 `deterministic = False`，否则
   `cache = true` 命中时会跳过写文件。

也可以按模块路径加载插件：

```python
from op_pipeline import load_plugins

load_plugins(modules=["my_company.ops"], paths=["ops_extra.py"], base_dir=".")
```

插件包在自身 `pyproject.toml` 声明 `[project.entry-points."op_pipeline.ops"]` 并安装后，
CLI 会自动发现，无需 `--plugin`。更多扩展方式见
[扩展设计](../design/extension-points.md)。

---

## 5. 常见错误与排查

| 现象 | 常见原因 | 处理 |
| --- | --- | --- |
| `OperatorNotFoundError: 未注册的算子 '...'` | 算子名拼错；插件没加载；`--plugin` 相对路径找错 | `python -m op_pipeline ops` 查名字；确认 `--plugin` 相对**配置文件目录**；或先 `python -m op_pipeline validate <cfg>` |
| `SpecValidationError`（`validate` 退出码 2） | 节点 id 重复、连了未声明端口、缺必填输入、引用未知句柄、依赖成环、覆盖未定义参数 | 错误信息会一次性列出全部问题，按清单逐个修 |
| `ParamValidationError` / 参数相关报错 | 参数未在算子中声明、类型不合法、必填缺失、超出 `minimum/maximum` | `python -m op_pipeline describe <op>` 看参数表；含 `${...}` 的参数在运行期才校验 |
| `RefResolutionError` | 参数/环境变量缺失，或句柄下钻路径不存在 | 检查 `${p.x}` / `${env.X}` / `${node.port.key}` 的层级 |
| 缓存没生效 / 改了代码结果没变 | 缓存目录位置不对，或算子未标记 `deterministic` | 缓存目录默认相对**工作目录**（CLI 默认配置文件所在目录）；用 `--no-cache` 对比；源码变更会按源码指纹自动失效 |
| `ModuleNotFoundError` / 提示安装 extras | 调用了可选依赖算子 | 按错误提示安装对应 extras，如 `pip install -e ".[parquet]"` |
| 远程节点一直不结束 | 没有启动 worker 进程 | 启动 `python -m op_pipeline worker --queue <jobs.jsonl> --out <results>`，详见[运维指南](operations.md) |

---

## 6. 下一步

- [运维指南](operations.md)：scheduler_app 定时调度、告警与保留策略、Web UI、RunStore 与远程 worker 排障；
- [API 参考](../reference/api.md)：稳定公开 API、稳定性承诺与分组索引；
- [配置规范](../reference/config-schema.md)：顶层/节点字段、引用语法、表达式子集与 CLI；
- [算子清单](../reference/operator-catalog.md)：68 个内置算子的输入/输出/参数契约；
- [扩展设计](../design/extension-points.md)：插件信任、入口点发布、替换执行器/缓存/句柄存储。
