# 内置算子契约清单

共 40 个算子，按 `ops/` 下的文件分组。字段约定：

- **入 / 出**：`名字:类型`，`?` 表示 optional（未接线时收到 `None`）；
- **参数**：`*` 表示必填；未标默认值即必填；
- 所有算子均可用 `python -m op_pipeline describe <name>` 查看机器可读契约。

---

## 1. IO（io_ops.py，6 个）

| 算子 | 入 | 出 | 参数 |
| --- | --- | --- | --- |
| `io.read_csv`（别名 `read_csv`） | — | `table:list` `rows:int` `path:str` | `path*`、`delimiter=","`、`encoding="utf-8-sig"`、`infer_types=true`、`sample=0` |
| `io.write_csv`（`write_csv`） | `table` | `path:str` `rows:int` | `path*`、`columns=None`、`encoding="utf-8-sig"`、`delimiter=","` |
| `io.read_json`（`read_json`） | — | `data:any` `table:list?` `path:str` | `path*`、`encoding="utf-8"` |
| `io.write_json`（`write_json`） | `data:any` | `path:str` `bytes:int` | `path*`、`indent=2`（0–8）、`ensure_ascii=false` |
| `io.read_text`（`read_text`） | — | `text:str` `length:int` | `path*`、`encoding="utf-8"` |
| `io.write_text`（`write_text`） | `text` | `path:str` `length:int` | `path*`、`encoding="utf-8"` |

行为要点：

- **CSV 类型推断**：`infer_types=true` 时，空串 → `None`，`true/false` → bool，整数/浮点 → int/float，其余保持字符串；
- `read_csv.sample=N` 只读前 N 行，用于预览大文件；
- `read_json` 仅在顶层是 `list[dict]` 时同时给出 `table` 视图，否则 `table=None`（下游需用 `${x.data}`）；
- 所有路径都经 `ctx.resolve_path()`，相对路径基准是工作目录；
- 写出前自动创建父目录；CSV 默认 `utf-8-sig`（Excel 直接打开不乱码）。

---

## 2. 表（table_ops.py，11 个）

| 算子 | 入 | 出 | 参数 |
| --- | --- | --- | --- |
| `table.filter_rows`（`filter`） | `table` | `table:list` `dropped:int` | `where*`（行级布尔表达式） |
| `table.select_columns`（`select`） | `table` | `table:list` | `columns*:list`、`missing="error"`（`error`/`ignore`） |
| `table.rename_columns` | `table` | `table:list` | `mapping*:dict`（旧名→新名） |
| `table.drop_columns` | `table` | `table:list` | `columns*:list` |
| `table.derive_column` | `table` | `table:list` | `name*`、`expr*`、`overwrite=true` |
| `table.sort_rows` | `table` | `table:list` | `by*`（str 或 list）、`desc=false` |
| `table.dedupe` | `table` | `table:list` `dropped:int` | `keys=None`（None 表示整行）、`keep="first"`（`first`/`last`） |
| `table.limit` | `table` | `table:list` | `n*:int`、`offset=0` |
| `table.join` | `left` `right` | `table:list` `matched:int` | `left_key*`、`right_key=None`、`how="inner"`（`inner`/`left`/`right`/`full`）、`suffix="_right"` |
| `table.union` | `left` `right` | `table:list` | `distinct=false` |
| `table.take_column` | `table` | `values:list` `count:int` | `column*`、`skip_null=true` |

行为要点与坑：

- **通用表格式**：`list[dict]`，字段名是字符串。多个算子以 `rows[0].keys()` 作为"全表列集"，
  若各行键不一致，可能丢列或报列不存在（参见 [技术债 T15](../design/extension-points.md#5-已知技术债与实现缺口)）。
- **行级表达式环境**：字段名直接可用，另有 `row`（整行）与 `i`（行号）。
- **排序**：`None` 在升序时排最后（通过 `(value is None, 归一化值)` 实现）；
  `desc=true` 会整体反转，因此 `None` 实际排最前。不同列类型会被 `_normalize()` 转成可比较值。
- **join**：
  - 哈希连接，右表按 `right_key`（缺省同 `left_key`）建索引；
  - 右表与左表同名列（且非同键）加 `suffix`；
  - `matched` 是**输出行数**，不是匹配对数；
  - `right`/`full` 连接中未匹配的右表行，其左表列为 `left_rows[0]` 的键（左表为空时为空 dict）。
- **derive_column**：`overwrite=false` 且列已存在时报错；表达式结果直接写入，不做类型推断。
- **dedupe**：`keys=None` 时按整行（键排序后）判重；`keep="last"` 会原位替换。
- **select_columns**：空表直接返回空表，不校验列名。

---

## 3. 统计（stats_ops.py，4 个）

聚合函数全集：`sum mean count min max median std nunique first last`。
其中 `std` 使用**总体标准差**（`statistics.pstdev`），`mean/std/sum` 会忽略非数值与 bool。

| 算子 | 入 | 出 | 参数 |
| --- | --- | --- | --- |
| `stats.aggregate`（`groupby`） | `table` | `table:list` `groups:int` | `group_by=None`（str/list，None 表示全表一组）、`metrics*:dict` |
| `stats.count_rows` | `table` | `count:int` `empty:bool` | — |
| `stats.value_counts` | `table` | `table:list` `total:int` | `column*`、`top=0`（0 表示全部） |
| `stats.describe` | `table` | `table:list` | `columns=None`（None 自动识别数值列） |

行为要点：

- `metrics` 写法：`{"amount": ["sum","mean"], "order_id": "count"}`，
  输出列名为 `"{列}_{函数}"`（如 `amount_sum`、`order_id_count`）；
- 聚合结果按分组列字符串排序，保证输出稳定；
- `value_counts` 输出行含 `value / count / share`，`share` 为四位小数；
- `describe` 输出 `column / count / nulls / sum / mean / min / max / std / median`；
- 无数值时统计量返回 `None`（不报错）。

---

## 4. 文本（text_ops.py，5 个）

| 算子 | 入 | 出 | 参数 |
| --- | --- | --- | --- |
| `text.template` | — | `text:str` `length:int` | `template*`、`values=None` |
| `text.split_lines` | `text` | `lines:list` `count:int` | `strip=true`、`skip_empty=true` |
| `text.join_lines` | `lines` | `text:str` `length:int` | `sep="\n"` |
| `text.regex_extract` | `text` | `matches:list` `count:int` | `pattern*`、`group=0`、`ignore_case=false` |
| `text.replace_regex` | `text` | `text:str` `replacements:int` | `pattern*`、`replacement*`、`count=0`（0 表示全部） |

行为要点：

- 模板占位符是 `{{key}}`，与流水线的 `${}` 语法刻意区分；**存在未提供取值的占位符即抛 `KeyError`**；
- `regex_extract` 指定 `group>0` 时只收集该捕获组存在且足够的匹配；
- 正则不做输入校验，非法 pattern 会在运行期抛 `re.error`（可按需重试）。

---

## 5. 报告（report_ops.py，2 个）

| 算子 | 入 | 出 | 参数 |
| --- | --- | --- | --- |
| `report.markdown` | — | `text:str` `length:int` | `title*`、`blocks=None`、`meta=None`、`generated_at=""` |
| `report.html` | — | `text:str` `length:int` | `title*`、`blocks=None`、`subtitle=""` |

`blocks` 是列表，每项按出现的键判定类型（顺序：`heading` → `kv` → `table` → `text`）：

| 块形态 | 字段 | 渲染 |
| --- | --- | --- |
| 标题 | `{"heading": str, "level": 2}` | Markdown 中 level 夹在 1–6；HTML 中夹在 2–4 |
| 键值 | `{"title": str?, "kv": {k: v}}` | Markdown 列表；HTML 为自适应网格卡片 |
| 表 | `{"title": str?, "table": [...], "columns": [...], "limit": 0}` | Markdown 表格，超限追加"已截断"说明；HTML 表格 |
| 文本 | `{"text": str}` | 原样段落 |

其他：

- Markdown 自动加标题、生成时间与 `run_id`、`meta` 键值列表；
- HTML 是**自包含单文件**（内联 CSS，无外部依赖），所有内容经 `html.escape()` 转义；
- 表格单元格中的 `|` 会被转义为 `\|`，换行替换为空格。

---

## 6. 控制（control_ops.py，6 个）

| 算子 | 入 | 出 | 参数 |
| --- | --- | --- | --- |
| `control.const`（`const`） | — | `value:any` | `value*:any` |
| `control.if_else` | `condition` `when_true` `when_false?` | `value:any` `branch:str` | `as_bool=true` |
| `control.switch` | `key:any` | `value:any` `branch:str` | `cases*:dict`、`default=None` |
| `control.assert` | `value:any` | `value:any` | `that*`、`message="断言失败"` |
| `control.passthrough` | `value:any` | `value:any` | — |
| `control.merge_dicts` | `left:dict` `right:dict` | `value:dict` | `deep=false` |

行为要点：

- `const` 是把流水线参数/常量引入数据流的唯一手段（其 `value` 可含 `${...}` 引用与嵌套结构）；
- `if_else` 的 `when_true` **不是** optional，两个分支都必须接线；只有 `when_false` 可以缺省（得到 `None`）；
  `as_bool=false` 时按原值真假判断并原样输出；
- `switch` 用**字符串化**后的键做匹配（`str(case_key) == str(key)`），命中返回对应值，否则 `default`；
- `assert` 失败抛 `AssertionError`（属于普通异常，会被 `retries` 重试——数据质量门禁场景应设 `retries=0`）；
  表达式环境含 `value`、节点参数名，`value` 为 dict 时其顶层键也被展开；
- `merge_dicts` 浅合并时后者覆盖前者，`deep=true` 时对嵌套 dict 递归合并；
- `passthrough` 常用于并行扇出给同一数据流起别名。

---

## 7. HTTP（http_ops.py，1 个）

| 算子 | 入 | 出 | 参数 |
| --- | --- | --- | --- |
| `http.request` | — | `status:int` `headers:dict` `json:any?` `text:str` `url:str` `ok:bool` | `url*`、`method="GET"`、`headers=None`、`timeout=10.0`（≥0.1）、`body=None`、`json_body=None` |

行为要点：

- 仅标准库 `urllib` 实现，**零第三方依赖**；
- `method` 只接受 `GET`/`POST`，其余值抛 `ConfigError`（配置错误，**不重试**）；
- `json` 视图规则：`Content-Type` 含 `json`，或响应体以 `{`/`[` 开头时尝试 `json.loads`，解析失败返回 `None`；
- `HTTPError`（4xx/5xx）也是"有响应的失败"：返回 `status`/`headers`/`text`（含 `json` 视图），但 `ok=False`；
- 连接失败（`URLError`）视为节点失败并正常走 `retries` 重试策略；
- `POST` 时 `json_body` 自动序列化并设 `Content-Type: application/json`，`body` 为原始字符串请求体；
- **`deterministic=False`**（外部状态）：结果永不落入节点缓存。

---

## 8. SQLite（db_ops.py，2 个）

标准库 `sqlite3` 实现，**零第三方依赖**。每个节点每次运行各自打开/关闭一个连接；
`db` 为相对工作目录的文件路径，`:memory:` 表示单节点内存库（跨节点不共享，闭环需用文件库）。

| 算子 | 入 | 出 | 参数 |
| --- | --- | --- | --- |
| `db.sqlite_exec` | `gate:any?` | `rowcount:int` `lastrowid:int?` | `db*`、`query*`、`params=None` |
| `db.sqlite_query` | `gate:any?` | `rows:list` `count:int` `columns:list` | `db*`、`query*`、`params=None` |

行为要点：

- `params` 为位置参数列表，按 `?` 占位符绑定（防注入）；
- `sqlite_exec` 返回 `rowcount`（DDL 语句 sqlite3 约定为 `-1`）与 `lastrowid`（无自增行时为 `None`）；
- `sqlite_query` 的 `rows` 为行字典表（列名 -> 值），`columns` 为列名列表，`count` 为行数；
- 引擎按依赖边调度：**写后读的节点必须显式接线**（可选输入端口 `gate`，接上游任意手柄）
  保证顺序，否则多个 db 节点会并行执行而互相踩踏；
- SQL 执行失败（如表不存在）抛 `sqlite3.OperationalError`，节点失败并走 `retries` 策略；
- **`deterministic=False`**（外部状态）：读写均改变/依赖库内容，永不落入节点缓存。

---

## 9. DataFrame（dataframe_ops.py，3 个）

pandas 为**可选依赖**（`pip install op-pipeline[dataframe]`），仅在 `run()` 内延迟导入：
未安装时算子照常注册，调用才报错。`df` 句柄是运行时内存对象（DataFrame），不可序列化。

| 算子 | 入 | 出 | 参数 |
| --- | --- | --- | --- |
| `df.from_table` | `table:list` | `df:any` `shape:tuple` | `columns=None`、`index_column=None` |
| `df.to_table` | `df:any` | `table:list` `rows:int` `columns:list` | `drop_index=true`、`column_order=None` |
| `df.to_csv` | `df:any` | `path:str` `rows:int` | `path*`、`index=false`、`encoding=utf-8` |

行为要点：

- `from_table` 的 `columns=None` 时按全表列集合并集顺序建列（与表算子 T15 治理一致）；
- `to_table` 把 pandas 空值（NaN/NaT）还原为 `None`，numpy 标量转原生 Python 类型，
  保证"`list[dict]` ↔ DataFrame 往返"逐值一致；
- `to_csv` 是 pandas 实现（可写行索引列），与 `io.write_csv`（纯标准库、行字典表）互补；
- `df` 输出是运行时对象：**不要对 df.* 节点开 `cache`**（缓存按 JSON 序列化会失败）。

---

## 10. 示例插件算子（examples/plugins/demo_ops.py）

不属于内置算子，需 `--plugin plugins/demo_ops.py`（`--plugin` 路径相对配置文件所在目录解析）后可用：

> 插件包也可以做成 pip 分发：在插件包 pyproject 声明 `[project.entry-points."op_pipeline.ops"]`
> 后安装，CLI 自动发现、免 `--plugin`（M3-4，发布指南见
> [扩展设计 §3.4](../design/extension-points.md#34-插件包发布指南pip-分发m3-4)）。

| 算子 | 入 | 出 | 参数 |
| --- | --- | --- | --- |
| `demo.word_count` | `text:str` | `counts:dict` `total:int` `unique:int` | `min_len=2`、`lowercase=true` |
| `demo.flaky_probe` | `value:any` | `value:any` `attempts:int` | `fail_times=1`、`sleep=0.0` |
| `demo.text_stats` | `lines:list` | `stats:dict` | — |

`demo.flaky_probe` 前 N 次故意失败，用于验证 `retries` / `retry_delay`；
`demo.text_stats` 输出中文键（`行数`/`总字符`/`平均长度`/`最长行`），配合句柄下钻
`${line_stats.stats.最长行}` 使用。
