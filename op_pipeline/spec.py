"""流水线规格（spec）：从配置文本到可校验、可执行的 DAG。

配置形态（JSON / TOML / YAML 三选一，内容同构）::

    name = "sales_report"
    [params]                     # 流水线级参数，支持外部覆盖
    input_path = "data/sales.csv"
    threshold  = 0.5

    [[nodes]]
    id   = "load"
    op   = "io.read_csv"
    [nodes.params]
    path = "${p.input_path}"

    [[nodes]]
    id    = "clean"
    op    = "table.filter_rows"
    [nodes.inputs]
    table = "${load.table}"      # 上游句柄
    [nodes.params]
    where = "amount > 0"

    [outputs]                    # 流水线对外产物
    report = "${stat.total}"

引用语法：
    ``${<node>.<port>}`` 或 ``${<handle>}``  -> 上游数据
    ``${p.<name>}``                          -> 流水线参数
    ``${env.<NAME>}``                        -> 环境变量（运行期解析）
    ``${env:NAME}`` / ``${env:NAME:-默认}``  -> 环境变量（解析期注入，可带默认值）
    ``${param:NAME}`` / ``${param:NAME:-默认}`` -> 运行级参数（解析期注入，可带默认值）
字符串中可混排（``"${p.dir}/${p.name}.json"``）。
"""
from __future__ import annotations

import difflib
import json
import logging
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Mapping

from .errors import (
    ConfigError,
    CycleError,
    RefResolutionError,
    SpecValidationError,
)
from .handlestore import as_mapping
from .ports import Port, type_label, value_matches

logger = logging.getLogger("op_pipeline.spec")

_REF_PATTERN = re.compile(r"\$\{([^{}]+)\}")

# M7-2 Task 6：解析期环境变量注入。``${env:NAME}`` / ``${env:NAME:-default}``
# 在构造 NodeSpec 之前完成替换；既有 ``${env.NAME}``（点号语法）仍由 RefResolver
# 在 build_plan/运行期解析，两者互不干扰（``env:`` 形式优先处理并全部消失）。
_ENV_PLACEHOLDER = re.compile(r"\$\{env:([^{}]*)\}")
_ENV_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
_SENSITIVE_ENV_NAME = re.compile(r"TOKEN|SECRET|PASSWORD|KEY", re.IGNORECASE)


def _substitute_env_text(
    text: str, where: str, env: Mapping[str, str], sensitive: list[str]
) -> str:
    """替换单个字符串里的 ``${env:...}`` 占位符，其余文本原样保留。"""

    def replace(match: re.Match[str]) -> str:
        body = match.group(1)
        if ":-" in body:
            name, default = body.split(":-", 1)
        else:
            name, default = body, None
        if not _ENV_NAME.fullmatch(name):
            raise SpecValidationError(
                [
                    f"{where} 的 ${{env:...}} 占位符语法错误：变量名 {name!r} 不合法"
                    "（应匹配 [A-Za-z_][A-Za-z0-9_]*）"
                ]
            )
        if name in env:
            value = env[name]
        elif default is not None:
            value = default
        else:
            raise SpecValidationError(
                [
                    f"{where} 引用的环境变量 '{name}' 未设置（占位符 ${{env:{name}}}）；"
                    f"可写成 ${{env:{name}:-默认值}}"
                ]
            )
        if _SENSITIVE_ENV_NAME.search(name):
            sensitive.append(str(value))
        return str(value)

    result = _ENV_PLACEHOLDER.sub(replace, text)
    if "${env:" in result:
        raise SpecValidationError(
            [f"{where} 的 ${{env:...}} 占位符语法错误：未闭合（缺少 '}}'）"]
        )
    return result


def _substitute_env_value(
    value: Any, where: str, env: Mapping[str, str], sensitive: list[str]
) -> Any:
    """递归替换参数值中的环境变量占位符（仅处理字符串）。"""
    if isinstance(value, str):
        return _substitute_env_text(value, where, env, sensitive)
    if isinstance(value, list):
        return [_substitute_env_value(item, where, env, sensitive) for item in value]
    if isinstance(value, Mapping):
        return {
            key: _substitute_env_value(item, where, env, sensitive)
            for key, item in value.items()
        }
    return value


def _resolve_env_params(
    raw: Mapping[str, Any], env: Mapping[str, str]
) -> tuple[dict[str, Any], tuple[str, ...]]:
    """解析期替换顶层 ``params`` 与各节点 ``params`` 中的环境变量占位符。"""
    sensitive: list[str] = []
    data = dict(raw)

    params = data.get("params")
    if isinstance(params, Mapping):
        data["params"] = {
            key: _substitute_env_value(value, f"流水线参数 '{key}'", env, sensitive)
            for key, value in params.items()
        }

    nodes = data.get("nodes")
    if isinstance(nodes, list):
        new_nodes: list[Any] = []
        for node in nodes:
            if not isinstance(node, Mapping):
                new_nodes.append(node)
                continue
            node_copy = dict(node)
            node_id = node.get("id") or node.get("name") or "?"
            node_params = node_copy.get("params")
            if isinstance(node_params, Mapping):
                where = f"节点 [{node_id}]"
                node_copy["params"] = {
                    key: _substitute_env_value(value, where, env, sensitive)
                    for key, value in node_params.items()
                }
            new_nodes.append(node_copy)
        data["nodes"] = new_nodes

    return data, tuple(sensitive)


# M8-2 Task 1：解析期运行级参数注入。``${param:NAME}`` /
# ``${param:NAME:-default}`` 与 ``${env:...}`` 同框架、互不干扰：
# 冒号前缀区分两类占位符，同一字符串可混排多个占位符，嵌套结构递归替换。
_PARAM_PLACEHOLDER = re.compile(r"\$\{param:([^{}]*)\}")
_PARAM_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")


def _resolve_param_body(
    body: str, where: str, lookup: Mapping[str, Any], sensitive: list[str]
) -> Any:
    """解析单个 ``${param:...}`` 占位符体，返回运行级参数值。"""
    if ":-" in body:
        name, default = body.split(":-", 1)
    else:
        name, default = body, None
    if not _PARAM_NAME.fullmatch(name):
        raise SpecValidationError(
            [
                f"{where} 的 ${{param:...}} 占位符语法错误：变量名 {name!r} 不合法"
                "（应匹配 [A-Za-z_][A-Za-z0-9_]*）"
            ]
        )
    if name in lookup:
        value = lookup[name]
        if _SENSITIVE_ENV_NAME.search(name):
            # 敏感键沿用 M7-2 打码机制：值进 sensitive_values，报告里显示 ***
            sensitive.append(str(value))
        return value
    if default is not None:
        return default
    raise SpecValidationError(
        [
            f"{where} 引用的运行级参数 '{name}' 未注入（占位符 ${{param:{name}}}）；"
            f"可写成 ${{param:{name}:-默认值}}"
        ]
    )


def _substitute_param_text(
    text: str, where: str, lookup: Mapping[str, Any], sensitive: list[str]
) -> Any:
    """替换单个字符串里的 ``${param:...}`` 占位符。

    整串恰好是一个占位符时保留参数值原类型（如 int/list/dict）；
    否则按字符串插值拼接。
    """
    whole = _PARAM_PLACEHOLDER.fullmatch(text)
    if whole:
        return _resolve_param_body(whole.group(1), where, lookup, sensitive)

    def replace(match: re.Match[str]) -> str:
        return str(_resolve_param_body(match.group(1), where, lookup, sensitive))

    result = _PARAM_PLACEHOLDER.sub(replace, text)
    if "${param:" in result:
        raise SpecValidationError(
            [f"{where} 的 ${{param:...}} 占位符语法错误：未闭合（缺少 '}}'）"]
        )
    return result


def _substitute_param_value(
    value: Any, where: str, lookup: Mapping[str, Any], sensitive: list[str]
) -> Any:
    """递归替换参数值中的运行级参数占位符（仅处理字符串）。"""
    if isinstance(value, str):
        return _substitute_param_text(value, where, lookup, sensitive)
    if isinstance(value, list):
        return [_substitute_param_value(item, where, lookup, sensitive) for item in value]
    if isinstance(value, Mapping):
        return {
            key: _substitute_param_value(item, where, lookup, sensitive)
            for key, item in value.items()
        }
    return value


def _resolve_param_refs(
    raw: Mapping[str, Any], injected: Mapping[str, Any] | None
) -> tuple[dict[str, Any], tuple[str, ...]]:
    """解析期替换顶层 ``params`` 与各节点 ``params`` 中的运行级参数占位符。

    查找表 = 配置 ``[params]`` 段（已完成 env 替换）与注入的运行级参数合并，
    注入优先。注入的参数随后合并进顶层 ``params``（同样注入优先），
    使 ``build_plan(overrides=...)`` 与运行报告可见。
    """
    sensitive: list[str] = []
    data = dict(raw)
    injected = dict(injected) if injected else {}

    config_params = data.get("params")
    config_params = dict(config_params) if isinstance(config_params, Mapping) else {}
    resolved_params = {
        key: _substitute_param_value(
            value, f"流水线参数 '{key}'", {**config_params, **injected}, sensitive
        )
        for key, value in config_params.items()
    }
    data["params"] = {**resolved_params, **injected}

    nodes = data.get("nodes")
    if isinstance(nodes, list):
        new_nodes: list[Any] = []
        for node in nodes:
            if not isinstance(node, Mapping):
                new_nodes.append(node)
                continue
            node_copy = dict(node)
            node_id = node.get("id") or node.get("name") or "?"
            node_params = node_copy.get("params")
            if isinstance(node_params, Mapping):
                where = f"节点 [{node_id}]"
                node_copy["params"] = {
                    key: _substitute_param_value(value, where, data["params"], sensitive)
                    for key, value in node_params.items()
                }
            new_nodes.append(node_copy)
        data["nodes"] = new_nodes

    return data, tuple(sensitive)


def _warn_unknown_keys(where: str, raw: Mapping[str, Any], known: frozenset[str]) -> None:
    """对映射中不属于 known 的键发告警。

    未知键会被静默忽略——把 ``[nodes]`` 拼成 ``[Nodes]`` 会得到
    "0 节点运行成功"的假象，因此这里必须给用户留痕（告警而非报错，
    是为了前向兼容：新增键不破坏旧版本库调用方）。
    """
    for key in raw:
        if key in known:
            continue
        # 候选排序：前缀匹配优先，其次长度更短，再按 difflib 比率降序。
        # 'retry' 的前缀候选是 'retry_on'/'retry_delay'，但最短近义键 'retries'
        # 才是用户期望的提示（既有契约），因此长度优先于单纯的前缀归属。
        prefix = [k for k in known if k.startswith(key)]
        close = difflib.get_close_matches(key, known, n=len(known), cutoff=0.6)
        candidates = list(dict.fromkeys(prefix + close))
        candidate = (
            min(
                candidates,
                key=lambda k: (
                    len(k),
                    -difflib.SequenceMatcher(None, key, k).ratio(),
                ),
            )
            if candidates
            else ""
        )
        hint = f"；是否想写 '{candidate}'？" if candidate else ""
        logger.warning("%s 含未知键 %r（将被忽略）%s", where, key, hint)


# --------------------------------------------------------------------------- #
# 配置格式版本化（M3-5）
# --------------------------------------------------------------------------- #
# 当前配置格式版本。老配置缺省按 "1"（首个发布格式）解析；
# 不兼容变更必须新增迁移器（见 MIGRATIONS）并升此版本号。
CURRENT_SPEC_VERSION = "1"

# 迁移器注册表：目标版本 -> 迁移函数。迁移函数接收一份配置 dict，
# 返回升级到该版本后的 dict（只做加法/改写，不做降级）。
# 升序逐级升级：1 -> 2 -> ... -> CURRENT_SPEC_VERSION。
MIGRATIONS: dict[str, Callable[[dict], dict]] = {}


def _next_version(version: int) -> str:
    """取下一个版本号的字符串表示（"1" -> "2"）。"""
    return str(version + 1)


def _migrate(raw: Mapping[str, Any]) -> dict[str, Any]:
    """版本检查 + 逐级迁移到当前格式。

    - ``spec_version`` 缺省视为 ``"1"``（首个发布格式），无需迁移；
    - 配置版本低于当前版本时按 ``MIGRATIONS`` 逐级升级；
    - 配置版本高于当前版本（未来文件）直接报错，绝不静默猜测；
    - 版本号非数字报错。
    """
    data = dict(raw)
    version = data.get("spec_version", "1")
    try:
        version_int = int(str(version))
    except (TypeError, ValueError):
        raise ConfigError(
            f"spec_version 必须是整数格式的字符串（如 \"1\"），实际是 {version!r}"
        ) from None
    if version_int > int(CURRENT_SPEC_VERSION):
        raise ConfigError(
            f"配置的 spec_version={version!r} 高于当前框架支持的版本 "
            f"{CURRENT_SPEC_VERSION!r}：请升级 op_pipeline 后再读取该配置"
        )
    while version_int < int(CURRENT_SPEC_VERSION):
        target = _next_version(version_int)
        migrator = MIGRATIONS.get(target)
        if migrator is None:
            raise ConfigError(
                f"缺少配置迁移器：{version} -> {target}（MIGRATIONS 未注册）"
            )
        data = migrator(data)
        version_int += 1
        version = target
    data["spec_version"] = CURRENT_SPEC_VERSION
    return data


# --------------------------------------------------------------------------- #
# 配置组合（M3-7）：顶层 includes
# --------------------------------------------------------------------------- #
def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _load_toml(path: Path) -> dict[str, Any]:
    import tomllib

    return tomllib.loads(path.read_text(encoding="utf-8"))


def _load_yaml(path: Path) -> dict[str, Any]:
    try:
        import yaml  # type: ignore
    except ImportError as exc:  # pragma: no cover
        raise ConfigError(
            "读取 YAML 需要 PyYAML：pip install pyyaml；"
            "或改用 .json / .toml 配置（标准库即可）"
        ) from exc
    return yaml.safe_load(path.read_text(encoding="utf-8"))


# 配置格式 loader 注册表（M3-8）：扩展名（含前导点）-> 解析函数。
# 内建 json/toml/yaml 也是注册项；第三方可用 PipelineSpec.register_loader 扩展新格式。
_LOADERS: dict[str, Callable[[Path], dict[str, Any]]] = {
    ".json": _load_json,
    ".toml": _load_toml,
    ".yaml": _load_yaml,
    ".yml": _load_yaml,
}


def _load_raw(path: Path) -> dict[str, Any]:
    """读取单个配置文件：按扩展名从 loader 注册表取解析器。"""
    if not path.exists():
        raise ConfigError(f"配置文件不存在：{path}")
    suffix = path.suffix.lower()
    loader = _LOADERS.get(suffix)
    if loader is None and suffix == "":
        loader = _LOADERS[".json"]  # 兼容历史行为：无后缀按 JSON 解析
    if loader is None:
        raise ConfigError(
            f"不支持的配置扩展名：{suffix or '(无后缀)'}（已注册格式："
            f"{', '.join(sorted(_LOADERS))}）"
        )
    data = loader(path)
    if not isinstance(data, Mapping):
        raise ConfigError(f"配置文件必须是对象（键值映射）：{path}")
    return dict(data)


def _resolve_includes(raw: Mapping[str, Any], base_dir: str | Path | None) -> dict[str, Any]:
    """递归合并顶层 ``includes``，返回不含 ``includes`` 键的合并配置。

    合并语义（主配置优先）：
    - ``params`` / ``defaults`` / ``outputs``：dict 逐键覆盖（include 先合入，主配置覆盖）；
    - ``nodes``：按 include 顺序拼接，节点 id 重复（跨文件或与主配置）即报错；
    - ``name`` / ``description`` / ``version`` / ``spec_version``：主配置优先。

    子配置的 ``includes`` 相对其自身所在目录解析；沿加载栈做环检测。
    """
    includes = raw.get("includes")
    if not includes:
        return dict(raw)
    if isinstance(includes, str):
        raise ConfigError("includes 必须是字符串路径数组，实际是单个字符串")
    if not isinstance(includes, list) or not all(isinstance(p, str) for p in includes):
        raise ConfigError(f"includes 必须是字符串路径数组，实际是 {includes!r}")
    if base_dir is None:
        raise ConfigError(
            "顶层 includes 需要文件上下文：请通过 from_file 加载配置，或显式传入 base_dir"
        )
    base = Path(base_dir)
    merged: dict[str, Any] = {"params": {}, "defaults": {}, "outputs": {}}
    nodes: list[dict[str, Any]] = []
    node_source: dict[str, str] = {}

    def _take(data: dict[str, Any], source: str) -> None:
        """把一份子配置合入 merged：dict 字段覆盖、nodes 拼接并查重。"""
        for key in ("params", "defaults", "outputs"):
            value = data.get(key) or {}
            if not isinstance(value, Mapping):
                raise ConfigError(f"文件 {source} 的 {key} 必须是映射")
            merged[key].update(dict(value))
        for node in data.get("nodes") or []:
            if not isinstance(node, Mapping):
                raise ConfigError(f"文件 {source} 的 nodes 元素必须是对象")
            nid = node.get("id") or node.get("name")
            if nid and nid in node_source:
                raise ConfigError(
                    f"includes 合并后节点 id '{nid}' 重复（来自 {node_source[nid]} 与 {source}）"
                )
            if nid:
                node_source[nid] = source
            nodes.append(dict(node))

    def _load(rel: str, bdir: Path, stack: tuple[Path, ...]) -> None:
        target = (bdir / rel).resolve()
        if target in stack:
            chain = " -> ".join(str(p) for p in stack + (target,))
            raise ConfigError(f"includes 存在循环引用：{chain}")
        if not target.exists():
            raise ConfigError(f"includes 引用的文件不存在：{rel}（相对 {bdir}）")
        data = _load_raw(target)
        _take(data, str(target))
        for sub in data.get("includes") or []:
            if not isinstance(sub, str):
                raise ConfigError(f"includes 必须是字符串路径数组，实际含 {sub!r}")
            _load(sub, target.parent, stack + (target,))

    for rel in includes:
        _load(rel, base, ())
    # 主配置节点也并入同一张表（查重），并覆盖 include 的 dict 字段
    _take(dict(raw), "主配置")
    for key in ("name", "description", "version", "spec_version"):
        if key in raw:
            merged[key] = raw[key]
    merged["nodes"] = nodes
    return merged


# --------------------------------------------------------------------------- #
# 数据模型
# --------------------------------------------------------------------------- #
# PipelineSpec.defaults 可设置的运行策略键（节点可逐项覆盖）
_STRATEGY_KEYS = frozenset(
    {"retries", "retry_delay", "timeout", "cache", "allow_failure", "executor"}
)


@dataclass
class NodeSpec:
    """一个流水线节点 = 一个算子实例 + 连线 + 运行策略。"""

    id: str
    op: str
    inputs: dict[str, Any] = field(default_factory=dict)
    outputs: dict[str, str] = field(default_factory=dict)
    params: dict[str, Any] = field(default_factory=dict)
    when: str | None = None
    retries: int = 0
    retry_delay: float = 0.0
    # 条件重试（M6-3 Task 9）：非空时仅重试这些异常类型名（type(exc).__name__）；
    # 空元组 = 不按类型过滤（现状：普通异常按 retries 重试）。
    retry_on: tuple[str, ...] = ()
    timeout: float | None = None
    cache: bool = False
    allow_failure: bool = False
    # 执行器选择（M2-2 / M5-1）：""=未指定（引擎按运行级 default 或 thread），
    # "thread"=线程（软超时），"process"=子进程（硬超时），
    # "queue"=远程队列执行器（M5-1：节点级声明触发引擎自建 QueueWorkerExecutor）
    executor: str = ""
    # 并发分组（M6-3 Task 8）："" = 未声明（不加组限流）；非空时若运行配置
    # （concurrency_limits）给出该组上限，则同组节点最多 N 个同时执行。
    concurrency_group: str = ""
    description: str = ""

    # from_dict 接受的合法键（dataclass 字段之外无类型注解，不会被当作字段）
    _KNOWN_KEYS = frozenset(
        {
            "id", "name", "op", "inputs", "outputs", "params", "when",
            "retries", "retry_delay", "retry_on", "timeout", "cache",
            "allow_failure", "executor", "concurrency_group", "description",
        }
    )

    @classmethod
    def from_dict(
        cls, raw: Mapping[str, Any], defaults: Mapping[str, Any] | None = None
    ) -> "NodeSpec":
        """从配置字典构造节点。

        运行策略字段（retries/retry_delay/timeout/cache/allow_failure）三级优先：
        节点显式值 > 流水线 ``defaults`` > 框架默认。合并发生在此处，
        之后（build_plan/引擎）不再区分值的来源。
        """
        if not isinstance(raw, Mapping):
            raise ConfigError(f"节点定义必须是对象，实际是 {type(raw).__name__}")
        node_id = raw.get("id") or raw.get("name")
        if not node_id:
            raise ConfigError(f"节点缺少 id：{raw!r}")
        op = raw.get("op")
        if not op:
            raise ConfigError(f"节点 [{node_id}] 缺少 op")
        _warn_unknown_keys(f"节点 [{node_id}]", raw, cls._KNOWN_KEYS)
        outputs = raw.get("outputs") or {}
        if not isinstance(outputs, Mapping):
            raise ConfigError(f"节点 [{node_id}] 的 outputs 必须是映射（端口 -> 句柄名）")
        # 三级合并：raw.get(key, defaults.get(key, 框架默认))
        fallback = defaults or {}
        merged = {k: raw.get(k, fallback.get(k)) for k in _STRATEGY_KEYS}
        timeout_raw = merged["timeout"]
        executor_raw = merged["executor"] or ""
        if executor_raw not in ("", "thread", "process", "process_pool", "queue"):
            raise ConfigError(
                f"节点 [{node_id}] 的 executor 只能是 thread/process/process_pool/queue，"
                f"实际是 {executor_raw!r}"
            )
        # M6-3 Task 8：并发分组必须是字符串（空串 = 未声明，不加组限流）
        group_raw = raw.get("concurrency_group", "")
        if not isinstance(group_raw, str):
            raise ConfigError(
                f"节点 [{node_id}] 的 concurrency_group 必须是字符串"
                f"（空串表示未声明），实际是 {type(group_raw).__name__}"
            )
        # M6-3 Task 9：retry_on 只接受字符串数组（单个字符串不是"数组"，明确报错）；
        # 缺省 / null = 空元组（不做类型过滤，保持 v0.6.0 行为）。
        retry_on_raw = raw.get("retry_on")
        if retry_on_raw is None:
            retry_on_raw = ()
        if not isinstance(retry_on_raw, (list, tuple)):
            raise ConfigError(
                f"节点 [{node_id}] 的 retry_on 必须是异常类型名的字符串数组，"
                f"实际是 {type(retry_on_raw).__name__}（{retry_on_raw!r}）"
            )
        if not all(isinstance(item, str) for item in retry_on_raw):
            raise ConfigError(
                f"节点 [{node_id}] 的 retry_on 元素必须都是字符串，实际是 {retry_on_raw!r}"
            )
        return cls(
            id=str(node_id),
            op=str(op),
            inputs=dict(raw.get("inputs") or {}),
            outputs={str(k): str(v) for k, v in outputs.items()},
            params=dict(raw.get("params") or {}),
            when=raw.get("when"),
            retries=int(merged["retries"] or 0),
            retry_delay=float(merged["retry_delay"] or 0.0),
            retry_on=tuple(retry_on_raw),
            timeout=None if timeout_raw in (None, 0) else float(timeout_raw),
            cache=bool(merged["cache"] or False),
            allow_failure=bool(merged["allow_failure"] or False),
            executor=str(executor_raw),
            concurrency_group=group_raw,
            description=str(raw.get("description") or ""),
        )


@dataclass
class PipelineSpec:
    name: str = "pipeline"
    description: str = ""
    version: str = "1"
    params: dict[str, Any] = field(default_factory=dict)
    nodes: list[NodeSpec] = field(default_factory=list)
    outputs: dict[str, str] = field(default_factory=dict)
    defaults: dict[str, Any] = field(default_factory=dict)
    # M7-2 Task 6：解析期替换过的敏感环境变量值（仅用于报告打码；不进入 to_dict）
    sensitive_values: tuple[str, ...] = ()

    # from_dict 接受的合法键（dataclass 字段之外无类型注解，不会被当作字段）
    _KNOWN_KEYS = frozenset(
        {"name", "description", "version", "params", "nodes", "outputs", "defaults",
         "spec_version", "includes"}
    )

    @classmethod
    def from_dict(
        cls,
        raw: Mapping[str, Any],
        base_dir: str | Path | None = None,
        *,
        env: Mapping[str, str] | None = None,
        params: Mapping[str, Any] | None = None,
    ) -> "PipelineSpec":
        if not isinstance(raw, Mapping):
            raise ConfigError("流水线配置根节点必须是对象")
        raw = _resolve_includes(raw, base_dir=base_dir)  # M3-7：合并顶层 includes
        raw = _migrate(raw)  # M3-5：版本检查 + 逐级迁移到当前格式
        # M7-2 Task 6：参数里的 ${env:NAME} / ${env:NAME:-default} 在解析期替换
        raw, sensitive_values = _resolve_env_params(
            raw, os.environ if env is None else env
        )
        # M8-2 Task 1：${param:NAME} / ${param:NAME:-default} 运行级参数占位符
        # 同样在解析期替换；注入的参数合并进顶层 params（注入优先）
        raw, param_sensitive = _resolve_param_refs(raw, params)
        _warn_unknown_keys("流水线配置", raw, cls._KNOWN_KEYS)
        nodes_raw = raw.get("nodes") or []
        if not isinstance(nodes_raw, list):
            raise ConfigError("nodes 必须是数组")
        defaults = dict(raw.get("defaults") or {})
        unknown = set(defaults) - _STRATEGY_KEYS
        if unknown:
            raise ConfigError(
                f"defaults 含未知策略键：{sorted(unknown)}；"
                f"可用键为 {sorted(_STRATEGY_KEYS)}"
            )
        return cls(
            name=str(raw.get("name") or "pipeline"),
            description=str(raw.get("description") or ""),
            version=str(raw.get("version") or "1"),
            params=dict(raw.get("params") or {}),
            nodes=[NodeSpec.from_dict(n, defaults=defaults) for n in nodes_raw],
            outputs={str(k): str(v) for k, v in (raw.get("outputs") or {}).items()},
            defaults=defaults,
            sensitive_values=sensitive_values + param_sensitive,
        )

    # ------------------------------------------------------------------ #
    @classmethod
    def register_loader(
        cls, suffix: str, fn: Callable[[Path], dict[str, Any]]
    ) -> None:
        """注册自定义配置格式的加载器（M3-8）。

        ``suffix`` 为扩展名（可省略前导点，``"fake"`` 与 ``".fake"`` 等价）；
        ``fn`` 接收文件路径，返回配置映射（dict）。注册后 ``from_file`` 即按该
        后缀分发。覆盖同后缀的内建/已有加载器以最后注册者为准。
        """
        if not isinstance(suffix, str) or not suffix.strip():
            raise ConfigError(f"loader 后缀必须是字符串，实际是 {suffix!r}")
        if not callable(fn):
            raise ConfigError(
                f"loader {suffix!r} 必须是可调用对象，实际是 {type(fn).__name__}"
            )
        norm = suffix.lower()
        if not norm.startswith("."):
            norm = "." + norm
        _LOADERS[norm] = fn

    @classmethod
    def from_file(
        cls,
        path: str | Path,
        *,
        env: Mapping[str, str] | None = None,
        params: Mapping[str, Any] | None = None,
    ) -> "PipelineSpec":
        path = Path(path).expanduser()
        data = _load_raw(path)
        return cls.from_dict(data, base_dir=path.parent, env=env, params=params)

    def to_dict(self) -> dict[str, Any]:
        return {
            "spec_version": CURRENT_SPEC_VERSION,
            "name": self.name,
            "description": self.description,
            "version": self.version,
            "params": self.params,
            "defaults": self.defaults,
            "outputs": self.outputs,
            "nodes": [
                {
                    "id": n.id,
                    "op": n.op,
                    "inputs": n.inputs,
                    "outputs": n.outputs,
                    "params": n.params,
                    "when": n.when,
                    "retries": n.retries,
                    "retry_delay": n.retry_delay,
                    "retry_on": list(n.retry_on),
                    "timeout": n.timeout,
                    "cache": n.cache,
                    "allow_failure": n.allow_failure,
                    "concurrency_group": n.concurrency_group,
                }
                for n in self.nodes
            ],
        }


# --------------------------------------------------------------------------- #
# 引用解析
# --------------------------------------------------------------------------- #
class RefResolver:
    """解析 ``${...}`` 引用。

    ``handles`` 是"句柄 -> 值"的映射，由引擎在执行时逐节点填充；
    校验阶段用 :meth:`static_target` 只做"目标是否存在"的静态检查。
    """

    def __init__(
        self,
        params: Mapping[str, Any],
        handles: Any = None,
        env: Mapping[str, str] | None = None,
    ) -> None:
        self.params = params
        # M2-3：引擎收敛点传入 HandleStore（取 snapshot 查表）；
        # 公共符号保持 Mapping 兼容（老用法 dict / {} 不变）
        self.handles = as_mapping(handles)
        self.env = os.environ if env is None else env

    # ------------------------------------------------------------------ #
    def resolve(self, value: Any) -> Any:
        """递归解析任意配置值中的引用。"""
        if isinstance(value, str):
            return self._resolve_str(value)
        if isinstance(value, list):
            return [self.resolve(v) for v in value]
        if isinstance(value, dict):
            return {k: self.resolve(v) for k, v in value.items()}
        return value

    def _resolve_str(self, text: str) -> Any:
        whole = _REF_PATTERN.fullmatch(text)
        if whole:
            return self._lookup(whole.group(1).strip())

        def sub(match: re.Match[str]) -> str:
            return str(self._lookup(match.group(1).strip()))

        return _REF_PATTERN.sub(sub, text)

    def _lookup(self, path: str) -> Any:
        if path.startswith("p.") or path.startswith("params."):
            key = path.split(".", 1)[1]
            if key not in self.params:
                raise RefResolutionError(
                    f"引用了未定义的流水线参数 '{key}'；已定义：{', '.join(self.params) or '（无）'}"
                )
            return self.params[key]
        if path.startswith("env."):
            key = path.split(".", 1)[1]
            if key not in self.env:
                raise RefResolutionError(f"环境变量 '{key}' 未设置")
            return self.env[key]
        if path in self.handles:
            return self.handles[path]
        # 支持句柄下钻：${node.port.field} / ${node.port.0}
        parts = path.split(".")
        for end in range(len(parts) - 1, 0, -1):
            prefix = ".".join(parts[:end])
            if prefix in self.handles:
                return self._traverse(self.handles[prefix], parts[end:], path)
        raise RefResolutionError(
            f"引用 '{path}' 无法解析：既不是已产出的句柄，也不是 p./env. 前缀参数"
        )

    @staticmethod
    def _traverse(value: Any, segments: list[str], path: str) -> Any:
        for segment in segments:
            if isinstance(value, dict) and segment in value:
                value = value[segment]
            elif isinstance(value, (list, tuple)) and segment.lstrip("-").isdigit():
                index = int(segment)
                try:
                    value = value[index]
                except IndexError as exc:
                    raise RefResolutionError(f"引用 '{path}' 下标越界：{segment}") from exc
            else:
                raise RefResolutionError(
                    f"引用 '{path}' 的路径段 '{segment}' 无法解析（当前值类型 {type(value).__name__}）"
                )
        return value

    # ------------------------------------------------------------------ #
    @staticmethod
    def static_target(value: Any) -> list[str]:
        """列出配置值里出现的所有引用路径（用于静态校验）。"""
        found: list[str] = []

        def walk(node: Any) -> None:
            if isinstance(node, str):
                found.extend(m.group(1).strip() for m in _REF_PATTERN.finditer(node))
            elif isinstance(node, list):
                for item in node:
                    walk(item)
            elif isinstance(node, dict):
                for item in node.values():
                    walk(item)

        walk(value)
        return found


# --------------------------------------------------------------------------- #
# 校验 + 拓扑排序
# --------------------------------------------------------------------------- #
def match_handle(target: str, handle_owner: Mapping[str, str]) -> str | None:
    """把 ``node.port.field`` 这类引用匹配到已声明的句柄（最长前缀优先）。"""
    parts = target.split(".")
    for end in range(len(parts), 0, -1):
        prefix = ".".join(parts[:end])
        if prefix in handle_owner:
            return prefix
    return None


@dataclass
class PlanNode:
    """校验通过后的可执行节点。"""

    spec: NodeSpec
    operator_cls: type
    operator: Any
    handles: dict[str, str]  # 端口 -> 句柄
    depends_on: list[str]


@dataclass
class Plan:
    spec: PipelineSpec
    params: dict[str, Any]
    nodes: list[PlanNode]
    order: list[str]
    handle_owner: dict[str, str]
    pipeline_outputs: dict[str, str]

    def node(self, node_id: str) -> PlanNode:
        for n in self.nodes:
            if n.spec.id == node_id:
                return n
        raise ConfigError(f"计划中没有节点 '{node_id}'")


def build_plan(
    spec: PipelineSpec,
    registry: Any,
    *,
    overrides: Mapping[str, Any] | None = None,
    env: Mapping[str, str] | None = None,
    strict: bool = True,
) -> Plan:
    """把规格编译成执行计划：校验端口/参数/引用/依赖，并给出拓扑序。"""
    problems: list[str] = []

    # 1) 流水线参数合并
    params = dict(spec.params)
    for key, value in (overrides or {}).items():
        if key not in params:
            problems.append(f"覆盖了未定义的流水线参数 '{key}'；已定义：{', '.join(params) or '（无）'}")
            continue
        params[key] = value
    # 参数自身也可能引用 env
    resolver_env = RefResolver(params, {}, env)
    try:
        params = resolver_env.resolve(params)
    except RefResolutionError as exc:
        problems.append(str(exc))

    # 2) 节点 id 唯一
    ids: dict[str, int] = {}
    for node in spec.nodes:
        ids[node.id] = ids.get(node.id, 0) + 1
    for node_id, count in ids.items():
        if count > 1:
            problems.append(f"节点 id '{node_id}' 重复出现 {count} 次")

    # 3) 算子存在 + 参数校验 + 句柄声明
    handle_owner: dict[str, str] = {}
    resolved_ops: dict[str, tuple[type, Any]] = {}
    for node in spec.nodes:
        try:
            cls = registry.get(node.op)
        except Exception as exc:  # OperatorNotFoundError
            problems.append(f"节点 [{node.id}]：{exc}")
            continue
        if RefResolver.static_target(node.params):
            # 参数里含上游引用，类型只能等运行期解析后再校验；
            # 这里仍做"参数名"静态校验，避免拼写错误拖到运行时才暴露。
            unknown = sorted(set(node.params) - set(cls.param_table))
            if unknown:
                problems.append(
                    f"节点 [{node.id}] 收到未声明的参数：{', '.join(unknown)}；"
                    f"可用：{', '.join(sorted(cls.param_table)) or '（无）'}"
                )
                continue
            instance = None
        else:
            try:
                instance = cls(**node.params)
            except Exception as exc:
                problems.append(f"节点 [{node.id}] 参数不合法：{exc}")
                continue
        resolved_ops[node.id] = (cls, instance)

        known_ports = {p.name for p in cls.outputs}
        for port, handle in node.outputs.items():
            if port not in known_ports:
                problems.append(
                    f"节点 [{node.id}] 重命名了未声明的输出端口 '{port}'；"
                    f"可用：{', '.join(sorted(known_ports)) or '（无）'}"
                )
        for port in cls.outputs:
            handle = node.outputs.get(port.name, f"{node.id}.{port.name}")
            if handle in handle_owner:
                problems.append(
                    f"句柄 '{handle}' 被节点 [{handle_owner[handle]}] 与 [{node.id}] 同时占用"
                )
            else:
                handle_owner[handle] = node.id

    # 4) 引用可达性 + 输入端口完备性 + 依赖推导
    plan_nodes: list[PlanNode] = []
    for node in spec.nodes:
        if node.id not in resolved_ops:
            continue
        cls, instance = resolved_ops[node.id]
        depends: set[str] = set()

        declared_inputs = {p.name for p in cls.inputs}
        for port in node.inputs:
            if port not in declared_inputs:
                problems.append(
                    f"节点 [{node.id}] 连接了未声明的输入端口 '{port}'；"
                    f"可用：{', '.join(sorted(declared_inputs)) or '（无）'}"
                )

        # 静态检查所有引用的目标是否会被产出 / 是否存在
        targets = RefResolver.static_target(node.inputs) + RefResolver.static_target(node.params)
        for target in targets:
            if target.startswith(("p.", "params.", "env.")):
                continue
            matched = match_handle(target, handle_owner)
            if matched:
                depends.add(handle_owner[matched])
                continue
            problems.append(
                f"节点 [{node.id}] 引用了未知句柄 '{target}'；"
                f"已声明句柄：{', '.join(sorted(handle_owner)) or '（无）'}"
            )

        if node.id in depends:
            problems.append(f"节点 [{node.id}] 直接引用了自己的输出（自环）")
            depends.discard(node.id)

        missing = [
            p.name for p in cls.inputs
            if not p.optional and p.name not in node.inputs
        ]
        if missing:
            problems.append(
                f"节点 [{node.id}] 缺少必填输入端口：{', '.join(missing)}"
                f"（算子 {cls.name} 声明了 {', '.join(p.name for p in cls.inputs)}）"
            )

        handles = {p.name: node.outputs.get(p.name, f"{node.id}.{p.name}") for p in cls.outputs}
        plan_nodes.append(
            PlanNode(
                spec=node,
                operator_cls=cls,
                operator=instance,
                handles=handles,
                depends_on=sorted(depends),
            )
        )

    # 5) 流水线输出引用
    for name, ref in spec.outputs.items():
        for target in RefResolver.static_target(ref):
            if target.startswith(("p.", "params.", "env.")):
                continue
            if match_handle(target, handle_owner) is None:
                problems.append(f"流水线输出 '{name}' 引用了未知句柄 '{target}'")

    # 6) 环检测 + 拓扑序
    order: list[str] = []
    if not problems:
        try:
            order = topological_order({n.spec.id: n.depends_on for n in plan_nodes})
        except CycleError as exc:
            problems.append(str(exc))

    if problems and strict:
        raise SpecValidationError(problems)

    return Plan(
        spec=spec,
        params=params,
        nodes=plan_nodes,
        order=order,
        handle_owner=handle_owner,
        pipeline_outputs=dict(spec.outputs),
    )


def topological_order(graph: Mapping[str, list[str]]) -> list[str]:
    """Kahn 拓扑排序；同层按名字排序保证输出稳定。"""
    indegree = {node: 0 for node in graph}
    children: dict[str, list[str]] = {node: [] for node in graph}
    for node, deps in graph.items():
        for dep in deps:
            if dep not in graph:
                raise CycleError(f"依赖 '{dep}' 不存在于图中")
            indegree[node] += 1
            children[dep].append(node)

    ready = sorted(n for n, d in indegree.items() if d == 0)
    order: list[str] = []
    while ready:
        current = ready.pop(0)
        order.append(current)
        for child in sorted(children[current]):
            indegree[child] -= 1
            if indegree[child] == 0:
                ready.append(child)
        ready.sort()

    if len(order) != len(graph):
        remaining = sorted(n for n in graph if n not in order)
        raise CycleError(f"依赖图存在环，涉及节点：{', '.join(remaining)}")
    return order


def check_handle_types(
    plan: Plan, values: Mapping[str, Any]
) -> list[str]:
    """软类型校验：句柄实际值与声明端口类型不符时给出告警。"""
    warnings: list[str] = []
    for node in plan.nodes:
        cls = node.operator_cls
        for port in cls.outputs:
            handle = node.handles[port.name]
            if handle not in values:
                continue
            value = values[handle]
            if not value_matches(port.type, value):
                warnings.append(
                    f"节点 [{node.spec.id}] 句柄 '{handle}' 声明 {type_label(port.type)}，"
                    f"实际 {type(value).__name__}"
                )
        for port in cls.inputs:
            if port.name not in node.spec.inputs:
                continue
            ref = node.spec.inputs[port.name]
            if isinstance(ref, str) and _REF_PATTERN.fullmatch(ref):
                handle = _REF_PATTERN.fullmatch(ref).group(1).strip()
                if handle in values and not value_matches(port.type, values[handle]):
                    warnings.append(
                        f"节点 [{node.spec.id}] 输入 '{port.name}' 期望 {type_label(port.type)}，"
                        f"上游 '{handle}' 实际 {type(values[handle]).__name__}"
                    )
    return warnings
