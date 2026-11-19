"""子流水线算子（M8 Task 2）：把一份子配置当作"大算子"在节点内执行。

算子 ``pipeline.run`` 通过顶层 :func:`op_pipeline.run` 执行子流水线配置：

- ``result`` 端口取回子流水线指定节点的输出句柄值；
- ``report`` 端口给出子 run 摘要（run_id/status/duration/节点数/失败节点
  id 列表）；子 run 的完整报告**不并入**父报告与 artifacts；
- ``run_id`` 端口单独给出子 run 标识，便于父流水线追溯。

防环与深度限制：模块级 ``threading.local`` 维护"当前线程正在执行的子流水线
配置绝对路径链"。由于子 run 的节点在线程池新线程中执行、thread-local 不会
自动跨线程继承，本模块借助 :class:`_SubRunHook`——挂在子 run 的
``on_node_start`` 上（该钩子在节点 worker 线程内、算子执行前触发）——把父
线程的执行链显式带入子 run 的每个节点线程，从而保证跨层级的循环引用与嵌套
深度都能被检出（子进程执行器无法继承进程内存，防环仅在进程内生效）。
"""

from __future__ import annotations

import threading
from pathlib import Path
from typing import Any

from ..errors import ConfigError, OperatorNotFoundError
from ..operator import Operator
from ..params import Param
from ..registry import default_registry, load_plugins, op
from ..spec import PipelineSpec

# 子流水线嵌套深度上限：pipeline.run 最多同时嵌套 5 层，第 6 层拒绝执行。
_MAX_SUB_DEPTH = 5

# thread-local 执行栈：记录当前线程正在执行的子流水线配置绝对路径链
# （Path 元组，越靠后嵌套越深）。顶层流水线（经 run() 直接启动）不入栈。
_STACK = threading.local()


def _current_chain() -> tuple[Path, ...]:
    """返回当前线程的子流水线执行链（空元组表示当前处于顶层流水线）。"""
    return getattr(_STACK, "chain", ()) or ()


def _chain_text(chain: tuple[Path, ...], tail: Path) -> str:
    """把执行链与本次试图执行的配置拼成 ``a -> b -> c`` 形式的链路文本。"""
    return " -> ".join(str(p) for p in (*chain, tail))


class _SubRunHook:
    """把父线程的执行链带入子 run 的节点线程。

    子 run 由 :func:`op_pipeline.run` 自建线程池执行，节点运行在新线程里，
    thread-local 不会自动继承；而 ``on_node_start`` 恰在节点 worker 线程内、
    算子执行之前触发，借它把执行链写到该线程的 thread-local 上即可完成
    跨层传递。子 run 的线程池一次一建，所有节点线程共用同一份父链。
    """

    def __init__(self, chain: tuple[Path, ...]) -> None:
        self._chain = chain

    def on_node_start(self, node_id: str, op_name: str) -> None:
        _STACK.chain = self._chain


@op("pipeline.run")
class SubPipeline(Operator):
    """执行子流水线配置，取回其指定节点的输出句柄。"""

    description = "执行子流水线（编排组合），取回子节点输出与运行摘要"
    inputs = []
    outputs = [("result", Any), ("report", dict), ("run_id", str)]
    # 子流水线可能包含任意（含非确定性）算子，结果永不落入节点缓存。
    deterministic = False
    params = {
        "config": Param(str, doc="子流水线配置文件路径（相对本流水线工作目录解析）"),
        "node": Param(str, doc="子流水线中取输出的节点 id"),
        "handle": Param(
            str, default=None,
            doc="端口名；None 表示取该节点第一个输出端口（按算子 ports 声明顺序）",
        ),
        "params": Param(
            dict, default=None,
            doc="注入子流水线的运行级参数（配合 ${param:NAME} 占位符）",
        ),
        "plugins": Param(
            list, default=None,
            doc="子配置插件文件路径列表（相对子配置所在目录解析）",
        ),
        "on_error": Param(
            str, default="propagate",
            doc="子 run 失败的处理策略，仅支持 propagate（子失败即父节点失败）",
        ),
    }

    def run(
        self,
        ctx,
        config: str,
        node: str,
        handle: str | None = None,
        params: dict[str, Any] | None = None,
        plugins: list[str] | None = None,
        on_error: str = "propagate",
    ) -> dict[str, Any]:
        if on_error != "propagate":
            raise ValueError(f"on_error 仅支持 'propagate'，得到 {on_error!r}")

        # 1. 解析子配置路径：相对路径按本流水线工作目录解析（ctx.resolve_path），
        #    统一转绝对路径用于防环比较与展示。
        sub_path = Path(ctx.resolve_path(str(config))).expanduser().resolve()
        chain = _current_chain()

        # 2. 防环：目标配置已在执行链中 → 循环引用（如 a.json -> b.json -> a.json）。
        if sub_path in chain:
            raise ConfigError(f"检测到子流水线循环引用：{_chain_text(chain, sub_path)}")

        # 3. 深度上限：执行链已达上限后再嵌套 → 拒绝，报当前深度与上限。
        if len(chain) >= _MAX_SUB_DEPTH:
            raise ConfigError(
                f"子流水线嵌套深度超限：本次为第 {len(chain) + 1} 层，"
                f"上限 {_MAX_SUB_DEPTH} 层；执行链：{_chain_text(chain, sub_path)}"
            )

        if not sub_path.is_file():
            raise ConfigError(f"子流水线配置文件不存在：{sub_path}")

        # 4. 预检（在真正执行子流水线之前）：加载子配置（params 同样注入，
        #    保证 ${param:} 解析与正式运行一致），校验目标节点与端口，
        #    让错配置尽早失败、不产生子 run 的副作用。
        sub_params = dict(params or {})
        sub_spec = PipelineSpec.from_file(sub_path, params=sub_params)
        node_spec = next(
            (n for n in sub_spec.nodes if n.id == node), None
        )
        if node_spec is None:
            available = ", ".join(n.id for n in sub_spec.nodes) or "（无）"
            raise ConfigError(
                f"子流水线 {sub_path} 中不存在节点 '{node}'；可用节点：{available}"
            )

        # 5. 加载子插件（幂等，run() 内部会再加载一次），据注册表取目标节点
        #    算子的输出端口声明顺序：handle=None 时取第一个输出端口。
        plugin_paths = [str(p) for p in (plugins or [])]
        load_plugins(paths=plugin_paths, base_dir=sub_path.parent)
        try:
            op_cls = default_registry.get(node_spec.op)
        except OperatorNotFoundError as exc:
            raise ConfigError(
                f"子流水线 {sub_path} 的节点 '{node}' 使用了未注册算子"
                f" '{node_spec.op}'；若为插件算子，请经 plugins 参数提供"
            ) from exc
        port_names = op_cls.output_names()
        if not port_names:
            raise ConfigError(
                f"子流水线 {sub_path} 的节点 '{node}'（算子 {node_spec.op}）没有输出端口"
            )
        port = handle if handle is not None else port_names[0]
        if handle is not None and handle not in port_names:
            raise ConfigError(
                f"子流水线 {sub_path} 的节点 '{node}'（算子 {node_spec.op}）"
                f"没有输出端口 '{handle}'；可用输出端口：{', '.join(port_names)}"
            )
        # 节点若重命名了输出（NodeSpec.outputs：端口 -> 句柄名），按映射取键，
        # 否则默认句柄名即 "节点.端口"。
        handle_key = node_spec.outputs.get(port, f"{node}.{port}")

        # 6. 入栈 -> 执行子流水线 -> 弹栈（try/finally 保证异常时也恢复）。
        new_chain = (*chain, sub_path)
        _STACK.chain = new_chain
        try:
            from .. import run  # 函数体内惰性导入，避免与包 __init__ 循环导入

            ctx.log(f"执行子流水线：{sub_path}（参数 {sorted(sub_params)}）")
            report, values = run(
                sub_path,
                params=sub_params,
                plugins=plugin_paths,
                hooks=[_SubRunHook(new_chain)],
            )
        finally:
            _STACK.chain = chain

        # 7. 子 run 失败（status != success）→ 父节点失败；错误信息含子 run_id、
        #    状态与失败节点 id 列表，并附失败详情（让嵌套层产生的 ConfigError
        #    等诊断信息随错误链冒泡到父流水线）。
        if report.status != "success":
            failed = [n for n in report.nodes if n.status == "failed"]
            details = "；".join(f"[{n.node_id}] {n.error}" for n in failed if n.error)
            raise RuntimeError(
                f"子流水线运行失败（子配置：{sub_path}）：run_id={report.run_id}，"
                f"status={report.status}，"
                f"失败节点：{', '.join(n.node_id for n in failed) if failed else '（无）'}"
                + (f"；失败详情：{details}" if details else "")
            )

        # 8. 从子 run 的句柄字典中取目标值；键缺失属异常情况（如算子未按
        #    声明返回），给出该节点实际可用的句柄键以便排查。
        if handle_key not in values:
            available = sorted(k for k in values if k.startswith(f"{node}."))
            raise ConfigError(
                f"子流水线 {sub_path} 的节点 '{node}' 未产出句柄 '{handle_key}'；"
                f"该节点可用句柄：{', '.join(available) or '（无）'}"
            )
        result_value = values[handle_key]

        # 9. 子 run 完整报告不并入父报告/artifacts，只保留摘要 dict。
        summary = {
            "run_id": report.run_id,
            "status": report.status,
            "duration": report.duration,
            "node_count": len(report.nodes),
            "failed_nodes": [n.node_id for n in report.nodes if n.status == "failed"],
        }
        ctx.log(
            f"子流水线完成：run_id={report.run_id} status={report.status} "
            f"duration={report.duration:.3f}s"
        )
        return {"result": result_value, "report": summary, "run_id": report.run_id}
