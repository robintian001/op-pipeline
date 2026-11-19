"""库嵌入模式：把 op_pipeline 当作库嵌进自己的应用（不经过 CLI）。

M1.5 交付的三个嵌入能力一次讲清：

1. 独立注册表：``Registry()`` 只放本应用需要的算子（内置算子自动补齐），
   ``default_registry`` 全程不受影响——适合同进程跑多套互不污染的算子集；
2. 取消事件：``cancel_event`` 置位后在跑节点自然结束、未调度节点跳过，
   报告给出 partial 与取消告警——适合"用户点了停止"这类应用层中断；
3. 纯内存运行：临时目录 + ``cache_dir=None``，跑完不留任何 ``.op_pipeline/``
   与缓存目录——适合嵌入到服务进程里跑临时任务。

运行：python examples/embedded_lib_demo.py（无需安装，脚本自行定位仓库内的包源码）
"""
from __future__ import annotations

import sys
import tempfile
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))  # 未安装时定位包源码

from op_pipeline import (  # noqa: E402
    Operator,
    Param,
    PipelineSpec,
    Registry,
    default_registry,
    load_plugins,
    op,
    run_pipeline,
)

# --------------------------------------------------------------------------- #
# 独立注册表：自定义算子定向注册进 reg，不碰 default_registry
# --------------------------------------------------------------------------- #
reg = Registry()


@op("lib.slow_double", registry=reg)
class SlowDouble(Operator):
    """睡一会儿再翻倍——正好演示"在跑节点自然结束"的取消语义。"""

    description = "延迟后翻倍（取消演示用）"
    version = "1"
    deterministic = False  # 依赖时间，禁缓存
    inputs = [("value", int)]
    outputs = [("value", int)]
    params = {"delay": Param(float, default=0.5, minimum=0.0)}

    def run(self, ctx, value, delay):
        time.sleep(delay)
        return {"value": value * 2}


PIPELINE = {
    "name": "embedded_demo",
    "params": {"delay": 0.6},
    "nodes": [
        {"id": "src", "op": "control.const", "params": {"value": 21}},
        {
            "id": "dbl",
            "op": "lib.slow_double",
            "inputs": {"value": "${src.value}"},
            "params": {"delay": "${p.delay}"},
        },
        {
            "id": "tail",
            "op": "lib.slow_double",
            "inputs": {"value": "${dbl.value}"},
            "params": {"delay": 0.5},
        },
    ],
    "outputs": {"final": "${tail.value}"},
}


def run_once(**kwargs):
    """在一次性临时目录里跑流水线（本身也是"纯内存"用法的一部分）。"""
    with tempfile.TemporaryDirectory() as workdir:
        return run_pipeline(
            PipelineSpec.from_dict(PIPELINE),
            registry=reg,
            workdir=workdir,
            cache_dir=None,
            **kwargs,
        )


def main() -> None:
    print("== 1. 独立注册表 ==")
    load_plugins(registry=reg)  # 不带插件路径：仅为触发内置算子补齐
    print(f"reg 算子数：{len(reg)}（内置自动补齐 + lib.slow_double）")
    print(f"lib.slow_double in default_registry -> "
          f"{'lib.slow_double' in default_registry}")
    report, values = run_once()
    print(f"完整运行：{report.status}，句柄 tail.value = {values['tail.value']}"
          "（句柄 = 节点.端口）")

    print("\n== 2. 取消事件 ==")
    cancel_event = threading.Event()
    threading.Timer(0.2, cancel_event.set).start()  # 0.2s 后"用户点了停止"
    report, _ = run_once(cancel_event=cancel_event)
    for node in report.nodes:
        print(f"  {node.node_id:<4} {node.status:<8} {node.note}")
    print(f"  运行状态：{report.status}（在跑节点自然结束，未调度节点跳过）")
    print(f"  warnings：{report.warnings}")

    print("\n== 3. 纯内存运行 ==")
    with tempfile.TemporaryDirectory() as workdir:
        run_pipeline(
            PipelineSpec.from_dict(PIPELINE),
            registry=reg,
            workdir=workdir,
            cache_dir=None,
        )
        leftovers = sorted(p.name for p in Path(workdir).iterdir())
    print(f"  跑完后的 workdir：{leftovers or '空（产物目录惰性创建，未用不建）'}")


if __name__ == "__main__":
    main()
