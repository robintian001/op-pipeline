"""自定义算子插件示例：不修改框架代码，也能扩展算子库。

用法::

    python -m op_pipeline run examples/pipeline_text.toml \\
        --plugin examples/plugins/demo_ops.py

要点：
1. 任何模块只要 ``import`` 时执行了 ``@op`` 装饰器，就完成注册；
2. 算子只依赖 ``op_pipeline`` 的公开基类，不接触引擎内部；
3. 同一个算子可以被多个流水线复用，也可以被单独单测。
"""
from __future__ import annotations

import re
import time
from typing import Any

from op_pipeline import Operator, Param, op

_PROBE_STATE: dict[tuple[str, str], int] = {}


@op("demo.word_count")
class WordCount(Operator):
    """统计词频（英文按空白/标点切分，中文按字符计）。"""

    description = "词频统计"
    inputs = [("text", str)]
    outputs = [("counts", dict), ("total", int), ("unique", int)]
    params = {
        "min_len": Param(int, default=2, minimum=0, doc="词长下限"),
        "lowercase": Param(bool, default=True),
    }

    def run(self, ctx, text, min_len, lowercase):
        raw = re.findall(r"[A-Za-z']+", text.lower() if lowercase else text)
        counts: dict[str, int] = {}
        for word in raw:
            if len(word) < min_len:
                continue
            counts[word] = counts.get(word, 0) + 1
        ordered = dict(sorted(counts.items(), key=lambda kv: (-kv[1], kv[0])))
        ctx.log(f"统计 {len(raw)} 个词，{len(ordered)} 个不同词")
        return {"counts": ordered, "total": sum(ordered.values()), "unique": len(ordered)}


@op("demo.flaky_probe")
class FlakyProbe(Operator):
    """前 N 次故意失败、之后成功的算子，用来演示 retries / allow_failure。

    失败次数按 (run_id, node_id) 记，重跑新流水线会重置。
    """

    description = "前 N 次失败的探测算子（演示重试）"
    inputs = [("value", Any)]
    outputs = [("value", Any), ("attempts", int)]
    params = {
        "fail_times": Param(int, default=1, minimum=0, doc="前几次调用故意失败"),
        "sleep": Param(float, default=0.0, minimum=0.0, doc="每次模拟耗时"),
    }

    def run(self, ctx, value, fail_times, sleep):
        key = (ctx.run_id, ctx.node_id)
        attempts = _PROBE_STATE.get(key, 0) + 1
        _PROBE_STATE[key] = attempts
        if sleep:
            time.sleep(sleep)
        if attempts <= fail_times:
            raise RuntimeError(f"探测第 {attempts} 次失败（预期内，用于演示重试）")
        return {"value": value, "attempts": attempts}


@op("demo.text_stats")
class TextStats(Operator):
    """把多行文本压成几个标量指标，便于写进报告。"""

    description = "文本指标汇总"
    inputs = [("lines", list)]
    outputs = [("stats", dict)]
    params = {}

    def run(self, ctx, lines):
        items = list(lines or [])
        stats = {
            "行数": len(items),
            "总字符": sum(len(str(x)) for x in items),
            "平均长度": round(sum(len(str(x)) for x in items) / len(items), 1) if items else 0,
            "最长行": max((len(str(x)) for x in items), default=0),
        }
        return {"stats": stats}
