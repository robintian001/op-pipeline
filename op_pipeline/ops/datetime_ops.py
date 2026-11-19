"""时间算子：解析、偏移与当前时间（标准库 datetime，零第三方依赖）。

流水线里时间以两种等价形态流转：
- ``datetime``：ISO 8601 文本（``datetime.isoformat()``），可读、可序列化；
- ``ts``：Unix 时间戳（秒，float），便于算术与排序。

非法时间输入抛 ``ValueError``（属于普通异常，节点会按 ``retries`` 策略重试）。
``time.now`` 标记 ``deterministic=False``：当前时间属于外部状态，永不落入节点缓存。
"""
from __future__ import annotations

from datetime import datetime, timedelta

from ..operator import Operator
from ..params import Param
from ..registry import op


def _parse_iso(text: str) -> datetime:
    """ISO 8601 文本 -> datetime；非法输入抛 ValueError（可重试类错误）。"""
    try:
        return datetime.fromisoformat(text)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"无法把 {text!r} 解析为 ISO 8601 时间：{exc}") from exc


@op("time.parse")
class TimeParse(Operator):
    """解析时间文本：缺省按 ISO 8601 解析，给了 ``format`` 则按 strftime 解析。"""

    description = "解析时间文本（ISO 或自定义格式）为 datetime 与时间戳"
    inputs = [("text", str)]
    outputs = [("datetime", str), ("ts", float)]
    params = {
        "format": Param(str, default=None, doc="strftime 格式；None 时按 ISO 8601 解析"),
    }

    def run(self, ctx, text, format):
        if text is None or str(text).strip() == "":
            raise ValueError("time.parse 收到空时间文本，无法解析")
        text = str(text)
        if format:
            try:
                dt = datetime.strptime(text, format)
            except (TypeError, ValueError) as exc:
                raise ValueError(f"无法按格式 '{format}' 解析时间文本 {text!r}：{exc}") from exc
        else:
            dt = _parse_iso(text)
        ts = dt.timestamp()
        ctx.log(f"解析时间 {text!r} -> {dt.isoformat()}（ts={ts}）")
        return {"datetime": dt.isoformat(), "ts": ts}


@op("time.shift")
class TimeShift(Operator):
    """时间偏移：``text``（ISO 文本）与 ``ts``（时间戳）两种输入都接受，
    至少给其一；按 days/hours/minutes/seconds 平移后输出双视图。
    """

    description = "时间偏移（ISO 文本或时间戳输入）"
    inputs = [("text", str, True), ("ts", float, True)]
    outputs = [("datetime", str), ("ts", float)]
    params = {
        "days": Param(int, default=0, doc="偏移天数，可为负"),
        "hours": Param(int, default=0, doc="偏移小时数，可为负"),
        "minutes": Param(int, default=0, doc="偏移分钟数，可为负"),
        "seconds": Param(float, default=0, doc="偏移秒数（可为小数），可为负"),
    }

    def run(self, ctx, text, ts, days, hours, minutes, seconds):
        if text is not None and str(text).strip() != "":
            dt = _parse_iso(str(text))
        elif ts is not None:
            try:
                dt = datetime.fromtimestamp(float(ts))
            except (TypeError, ValueError, OverflowError, OSError) as exc:
                raise ValueError(f"无法把时间戳 {ts!r} 还原为 datetime：{exc}") from exc
        else:
            raise ValueError("time.shift 需要 text（ISO 文本）或 ts（时间戳）至少一个输入")
        shifted = dt + timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds)
        ctx.log(f"时间偏移 {dt.isoformat()} + ({days}d {hours}h {minutes}m {seconds}s) -> {shifted.isoformat()}")
        return {"datetime": shifted.isoformat(), "ts": shifted.timestamp()}


@op("time.now")
class TimeNow(Operator):
    """当前时间：输出 datetime（ISO 文本）、时间戳与日期（YYYY-MM-DD）三视图。

    ``deterministic=False``：当前时间属于外部状态，即使节点配置了 ``cache``
    也永不落入缓存（与 http.request / db.* 算子同策略）。
    """

    description = "当前时间（datetime / 时间戳 / 日期三视图）"
    outputs = [("datetime", str), ("ts", float), ("date", str)]
    deterministic = False  # 外部状态：结果不可缓存

    def run(self, ctx):
        now = datetime.now()
        ctx.log(f"当前时间 {now.isoformat()}")
        return {
            "datetime": now.isoformat(),
            "ts": now.timestamp(),
            "date": now.date().isoformat(),
        }
