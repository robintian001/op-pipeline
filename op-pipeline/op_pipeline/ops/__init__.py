"""内置算子包：导入即注册（``@op`` 装饰器副作用）。"""
from . import (  # noqa: F401
    control_ops,
    io_ops,
    report_ops,
    stats_ops,
    table_ops,
    text_ops,
)

__all__ = ["control_ops", "io_ops", "report_ops", "stats_ops", "table_ops", "text_ops"]
