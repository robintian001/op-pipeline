"""scheduler_app —— op_pipeline 的独立外围服务子包（M5-2）。

调度守护进程（scheduler.py，Task 5）+ 轻量 Web UI（web.py，Task 6）。
本包是项目根下的**独立包**，只 import op_pipeline 的公开 API；
op_pipeline 内核保持"零依赖、零守护进程"承诺，任何内核模块不得引用本包
（tests/test_scheduler_app.py 有断言）。

仅使用 Python 标准库：argparse / sqlite3 / http.server / threading / time。

用法::

    from scheduler_app import run_scheduler, create_server

    # 调度：每 60 秒跑一次流水线，结果写 RunStore
    run_scheduler("pipeline.json", every=60, db="runs.db")

    # Web UI：监听随机端口（port=0 自动分配）
    server = create_server("runs.db", port=0)
"""
from __future__ import annotations

from .scheduler import main as scheduler_main, run_scheduler
from .web import create_server

__all__ = ["create_server", "run_scheduler", "scheduler_main"]
__version__ = "0.1.0"
