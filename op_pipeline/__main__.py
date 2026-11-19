"""让 ``python -m op_pipeline`` 可用。"""
from __future__ import annotations

from .cli import main

if __name__ == "__main__":
    raise SystemExit(main())
