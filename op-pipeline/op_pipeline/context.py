"""运行上下文：算子拿到的"环境句柄"。

算子不直接碰全局状态、不自己拼路径，所有副作用都通过 ctx 走，好处是：
- 单测时可以构造一个假 ctx，不需要真文件系统；
- 日志、产物目录、缓存、参数都被统一治理；
- 未来要接"分布式执行/多租户"时只需要换 ctx 实现。
"""
from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping


class NodeContext:
    """绑定到某个节点的上下文。"""

    def __init__(
        self,
        run: "RunContext",
        node_id: str,
        op_name: str,
    ) -> None:
        self.run = run
        self.node_id = node_id
        self.op_name = op_name
        self.params: Mapping[str, Any] = {}
        self.logger = run.logger.getChild(node_id) if hasattr(run.logger, "getChild") else run.logger

    # ------------------------------------------------------------------ #
    @property
    def run_id(self) -> str:
        return self.run.run_id

    @property
    def pipeline_params(self) -> Mapping[str, Any]:
        return self.run.params

    def log(self, message: str, *, level: int = logging.INFO) -> None:
        self.logger.log(level, "[%s] %s", self.node_id, message)

    def artifact_path(self, name: str) -> Path:
        """为本节点准备一个落盘路径（自动建目录）。"""
        path = self.run.artifacts_dir / self.node_id / name
        path.parent.mkdir(parents=True, exist_ok=True)
        return path

    def save_artifact(self, name: str, payload: Any) -> Path:
        path = self.artifact_path(name)
        if isinstance(payload, (dict, list)):
            path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        elif isinstance(payload, str):
            path.write_text(payload, encoding="utf-8")
        elif isinstance(payload, bytes):
            path.write_bytes(payload)
        else:
            raise TypeError(f"save_artifact 只支持 dict/list/str/bytes，收到 {type(payload).__name__}")
        self.log(f"产物已写入 {path}")
        return path

    def resolve_path(self, raw: str) -> Path:
        """把相对路径解析到 `工作目录`，绝对路径原样返回。"""
        path = Path(raw).expanduser()
        return path if path.is_absolute() else self.run.workdir / path


@dataclass
class RunContext:
    run_id: str
    pipeline: str
    params: dict[str, Any]
    workdir: Path
    artifacts_dir: Path
    logger: logging.Logger
    cache: Any = None
    started_at: float = field(default_factory=time.time)
    metadata: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def create(
        cls,
        *,
        pipeline: str,
        params: Mapping[str, Any],
        workdir: str | Path,
        run_id: str | None = None,
        logger: logging.Logger | None = None,
        cache: Any = None,
    ) -> "RunContext":
        workdir_path = Path(workdir).expanduser().resolve()
        run_id = run_id or time.strftime("%Y%m%d-%H%M%S")
        artifacts = workdir_path / ".op_pipeline" / "artifacts" / run_id
        artifacts.mkdir(parents=True, exist_ok=True)
        return cls(
            run_id=run_id,
            pipeline=pipeline,
            params=dict(params),
            workdir=workdir_path,
            artifacts_dir=artifacts,
            logger=logger or logging.getLogger("op_pipeline"),
            cache=cache,
        )

    def for_node(self, node_id: str, op_name: str) -> NodeContext:
        return NodeContext(self, node_id, op_name)
