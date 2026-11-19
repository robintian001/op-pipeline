"""依赖图渲染：把配置变成可评审的图（Mermaid / Graphviz / 文本）。"""
from __future__ import annotations

from typing import Any

from .spec import Plan


def _label(node: Any) -> str:
    outputs = ", ".join(node.handles.values())
    return f"{node.spec.id}<br/>{node.spec.op}" + (f"<br/>{outputs}" if outputs else "")


def to_mermaid(plan: Plan) -> str:
    lines = ["flowchart LR"]
    for node in plan.nodes:
        lines.append(f'  {node.spec.id}["{_label(node)}"]')
    for node in plan.nodes:
        for dep in node.depends_on:
            lines.append(f"  {dep} --> {node.spec.id}")
    lines.append("  classDef cache fill:#e8f5f2,stroke:#0f766e")
    cached = [n.spec.id for n in plan.nodes if n.spec.cache]
    if cached:
        lines.append(f"  class {','.join(cached)} cache")
    return "\n".join(lines)


def to_dot(plan: Plan) -> str:
    lines = ["digraph pipeline {", "  rankdir=LR;", '  node [shape=box, style="rounded", fontname="Helvetica"];']
    for node in plan.nodes:
        label = f"{node.spec.id}\\n{node.spec.op}"
        if node.spec.cache:
            label += "\\n(cache)"
        lines.append(f'  "{node.spec.id}" [label="{label}"];')
    for node in plan.nodes:
        for dep in node.depends_on:
            lines.append(f'  "{dep}" -> "{node.spec.id}";')
    lines.append("}")
    return "\n".join(lines)


def to_ascii(plan: Plan) -> str:
    id_w = max((len(n.spec.id) for n in plan.nodes), default=4)
    op_w = max((len(n.spec.op) for n in plan.nodes), default=4)
    lines = []
    for index, node in enumerate(plan.nodes, 1):
        deps = ", ".join(node.depends_on) or "<入口>"
        flags = []
        if node.spec.cache:
            flags.append("cache")
        if node.spec.retries:
            flags.append(f"retry×{node.spec.retries}")
        if node.spec.when:
            flags.append(f"when: {node.spec.when}")
        suffix = ("  [" + "; ".join(flags) + "]") if flags else ""
        lines.append(
            f"{index:>2}. {node.spec.id:<{id_w}}  {node.spec.op:<{op_w}}  <- {deps}{suffix}"
        )
    return "\n".join(lines)
