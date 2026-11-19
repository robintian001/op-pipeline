"""文本算子。"""
from __future__ import annotations

import re

from ..operator import Operator
from ..params import Param
from ..registry import op

_TEMPLATE_PATTERN = re.compile(r"\{\{([A-Za-z_][A-Za-z0-9_]*)\}\}")


@op("text.template")
class RenderTemplate(Operator):
    """用 ``{{key}}`` 占位符渲染文本（避开流水线自己的 ``${}`` 语法）。"""

    description = "模板渲染"
    outputs = [("text", str), ("length", int)]
    params = {
        "template": Param(str),
        "values": Param(dict, default=None, doc="占位符取值"),
    }

    def run(self, ctx, template, values):
        values = values or {}
        missing: set[str] = set()

        def repl(match: re.Match[str]) -> str:
            key = match.group(1)
            if key not in values:
                missing.add(key)
                return match.group(0)
            return str(values[key])

        text = _TEMPLATE_PATTERN.sub(repl, template)
        if missing:
            raise KeyError(f"模板占位符未提供取值：{', '.join(sorted(missing))}")
        return {"text": text, "length": len(text)}


@op("text.split_lines")
class SplitLines(Operator):
    """按行切分文本。"""

    description = "文本按行切分"
    inputs = [("text", str)]
    outputs = [("lines", list), ("count", int)]
    params = {
        "strip": Param(bool, default=True),
        "skip_empty": Param(bool, default=True),
    }

    def run(self, ctx, text, strip, skip_empty):
        lines = str(text).splitlines()
        if strip:
            lines = [line.strip() for line in lines]
        if skip_empty:
            lines = [line for line in lines if line]
        return {"lines": lines, "count": len(lines)}


@op("text.join_lines")
class JoinLines(Operator):
    """列表按分隔符拼回文本。"""

    description = "列表拼接为文本"
    inputs = [("lines", list)]
    outputs = [("text", str), ("length", int)]
    params = {"sep": Param(str, default="\n")}

    def run(self, ctx, lines, sep):
        text = sep.join(str(item) for item in lines or [])
        return {"text": text, "length": len(text)}


@op("text.regex_extract")
class RegexExtract(Operator):
    """正则抽取，返回匹配列表。"""

    description = "正则抽取"
    inputs = [("text", str)]
    outputs = [("matches", list), ("count", int)]
    params = {
        "pattern": Param(str),
        "group": Param(int, default=0, minimum=0, doc="取第几个捕获组"),
        "ignore_case": Param(bool, default=False),
    }

    def run(self, ctx, text, pattern, group, ignore_case):
        flags = re.IGNORECASE if ignore_case else 0
        compiled = re.compile(pattern, flags)
        if group:
            matches = [m.group(group) for m in compiled.finditer(str(text)) if m.lastindex and m.lastindex >= group]
        else:
            matches = [m.group(0) for m in compiled.finditer(str(text))]
        return {"matches": matches, "count": len(matches)}


@op("text.replace_regex")
class ReplaceRegex(Operator):
    """正则替换。"""

    description = "正则替换"
    inputs = [("text", str)]
    outputs = [("text", str), ("replacements", int)]
    params = {
        "pattern": Param(str),
        "replacement": Param(str),
        "count": Param(int, default=0, minimum=0, doc="0 表示全部替换"),
    }

    def run(self, ctx, text, pattern, replacement, count):
        result, n = re.subn(pattern, replacement, str(text), count=count)
        return {"text": result, "replacements": n}
