"""HTTP 算子：把出站请求收进算子层（标准库 urllib，零第三方依赖）。"""
from __future__ import annotations

import json
import urllib.error
import urllib.request
from typing import Any

from ..errors import ConfigError
from ..operator import Operator
from ..params import Param
from ..registry import op

_VALID_METHODS = ("GET", "POST")


@op("http.request")
class HttpRequest(Operator):
    """发起 HTTP 请求，返回状态码/响应头/文本与可选的 json 视图。

    状态码 2xx/3xx 视为 ``ok=True``；4xx/5xx 也会返回响应体（text/json 视图），
    但 ``ok=False``。连接失败（URLError）视为节点失败并走重试策略；
    方法不合法（非 GET/POST）抛 ``ConfigError``（配置错误，不重试）。
    """

    description = "HTTP 请求（GET/POST），json/text 双视图"
    outputs = [
        ("status", int),
        ("headers", dict),
        ("json", Any, True),
        ("text", str),
        ("url", str),
        ("ok", bool),
    ]
    params = {
        "url": Param(str, doc="请求 URL"),
        "method": Param(str, default="GET", doc="HTTP 方法：GET 或 POST"),
        "headers": Param(dict, default=None, doc="附加请求头"),
        "timeout": Param(float, default=10.0, minimum=0.1, doc="超时秒数"),
        "body": Param(str, default=None, doc="原始请求体（POST）"),
        "json_body": Param(Any, default=None, doc="JSON 请求体（POST，自动序列化并设 Content-Type）"),
    }
    deterministic = False  # 外部状态：结果不可缓存

    def run(self, ctx, url, method, headers, timeout, body, json_body):
        method = (method or "GET").upper()
        if method not in _VALID_METHODS:
            raise ConfigError(f"http.request 仅支持 GET/POST，收到：{method}")
        req_headers = dict(headers or {})
        data = None
        if method == "POST":
            if json_body is not None:
                data = json.dumps(json_body).encode("utf-8")
                req_headers.setdefault("Content-Type", "application/json")
            elif body:
                data = body.encode("utf-8")
        request = urllib.request.Request(url, data=data, headers=req_headers, method=method)
        try:
            with urllib.request.urlopen(request, timeout=timeout) as resp:
                status, headers_out, text = _read_response(resp)
        except urllib.error.HTTPError as exc:
            # HTTPError 也是"有响应的失败"：返回状态码与响应体，ok=False
            status, headers_out, text = _read_response(exc)
        except urllib.error.URLError as exc:
            reason = exc.reason if isinstance(exc.reason, str) else str(exc.reason)
            raise RuntimeError(f"http.request 请求失败：{reason}") from exc
        parsed = _try_json(headers_out, text)
        ctx.log(f"http {method} {url} -> {status}（{len(text)} 字符）")
        return {
            "status": status,
            "headers": headers_out,
            "json": parsed,
            "text": text,
            "url": url,
            "ok": 200 <= status < 400,
        }


def _read_response(resp: Any) -> tuple[int, dict[str, str], str]:
    raw = resp.read()
    charset = getattr(resp.headers, "get_content_charset", lambda: None)() or "utf-8"
    text = raw.decode(charset, errors="replace")
    status = getattr(resp, "status", getattr(resp, "code", 0))
    headers = {k: v for k, v in resp.headers.items()}
    return status, headers, text


def _try_json(headers: dict[str, str], text: str) -> Any:
    """json 视图规则：Content-Type 含 json，或文本以 {/[ 开头时尝试解析。"""
    ctype = (headers.get("Content-Type") or "").lower()
    if "json" not in ctype:
        stripped = text.lstrip()
        if not (stripped.startswith("{") or stripped.startswith("[")):
            return None
    try:
        return json.loads(text)
    except (ValueError, TypeError):
        return None
