"""压缩算子：gzip 压缩写出与解压读取。

``compress.gzip`` 标记 ``deterministic=False``（有落盘副作用，结果不可缓存）；
``compress.ungzip`` 沿 io_ops 读算子惯例（默认 deterministic，不做标注）。
"""
from __future__ import annotations

import gzip

from ..operator import Operator
from ..params import Param
from ..registry import op


@op("compress.gzip")
class GzipCompress(Operator):
    """把文本 gzip 压缩后写出为文件。"""

    description = "文本 gzip 压缩写出"
    inputs = [("text", str)]
    outputs = [("path", str), ("bytes", int)]
    params = {
        "path": Param(str, doc="输出 gzip 文件路径，相对工作目录"),
        "encoding": Param(str, default="utf-8"),
        "mtime": Param(int, default=0, minimum=0, doc="写入 gzip 头的时间戳，默认 0 保证确定性"),
    }
    deterministic = False  # 落盘副作用：结果不可缓存

    def run(self, ctx, text, path, encoding, mtime):
        target = ctx.resolve_path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        # 用 gzip.compress 显式传 mtime 后二进制写出；gzip.open 的 wt 模式无法固定 mtime
        payload = gzip.compress(str(text).encode(encoding), mtime=mtime)
        target.write_bytes(payload)
        ctx.log(f"写出 gzip -> {target}（{len(payload)} 字节）")
        return {"path": str(target), "bytes": len(payload)}


@op("compress.ungzip")
class GzipDecompress(Operator):
    """读取 gzip 文件并解压为文本。"""

    description = "gzip 文件解压读取"
    outputs = [("text", str), ("length", int)]
    params = {
        "path": Param(str, doc="gzip 文件路径，相对工作目录"),
        "encoding": Param(str, default="utf-8"),
    }

    def run(self, ctx, path, encoding):
        target = ctx.resolve_path(path)
        if not target.exists():
            raise FileNotFoundError(f"gzip 文件不存在：{target}")
        text = gzip.decompress(target.read_bytes()).decode(encoding)
        ctx.log(f"读取 gzip <- {target}（{len(text)} 字符）")
        return {"text": text, "length": len(text)}
