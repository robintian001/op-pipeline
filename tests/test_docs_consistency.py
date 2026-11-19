"""文档一致性测试（M6-4）：算子清单声明的数量必须与注册表实际一致。

解析 ``docs/reference/operator-catalog.md``：
1. 表头的"共 N 个算子"；
2. 每个 ``## x. …（N 个）`` 分组标题里的数量。

断言表头数量 == 各分组数量之和 == 内置算子实际数。内置算子从
``default_registry`` 中按模块前缀 ``op_pipeline.ops.*`` 统计——测试发现机制
会 import 其它测试模块，其中的示例 ``@op`` 也注册进全局表，直接用
``len(default_registry)`` 会把这些测试算子算进来。
三者不一致时在断言信息里打印总数、分组和、注册表数与分组明细，便于定位漂移。

注意：``## 11. 示例插件算子`` 这类分组不属于内置算子，标题不含"（N 个）"
因而不会被统计；若将来出现标题带数量但确实不属于内置注册表的分组，
请加入 ``_NON_BUILTIN_HEADINGS`` 并在注释里写明原因。
"""
from __future__ import annotations

import re
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from op_pipeline import default_registry  # noqa: E402

CATALOG = ROOT / "docs" / "reference" / "operator-catalog.md"

_TOTAL_RE = re.compile(r"共\s*(\d+)\s*个算子")
_GROUP_RE = re.compile(r"^##\s+(\d+)\.\s*(.+)$")
_GROUP_COUNT_RE = re.compile(r"（[^）]*?(\d+)\s*个")

# 标题含数量但不计入内置注册表的分组（当前无；示例插件分组标题本身不含数量）。
_NON_BUILTIN_HEADINGS: tuple[str, ...] = ("示例插件",)


class DocsConsistencyTest(unittest.TestCase):
    """校验算子清单与内置注册表的数量一致性。"""

    def test_operator_catalog_counts_match_registry(self) -> None:
        """表头数量、分组数量之和与注册表算子数三者必须一致。"""
        text = CATALOG.read_text(encoding="utf-8")

        total_match = _TOTAL_RE.search(text)
        self.assertIsNotNone(
            total_match,
            f"{CATALOG} 未找到表头数量声明（期望形如“共 N 个算子”）",
        )
        declared_total = int(total_match.group(1))

        groups: dict[str, int] = {}
        for line in text.splitlines():
            heading = _GROUP_RE.match(line)
            if heading is None:
                continue
            title = heading.group(2)
            count_match = _GROUP_COUNT_RE.search(title)
            if count_match is None:
                continue
            if any(marker in title for marker in _NON_BUILTIN_HEADINGS):
                continue
            groups[f"{heading.group(1)}. {title}"] = int(count_match.group(1))

        self.assertTrue(groups, f"{CATALOG} 未解析到任何“（N 个）”分组数量")

        group_sum = sum(groups.values())
        # 测试发现机制会 import 其它测试模块，其中的示例 @op 注册进全局
        # default_registry；按模块前缀只统计 op_pipeline.ops.* 的内置算子。
        registry_items = default_registry.describe()
        registry_count = sum(
            1
            for item in registry_items
            if str(item.get("module", "")).startswith("op_pipeline.ops.")
        )
        detail = "；".join(f"{name} = {count}" for name, count in groups.items())
        message = (
            "算子数量漂移："
            f"表头声明 {declared_total}，分组标题合计 {group_sum}，"
            f"注册表内置算子实际 {registry_count}"
            f"（default_registry 共 {len(registry_items)}，含测试模块注册）\n"
            f"分组明细：{detail}"
        )

        self.assertEqual(declared_total, group_sum, message)
        self.assertEqual(declared_total, registry_count, message)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
