"""MySQL / PostgreSQL 算子测试：延迟导入报错 + 参数校验（无需真实数据库）。

运行：``python -m unittest discover -s tests -k test_db_mysql_pg -v``

延迟导入路径用 ``sys.modules`` 置 ``None`` 模拟「未安装驱动」，在任何环境（装没装
pymysql/psycopg2）都稳定：``import pymysql`` 遇到 ``sys.modules["pymysql"]=None``
会抛 ``ImportError``，正好走算子内的延迟导入兜底分支。
"""
from __future__ import annotations

import sys
import tempfile
import unittest
import unittest.mock
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from op_pipeline import ParamValidationError, RunContext, default_registry  # noqa: E402


def _make_ctx(op_name: str):
    """构造最小 NodeContext：校验/延迟导入在 run 早期触发，不触真实数据库。"""
    run_ctx = RunContext.create(pipeline="t", params={}, workdir=tempfile.gettempdir())
    return run_ctx.for_node("n", op_name)


class MysqlOpTest(unittest.TestCase):
    """db.read_mysql / db.write_mysql：延迟导入报错与参数校验。"""

    def test_lazy_import_error_on_read(self):
        # 模拟未安装 pymysql：sys.modules 置 None 使 `import pymysql` 抛 ImportError
        with unittest.mock.patch.dict(sys.modules, {"pymysql": None}):
            op_ = default_registry.get("db.read_mysql")(
                dsn="mysql://user:pass@localhost:3306/db", query="SELECT 1"
            )
            with self.assertRaises(RuntimeError) as cm:
                op_.execute(ctx=_make_ctx("db.read_mysql"), inputs={})
        self.assertIn("pymysql", str(cm.exception))
        self.assertIn("op-pipeline[mysql]", str(cm.exception))

    def test_lazy_import_error_on_write(self):
        with unittest.mock.patch.dict(sys.modules, {"pymysql": None}):
            op_ = default_registry.get("db.write_mysql")(
                dsn="mysql://user:pass@localhost:3306/db", table="t"
            )
            with self.assertRaises(RuntimeError) as cm:
                op_.execute(ctx=_make_ctx("db.write_mysql"), inputs={})
        self.assertIn("pymysql", str(cm.exception))
        self.assertIn("op-pipeline[mysql]", str(cm.exception))

    def test_read_requires_dsn(self):
        with self.assertRaises(ParamValidationError):
            default_registry.get("db.read_mysql")(query="SELECT 1")

    def test_write_requires_dsn(self):
        with self.assertRaises(ParamValidationError):
            default_registry.get("db.write_mysql")(table="t")

    def test_write_requires_table(self):
        with self.assertRaises(ParamValidationError):
            default_registry.get("db.write_mysql")(dsn="mysql://user:pass@localhost:3306/db")

    def test_read_requires_query_or_table(self):
        op_ = default_registry.get("db.read_mysql")(dsn="mysql://user:pass@localhost:3306/db")
        with self.assertRaises(ParamValidationError):
            op_.execute(ctx=_make_ctx("db.read_mysql"), inputs={})

    def test_deterministic_is_false(self):
        for name in ("db.read_mysql", "db.write_mysql"):
            cls = default_registry.get(name)
            self.assertFalse(cls.deterministic, f"{name} 涉及外部状态，不得缓存")


class PostgresOpTest(unittest.TestCase):
    """db.read_postgres / db.write_postgres：延迟导入报错与参数校验。"""

    def test_lazy_import_error_on_read(self):
        # 模拟未安装 psycopg2：sys.modules 置 None 使 `import psycopg2` 抛 ImportError
        with unittest.mock.patch.dict(sys.modules, {"psycopg2": None}):
            op_ = default_registry.get("db.read_postgres")(
                dsn="postgresql://user:pass@localhost:5432/db", query="SELECT 1"
            )
            with self.assertRaises(RuntimeError) as cm:
                op_.execute(ctx=_make_ctx("db.read_postgres"), inputs={})
        self.assertIn("psycopg2", str(cm.exception))
        self.assertIn("op-pipeline[postgres]", str(cm.exception))

    def test_lazy_import_error_on_write(self):
        with unittest.mock.patch.dict(sys.modules, {"psycopg2": None}):
            op_ = default_registry.get("db.write_postgres")(
                dsn="postgresql://user:pass@localhost:5432/db", table="t"
            )
            with self.assertRaises(RuntimeError) as cm:
                op_.execute(ctx=_make_ctx("db.write_postgres"), inputs={})
        self.assertIn("psycopg2", str(cm.exception))
        self.assertIn("op-pipeline[postgres]", str(cm.exception))

    def test_read_requires_dsn(self):
        with self.assertRaises(ParamValidationError):
            default_registry.get("db.read_postgres")(query="SELECT 1")

    def test_write_requires_dsn(self):
        with self.assertRaises(ParamValidationError):
            default_registry.get("db.write_postgres")(table="t")

    def test_write_requires_table(self):
        with self.assertRaises(ParamValidationError):
            default_registry.get("db.write_postgres")(dsn="postgresql://user:pass@localhost:5432/db")

    def test_read_requires_query_or_table(self):
        op_ = default_registry.get("db.read_postgres")(dsn="postgresql://user:pass@localhost:5432/db")
        with self.assertRaises(ParamValidationError):
            op_.execute(ctx=_make_ctx("db.read_postgres"), inputs={})

    def test_deterministic_is_false(self):
        for name in ("db.read_postgres", "db.write_postgres"):
            cls = default_registry.get(name)
            self.assertFalse(cls.deterministic, f"{name} 涉及外部状态，不得缓存")


if __name__ == "__main__":
    unittest.main()
