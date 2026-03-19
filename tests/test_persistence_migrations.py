"""Tests for database migration system with PostgreSQL DDL and repository layer."""

from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from quant_exchange.persistence.migrations import (
    BatchWriteConfig,
    DataTier,
    IndexDefinition,
    Migration,
    MigrationManager,
    MigrationStatus,
    PostgreSQLDDL,
)


class MigrationTests(unittest.TestCase):
    """Test migration dataclass."""

    def test_migration_creation(self) -> None:
        """Verify migration is created correctly."""
        migration = Migration(
            version="001",
            description="Create users table",
            up_sql="CREATE TABLE users (id TEXT PRIMARY KEY);",
            down_sql="DROP TABLE users;",
        )
        self.assertEqual(migration.version, "001")
        self.assertEqual(migration.description, "Create users table")
        self.assertEqual(migration.dependencies, [])


class PostgreSQLDDLTests(unittest.TestCase):
    """Test PostgreSQL DDL generation."""

    def test_generate_create_table_sql(self) -> None:
        """Verify CREATE TABLE SQL generation."""
        sql = PostgreSQLDDL.generate_create_table_sql(
            table_name="test_table",
            columns={
                "id": "VARCHAR(100) PRIMARY KEY",
                "name": "VARCHAR(200) NOT NULL",
                "created_at": "TIMESTAMPTZ NOT NULL",
            },
        )
        self.assertIn("CREATE TABLE test_table", sql)
        self.assertIn("id", sql)
        self.assertIn("name", sql)

    def test_generate_create_index_sql(self) -> None:
        """Verify CREATE INDEX SQL generation."""
        index = IndexDefinition(
            name="idx_test_table_name",
            table="test_table",
            columns=["name"],
        )
        sql = PostgreSQLDDL.generate_create_index_sql(index)
        self.assertIn("CREATE INDEX idx_test_table_name", sql)
        self.assertIn("test_table", sql)
        self.assertIn("name", sql)

    def test_generate_unique_index_sql(self) -> None:
        """Verify unique index SQL generation."""
        index = IndexDefinition(
            name="idx_test_table_unique",
            table="test_table",
            columns=["email"],
            is_unique=True,
        )
        sql = PostgreSQLDDL.generate_create_index_sql(index)
        self.assertIn("UNIQUE INDEX", sql)

    def test_system_tables_ddl(self) -> None:
        """Verify system tables DDL generation."""
        ddl_list = PostgreSQLDDL.system_tables_ddl()
        self.assertGreater(len(ddl_list), 0)
        self.assertTrue(any("schema_migrations" in sql for sql in ddl_list))

    def test_core_tables_ddl(self) -> None:
        """Verify core tables DDL generation."""
        ddl_list = PostgreSQLDDL.core_tables_ddl()
        self.assertGreater(len(ddl_list), 0)
        self.assertTrue(any("sys_users" in sql for sql in ddl_list))

    def test_reference_tables_ddl(self) -> None:
        """Verify reference tables DDL generation."""
        ddl_list = PostgreSQLDDL.reference_tables_ddl()
        self.assertGreater(len(ddl_list), 0)
        self.assertTrue(any("ref_exchanges" in sql for sql in ddl_list))

    def test_trading_tables_ddl(self) -> None:
        """Verify trading tables DDL generation."""
        ddl_list = PostgreSQLDDL.trading_tables_ddl()
        self.assertGreater(len(ddl_list), 0)
        self.assertTrue(any("trade_orders" in sql for sql in ddl_list))

    def test_market_data_tables_ddl(self) -> None:
        """Verify market data tables DDL generation."""
        ddl_list = PostgreSQLDDL.market_data_tables_ddl()
        self.assertGreater(len(ddl_list), 0)
        self.assertTrue(any("mkt_market_klines" in sql for sql in ddl_list))


class BatchWriteConfigTests(unittest.TestCase):
    """Test batch write configuration."""

    def test_default_config(self) -> None:
        """Verify default batch write config."""
        config = BatchWriteConfig()
        self.assertEqual(config.batch_size, 1000)
        self.assertEqual(config.max_workers, 4)
        self.assertEqual(config.flush_interval_seconds, 5.0)

    def test_custom_config(self) -> None:
        """Verify custom batch write config."""
        config = BatchWriteConfig(batch_size=500, max_workers=8)
        self.assertEqual(config.batch_size, 500)
        self.assertEqual(config.max_workers, 8)


class IndexDefinitionTests(unittest.TestCase):
    """Test index definition."""

    def test_index_definition_defaults(self) -> None:
        """Verify index definition defaults."""
        index = IndexDefinition(
            name="idx_test",
            table="test_table",
            columns=["col1"],
        )
        self.assertFalse(index.is_unique)
        self.assertFalse(index.is_partial)
        self.assertIsNone(index.where_clause)
        self.assertEqual(index.include_columns, [])


if __name__ == "__main__":
    unittest.main()
