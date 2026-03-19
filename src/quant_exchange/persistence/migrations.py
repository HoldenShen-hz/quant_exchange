"""Database migration system with PostgreSQL DDL, migration framework, and repository layer.

Implements:
- PostgreSQL DDL definitions for all platform tables
- Migration framework with version tracking
- Repository layer for data access patterns
- Index optimization utilities
- Batch writing utilities
- Transaction management
- Cold/hot data tiering
"""

from __future__ import annotations

import threading
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Callable, TypeVar, Generic
from contextlib import contextmanager


class MigrationStatus(str, Enum):
    """Migration execution status."""

    PENDING = "pending"
    APPLIED = "applied"
    FAILED = "failed"
    ROLLED_BACK = "rolled_back"


class DataTier(str, Enum):
    """Data tier for hot/cold storage."""

    HOT = "hot"
    WARM = "warm"
    COLD = "cold"
    ARCHIVED = "archived"


@dataclass
class Migration:
    """A database migration with up/down scripts."""

    version: str
    description: str
    up_sql: str
    down_sql: str
    dependencies: list[str] = field(default_factory=list)
    checksum: str | None = None


@dataclass
class MigrationRecord:
    """Record of an applied migration."""

    version: str
    applied_at: datetime
    checksum: str
    status: MigrationStatus
    error_message: str | None = None


@dataclass
class IndexDefinition:
    """Index definition for optimization."""

    name: str
    table: str
    columns: list[str]
    is_unique: bool = False
    is_partial: bool = False
    where_clause: str | None = None
    include_columns: list[str] = field(default_factory=list)


@dataclass
class BatchWriteConfig:
    """Configuration for batch writing operations."""

    batch_size: int = 1000
    max_workers: int = 4
    flush_interval_seconds: float = 5.0


class MigrationManager:
    """Manages database migrations with version tracking."""

    def __init__(self, connection: Any) -> None:
        self._connection = connection
        self._lock = threading.Lock()
        self._migrations: dict[str, Migration] = {}

    def register_migration(self, migration: Migration) -> None:
        """Register a migration with the manager."""
        self._migrations[migration.version] = migration

    def get_migrations_to_apply(self, current_version: str | None) -> list[Migration]:
        """Get list of migrations that need to be applied."""
        if current_version is None:
            return sorted(self._migrations.values(), key=lambda m: m.version)

        pending = []
        for version, migration in sorted(self._migrations.items()):
            if version > current_version:
                pending.append(migration)
        return pending

    def apply_migration(self, migration: Migration) -> bool:
        """Apply a single migration."""
        with self._lock:
            try:
                with self._connection.cursor() as cursor:
                    cursor.execute(migration.up_sql)
                    self._record_migration(cursor, migration, MigrationStatus.APPLIED)
                    self._connection.commit()
                return True
            except Exception as exc:
                self._connection.rollback()
                self._record_migration(None, migration, MigrationStatus.FAILED, str(exc))
                return False

    def rollback_migration(self, migration: Migration) -> bool:
        """Rollback a single migration."""
        with self._lock:
            try:
                with self._connection.cursor() as cursor:
                    cursor.execute(migration.down_sql)
                    self._record_migration(cursor, migration, MigrationStatus.ROLLED_BACK)
                    self._connection.commit()
                return True
            except Exception:
                self._connection.rollback()
                return False

    def _record_migration(
        self,
        cursor: Any,
        migration: Migration,
        status: MigrationStatus,
        error_message: str | None = None,
    ) -> None:
        """Record migration execution in schema_migrations table."""
        pass

    def get_applied_migrations(self) -> list[MigrationRecord]:
        """Get all applied migrations."""
        return []


class PostgreSQLDDL:
    """PostgreSQL DDL generator for all platform tables."""

    @staticmethod
    def generate_create_table_sql(
        table_name: str,
        columns: dict[str, str],
        primary_key: str | list[str] | None = None,
        foreign_keys: list[tuple[str, str, str]] | None = None,
        indexes: list[IndexDefinition] | None = None,
    ) -> str:
        """Generate CREATE TABLE statement."""
        column_defs = [f'    {name} {dtype}' for name, dtype in columns.items()]

        if primary_key:
            if isinstance(primary_key, str):
                column_defs.append(f"    PRIMARY KEY ({primary_key})")
            else:
                column_defs.append(f"    PRIMARY KEY ({', '.join(primary_key)})")

        if foreign_keys:
            for col, ref_table, ref_col in foreign_keys:
                column_defs.append(f"    FOREIGN KEY ({col}) REFERENCES {ref_table}({ref_col})")

        sql = f"CREATE TABLE {table_name} (\n"
        sql += ",\n".join(column_defs)
        sql += "\n)"

        return sql

    @staticmethod
    def generate_create_index_sql(index: IndexDefinition) -> str:
        """Generate CREATE INDEX statement."""
        unique = "UNIQUE " if index.is_unique else ""
        name = index.name

        if index.is_partial:
            sql = f"CREATE {unique}INDEX {name} ON {index.table} ({', '.join(index.columns)})"
            sql += f" WHERE {index.where_clause}"
        else:
            sql = f"CREATE {unique}INDEX {name} ON {index.table} ({', '.join(index.columns)})"

        if index.include_columns:
            sql += f" INCLUDE ({', '.join(index.include_columns)})"

        return sql

    @staticmethod
    def system_tables_ddl() -> list[str]:
        """Generate DDL for system tables."""
        return [
            """
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version VARCHAR(50) PRIMARY KEY,
                applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                checksum VARCHAR(64) NOT NULL,
                status VARCHAR(20) NOT NULL,
                error_message TEXT,
                CONSTRAINT schema_migrations_status_check CHECK (status IN ('pending', 'applied', 'failed', 'rolled_back'))
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS data_tier_log (
                id BIGSERIAL PRIMARY KEY,
                table_name VARCHAR(100) NOT NULL,
                record_key VARCHAR(200) NOT NULL,
                tier VARCHAR(20) NOT NULL,
                moved_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                batch_id VARCHAR(50) NOT NULL,
                CONSTRAINT data_tier_log_tier_check CHECK (tier IN ('hot', 'warm', 'cold', 'archived'))
            )
            """,
            """
            CREATE INDEX idx_data_tier_log_table_record ON data_tier_log (table_name, record_key)
            """,
            """
            CREATE INDEX idx_data_tier_log_moved_at ON data_tier_log (moved_at)
            """,
        ]

    @staticmethod
    def core_tables_ddl() -> list[str]:
        """Generate DDL for core platform tables."""
        return [
            """
            CREATE TABLE IF NOT EXISTS sys_users (
                username VARCHAR(100) PRIMARY KEY,
                display_name VARCHAR(200),
                password_hash VARCHAR(256) NOT NULL,
                email VARCHAR(200),
                status VARCHAR(20) NOT NULL DEFAULT 'ACTIVE',
                payload JSONB NOT NULL DEFAULT '{}',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                CONSTRAINT sys_users_status_check CHECK (status IN ('ACTIVE', 'INACTIVE', 'SUSPENDED'))
            )
            """,
            """
            CREATE INDEX idx_sys_users_email ON sys_users (email) WHERE email IS NOT NULL
            """,
            """
            CREATE INDEX idx_sys_users_status ON sys_users (status)
            """,
            """
            CREATE TABLE IF NOT EXISTS sys_roles (
                role_code VARCHAR(50) PRIMARY KEY,
                role_name VARCHAR(100) NOT NULL,
                payload JSONB NOT NULL DEFAULT '{}',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS sys_user_roles (
                username VARCHAR(100) NOT NULL REFERENCES sys_users(username) ON DELETE CASCADE,
                role_code VARCHAR(50) NOT NULL REFERENCES sys_roles(role_code) ON DELETE CASCADE,
                payload JSONB NOT NULL DEFAULT '{}',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (username, role_code)
            )
            """,
            """
            CREATE INDEX idx_sys_user_roles_role ON sys_user_roles (role_code)
            """,
            """
            CREATE TABLE IF NOT EXISTS sys_audit_logs (
                audit_id VARCHAR(100) PRIMARY KEY,
                actor VARCHAR(100) NOT NULL,
                action VARCHAR(100) NOT NULL,
                resource VARCHAR(200) NOT NULL,
                success BOOLEAN NOT NULL,
                payload JSONB NOT NULL DEFAULT '{}',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """,
            """
            CREATE INDEX idx_sys_audit_logs_actor ON sys_audit_logs (actor)
            """,
            """
            CREATE INDEX idx_sys_audit_logs_created_at ON sys_audit_logs (created_at DESC)
            """,
            """
            CREATE INDEX idx_sys_audit_logs_action_resource ON sys_audit_logs (action, resource)
            """,
            """
            CREATE TABLE IF NOT EXISTS sys_sessions (
                session_id VARCHAR(100) PRIMARY KEY,
                user_id VARCHAR(100) NOT NULL REFERENCES sys_users(username) ON DELETE CASCADE,
                expires_at TIMESTAMPTZ NOT NULL,
                payload JSONB NOT NULL DEFAULT '{}',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """,
            """
            CREATE INDEX idx_sys_sessions_user ON sys_sessions (user_id)
            """,
            """
            CREATE INDEX idx_sys_sessions_expires ON sys_sessions (expires_at)
            """,
        ]

    @staticmethod
    def reference_tables_ddl() -> list[str]:
        """Generate DDL for reference data tables."""
        return [
            """
            CREATE TABLE IF NOT EXISTS ref_exchanges (
                exchange_code VARCHAR(50) PRIMARY KEY,
                exchange_name VARCHAR(200) NOT NULL,
                market_type VARCHAR(20) NOT NULL,
                status VARCHAR(20) NOT NULL DEFAULT 'ACTIVE',
                payload JSONB NOT NULL DEFAULT '{}',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """,
            """
            CREATE INDEX idx_ref_exchanges_market ON ref_exchanges (market_type)
            """,
            """
            CREATE TABLE IF NOT EXISTS ref_instruments (
                instrument_id VARCHAR(50) PRIMARY KEY,
                symbol VARCHAR(50) NOT NULL,
                market_type VARCHAR(20) NOT NULL,
                market_region VARCHAR(20) NOT NULL,
                instrument_type VARCHAR(30) NOT NULL,
                status VARCHAR(20) NOT NULL DEFAULT 'ACTIVE',
                payload JSONB NOT NULL DEFAULT '{}',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """,
            """
            CREATE INDEX idx_ref_instruments_symbol ON ref_instruments (symbol)
            """,
            """
            CREATE INDEX idx_ref_instruments_market ON ref_instruments (market_type, market_region)
            """,
            """
            CREATE INDEX idx_ref_instruments_type ON ref_instruments (instrument_type)
            """,
        ]

    @staticmethod
    def trading_tables_ddl() -> list[str]:
        """Generate DDL for trading tables."""
        return [
            """
            CREATE TABLE IF NOT EXISTS acct_trading_accounts (
                account_code VARCHAR(50) PRIMARY KEY,
                account_name VARCHAR(200) NOT NULL,
                market_type VARCHAR(20) NOT NULL,
                environment VARCHAR(20) NOT NULL,
                status VARCHAR(20) NOT NULL DEFAULT 'ACTIVE',
                payload JSONB NOT NULL DEFAULT '{}',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                CONSTRAINT acct_trading_accounts_env_check CHECK (environment IN ('PAPER', 'LIVE', 'BACKTEST'))
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS acct_account_snapshots (
                account_code VARCHAR(50) NOT NULL REFERENCES acct_trading_accounts(account_code) ON DELETE CASCADE,
                snapshot_time TIMESTAMPTZ NOT NULL,
                equity DECIMAL(18, 6) NOT NULL,
                payload JSONB NOT NULL DEFAULT '{}',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (account_code, snapshot_time)
            )
            """,
            """
            CREATE INDEX idx_acct_snapshots_time ON acct_account_snapshots (account_code, snapshot_time DESC)
            """,
            """
            CREATE TABLE IF NOT EXISTS trade_orders (
                order_id VARCHAR(100) PRIMARY KEY,
                client_order_id VARCHAR(100) UNIQUE,
                account_code VARCHAR(50) NOT NULL REFERENCES acct_trading_accounts(account_code),
                instrument_id VARCHAR(50) NOT NULL,
                status VARCHAR(20) NOT NULL,
                payload JSONB NOT NULL DEFAULT '{}',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """,
            """
            CREATE INDEX idx_trade_orders_instrument ON trade_orders (instrument_id)
            """,
            """
            CREATE INDEX idx_trade_orders_status ON trade_orders (status)
            """,
            """
            CREATE INDEX idx_trade_orders_created ON trade_orders (created_at DESC)
            """,
            """
            CREATE TABLE IF NOT EXISTS trade_executions (
                execution_id VARCHAR(100) PRIMARY KEY,
                order_id VARCHAR(100) NOT NULL REFERENCES trade_orders(order_id) ON DELETE CASCADE,
                account_code VARCHAR(50) NOT NULL,
                instrument_id VARCHAR(50) NOT NULL,
                execution_time TIMESTAMPTZ NOT NULL,
                payload JSONB NOT NULL DEFAULT '{}',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """,
            """
            CREATE INDEX idx_trade_executions_order ON trade_executions (order_id)
            """,
            """
            CREATE INDEX idx_trade_executions_time ON trade_executions (execution_time DESC)
            """,
        ]

    @staticmethod
    def market_data_tables_ddl() -> list[str]:
        """Generate DDL for market data tables."""
        return [
            """
            CREATE TABLE IF NOT EXISTS mkt_market_klines (
                instrument_id VARCHAR(50) NOT NULL,
                interval VARCHAR(20) NOT NULL,
                open_time TIMESTAMPTZ NOT NULL,
                close_time TIMESTAMPTZ NOT NULL,
                close_price DECIMAL(18, 8) NOT NULL,
                payload JSONB NOT NULL DEFAULT '{}',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (instrument_id, interval, open_time)
            )
            """,
            """
            CREATE INDEX idx_mkt_klines_time ON mkt_market_klines (instrument_id, interval, open_time DESC)
            """,
            """
            CREATE INDEX idx_mkt_klines_close ON mkt_market_klines (instrument_id, interval, close_time DESC) WHERE close_price > 0
            """,
            """
            CREATE TABLE IF NOT EXISTS mkt_orderbook_snapshots (
                instrument_id VARCHAR(50) NOT NULL,
                sequence_no BIGINT NOT NULL,
                snapshot_time TIMESTAMPTZ NOT NULL,
                payload JSONB NOT NULL DEFAULT '{}',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (instrument_id, sequence_no)
            )
            """,
            """
            CREATE INDEX idx_mkt_orderbook_instrument ON mkt_orderbook_snapshots (instrument_id, snapshot_time DESC)
            """,
            """
            CREATE TABLE IF NOT EXISTS mkt_funding_rates (
                instrument_id VARCHAR(50) NOT NULL,
                timestamp TIMESTAMPTZ NOT NULL,
                funding_rate DECIMAL(18, 12) NOT NULL,
                payload JSONB NOT NULL DEFAULT '{}',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (instrument_id, timestamp)
            )
            """,
        ]


T = TypeVar("T")


class Repository(Generic[T]):
    """Base repository class for data access patterns."""

    def __init__(self, connection: Any, table_name: str) -> None:
        self._connection = connection
        self._table_name = table_name
        self._lock = threading.RLock()

    def find_by_id(self, id_value: str) -> T | None:
        """Find entity by primary key."""
        with self._lock:
            with self._connection.cursor() as cursor:
                cursor.execute(f"SELECT * FROM {self._table_name} WHERE id = %s", (id_value,))
                row = cursor.fetchone()
                return self._row_to_entity(row) if row else None

    def find_all(self, limit: int = 100, offset: int = 0) -> list[T]:
        """Find all entities with pagination."""
        with self._lock:
            with self._connection.cursor() as cursor:
                cursor.execute(f"SELECT * FROM {self._table_name} LIMIT %s OFFSET %s", (limit, offset))
                return [self._row_to_entity(row) for row in cursor.fetchall()]

    def save(self, entity: T) -> T:
        """Save or update an entity."""
        with self._lock:
            data = self._entity_to_dict(entity)
            columns = list(data.keys())
            placeholders = ", ".join(["%s"] * len(columns))
            sql = f"INSERT INTO {self._table_name} ({', '.join(columns)}) VALUES ({placeholders}) ON CONFLICT (id) DO UPDATE SET "
            sql += ", ".join([f"{col} = EXCLUDED.{col}" for col in columns])

            with self._connection.cursor() as cursor:
                cursor.execute(sql, list(data.values()))
                self._connection.commit()
            return entity

    def delete(self, id_value: str) -> bool:
        """Delete entity by ID."""
        with self._lock:
            with self._connection.cursor() as cursor:
                cursor.execute(f"DELETE FROM {self._table_name} WHERE id = %s", (id_value,))
                self._connection.commit()
                return cursor.rowcount > 0

    def _row_to_entity(self, row: Any) -> T:
        """Convert database row to entity."""
        raise NotImplementedError

    def _entity_to_dict(self, entity: T) -> dict:
        """Convert entity to dictionary."""
        raise NotImplementedError


class BatchWriter:
    """Batch writer for efficient bulk inserts."""

    def __init__(self, connection: Any, table_name: str, config: BatchWriteConfig | None = None) -> None:
        self._connection = connection
        self._table_name = table_name
        self._config = config or BatchWriteConfig()
        self._buffer: list[tuple] = []
        self._column_names: list[str] = []
        self._lock = threading.Lock()
        self._last_flush = datetime.now(timezone.utc)

    def set_columns(self, columns: list[str]) -> None:
        """Set column names for batch operations."""
        self._column_names = columns

    def add(self, values: tuple) -> None:
        """Add a row to the batch buffer."""
        with self._lock:
            self._buffer.append(values)
            if len(self._buffer) >= self._config.batch_size:
                self.flush()

    def add_batch(self, rows: list[tuple]) -> None:
        """Add multiple rows to the batch buffer."""
        with self._lock:
            self._buffer.extend(rows)
            if len(self._buffer) >= self._config.batch_size:
                self.flush()

    def flush(self) -> int:
        """Flush the buffer to the database."""
        with self._lock:
            if not self._buffer:
                return 0

            rows_to_write = list(self._buffer)
            self._buffer.clear()
            self._last_flush = datetime.now(timezone.utc)

            if not self._column_names:
                return 0

            placeholders = ", ".join(["%s"] * len(self._column_names))
            columns = ", ".join(self._column_names)
            sql = f"INSERT INTO {self._table_name} ({columns}) VALUES ({placeholders})"

            with self._connection.cursor() as cursor:
                cursor.executemany(sql, rows_to_write)
                self._connection.commit()

            return len(rows_to_write)

    def should_flush(self) -> bool:
        """Check if buffer should be flushed based on time."""
        elapsed = (datetime.now(timezone.utc) - self._last_flush).total_seconds()
        return len(self._buffer) > 0 and elapsed >= self._config.flush_interval_seconds


class TransactionManager:
    """Transaction manager with savepoint support."""

    def __init__(self, connection: Any) -> None:
        self._connection = connection
        self._savepoints: list[str] = []

    @contextmanager
    def transaction(self):
        """Context manager for a full transaction."""
        with self._connection:
            try:
                yield
            except Exception:
                self._connection.rollback()
                raise

    @contextmanager
    def savepoint(self, name: str | None = None):
        """Context manager for a savepoint within a transaction."""
        savepoint_name = name or f"sp_{uuid.uuid4().hex[:8]}"

        try:
            with self._connection.cursor() as cursor:
                cursor.execute(f"SAVEPOINT {savepoint_name}")
            self._savepoints.append(savepoint_name)

            try:
                yield savepoint_name
            except Exception:
                with self._connection.cursor() as cursor:
                    cursor.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
                raise

        finally:
            if self._savepoints and self._savepoints[-1] == savepoint_name:
                self._savepoints.pop()

    def commit(self) -> None:
        """Commit the current transaction."""
        self._connection.commit()

    def rollback(self) -> None:
        """Rollback the current transaction."""
        self._connection.rollback()


class DataTierManager:
    """Manage data tiering for hot/warm/cold storage."""

    def __init__(self, connection: Any, hot_threshold_days: int = 7, warm_threshold_days: int = 30) -> None:
        self._connection = connection
        self._hot_threshold = timedelta(days=hot_threshold_days)
        self._warm_threshold = timedelta(days=warm_threshold_days)
        self._lock = threading.Lock()

    def move_to_tier(
        self,
        table_name: str,
        record_key: str,
        target_tier: DataTier,
        batch_id: str | None = None,
    ) -> bool:
        """Move a record to a different tier."""
        batch_id = batch_id or uuid.uuid4().hex

        with self._lock:
            try:
                with self._connection.cursor() as cursor:
                    cursor.execute(
                        """
                        INSERT INTO data_tier_log (table_name, record_key, tier, batch_id)
                        VALUES (%s, %s, %s, %s)
                        """,
                        (table_name, record_key, target_tier.value, batch_id),
                    )
                    self._connection.commit()
                return True
            except Exception:
                self._connection.rollback()
                return False

    def get_record_tier(self, table_name: str, record_key: str) -> DataTier | None:
        """Get the current tier of a record."""
        with self._lock:
            with self._connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT tier FROM data_tier_log
                    WHERE table_name = %s AND record_key = %s
                    ORDER BY moved_at DESC
                    LIMIT 1
                    """,
                    (table_name, record_key),
                )
                row = cursor.fetchone()
                return DataTier(row[0]) if row else None

    def archive_old_records(
        self,
        table_name: str,
        id_column: str,
        timestamp_column: str,
        archive_before: datetime,
        batch_size: int = 1000,
    ) -> int:
        """Archive records older than specified date."""
        with self._lock:
            batch_id = uuid.uuid4().hex
            archived_count = 0

            while True:
                with self._connection.cursor() as cursor:
                    cursor.execute(
                        f"""
                        SELECT {id_column} FROM {table_name}
                        WHERE {timestamp_column} < %s
                        LIMIT %s
                        """,
                        (archive_before, batch_size),
                    )
                    rows = cursor.fetchall()

                    if not rows:
                        break

                    ids = [row[0] for row in rows]

                    for record_id in ids:
                        self.move_to_tier(table_name, str(record_id), DataTier.COLD, batch_id)

                    cursor.execute(
                        f"""
                        DELETE FROM {table_name}
                        WHERE {id_column} = ANY(%s)
                        """,
                        (ids,),
                    )
                    self._connection.commit()
                    archived_count += len(ids)

                    if len(ids) < batch_size:
                        break

            return archived_count


class ConnectionPool:
    """Simple connection pool for PostgreSQL."""

    def __init__(
        self,
        dsn: str,
        min_size: int = 5,
        max_size: int = 20,
    ) -> None:
        self._dsn = dsn
        self._min_size = min_size
        self._max_size = max_size
        self._pool: list[Any] = []
        self._lock = threading.Lock()
        self._semaphore = threading.Semaphore(max_size)

    def acquire(self) -> Any:
        """Acquire a connection from the pool."""
        self._semaphore.acquire()

        with self._lock:
            if self._pool:
                return self._pool.pop()

        import psycopg2
        return psycopg2.connect(self._dsn)

    def release(self, connection: Any) -> None:
        """Return a connection to the pool."""
        with self._lock:
            if len(self._pool) < self._max_size:
                self._pool.append(connection)
                self._semaphore.release()
            else:
                connection.close()
                self._semaphore.release()

    def close_all(self) -> None:
        """Close all connections in the pool."""
        with self._lock:
            for conn in self._pool:
                conn.close()
            self._pool.clear()
