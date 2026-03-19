"""SQLite-backed persistence layer for core and enhanced platform entities."""

from __future__ import annotations

import json
import sqlite3
import threading
from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any


def _json_default(value: Any) -> Any:
    """Serialize dataclasses and datetimes for JSON persistence."""

    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Enum):
        return value.value
    if is_dataclass(value):
        return asdict(value)
    raise TypeError(f"Object of type {type(value)!r} is not JSON serializable.")


class SQLitePersistence:
    """Persist platform state into a local SQLite database."""

    def __init__(self, path: str = ":memory:") -> None:
        self.path = path
        self._lock = threading.RLock()
        if path != ":memory:":
            Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(path, check_same_thread=False, timeout=30.0)
        self.connection.row_factory = sqlite3.Row
        self._configure_connection()
        self.initialize_schema()

    def close(self) -> None:
        """Close the underlying SQLite connection."""

        with self._lock:
            self.connection.close()

    def initialize_schema(self) -> None:
        """Create the documented core and enhanced tables used by the MVP runtime."""

        ddl = [
            """
            CREATE TABLE IF NOT EXISTS sys_users (
                username TEXT PRIMARY KEY,
                display_name TEXT,
                password_hash TEXT NOT NULL,
                email TEXT,
                status TEXT NOT NULL,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS sys_roles (
                role_code TEXT PRIMARY KEY,
                role_name TEXT NOT NULL,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS sys_user_roles (
                username TEXT NOT NULL,
                role_code TEXT NOT NULL,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL,
                PRIMARY KEY (username, role_code)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS sys_audit_logs (
                audit_id TEXT PRIMARY KEY,
                actor TEXT NOT NULL,
                action TEXT NOT NULL,
                resource TEXT NOT NULL,
                success INTEGER NOT NULL,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS ref_exchanges (
                exchange_code TEXT PRIMARY KEY,
                exchange_name TEXT NOT NULL,
                market_type TEXT NOT NULL,
                status TEXT NOT NULL,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS ref_instruments (
                instrument_id TEXT PRIMARY KEY,
                symbol TEXT NOT NULL,
                market_type TEXT NOT NULL,
                market_region TEXT NOT NULL,
                instrument_type TEXT NOT NULL,
                status TEXT NOT NULL,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS ref_stock_profiles (
                instrument_id TEXT PRIMARY KEY,
                symbol TEXT NOT NULL,
                company_name TEXT NOT NULL,
                market_region TEXT NOT NULL,
                exchange_code TEXT NOT NULL,
                board TEXT NOT NULL,
                sector TEXT NOT NULL,
                industry TEXT NOT NULL,
                pe_ttm REAL,
                roe REAL,
                market_cap REAL,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS ref_stock_financial_history (
                snapshot_key TEXT PRIMARY KEY,
                instrument_id TEXT NOT NULL,
                report_date TEXT NOT NULL,
                period_type TEXT NOT NULL,
                fiscal_year INTEGER NOT NULL,
                revenue REAL,
                net_income REAL,
                operating_cashflow REAL,
                free_cashflow REAL,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_ref_stock_financial_history_instrument_report_date
            ON ref_stock_financial_history (instrument_id, report_date DESC)
            """,
            """
            CREATE TABLE IF NOT EXISTS acct_trading_accounts (
                account_code TEXT PRIMARY KEY,
                account_name TEXT NOT NULL,
                market_type TEXT NOT NULL,
                environment TEXT NOT NULL,
                status TEXT NOT NULL,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS acct_account_snapshots (
                account_code TEXT NOT NULL,
                snapshot_time TEXT NOT NULL,
                equity REAL NOT NULL,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL,
                PRIMARY KEY (account_code, snapshot_time)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS strat_strategies (
                strategy_code TEXT PRIMARY KEY,
                strategy_name TEXT NOT NULL,
                category TEXT,
                status TEXT NOT NULL,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS strat_strategy_runs (
                run_id TEXT PRIMARY KEY,
                strategy_code TEXT NOT NULL,
                mode TEXT NOT NULL,
                status TEXT NOT NULL,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS mkt_market_klines (
                instrument_id TEXT NOT NULL,
                interval TEXT NOT NULL,
                open_time TEXT NOT NULL,
                close_time TEXT NOT NULL,
                close_price REAL NOT NULL,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL,
                PRIMARY KEY (instrument_id, interval, open_time)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS mkt_stock_minute_bars (
                bar_key TEXT PRIMARY KEY,
                instrument_id TEXT NOT NULL,
                bar_time TEXT NOT NULL,
                market_region TEXT NOT NULL,
                exchange_code TEXT NOT NULL,
                close_price REAL NOT NULL,
                volume REAL NOT NULL,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_mkt_stock_minute_bars_instrument_time
            ON mkt_stock_minute_bars (instrument_id, bar_time DESC)
            """,
            """
            CREATE TABLE IF NOT EXISTS trade_orders (
                order_id TEXT PRIMARY KEY,
                client_order_id TEXT UNIQUE,
                instrument_id TEXT NOT NULL,
                status TEXT NOT NULL,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS trade_executions (
                execution_id TEXT PRIMARY KEY,
                order_id TEXT NOT NULL,
                instrument_id TEXT NOT NULL,
                execution_time TEXT NOT NULL,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS trade_positions (
                position_key TEXT PRIMARY KEY,
                instrument_id TEXT NOT NULL,
                quantity REAL NOT NULL,
                payload TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS risk_risk_rules (
                rule_code TEXT PRIMARY KEY,
                rule_type TEXT NOT NULL,
                action_type TEXT NOT NULL,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS risk_rule_bindings (
                binding_key TEXT PRIMARY KEY,
                rule_code TEXT NOT NULL,
                scope_type TEXT NOT NULL,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS risk_risk_events (
                event_id TEXT PRIMARY KEY,
                severity TEXT NOT NULL,
                reason_code TEXT NOT NULL,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS risk_kill_switch_events (
                event_id TEXT PRIMARY KEY,
                target_type TEXT NOT NULL,
                status TEXT NOT NULL,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS rpt_daily_account_metrics (
                metric_key TEXT PRIMARY KEY,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS rpt_daily_strategy_metrics (
                metric_key TEXT PRIMARY KEY,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS ops_scheduled_jobs (
                job_code TEXT PRIMARY KEY,
                job_name TEXT NOT NULL,
                job_type TEXT NOT NULL,
                status TEXT NOT NULL,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS ops_job_runs (
                run_no TEXT PRIMARY KEY,
                job_code TEXT NOT NULL,
                status TEXT NOT NULL,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS ops_alerts (
                alert_code TEXT PRIMARY KEY,
                severity TEXT NOT NULL,
                status TEXT NOT NULL,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS ops_strategy_bots (
                bot_id TEXT PRIMARY KEY,
                bot_name TEXT NOT NULL,
                template_code TEXT NOT NULL,
                instrument_id TEXT NOT NULL,
                status TEXT NOT NULL,
                mode TEXT NOT NULL,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS ops_bot_commands (
                command_id TEXT PRIMARY KEY,
                bot_id TEXT NOT NULL,
                command TEXT NOT NULL,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS ops_notifications (
                notification_id TEXT PRIMARY KEY,
                bot_id TEXT NOT NULL,
                level TEXT NOT NULL,
                event_type TEXT NOT NULL,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS web_saved_workspaces (
                workspace_key TEXT PRIMARY KEY,
                client_id TEXT NOT NULL,
                workspace_code TEXT NOT NULL,
                last_active_instrument_id TEXT,
                compare_left TEXT,
                compare_right TEXT,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS web_activity_logs (
                event_id TEXT PRIMARY KEY,
                client_id TEXT NOT NULL,
                workspace_code TEXT NOT NULL,
                event_type TEXT NOT NULL,
                path TEXT,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS web_learning_progress (
                progress_key TEXT PRIMARY KEY,
                principal_type TEXT NOT NULL,
                principal_id TEXT NOT NULL,
                username TEXT,
                current_lesson_id TEXT,
                best_score REAL,
                last_score REAL,
                quiz_attempts INTEGER NOT NULL,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS web_learning_attempts (
                attempt_id TEXT PRIMARY KEY,
                principal_type TEXT NOT NULL,
                principal_id TEXT NOT NULL,
                username TEXT,
                score REAL NOT NULL,
                passed INTEGER NOT NULL,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS intel_sources (
                source_code TEXT PRIMARY KEY,
                source_name TEXT NOT NULL,
                source_type TEXT NOT NULL,
                status TEXT NOT NULL,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS intel_documents (
                document_uid TEXT PRIMARY KEY,
                source_code TEXT NOT NULL,
                instrument_id TEXT NOT NULL,
                published_at TEXT NOT NULL,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS intel_sentiment_scores (
                sentiment_key TEXT PRIMARY KEY,
                document_uid TEXT NOT NULL,
                instrument_id TEXT NOT NULL,
                sentiment_label TEXT NOT NULL,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS intel_directional_signals (
                signal_id TEXT PRIMARY KEY,
                instrument_id TEXT NOT NULL,
                horizon TEXT NOT NULL,
                direction_label TEXT NOT NULL,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS universe_universes (
                universe_code TEXT PRIMARY KEY,
                universe_name TEXT NOT NULL,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS universe_rules (
                rule_key TEXT PRIMARY KEY,
                universe_code TEXT NOT NULL,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS universe_snapshots (
                snapshot_key TEXT PRIMARY KEY,
                universe_code TEXT NOT NULL,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS feature_definitions (
                feature_code TEXT PRIMARY KEY,
                feature_name TEXT NOT NULL,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS feature_versions (
                version_key TEXT PRIMARY KEY,
                feature_code TEXT NOT NULL,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS feature_values (
                value_key TEXT PRIMARY KEY,
                feature_code TEXT NOT NULL,
                instrument_id TEXT NOT NULL,
                event_time TEXT NOT NULL,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS research_projects (
                project_code TEXT PRIMARY KEY,
                project_name TEXT NOT NULL,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS research_notebooks (
                notebook_key TEXT PRIMARY KEY,
                project_code TEXT NOT NULL,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS research_datasets (
                dataset_code TEXT PRIMARY KEY,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS ml_experiments (
                experiment_code TEXT PRIMARY KEY,
                experiment_name TEXT NOT NULL,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS ml_experiment_runs (
                run_code TEXT PRIMARY KEY,
                experiment_code TEXT NOT NULL,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS ml_models (
                model_code TEXT PRIMARY KEY,
                model_name TEXT NOT NULL,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS ml_model_versions (
                version_code TEXT PRIMARY KEY,
                model_code TEXT NOT NULL,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS ml_model_deployments (
                deployment_code TEXT PRIMARY KEY,
                model_code TEXT NOT NULL,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS ml_model_drift_metrics (
                metric_key TEXT PRIMARY KEY,
                model_code TEXT NOT NULL,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS audit_jobs (
                audit_job_code TEXT PRIMARY KEY,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS audit_results (
                result_key TEXT PRIMARY KEY,
                audit_job_code TEXT NOT NULL,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS ems_execution_algorithms (
                algorithm_code TEXT PRIMARY KEY,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS ems_order_baskets (
                basket_code TEXT PRIMARY KEY,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS ems_router_decisions (
                decision_code TEXT PRIMARY KEY,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS ems_router_policies (
                policy_code TEXT PRIMARY KEY,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS ledger_virtual_accounts (
                account_code TEXT PRIMARY KEY,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS ledger_entries (
                entry_code TEXT PRIMARY KEY,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS ledger_transfers (
                transfer_code TEXT PRIMARY KEY,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS alt_data_sources (
                source_code TEXT PRIMARY KEY,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS alt_datasets (
                dataset_code TEXT PRIMARY KEY,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS alt_dataset_records (
                record_key TEXT PRIMARY KEY,
                dataset_code TEXT NOT NULL,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS replay_event_logs (
                event_code TEXT PRIMARY KEY,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS replay_state_snapshots (
                snapshot_code TEXT PRIMARY KEY,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS replay_jobs (
                replay_job_code TEXT PRIMARY KEY,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS replay_shadow_deployments (
                shadow_code TEXT PRIMARY KEY,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS opt_option_chains (
                chain_code TEXT PRIMARY KEY,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS mm_market_making_configs (
                config_code TEXT PRIMARY KEY,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS dex_liquidity_positions (
                position_code TEXT PRIMARY KEY,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """,
        ]
        with self._lock:
            cursor = self.connection.cursor()
            for statement in ddl:
                cursor.execute(statement)
            self.connection.commit()
        self._ensure_column("trade_orders", "account_code", "TEXT")
        self._ensure_column("trade_orders", "environment", "TEXT")
        self._ensure_column("trade_orders", "strategy_id", "TEXT")
        self._ensure_column("trade_executions", "account_code", "TEXT")
        self._ensure_column("trade_positions", "account_code", "TEXT")
        self._ensure_column("web_saved_workspaces", "principal_type", "TEXT")
        self._ensure_column("web_saved_workspaces", "principal_id", "TEXT")
        self._ensure_column("web_saved_workspaces", "username", "TEXT")
        self._ensure_column("web_activity_logs", "principal_type", "TEXT")
        self._ensure_column("web_activity_logs", "principal_id", "TEXT")
        self._ensure_column("web_activity_logs", "username", "TEXT")

    def _configure_connection(self) -> None:
        """Tune SQLite for concurrent background refresh and UI read workloads."""

        with self._lock:
            self.connection.execute("PRAGMA foreign_keys = ON")
            self.connection.execute("PRAGMA busy_timeout = 30000")
            if self.path != ":memory:":
                self.connection.execute("PRAGMA journal_mode = WAL")
                self.connection.execute("PRAGMA synchronous = NORMAL")

    def _serialize(self, payload: Any) -> str:
        """Serialize an arbitrary payload into JSON text."""

        return json.dumps(payload, default=_json_default, sort_keys=True)

    def _deserialize(self, payload: str) -> Any:
        """Load persisted JSON text back into a Python object."""

        return json.loads(payload)

    def upsert_record(
        self,
        table: str,
        key_column: str,
        key_value: str,
        payload: dict[str, Any],
        *,
        extra_columns: dict[str, Any] | None = None,
        commit: bool = True,
    ) -> None:
        """Insert or replace a JSON-backed record in the selected table."""

        timestamp = datetime.now(timezone.utc).isoformat()
        values = {key_column: key_value, "payload": self._serialize(payload), **(extra_columns or {})}
        columns = list(values)
        available_columns = self._columns_for(table)
        if "created_at" in available_columns and "created_at" not in columns:
            values["created_at"] = timestamp
            columns.append("created_at")
        if "updated_at" in available_columns and "updated_at" not in columns:
            values["updated_at"] = timestamp
            columns.append("updated_at")
        placeholders = ", ".join(f":{column}" for column in columns)
        sql = f"INSERT OR REPLACE INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"
        with self._lock:
            try:
                self.connection.execute(sql, values)
                if commit:
                    self.connection.commit()
            except Exception:
                self.connection.rollback()
                raise

    def insert_record(
        self,
        table: str,
        payload: dict[str, Any],
        *,
        extra_columns: dict[str, Any] | None = None,
        commit: bool = True,
    ) -> None:
        """Insert a JSON-backed record without a dedicated application key."""

        timestamp = datetime.now(timezone.utc).isoformat()
        values = {"payload": self._serialize(payload), **(extra_columns or {})}
        columns = list(values)
        if "created_at" in self._columns_for(table) and "created_at" not in columns:
            values["created_at"] = timestamp
            columns.append("created_at")
        sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join(f':{c}' for c in columns)})"
        with self._lock:
            try:
                self.connection.execute(sql, values)
                if commit:
                    self.connection.commit()
            except Exception:
                self.connection.rollback()
                raise

    def fetch_all(
        self,
        table: str,
        *,
        where: str | None = None,
        params: dict[str, Any] | None = None,
        order_by: str | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        """Return rows from a table as dictionaries, decoding JSON payloads when present."""

        sql = f"SELECT * FROM {table}"
        if where:
            sql += f" WHERE {where}"
        if order_by:
            sql += f" ORDER BY {order_by}"
        if limit is not None:
            sql += f" LIMIT {int(limit)}"
        with self._lock:
            rows = self.connection.execute(sql, params or {}).fetchall()
        return [self._row_to_dict(row) for row in rows]

    def fetch_one(self, table: str, *, where: str, params: dict[str, Any]) -> dict[str, Any] | None:
        """Return a single row from a table."""

        sql = f"SELECT * FROM {table} WHERE {where}"
        with self._lock:
            row = self.connection.execute(sql, params).fetchone()
        return None if row is None else self._row_to_dict(row)

    def count(self, table: str) -> int:
        """Return the number of rows stored in a table."""

        with self._lock:
            row = self.connection.execute(f"SELECT COUNT(*) AS cnt FROM {table}").fetchone()
        return int(row["cnt"])

    def delete_where(self, table: str, *, where: str, params: dict[str, Any] | None = None, commit: bool = True) -> int:
        """Delete rows from a table and return the number of affected records."""

        with self._lock:
            try:
                cursor = self.connection.execute(f"DELETE FROM {table} WHERE {where}", params or {})
                if commit:
                    self.connection.commit()
            except Exception:
                self.connection.rollback()
                raise
        return int(cursor.rowcount)

    def tables(self) -> set[str]:
        """Return the set of available table names."""

        with self._lock:
            rows = self.connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'"
            ).fetchall()
        return {row["name"] for row in rows}

    def _row_to_dict(self, row: sqlite3.Row) -> dict[str, Any]:
        """Decode a SQLite row into a plain dictionary."""

        result = dict(row)
        if "payload" in result:
            result["payload"] = self._deserialize(result["payload"])
        return result

    def _columns_for(self, table: str) -> set[str]:
        """Return the set of columns defined for a table."""

        with self._lock:
            rows = self.connection.execute(f"PRAGMA table_info({table})").fetchall()
        columns: set[str] = set()
        for row in rows:
            if isinstance(row, sqlite3.Row):
                columns.add(row["name"])
                continue
            if isinstance(row, dict):
                columns.add(str(row["name"]))
                continue
            columns.add(str(row[1]))
        return columns

    def _ensure_column(self, table: str, column: str, column_type: str) -> None:
        """Add a column when an existing SQLite table is missing it."""

        if column in self._columns_for(table):
            return
        with self._lock:
            try:
                self.connection.execute(f"ALTER TABLE {table} ADD COLUMN {column} {column_type}")
                self.connection.commit()
            except Exception:
                self.connection.rollback()
                raise

    def raw_fetchall(self, sql: str, params: Any | None = None) -> list[sqlite3.Row | tuple]:
        """Execute one SQL statement and fetch all rows under the connection lock."""

        with self._lock:
            return self.connection.execute(sql, params or ()).fetchall()

    def commit(self) -> None:
        """Commit the current transaction under the connection lock."""

        with self._lock:
            self.connection.commit()
