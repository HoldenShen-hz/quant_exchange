from __future__ import annotations

import threading
import tempfile
import unittest
from pathlib import Path

from quant_exchange.persistence import SQLitePersistence


class PersistenceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.db = SQLitePersistence()

    def tearDown(self) -> None:
        self.db.close()

    def test_db_01_core_and_enhanced_tables_exist(self) -> None:
        tables = self.db.tables()
        self.assertIn("sys_users", tables)
        self.assertIn("ref_instruments", tables)
        self.assertIn("ref_stock_profiles", tables)
        self.assertIn("ref_stock_financial_history", tables)
        self.assertIn("trade_orders", tables)
        self.assertIn("mkt_stock_minute_bars", tables)
        self.assertIn("feature_definitions", tables)
        self.assertIn("replay_jobs", tables)

    def test_db_02_upsert_and_fetch_core_record(self) -> None:
        self.db.upsert_record(
            "sys_users",
            "username",
            "alice",
            {"username": "alice", "roles": ["admin"]},
            extra_columns={
                "display_name": "Alice",
                "password_hash": "hash",
                "status": "ACTIVE",
            },
        )
        row = self.db.fetch_one("sys_users", where="username = :username", params={"username": "alice"})
        self.assertEqual(row["payload"]["username"], "alice")
        self.assertEqual(row["display_name"], "Alice")

    def test_db_03_upsert_and_fetch_enhanced_record(self) -> None:
        self.db.upsert_record(
            "feature_definitions",
            "feature_code",
            "mom_3",
            {"feature_code": "mom_3", "expression": "momentum:3"},
            extra_columns={"feature_name": "Momentum 3"},
        )
        rows = self.db.fetch_all("feature_definitions")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["payload"]["expression"], "momentum:3")

    def test_db_04_stock_history_tables_store_records(self) -> None:
        self.db.upsert_record(
            "ref_stock_financial_history",
            "snapshot_key",
            "MSFT.US:FY:2025-12-31",
            {"instrument_id": "MSFT.US", "report_date": "2025-12-31", "revenue": 100.0},
            extra_columns={
                "instrument_id": "MSFT.US",
                "report_date": "2025-12-31",
                "period_type": "FY",
                "fiscal_year": 2025,
                "revenue": 100.0,
                "net_income": 20.0,
                "operating_cashflow": 24.0,
                "free_cashflow": 18.0,
            },
        )
        self.db.upsert_record(
            "mkt_stock_minute_bars",
            "bar_key",
            "MSFT.US:2026-03-17T13:30:00+00:00",
            {"instrument_id": "MSFT.US", "bar_time": "2026-03-17T13:30:00+00:00", "close": 410.5, "volume": 1200},
            extra_columns={
                "instrument_id": "MSFT.US",
                "bar_time": "2026-03-17T13:30:00+00:00",
                "market_region": "US",
                "exchange_code": "NASDAQ",
                "close_price": 410.5,
                "volume": 1200,
            },
        )
        financial = self.db.fetch_one(
            "ref_stock_financial_history",
            where="instrument_id = :instrument_id",
            params={"instrument_id": "MSFT.US"},
        )
        minute = self.db.fetch_one(
            "mkt_stock_minute_bars",
            where="instrument_id = :instrument_id",
            params={"instrument_id": "MSFT.US"},
        )
        self.assertEqual(financial["payload"]["report_date"], "2025-12-31")
        self.assertEqual(minute["payload"]["close"], 410.5)

    def test_db_05_connection_can_be_used_from_background_thread(self) -> None:
        self.db.upsert_record(
            "sys_users",
            "username",
            "thread-user",
            {"username": "thread-user"},
            extra_columns={"display_name": "Thread User", "password_hash": "hash", "status": "ACTIVE"},
        )
        result: dict[str, object] = {}

        def worker() -> None:
            row = self.db.fetch_one("sys_users", where="username = :username", params={"username": "thread-user"})
            result["username"] = row["payload"]["username"] if row is not None else None

        thread = threading.Thread(target=worker)
        thread.start()
        thread.join(timeout=2.0)
        self.assertEqual(result.get("username"), "thread-user")

    def test_db_06_columns_for_accepts_plain_tuple_rows(self) -> None:
        original_row_factory = self.db.connection.row_factory
        self.db.connection.row_factory = None
        try:
            columns = self.db._columns_for("sys_users")
        finally:
            self.db.connection.row_factory = original_row_factory
        self.assertIn("username", columns)
        self.assertIn("payload", columns)

    def test_db_07_concurrent_reads_and_writes_share_one_connection_safely(self) -> None:
        errors: list[Exception] = []

        def writer() -> None:
            try:
                for index in range(40):
                    username = f"user-{index}"
                    self.db.upsert_record(
                        "sys_users",
                        "username",
                        username,
                        {"username": username},
                        extra_columns={"display_name": username, "password_hash": "hash", "status": "ACTIVE"},
                    )
            except Exception as exc:  # pragma: no cover - regression capture
                errors.append(exc)

        def reader() -> None:
            try:
                for _ in range(40):
                    self.db.raw_fetchall("SELECT * FROM sys_users")
            except Exception as exc:  # pragma: no cover - regression capture
                errors.append(exc)

        writer_thread = threading.Thread(target=writer)
        reader_thread = threading.Thread(target=reader)
        writer_thread.start()
        reader_thread.start()
        writer_thread.join(timeout=2.0)
        reader_thread.join(timeout=2.0)
        self.assertFalse(errors)

    def test_db_08_file_database_enables_busy_timeout_and_wal(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            database_path = str(Path(temp_dir) / "runtime.sqlite3")
            db = SQLitePersistence(database_path)
            try:
                journal_mode = db.raw_fetchall("PRAGMA journal_mode")[0][0]
                busy_timeout = db.raw_fetchall("PRAGMA busy_timeout")[0][0]
            finally:
                db.close()
        self.assertEqual(str(journal_mode).lower(), "wal")
        self.assertEqual(int(busy_timeout), 30000)


if __name__ == "__main__":
    unittest.main()
