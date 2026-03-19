from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from quant_exchange.config import AppSettings
from quant_exchange.platform import QuantTradingPlatform
from quant_exchange.stocks import RealtimeMarketService


class RealtimeMarketTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database_path = str(Path(self.temp_dir.name) / "market.sqlite3")
        self.platform = QuantTradingPlatform(AppSettings.from_mapping({"database": {"url": self.database_path}}))

    def tearDown(self) -> None:
        self.platform.close()
        self.temp_dir.cleanup()

    def test_rt_01_snapshot_contains_market_summary_and_live_quotes(self) -> None:
        as_of = datetime(2026, 3, 17, 2, 0, tzinfo=timezone.utc)
        service = RealtimeMarketService(self.platform.stocks, update_interval_seconds=60.0, clock=lambda: as_of)
        snapshot = service.refresh_once(as_of)

        self.assertEqual(snapshot["source"], "background_market_stream")
        self.assertEqual(snapshot["universe_count"], len(self.platform.stocks.profiles))
        self.assertTrue(snapshot["live_window"])
        self.assertEqual(snapshot["recommended_poll_ms"], 4000)
        self.assertGreater(snapshot["open_quote_count"], 0)
        quote_map = {quote["instrument_id"]: quote for quote in snapshot["quotes"]}
        self.assertIn("600519.SH", quote_map)
        self.assertEqual(quote_map["600519.SH"]["market_status"], "OPEN")
        self.assertGreater(snapshot["summary"]["open_count"], 0)
        self.assertIn("top_gainers", snapshot["summary"])
        self.assertIn("most_active", snapshot["summary"])

    def test_rt_02_open_market_refresh_advances_volume_and_turnover(self) -> None:
        start_at = datetime(2026, 3, 17, 2, 0, tzinfo=timezone.utc)
        service = RealtimeMarketService(self.platform.stocks, update_interval_seconds=60.0, clock=lambda: start_at)
        baseline = {
            quote["instrument_id"]: quote
            for quote in service.refresh_once(start_at)["quotes"]
        }
        updated = {
            quote["instrument_id"]: quote
            for quote in service.refresh_once(start_at + timedelta(minutes=1))["quotes"]
        }

        self.assertNotEqual(
            service.snapshot(["600519.SH"])["as_of"],
            baseline["600519.SH"]["quote_time"],
        )
        self.assertEqual(updated["600519.SH"]["market_status"], "OPEN")
        self.assertGreater(updated["600519.SH"]["volume"], baseline["600519.SH"]["volume"])
        self.assertGreater(updated["600519.SH"]["turnover"], baseline["600519.SH"]["turnover"])
        minute_rows = self.platform.persistence.fetch_all(
            "mkt_stock_minute_bars",
            where="instrument_id = :instrument_id",
            params={"instrument_id": "600519.SH"},
        )
        self.assertGreaterEqual(len(minute_rows), 1)

    def test_rt_03_closed_market_snapshot_uses_idle_polling(self) -> None:
        closed_at = datetime(2026, 3, 17, 1, 0, tzinfo=timezone.utc)
        service = RealtimeMarketService(self.platform.stocks, update_interval_seconds=60.0, clock=lambda: closed_at)
        baseline = {
            quote["instrument_id"]: quote
            for quote in service.refresh_once(closed_at)["quotes"]
        }
        snapshot = service.refresh_once(closed_at + timedelta(minutes=1))
        updated = {quote["instrument_id"]: quote for quote in snapshot["quotes"]}

        self.assertFalse(snapshot["live_window"])
        self.assertEqual(snapshot["recommended_poll_ms"], 30000)
        self.assertEqual(snapshot["open_quote_count"], 0)
        self.assertEqual(snapshot["extended_quote_count"], 0)
        self.assertEqual(snapshot["summary"]["open_count"], 0)
        self.assertEqual(updated["600519.SH"]["volume"], baseline["600519.SH"]["volume"])
        self.assertEqual(updated["MSFT.US"]["market_status"], "CLOSED")


if __name__ == "__main__":
    unittest.main()
