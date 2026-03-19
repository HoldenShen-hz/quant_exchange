from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from quant_exchange.config import AppSettings
from quant_exchange.platform import QuantTradingPlatform


class PaperTradingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        database_path = str(Path(self.temp_dir.name) / "paper.sqlite3")
        self.platform = QuantTradingPlatform(AppSettings.from_mapping({"database": {"url": database_path}}))
        self._seed_open_minute_bar()

    def tearDown(self) -> None:
        self.platform.close()
        self.temp_dir.cleanup()

    def _seed_open_minute_bar(self) -> None:
        self.platform.stocks.save_minute_bar(
            "MSFT.US",
            {
                "instrument_id": "MSFT.US",
                "bar_time": "2026-03-17T14:01:00+00:00",
                "open": 418.2,
                "high": 420.1,
                "low": 417.8,
                "close": 419.5,
                "volume": 5_000,
                "turnover": 2_097_500.0,
                "market_region": "US",
                "exchange_code": "NASDAQ",
                "source": "test_seed",
            },
        )

    def test_pp_01_market_order_updates_paper_position(self) -> None:
        payload = self.platform.api.submit_paper_order(
            instrument_id="MSFT.US",
            side="buy",
            quantity=10,
            order_type="market",
        )
        self.assertEqual(payload["code"], "OK")
        dashboard = payload["data"]
        self.assertEqual(dashboard["last_action"]["type"], "submitted")
        self.assertTrue(any(item["instrument_id"] == "MSFT.US" for item in dashboard["positions"]))
        position = next(item for item in dashboard["positions"] if item["instrument_id"] == "MSFT.US")
        self.assertGreater(position["quantity"], 0)
        self.assertGreaterEqual(len(dashboard["fills"]), 1)

    def test_pp_02_large_order_can_be_partially_filled(self) -> None:
        latest_bar = self.platform.stocks.get_minute_bars("MSFT.US", limit=1)["bars"][-1]
        constrained_bar = dict(latest_bar)
        constrained_bar["volume"] = 20
        self.platform.stocks.save_minute_bar("MSFT.US", constrained_bar)

        payload = self.platform.api.submit_paper_order(
            instrument_id="MSFT.US",
            side="buy",
            quantity=100,
            order_type="market",
        )
        self.assertEqual(payload["code"], "OK")
        dashboard = payload["data"]
        order = dashboard["orders"][0]
        self.assertEqual(order["status"], "partially_filled")
        self.assertLess(order["filled_quantity"], order["quantity"])

    def test_pp_03_limit_order_can_be_cancelled(self) -> None:
        payload = self.platform.api.submit_paper_order(
            instrument_id="MSFT.US",
            side="buy",
            quantity=10,
            order_type="limit",
            limit_price=1.0,
        )
        self.assertEqual(payload["code"], "OK")
        dashboard = payload["data"]
        order = dashboard["orders"][0]
        self.assertEqual(order["status"], "accepted")

        cancelled = self.platform.api.cancel_paper_order(order["order_id"])
        self.assertEqual(cancelled["code"], "OK")
        latest_order = cancelled["data"]["orders"][0]
        self.assertEqual(latest_order["status"], "cancelled")

    def test_pp_04_risk_rejection_is_reported(self) -> None:
        payload = self.platform.api.submit_paper_order(
            instrument_id="MSFT.US",
            side="buy",
            quantity=10_000,
            order_type="market",
        )
        self.assertEqual(payload["code"], "OK")
        self.assertEqual(payload["data"]["last_action"]["type"], "risk_reject")
        self.assertIn("order_notional_limit", payload["data"]["last_action"]["reasons"])

    def test_pp_05_dashboard_includes_strategy_difference_summary(self) -> None:
        self.platform.api.submit_paper_order(
            instrument_id="MSFT.US",
            side="buy",
            quantity=10,
            order_type="market",
        )
        dashboard = self.platform.api.get_paper_trading_dashboard(instrument_id="MSFT.US")
        self.assertEqual(dashboard["code"], "OK")
        diff = dashboard["data"]["strategy_diff"]
        self.assertIsNotNone(diff)
        self.assertEqual(diff["instrument_id"], "MSFT.US")
        self.assertIn("signal_target_weight", diff)
        self.assertIn("summary", diff)
        self.assertIsNotNone(diff["backtest_metrics"])


if __name__ == "__main__":
    unittest.main()
