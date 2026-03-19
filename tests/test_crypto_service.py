from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from quant_exchange.config import AppSettings
from quant_exchange.platform import QuantTradingPlatform


class CryptoWorkbenchTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database_path = str(Path(self.temp_dir.name) / "crypto.sqlite3")
        self.platform = QuantTradingPlatform(AppSettings.from_mapping({"database": {"url": self.database_path}}))

    def tearDown(self) -> None:
        self.platform.close()
        self.temp_dir.cleanup()

    def test_cr_01_crypto_universe_summary_and_assets_are_available(self) -> None:
        summary = self.platform.api.crypto_universe_summary(featured_limit=3)
        self.assertEqual(summary["code"], "OK")
        self.assertEqual(summary["data"]["source"], "simulated_crypto_exchange")
        self.assertGreaterEqual(summary["data"]["total_count"], 5)
        self.assertEqual(len(summary["data"]["featured_assets"]), 3)

        assets = self.platform.api.list_crypto_assets()
        self.assertEqual(assets["code"], "OK")
        self.assertGreaterEqual(len(assets["data"]), 5)
        instrument_ids = {item["instrument_id"] for item in assets["data"]}
        self.assertIn("BTCUSDT", instrument_ids)
        self.assertTrue(all(item["market_status"] == "OPEN" for item in assets["data"]))

    def test_cr_02_crypto_detail_and_history_support_symbol_normalization(self) -> None:
        detail = self.platform.api.get_crypto_detail("BTC/USDT")
        self.assertEqual(detail["code"], "OK")
        self.assertEqual(detail["data"]["instrument_id"], "BTCUSDT")
        self.assertEqual(detail["data"]["asset_name"], "Bitcoin")
        self.assertTrue(detail["data"]["microstructure"]["trades_24x7"])

        history = self.platform.api.get_crypto_history("BTCUSDT", limit=45)
        self.assertEqual(history["code"], "OK")
        self.assertEqual(history["data"]["instrument_id"], "BTCUSDT")
        self.assertEqual(history["data"]["source"], "simulated_crypto_exchange")
        self.assertEqual(len(history["data"]["bars"]), 45)
        self.assertGreater(history["data"]["summary"]["latest_close"], 0)


if __name__ == "__main__":
    unittest.main()
