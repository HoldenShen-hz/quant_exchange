from __future__ import annotations

import csv
import gzip
import tempfile
import unittest
from pathlib import Path

from quant_exchange.core.models import Instrument, MarketType
from quant_exchange.stocks import StockDirectoryService, StockProfile


class StockHistoryServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.history_root = Path(self.temp_dir.name)
        self.service = StockDirectoryService(history_root=self.history_root)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_sh_01_local_a_share_history_file_is_used_when_available(self) -> None:
        instrument = Instrument(
            instrument_id="600519.SH",
            symbol="600519.SH",
            market=MarketType.STOCK,
            instrument_type="equity",
            market_region="CN",
            lot_size=100,
        )
        profile = StockProfile(
            instrument_id="600519.SH",
            symbol="600519.SH",
            company_name="贵州茅台",
            market_region="CN",
            exchange_code="SSE",
            board="Main Board",
            sector="Consumer Staples",
            industry="Baijiu",
            last_price=1920.0,
        )
        self.service.upsert_stock(instrument, profile)
        target = self.history_root / "cn_equities" / "a_share" / "daily_raw" / "sse" / "600519.csv.gz"
        target.parent.mkdir(parents=True, exist_ok=True)
        with gzip.open(target, "wt", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=["date", "code", "open", "high", "low", "close", "volume", "amount", "turn", "pctChg"])
            writer.writeheader()
            writer.writerow(
                {
                    "date": "2025-01-02",
                    "code": "sh.600519",
                    "open": "1800.00",
                    "high": "1825.00",
                    "low": "1790.00",
                    "close": "1812.00",
                    "volume": "1200",
                    "amount": "2174400",
                    "turn": "0.2",
                    "pctChg": "1.1",
                }
            )

        payload = self.service.get_stock_history("600519.SH", limit=20)
        self.assertEqual(payload["source"], "local_a_share_raw")
        self.assertEqual(len(payload["bars"]), 1)
        self.assertEqual(payload["bars"][0]["trade_date"], "2025-01-02")
        self.assertEqual(payload["bars"][0]["close"], 1812.0)

    def test_sh_02_generated_history_exists_for_non_cn_stocks(self) -> None:
        instrument = Instrument(
            instrument_id="MSFT.US",
            symbol="MSFT.US",
            market=MarketType.STOCK,
            instrument_type="equity",
            market_region="US",
        )
        profile = StockProfile(
            instrument_id="MSFT.US",
            symbol="MSFT.US",
            company_name="Microsoft",
            market_region="US",
            exchange_code="NASDAQ",
            board="Large Cap",
            sector="Technology",
            industry="Software",
            last_price=420.0,
        )
        self.service.upsert_stock(instrument, profile)

        payload = self.service.get_stock_history("MSFT.US", limit=30)
        self.assertEqual(payload["source"], "generated_demo")
        self.assertEqual(len(payload["bars"]), 30)
        self.assertGreater(payload["summary"]["latest_close"], 0)

    def test_sh_03_local_history_tolerates_blank_numeric_fields(self) -> None:
        instrument = Instrument(
            instrument_id="600000.SH",
            symbol="600000.SH",
            market=MarketType.STOCK,
            instrument_type="equity",
            market_region="CN",
            lot_size=100,
        )
        profile = StockProfile(
            instrument_id="600000.SH",
            symbol="600000.SH",
            company_name="浦发银行",
            market_region="CN",
            exchange_code="SSE",
            board="Main Board",
            sector="Financials",
            industry="Banking",
            last_price=12.0,
        )
        self.service.upsert_stock(instrument, profile)
        target = self.history_root / "cn_equities" / "a_share" / "daily_raw" / "sse" / "600000.csv.gz"
        target.parent.mkdir(parents=True, exist_ok=True)
        with gzip.open(target, "wt", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=["date", "code", "open", "high", "low", "close", "volume", "amount", "turn", "pctChg"])
            writer.writeheader()
            writer.writerow(
                {
                    "date": "2025-01-03",
                    "code": "sh.600000",
                    "open": "",
                    "high": "",
                    "low": "",
                    "close": "12.34",
                    "volume": "",
                    "amount": "",
                    "turn": "",
                    "pctChg": "",
                }
            )

        payload = self.service.get_stock_history("600000.SH", limit=10)
        self.assertEqual(payload["source"], "local_a_share_raw")
        self.assertEqual(payload["bars"][0]["close"], 12.34)
        self.assertEqual(payload["bars"][0]["volume"], 0.0)


if __name__ == "__main__":
    unittest.main()
