from __future__ import annotations

import gzip
import tempfile
import unittest
from pathlib import Path

from quant_exchange.ingestion import EastmoneyAShareHistoryDownloader


class AShareHistoryDownloaderTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.downloader = EastmoneyAShareHistoryDownloader(output_dir=self.temp_dir.name, max_workers=1)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_as_01_parse_stock_list_payload(self) -> None:
        payload = {
            "data": {
                "diff": [
                    {"f12": "600000", "f13": 1, "f14": "浦发银行"},
                    {"f12": "000001", "f13": 0, "f14": "平安银行"},
                ]
            }
        }
        refs = self.downloader._parse_stock_list_payload(payload)
        self.assertEqual([ref.symbol for ref in refs], ["600000", "000001"])
        self.assertEqual(refs[0].exchange_code, "SSE")
        self.assertEqual(refs[1].secid, "0.000001")

    def test_as_02_parse_kline_payload_and_write_csv(self) -> None:
        payload = {
            "data": {
                "klines": [
                    "2025-01-02,10.00,10.20,10.30,9.90,1000,1000000,4.00,2.00,0.20,1.20",
                    "2025-01-03,10.20,10.10,10.25,10.00,900,910000,2.45,-0.98,-0.10,1.10",
                ]
            }
        }
        rows = self.downloader._parse_kline_payload(payload)
        self.assertEqual(len(rows), 2)
        target = Path(self.temp_dir.name) / "sample.csv.gz"
        self.downloader._write_csv_gz(target, rows)
        with gzip.open(target, "rt", encoding="utf-8") as handle:
            content = handle.read()
        self.assertIn("trade_date,open,close,high,low,volume,amount", content)
        self.assertIn("2025-01-02,10.00,10.20", content)


if __name__ == "__main__":
    unittest.main()
