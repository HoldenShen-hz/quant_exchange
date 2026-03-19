from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from quant_exchange.ingestion.a_share_baostock import BaoStockAShareHistoryDownloader


class BaoStockAShareHistoryDownloaderTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.downloader = BaoStockAShareHistoryDownloader(output_dir=self.temp_dir.name)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_bs_01_a_share_code_filter_is_correct(self) -> None:
        self.assertTrue(self.downloader._is_a_share_stock("sh.600000"))
        self.assertTrue(self.downloader._is_a_share_stock("sz.300750"))
        self.assertFalse(self.downloader._is_a_share_stock("sh.000001"))
        self.assertFalse(self.downloader._is_a_share_stock("sh.510300"))

    def test_bs_02_write_csv_gz_and_count_rows(self) -> None:
        target = Path(self.temp_dir.name) / "sample.csv.gz"
        rows = [
            {
                "date": "2025-01-02",
                "code": "sh.600000",
                "open": "10.0",
                "high": "10.2",
                "low": "9.9",
                "close": "10.1",
                "volume": "1000",
                "amount": "1000000",
                "turn": "0.8",
                "pctChg": "1.1",
            }
        ]
        self.downloader._write_csv_gz(target, rows)
        self.assertEqual(self.downloader._count_rows(target), 1)

    def test_bs_03_candidate_query_days_step_back(self) -> None:
        days = self.downloader._candidate_query_days("2026-03-17")
        self.assertEqual(days[0], "2026-03-17")
        self.assertEqual(days[1], "2026-03-16")
        self.assertEqual(len(days), 15)


if __name__ == "__main__":
    unittest.main()
