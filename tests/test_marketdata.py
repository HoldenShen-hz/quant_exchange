from __future__ import annotations

import unittest
from datetime import timedelta

from quant_exchange.core.models import Kline, Tick
from quant_exchange.marketdata import DataQualityStatus, MarketDataStore

from .fixtures import sample_instrument, sample_klines


class MarketDataTests(unittest.TestCase):
    def setUp(self) -> None:
        self.store = MarketDataStore()
        self.store.add_instrument(sample_instrument())

    def test_md_01_history_kline_ingest_and_query(self) -> None:
        bars = sample_klines()
        accepted = self.store.ingest_klines(bars)
        queried = self.store.query_klines("BTCUSDT", "1d")
        self.assertEqual(accepted, len(bars))
        self.assertEqual(len(queried), len(bars))
        self.assertEqual(queried[-1].close, 115.0)

    def test_md_03_duplicate_kline_does_not_pollute_standard_layer(self) -> None:
        bar = sample_klines()[0]
        accepted = self.store.ingest_klines([bar, bar])
        queried = self.store.query_klines("BTCUSDT", "1d")
        self.assertEqual(accepted, 1)
        self.assertEqual(len(queried), 1)
        self.assertTrue(any(issue.issue_type == DataQualityStatus.DUPLICATE for issue in self.store.quality_issues))

    def test_md_04_out_of_order_tick_is_sorted_on_query(self) -> None:
        bars = sample_klines()
        tick_late = Tick("BTCUSDT", bars[1].open_time + timedelta(minutes=2), 101.0, 0.2)
        tick_early = Tick("BTCUSDT", bars[1].open_time + timedelta(minutes=1), 100.5, 0.1)
        self.store.ingest_ticks([tick_late, tick_early])
        queried = self.store.query_ticks("BTCUSDT")
        self.assertEqual([item.price for item in queried], [100.5, 101.0])

    def test_md_05_future_timestamp_is_rejected(self) -> None:
        bar = sample_klines()[0]
        future_bar = Kline(
            instrument_id=bar.instrument_id,
            timeframe=bar.timeframe,
            open_time=bar.open_time + timedelta(days=800),
            close_time=bar.close_time + timedelta(days=800),
            open=bar.open,
            high=bar.high,
            low=bar.low,
            close=bar.close,
            volume=bar.volume,
        )
        accepted = self.store.ingest_klines([future_bar])
        self.assertEqual(accepted, 0)
        self.assertTrue(any(issue.issue_type == DataQualityStatus.FUTURE_TIMESTAMP for issue in self.store.quality_issues))


if __name__ == "__main__":
    unittest.main()
