"""Tests for enhanced market data features.

Tests:
- Order book snapshot storage
- Funding rate tracking
- Account snapshot synchronization
- Subscription management
- Data quality checks
- Staleness detection
"""

from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from quant_exchange.core.models import (
    AccountSnapshot,
    FundingRate,
    Instrument,
    Kline,
    MarketType,
    OrderBookLevel,
    OrderBookSnapshot,
    Tick,
)
from quant_exchange.marketdata import DataQualityIssue, DataQualityStatus, MarketDataStore, Subscription


def utc_now() -> datetime:
    """Return a timezone-aware UTC timestamp."""
    return datetime.now(timezone.utc)


def sample_instrument(instrument_id: str = "BTCUSDT") -> Instrument:
    """Create a sample instrument."""
    return Instrument(
        instrument_id=instrument_id,
        symbol=instrument_id,
        market=MarketType.CRYPTO,
        lot_size=0.001,
    )


def sample_kline(instrument_id: str = "BTCUSDT", offset: int = 0) -> Kline:
    """Create a sample kline."""
    base_time = utc_now() - timedelta(hours=offset + 1)  # Ensure it's in the past
    return Kline(
        instrument_id=instrument_id,
        timeframe="1m",
        open_time=base_time,
        close_time=base_time + timedelta(minutes=1),
        open=50000.0 + offset,
        high=50100.0 + offset,
        low=49900.0 + offset,
        close=50050.0 + offset,
        volume=100.0,
    )


def sample_tick(instrument_id: str = "BTCUSDT", offset: int = 0) -> Tick:
    """Create a sample tick."""
    return Tick(
        instrument_id=instrument_id,
        timestamp=utc_now() - timedelta(seconds=offset),
        price=50000.0 + offset,
        size=1.0,
    )


def sample_orderbook(instrument_id: str = "BTCUSDT", seq: int = 1) -> OrderBookSnapshot:
    """Create a sample order book snapshot."""
    return OrderBookSnapshot(
        instrument_id=instrument_id,
        timestamp=utc_now(),
        bid_levels=(
            OrderBookLevel(price=49900.0, quantity=10.0),
            OrderBookLevel(price=49800.0, quantity=20.0),
        ),
        ask_levels=(
            OrderBookLevel(price=50100.0, quantity=15.0),
            OrderBookLevel(price=50200.0, quantity=25.0),
        ),
        sequence_no=seq,
    )


class OrderBookTests(unittest.TestCase):
    """Test order book snapshot storage and querying."""

    def setUp(self) -> None:
        self.store = MarketDataStore()

    def test_ingest_orderbook_stores_snapshot(self) -> None:
        """Verify order book snapshot is stored."""
        ob = sample_orderbook()
        result = self.store.ingest_orderbook(ob)

        self.assertTrue(result)
        stored = self.store.get_orderbook("BTCUSDT")
        self.assertIsNotNone(stored)
        self.assertEqual(stored.instrument_id, "BTCUSDT")

    def test_ingest_orderbook_rejects_out_of_sequence(self) -> None:
        """Verify out-of-sequence order book is rejected."""
        ob1 = sample_orderbook(seq=2)
        ob2 = sample_orderbook(seq=1)

        self.store.ingest_orderbook(ob1)
        result = self.store.ingest_orderbook(ob2)

        self.assertFalse(result)

    def test_latest_orderbook_preserved(self) -> None:
        """Verify latest order book is preserved."""
        ob1 = sample_orderbook(seq=1)
        ob2 = sample_orderbook(seq=2)

        self.store.ingest_orderbook(ob1)
        self.store.ingest_orderbook(ob2)

        stored = self.store.get_orderbook("BTCUSDT")
        self.assertEqual(stored.sequence_no, 2)


class FundingRateTests(unittest.TestCase):
    """Test funding rate tracking for perpetual contracts."""

    def setUp(self) -> None:
        self.store = MarketDataStore()

    def test_ingest_funding_rate(self) -> None:
        """Verify funding rate is stored."""
        fr = FundingRate(
            instrument_id="BTCUSDT",
            timestamp=utc_now(),
            funding_rate=0.0001,
        )
        self.store.ingest_funding_rate(fr)

        latest = self.store.get_latest_funding_rate("BTCUSDT")
        self.assertIsNotNone(latest)
        self.assertEqual(latest.funding_rate, 0.0001)

    def test_query_funding_rates(self) -> None:
        """Verify funding rate history can be queried."""
        fr1 = FundingRate(instrument_id="BTCUSDT", timestamp=utc_now() - timedelta(hours=1), funding_rate=0.0001)
        fr2 = FundingRate(instrument_id="BTCUSDT", timestamp=utc_now(), funding_rate=0.0002)

        self.store.ingest_funding_rate(fr1)
        self.store.ingest_funding_rate(fr2)

        rates = self.store.query_funding_rates("BTCUSDT")
        self.assertEqual(len(rates), 2)


class AccountSnapshotTests(unittest.TestCase):
    """Test account snapshot synchronization."""

    def setUp(self) -> None:
        self.store = MarketDataStore()

    def test_ingest_account_snapshot(self) -> None:
        """Verify account snapshot is stored."""
        snapshot = AccountSnapshot(
            account_id="acc1",
            timestamp=utc_now(),
            cash=100000.0,
            equity=150000.0,
            margin_ratio=0.5,
        )
        self.store.ingest_account_snapshot(snapshot)

        latest = self.store.get_latest_account_snapshot("acc1")
        self.assertIsNotNone(latest)
        self.assertEqual(latest.equity, 150000.0)

    def test_query_account_snapshots(self) -> None:
        """Verify account snapshot history can be queried."""
        s1 = AccountSnapshot(account_id="acc1", timestamp=utc_now() - timedelta(hours=1),
                            cash=100000.0, equity=100000.0)
        s2 = AccountSnapshot(account_id="acc1", timestamp=utc_now(),
                            cash=90000.0, equity=150000.0)

        self.store.ingest_account_snapshot(s1)
        self.store.ingest_account_snapshot(s2)

        snapshots = self.store.query_account_snapshots("acc1")
        self.assertEqual(len(snapshots), 2)


class SubscriptionTests(unittest.TestCase):
    """Test subscription management."""

    def setUp(self) -> None:
        self.store = MarketDataStore()
        self.store.add_instrument(sample_instrument())

    def test_subscribe_creates_subscription(self) -> None:
        """Verify subscription is created."""
        callback_called = False

        def callback(data):
            nonlocal callback_called
            callback_called = True

        sub = self.store.subscribe("BTCUSDT", "kline", "1m", callback)

        self.assertIsNotNone(sub.subscription_id)
        self.assertEqual(sub.instrument_id, "BTCUSDT")
        self.assertTrue(sub.is_active)

    def test_unsubscribe_deactivates(self) -> None:
        """Verify unsubscribe deactivates subscription."""
        sub = self.store.subscribe("BTCUSDT", "kline")

        result = self.store.unsubscribe(sub.subscription_id)

        self.assertTrue(result)
        retrieved = self.store.get_subscription(sub.subscription_id)
        self.assertFalse(retrieved.is_active)

    def test_trigger_callback(self) -> None:
        """Verify callback is triggered."""
        received_data = []

        def callback(data):
            received_data.append(data)

        sub = self.store.subscribe("BTCUSDT", "kline", "1m", callback)
        self.store.trigger_subscription_callback("BTCUSDT", "kline", {"price": 50000})

        self.assertEqual(len(received_data), 1)
        self.assertEqual(received_data[0]["price"], 50000)


class DataQualityTests(unittest.TestCase):
    """Test data quality checks and issue tracking."""

    def setUp(self) -> None:
        self.store = MarketDataStore()
        self.store.add_instrument(sample_instrument())

    def test_duplicate_kline_rejected(self) -> None:
        """Verify duplicate kline is rejected and issue recorded."""
        kline = sample_kline()
        self.store.ingest_klines([kline])
        self.store.ingest_klines([kline])

        issues = self.store.get_quality_issues(instrument_id="BTCUSDT")
        self.assertEqual(len(issues), 1)
        self.assertEqual(issues[0].issue_type, DataQualityStatus.DUPLICATE)

    def test_future_kline_rejected(self) -> None:
        """Verify future kline is rejected."""
        future_time = utc_now() + timedelta(hours=1)
        kline = Kline(
            instrument_id="BTCUSDT",
            timeframe="1m",
            open_time=future_time,
            close_time=future_time + timedelta(minutes=1),
            open=50000.0,
            high=50100.0,
            low=49900.0,
            close=50050.0,
            volume=100.0,
        )

        self.store.ingest_klines([kline])

        issues = self.store.get_quality_issues(instrument_id="BTCUSDT")
        self.assertEqual(len(issues), 1)
        self.assertEqual(issues[0].issue_type, DataQualityStatus.FUTURE_TIMESTAMP)


class StalenessTests(unittest.TestCase):
    """Test staleness detection."""

    def setUp(self) -> None:
        self.store = MarketDataStore()
        self.store.add_instrument(sample_instrument())

    def test_stale_instrument_detected(self) -> None:
        """Verify stale instrument is detected."""
        # Don't add any data
        self.assertTrue(self.store.is_stale("BTCUSDT"))

    def test_non_stale_instrument(self) -> None:
        """Verify non-stale instrument is detected."""
        self.store.ingest_klines([sample_kline()])
        self.assertFalse(self.store.is_stale("BTCUSDT"))

    def test_get_stale_instruments(self) -> None:
        """Verify stale instruments list."""
        self.store.ingest_klines([sample_kline()])
        self.assertEqual(len(self.store.get_stale_instruments()), 0)


class LatestPriceTests(unittest.TestCase):
    """Test latest price queries."""

    def setUp(self) -> None:
        self.store = MarketDataStore()
        self.store.add_instrument(sample_instrument())

    def test_latest_price_from_ticks(self) -> None:
        """Verify latest price comes from ticks when available."""
        tick = sample_tick()
        self.store.ingest_ticks([tick])

        price = self.store.latest_price("BTCUSDT")
        self.assertEqual(price, 50000.0)

    def test_latest_price_from_klines(self) -> None:
        """Verify latest price falls back to klines."""
        kline = sample_kline()
        self.store.ingest_klines([kline])

        price = self.store.latest_price("BTCUSDT", timeframe="1m")
        self.assertEqual(price, 50050.0)

    def test_latest_prices_multiple_instruments(self) -> None:
        """Verify latest prices for multiple instruments."""
        self.store.add_instrument(sample_instrument("ETHUSDT"))
        self.store.ingest_ticks([sample_tick("BTCUSDT")])
        self.store.ingest_ticks([sample_tick("ETHUSDT", offset=1)])

        prices = self.store.latest_prices(["BTCUSDT", "ETHUSDT"])
        self.assertEqual(prices["BTCUSDT"], 50000.0)
        self.assertEqual(prices["ETHUSDT"], 50001.0)


class MarketSnapshotTests(unittest.TestCase):
    """Test market snapshot generation."""

    def setUp(self) -> None:
        self.store = MarketDataStore()
        self.store.add_instrument(sample_instrument())

    def test_get_market_snapshot(self) -> None:
        """Verify market snapshot contains all instruments."""
        self.store.ingest_klines([sample_kline()])
        self.store.ingest_funding_rate(
            FundingRate(instrument_id="BTCUSDT", timestamp=utc_now(), funding_rate=0.0001)
        )
        self.store.ingest_orderbook(sample_orderbook())

        snapshot = self.store.get_market_snapshot()

        self.assertIn("BTCUSDT", snapshot["instruments"])
        self.assertIn("snapshot_time", snapshot)
        self.assertIn("stale_instruments", snapshot)


if __name__ == "__main__":
    unittest.main()
