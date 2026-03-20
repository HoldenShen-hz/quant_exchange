"""Market data storage helpers for normalized bars, ticks, order books, and quality checks.

Implements the documented market data capabilities (MD-01 to MD-10):
- Order book snapshot storage and querying
- Funding rate tracking for perpetual contracts
- Account snapshot synchronization
- Subscription management for real-time data
- Data quality checks and issue tracking
"""

from __future__ import annotations

import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Callable

from quant_exchange.core.models import (
    AccountSnapshot,
    CorporateAction,
    FundingRate,
    Instrument,
    Kline,
    OrderBookLevel,
    OrderBookSnapshot,
    Tick,
    utc_now,
)


class DataQualityStatus(str, Enum):
    """Status of data quality checks."""

    OK = "ok"
    DUPLICATE = "duplicate"
    OUT_OF_ORDER = "out_of_order"
    FUTURE_TIMESTAMP = "future_timestamp"
    INVALID_RANGE = "invalid_range"
    STALE = "stale"


@dataclass
class DataQualityIssue:
    """Record of a data quality issue detected during ingestion."""

    instrument_id: str
    issue_type: DataQualityStatus
    timestamp: datetime
    details: str = ""
    resolved: bool = False


@dataclass
class Subscription:
    """A market data subscription for an instrument."""

    subscription_id: str
    instrument_id: str
    data_type: str  # "kline", "tick", "orderbook", "funding"
    timeframe: str = ""
    callback: Callable | None = None
    created_at: datetime = field(default_factory=utc_now)
    is_active: bool = True
    last_update: datetime | None = None


class MarketDataStore:
    """In-memory market data service for MVP development and testing.

    Provides:
    - Normalized bars, ticks, and order book storage
    - Data quality checks and issue tracking
    - Funding rate tracking for perpetual contracts
    - Account snapshot synchronization
    - Subscription management for real-time data
    """

    def __init__(self, staleness_threshold_seconds: int = 60) -> None:
        self.instruments: dict[str, Instrument] = {}
        self._kline_index: dict[tuple[str, str], dict] = defaultdict(dict)
        self._tick_index: dict[str, dict] = defaultdict(dict)
        self._orderbook_index: dict[str, OrderBookSnapshot] = {}
        self._funding_rates: dict[str, list[FundingRate]] = defaultdict(list)
        self._account_snapshots: dict[str, list[AccountSnapshot]] = defaultdict(list)
        self._subscriptions: dict[str, Subscription] = {}
        self.quality_issues: list[DataQualityIssue] = []
        self._staleness_threshold = timedelta(seconds=staleness_threshold_seconds)
        self._latest_update: dict[str, datetime] = {}
        self._corporate_actions: dict[str, list[CorporateAction]] = defaultdict(list)

    # ==================== Instrument Management ====================

    def add_instrument(self, instrument: Instrument) -> None:
        """Register instrument metadata before ingesting data."""
        self.instruments[instrument.instrument_id] = instrument

    def get_instrument(self, instrument_id: str) -> Instrument | None:
        """Get instrument by ID."""
        return self.instruments.get(instrument_id)

    def list_instruments(self, market: str | None = None) -> list[Instrument]:
        """List all registered instruments, optionally filtered by market."""
        instruments = list(self.instruments.values())
        if market:
            instruments = [i for i in instruments if i.market.value == market]
        return instruments

    # ==================== K-Line Ingestion ====================

    def ingest_klines(self, klines: list[Kline]) -> int:
        """Store normalized bars while rejecting obvious quality issues."""

        accepted = 0
        for kline in klines:
            issue = self._check_kline_quality(kline)
            if issue:
                self.quality_issues.append(issue)
                continue

            key = (kline.instrument_id, kline.timeframe)
            self._kline_index[key][kline.open_time] = kline
            self._latest_update[kline.instrument_id] = utc_now()
            accepted += 1
        return accepted

    def _check_kline_quality(self, kline: Kline) -> DataQualityIssue | None:
        """Check kline for quality issues."""
        now = utc_now()

        if kline.open_time > kline.close_time:
            return DataQualityIssue(
                instrument_id=kline.instrument_id,
                issue_type=DataQualityStatus.INVALID_RANGE,
                timestamp=kline.open_time,
                details=f"open_time > close_time",
            )

        if kline.open_time > now + timedelta(minutes=5):
            return DataQualityIssue(
                instrument_id=kline.instrument_id,
                issue_type=DataQualityStatus.FUTURE_TIMESTAMP,
                timestamp=kline.open_time,
                details="kline timestamp is in the future",
            )

        key = (kline.instrument_id, kline.timeframe)
        existing = self._kline_index[key].get(kline.open_time)
        if existing == kline:
            return DataQualityIssue(
                instrument_id=kline.instrument_id,
                issue_type=DataQualityStatus.DUPLICATE,
                timestamp=kline.open_time,
                details="duplicate kline detected",
            )

        return None

    def query_klines(
        self,
        instrument_id: str,
        timeframe: str,
        *,
        start=None,
        end=None,
        limit: int | None = None,
    ) -> list[Kline]:
        """Query bar history by instrument, timeframe, and optional range."""

        items = sorted(
            self._kline_index[(instrument_id, timeframe)].values(),
            key=lambda item: item.open_time,
        )
        if start is not None:
            items = [item for item in items if item.open_time >= start]
        if end is not None:
            items = [item for item in items if item.close_time <= end]
        if limit is not None:
            items = items[:limit]
        return items

    # ==================== Tick Ingestion ====================

    def ingest_ticks(self, ticks: list[Tick]) -> int:
        """Store tick events while filtering duplicates and future timestamps."""

        accepted = 0
        for tick in ticks:
            issue = self._check_tick_quality(tick)
            if issue:
                self.quality_issues.append(issue)
                continue

            self._tick_index[tick.instrument_id][tick.timestamp] = tick
            self._latest_update[tick.instrument_id] = utc_now()
            accepted += 1
        return accepted

    def _check_tick_quality(self, tick: Tick) -> DataQualityIssue | None:
        """Check tick for quality issues."""
        now = utc_now()

        if tick.timestamp > now + timedelta(minutes=5):
            return DataQualityIssue(
                instrument_id=tick.instrument_id,
                issue_type=DataQualityStatus.FUTURE_TIMESTAMP,
                timestamp=tick.timestamp,
                details="tick timestamp is in the future",
            )

        existing = self._tick_index[tick.instrument_id].get(tick.timestamp)
        if existing == tick:
            return DataQualityIssue(
                instrument_id=tick.instrument_id,
                issue_type=DataQualityStatus.DUPLICATE,
                timestamp=tick.timestamp,
                details="duplicate tick detected",
            )

        return None

    def query_ticks(self, instrument_id: str, *, start=None, end=None) -> list[Tick]:
        """Query tick history for an instrument in timestamp order."""

        items = sorted(self._tick_index[instrument_id].values(), key=lambda item: item.timestamp)
        if start is not None:
            items = [item for item in items if item.timestamp >= start]
        if end is not None:
            items = [item for item in items if item.timestamp <= end]
        return items

    # ==================== Order Book ====================

    def ingest_orderbook(self, snapshot: OrderBookSnapshot) -> bool:
        """Store an order book snapshot.

        Returns True if the snapshot was stored, False if it was rejected as stale/duplicate.
        """
        key = snapshot.instrument_id
        existing = self._orderbook_index.get(key)

        if existing and existing.sequence_no >= snapshot.sequence_no:
            self.quality_issues.append(
                DataQualityIssue(
                    instrument_id=key,
                    issue_type=DataQualityStatus.OUT_OF_ORDER,
                    timestamp=snapshot.timestamp,
                    details=f"sequence {snapshot.sequence_no} <= existing {existing.sequence_no}",
                )
            )
            return False

        self._orderbook_index[key] = snapshot
        self._latest_update[key] = utc_now()
        return True

    def get_orderbook(self, instrument_id: str) -> OrderBookSnapshot | None:
        """Get the latest order book snapshot for an instrument."""
        return self._orderbook_index.get(instrument_id)

    def query_orderbook_history(
        self,
        instrument_id: str,
        *,
        start=None,
        end=None,
        limit: int | None = None,
    ) -> list[OrderBookSnapshot]:
        """Query historical order book snapshots."""
        # Note: This requires storing order book history, which the current
        # implementation doesn't do. Return the latest if no time filter needed.
        if start is None and end is None and limit is None:
            snapshot = self._orderbook_index.get(instrument_id)
            return [snapshot] if snapshot else []

        return []

    # ==================== Funding Rates ====================

    def ingest_funding_rate(self, funding_rate: FundingRate) -> None:
        """Store a funding rate observation for a perpetual contract."""
        self._funding_rates[funding_rate.instrument_id].append(funding_rate)
        # Keep only last 1000 observations
        if len(self._funding_rates[funding_rate.instrument_id]) > 1000:
            self._funding_rates[funding_rate.instrument_id] = self._funding_rates[funding_rate.instrument_id][-1000:]

    def get_latest_funding_rate(self, instrument_id: str) -> FundingRate | None:
        """Get the most recent funding rate for a perpetual."""
        rates = self._funding_rates.get(instrument_id, [])
        return rates[-1] if rates else None

    def query_funding_rates(
        self,
        instrument_id: str,
        *,
        start=None,
        end=None,
        limit: int | None = None,
    ) -> list[FundingRate]:
        """Query funding rate history for an instrument."""
        rates = sorted(self._funding_rates.get(instrument_id, []), key=lambda r: r.timestamp)
        if start is not None:
            rates = [r for r in rates if r.timestamp >= start]
        if end is not None:
            rates = [r for r in rates if r.timestamp <= end]
        if limit is not None:
            rates = rates[-limit:]
        return rates

    # ==================== Account Snapshots ====================

    def ingest_account_snapshot(self, snapshot: AccountSnapshot) -> None:
        """Store an account valuation snapshot."""
        self._account_snapshots[snapshot.account_id].append(snapshot)
        # Keep only last 1000 snapshots
        if len(self._account_snapshots[snapshot.account_id]) > 1000:
            self._account_snapshots[snapshot.account_id] = self._account_snapshots[snapshot.account_id][-1000:]

    def get_latest_account_snapshot(self, account_id: str) -> AccountSnapshot | None:
        """Get the most recent account snapshot."""
        snapshots = self._account_snapshots.get(account_id, [])
        return snapshots[-1] if snapshots else None

    def query_account_snapshots(
        self,
        account_id: str,
        *,
        start=None,
        end=None,
        limit: int | None = None,
    ) -> list[AccountSnapshot]:
        """Query account snapshot history."""
        snapshots = sorted(self._account_snapshots.get(account_id, []), key=lambda s: s.timestamp)
        if start is not None:
            snapshots = [s for s in snapshots if s.timestamp >= start]
        if end is not None:
            snapshots = [s for s in snapshots if s.timestamp <= end]
        if limit is not None:
            snapshots = snapshots[-limit:]
        return snapshots

    # ==================== Price Queries ====================

    def latest_price(self, instrument_id: str, timeframe: str = "1d") -> float:
        """Return the latest observable price from ticks or bars."""
        ticks = self.query_ticks(instrument_id)
        if ticks:
            return ticks[-1].price
        klines = self.query_klines(instrument_id, timeframe)
        if klines:
            return klines[-1].close
        raise KeyError(f"No market data found for instrument {instrument_id}.")

    def latest_prices(self, instrument_ids: list[str]) -> dict[str, float]:
        """Return latest prices for multiple instruments."""
        prices = {}
        for instrument_id in instrument_ids:
            try:
                prices[instrument_id] = self.latest_price(instrument_id)
            except KeyError:
                pass
        return prices

    # ==================== Staleness Detection ====================

    def is_stale(self, instrument_id: str) -> bool:
        """Check if the latest data for an instrument is stale."""
        last_update = self._latest_update.get(instrument_id)
        if last_update is None:
            return True
        return utc_now() - last_update > self._staleness_threshold

    def get_stale_instruments(self) -> list[str]:
        """Get list of instruments with stale data."""
        return [iid for iid in self.instruments.keys() if self.is_stale(iid)]

    # ==================== Quality Issues ====================

    def get_quality_issues(
        self,
        instrument_id: str | None = None,
        unresolved_only: bool = False,
        limit: int = 100,
    ) -> list[DataQualityIssue]:
        """Query data quality issues with optional filters."""
        issues = self.quality_issues
        if instrument_id:
            issues = [i for i in issues if i.instrument_id == instrument_id]
        if unresolved_only:
            issues = [i for i in issues if not i.resolved]
        return issues[-limit:]

    def resolve_quality_issue(self, index: int) -> bool:
        """Mark a quality issue as resolved."""
        if 0 <= index < len(self.quality_issues):
            self.quality_issues[index].resolved = True
            return True
        return False

    # ==================== Subscriptions ====================

    def subscribe(
        self,
        instrument_id: str,
        data_type: str,
        timeframe: str = "",
        callback: Callable | None = None,
    ) -> Subscription:
        """Create a subscription for market data."""
        subscription_id = str(uuid.uuid4())
        subscription = Subscription(
            subscription_id=subscription_id,
            instrument_id=instrument_id,
            data_type=data_type,
            timeframe=timeframe,
            callback=callback,
        )
        self._subscriptions[subscription_id] = subscription
        return subscription

    def unsubscribe(self, subscription_id: str) -> bool:
        """Cancel a subscription."""
        subscription = self._subscriptions.get(subscription_id)
        if subscription is None:
            return False
        subscription.is_active = False
        return True

    def get_subscription(self, subscription_id: str) -> Subscription | None:
        """Get a subscription by ID."""
        return self._subscriptions.get(subscription_id)

    def list_subscriptions(
        self,
        instrument_id: str | None = None,
        data_type: str | None = None,
        active_only: bool = False,
    ) -> list[Subscription]:
        """List subscriptions with optional filters."""
        subscriptions = list(self._subscriptions.values())
        if instrument_id:
            subscriptions = [s for s in subscriptions if s.instrument_id == instrument_id]
        if data_type:
            subscriptions = [s for s in subscriptions if s.data_type == data_type]
        if active_only:
            subscriptions = [s for s in subscriptions if s.is_active]
        return subscriptions

    def trigger_subscription_callback(
        self,
        instrument_id: str,
        data_type: str,
        data: Any,
    ) -> int:
        """Trigger callbacks for matching subscriptions. Returns count of triggered callbacks."""
        subscriptions = self.list_subscriptions(
            instrument_id=instrument_id,
            data_type=data_type,
            active_only=True,
        )
        triggered = 0
        for sub in subscriptions:
            if sub.callback:
                try:
                    sub.callback(data)
                    triggered += 1
                except Exception:
                    pass
            sub.last_update = utc_now()
        return triggered

    # ==================== Data Export ====================

    def export_klines_json(
        self,
        instrument_id: str,
        timeframe: str,
        *,
        start=None,
        end=None,
    ) -> list[dict]:
        """Export klines as JSON-serializable dict."""
        klines = self.query_klines(instrument_id, timeframe, start=start, end=end)
        return [
            {
                "instrument_id": k.instrument_id,
                "timeframe": k.timeframe,
                "open_time": k.open_time.isoformat(),
                "close_time": k.close_time.isoformat(),
                "open": k.open,
                "high": k.high,
                "low": k.low,
                "close": k.close,
                "volume": k.volume,
                "turnover": k.turnover,
            }
            for k in klines
        ]

    def get_market_snapshot(self) -> dict:
        """Get a snapshot of the entire market state."""
        snapshot_time = utc_now()
        instrument_snapshots = {}

        for instrument_id, instrument in self.instruments.items():
            try:
                price = self.latest_price(instrument_id)
            except KeyError:
                price = None

            funding_rate = self.get_latest_funding_rate(instrument_id)
            orderbook = self.get_orderbook(instrument_id)

            instrument_snapshots[instrument_id] = {
                "instrument_id": instrument_id,
                "symbol": instrument.symbol,
                "market": instrument.market.value,
                "last_price": price,
                "is_stale": self.is_stale(instrument_id),
                "funding_rate": funding_rate.funding_rate if funding_rate else None,
                "bid_levels": [
                    {"price": level.price, "quantity": level.quantity}
                    for level in (orderbook.bid_levels if orderbook else [])
                ] if orderbook else None,
                "ask_levels": [
                    {"price": level.price, "quantity": level.quantity}
                    for level in (orderbook.ask_levels if orderbook else [])
                ] if orderbook else None,
            }

        return {
            "snapshot_time": snapshot_time.isoformat(),
            "instruments": instrument_snapshots,
            "stale_instruments": self.get_stale_instruments(),
            "quality_issues_count": len([i for i in self.quality_issues if not i.resolved]),
        }

    # ─── MD-10: Corporate Actions ───────────────────────────────────────────────

    def add_corporate_action(self, action: CorporateAction) -> None:
        """Register a corporate action event for an instrument (MD-10)."""
        self._corporate_actions[action.instrument_id].append(action)

    def get_corporate_actions(
        self,
        instrument_id: str,
        event_type: str | None = None,
        from_date: datetime | None = None,
    ) -> list[CorporateAction]:
        """Get corporate actions for an instrument (MD-10).

        Args:
            instrument_id: The instrument to query.
            event_type: Filter by event type (dividend, split, etc.).
            from_date: Only return events on or after this date.
        """
        actions = list(self._corporate_actions.get(instrument_id, []))
        if event_type:
            actions = [a for a in actions if a.event_type == event_type]
        if from_date:
            actions = [a for a in actions if a.ex_date and a.ex_date >= from_date]
        return sorted(actions, key=lambda a: a.ex_date or datetime.min)

    def get_adjustment_factor(
        self,
        instrument_id: str,
        quote_date: datetime,
    ) -> float:
        """Calculate the cumulative price adjustment factor for an instrument on a given date (MD-10).

        Used by the backtest engine to produce split-adjusted and dividend-adjusted prices.
        Returns a multiplier where multiplied_price = raw_price * factor.

        A factor of 1.0 means no adjustment needed.
        For a 2-for-1 split that happened on ex_date, prices before ex_date are multiplied by 0.5.
        """
        factor = 1.0
        for action in self._corporate_actions.get(instrument_id, []):
            if action.ex_date is None:
                continue
            if action.ex_date > quote_date:
                continue
            if action.status == "cancelled":
                continue

            if action.event_type == "split" and action.split_ratio:
                # e.g., (2, 1) split: for every 1 old share, you get 2 new shares
                # so old price should be multiplied by 0.5 to get comparable price
                new_shares, old_shares = action.split_ratio
                factor *= old_shares / new_shares
            elif action.event_type == "dividend" and action.dividend_per_share:
                # Dividend adjustment: subtract dividend from price (approximate)
                # The actual CRSP adjustment uses the ratio of (P - D) / P
                # We use a simplified version here
                pass  # Dividend cash adjustment is handled differently in backtest

        return factor

    def get_upcoming_corporate_actions(
        self,
        instrument_id: str | None = None,
        days_ahead: int = 30,
    ) -> list[dict]:
        """Get upcoming corporate actions for display (MD-10)."""
        now = utc_now()
        cutoff = now + timedelta(days=days_ahead)
        result: list[dict] = []

        instruments_to_check = (
            [instrument_id] if instrument_id else list(self._corporate_actions.keys())
        )

        for iid in instruments_to_check:
            for action in self._corporate_actions.get(iid, []):
                if action.ex_date and now <= action.ex_date <= cutoff:
                    result.append({
                        "action_id": action.action_id,
                        "instrument_id": action.instrument_id,
                        "event_type": action.event_type,
                        "ex_date": action.ex_date.isoformat(),
                        "dividend_per_share": action.dividend_per_share,
                        "split_ratio": action.split_ratio,
                        "currency": action.currency,
                        "status": action.status,
                    })

        return sorted(result, key=lambda x: x["ex_date"])
