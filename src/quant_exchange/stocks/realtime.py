"""Background market-tape service for live stock quote snapshots."""

from __future__ import annotations

import hashlib
import random
import threading
from dataclasses import dataclass
from datetime import datetime, time, timezone
from typing import Any, Callable
from zoneinfo import ZoneInfo


@dataclass(slots=True)
class RealtimeQuoteState:
    """Mutable live quote state for one stock instrument."""

    instrument_id: str
    symbol: str
    company_name: str
    market_region: str
    exchange_code: str
    last_price: float
    previous_close: float
    open_price: float
    high_price: float
    low_price: float
    volume: float
    turnover: float
    market_status: str
    quote_time: datetime
    source: str

    def to_payload(self) -> dict[str, Any]:
        """Return a JSON-safe live quote payload."""

        change = self.last_price - self.previous_close
        change_pct = 0.0 if abs(self.previous_close) < 1e-12 else change / self.previous_close * 100
        return {
            "instrument_id": self.instrument_id,
            "symbol": self.symbol,
            "company_name": self.company_name,
            "market_region": self.market_region,
            "exchange_code": self.exchange_code,
            "last_price": round(self.last_price, 2),
            "previous_close": round(self.previous_close, 2),
            "open_price": round(self.open_price, 2),
            "high_price": round(self.high_price, 2),
            "low_price": round(self.low_price, 2),
            "volume": round(self.volume, 0),
            "turnover": round(self.turnover, 2),
            "change": round(change, 2),
            "change_pct": round(change_pct, 2),
            "market_status": self.market_status,
            "quote_time": self.quote_time.isoformat(),
            "source": self.source,
        }


class RealtimeMarketService:
    """Maintain a background-updated quote book for the stock workbench."""

    ACTIVE_POLL_MS = 4_000
    EXTENDED_POLL_MS = 8_000
    IDLE_POLL_MS = 30_000

    MARKET_TIMEZONES = {
        "CN": "Asia/Shanghai",
        "HK": "Asia/Hong_Kong",
        "US": "America/New_York",
    }
    MARKET_SESSIONS = {
        "CN": (
            ("OPEN", time(9, 30), time(11, 30)),
            ("OPEN", time(13, 0), time(15, 0)),
        ),
        "HK": (
            ("OPEN", time(9, 30), time(12, 0)),
            ("OPEN", time(13, 0), time(16, 0)),
        ),
        "US": (
            ("PRE", time(4, 0), time(9, 30)),
            ("OPEN", time(9, 30), time(16, 0)),
            ("POST", time(16, 0), time(20, 0)),
        ),
    }

    def __init__(
        self,
        stock_directory,
        *,
        update_interval_seconds: float = 3.0,
        persist_minute_bars: bool = True,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.stock_directory = stock_directory
        self.update_interval_seconds = update_interval_seconds
        self.persist_minute_bars = persist_minute_bars
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self._lock = threading.RLock()
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._rngs: dict[str, random.Random] = {}
        self._quotes: dict[str, RealtimeQuoteState] = {}
        self._last_totals: dict[str, tuple[float, float]] = {}
        self._minute_bars: dict[str, dict[str, Any]] = {}
        self._summary: dict[str, Any] = {}
        self._last_refresh_at: datetime | None = None
        self.refresh_once(self.clock())

    def start(self) -> None:
        """Start the background refresh thread if it is not already running."""

        if self._thread is not None and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run, name="quant-exchange-market-feed", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """Stop the background refresh thread and wait for it to exit."""

        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=max(self.update_interval_seconds * 2.0, 0.5))
            self._thread = None

    def refresh_once(self, as_of: datetime | None = None) -> dict[str, Any]:
        """Advance the whole-market snapshot by one refresh cycle."""

        as_of = as_of or self.clock()
        pending_bars: list[tuple[str, dict[str, Any]]] = []
        with self._lock:
            self._bootstrap_quotes(as_of)
            for instrument_id, profile in self.stock_directory.profiles.items():
                quote = self._quotes[instrument_id]
                quote.market_status = self._market_status(profile.market_region, as_of)
                quote.quote_time = as_of
                if quote.market_status != "CLOSED":
                    self._advance_quote(quote, profile)
                    pending_bars.append((quote.instrument_id, self._build_minute_bar(quote, as_of)))
            self._last_refresh_at = as_of
            self._summary = self._build_summary(as_of)
            snapshot = self.snapshot()
        if self.persist_minute_bars:
            for instrument_id, bar in pending_bars:
                self.stock_directory.save_minute_bar(instrument_id, bar)
        return snapshot

    def snapshot(self, instrument_ids: list[str] | None = None) -> dict[str, Any]:
        """Return the latest market snapshot for selected instruments or the full universe."""

        with self._lock:
            selected_ids = instrument_ids or list(self._quotes)
            quotes = [self._quotes[instrument_id].to_payload() for instrument_id in selected_ids if instrument_id in self._quotes]
            open_count = sum(1 for quote in quotes if quote["market_status"] == "OPEN")
            extended_count = sum(1 for quote in quotes if quote["market_status"] in {"PRE", "POST"})
            return {
                "as_of": self._last_refresh_at.isoformat() if self._last_refresh_at else None,
                "source": "background_market_stream",
                "interval_seconds": self.update_interval_seconds,
                "universe_count": len(self._quotes),
                "open_quote_count": open_count,
                "extended_quote_count": extended_count,
                "live_window": open_count > 0 or extended_count > 0,
                "recommended_poll_ms": self._recommended_poll_ms(open_count, extended_count),
                "quotes": quotes,
                "summary": self._summary,
            }

    def _run(self) -> None:
        """Refresh the quote book on a fixed interval until stopped."""

        while not self._stop_event.wait(self.update_interval_seconds):
            self.refresh_once()

    def _bootstrap_quotes(self, as_of: datetime) -> None:
        """Populate initial quote state for any stock not yet tracked."""

        for instrument_id, profile in self.stock_directory.profiles.items():
            if instrument_id in self._quotes:
                continue
            history_source, bars = self.stock_directory._history_payload(instrument_id)
            latest_bar = bars[-1]
            previous_bar = bars[-2] if len(bars) >= 2 else latest_bar
            market_status = self._market_status(profile.market_region, as_of)
            self._quotes[instrument_id] = RealtimeQuoteState(
                instrument_id=instrument_id,
                symbol=profile.symbol,
                company_name=profile.company_name,
                market_region=profile.market_region,
                exchange_code=profile.exchange_code,
                last_price=float(latest_bar["close"]),
                previous_close=float(previous_bar["close"]),
                open_price=float(latest_bar["open"]),
                high_price=float(latest_bar["high"]),
                low_price=float(latest_bar["low"]),
                volume=float(latest_bar["volume"]),
                turnover=float(latest_bar["volume"]) * float(latest_bar["close"]),
                market_status=market_status,
                quote_time=as_of,
                source=f"market_stream:{history_source}",
            )
            seed = int(hashlib.sha256(instrument_id.encode("utf-8")).hexdigest()[:16], 16)
            self._rngs[instrument_id] = random.Random(seed)
            self._last_totals[instrument_id] = (
                self._quotes[instrument_id].volume,
                self._quotes[instrument_id].turnover,
            )

    def _advance_quote(self, quote: RealtimeQuoteState, profile) -> None:
        """Apply one deterministic pseudo-live tick to a quote."""

        rng = self._rngs[quote.instrument_id]
        volatility = max(abs(quote.high_price - quote.low_price) / max(quote.last_price, 1.0), 0.004)
        phase_scale = {"PRE": 0.25, "OPEN": 0.55, "POST": 0.18}.get(quote.market_status, 0.0)
        drift = (quote.last_price - quote.previous_close) / max(quote.previous_close, 1.0) * 0.04
        shock = (rng.random() - 0.5) * volatility * phase_scale
        new_price = max(0.2, quote.last_price * (1.0 + drift + shock))
        quote.last_price = round(new_price, 2)
        quote.high_price = round(max(quote.high_price, quote.last_price), 2)
        quote.low_price = round(min(quote.low_price, quote.last_price), 2)
        liquidity_scale = max((profile.market_cap or 5_000.0) / 50_000.0, 0.1)
        volume_increment = max(1.0, liquidity_scale * (700 + rng.random() * 1_400) * (1.0 + phase_scale))
        quote.volume += volume_increment
        quote.turnover += volume_increment * quote.last_price

    def _build_minute_bar(self, quote: RealtimeQuoteState, as_of: datetime) -> dict[str, Any]:
        """Aggregate one evolving minute bar in memory before persisting it."""

        previous_volume, previous_turnover = self._last_totals.get(quote.instrument_id, (quote.volume, quote.turnover))
        delta_volume = max(quote.volume - previous_volume, 0.0)
        delta_turnover = max(quote.turnover - previous_turnover, 0.0)
        minute_time = as_of.astimezone(timezone.utc).replace(second=0, microsecond=0)
        bar_time = minute_time.isoformat()
        current = self._minute_bars.get(quote.instrument_id)
        if current is None or current["bar_time"] != bar_time:
            current = {
                "instrument_id": quote.instrument_id,
                "bar_time": bar_time,
                "open": round(quote.last_price, 2),
                "high": round(quote.last_price, 2),
                "low": round(quote.last_price, 2),
                "close": round(quote.last_price, 2),
                "volume": round(delta_volume, 0),
                "turnover": round(delta_turnover, 2),
                "market_region": quote.market_region,
                "exchange_code": quote.exchange_code,
                "source": quote.source,
            }
        else:
            current["high"] = round(max(float(current["high"]), quote.last_price), 2)
            current["low"] = round(min(float(current["low"]), quote.last_price), 2)
            current["close"] = round(quote.last_price, 2)
            current["volume"] = round(float(current["volume"]) + delta_volume, 0)
            current["turnover"] = round(float(current["turnover"]) + delta_turnover, 2)
            current["source"] = quote.source
        self._minute_bars[quote.instrument_id] = current
        self._last_totals[quote.instrument_id] = (quote.volume, quote.turnover)
        return dict(current)

    def _build_summary(self, as_of: datetime) -> dict[str, Any]:
        """Aggregate the quote book into whole-market summary cards."""

        payloads = [quote.to_payload() for quote in self._quotes.values()]
        gainers = sorted(payloads, key=lambda item: item["change_pct"], reverse=True)
        losers = sorted(payloads, key=lambda item: item["change_pct"])
        most_active = sorted(payloads, key=lambda item: item["turnover"], reverse=True)
        region_summary = []
        for region in sorted({payload["market_region"] for payload in payloads}):
            region_quotes = [payload for payload in payloads if payload["market_region"] == region]
            avg_change = sum(item["change_pct"] for item in region_quotes) / max(len(region_quotes), 1)
            open_count = sum(1 for item in region_quotes if item["market_status"] == "OPEN")
            extended_count = sum(1 for item in region_quotes if item["market_status"] in {"PRE", "POST"})
            region_summary.append(
                {
                    "market_region": region,
                    "count": len(region_quotes),
                    "average_change_pct": round(avg_change, 2),
                    "open_count": open_count,
                    "extended_count": extended_count,
                }
            )
        return {
            "as_of": as_of.isoformat(),
            "advancing": sum(1 for item in payloads if item["change"] > 0),
            "declining": sum(1 for item in payloads if item["change"] < 0),
            "unchanged": sum(1 for item in payloads if item["change"] == 0),
            "open_count": sum(1 for item in payloads if item["market_status"] == "OPEN"),
            "extended_count": sum(1 for item in payloads if item["market_status"] in {"PRE", "POST"}),
            "total_turnover": round(sum(item["turnover"] for item in payloads), 2),
            "top_gainers": gainers[:3],
            "top_losers": losers[:3],
            "most_active": most_active[:3],
            "region_summary": region_summary,
        }

    def _recommended_poll_ms(self, open_count: int, extended_count: int) -> int:
        """Return the recommended browser polling interval for the current session state."""

        if open_count > 0:
            return self.ACTIVE_POLL_MS
        if extended_count > 0:
            return self.EXTENDED_POLL_MS
        return self.IDLE_POLL_MS

    def _market_status(self, market_region: str, as_of: datetime) -> str:
        """Return the session status for one market region at a specific time."""

        zone_name = self.MARKET_TIMEZONES.get(market_region)
        if zone_name is None:
            return "CLOSED"
        local_time = as_of.astimezone(ZoneInfo(zone_name))
        if local_time.weekday() >= 5:
            return "CLOSED"
        sessions = self.MARKET_SESSIONS.get(market_region, ())
        for label, start_at, end_at in sessions:
            if start_at <= local_time.time() < end_at:
                return label
        return "CLOSED"
