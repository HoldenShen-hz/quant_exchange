"""FX and precious metals market service (FX-01 ~ FX-04).

Covers:
- Major, minor, and cross currency pairs
- Precious metals (gold, silver, crude oil, natural gas)
- Economic calendar events with impact levels
- Currency strength indicators
- Cross-asset risk observation
"""

from __future__ import annotations

import math
import uuid
from collections import defaultdict
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


# ─────────────────────────────────────────────────────────────────────────────
# Enums
# ─────────────────────────────────────────────────────────────────────────────

class CurrencyRole(str, Enum):
    """Role of a currency in a pair."""

    BASE = "base"
    QUOTE = "quote"


class EconomicImpact(str, Enum):
    """Impact level of an economic calendar event."""

    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class MetalType(str, Enum):
    """Precious metals and commodities supported."""

    GOLD = "XAU"
    SILVER = "XAG"
    CRUDE_OIL = "CL"
    NATURAL_GAS = "NG"


# ─────────────────────────────────────────────────────────────────────────────
# Data Models
# ─────────────────────────────────────────────────────────────────────────────

@dataclass(slots=True)
class FXQuote:
    """Real-time FX quote for a currency pair."""

    pair: str          # e.g. "EURUSD"
    bid: float
    ask: float
    mid: float
    spread_bps: float
    timestamp: datetime


@dataclass(slots=True)
class CurrencyPair:
    """Currency pair metadata."""

    pair: str
    base_currency: str
    quote_currency: str
    pip: float
    lot_size: float
    trading_hours: str = "24x7"
    settlement: str = "T+2"


@dataclass(slots=True)
class MetalQuote:
    """Quote for a precious metal or commodity."""

    symbol: str
    bid: float
    ask: float
    mid: float
    spread_bps: float
    timestamp: datetime


@dataclass(slots=True)
class EconomicEvent:
    """Economic calendar event."""

    event_id: str
    currency: str
    event_name: str
    impact: EconomicImpact
    event_time: datetime
    actual: float | None = None
    forecast: float | None = None
    previous: float | None = None
    unit: str = ""
    period: str = ""


@dataclass(slots=True)
class CurrencyStrengthIndicator:
    """Currency strength score based on multi-pair analysis."""

    currency: str
    score: float              # -100 to +100
    timestamp: datetime
    contributing_pairs: tuple[str, ...] = field(default_factory=tuple)


@dataclass(slots=True)
class CrossAssetRisk:
    """Cross-asset risk observation for multi-asset portfolios."""

    timestamp: datetime
    fx_risk: float           # Portfolio P&L sensitivity to FX moves
    commodity_risk: float
    correlation_regime: str  # "risk_on", "risk_off", "transitioning"
    stress_scenario: str | None = None


# ─────────────────────────────────────────────────────────────────────────────
# FX Service
# ─────────────────────────────────────────────────────────────────────────────

class FXService:
    """FX and precious metals market data and analytics service (FX-01 ~ FX-04).

    Provides:
    - Currency pair registry with metadata
    - Real-time FX quotes
    - Precious metals quotes
    - Economic calendar management
    - Currency strength indicators
    - Cross-asset risk observation
    """

    def __init__(self, persistence=None) -> None:
        self.persistence = persistence
        self._pairs: dict[str, CurrencyPair] = {}
        self._quotes: dict[str, FXQuote] = {}
        self._metal_quotes: dict[str, MetalQuote] = {}
        self._economic_events: list[EconomicEvent] = []
        self._currency_strengths: dict[str, CurrencyStrengthIndicator] = {}
        self._cross_asset_risks: list[CrossAssetRisk] = []
        self._registered_metals: dict[str, MetalType] = {}

        self._init_currency_pairs()
        self._init_metals()

    def _init_currency_pairs(self) -> None:
        """Initialize standard currency pair registry."""
        standard_pairs = [
            ("EURUSD", "EUR", "USD", 0.0001, 100000),
            ("GBPUSD", "GBP", "USD", 0.0001, 100000),
            ("USDJPY", "USD", "JPY", 0.01, 100000),
            ("USDCHF", "USD", "CHF", 0.0001, 100000),
            ("AUDUSD", "AUD", "USD", 0.0001, 100000),
            ("USDCAD", "USD", "CAD", 0.0001, 100000),
            ("NZDUSD", "NZD", "USD", 0.0001, 100000),
            ("EURGBP", "EUR", "GBP", 0.0001, 100000),
            ("EURJPY", "EUR", "JPY", 0.01, 100000),
            ("GBPJPY", "GBP", "JPY", 0.01, 100000),
            ("AUDJPY", "AUD", "JPY", 0.01, 100000),
            ("EURAUD", "EUR", "AUD", 0.0001, 100000),
            ("EURCHF", "EUR", "CHF", 0.0001, 100000),
            ("AUDCAD", "AUD", "CAD", 0.0001, 100000),
            ("AUDNZD", "AUD", "NZD", 0.0001, 100000),
            ("CADJPY", "CAD", "JPY", 0.01, 100000),
            ("CHFJPY", "CHF", "JPY", 0.01, 100000),
            ("EURCAD", "EUR", "CAD", 0.0001, 100000),
            ("GBPAUD", "GBP", "AUD", 0.0001, 100000),
            ("GBPCAD", "GBP", "CAD", 0.0001, 100000),
            ("USDHKD", "USD", "HKD", 0.0001, 100000),
            ("USDSGD", "USD", "SGD", 0.0001, 100000),
            ("USDCNH", "USD", "CNH", 0.0001, 100000),
            ("USDKRW", "USD", "KRW", 0.01, 100000),
            ("USDINR", "USD", "INR", 0.01, 100000),
            ("USDMXN", "USD", "MXN", 0.0001, 100000),
            ("USDBRL", "USD", "BRL", 0.0001, 100000),
            ("USDZAR", "USD", "ZAR", 0.0001, 100000),
            ("USDCZK", "USD", "CZK", 0.0001, 100000),
            ("USDPLN", "USD", "PLN", 0.0001, 100000),
            ("USDSEK", "USD", "SEK", 0.0001, 100000),
            ("USDNOK", "USD", "NOK", 0.0001, 100000),
            ("USDTRY", "USD", "TRY", 0.0001, 100000),
        ]
        for pair_code, base, quote, pip, lot_size in standard_pairs:
            self._pairs[pair_code] = CurrencyPair(
                pair=pair_code,
                base_currency=base,
                quote_currency=quote,
                pip=pip,
                lot_size=lot_size,
            )

    def _init_metals(self) -> None:
        """Initialize precious metals registry."""
        self._registered_metals = {
            "XAUUSD": MetalType.GOLD,
            "XAGUSD": MetalType.SILVER,
            "CLUSD": MetalType.CRUDE_OIL,
            "NGUSD": MetalType.NATURAL_GAS,
        }

    # ── FX Quotes ───────────────────────────────────────────────────────────

    def update_quote(self, pair: str, bid: float, ask: float) -> FXQuote:
        """Update the live quote for a currency pair."""
        mid = (bid + ask) / 2.0
        spread_bps = (ask - bid) / mid * 10000 if mid > 0 else 0.0
        quote = FXQuote(
            pair=pair,
            bid=bid,
            ask=ask,
            mid=mid,
            spread_bps=spread_bps,
            timestamp=datetime.now(timezone.utc),
        )
        self._quotes[pair] = quote
        return quote

    def get_quote(self, pair: str) -> FXQuote | None:
        """Get the current quote for a currency pair."""
        return self._quotes.get(pair)

    def get_all_quotes(self) -> dict[str, FXQuote]:
        """Get all current FX quotes."""
        return dict(self._quotes)

    def convert_currency(self, amount: float, from_pair: str, to_quote: str) -> float:
        """Convert an amount from one currency pair to another through USD."""
        from_quote = self._quotes.get(from_pair)
        if from_quote is None:
            return 0.0

        base, quote = from_pair[:3], from_pair[3:]
        # Convert to USD (quote currency is USD for most pairs)
        if quote == "USD":
            usd_amount = amount * from_quote.mid
        elif base == "USD":
            usd_amount = amount / from_quote.mid
        else:
            # Cross pair: convert through USD
            from_usd_quote = self._quotes.get(f"{base}USD")
            to_usd_quote = self._quotes.get(f"{to_quote}USD")
            if from_usd_quote and to_usd_quote:
                usd_amount = amount * from_usd_quote.mid
            else:
                return 0.0

        if to_quote == "USD":
            return usd_amount
        to_pair = f"{to_quote}USD"
        to_quote_obj = self._quotes.get(to_pair)
        if to_quote_obj:
            return usd_amount / to_quote_obj.mid
        return 0.0

    # ── Precious Metals ─────────────────────────────────────────────────────

    def update_metal_quote(self, symbol: str, bid: float, ask: float) -> MetalQuote:
        """Update the live quote for a precious metal or commodity."""
        mid = (bid + ask) / 2.0
        spread_bps = (ask - bid) / mid * 10000 if mid > 0 else 0.0
        quote = MetalQuote(
            symbol=symbol,
            bid=bid,
            ask=ask,
            mid=mid,
            spread_bps=spread_bps,
            timestamp=datetime.now(timezone.utc),
        )
        self._metal_quotes[symbol] = quote
        return quote

    def get_metal_quote(self, symbol: str) -> MetalQuote | None:
        """Get the current quote for a metal or commodity."""
        return self._metal_quotes.get(symbol)

    def get_all_metal_quotes(self) -> dict[str, MetalQuote]:
        """Get all current metal/commodity quotes."""
        return dict(self._metal_quotes)

    # ── Economic Calendar ───────────────────────────────────────────────────

    def add_economic_event(
        self,
        currency: str,
        event_name: str,
        impact: EconomicImpact,
        event_time: datetime,
        *,
        forecast: float | None = None,
        previous: float | None = None,
        unit: str = "",
        period: str = "",
    ) -> EconomicEvent:
        """Add an economic calendar event."""
        event = EconomicEvent(
            event_id=f"econ:{uuid.uuid4().hex[:8]}",
            currency=currency,
            event_name=event_name,
            impact=impact,
            event_time=event_time,
            forecast=forecast,
            previous=previous,
            unit=unit,
            period=period,
        )
        self._economic_events.append(event)
        return event

    def update_event_actual(self, event_id: str, actual: float) -> bool:
        """Update the actual value for an economic event."""
        for event in self._economic_events:
            if event.event_id == event_id:
                event.actual = actual
                return True
        return False

    def get_economic_events(
        self,
        currency: str | None = None,
        impact: EconomicImpact | None = None,
        window_hours: int = 24,
    ) -> list[EconomicEvent]:
        """Get economic events within a time window, optionally filtered."""
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(hours=window_hours)
        results = []
        for event in self._economic_events:
            if event.event_time < cutoff:
                continue
            if currency and event.currency != currency:
                continue
            if impact and event.impact != impact:
                continue
            results.append(event)
        return sorted(results, key=lambda e: e.event_time)

    def get_high_impact_events(self, window_hours: int = 24) -> list[EconomicEvent]:
        """Get all high and critical impact events in a window."""
        return self.get_economic_events(impact=EconomicImpact.HIGH, window_hours=window_hours) + \
               self.get_economic_events(impact=EconomicImpact.CRITICAL, window_hours=window_hours)

    # ── Currency Strength Indicator ─────────────────────────────────────────

    def compute_currency_strength(self, base_currencies: list[str]) -> dict[str, CurrencyStrengthIndicator]:
        """Compute relative currency strength from all available pair quotes.

        For each currency, aggregates mid-price changes across all pairs
        where the currency is either base or quote, producing a strength score.
        """
        now = datetime.now(timezone.utc)
        results: dict[str, CurrencyStrengthIndicator] = {}

        for pair_obj in self._pairs.values():
            pair = pair_obj.pair
            quote = self._quotes.get(pair)
            if quote is None:
                continue

            base = pair_obj.base_currency
            quote_ccy = pair_obj.quote_currency

            for ccy in base_currencies:
                if ccy not in results:
                    results[ccy] = CurrencyStrengthIndicator(
                        currency=ccy,
                        score=0.0,
                        timestamp=now,
                        contributing_pairs=(),
                    )

                score_delta = 0.0
                if ccy == base:
                    score_delta = quote.mid - quote.bid  # Positive = base strengthening
                elif ccy == quote_ccy:
                    score_delta = quote.ask - quote.mid  # Positive = quote weakening

                # Normalize by spread to avoid noise
                if quote.spread_bps > 0:
                    score_delta = score_delta / (quote.spread_bps / 10000) if quote.spread_bps / 10000 != 0 else 0.0

                csi = results[ccy]
                old_score = csi.score * len(csi.contributing_pairs)
                new_pairs = csi.contributing_pairs + (pair,)
                new_score = (old_score + score_delta) / len(new_pairs)
                results[ccy] = CurrencyStrengthIndicator(
                    currency=ccy,
                    score=new_score,
                    timestamp=now,
                    contributing_pairs=new_pairs,
                )

        # Normalize scores to -100..+100 range
        if results:
            all_scores = [r.score for r in results.values()]
            max_abs = max(abs(s) for s in all_scores) if all_scores else 1.0
            for ccy, csi in results.items():
                normalized = (csi.score / max_abs) * 100 if max_abs != 0 else 0.0
                results[ccy] = CurrencyStrengthIndicator(
                    currency=ccy,
                    score=normalized,
                    timestamp=now,
                    contributing_pairs=csi.contributing_pairs,
                )

        for ccy, csi in results.items():
            self._currency_strengths[ccy] = csi

        return results

    def get_currency_strength(self, currency: str) -> CurrencyStrengthIndicator | None:
        """Get the most recent currency strength for a currency."""
        return self._currency_strengths.get(currency)

    def get_all_currency_strengths(self) -> dict[str, CurrencyStrengthIndicator]:
        """Get all currency strength indicators."""
        return dict(self._currency_strengths)

    # ── Cross-Asset Risk ────────────────────────────────────────────────────

    def compute_cross_asset_risk(
        self,
        positions: dict[str, float],  # instrument_id -> quantity
        prices: dict[str, float],       # instrument_id -> current price
    ) -> CrossAssetRisk:
        """Compute cross-asset risk from FX and commodity positions.

        Estimates portfolio sensitivity to:
        - FX moves (based on USD-related pairs held)
        - Commodity moves (gold, oil positions)
        """
        now = datetime.now(timezone.utc)
        fx_exposure = 0.0
        commodity_exposure = 0.0

        metals = {"XAUUSD", "XAGUSD"}
        for inst_id, qty in positions.items():
            price = prices.get(inst_id, 0.0)
            notional = abs(qty * price)

            if inst_id in metals:
                commodity_exposure += notional
            elif inst_id[:3] in ("EUR", "GBP", "AUD", "NZD", "USD", "CAD", "CHF"):
                fx_exposure += notional

        # Determine correlation regime
        if fx_exposure > commodity_exposure * 2:
            regime = "risk_on"
        elif commodity_exposure > fx_exposure * 2:
            regime = "risk_off"
        else:
            regime = "transitioning"

        risk = CrossAssetRisk(
            timestamp=now,
            fx_risk=fx_exposure,
            commodity_risk=commodity_exposure,
            correlation_regime=regime,
        )
        self._cross_asset_risks.append(risk)
        return risk

    def get_latest_cross_asset_risk(self) -> CrossAssetRisk | None:
        """Get the most recent cross-asset risk observation."""
        return self._cross_asset_risks[-1] if self._cross_asset_risks else None
