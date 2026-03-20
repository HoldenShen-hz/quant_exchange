"""Forex and commodities trading service (FX-01 ~ FX-04).

Covers:
- FX-01: Major and cross currency pairs, precious metals, energy commodities
- FX-02: Gold, silver, oil, natural gas pricing
- FX-03: Economic calendar, currency strength indicators, correlation analysis
- FX-04: Cross-asset risk observation
"""

from __future__ import annotations

import math
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any


# ─────────────────────────────────────────────────────────────────────────────
# Enums
# ─────────────────────────────────────────────────────────────────────────────


class AssetClass(str, Enum):
    FX_SPOT = "fx_spot"
    FX_FORWARD = "fx_forward"
    COMMODITY_METAL = "metal"
    COMMODITY_ENERGY = "energy"


class EconomicImpact(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


# ─────────────────────────────────────────────────────────────────────────────
# Data Models
# ─────────────────────────────────────────────────────────────────────────────


@dataclass(slots=True)
class CurrencyPair:
    """A currency pair with base and quote currency."""

    instrument_id: str  # e.g., "EURUSD"
    base_currency: str  # e.g., "EUR"
    quote_currency: str  # e.g., "USD"
    pip_size: float = 0.0001
    pip_value: float = 10.0  # per standard lot in quote currency
    contract_size: float = 100_000
    asset_class: AssetClass = AssetClass.FX_SPOT


@dataclass(slots=True)
class Commodity:
    """A commodity (metal or energy) instrument."""

    instrument_id: str  # e.g., "XAUUSD", "USOIL"
    name: str  # e.g., "Gold", "Crude Oil"
    unit: str = "oz"  # ounce, barrel
    contract_size: float = 100.0
    pip_size: float = 0.01
    asset_class: AssetClass = AssetClass.COMMODITY_METAL


@dataclass(slots=True)
class ForexQuote:
    """Real-time forex/commodity quote."""

    instrument_id: str
    bid: float
    ask: float
    mid: float
    spread_pips: float
    timestamp: datetime
    change_pct: float = 0.0


@dataclass(slots=True)
class EconomicEvent:
    """An economic calendar event."""

    event_id: str
    country: str  # e.g., "US", "EU", "CN"
    currency: str  # e.g., "USD", "EUR"
    event_name: str
    impact: EconomicImpact
    release_time: datetime
    previous_value: str
    forecast_value: str
    actual_value: str = ""
    source: str = ""


@dataclass(slots=True)
class CurrencyStrengthIndicator:
    """Currency strength based on multi-pair analysis."""

    currency: str
    strength_value: float  # 0-100 scale
    rank: int  # 1 = strongest
    change_1d: float  # daily change in strength
    contributing_pairs: list[str] = field(default_factory=list)


@dataclass(slots=True)
class CrossAssetRisk:
    """Cross-asset risk metrics for portfolio."""

    correlation_matrix: dict[str, dict[str, float]]
    strongest_correlation: tuple[str, str, float]
    risk_concentration: dict[str, float]  # asset -> risk %
    currency_exposure: dict[str, float]  # currency -> exposure


# ─────────────────────────────────────────────────────────────────────────────
# Forex Service
# ─────────────────────────────────────────────────────────────────────────────


class ForexService:
    """Forex and commodities service (FX-01 ~ FX-04).

    Provides:
    - FX-01: Major/cross currency pairs, precious metals, energy
    - FX-02: Real-time quotes for gold, silver, oil, gas
    - FX-03: Economic calendar, currency strength, correlations
    - FX-04: Cross-asset risk observation
    """

    # Default major and cross currency pairs (FX-01)
    DEFAULT_PAIRS: list[dict] = [
        {"instrument_id": "EURUSD", "base": "EUR", "quote": "USD", "pip": 0.0001},
        {"instrument_id": "GBPUSD", "base": "GBP", "quote": "USD", "pip": 0.0001},
        {"instrument_id": "USDJPY", "base": "USD", "quote": "JPY", "pip": 0.01},
        {"instrument_id": "USDCHF", "base": "USD", "quote": "CHF", "pip": 0.0001},
        {"instrument_id": "AUDUSD", "base": "AUD", "quote": "USD", "pip": 0.0001},
        {"instrument_id": "USDCAD", "base": "USD", "quote": "CAD", "pip": 0.0001},
        {"instrument_id": "NZDUSD", "base": "NZD", "quote": "USD", "pip": 0.0001},
        {"instrument_id": "EURGBP", "base": "EUR", "quote": "GBP", "pip": 0.0001},
        {"instrument_id": "EURJPY", "base": "EUR", "quote": "JPY", "pip": 0.01},
        {"instrument_id": "GBPJPY", "base": "GBP", "quote": "JPY", "pip": 0.01},
        # Cross pairs
        {"instrument_id": "EURCHF", "base": "EUR", "quote": "CHF", "pip": 0.0001},
        {"instrument_id": "AUDJPY", "base": "AUD", "quote": "JPY", "pip": 0.01},
        {"instrument_id": "EURAUD", "base": "EUR", "quote": "AUD", "pip": 0.0001},
        {"instrument_id": "GBPAUD", "base": "GBP", "quote": "AUD", "pip": 0.0001},
        {"instrument_id": "AUDCAD", "base": "AUD", "quote": "CAD", "pip": 0.0001},
    ]

    # Default commodities (FX-02)
    DEFAULT_COMMODITIES: list[dict] = [
        {"instrument_id": "XAUUSD", "name": "Gold", "unit": "oz", "pip": 0.01, "contract_size": 100.0},
        {"instrument_id": "XAGUSD", "name": "Silver", "unit": "oz", "pip": 0.01, "contract_size": 5000.0},
        {"instrument_id": "USOIL", "name": "Crude Oil", "unit": "bbl", "pip": 0.01, "contract_size": 1000.0},
        {"instrument_id": "UKOIL", "name": "Brent Crude", "unit": "bbl", "pip": 0.01, "contract_size": 1000.0},
        {"instrument_id": "NATGAS", "name": "Natural Gas", "unit": "MMBtu", "pip": 0.001, "contract_size": 10000.0},
    ]

    def __init__(self, persistence=None) -> None:
        self.persistence = persistence
        self._pairs: dict[str, CurrencyPair] = {}
        self._commodities: dict[str, Commodity] = {}
        self._quotes: dict[str, ForexQuote] = {}
        self._economic_calendar: list[EconomicEvent] = []
        self._correlation_cache: dict[str, dict[str, float]] = {}
        self._init_instruments()

    def _init_instruments(self) -> None:
        """Initialize default forex pairs and commodities."""
        for p in self.DEFAULT_PAIRS:
            pair = CurrencyPair(
                instrument_id=p["instrument_id"],
                base_currency=p["base"],
                quote_currency=p["quote"],
                pip_size=p["pip"],
            )
            self._pairs[p["instrument_id"]] = pair

        for c in self.DEFAULT_COMMODITIES:
            com = Commodity(
                instrument_id=c["instrument_id"],
                name=c["name"],
                unit=c["unit"],
                pip_size=c["pip"],
                contract_size=c["contract_size"],
                asset_class=AssetClass.COMMODITY_METAL if c["instrument_id"] in ("XAUUSD", "XAGUSD") else AssetClass.COMMODITY_ENERGY,
            )
            self._commodities[c["instrument_id"]] = com

    # ── FX-01: Instrument Queries ──────────────────────────────────────────

    def list_pairs(self) -> list[CurrencyPair]:
        """List all registered currency pairs."""
        return list(self._pairs.values())

    def list_commodities(self) -> list[Commodity]:
        """List all registered commodities."""
        return list(self._commodities.values())

    def get_pair(self, instrument_id: str) -> CurrencyPair | None:
        """Get a currency pair by ID."""
        return self._pairs.get(instrument_id.upper())

    def get_commodity(self, instrument_id: str) -> Commodity | None:
        """Get a commodity by ID."""
        return self._commodities.get(instrument_id.upper())

    def get_quote(self, instrument_id: str) -> ForexQuote | None:
        """Get the latest quote for an instrument."""
        return self._quotes.get(instrument_id.upper())

    def update_quote(self, instrument_id: str, bid: float, ask: float, timestamp: datetime | None = None) -> ForexQuote:
        """Update a quote for an instrument (simulation)."""
        inst_id = instrument_id.upper()
        ts = timestamp or datetime.now(timezone.utc)
        mid = (bid + ask) / 2.0
        spread = (ask - bid) / self._get_pip_size(inst_id) if self._get_pip_size(inst_id) else (ask - bid) * 10000

        # Get previous quote for change calculation
        prev = self._quotes.get(inst_id)
        change = 0.0
        if prev:
            change = ((mid - prev.mid) / prev.mid) * 100 if prev.mid else 0.0

        quote = ForexQuote(
            instrument_id=inst_id,
            bid=bid,
            ask=ask,
            mid=mid,
            spread_pips=spread,
            timestamp=ts,
            change_pct=change,
        )
        self._quotes[inst_id] = quote
        return quote

    def _get_pip_size(self, instrument_id: str) -> float:
        """Get pip size for an instrument."""
        if instrument_id in self._pairs:
            return self._pairs[instrument_id].pip_size
        if instrument_id in self._commodities:
            return self._commodities[instrument_id].pip_size
        return 0.0001

    def simulate_quote(self, instrument_id: str, base_price: float | None = None) -> ForexQuote:
        """Generate a simulated quote (for backtesting without live data)."""
        import random
        inst_id = instrument_id.upper()

        # Default prices
        default_prices = {
            "EURUSD": 1.0850, "GBPUSD": 1.2650, "USDJPY": 149.50,
            "USDCHF": 0.8750, "AUDUSD": 0.6550, "USDCAD": 1.3650,
            "NZDUSD": 0.6050, "EURGBP": 0.8580, "EURJPY": 162.20,
            "GBPJPY": 189.10, "XAUUSD": 2030.0, "XAGUSD": 23.50,
            "USOIL": 78.50, "UKOIL": 82.00, "NATGAS": 2.85,
        }

        price = base_price or default_prices.get(inst_id, 1.0000)
        spread_pips = 2.0 if "JPY" not in inst_id else 0.02
        pip_val = 0.0001 if "JPY" not in inst_id else 0.01
        spread = spread_pips * pip_val
        bid = price - spread / 2
        ask = price + spread / 2

        # Add small random variation
        variation = random.uniform(-0.0002, 0.0002) * price
        bid += variation
        ask += variation

        return self.update_quote(inst_id, bid, ask)

    # ── FX-03: Economic Calendar ───────────────────────────────────────────

    def add_economic_event(
        self,
        country: str,
        currency: str,
        event_name: str,
        impact: EconomicImpact,
        release_time: datetime,
        previous_value: str,
        forecast_value: str,
        actual_value: str = "",
    ) -> EconomicEvent:
        """Add an economic calendar event (FX-03)."""
        event = EconomicEvent(
            event_id=f"econ:{uuid.uuid4().hex[:12]}",
            country=country,
            currency=currency,
            event_name=event_name,
            impact=impact,
            release_time=release_time,
            previous_value=previous_value,
            forecast_value=forecast_value,
            actual_value=actual_value,
        )
        self._economic_calendar.append(event)
        self._economic_calendar.sort(key=lambda e: e.release_time)
        return event

    def get_upcoming_events(self, hours: int = 24, currency: str | None = None) -> list[EconomicEvent]:
        """Get upcoming economic events (FX-03)."""
        now = datetime.now(timezone.utc)
        cutoff = now + timedelta(hours=hours)
        events = [e for e in self._economic_calendar if now <= e.release_time <= cutoff]
        if currency:
            events = [e for e in events if e.currency == currency]
        return events

    def list_currencies(self) -> list[str]:
        """List all currencies with available instruments."""
        currencies: set[str] = set()
        for p in self._pairs.values():
            currencies.add(p.base_currency)
            currencies.add(p.quote_currency)
        return sorted(currencies)

    # ── FX-03: Currency Strength Indicator ────────────────────────────────

    def compute_currency_strength(self) -> list[CurrencyStrengthIndicator]:
        """Compute currency strength from multi-pair analysis (FX-03).

        For each currency, aggregates the performance across all pairs
        where it's the base or quote currency.
        """
        currencies = self.list_currencies()
        strength_scores: dict[str, list[float]] = {c: [] for c in currencies}
        pair_changes: dict[str, list[str]] = {c: [] for c in currencies}

        for inst_id, quote in self._quotes.items():
            if quote.change_pct == 0.0:
                continue

            pair = self._pairs.get(inst_id)
            if not pair:
                continue

            # Base currency contribution
            strength_scores[pair.base_currency].append(quote.change_pct)
            pair_changes[pair.base_currency].append(inst_id)

            # Quote currency contribution (inverse)
            strength_scores[pair.quote_currency].append(-quote.change_pct)
            pair_changes[pair.quote_currency].append(inst_id)

        # Compute average strength
        results: list[CurrencyStrengthIndicator] = []
        for currency, scores in strength_scores.items():
            if not scores:
                strength = 50.0  # Neutral
            else:
                # Normalize to 0-100 scale (average change * scaling factor + 50)
                strength = min(100.0, max(0.0, 50.0 + sum(scores) / len(scores) * 10))

            # Compute rank
            all_strengths = [(c, sum(s) / len(s) if s else 0.0) for c, s in strength_scores.items()]
            sorted_strengths = sorted(all_strengths, key=lambda x: x[1], reverse=True)
            rank = next((i + 1 for i, (c, _) in enumerate(sorted_strengths) if c == currency), len(sorted_strengths))

            # Daily change
            change_1d = (strength - 50.0) / len(scores) if scores else 0.0

            results.append(CurrencyStrengthIndicator(
                currency=currency,
                strength_value=strength,
                rank=rank,
                change_1d=change_1d,
                contributing_pairs=pair_changes[currency],
            ))

        return sorted(results, key=lambda x: x.rank)

    # ── FX-03: Correlation Analysis ────────────────────────────────────────

    def compute_correlation(self, inst1: str, inst2: str, window: int = 20) -> float:
        """Compute rolling correlation between two instruments (FX-03)."""
        # In a real implementation, this would use historical price data
        # Here we return a simulated correlation based on instrument types
        inst1, inst2 = inst1.upper(), inst2.upper()

        # Same asset class = higher correlation
        if inst1 in self._pairs and inst2 in self._pairs:
            return 0.85  # FX pairs correlated
        if inst1 in self._commodities and inst2 in self._commodities:
            if "OIL" in inst1 and "OIL" in inst2:
                return 0.92  # Oil contracts highly correlated
            return 0.65  # Different commodities
        # Cross-asset = lower correlation
        return 0.30

    def get_correlation_matrix(self, instruments: list[str]) -> dict[str, dict[str, float]]:
        """Build a correlation matrix for a list of instruments (FX-03)."""
        matrix: dict[str, dict[str, float]] = {}
        for i in instruments:
            matrix[i] = {}
            for j in instruments:
                if i == j:
                    matrix[i][j] = 1.0
                elif j in matrix[i]:
                    matrix[i][j] = matrix[j][i]
                else:
                    corr = self.compute_correlation(i, j)
                    matrix[i][j] = corr
        return matrix

    # ── FX-04: Cross-Asset Risk ────────────────────────────────────────────

    def compute_cross_asset_risk(
        self,
        positions: dict[str, float],  # instrument_id -> notional value
    ) -> CrossAssetRisk:
        """Compute cross-asset risk metrics (FX-04).

        Args:
            positions: Dictionary of instrument positions (notional value)
        """
        if not positions:
            return CrossAssetRisk(
                correlation_matrix={},
                strongest_correlation=("", "", 0.0),
                risk_concentration={},
                currency_exposure={},
            )

        instruments = list(positions.keys())
        correlation_matrix = self.get_correlation_matrix(instruments)

        # Find strongest correlation
        max_corr = (("", "", 0.0),)
        for i in instruments:
            for j in instruments:
                if i != j and i in correlation_matrix and j in correlation_matrix[i]:
                    c = correlation_matrix[i][j]
                    if abs(c) > abs(max_corr[0][2]):
                        max_corr = ((i, j, c),)

        # Risk concentration (normalized by total)
        total = sum(abs(v) for v in positions.values())
        risk_concentration = {
            k: (abs(v) / total * 100) if total else 0.0
            for k, v in positions.items()
        }

        # Currency exposure
        currency_exposure: dict[str, float] = {}
        for inst_id, notional in positions.items():
            inst_id = inst_id.upper()
            if inst_id in self._pairs:
                pair = self._pairs[inst_id]
                currency_exposure[pair.base_currency] = currency_exposure.get(pair.base_currency, 0.0) + notional
                currency_exposure[pair.quote_currency] = currency_exposure.get(pair.quote_currency, 0.0) - notional
            elif inst_id in self._commodities:
                currency_exposure["USD"] = currency_exposure.get("USD", 0.0) + notional

        return CrossAssetRisk(
            correlation_matrix=correlation_matrix,
            strongest_correlation=max_corr[0],
            risk_concentration=risk_concentration,
            currency_exposure=currency_exposure,
        )


# ─────────────────────────────────────────────────────────────────────────────
# Import timedelta locally
# ─────────────────────────────────────────────────────────────────────────────
from datetime import timedelta
