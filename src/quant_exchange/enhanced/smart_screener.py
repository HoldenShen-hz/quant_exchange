"""Smart stock screener service (SCREEN-01 ~ SCREEN-06).

Covers:
- Natural language query processing
- Technical/Fundamental pattern recognition
- Multi-factor quantitative screening
- Custom screener builder
- Hit rate tracking
"""

from __future__ import annotations

import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


# ─────────────────────────────────────────────────────────────────────────────
# Enums
# ─────────────────────────────────────────────────────────────────────────────

class ScreenEntity(str, Enum):
    STOCK = "stock"
    ETF = "etf"
    OPTION = "option"
    FUTURE = "future"
    FOREX = "forex"
    CRYPTO = "crypto"
    ALL = "all"


class ScreenDirection(str, Enum):
    ABOVE = "above"    # greater than threshold
    BELOW = "below"     # less than threshold
    CROSSES_ABOVE = "crosses_above"
    CROSSES_BELOW = "crosses_below"
    EQUALS = "equals"
    WITHIN = "within"   # between two values


class PatternType(str, Enum):
    # Candlestick patterns
    DOJI = "doji"
    HAMMER = "hammer"
    SHOOTING_STAR = "shooting_star"
    ENGULFING_BULLISH = "engulfing_bullish"
    ENGULFING_BEARISH = "engulfing_bearish"
    MORNING_STAR = "morning_star"
    EVENING_STAR = "evening_star"
    # Chart patterns
    DOUBLE_TOP = "double_top"
    DOUBLE_BOTTOM = "double_bottom"
    HEAD_AND_SHOULDERS = "head_and_shoulders"
    TRIANGLE_ASCENDING = "triangle_ascending"
    TRIANGLE_DESCENDING = "triangle_descending"
    FLAG_BULLISH = "flag_bullish"
    FLAG_BEARISH = "flag_bearish"
    # Technical
    BREAKOUT = "breakout"
    BREAKDOWN = "breakdown"
    SUPPORT_BREAK = "support_break"
    RESISTANCE_BREAK = "resistance_break"


@dataclass(slots=True)
class FactorCondition:
    """A single factor condition in a screener."""

    factor: str  # e.g., "rsi", "pe_ratio", "volume_ratio"
    operator: ScreenDirection
    value: float
    params: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class ScreeningResult:
    """Result of a stock screening."""

    result_id: str
    screener_id: str
    instrument_id: str
    match_score: float  # 0.0 - 1.0
    matched_conditions: tuple[str, ...] = field(default_factory=tuple)
    factor_values: dict[str, float] = field(default_factory=dict)
    rank: int = 0
    created_at: str = field(default_factory=_now)


@dataclass(slots=True)
class ScreenerDefinition:
    """A saved screener definition."""

    screener_id: str
    user_id: str
    name: str
    description: str = ""
    entity_type: ScreenEntity = ScreenEntity.STOCK
    conditions: tuple[FactorCondition, ...] = field(default_factory=tuple)
    pattern_filters: tuple[PatternType, ...] = field(default_factory=tuple)
    timeframes: tuple[str, ...] = field(default_factory=lambda: ("1d",))
    universe: tuple[str, ...] = field(default_factory=tuple)  # instrument IDs or groups
    limit: int = 50
    sort_by: str = "match_score"  # match_score, alphabetical, factor_value
    sort_order: str = "desc"
    is_public: bool = False
    hit_count: int = 0
    miss_count: int = 0
    created_at: str = field(default_factory=_now)
    updated_at: str = field(default_factory=_now)


@dataclass(slots=True)
class PatternMatch:
    """A detected chart pattern."""

    match_id: str
    instrument_id: str
    pattern_type: PatternType
    confidence: float  # 0.0 - 1.0
    start_time: str
    end_time: str
    price_start: float
    price_end: float
    breakout_direction: str | None = None  # "bullish", "bearish", None
    projected_target: float | None = None
    stop_loss: float | None = None
    created_at: str = field(default_factory=_now)


@dataclass(slots=True)
class ScreenerPerformance:
    """Track screener hit rate over time."""

    screener_id: str
    total_scans: int = 0
    total_matches: int = 0
    confirmed_winners: int = 0
    confirmed_losers: int = 0
    pending_results: int = 0
    hit_rate_pct: float = 0.0
    avg_gain_pct: float = 0.0
    avg_loss_pct: float = 0.0
    avg_hold_days: float = 0.0
    updated_at: str = field(default_factory=_now)


# ─────────────────────────────────────────────────────────────────────────────
# Factor Definitions
# ─────────────────────────────────────────────────────────────────────────────

TECHNICAL_FACTORS = {
    "rsi": {"name": "RSI (14)", "type": "oscillator", "min": 0, "max": 100},
    "rsi_7": {"name": "RSI (7)", "type": "oscillator", "min": 0, "max": 100},
    "rsi_21": {"name": "RSI (21)", "type": "oscillator", "min": 0, "max": 100},
    "macd": {"name": "MACD", "type": "oscillator", "min": -10, "max": 10},
    "macd_signal": {"name": "MACD Signal", "type": "oscillator", "min": -10, "max": 10},
    "macd_histogram": {"name": "MACD Histogram", "type": "oscillator", "min": -5, "max": 5},
    "ma_50": {"name": "MA 50", "type": "ma", "min": 0, "max": 1000},
    "ma_200": {"name": "MA 200", "type": "ma", "min": 0, "max": 1000},
    "price_above_ma50": {"name": "Price > MA50", "type": "boolean", "min": 0, "max": 1},
    "price_above_ma200": {"name": "Price > MA200", "type": "boolean", "min": 0, "max": 1},
    "volume_ratio": {"name": "Volume / Avg Volume", "type": "ratio", "min": 0, "max": 20},
    "price_change_1d": {"name": "Price Change 1D %", "type": "percent", "min": -50, "max": 50},
    "price_change_5d": {"name": "Price Change 5D %", "type": "percent", "min": -50, "max": 50},
    "price_change_1m": {"name": "Price Change 1M %", "type": "percent", "min": -100, "max": 100},
    "volatility_20d": {"name": "Volatility 20D %", "type": "percent", "min": 0, "max": 100},
    "atr_percent": {"name": "ATR %", "type": "percent", "min": 0, "max": 20},
    "bollinger_upper": {"name": "Bollinger Upper Band", "type": "price", "min": 0, "max": 1000},
    "bollinger_lower": {"name": "Bollinger Lower Band", "type": "price", "min": 0, "max": 1000},
    "bollinger_position": {"name": "Bollinger Position %", "type": "percent", "min": 0, "max": 100},
    "stoch_k": {"name": "Stochastic %K", "type": "oscillator", "min": 0, "max": 100},
    "stoch_d": {"name": "Stochastic %D", "type": "oscillator", "min": 0, "max": 100},
    "adx": {"name": "ADX", "type": "trend", "min": 0, "max": 100},
}

FUNDAMENTAL_FACTORS = {
    "pe_ratio": {"name": "P/E Ratio", "type": "ratio", "min": 0, "max": 100},
    "forward_pe": {"name": "Forward P/E", "type": "ratio", "min": 0, "max": 100},
    "peg_ratio": {"name": "PEG Ratio", "type": "ratio", "min": 0, "max": 10},
    "pb_ratio": {"name": "P/B Ratio", "type": "ratio", "min": 0, "max": 20},
    "ps_ratio": {"name": "P/S Ratio", "type": "ratio", "min": 0, "max": 50},
    "dividend_yield": {"name": "Dividend Yield %", "type": "percent", "min": 0, "max": 20},
    "payout_ratio": {"name": "Payout Ratio %", "type": "percent", "min": 0, "max": 200},
    "debt_to_equity": {"name": "Debt/Equity", "type": "ratio", "min": 0, "max": 10},
    "current_ratio": {"name": "Current Ratio", "type": "ratio", "min": 0, "max": 5},
    "quick_ratio": {"name": "Quick Ratio", "type": "ratio", "min": 0, "max": 5},
    "roe": {"name": "ROE %", "type": "percent", "min": -50, "max": 100},
    "roa": {"name": "ROA %", "type": "percent", "min": -20, "max": 50},
    "gross_margin": {"name": "Gross Margin %", "type": "percent", "min": 0, "max": 100},
    "operating_margin": {"name": "Operating Margin %", "type": "percent", "min": -50, "max": 50},
    "net_margin": {"name": "Net Margin %", "type": "percent", "min": -50, "max": 50},
    "revenue_growth": {"name": "Revenue Growth %", "type": "percent", "min": -50, "max": 100},
    "earnings_growth": {"name": "Earnings Growth %", "type": "percent", "min": -50, "max": 100},
    "market_cap": {"name": "Market Cap (B)", "type": "currency", "min": 0, "max": 10000},
    "revenue": {"name": "Revenue (B)", "type": "currency", "min": 0, "max": 1000},
    "shares_outstanding": {"name": "Shares Outstanding (M)", "type": "shares", "min": 0, "max": 50000},
    "short_float": {"name": "Short Float %", "type": "percent", "min": 0, "max": 50},
    "insider_ownership": {"name": "Insider Ownership %", "type": "percent", "min": 0, "max": 100},
}

ALL_FACTORS = {**TECHNICAL_FACTORS, **FUNDAMENTAL_FACTORS}


# ─────────────────────────────────────────────────────────────────────────────
# Smart Screener Service
# ─────────────────────────────────────────────────────────────────────────────

class SmartScreenerService:
    """Smart stock screener service (SCREEN-01 ~ SCREEN-06).

    Provides:
    - Natural language query processing
    - Technical/Fundamental pattern recognition
    - Multi-factor quantitative screening
    - Custom screener builder
    - Hit rate tracking
    """

    def __init__(self, persistence=None) -> None:
        self.persistence = persistence
        self._screeners: dict[str, ScreenerDefinition] = {}
        self._results: dict[str, list[ScreeningResult]] = {}
        self._patterns: dict[str, list[PatternMatch]] = {}
        self._performance: dict[str, ScreenerPerformance] = {}
        self._watchlists: dict[str, list[str]] = {}  # watchlist_id -> instrument_ids
        self._factor_cache: dict[str, dict[str, float]] = {}  # instrument_id -> factor_values

    # ── Natural Language Query Processing ─────────────────────────────────

    def parse_natural_query(self, query: str) -> ScreenerDefinition:
        """Parse a natural language query into a screener definition."""
        query_lower = query.lower()

        conditions = []
        pattern_filters = []

        # Detect pattern mentions
        pattern_keywords = {
            "doji": PatternType.DOJI,
            "hammer": PatternType.HAMMER,
            "shooting star": PatternType.SHOOTING_STAR,
            "engulfing": PatternType.ENGULFING_BULLISH,
            "morning star": PatternType.MORNING_STAR,
            "evening star": PatternType.EVENING_STAR,
            "double top": PatternType.DOUBLE_TOP,
            "double bottom": PatternType.DOUBLE_BOTTOM,
            "head and shoulders": PatternType.HEAD_AND_SHOULDERS,
            "breakout": PatternType.BREAKOUT,
            "breakdown": PatternType.BREAKDOWN,
        }
        for kw, pattern in pattern_keywords.items():
            if kw in query_lower:
                pattern_filters.append(pattern)

        # RSI conditions
        if "oversold" in query_lower or "rsi below" in query_lower:
            conditions.append(FactorCondition(factor="rsi", operator=ScreenDirection.BELOW, value=30))
        if "overbought" in query_lower or "rsi above" in query_lower:
            conditions.append(FactorCondition(factor="rsi", operator=ScreenDirection.ABOVE, value=70))
        if "rsi" in query_lower:
            import re
            match = re.search(r"rsi (above|below|crosses?|>) (\d+)", query_lower)
            if match:
                op = ScreenDirection.ABOVE if "above" in match.group(1) or ">" in match.group(1) else ScreenDirection.BELOW
                conditions.append(FactorCondition(factor="rsi", operator=op, value=float(match.group(2))))

        # Moving average conditions
        if "above ma50" in query_lower or "price above 50 day" in query_lower:
            conditions.append(FactorCondition(factor="price_above_ma50", operator=ScreenDirection.ABOVE, value=0.5))
        if "above ma200" in query_lower or "price above 200 day" in query_lower:
            conditions.append(FactorCondition(factor="price_above_ma200", operator=ScreenDirection.ABOVE, value=0.5))
        if "below ma50" in query_lower:
            conditions.append(FactorCondition(factor="price_above_ma50", operator=ScreenDirection.BELOW, value=0.5))
        if "below ma200" in query_lower:
            conditions.append(FactorCondition(factor="price_above_ma200", operator=ScreenDirection.BELOW, value=0.5))

        # Volume conditions
        if "high volume" in query_lower or "unusual volume" in query_lower:
            conditions.append(FactorCondition(factor="volume_ratio", operator=ScreenDirection.ABOVE, value=2.0))
        if "low volume" in query_lower:
            conditions.append(FactorCondition(factor="volume_ratio", operator=ScreenDirection.BELOW, value=0.5))

        # Price change conditions
        if "up 5%" in query_lower or "gained 5%" in query_lower:
            conditions.append(FactorCondition(factor="price_change_1d", operator=ScreenDirection.ABOVE, value=5.0))
        if "down 5%" in query_lower or "lost 5%" in query_lower:
            conditions.append(FactorCondition(factor="price_change_1d", operator=ScreenDirection.BELOW, value=-5.0))

        # Fundamental conditions
        if "low pe" in query_lower or "cheap pe" in query_lower:
            conditions.append(FactorCondition(factor="pe_ratio", operator=ScreenDirection.BELOW, value=15.0))
        if "high pe" in query_lower or "expensive pe" in query_lower:
            conditions.append(FactorCondition(factor="pe_ratio", operator=ScreenDirection.ABOVE, value=30.0))
        if "high dividend" in query_lower:
            conditions.append(FactorCondition(factor="dividend_yield", operator=ScreenDirection.ABOVE, value=3.0))
        if "profitable" in query_lower or "positive earnings" in query_lower:
            conditions.append(FactorCondition(factor="net_margin", operator=ScreenDirection.ABOVE, value=0.0))
        if "growing revenue" in query_lower:
            conditions.append(FactorCondition(factor="revenue_growth", operator=ScreenDirection.ABOVE, value=10.0))

        # Sector filters (would need mapping in real impl)
        entity_type = ScreenEntity.STOCK
        if "etf" in query_lower:
            entity_type = ScreenEntity.ETF
        elif "crypto" in query_lower or "bitcoin" in query_lower or "ethereum" in query_lower:
            entity_type = ScreenEntity.CRYPTO

        # Build screener
        screener = ScreenerDefinition(
            screener_id=f"scr:{uuid.uuid4().hex[:12]}",
            user_id="system",
            name=query[:50],
            description=query,
            entity_type=entity_type,
            conditions=tuple(conditions),
            pattern_filters=tuple(pattern_filters),
        )
        return screener

    def screen_from_query(
        self,
        user_id: str,
        query: str,
        universe: list[str] | None = None,
    ) -> list[ScreeningResult]:
        """Run a screen from natural language query."""
        screener = self.parse_natural_query(query)
        screener.user_id = user_id
        if universe:
            screener.universe = tuple(universe)

        # Save and run
        self._screeners[screener.screener_id] = screener
        return self.run_screener(screener.screener_id, universe)

    # ── Screener Management ─────────────────────────────────────────────────

    def create_screener(
        self,
        user_id: str,
        name: str,
        conditions: list[FactorCondition],
        description: str = "",
        entity_type: ScreenEntity = ScreenEntity.STOCK,
        pattern_filters: list[PatternType] | None = None,
        timeframes: list[str] | None = None,
        universe: list[str] | None = None,
        limit: int = 50,
    ) -> ScreenerDefinition:
        """Create a new screener."""
        screener_id = f"scr:{uuid.uuid4().hex[:12]}"
        screener = ScreenerDefinition(
            screener_id=screener_id,
            user_id=user_id,
            name=name,
            description=description,
            entity_type=entity_type,
            conditions=tuple(conditions),
            pattern_filters=tuple(pattern_filters) if pattern_filters else (),
            timeframes=tuple(timeframes) if timeframes else ("1d",),
            universe=tuple(universe) if universe else (),
            limit=limit,
        )
        self._screeners[screener_id] = screener
        self._performance[screener_id] = ScreenerPerformance(screener_id=screener_id)
        return screener

    def get_screener(self, screener_id: str) -> ScreenerDefinition | None:
        """Get a screener by ID."""
        return self._screeners.get(screener_id)

    def get_user_screeners(self, user_id: str, include_public: bool = False) -> list[ScreenerDefinition]:
        """Get all screeners for a user."""
        screeners = [s for s in self._screeners.values() if s.user_id == user_id]
        if include_public:
            screeners.extend([s for s in self._screeners.values() if s.is_public and s.user_id != user_id])
        return screeners

    def update_screener(
        self,
        screener_id: str,
        conditions: list[FactorCondition] | None = None,
        pattern_filters: list[PatternType] | None = None,
        name: str | None = None,
        limit: int | None = None,
    ) -> ScreenerDefinition | None:
        """Update a screener."""
        screener = self._screeners.get(screener_id)
        if not screener:
            return None
        if conditions is not None:
            screener.conditions = tuple(conditions)
        if pattern_filters is not None:
            screener.pattern_filters = tuple(pattern_filters)
        if name is not None:
            screener.name = name
        if limit is not None:
            screener.limit = limit
        screener.updated_at = _now()
        return screener

    def delete_screener(self, screener_id: str) -> bool:
        """Delete a screener."""
        if screener_id in self._screeners:
            del self._screeners[screener_id]
            return True
        return False

    # ── Screener Execution ──────────────────────────────────────────────────

    def run_screener(
        self,
        screener_id: str,
        universe: list[str] | None = None,
    ) -> list[ScreeningResult]:
        """Run a screener and return matching instruments."""
        screener = self._screeners.get(screener_id)
        if not screener:
            return []

        # Determine universe
        if universe:
            instrument_ids = universe
        elif screener.universe:
            instrument_ids = list(screener.universe)
        else:
            # Default universe - would normally come from market data
            instrument_ids = ["AAPL", "MSFT", "GOOGL", "AMZN", "META", "TSLA", "NVDA", "JPM", "V", "JNJ"]

        results = []
        for instrument_id in instrument_ids[:screener.limit * 2]:  # overscan for filtering
            result = self._evaluate_instrument(screener, instrument_id)
            if result and result.match_score > 0:
                results.append(result)

        # Sort and rank
        reverse = screener.sort_order == "desc"
        results.sort(key=lambda r: getattr(r, screener.sort_by), reverse=reverse)
        for i, r in enumerate(results[:screener.limit]):
            r.rank = i + 1

        self._results[screener_id] = results[:screener.limit]
        return results[:screener.limit]

    def _evaluate_instrument(
        self,
        screener: ScreenerDefinition,
        instrument_id: str,
    ) -> ScreeningResult | None:
        """Evaluate a single instrument against screener conditions."""
        # Get or simulate factor values
        factor_values = self._get_simulated_factors(instrument_id)

        matched_conditions = []
        total_score = 0.0

        for cond in screener.conditions:
            factor_val = factor_values.get(cond.factor)
            if factor_val is None:
                continue

            matches = self._check_condition(factor_val, cond)
            if matches:
                matched_conditions.append(cond.factor)
                total_score += 1.0 / len(screener.conditions) if screener.conditions else 1.0

        # Check pattern filters
        instrument_patterns = self._patterns.get(instrument_id, [])
        for pf in screener.pattern_filters:
            if any(p.pattern_type == pf for p in instrument_patterns):
                matched_conditions.append(pf.value)
                total_score += 0.5  # bonus for pattern match

        if not matched_conditions:
            return None

        match_score = min(total_score, 1.0)
        result = ScreeningResult(
            result_id=f"res:{uuid.uuid4().hex[:12]}",
            screener_id=screener.screener_id,
            instrument_id=instrument_id,
            match_score=match_score,
            matched_conditions=tuple(matched_conditions),
            factor_values=factor_values,
        )
        return result

    def _check_condition(self, value: float, cond: FactorCondition) -> bool:
        """Check if a value matches a condition."""
        if cond.operator == ScreenDirection.ABOVE:
            return value > cond.value
        elif cond.operator == ScreenDirection.BELOW:
            return value < cond.value
        elif cond.operator == ScreenDirection.EQUALS:
            return abs(value - cond.value) < 0.001
        elif cond.operator == ScreenDirection.CROSSES_ABOVE:
            return value > cond.value  # simplified
        elif cond.operator == ScreenDirection.CROSSES_BELOW:
            return value < cond.value  # simplified
        elif cond.operator == ScreenDirection.WITHIN:
            return True  # would need min/max from cond.params
        return False

    def _get_simulated_factors(self, instrument_id: str) -> dict[str, float]:
        """Get simulated factor values for an instrument (mock data)."""
        # Simulated data for testing - in real impl would query market data
        import random
        random.seed(hash(instrument_id) % (2**32))

        return {
            "rsi": random.uniform(20, 80),
            "rsi_7": random.uniform(20, 80),
            "macd": random.uniform(-2, 2),
            "volume_ratio": random.uniform(0.5, 3.0),
            "price_above_ma50": 1.0 if random.random() > 0.3 else 0.0,
            "price_above_ma200": 1.0 if random.random() > 0.4 else 0.0,
            "price_change_1d": random.uniform(-5, 5),
            "price_change_5d": random.uniform(-10, 10),
            "pe_ratio": random.uniform(10, 40),
            "dividend_yield": random.uniform(0, 4),
            "market_cap": random.uniform(10, 3000),
            "volume": random.uniform(1000000, 50000000),
        }

    def get_last_results(self, screener_id: str) -> list[ScreeningResult]:
        """Get the most recent screening results."""
        return self._results.get(screener_id, [])

    # ── Pattern Detection ───────────────────────────────────────────────────

    def add_pattern(
        self,
        instrument_id: str,
        pattern_type: PatternType,
        confidence: float,
        start_time: str,
        end_time: str,
        price_start: float,
        price_end: float,
        breakout_direction: str | None = None,
        projected_target: float | None = None,
        stop_loss: float | None = None,
    ) -> PatternMatch:
        """Add a detected pattern for an instrument."""
        match_id = f"pat:{uuid.uuid4().hex[:12]}"
        pattern = PatternMatch(
            match_id=match_id,
            instrument_id=instrument_id,
            pattern_type=pattern_type,
            confidence=confidence,
            start_time=start_time,
            end_time=end_time,
            price_start=price_start,
            price_end=price_end,
            breakout_direction=breakout_direction,
            projected_target=projected_target,
            stop_loss=stop_loss,
        )
        if instrument_id not in self._patterns:
            self._patterns[instrument_id] = []
        self._patterns[instrument_id].append(pattern)
        return pattern

    def get_patterns(
        self,
        instrument_id: str | None = None,
        pattern_type: PatternType | None = None,
    ) -> list[PatternMatch]:
        """Get patterns for instruments."""
        if instrument_id:
            patterns = self._patterns.get(instrument_id, [])
        else:
            patterns = [p for pts in self._patterns.values() for p in pts]

        if pattern_type:
            patterns = [p for p in patterns if p.pattern_type == pattern_type]
        return patterns

    # ── Performance Tracking ─────────────────────────────────────────────────

    def record_result_outcome(
        self,
        screener_id: str,
        instrument_id: str,
        outcome: str,  # "win", "loss", "pending"
        gain_pct: float = 0.0,
        hold_days: int = 0,
    ) -> None:
        """Record the outcome of a screening result."""
        perf = self._performance.get(screener_id)
        if not perf:
            perf = ScreenerPerformance(screener_id=screener_id)
            self._performance[screener_id] = perf

        perf.total_scans += 1
        if outcome == "win":
            perf.confirmed_winners += 1
            perf.avg_gain_pct = (perf.avg_gain_pct * (perf.confirmed_winners - 1) + gain_pct) / perf.confirmed_winners
        elif outcome == "loss":
            perf.confirmed_losers += 1
            perf.avg_loss_pct = (perf.avg_loss_pct * (perf.confirmed_losers - 1) + gain_pct) / perf.confirmed_losers
        else:
            perf.pending_results += 1

        total_confirmed = perf.confirmed_winners + perf.confirmed_losers
        if total_confirmed > 0:
            perf.hit_rate_pct = (perf.confirmed_winners / total_confirmed) * 100

        if hold_days > 0:
            perf.avg_hold_days = (perf.avg_hold_days * (total_confirmed - 1) + hold_days) / max(total_confirmed, 1)

        perf.updated_at = _now()

    def get_performance(self, screener_id: str) -> ScreenerPerformance | None:
        """Get performance metrics for a screener."""
        return self._performance.get(screener_id)

    # ── Watchlists ──────────────────────────────────────────────────────────

    def create_watchlist(self, user_id: str, name: str) -> dict[str, Any]:
        """Create a new watchlist."""
        watchlist_id = f"wl:{uuid.uuid4().hex[:12]}"
        self._watchlists[watchlist_id] = []
        return {
            "watchlist_id": watchlist_id,
            "user_id": user_id,
            "name": name,
            "instrument_ids": [],
        }

    def add_to_watchlist(self, watchlist_id: str, instrument_id: str) -> bool:
        """Add an instrument to a watchlist."""
        if watchlist_id not in self._watchlists:
            return False
        if instrument_id not in self._watchlists[watchlist_id]:
            self._watchlists[watchlist_id].append(instrument_id)
        return True

    def remove_from_watchlist(self, watchlist_id: str, instrument_id: str) -> bool:
        """Remove an instrument from a watchlist."""
        if watchlist_id not in self._watchlists:
            return False
        if instrument_id in self._watchlists[watchlist_id]:
            self._watchlists[watchlist_id].remove(instrument_id)
            return True
        return False

    def get_watchlist(self, watchlist_id: str) -> list[str] | None:
        """Get all instruments in a watchlist."""
        return self._watchlists.get(watchlist_id)
