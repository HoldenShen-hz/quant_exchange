"""Multi-level pre-trade risk controls implementing the documented hierarchy.

Risk evaluation levels (enforced in order):
1. System-level   – kill switch, data staleness, market interruption
2. Order-level    – quantity, notional, price deviation, frequency, duplicate signals
3. Position-level – position notional caps
4. Strategy-level – daily loss, strategy drawdown, consecutive losses
5. Account-level  – gross notional, leverage, drawdown, margin ratio
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

from quant_exchange.core.models import Alert, AlertSeverity, OrderRequest, OrderSide, OrderType, PortfolioSnapshot, RiskDecision, RiskLimits


def utc_now() -> datetime:
    """Return a timezone-aware UTC timestamp."""
    return datetime.now(timezone.utc)


@dataclass
class MarketInterruptionState:
    """Track market interruption state for automatic risk actions."""

    isInterrupted: bool = False
    interruption_start: datetime | None = None
    last_healthy_timestamp: datetime | None = None
    auto_stop_triggered: bool = False
    reason: str = ""


@dataclass
class MarginWarningState:
    """Track margin state for warning and auto-action triggers."""

    is_warned: bool = False
    is_critical: bool = False
    warning_triggered_at: datetime | None = None
    critical_triggered_at: datetime | None = None
    auto_flatten_triggered: bool = False


@dataclass
class SignalDuplicateState:
    """Track signal repetition to detect and limit duplicate signals."""

    last_signal_instrument: str = ""
    last_signal_direction: str = ""
    last_signal_time: datetime | None = None
    repeat_count: int = 0


class RiskEngine:
    """Evaluate order requests against multi-level account and portfolio guardrails.

    Implements the documented risk hierarchy:
    - System-level: kill switch, data staleness, market interruption auto-stop
    - Order-level: quantity, notional, price deviation, frequency, duplicate signals
    - Position-level: position notional caps
    - Strategy-level: daily loss, strategy drawdown, consecutive losses
    - Account-level: gross notional, leverage, drawdown, margin ratio
    """

    def __init__(self, limits: RiskLimits | None = None) -> None:
        self.limits = limits or RiskLimits()
        self.kill_switch_active = False
        self._data_stale = False
        # Order frequency tracking
        self._order_timestamps: list[datetime] = []
        # Strategy-level tracking
        self._strategy_daily_pnl: dict[str, float] = defaultdict(float)
        self._strategy_consecutive_losses: dict[str, int] = defaultdict(int)
        # Market interruption tracking
        self._market_interruption = MarketInterruptionState()
        self._interruption_detection_window = timedelta(minutes=5)
        self._auto_stop_on_interruption = True
        # Margin warning tracking
        self._margin_states: dict[str, MarginWarningState] = defaultdict(MarginWarningState)
        # Duplicate signal tracking
        self._signal_states: dict[str, SignalDuplicateState] = defaultdict(SignalDuplicateState)
        self._max_duplicate_signals = 3
        # Audit trail for risk evaluations
        self._evaluation_log: list[dict[str, Any]] = []
        # Alerts generated
        self.alerts: list[Alert] = []

    def activate_kill_switch(self) -> None:
        """Block all new orders until the switch is released."""

        self.kill_switch_active = True
        self._record_alert(
            code="kill_switch_activated",
            severity=AlertSeverity.EMERGENCY,
            message="Kill switch activated - all trading blocked",
        )

    def release_kill_switch(self) -> None:
        """Re-enable order flow after a manual stop."""

        self.kill_switch_active = False
        self._record_alert(
            code="kill_switch_released",
            severity=AlertSeverity.INFO,
            message="Kill switch released - trading resumed",
        )

    def mark_data_stale(self, stale: bool = True) -> None:
        """Flag data feed as stale to block new orders at system level."""

        self._data_stale = stale
        if stale:
            self._record_alert(
                code="data_stale",
                severity=AlertSeverity.WARNING,
                message="Market data marked as stale - orders will be rejected",
            )

    def mark_market_interrupted(
        self,
        interrupted: bool,
        reason: str = "",
        timestamp: datetime | None = None,
    ) -> bool:
        """Mark market as interrupted and optionally trigger automatic stop.

        Returns True if auto-stop was triggered due to the interruption.
        """
        now = timestamp or utc_now()
        self._market_interruption.isInterrupted = interrupted
        self._market_interruption.reason = reason

        if interrupted:
            self._market_interruption.interruption_start = now
            self._record_alert(
                code="market_interrupted",
                severity=AlertSeverity.CRITICAL,
                message=f"Market interruption detected: {reason}",
            )
            if self._auto_stop_on_interruption and not self.kill_switch_active:
                self.activate_kill_switch()
                self._market_interruption.auto_stop_triggered = True
                return True
        else:
            self._market_interruption.isInterrupted = False
            self._market_interruption.interruption_start = None
            self._market_interruption.auto_stop_triggered = False

        return False

    def update_market_health(self, timestamp: datetime | None = None) -> None:
        """Update market health timestamp to detect interruptions."""
        now = timestamp or utc_now()
        self._market_interruption.last_healthy_timestamp = now

        if self._market_interruption.isInterrupted:
            self._market_interruption.isInterrupted = False
            self._market_interruption.auto_stop_triggered = False
            if self.kill_switch_active:
                self.release_kill_switch()

    def check_market_interruption_auto_stop(self, current_timestamp: datetime | None = None) -> bool:
        """Check if market has been interrupted and trigger auto-stop if needed.

        Returns True if auto-stop was triggered.
        """
        now = current_timestamp or utc_now()
        last_healthy = self._market_interruption.last_healthy_timestamp

        if last_healthy is None:
            self.update_market_health(now)
            return False

        time_since_healthy = now - last_healthy
        if time_since_healthy > self._interruption_detection_window:
            if not self._market_interruption.isInterrupted:
                return self.mark_market_interrupted(
                    interrupted=True,
                    reason=f"No market data for {time_since_healthy}",
                    timestamp=now,
                )

        return False

    def check_margin_warning(
        self,
        instrument_id: str,
        margin_ratio: float,
        position_value: float,
    ) -> tuple[bool, bool]:
        """Check margin ratio and trigger warnings or critical alerts.

        Returns (is_warned, is_critical).
        """
        state = self._margin_states[instrument_id]
        now = utc_now()

        if margin_ratio >= self.limits.margin_block_ratio:
            if not state.is_critical:
                state.is_critical = True
                state.is_warned = True  # Critical implies warning
                state.critical_triggered_at = now
                state.auto_flatten_triggered = True
                self._record_alert(
                    code="margin_critical",
                    severity=AlertSeverity.EMERGENCY,
                    message=f"Margin ratio {margin_ratio:.2%} at liquidation level for {instrument_id}",
                )
            return (state.is_warned, state.is_critical)

        elif margin_ratio >= self.limits.margin_warning_ratio:
            if not state.is_warned:
                state.is_warned = True
                state.warning_triggered_at = now
                self._record_alert(
                    code="margin_warning",
                    severity=AlertSeverity.WARNING,
                    message=f"Margin ratio {margin_ratio:.2%} approaching critical for {instrument_id}",
                )
            return (True, state.is_critical)

        else:
            state.is_warned = False
            state.is_critical = False
            state.warning_triggered_at = None
            state.critical_triggered_at = None
            state.auto_flatten_triggered = False

        return (False, False)

    def check_duplicate_signal(
        self,
        instrument_id: str,
        direction: str,
        max_repeats: int | None = None,
    ) -> tuple[bool, int]:
        """Check if signal is duplicate and increment repeat count.

        Returns (is_duplicate, repeat_count).
        """
        max_repeats = max_repeats or self._max_duplicate_signals
        state = self._signal_states[instrument_id]
        now = utc_now()

        if state.last_signal_instrument == instrument_id and state.last_signal_direction == direction:
            state.repeat_count += 1
            is_duplicate = state.repeat_count >= max_repeats
            if is_duplicate:
                self._record_alert(
                    code="duplicate_signal",
                    severity=AlertSeverity.WARNING,
                    message=f"Duplicate signal detected for {instrument_id}: {direction} (count: {state.repeat_count})",
                )
        else:
            state.last_signal_instrument = instrument_id
            state.last_signal_direction = direction
            state.last_signal_time = now
            state.repeat_count = 1
            is_duplicate = False

        return (is_duplicate, state.repeat_count)

    def reset_duplicate_signal_tracking(self, instrument_id: str | None = None) -> None:
        """Reset duplicate signal tracking for an instrument or all instruments."""
        if instrument_id:
            if instrument_id in self._signal_states:
                del self._signal_states[instrument_id]
        else:
            self._signal_states.clear()

    def record_strategy_pnl(self, strategy_id: str, pnl: float) -> None:
        """Update the strategy-level running PnL for daily loss checks."""

        self._strategy_daily_pnl[strategy_id] += pnl

    def record_strategy_trade_result(self, strategy_id: str, won: bool) -> None:
        """Track consecutive losing trades for auto-flatten logic."""

        if won:
            self._strategy_consecutive_losses[strategy_id] = 0
        else:
            self._strategy_consecutive_losses[strategy_id] += 1

    def check_auto_deleverage(
        self,
        snapshot: PortfolioSnapshot,
        consecutive_losses: int,
        drawdown: float,
    ) -> tuple[bool, str]:
        """Check if auto-deleveraging should be triggered.

        Returns (should_deleverage, reason).
        Triggers when drawdown exceeds threshold OR consecutive losses exceed limit.
        """
        if consecutive_losses >= self.limits.max_consecutive_losses:
            return (True, f"consecutive_losses_limit: {consecutive_losses}")
        if drawdown > self.limits.max_drawdown:
            return (True, f"drawdown_limit: {drawdown:.2%}")
        if snapshot.margin_ratio is not None and snapshot.margin_ratio >= self.limits.margin_warning_ratio:
            return (True, f"margin_warning: {snapshot.margin_ratio:.2%}")
        return (False, "")

    def reset_daily_counters(self) -> None:
        """Reset daily tracking accumulators (call at start of trading day)."""

        self._strategy_daily_pnl.clear()
        self._order_timestamps.clear()
        self._margin_states.clear()
        self._signal_states.clear()

    def evaluate_order(
        self,
        request: OrderRequest,
        *,
        price: float,
        current_position_qty: float,
        snapshot: PortfolioSnapshot,
        margin_ratio: float | None = None,
        check_duplicate_signal: bool = False,
        signal_direction: str | None = None,
    ) -> RiskDecision:
        """Approve or reject an order using multi-level risk hierarchy.

        Evaluation order: system → order → position → strategy → account.
        """

        reasons: list[str] = []
        now = datetime.now(timezone.utc)

        # --- Level 0: System-level safeguards ---
        if self.kill_switch_active:
            reasons.append("kill_switch_active")
        if self._data_stale:
            reasons.append("market_data_stale")
        if self._market_interruption.isInterrupted:
            reasons.append("market_interrupted")

        # --- Level 1: Order-level checks ---
        notional = abs(request.quantity * price)
        if request.quantity <= 0:
            reasons.append("quantity_must_be_positive")
        if request.quantity > self.limits.max_single_order_quantity:
            reasons.append("single_order_quantity_limit")
        if notional > self.limits.max_order_notional:
            reasons.append("order_notional_limit")
        # Price deviation check: for market orders with a submitted price estimate,
        # reject if the price deviates too far from the current market reference
        if (
            request.price is not None
            and price > 0
            and request.order_type == OrderType.MARKET
        ):
            deviation = abs(request.price - price) / price
            if deviation > self.limits.max_price_deviation:
                reasons.append("price_deviation_limit")
        # Order frequency throttling
        cutoff = now - timedelta(minutes=1)
        self._order_timestamps = [t for t in self._order_timestamps if t > cutoff]
        if len(self._order_timestamps) >= self.limits.max_order_frequency:
            reasons.append("order_frequency_limit")
        self._order_timestamps.append(now)

        # Duplicate signal check
        if check_duplicate_signal and signal_direction:
            is_dup, count = self.check_duplicate_signal(
                request.instrument_id,
                signal_direction,
            )
            if is_dup:
                reasons.append("duplicate_signal_limit")

        # --- Level 2: Position-level checks ---
        projected_qty = current_position_qty + (
            request.quantity if request.side == OrderSide.BUY else -request.quantity
        )
        if abs(projected_qty * price) > self.limits.max_position_notional:
            reasons.append("position_notional_limit")

        # --- Level 3: Strategy-level checks ---
        strategy_id = request.strategy_id
        if strategy_id != "manual":
            daily_loss = self._strategy_daily_pnl.get(strategy_id, 0.0)
            if daily_loss < -self.limits.max_strategy_daily_loss:
                reasons.append("strategy_daily_loss_limit")
            consec = self._strategy_consecutive_losses.get(strategy_id, 0)
            if consec >= self.limits.max_consecutive_losses:
                reasons.append("strategy_consecutive_losses_limit")

        # --- Level 4: Account-level checks ---
        projected_gross = snapshot.gross_exposure + notional
        if projected_gross > self.limits.max_gross_notional:
            reasons.append("gross_notional_limit")
        projected_equity = max(snapshot.equity, 1.0)
        projected_leverage = projected_gross / projected_equity
        if projected_leverage > self.limits.max_leverage:
            reasons.append("leverage_limit")
        if snapshot.drawdown > self.limits.max_drawdown:
            reasons.append("drawdown_limit")

        # Margin ratio check (if provided)
        if margin_ratio is not None:
            is_warned, is_critical = self.check_margin_warning(
                request.instrument_id,
                margin_ratio,
                notional,
            )
            if is_critical:
                reasons.append("margin_critical")

        decision = RiskDecision(approved=not reasons, reasons=tuple(reasons))
        self._evaluation_log.append({
            "timestamp": now.isoformat(),
            "client_order_id": request.client_order_id,
            "strategy_id": strategy_id,
            "approved": decision.approved,
            "reasons": list(reasons),
            "notional": notional,
        })
        return decision

    def _record_alert(
        self,
        code: str,
        severity: AlertSeverity,
        message: str,
        **context,
    ) -> None:
        """Record an alert for monitoring and notification."""
        alert = Alert(
            code=code,
            severity=severity,
            message=message,
            timestamp=utc_now(),
            context=context,
        )
        self.alerts.append(alert)

    @property
    def evaluation_log(self) -> list[dict[str, Any]]:
        """Return the audit trail of all risk evaluations."""

        return list(self._evaluation_log)

    @property
    def market_interruption_state(self) -> MarketInterruptionState:
        """Return the current market interruption state."""
        return self._market_interruption

    @property
    def margin_warning_states(self) -> dict[str, MarginWarningState]:
        """Return margin warning states for all instruments."""
        return dict(self._margin_states)

    # ── RK-06: Black Swan Protection ─────────────────────────────────────────

    def calculate_cornish_fisher_var(
        self,
        returns: list[float],
        confidence: float = 0.95,
    ) -> float:
        """Calculate Value-at-Risk using the Cornish-Fisher expansion (RK-06).

        Adjusts the standard normal quantile for sample skewness and excess
        kurtosis, providing a more accurate VaR estimate for fat-tailed
        (leptokurtic) return distributions common during market crises.

        Uses the formula:
          z_cf = z + (z^2 - 1)*S/6 + (z^3 - 3z)*K/24 - (2z^3 - 5z)*S^2/36

        where z = normal quantile, S = skewness, K = excess kurtosis.
        """
        import math
        if len(returns) < 10:
            return 0.0

        n = len(returns)
        mean_r = sum(returns) / n
        var_r = sum((r - mean_r) ** 2 for r in returns) / n
        std_r = var_r ** 0.5

        if std_r == 0:
            return 0.0

        # Skewness: E[(r - mu)^3] / sigma^3
        skew = sum((r - mean_r) ** 3 for r in returns) / (n * std_r ** 3)
        # Excess kurtosis: E[(r - mu)^4] / sigma^4 - 3
        kurt = sum((r - mean_r) ** 4 for r in returns) / (n * std_r ** 4) - 3

        # Normal quantile for confidence level
        z = self._normal_quantile(confidence)

        # Cornish-Fisher adjustment
        z_cf = (
            z
            + (z * z - 1) * skew / 6
            + (z * z * z - 3 * z) * kurt / 24
            - (2 * z * z * z - 5 * z) * skew * skew / 36
        )

        var_pct = -(mean_r + z_cf * std_r)
        return round(var_pct, 6)

    def calculate_expected_shortfall(
        self,
        returns: list[float],
        confidence: float = 0.95,
    ) -> float:
        """Calculate Expected Shortfall (CVaR/ES) at the given confidence level (RK-06).

        ES is the mean of all losses beyond VaR, providing a more complete
        picture of tail risk than VaR alone.
        """
        import math
        if len(returns) < 5:
            return 0.0

        sorted_returns = sorted(returns)
        n = len(sorted_returns)
        cutoff_idx = int(n * (1 - confidence))
        cutoff_idx = max(1, cutoff_idx)

        # Tail losses (the worst `cutoff_idx` returns, expressed as losses)
        tail_losses = [-r for r in sorted_returns[:cutoff_idx] if r < 0]

        if not tail_losses:
            return 0.0

        es = sum(tail_losses) / len(tail_losses)
        return round(es, 6)

    def check_circuit_breakers(
        self,
        price_history: list[float],
        symbol: str,
        *,
        level1_threshold: float = 0.05,   # 5% drop → Level 1 halt warning
        level2_threshold: float = 0.10,   # 10% drop → Level 2 halt
        level3_threshold: float = 0.20,   # 20% drop → Level 3 market-wide halt
    ) -> dict[str, Any]:
        """Check circuit breaker levels based on today's opening price (RK-06).

        Returns the triggered circuit breaker level (if any) and recommended actions.
        Standard Chinese/US futures circuit breaker rules:
        - Level 1 (5% down): 15-minute cooling-off period
        - Level 2 (10% down): 15-minute halt, then 5-min reopen
        - Level 3 (20% down): Full market halt for remainder of session
        """
        import math
        if len(price_history) < 2:
            return {"level": 0, "triggered": False, "pct_decline": 0.0}

        open_price = price_history[0]
        current_price = price_history[-1]

        if open_price <= 0:
            return {"level": 0, "triggered": False, "pct_decline": 0.0}

        pct_decline = (open_price - current_price) / open_price

        level = 0
        if pct_decline >= level3_threshold:
            level = 3
        elif pct_decline >= level2_threshold:
            level = 2
        elif pct_decline >= level1_threshold:
            level = 1

        triggered = level > 0

        if triggered:
            self._record_alert(
                code=f"circuit_breaker_L{level}",
                severity=AlertSeverity.EMERGENCY if level == 3 else AlertSeverity.CRITICAL,
                message=f"Circuit breaker Level {level} triggered for {symbol}: decline {pct_decline:.2%}",
                context={
                    "symbol": symbol,
                    "level": level,
                    "pct_decline": round(pct_decline, 6),
                    "open_price": open_price,
                    "current_price": current_price,
                    "threshold_L1": level1_threshold,
                    "threshold_L2": level2_threshold,
                    "threshold_L3": level3_threshold,
                },
            )

        actions = {1: "cooling_off_period", 2: "15min_halt", 3: "full_halt"}
        return {
            "level": level,
            "triggered": triggered,
            "pct_decline": round(pct_decline, 6),
            "symbol": symbol,
            "open_price": open_price,
            "current_price": current_price,
            "recommended_action": actions.get(level, "continue_trading"),
        }

    def detect_correlation_spike(
        self,
        returns_matrix: dict[str, list[float]],
        *,
        spike_threshold: float = 0.70,
        window: int = 20,
    ) -> dict[str, Any]:
        """Detect when cross-instrument correlations spike above a threshold (RK-06).

        During market stress (black swan events), correlations tend to 1.0
        as all assets sell off simultaneously. This detects such spikes by
        computing the average pairwise correlation in a rolling window and
        flagging when it exceeds spike_threshold.

        Returns average correlation, spike status, and contributing pairs.
        """
        import math
        tickers = list(returns_matrix.keys())
        if len(tickers) < 2:
            return {"spike_detected": False, "avg_correlation": 0.0, "pairs": []}

        # Build correlation matrix using latest `window` returns
        corrs: list[float] = []
        spike_pairs: list[dict[str, Any]] = []

        for i in range(len(tickers)):
            for j in range(i + 1, len(tickers)):
                tki, tkj = tickers[i], tickers[j]
                ri = returns_matrix[tki][-window:]
                rj = returns_matrix[tkj][-window:]

                if len(ri) < window or len(rj) < window:
                    continue

                corr = self._pearson_corr(ri, rj)
                corrs.append(corr)

                if corr >= spike_threshold:
                    spike_pairs.append({
                        "pair": f"{tki}/{tkj}",
                        "correlation": round(corr, 4),
                    })

        if not corrs:
            return {"spike_detected": False, "avg_correlation": 0.0, "pairs": []}

        avg_corr = sum(corrs) / len(corrs)
        spike_detected = avg_corr >= spike_threshold or any(c >= spike_threshold for c in corrs)

        if spike_detected:
            self._record_alert(
                code="correlation_spike",
                severity=AlertSeverity.CRITICAL,
                message=f"Correlation spike detected: avg={avg_corr:.3f}, {len(spike_pairs)} pairs above {spike_threshold:.0%}",
                context={"avg_correlation": round(avg_corr, 4), "spike_pairs": spike_pairs},
            )

        return {
            "spike_detected": spike_detected,
            "avg_correlation": round(avg_corr, 4),
            "max_correlation": round(max(corrs), 4) if corrs else 0.0,
            "threshold": spike_threshold,
            "window": window,
            "spike_pairs": spike_pairs,
        }

    def calculate_conditional_drawdown_risk(
        self,
        equity_curve: list[float],
        confidence: float = 0.95,
    ) -> float:
        """Calculate Conditional Drawdown at Risk (CDaR) (RK-06).

        Average expected drawdown conditional on being in a drawdown state
        at the given confidence level.
        """
        import math
        if len(equity_curve) < 10:
            return 0.0

        # Compute drawdown series
        highs = [equity_curve[0]]
        for p in equity_curve[1:]:
            highs.append(max(highs[-1], p))

        drawdowns = [(highs[i] - equity_curve[i]) / highs[i] if highs[i] > 0 else 0.0 for i in range(len(equity_curve))]
        drawdowns = [d for d in drawdowns if d > 0]

        if not drawdowns:
            return 0.0

        sorted_dd = sorted(drawdowns, reverse=True)
        n = len(sorted_dd)
        cutoff_idx = max(1, int(n * (1 - confidence)))
        cdar = sum(sorted_dd[:cutoff_idx]) / len(sorted_dd[:cutoff_idx]) if sorted_dd[:cutoff_idx] else 0.0
        return round(cdar, 6)

    # ── Internal Helpers ──────────────────────────────────────────────────────

    @staticmethod
    def _normal_quantile(p: float) -> float:
        """Approximate inverse normal CDF (quantile) using rational approximation."""
        import math
        if p <= 0:
            return -float("inf")
        if p >= 1:
            return float("inf")
        if p < 0.5:
            t = (-2.0 * math.log(p)) ** 0.5
            # Rational approximation numerator and denominator for p < 0.5
            a = 0.000248 + 0.036348
            b = -0.020523
            c = 0.128379
            d = -0.218519
            num = ((a * t + b) * t + c) * t + d
            e = t + 0.968978
            f = 0.303447
            g = 1.260083
            h = 1.265512
            den = ((e * t + f) * t + g) / h
            z = -(num / den)
        else:
            t = (-2.0 * math.log(1.0 - p)) ** 0.5
            # Rational approximation numerator and denominator for p >= 0.5
            a = -0.000248 + 0.036348
            b = -0.020523
            c = 0.128379
            d = -0.218519
            num = ((a * t + b) * t + c) * t + d
            e = t + 0.968978
            f = 0.303447
            g = 1.260083
            h = 1.265512
            den = ((e * t + f) * t + g) / h
            z = num / den
        return z

    @staticmethod
    def _pearson_corr(x: list[float], y: list[float]) -> float:
        """Compute Pearson correlation coefficient between two series."""
        import math
        n = len(x)
        if n == 0 or n != len(y):
            return 0.0
        mean_x = sum(x) / n
        mean_y = sum(y) / n
        num = sum((xi - mean_x) * (yi - mean_y) for xi, yi in zip(x, y))
        den_x = sum((xi - mean_x) ** 2 for xi in x) ** 0.5
        den_y = sum((yi - mean_y) ** 2 for yi in y) ** 0.5
        if den_x == 0 or den_y == 0:
            return 0.0
        return num / (den_x * den_y)


# ─────────────────────────────────────────────────────────────────────────────
# RK-03: Instrument-Level Risk Controls (Liquidity & Volatility Filtering)
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class InstrumentRiskState:
    """Risk state for an individual instrument."""

    instrument_id: str
    current_volatility: float = 0.0
    average_volatility: float = 0.0
    volatility_rank: float = 0.0  # percentile rank
    average_daily_volume: float = 0.0
    current_volume: float = 0.0
    volume_rank: float = 0.0  # percentile rank
    liquidity_score: float = 1.0  # 0-1, computed from volume and spread
    is_tradeable: bool = True
    block_reason: str = ""
    last_updated: datetime | None = None


class InstrumentRiskFilter:
    """RK-03: Instrument-level risk filtering based on liquidity and volatility.

    Provides:
    - Volatility-based trading restrictions
    - Liquidity-based order sizing limits
    - Per-instrument risk state tracking
    """

    def __init__(
        self,
        max_volatility: float = 0.50,
        min_volume: float = 10000.0,
        min_liquidity_score: float = 0.1,
    ) -> None:
        self.max_volatility = max_volatility
        self.min_volume = min_volume
        self.min_liquidity_score = min_liquidity_score
        self._instrument_states: dict[str, InstrumentRiskState] = {}
        self._volume_history: dict[str, list[float]] = defaultdict(list)
        self._volatility_history: dict[str, list[float]] = defaultdict(list)

    def update_instrument_data(
        self,
        instrument_id: str,
        current_price: float,
        previous_price: float,
        volume: float,
        high: float | None = None,
        low: float | None = None,
    ) -> InstrumentRiskState:
        """Update risk state for an instrument based on latest market data."""
        state = self._instrument_states.get(instrument_id)
        if state is None:
            state = InstrumentRiskState(instrument_id=instrument_id)
            self._instrument_states[instrument_id] = state

        # Calculate volatility (simplified: daily range / price)
        if high is not None and low is not None and current_price > 0:
            state.current_volatility = (high - low) / current_price
        elif previous_price > 0:
            state.current_volatility = abs(current_price - previous_price) / previous_price

        # Update volatility history (keep 20 periods)
        self._volatility_history[instrument_id].append(state.current_volatility)
        if len(self._volatility_history[instrument_id]) > 20:
            self._volatility_history[instrument_id] = self._volatility_history[instrument_id][-20:]

        # Calculate average volatility
        if self._volatility_history[instrument_id]:
            state.average_volatility = sum(self._volatility_history[instrument_id]) / len(self._volatility_history[instrument_id])

        # Calculate volatility rank (percentile)
        if len(self._volatility_history[instrument_id]) >= 5:
            sorted_vols = sorted(self._volatility_history[instrument_id])
            rank_idx = bisect_left(sorted_vols, state.current_volatility)
            state.volatility_rank = rank_idx / len(sorted_vols)

        # Update volume history (keep 20 periods)
        self._volume_history[instrument_id].append(volume)
        if len(self._volume_history[instrument_id]) > 20:
            self._volume_history[instrument_id] = self._volume_history[instrument_id][-20:]

        # Calculate volume rank
        if len(self._volume_history[instrument_id]) >= 5:
            sorted_volumes = sorted(self._volume_history[instrument_id])
            rank_idx = bisect_left(sorted_volumes, volume)
            state.volume_rank = rank_idx / len(sorted_volumes)

        # Calculate average daily volume
        if self._volume_history[instrument_id]:
            state.average_daily_volume = sum(self._volume_history[instrument_id]) / len(self._volume_history[instrument_id])

        state.current_volume = volume

        # Compute liquidity score (0-1, higher is better)
        vol_score = max(0.0, 1.0 - state.volatility_rank)  # Lower volatility = higher score
        volume_score = state.volume_rank  # Higher volume = higher score
        state.liquidity_score = 0.6 * volume_score + 0.4 * vol_score

        # Determine if instrument is tradeable
        state.is_tradeable = True
        state.block_reason = ""

        if state.current_volatility > self.max_volatility:
            state.is_tradeable = False
            state.block_reason = f"volatility_exceeded:{state.current_volatility:.2%}>{self.max_volatility:.2%}"

        if volume < self.min_volume:
            state.is_tradeable = False
            state.block_reason = f"volume_below_min:{volume:.0f}<{self.min_volume:.0f}"

        if state.liquidity_score < self.min_liquidity_score:
            state.is_tradeable = False
            state.block_reason = f"liquidity_score_low:{state.liquidity_score:.2f}<{self.min_liquidity_score:.2f}"

        state.last_updated = utc_now()
        return state

    def check_instrument_tradeable(
        self,
        instrument_id: str,
        order_notional: float | None = None,
    ) -> tuple[bool, str]:
        """Check if an instrument can be traded.

        Returns (is_tradeable, reason).
        """
        state = self._instrument_states.get(instrument_id)
        if state is None:
            return (True, "")  # Unknown instruments are allowed

        if not state.is_tradeable:
            return (False, state.block_reason)

        # Additional check: order notional vs liquidity
        if order_notional is not None and state.current_volume > 0:
            max_order_pct = 0.05  # Max 5% of daily volume per order
            if order_notional / state.average_daily_volume > max_order_pct:
                return (False, f"order_size_exceeds_liquidity:{order_notional/state.average_daily_volume:.1%}>5%")

        return (True, "")

    def get_instrument_state(self, instrument_id: str) -> InstrumentRiskState | None:
        """Get current risk state for an instrument."""
        return self._instrument_states.get(instrument_id)

    def get_all_blocked_instruments(self) -> list[tuple[str, str]]:
        """Get list of all blocked instruments and reasons."""
        return [
            (iid, state.block_reason)
            for iid, state in self._instrument_states.items()
            if not state.is_tradeable
        ]

    def get_volatility_rank(self, instrument_id: str) -> float:
        """Get the volatility rank for an instrument (0-1, higher = more volatile)."""
        state = self._instrument_states.get(instrument_id)
        return state.volatility_rank if state else 0.0


# ─────────────────────────────────────────────────────────────────────────────
# RK-07: Enhanced Risk Audit Trail with Reason Codes
# ─────────────────────────────────────────────────────────────────────────────

class RiskReasonCode:
    """Standardized reason codes for risk rejections (RK-07)."""

    # System-level codes (S*)
    S_KILL_SWITCH = "S001"  # Kill switch active
    S_DATA_STALE = "S002"  # Market data stale
    S_MARKET_INTERRUPTED = "S003"  # Market interrupted
    S_SYSTEM_OVERLOAD = "S004"  # System overload

    # Order-level codes (O*)
    O_QUANTITY_INVALID = "O001"  # Quantity <= 0
    O_QUANTITY_EXCEEDED = "O002"  # Single order qty limit
    O_NOTIONAL_EXCEEDED = "O003"  # Order notional limit
    O_PRICE_DEVIATION = "O004"  # Price deviation too large
    O_FREQUENCY_LIMIT = "O005"  # Order frequency exceeded
    O_DUPLICATE_SIGNAL = "O006"  # Duplicate signal detected
    O_SPREAD_TOO_WIDE = "O007"  # Bid-ask spread too wide

    # Position-level codes (P*)
    P_NOTIONAL_EXCEEDED = "P001"  # Position notional limit
    P_CONCENTRATION_HIGH = "P002"  # Position concentration too high

    # Strategy-level codes (T*) - T for trading/strategy
    T_DAILY_LOSS_LIMIT = "T001"  # Strategy daily loss limit
    T_CONSECUTIVE_LOSSES = "T002"  # Consecutive losses limit
    T_STRATEGY_DISABLED = "T003"  # Strategy disabled

    # Account-level codes (A*)
    A_GROSS_NOTIONAL = "A001"  # Gross notional limit
    A_LEVERAGE_EXCEEDED = "A002"  # Leverage limit
    A_DRAWDOWN_EXCEEDED = "A003"  # Drawdown limit
    A_MARGIN_WARNING = "A004"  # Margin warning
    A_MARGIN_CRITICAL = "A005"  # Margin critical

    # Instrument-level codes (I*) - RK-03
    I_VOLATILITY_HIGH = "I001"  # Volatility too high
    I_VOLUME_LOW = "I002"  # Volume too low
    I_LIQUIDITY_LOW = "I003"  # Liquidity score too low
    I_ORDER_SIZE_LARGE = "I004"  # Order size exceeds liquidity

    # Black swan codes (B*)
    B_BLACK_SWAN_EVENT = "B001"  # Black swan event detected
    B_CORRELATION_SPIKE = "B002"  # Correlation spike
    B_VIX_SPIKE = "B003"  # VIX above threshold


@dataclass
class RiskAuditEntry:
    """Enhanced audit trail entry with standardized reason codes (RK-07)."""

    entry_id: str
    timestamp: datetime
    client_order_id: str
    instrument_id: str
    strategy_id: str
    decision: str  # "approved" or "rejected"
    primary_reason_code: str
    all_reason_codes: tuple[str, ...]
    reason_descriptions: tuple[str, ...]
    notional: float
    quantity: float
    price: float
    margin_ratio: float | None
    snapshot_equity: float
    snapshot_leverage: float
    evaluation_time_ms: float
    risk_level: str  # "system", "order", "position", "strategy", "account", "instrument"


@dataclass
class RiskAuditSummary:
    """Summary statistics for risk audit (RK-07)."""

    total_evaluations: int
    total_rejections: int
    rejection_rate: float
    rejections_by_code: dict[str, int]
    rejections_by_level: dict[str, int]
    average_evaluation_time_ms: float
    period_start: datetime
    period_end: datetime


class RiskAuditLogger:
    """RK-07: Enhanced risk audit trail with standardized reason codes.

    Provides:
    - Standardized reason code classification
    - Detailed audit entries
    - Rejection analysis and reporting
    - Compliance-friendly audit format
    """

    REASON_CODE_MAP: dict[str, tuple[str, str]] = {
        # System-level
        "kill_switch_active": (RiskReasonCode.S_KILL_SWITCH, "Kill switch is active"),
        "market_data_stale": (RiskReasonCode.S_DATA_STALE, "Market data is stale"),
        "market_interrupted": (RiskReasonCode.S_MARKET_INTERRUPTED, "Market is interrupted"),
        # Order-level
        "quantity_must_be_positive": (RiskReasonCode.O_QUANTITY_INVALID, "Order quantity must be positive"),
        "single_order_quantity_limit": (RiskReasonCode.O_QUANTITY_EXCEEDED, "Single order quantity exceeds limit"),
        "order_notional_limit": (RiskReasonCode.O_NOTIONAL_EXCEEDED, "Order notional exceeds limit"),
        "price_deviation_limit": (RiskReasonCode.O_PRICE_DEVIATION, "Price deviation exceeds limit"),
        "order_frequency_limit": (RiskReasonCode.O_FREQUENCY_LIMIT, "Order frequency limit exceeded"),
        "duplicate_signal_limit": (RiskReasonCode.O_DUPLICATE_SIGNAL, "Duplicate signal limit exceeded"),
        # Position-level
        "position_notional_limit": (RiskReasonCode.P_NOTIONAL_EXCEEDED, "Position notional exceeds limit"),
        # Strategy-level
        "strategy_daily_loss_limit": (RiskReasonCode.T_DAILY_LOSS_LIMIT, "Strategy daily loss limit exceeded"),
        "strategy_consecutive_losses_limit": (RiskReasonCode.T_CONSECUTIVE_LOSSES, "Strategy consecutive losses limit"),
        # Account-level
        "gross_notional_limit": (RiskReasonCode.A_GROSS_NOTIONAL, "Gross notional exceeds limit"),
        "leverage_limit": (RiskReasonCode.A_LEVERAGE_EXCEEDED, "Leverage exceeds limit"),
        "drawdown_limit": (RiskReasonCode.A_DRAWDOWN_EXCEEDED, "Drawdown exceeds limit"),
        "margin_warning": (RiskReasonCode.A_MARGIN_WARNING, "Margin ratio at warning level"),
        "margin_critical": (RiskReasonCode.A_MARGIN_CRITICAL, "Margin ratio at critical level"),
        # Instrument-level (RK-03)
        "volatility_exceeded": (RiskReasonCode.I_VOLATILITY_HIGH, "Instrument volatility exceeds threshold"),
        "volume_below_min": (RiskReasonCode.I_VOLUME_LOW, "Instrument volume below minimum"),
        "liquidity_score_low": (RiskReasonCode.I_LIQUIDITY_LOW, "Instrument liquidity score too low"),
        "order_size_exceeds_liquidity": (RiskReasonCode.I_ORDER_SIZE_LARGE, "Order size exceeds liquidity"),
    }

    def __init__(self) -> None:
        self._entries: list[RiskAuditEntry] = []
        self._max_entries = 10000

    def _get_reason_code(self, reason: str) -> tuple[str, str]:
        """Get standardized reason code and description for a reason string."""
        for key, (code, desc) in self.REASON_CODE_MAP.items():
            if key in reason:
                return (code, desc)
        # Default code for unknown reasons
        return ("X999", f"Unknown reason: {reason}")

    def _classify_level(self, reason: str) -> str:
        """Classify the risk level of a rejection."""
        if reason.startswith("S"):
            return "system"
        elif reason.startswith("O"):
            return "order"
        elif reason.startswith("P"):
            return "position"
        elif reason.startswith("T"):
            return "strategy"
        elif reason.startswith("A"):
            return "account"
        elif reason.startswith("I"):
            return "instrument"
        elif reason.startswith("B"):
            return "blackswan"
        return "unknown"

    def log_evaluation(
        self,
        decision: "RiskDecision",
        request: "OrderRequest",
        price: float,
        snapshot: "PortfolioSnapshot",
        margin_ratio: float | None = None,
        evaluation_time_ms: float = 0.0,
    ) -> RiskAuditEntry:
        """Log a risk evaluation decision."""
        import uuid

        reason_codes = []
        reason_descriptions = []
        for reason in decision.reasons:
            code, desc = self._get_reason_code(reason)
            reason_codes.append(code)
            reason_descriptions.append(desc)

        primary_code = reason_codes[0] if reason_codes else ""
        primary_level = self._classify_level(primary_code) if primary_code else "unknown"

        entry = RiskAuditEntry(
            entry_id=f"raudit:{uuid.uuid4().hex[:12]}",
            timestamp=utc_now(),
            client_order_id=request.client_order_id,
            instrument_id=request.instrument_id,
            strategy_id=request.strategy_id,
            decision="approved" if decision.approved else "rejected",
            primary_reason_code=primary_code,
            all_reason_codes=tuple(reason_codes),
            reason_descriptions=tuple(reason_descriptions),
            notional=abs(request.quantity * price),
            quantity=request.quantity,
            price=price,
            margin_ratio=margin_ratio,
            snapshot_equity=snapshot.equity,
            snapshot_leverage=snapshot.leverage,
            evaluation_time_ms=evaluation_time_ms,
            risk_level=primary_level,
        )

        self._entries.append(entry)
        if len(self._entries) > self._max_entries:
            self._entries = self._entries[-self._max_entries:]

        return entry

    def get_audit_entries(
        self,
        strategy_id: str | None = None,
        instrument_id: str | None = None,
        limit: int = 100,
    ) -> list[RiskAuditEntry]:
        """Get audit entries with optional filtering."""
        entries = self._entries

        if strategy_id:
            entries = [e for e in entries if e.strategy_id == strategy_id]
        if instrument_id:
            entries = [e for e in entries if e.instrument_id == instrument_id]

        return entries[-limit:]

    def get_rejection_summary(
        self,
        period_start: datetime | None = None,
        period_end: datetime | None = None,
    ) -> RiskAuditSummary:
        """Get summary statistics of rejections for a period."""
        entries = self._entries

        if period_start:
            entries = [e for e in entries if e.timestamp >= period_start]
        if period_end:
            entries = [e for e in entries if e.timestamp <= period_end]

        rejected = [e for e in entries if e.decision == "rejected"]
        rejections_by_code: dict[str, int] = defaultdict(int)
        rejections_by_level: dict[str, int] = defaultdict(int)

        for entry in rejected:
            for code in entry.all_reason_codes:
                rejections_by_code[code] += 1
            rejections_by_level[entry.risk_level] += 1

        avg_eval_time = (
            sum(e.evaluation_time_ms for e in entries) / len(entries)
            if entries else 0.0
        )

        return RiskAuditSummary(
            total_evaluations=len(entries),
            total_rejections=len(rejected),
            rejection_rate=len(rejected) / len(entries) if entries else 0.0,
            rejections_by_code=dict(rejections_by_code),
            rejections_by_level=dict(rejections_by_level),
            average_evaluation_time_ms=avg_eval_time,
            period_start=period_start or (entries[0].timestamp if entries else utc_now()),
            period_end=period_end or (entries[-1].timestamp if entries else utc_now()),
        )

    def get_top_rejection_reasons(self, n: int = 10) -> list[tuple[str, int]]:
        """Get the top N rejection reasons by frequency."""
        summary = self.get_rejection_summary()
        sorted_reasons = sorted(
            summary.rejections_by_code.items(),
            key=lambda x: x[1],
            reverse=True,
        )
        return sorted_reasons[:n]

    def export_audit_csv(self, limit: int | None = None) -> str:
        """Export audit entries as CSV for compliance reporting."""
        import csv
        import io

        entries = self._entries[-limit:] if limit else self._entries

        output = io.StringIO()
        writer = csv.writer(output)

        # Header
        writer.writerow([
            "entry_id", "timestamp", "client_order_id", "instrument_id", "strategy_id",
            "decision", "primary_reason_code", "all_reason_codes", "reason_descriptions",
            "notional", "quantity", "price", "margin_ratio", "snapshot_equity",
            "snapshot_leverage", "evaluation_time_ms", "risk_level",
        ])

        # Data
        for entry in entries:
            writer.writerow([
                entry.entry_id,
                entry.timestamp.isoformat(),
                entry.client_order_id,
                entry.instrument_id,
                entry.strategy_id,
                entry.decision,
                entry.primary_reason_code,
                "|".join(entry.all_reason_codes),
                "|".join(entry.reason_descriptions),
                entry.notional,
                entry.quantity,
                entry.price,
                entry.margin_ratio,
                entry.snapshot_equity,
                entry.snapshot_leverage,
                entry.evaluation_time_ms,
                entry.risk_level,
            ])

        return output.getvalue()


# Import bisect_left for volatility rank calculation
from bisect import bisect_left
