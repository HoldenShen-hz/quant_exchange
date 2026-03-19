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
