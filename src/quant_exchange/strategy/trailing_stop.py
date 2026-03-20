"""Trailing-stop strategy: track peak price and exit when price drops by trail pct."""

from __future__ import annotations

from quant_exchange.core.models import StrategySignal
from quant_exchange.strategy.base import BaseStrategy, StrategyContext


class TrailingStopStrategy(BaseStrategy):
    """Trend-following strategy that maintains a position while price makes new highs,
    and exits (or reverses) when price drops below the trailing high by a threshold.
    """

    def __init__(self, strategy_id: str = "trailing_stop", params: dict | None = None) -> None:
        defaults = {
            "trail_pct": 0.05,  # Exit when price drops 5% from peak
            "entry_pct": 0.02,  # Enter when price rises 2% from trough
            "max_weight": 0.9,
            "lookback_bars": 20,  # Number of bars to track peak/trough
        }
        merged = defaults | (params or {})
        super().__init__(strategy_id, merged)

    def generate_signal(self, context: StrategyContext) -> StrategySignal:
        """Generate a target weight based on trailing high/low thresholds."""
        params = self.params
        trail_pct = float(params["trail_pct"])
        entry_pct = float(params["entry_pct"])
        max_weight = float(params["max_weight"])
        lookback = int(params["lookback_bars"])

        current_price = context.current_bar.close
        if current_price <= 0:
            return self._zero_signal(context, "invalid_price")

        # Use history to find peak and trough
        close_prices = context.close_prices
        if len(close_prices) < lookback:
            return self._zero_signal(context, "insufficient_history")

        recent_prices = close_prices[-lookback:]
        peak = max(recent_prices)
        trough = min(recent_prices)

        current_position = context.position.quantity
        avg_cost = context.position.average_cost

        # Determine entry/exit signals
        # If flat: enter long if price within entry_pct of trough (bouncing up)
        # If long: exit if price dropped trail_pct from peak
        if current_position <= 0:
            # Check for entry: price near trough and starting to turn up
            if trough > 0 and (peak - trough) / trough < entry_pct * 2:
                # Market is bouncing from recent low
                target_weight = max_weight * 0.5
                reason = "bounce_entry"
            elif avg_cost > 0 and (current_price - trough) / trough > entry_pct:
                target_weight = max_weight
                reason = "breakout_entry"
            else:
                target_weight = 0.0
                reason = "awaiting_signal"
        else:
            # Check for exit: trailing stop
            if peak > 0 and (peak - current_price) / peak >= trail_pct:
                target_weight = 0.0
                reason = "trailing_stop_exit"
            elif avg_cost > 0 and current_price < avg_cost * (1 - trail_pct):
                target_weight = 0.0
                reason = "stop_loss_exit"
            else:
                target_weight = max_weight
                reason = "trailing_hold"

        return StrategySignal(
            instrument_id=context.instrument.instrument_id,
            timestamp=context.current_bar.close_time,
            target_weight=target_weight,
            reason=reason,
            metadata={
                "peak": peak,
                "trough": trough,
                "trail_pct": round(trail_pct * 100, 2),
                "current_price": current_price,
                "peak_drawdown_pct": round((peak - current_price) / peak * 100, 2) if peak > 0 else 0,
            },
        )

    def _zero_signal(self, context: StrategyContext, reason: str) -> StrategySignal:
        return StrategySignal(
            instrument_id=context.instrument.instrument_id,
            timestamp=context.current_bar.close_time,
            target_weight=0.0,
            reason=reason,
        )
