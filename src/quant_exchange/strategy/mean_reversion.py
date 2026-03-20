"""Mean-reversion strategy: buy when oversold, sell when overbought relative to moving average."""

from __future__ import annotations

from quant_exchange.core.models import StrategySignal
from quant_exchange.strategy.base import BaseStrategy, StrategyContext
from quant_exchange.strategy.factors import sma
from quant_exchange.core.utils import stddev


class MeanReversionStrategy(BaseStrategy):
    """Mean-reversion strategy that buys oversold and sells overbought conditions.

    Uses z-score of current price deviation from SMA to generate signals:
    - z < -threshold  -> oversold -> long signal
    - z > +threshold  -> overbought -> close/exit signal
    """

    def __init__(self, strategy_id: str = "mean_reversion", params: dict | None = None) -> None:
        defaults = {
            "ma_window": 20,
            "z_threshold": 2.0,  # Enter when |z-score| > threshold
            "max_weight": 0.85,
            "exit_threshold": 0.5,  # Exit when z-score returns within this band
            "volatility_adjust": True,
        }
        merged = defaults | (params or {})
        super().__init__(strategy_id, merged)

    def generate_signal(self, context: StrategyContext) -> StrategySignal:
        """Generate a target weight based on z-score of price deviation from mean."""
        params = self.params
        window = int(params["ma_window"])
        z_threshold = float(params["z_threshold"])
        max_weight = float(params["max_weight"])
        exit_threshold = float(params["exit_threshold"])

        closes = context.close_prices
        if len(closes) < window:
            return self._zero_signal(context, "insufficient_history")

        current_price = context.current_bar.close
        if current_price <= 0:
            return self._zero_signal(context, "invalid_price")

        # Calculate z-score
        ma = sma(closes, window)
        std = stddev(closes[-window:])
        if std <= 0:
            return self._zero_signal(context, "zero_volatility")

        z_score = (current_price - ma) / std

        # Determine target weight
        if abs(z_score) < exit_threshold:
            target_weight = 0.0
            reason = "mean_reversion_complete"
        elif z_score < -z_threshold:
            # Oversold: mean reversion bounce expected
            # Scale position by how far oversold
            strength = min(abs(z_score) / z_threshold, 2.0)
            target_weight = max_weight * strength * 0.5
            reason = f"oversold_z_{round(z_score, 2)}"
        elif z_score > z_threshold:
            # Overbought: expect reversion to mean (down)
            strength = min(abs(z_score) / z_threshold, 2.0)
            target_weight = -max_weight * strength * 0.5
            reason = f"overbought_z_{round(z_score, 2)}"
        else:
            target_weight = 0.0
            reason = f"within_band_z_{round(z_score, 2)}"

        target_weight = max(-max_weight, min(max_weight, target_weight))

        return StrategySignal(
            instrument_id=context.instrument.instrument_id,
            timestamp=context.current_bar.close_time,
            target_weight=target_weight,
            reason=reason,
            metadata={
                "ma": round(ma, 4),
                "std": round(std, 4),
                "z_score": round(z_score, 4),
                "window": window,
            },
        )

    def _zero_signal(self, context: StrategyContext, reason: str) -> StrategySignal:
        return StrategySignal(
            instrument_id=context.instrument.instrument_id,
            timestamp=context.current_bar.close_time,
            target_weight=0.0,
            reason=reason,
        )
