"""Reference strategy that combines moving averages with sentiment bias."""

from __future__ import annotations

from quant_exchange.core.models import Direction, StrategySignal
from quant_exchange.strategy.base import BaseStrategy, StrategyContext
from quant_exchange.strategy.factors import momentum, realized_volatility, sma


class MovingAverageSentimentStrategy(BaseStrategy):
    """A deterministic demo strategy aligned with the project design docs."""

    def __init__(self, strategy_id: str = "ma_sentiment", params: dict | None = None) -> None:
        defaults = {
            "fast_window": 3,
            "slow_window": 5,
            "sentiment_threshold": 0.05,
            "max_weight": 0.9,
            "volatility_cap": 0.80,
        }
        merged = defaults | (params or {})
        super().__init__(strategy_id, merged)

    def generate_signal(self, context: StrategyContext) -> StrategySignal:
        """Translate trend and sentiment inputs into a target portfolio weight."""

        fast = sma(context.close_prices, self.params["fast_window"])
        slow = sma(context.close_prices, self.params["slow_window"])
        if fast == 0 or slow == 0:
            return StrategySignal(
                instrument_id=context.instrument.instrument_id,
                timestamp=context.current_bar.close_time,
                target_weight=0.0,
                reason="insufficient_history",
            )
        trend_up = fast > slow
        trend_down = fast < slow
        bias = context.latest_bias
        current_momentum = momentum(context.close_prices, min(3, len(context.close_prices) - 1))
        volatility = realized_volatility(context.close_prices, min(5, len(context.close_prices) - 1))
        risk_scale = 1.0 if volatility <= self.params["volatility_cap"] else 0.5
        target_weight = 0.0
        if trend_up and bias.score >= self.params["sentiment_threshold"]:
            target_weight = self.params["max_weight"] * risk_scale
            reason = "trend_up_positive_bias"
        elif trend_down and bias.score <= -self.params["sentiment_threshold"]:
            target_weight = -self.params["max_weight"] * risk_scale
            reason = "trend_down_negative_bias"
        elif bias.direction == Direction.FLAT and abs(current_momentum) < 0.01:
            target_weight = 0.0
            reason = "neutral_regime"
        else:
            reason = "signal_filtered"
        return StrategySignal(
            instrument_id=context.instrument.instrument_id,
            timestamp=context.current_bar.close_time,
            target_weight=target_weight,
            reason=reason,
            metadata={
                "fast": fast,
                "slow": slow,
                "bias_score": bias.score,
                "momentum": current_momentum,
                "volatility": volatility,
            },
        )
