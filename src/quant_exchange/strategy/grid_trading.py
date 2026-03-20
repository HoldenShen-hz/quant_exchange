"""Grid-trading strategy: buy on lower grid levels, sell on upper grid levels."""

from __future__ import annotations

from quant_exchange.core.models import StrategySignal
from quant_exchange.strategy.base import BaseStrategy, StrategyContext


class GridTradingStrategy(BaseStrategy):
    """Grid trading strategy that places symmetric buy/sell orders around a reference price.

    The grid consists of evenly spaced levels. When price crosses a level downward,
    a long position is accumulated; when price crosses upward, positions are reduced.
    """

    def __init__(self, strategy_id: str = "grid_trading", params: dict | None = None) -> None:
        defaults = {
            "grid_levels": 5,
            "grid_spacing_pct": 0.02,  # 2% between grid lines
            "position_per_grid": 0.15,  # fraction of equity per grid level
            "max_total_position": 0.9,  # max total long exposure
            "reference_price": None,  # None = use entry price / baseline
        }
        merged = defaults | (params or {})
        super().__init__(strategy_id, merged)

    def generate_signal(self, context: StrategyContext) -> StrategySignal:
        """Generate a target weight based on the current price vs grid levels."""
        params = self.params
        n_levels = int(params["grid_levels"])
        spacing = float(params["grid_spacing_pct"])
        pos_per_level = float(params["position_per_grid"])
        max_pos = float(params["max_total_position"])

        current_price = context.current_bar.close
        if current_price <= 0:
            return self._zero_signal(context, "invalid_price")

        # Reference price: use baseline_price from context or current price
        baseline = context.position.average_cost if context.position.average_cost > 0 else current_price
        if baseline <= 0:
            baseline = current_price

        # Compute grid boundaries (symmetric around baseline)
        lower_bound = baseline * (1 - spacing * n_levels)
        upper_bound = baseline * (1 + spacing * n_levels)
        grid_range = upper_bound - lower_bound
        if grid_range <= 0:
            return self._zero_signal(context, "invalid_grid_range")

        # Determine which grid band we are in (0 = lowest, n_levels-1 = highest)
        position_in_range = (current_price - lower_bound) / grid_range
        band = int(position_in_range * n_levels)
        band = max(0, min(band, n_levels - 1))

        # Target weight: accumulate long as price drops (buy low grid), reduce as rises
        # band=0 (lowest price) -> max long, band=n-1 (highest price) -> min long
        target_weight = (1.0 - band / (n_levels - 1)) * max_pos if n_levels > 1 else max_pos
        target_weight = max(0.0, min(target_weight, max_pos))

        reason = f"grid_band_{band}_of_{n_levels}"

        return StrategySignal(
            instrument_id=context.instrument.instrument_id,
            timestamp=context.current_bar.close_time,
            target_weight=target_weight,
            reason=reason,
            metadata={
                "grid_band": band,
                "n_levels": n_levels,
                "spacing_pct": round(spacing * 100, 2),
                "reference_price": baseline,
                "current_price": current_price,
            },
        )

    def _zero_signal(self, context: StrategyContext, reason: str) -> StrategySignal:
        return StrategySignal(
            instrument_id=context.instrument.instrument_id,
            timestamp=context.current_bar.close_time,
            target_weight=0.0,
            reason=reason,
        )
