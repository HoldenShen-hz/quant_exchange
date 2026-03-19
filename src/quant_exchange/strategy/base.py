"""Strategy interfaces and shared runtime context objects."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

from quant_exchange.core.models import (
    DirectionalBias,
    Fill,
    Instrument,
    Kline,
    Order,
    Position,
    RiskDecision,
    StrategySignal,
)


@dataclass(slots=True, frozen=True)
class StrategyContext:
    """Immutable runtime snapshot passed into strategy logic."""

    instrument: Instrument
    current_bar: Kline
    history: tuple[Kline, ...]
    position: Position
    cash: float
    equity: float
    latest_bias: DirectionalBias
    risk_limits: dict[str, Any] = field(default_factory=dict)
    account_id: str = ""

    def __post_init__(self) -> None:
        """Guard against empty or forward-looking history slices."""

        if not self.history:
            raise ValueError("strategy_history_cannot_be_empty")
        if self.history[-1].close_time > self.current_bar.close_time:
            raise ValueError("lookahead_bias_detected")
        for idx in range(1, len(self.history)):
            if self.history[idx - 1].close_time > self.history[idx].close_time:
                raise ValueError("strategy_history_out_of_order")

    @property
    def close_prices(self) -> list[float]:
        """Expose close prices as a convenience view for factor functions."""

        return [bar.close for bar in self.history]

    @property
    def volumes(self) -> list[float]:
        """Expose volumes as a convenience view for factor functions."""

        return [bar.volume for bar in self.history]

    @property
    def highs(self) -> list[float]:
        """Expose high prices as a convenience view for factor functions."""

        return [bar.high for bar in self.history]

    @property
    def lows(self) -> list[float]:
        """Expose low prices as a convenience view for factor functions."""

        return [bar.low for bar in self.history]


class BaseStrategy(ABC):
    """Abstract base class for reusable strategy implementations.

    Provides lifecycle hooks matching the documented strategy framework:
    - on_init: one-time initialization
    - on_bar: called on each new K-line event
    - on_tick: called on each tick event (optional)
    - generate_signal: produce a target portfolio weight
    - target_position: alternative output as absolute quantity
    - on_order_update: react to order state changes
    - on_risk_event: react to risk control events
    """

    def __init__(self, strategy_id: str, params: dict | None = None) -> None:
        self.strategy_id = strategy_id
        self.params = params or {}
        self._version: str = "1.0"
        self._run_id: str = ""

    def on_init(self, context: StrategyContext) -> None:
        """One-time initialization hook called before the first bar."""

    def on_bar(self, context: StrategyContext) -> None:
        """Called on each new K-line bar before generate_signal."""

    def on_tick(self, instrument_id: str, price: float, size: float, timestamp: Any = None) -> None:
        """Called on each tick-level event (optional, override for tick strategies)."""

    @abstractmethod
    def generate_signal(self, context: StrategyContext) -> StrategySignal | None:
        """Return the desired target portfolio weight for the instrument."""

    def target_position(self, context: StrategyContext) -> float | None:
        """Alternative output: return an absolute target quantity instead of weight.

        Returns None by default, meaning the engine should use generate_signal instead.
        """

        return None

    def on_order_update(self, order: Order, fill: Fill | None = None) -> None:
        """React to order state changes and fill events."""

    def on_risk_event(self, decision: RiskDecision) -> None:
        """React to risk engine decisions that affect this strategy."""


class StrategyRegistry:
    """In-memory registry used to look up strategies by identifier."""

    def __init__(self) -> None:
        self._strategies: dict[str, BaseStrategy] = {}

    def register(self, strategy: BaseStrategy) -> None:
        """Register a strategy instance for later retrieval."""

        self._strategies[strategy.strategy_id] = strategy

    def get(self, strategy_id: str) -> BaseStrategy:
        """Return a previously registered strategy instance."""

        return self._strategies[strategy_id]

    def list_all(self) -> list[BaseStrategy]:
        """Return all registered strategies."""

        return list(self._strategies.values())

    def has(self, strategy_id: str) -> bool:
        """Check if a strategy is registered."""

        return strategy_id in self._strategies
