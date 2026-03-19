"""Strategy interfaces and built-in strategy templates."""

from .base import BaseStrategy, StrategyContext, StrategyRegistry
from .config import (
    StrategyConfigLoader,
    StrategyParameterSet,
    StrategyParameterStore,
    StrategyRun,
    StrategyRunRecorder,
    StrategyVersion,
    StrategyVersionManager,
)
from .moving_average_sentiment import MovingAverageSentimentStrategy

__all__ = [
    "BaseStrategy",
    "StrategyContext",
    "StrategyRegistry",
    "MovingAverageSentimentStrategy",
    # Config management
    "StrategyConfigLoader",
    "StrategyParameterSet",
    "StrategyParameterStore",
    "StrategyRun",
    "StrategyRunRecorder",
    "StrategyVersion",
    "StrategyVersionManager",
]
