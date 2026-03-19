"""Numerical helpers used by factor, risk, and reporting components."""

from __future__ import annotations

import math
from statistics import fmean, pstdev
from typing import Iterable, Sequence


def clamp(value: float, lower: float, upper: float) -> float:
    """Clamp a value into an inclusive numeric range."""

    return max(lower, min(upper, value))


def safe_div(numerator: float, denominator: float, default: float = 0.0) -> float:
    """Divide safely and fall back to a default value near zero denominators."""

    if abs(denominator) < 1e-12:
        return default
    return numerator / denominator


def mean(values: Sequence[float]) -> float:
    """Return the arithmetic mean or zero for an empty sequence."""

    return fmean(values) if values else 0.0


def stddev(values: Sequence[float]) -> float:
    """Return the population standard deviation for a sequence."""

    return pstdev(values) if len(values) > 1 else 0.0


def annualize_return(total_return: float, periods: int, periods_per_year: int = 252) -> float:
    """Convert a cumulative return into an annualized return estimate."""

    if periods <= 0:
        return 0.0
    growth = 1.0 + total_return
    if growth <= 0:
        return -1.0
    return growth ** (periods_per_year / periods) - 1.0


def max_drawdown(equity_values: Iterable[float]) -> float:
    """Compute the maximum peak-to-trough drawdown."""

    peak = 0.0
    worst = 0.0
    for equity in equity_values:
        peak = max(peak, equity)
        if peak > 0:
            worst = max(worst, (peak - equity) / peak)
    return worst


def sharpe_ratio(returns: Sequence[float], periods_per_year: int = 252) -> float:
    """Compute the annualized Sharpe ratio from periodic returns."""

    if len(returns) < 2:
        return 0.0
    mean_return = mean(returns)
    vol = stddev(returns)
    if vol == 0:
        return 0.0
    return math.sqrt(periods_per_year) * mean_return / vol


def sortino_ratio(returns: Sequence[float], periods_per_year: int = 252) -> float:
    """Compute the annualized Sortino ratio using downside volatility only."""

    if len(returns) < 2:
        return 0.0
    downside = [value for value in returns if value < 0]
    downside_vol = stddev(downside)
    if downside_vol == 0:
        return 0.0
    return math.sqrt(periods_per_year) * mean(returns) / downside_vol
