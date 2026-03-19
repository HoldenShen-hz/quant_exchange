"""Technical analysis factor functions shared across strategies and tests.

This library provides reusable, deterministic factor functions covering:
- Trend & Momentum: SMA, EMA, momentum, MACD
- Mean Reversion: z-score, RSI, Bollinger Bands
- Volatility: realized vol, ATR, EWMA vol
- Volume & Liquidity: OBV, VWAP
- Technical Oscillators: Stochastic, Williams %R, CCI
"""

from __future__ import annotations

from math import sqrt

from quant_exchange.core.utils import mean, stddev


# ---------------------------------------------------------------------------
# Trend & Momentum
# ---------------------------------------------------------------------------

def sma(values: list[float], window: int) -> float:
    """Return the simple moving average over the trailing window."""

    if window <= 0 or len(values) < window:
        return 0.0
    return mean(values[-window:])


def ema(values: list[float], window: int) -> float:
    """Return the exponentially weighted moving average."""

    if window <= 0 or not values:
        return 0.0
    multiplier = 2.0 / (window + 1.0)
    result = values[0]
    for value in values[1:]:
        result = (value - result) * multiplier + result
    return result


def momentum(values: list[float], window: int) -> float:
    """Return trailing price momentum as a fractional change."""

    if window <= 0 or len(values) <= window:
        return 0.0
    previous = values[-window - 1]
    if previous == 0:
        return 0.0
    return values[-1] / previous - 1.0


def macd(values: list[float], fast: int = 12, slow: int = 26, signal: int = 9) -> tuple[float, float, float]:
    """Compute MACD line, signal line, and histogram.

    Returns (macd_line, signal_line, histogram).
    """

    if not values or len(values) < slow:
        return 0.0, 0.0, 0.0
    fast_ema = ema(values, fast)
    slow_ema = ema(values, slow)
    macd_line = fast_ema - slow_ema
    # Build MACD series for signal line
    macd_series: list[float] = []
    for i in range(slow - 1, len(values)):
        subset = values[: i + 1]
        macd_series.append(ema(subset, fast) - ema(subset, slow))
    signal_line = ema(macd_series, signal) if len(macd_series) >= signal else macd_line
    histogram = macd_line - signal_line
    return macd_line, signal_line, histogram


def rate_of_change(values: list[float], window: int) -> float:
    """Return the rate of change (ROC) as a percentage."""

    if window <= 0 or len(values) <= window:
        return 0.0
    previous = values[-window - 1]
    if previous == 0:
        return 0.0
    return (values[-1] - previous) / previous * 100.0


# ---------------------------------------------------------------------------
# Mean Reversion
# ---------------------------------------------------------------------------

def zscore(values: list[float], window: int) -> float:
    """Return the trailing z-score of the latest observation."""

    if window <= 0 or len(values) < window:
        return 0.0
    subset = values[-window:]
    volatility = stddev(subset)
    if volatility == 0:
        return 0.0
    return (subset[-1] - mean(subset)) / volatility


def bollinger_bands(values: list[float], window: int = 20, num_std: float = 2.0) -> tuple[float, float, float]:
    """Compute Bollinger Bands: (upper, middle, lower)."""

    if window <= 0 or len(values) < window:
        return 0.0, 0.0, 0.0
    subset = values[-window:]
    middle = mean(subset)
    sd = stddev(subset)
    return middle + num_std * sd, middle, middle - num_std * sd


def bollinger_percent_b(values: list[float], window: int = 20, num_std: float = 2.0) -> float:
    """Return Bollinger %B: position of price relative to bands (0-1 range, can exceed)."""

    upper, middle, lower = bollinger_bands(values, window, num_std)
    if upper == lower:
        return 0.5
    return (values[-1] - lower) / (upper - lower)


# ---------------------------------------------------------------------------
# Volatility
# ---------------------------------------------------------------------------

def realized_volatility(values: list[float], window: int) -> float:
    """Estimate annualized realized volatility from trailing returns."""

    if window <= 1 or len(values) <= window:
        return 0.0
    returns = []
    subset = values[-(window + 1) :]
    for idx in range(1, len(subset)):
        previous = subset[idx - 1]
        if previous == 0:
            continue
        returns.append(subset[idx] / previous - 1.0)
    return stddev(returns) * sqrt(252)


def atr(highs: list[float], lows: list[float], closes: list[float], window: int = 14) -> float:
    """Compute Average True Range (ATR) over a trailing window."""

    n = min(len(highs), len(lows), len(closes))
    if window <= 0 or n < window + 1:
        return 0.0
    true_ranges: list[float] = []
    for i in range(n - window, n):
        tr = max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i - 1]),
            abs(lows[i] - closes[i - 1]),
        )
        true_ranges.append(tr)
    return mean(true_ranges)


def ewma_volatility(values: list[float], window: int = 20, decay: float = 0.94) -> float:
    """Compute EWMA (exponentially weighted) volatility estimate."""

    if window <= 1 or len(values) <= window:
        return 0.0
    returns: list[float] = []
    subset = values[-(window + 1) :]
    for idx in range(1, len(subset)):
        prev = subset[idx - 1]
        if prev == 0:
            continue
        returns.append(subset[idx] / prev - 1.0)
    if not returns:
        return 0.0
    variance = returns[0] ** 2
    for r in returns[1:]:
        variance = decay * variance + (1 - decay) * r ** 2
    return sqrt(variance) * sqrt(252)


# ---------------------------------------------------------------------------
# Technical Oscillators
# ---------------------------------------------------------------------------

def rsi(values: list[float], window: int = 14) -> float:
    """Compute the relative strength index over a trailing window."""

    if window <= 0 or len(values) <= window:
        return 50.0
    gains: list[float] = []
    losses: list[float] = []
    subset = values[-(window + 1) :]
    for idx in range(1, len(subset)):
        delta = subset[idx] - subset[idx - 1]
        if delta >= 0:
            gains.append(delta)
        else:
            losses.append(-delta)
    avg_gain = mean(gains)
    avg_loss = mean(losses)
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - 100.0 / (1.0 + rs)


def stochastic_k(highs: list[float], lows: list[float], closes: list[float], window: int = 14) -> float:
    """Compute Stochastic %K oscillator."""

    n = min(len(highs), len(lows), len(closes))
    if window <= 0 or n < window:
        return 50.0
    highest = max(highs[-window:])
    lowest = min(lows[-window:])
    if highest == lowest:
        return 50.0
    return (closes[-1] - lowest) / (highest - lowest) * 100.0


def williams_r(highs: list[float], lows: list[float], closes: list[float], window: int = 14) -> float:
    """Compute Williams %R oscillator (range -100 to 0)."""

    n = min(len(highs), len(lows), len(closes))
    if window <= 0 or n < window:
        return -50.0
    highest = max(highs[-window:])
    lowest = min(lows[-window:])
    if highest == lowest:
        return -50.0
    return (highest - closes[-1]) / (highest - lowest) * -100.0


def cci(highs: list[float], lows: list[float], closes: list[float], window: int = 20) -> float:
    """Compute Commodity Channel Index (CCI)."""

    n = min(len(highs), len(lows), len(closes))
    if window <= 0 or n < window:
        return 0.0
    typical_prices = [(highs[i] + lows[i] + closes[i]) / 3.0 for i in range(n - window, n)]
    tp_mean = mean(typical_prices)
    mean_deviation = mean([abs(tp - tp_mean) for tp in typical_prices])
    if mean_deviation == 0:
        return 0.0
    return (typical_prices[-1] - tp_mean) / (0.015 * mean_deviation)


# ---------------------------------------------------------------------------
# Volume & Liquidity
# ---------------------------------------------------------------------------

def obv(closes: list[float], volumes: list[float]) -> float:
    """Compute On-Balance Volume (OBV). Returns the cumulative OBV value."""

    n = min(len(closes), len(volumes))
    if n < 2:
        return 0.0
    cumulative = 0.0
    for i in range(1, n):
        if closes[i] > closes[i - 1]:
            cumulative += volumes[i]
        elif closes[i] < closes[i - 1]:
            cumulative -= volumes[i]
    return cumulative


def vwap(highs: list[float], lows: list[float], closes: list[float], volumes: list[float]) -> float:
    """Compute Volume-Weighted Average Price (VWAP)."""

    n = min(len(highs), len(lows), len(closes), len(volumes))
    if n == 0:
        return 0.0
    total_volume = sum(volumes[:n])
    if total_volume == 0:
        return 0.0
    cum_tp_volume = sum(
        ((highs[i] + lows[i] + closes[i]) / 3.0) * volumes[i]
        for i in range(n)
    )
    return cum_tp_volume / total_volume


def amihud_illiquidity(closes: list[float], volumes: list[float], window: int = 20) -> float:
    """Compute Amihud illiquidity ratio (avg |return| / volume)."""

    n = min(len(closes), len(volumes))
    if window <= 0 or n <= window:
        return 0.0
    ratios: list[float] = []
    for i in range(n - window, n):
        if closes[i - 1] == 0 or volumes[i] == 0:
            continue
        ret = abs(closes[i] / closes[i - 1] - 1.0)
        ratios.append(ret / volumes[i])
    return mean(ratios) if ratios else 0.0


# ---------------------------------------------------------------------------
# Fundamental-style factors (value scoring)
# ---------------------------------------------------------------------------

def pe_score(pe_ratio: float, sector_median: float = 15.0) -> float:
    """Score a P/E ratio relative to sector median. Lower P/E scores higher (0-1)."""

    if pe_ratio <= 0 or sector_median <= 0:
        return 0.5
    ratio = pe_ratio / sector_median
    return max(0.0, min(1.0, 1.0 - (ratio - 1.0) * 0.5))


def roe_score(roe: float) -> float:
    """Score return on equity. Higher ROE scores higher (0-1)."""

    return max(0.0, min(1.0, roe / 0.25))
