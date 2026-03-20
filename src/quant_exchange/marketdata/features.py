"""Feature engineering pipeline for quantitative research (MD-05).

Provides a multi-stage pipeline that transforms raw OHLCV bars into
standardized feature vectors suitable for ML models or factor investing.

Stages:
  raw → standardize → features → cross_sectional → output
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from quant_exchange.core.utils import mean, stddev
from quant_exchange.strategy.factors import (
    atr, amihud_illiquidity, bollinger_bands, bollinger_percent_b,
    cci, ema, macd, momentum, obv, realized_volatility, rsi,
    sma, stochastic_k, vwap, williams_r, zscore,
)


@dataclass
class FeatureVector:
    """A single instrument's feature vector at one point in time."""

    instrument_id: str
    timestamp: str
    features: dict[str, float]
    standardized: dict[str, float] | None = None


@dataclass
class CrossSectionalFeatures:
    """Cross-sectional (relative) features across a universe at one timestamp."""

    timestamp: str
    vectors: list[FeatureVector]
    ranks: dict[str, dict[str, int]]  # feature_name → instrument_id → rank
    percentile: dict[str, dict[str, float]]  # feature_name → instrument_id → pctile


class FeaturePipeline:
    """Multi-stage feature engineering pipeline (MD-05).

    Transforms raw OHLCV bars into technical, sentiment, microstructure,
    and cross-sectional features.

    Example:
        pipeline = FeaturePipeline(window_tech=20, window_vol=20)
        bars = market_data.get_klines("AAPL", "1d", limit=60)
        result = pipeline.transform(bars)
        print(result.features["rsi_14"])  # raw RSI value
        print(result.standardized["rsi_14"])  # z-score normalized
    """

    def __init__(
        self,
        *,
        window_tech: int = 20,
        window_vol: int = 20,
        window_rsi: int = 14,
        window_atr: int = 14,
        window_bb: int = 20,
        fast_ema: int = 12,
        slow_ema: int = 26,
    ) -> None:
        self.window_tech = window_tech
        self.window_vol = window_vol
        self.window_rsi = window_rsi
        self.window_atr = window_atr
        self.window_bb = window_bb
        self.fast_ema = fast_ema
        self.slow_ema = slow_ema
        # Rolling history for each instrument
        self._history: dict[str, list[dict]] = {}

    def ingest(self, instrument_id: str, bars: list[dict]) -> None:
        """Ingest bars and maintain rolling history for one instrument (MD-05).

        bars: list of dicts with keys open, high, low, close, volume
        """
        if instrument_id not in self._history:
            self._history[instrument_id] = []
        self._history[instrument_id].extend(bars)
        # Keep rolling window + buffer
        max_window = max(self.window_tech, self.window_vol, 60)
        if len(self._history[instrument_id]) > max_window * 2:
            self._history[instrument_id] = self._history[instrument_id][-max_window:]

    def transform(self, instrument_id: str) -> FeatureVector | None:
        """Compute all features for the latest bar of one instrument (MD-05)."""
        history = self._history.get(instrument_id, [])
        if len(history) < max(self.window_tech, self.window_vol, self.window_rsi, 30):
            return None

        closes = [b["close"] for b in history]
        highs = [b.get("high", b["close"]) for b in history]
        lows = [b.get("low", b["close"]) for b in history]
        volumes = [float(b.get("volume", 0)) for b in history]

        features: dict[str, float] = {}

        # ── Trend & Momentum features ──────────────────────────────────────────
        features["sma_20"] = sma(closes, 20)
        features["sma_60"] = sma(closes, min(60, len(closes)))
        features["ema_12"] = ema(closes, 12)
        features["ema_26"] = ema(closes, 26)
        features["price_to_sma20"] = closes[-1] / features["sma_20"] - 1 if features["sma_20"] else 0.0
        features["price_to_sma60"] = closes[-1] / features["sma_60"] - 1 if features["sma_60"] else 0.0

        macd_line, signal_line, histogram = macd(closes, self.fast_ema, self.slow_ema)
        features["macd_line"] = macd_line
        features["macd_signal"] = signal_line
        features["macd_histogram"] = histogram

        features["momentum_5"] = momentum(closes, 5)
        features["momentum_10"] = momentum(closes, 10)
        features["momentum_20"] = momentum(closes, 20)
        features["roc_14"] = (closes[-1] / closes[-14] - 1) if len(closes) >= 15 else 0.0

        # ── Mean Reversion features ──────────────────────────────────────────────
        features["rsi_14"] = rsi(closes, self.window_rsi)
        features["rsi_28"] = rsi(closes, 28)
        features["zscore_20"] = zscore(closes, 20)
        features["zscore_60"] = zscore(closes, 60)

        upper, middle, lower = bollinger_bands(closes, self.window_bb)
        features["bb_upper"] = upper
        features["bb_middle"] = middle
        features["bb_lower"] = lower
        features["bb_percent"] = bollinger_percent_b(closes, self.window_bb)

        # ── Volatility features ─────────────────────────────────────────────────
        features["realized_vol_20"] = realized_volatility(closes, 20)
        features["realized_vol_60"] = realized_volatility(closes, min(60, len(closes)))
        features["ewma_vol"] = ewma_volatility(closes, self.window_vol)
        features["atr_14"] = atr(highs, lows, closes, self.window_atr)
        features["atr_percent"] = features["atr_14"] / closes[-1] if closes[-1] else 0.0

        # ── Oscillator features ─────────────────────────────────────────────────
        features["stoch_k"] = stochastic_k(highs, lows, closes, 14)
        features["williams_r"] = williams_r(highs, lows, closes, 14)
        features["cci_20"] = cci(highs, lows, closes, 20)

        # ── Volume & Liquidity features ─────────────────────────────────────────
        features["obv"] = obv(closes, volumes)
        features["obv_slope"] = self._obv_slope(history)
        features["vwap"] = vwap(highs, lows, closes, volumes)
        features["price_to_vwap"] = closes[-1] / features["vwap"] - 1 if features["vwap"] else 0.0
        features["amihud_illiquidity"] = amihud_illiquidity(closes, volumes, 20)
        features["volume_ma_ratio"] = volumes[-1] / sma(volumes, 20) if len(volumes) >= 20 else 1.0

        # ── Market Microstructure features ───────────────────────────────────────
        features["high_low_range"] = (highs[-1] - lows[-1]) / closes[-1] if closes[-1] else 0.0
        features["close_position"] = (closes[-1] - lows[-1]) / (highs[-1] - lows[-1]) if (highs[-1] - lows[-1]) else 0.5
        features["intraday_intensity"] = self._intraday_intensity(history)

        # ── Standardized features ────────────────────────────────────────────────
        standardized = self.standardize(features)

        return FeatureVector(
            instrument_id=instrument_id,
            timestamp=history[-1].get("timestamp", history[-1].get("close_time", "")),
            features=features,
            standardized=standardized,
        )

    def transform_universe(self, instrument_ids: list[str]) -> CrossSectionalFeatures:
        """Compute features for all instruments and derive cross-sectional ranks (MD-05)."""
        vectors: list[FeatureVector] = []
        for iid in instrument_ids:
            v = self.transform(iid)
            if v is not None:
                vectors.append(v)

        if not vectors:
            return CrossSectionalFeatures(timestamp="", vectors=[], ranks={}, percentile={})

        # Collect all feature names from the first vector
        feature_names = list(vectors[0].features.keys())
        ranks: dict[str, dict[str, int]] = {fn: {} for fn in feature_names}
        percentile: dict[str, dict[str, float]] = {fn: {} for fn in feature_names}

        for fn in feature_names:
            # Sort instruments by feature value (descending)
            sorted_vecs = sorted(vectors, key=lambda v: v.features.get(fn, 0), reverse=True)
            n = len(sorted_vecs)
            for rank, vec in enumerate(sorted_vecs, 1):
                ranks[fn][vec.instrument_id] = rank
                percentile[fn][vec.instrument_id] = round(rank / n * 100, 2)

        return CrossSectionalFeatures(
            timestamp=vectors[-1].timestamp if vectors else "",
            vectors=vectors,
            ranks=ranks,
            percentile=percentile,
        )

    def standardize(self, features: dict[str, float]) -> dict[str, float]:
        """Z-score standardize a feature dict using rolling statistics (MD-05).

        Uses a simple running mean/std approach from the stored history.
        """
        result: dict[str, float] = {}
        for name, value in features.items():
            series = self._get_feature_series(name)
            if len(series) < 10:
                result[name] = 0.0
                continue
            m = mean(series)
            s = stddev(series)
            result[name] = (value - m) / s if s != 0 else 0.0
        return result

    def _get_feature_series(self, feature_name: str) -> list[float]:
        """Return the rolling historical series for one feature across all instruments."""
        # Aggregate history from all instruments
        all_vals: list[float] = []
        for history in self._history.values():
            closes = [b["close"] for b in history]
            if not closes:
                continue
            # Compute the feature for each bar and collect
            if feature_name == "rsi_14":
                all_vals.extend([rsi(closes[:i+1], 14)] for i in range(14, len(closes)))
            elif feature_name == "sma_20":
                all_vals.extend([sma(closes[:i+1], 20)] for i in range(20, len(closes)))
            elif feature_name == "realized_vol_20":
                all_vals.extend([realized_volatility(closes[:i+1], 20)] for i in range(20, len(closes)))
            # Fallback: just use close values
            else:
                all_vals.extend(closes)
        return all_vals

    def _obv_slope(self, history: list[dict], window: int = 10) -> float:
        """Compute slope of OBV over recent window (MD-05)."""
        if len(history) < window + 1:
            return 0.0
        closes = [b["close"] for b in history]
        volumes = [float(b.get("volume", 0)) for b in history]
        obv_vals = []
        cumulative = 0.0
        for i in range(len(closes)):
            if i == 0:
                cumulative = volumes[i]
            elif closes[i] > closes[i-1]:
                cumulative += volumes[i]
            else:
                cumulative -= volumes[i]
            obv_vals.append(cumulative)
        if len(obv_vals) < window:
            return 0.0
        recent = obv_vals[-window:]
        # Simple linear slope
        n = len(recent)
        if n < 2:
            return 0.0
        x_mean = (n - 1) / 2
        y_mean = mean(recent)
        numerator = sum((i - x_mean) * (y - y_mean) for i, y in enumerate(recent))
        denominator = sum((i - x_mean) ** 2 for i in range(n))
        slope = numerator / denominator if denominator != 0 else 0.0
        return slope

    def _intraday_intensity(self, history: list[dict], window: int = 20) -> float:
        """Compute intraday intensity (close vs high-low range) averaged over window (MD-05)."""
        if len(history) < window:
            return 0.0
        intensities = []
        for b in history[-window:]:
            high = b.get("high", b["close"])
            low = b.get("low", b["close"])
            close = b["close"]
            if high != low:
                intensities.append((close - low) / (high - low))
            else:
                intensities.append(0.5)
        return mean(intensities)
