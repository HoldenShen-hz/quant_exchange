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
    cci, ema, ewma_volatility, macd, momentum, obv, realized_volatility, rsi,
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


@dataclass
class ICResult:
    """Result of Information Coefficient analysis for a single factor."""

    factor_name: str
    ic_series: list[float]
    ic_mean: float
    ic_std: float
    ic_ir: float
    hit_rate: float
    periods: int
    top_decile_return: float
    bottom_decile_return: float
    spread_return: float


@dataclass
class IRResult:
    """Information Ratio analysis across multiple lookback windows."""

    factor_name: str
    ir_by_horizon: dict[int, float]
    mean_ir: float
    ic_decay_rate: float
    best_horizon: int
    worst_horizon: int
    stability_score: float


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
            for rank_val, vec in enumerate(sorted_vecs, 1):
                ranks[fn][vec.instrument_id] = rank_val
                percentile[fn][vec.instrument_id] = round(rank_val / n * 100, 2)

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
        all_vals: list[float] = []
        for history in self._history.values():
            closes = [b["close"] for b in history]
            if not closes:
                continue
            if feature_name == "rsi_14":
                all_vals.extend([rsi(closes[:i+1], 14)] for i in range(14, len(closes)))
            elif feature_name == "sma_20":
                all_vals.extend([sma(closes[:i+1], 20)] for i in range(20, len(closes)))
            elif feature_name == "realized_vol_20":
                all_vals.extend([realized_volatility(closes[:i+1], 20)] for i in range(20, len(closes)))
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
        n = len(recent)
        if n < 2:
            return 0.0
        x_mean = (n - 1) / 2
        y_mean = mean(recent)
        numerator = sum((i - x_mean) * (y - y_mean) for i, y in enumerate(recent))
        denominator = sum((i - x_mean) ** 2 for i in range(n))
        return numerator / denominator if denominator != 0 else 0.0

    def _intraday_intensity(self, history: list[dict], window: int = 20) -> float:
        """Compute intraday intensity averaged over window (MD-05)."""
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

    # ── ST-05: Cross-Sectional Factor Analysis ─────────────────────────────────

    def compute_ic(
        self,
        factor_name: str,
        forward_returns: dict[str, list[float]],
        *,
        lookback: int = 20,
    ) -> ICResult | None:
        """Compute Information Coefficient (IC) for a factor (ST-05).

        IC = Pearson correlation between factor values and forward returns.
        """
        all_iids = list(self._history.keys())
        if len(all_iids) < 3:
            return None

        min_len = min(len(self._history[iid]) for iid in all_iids)
        if min_len < lookback + 5:
            return None

        ic_series: list[float] = []
        top_returns: list[float] = []
        bottom_returns: list[float] = []

        for t in range(lookback, min_len - 1):
            factor_vals: dict[str, float] = {}
            for iid in all_iids:
                hist = self._history[iid]
                closes = [b["close"] for b in hist]
                if len(closes) <= t:
                    continue
                factor_vals[iid] = self._factor_at_time(iid, factor_name, t)

            if len(factor_vals) < 3:
                continue

            fwd_returns: dict[str, float] = {}
            for iid in all_iids:
                hist = self._history[iid]
                closes = [b["close"] for b in hist]
                if len(closes) <= t + 1 or closes[t] == 0:
                    continue
                fwd_returns[iid] = (closes[t + 1] - closes[t]) / closes[t]

            common_iids = set(factor_vals) & set(fwd_returns)
            if len(common_iids) < 3:
                continue

            sorted_factors = sorted(common_iids, key=lambda iid: factor_vals[iid], reverse=True)
            n = len(sorted_factors)
            top_n = sorted_factors[:max(1, n // 10)]
            bottom_n = sorted_factors[-max(1, n // 10):]

            top_ret = mean([fwd_returns[iid] for iid in top_n]) if top_n else 0.0
            bottom_ret = mean([fwd_returns[iid] for iid in bottom_n]) if bottom_n else 0.0
            top_returns.append(top_ret)
            bottom_returns.append(bottom_ret)

            ic = self._pearson_corr(
                [factor_vals[iid] for iid in common_iids],
                [fwd_returns[iid] for iid in common_iids],
            )
            ic_series.append(ic)

        if not ic_series:
            return None

        ic_mean_val = mean(ic_series)
        ic_std_val = stddev(ic_series) if len(ic_series) > 1 else 0.0
        hit_rate = sum(1 for ic in ic_series if ic > 0) / len(ic_series)
        ir = ic_mean_val / ic_std_val if ic_std_val != 0 else 0.0

        return ICResult(
            factor_name=factor_name,
            ic_series=[round(ic, 4) for ic in ic_series],
            ic_mean=round(ic_mean_val, 4),
            ic_std=round(ic_std_val, 4),
            ic_ir=round(ir, 4),
            hit_rate=round(hit_rate, 4),
            periods=len(ic_series),
            top_decile_return=round(mean(top_returns) if top_returns else 0.0, 6),
            bottom_decile_return=round(mean(bottom_returns) if bottom_returns else 0.0, 6),
            spread_return=round(
                (mean(top_returns) - mean(bottom_returns)) if top_returns and bottom_returns else 0.0, 6
            ),
        )

    def compute_ir(
        self,
        factor_name: str,
        forward_returns: dict[str, list[float]],
        horizons: list[int] | None = None,
    ) -> IRResult | None:
        """Compute Information Ratio across different horizons (ST-05)."""
        horizons = horizons or [1, 5, 10, 20]
        ic_by_horizon: dict[int, float] = {}
        for h in horizons:
            ic_result = self.compute_ic(factor_name, forward_returns, lookback=h)
            ic_by_horizon[h] = ic_result.ic_ir if ic_result else 0.0

        if not ic_by_horizon:
            return None

        ir_values = list(ic_by_horizon.values())
        mean_ir = mean(ir_values)
        best_horizon = max(ic_by_horizon, key=ic_by_horizon.get)
        worst_horizon = min(ic_by_horizon, key=ic_by_horizon.get)

        sorted_horizons = sorted(ic_by_horizon.keys())
        decay_rate = 0.0
        if len(sorted_horizons) >= 2:
            decay_rate = ic_by_horizon[sorted_horizons[0]] - ic_by_horizon[sorted_horizons[-1]]

        return IRResult(
            factor_name=factor_name,
            ir_by_horizon={h: round(v, 4) for h, v in ic_by_horizon.items()},
            mean_ir=round(mean_ir, 4),
            ic_decay_rate=round(decay_rate, 4),
            best_horizon=best_horizon,
            worst_horizon=worst_horizon,
            stability_score=round(abs(mean_ir) / max(stddev(ir_values) if len(ir_values) > 1 else 1, 0.001), 4),
        )

    def get_factor_report(
        self,
        factor_name: str,
        forward_returns: dict[str, list[float]],
    ) -> dict[str, Any]:
        """Generate a comprehensive factor quality report (ST-05)."""
        ic = self.compute_ic(factor_name, forward_returns)
        ir = self.compute_ir(factor_name, forward_returns)

        if ic is None:
            return {
                "factor_name": factor_name,
                "status": "INSUFFICIENT_DATA",
                "note": "Need at least 3 instruments and 25 bars of history",
            }

        quality = "POOR"
        if ic.ic_ir > 0.75:
            quality = "EXCELLENT"
        elif ic.ic_ir > 0.5:
            quality = "GOOD"
        elif ic.ic_ir > 0.25:
            quality = "FAIR"
        elif ic.ic_ir > 0:
            quality = "WEAK"

        recommendations = []
        if ic.ic_ir < 0.25:
            recommendations.append({
                "priority": "HIGH",
                "action": "Review factor construction",
                "reason": f"IC IR of {ic.ic_ir:.3f} indicates weak predictive power",
            })
        if ic.hit_rate < 0.52:
            recommendations.append({
                "priority": "MEDIUM",
                "action": "Consider combining with other factors",
                "reason": f"Hit rate {ic.hit_rate:.1%} barely above random",
            })
        if ir and ir.ic_decay_rate > 0.3:
            recommendations.append({
                "priority": "MEDIUM",
                "action": "Use shorter rebalance frequency",
                "reason": f"IC decays {ir.ic_decay_rate:.3f} over longer horizons",
            })
        if not recommendations:
            recommendations.append({
                "priority": "INFO",
                "action": "Factor meets quality thresholds",
                "reason": f"IC IR {ic.ic_ir:.3f} with hit rate {ic.hit_rate:.1%}",
            })

        return {
            "factor_name": factor_name,
            "quality_rating": quality,
            "ic": {
                "mean": ic.ic_mean,
                "std": ic.ic_std,
                "ir": ic.ic_ir,
                "hit_rate": ic.hit_rate,
                "periods": ic.periods,
                "top_decile_return": ic.top_decile_return,
                "bottom_decile_return": ic.bottom_decile_return,
                "spread_return": ic.spread_return,
                "series": ic.ic_series,
            },
            "ir": {
                "mean": ir.mean_ir if ir else None,
                "by_horizon": ir.ir_by_horizon if ir else {},
                "best_horizon": ir.best_horizon if ir else None,
                "decay_rate": ir.ic_decay_rate if ir else None,
                "stability": ir.stability_score if ir else None,
            } if ir else {},
            "recommendations": recommendations,
        }

    def compute_industry_neutral_zscore(
        self,
        factor_name: str,
        industry_mapping: dict[str, str],
    ) -> dict[str, float]:
        """Compute industry-neutral z-score for a factor (ST-05)."""
        factor_values: dict[str, float] = {}
        for iid in self._history:
            vec = self.transform(iid)
            if vec is None:
                continue
            factor_values[iid] = vec.features.get(factor_name, 0.0)

        if not factor_values:
            return {}

        industry_groups: dict[str, list[float]] = {}
        for iid, val in factor_values.items():
            industry = industry_mapping.get(iid, "Unknown")
            industry_groups.setdefault(industry, []).append(val)

        industry_stats: dict[str, tuple[float, float]] = {}
        for ind, vals in industry_groups.items():
            industry_stats[ind] = (mean(vals), stddev(vals) if len(vals) > 1 else 1.0)

        global_mean = mean(list(factor_values.values()))
        neutralized: dict[str, float] = {}
        for iid, val in factor_values.items():
            industry = industry_mapping.get(iid, "Unknown")
            ind_mean, ind_std = industry_stats.get(industry, (global_mean, 1.0))
            neutralized[iid] = (val - ind_mean) / ind_std if ind_std != 0 else 0.0

        return neutralized

    def _factor_at_time(self, instrument_id: str, factor_name: str, t: int) -> float:
        """Compute one factor value at a specific historical timestamp."""
        hist = self._history[instrument_id]
        closes = [b["close"] for b in hist[:t+1]]
        highs = [b.get("high", b["close"]) for b in hist[:t+1]]
        lows = [b.get("low", b["close"]) for b in hist[:t+1]]
        volumes = [float(b.get("volume", 0)) for b in hist[:t+1]]

        if factor_name == "rsi_14":
            return rsi(closes, 14)
        elif factor_name == "momentum_5":
            return momentum(closes, 5)
        elif factor_name == "momentum_20":
            return momentum(closes, 20)
        elif factor_name == "zscore_20":
            return zscore(closes, 20)
        elif factor_name == "realized_vol_20":
            return realized_volatility(closes, 20)
        elif factor_name == "price_to_sma20":
            sma_val = sma(closes, 20)
            return (closes[-1] / sma_val - 1) if sma_val else 0.0
        elif factor_name == "atr_14":
            return atr(highs, lows, closes, 14)
        elif factor_name == "volume_ma_ratio":
            vol_ma = sma(volumes, 20)
            return volumes[-1] / vol_ma if vol_ma else 1.0
        elif factor_name == "macd_histogram":
            _, _, histogram = macd(closes, 12, 26)
            return histogram
        elif factor_name == "stoch_k":
            return stochastic_k(highs, lows, closes, 14)
        elif factor_name == "williams_r":
            return williams_r(highs, lows, closes, 14)
        elif factor_name == "bb_percent":
            return bollinger_percent_b(closes, 20)
        return closes[-1] if closes else 0.0

    @staticmethod
    def _pearson_corr(x: list[float], y: list[float]) -> float:
        """Compute Pearson correlation between two equal-length lists."""
        n = len(x)
        if n == 0 or n != len(y) or n < 2:
            return 0.0
        mean_x = mean(x)
        mean_y = mean(y)
        num = sum((x[i] - mean_x) * (y[i] - mean_y) for i in range(n))
        den_x = sum((x[i] - mean_x) ** 2 for i in range(n))
        den_y = sum((y[i] - mean_y) ** 2 for i in range(n))
        den = (den_x * den_y) ** 0.5
        return num / den if den != 0 else 0.0
