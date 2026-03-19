from __future__ import annotations

from datetime import timedelta
from math import sqrt
from statistics import pstdev
import unittest

from quant_exchange.core.models import Direction, DirectionalBias, Position
from quant_exchange.strategy import MovingAverageSentimentStrategy, StrategyContext, StrategyRegistry
from quant_exchange.strategy.factors import (
    amihud_illiquidity, atr, bollinger_bands, bollinger_percent_b, cci, ema,
    ewma_volatility, momentum, obv, pe_score, rate_of_change, realized_volatility,
    roe_score, rsi, sma, stochastic_k, vwap, williams_r, zscore,
)

from .fixtures import sample_documents, sample_instrument, sample_klines
from quant_exchange.intelligence import IntelligenceEngine


class StrategyTests(unittest.TestCase):
    def _build_context(
        self,
        prices: list[float],
        *,
        bias_score: float,
        bias_direction: Direction,
    ) -> StrategyContext:
        """Build a deterministic strategy context for factor and signal tests."""

        bars = sample_klines(prices)
        latest_bar = bars[-1]
        return StrategyContext(
            instrument=sample_instrument(),
            current_bar=latest_bar,
            history=tuple(bars),
            position=Position("BTCUSDT"),
            cash=100_000.0,
            equity=100_000.0,
            latest_bias=DirectionalBias(
                instrument_id="BTCUSDT",
                as_of=latest_bar.close_time,
                window=timedelta(days=1),
                score=bias_score,
                direction=bias_direction,
                confidence=0.8,
                supporting_documents=2,
            ),
        )

    def test_st_01_strategy_registry_and_template_init(self) -> None:
        strategy = MovingAverageSentimentStrategy()
        registry = StrategyRegistry()
        registry.register(strategy)
        self.assertIs(registry.get(strategy.strategy_id), strategy)

    def test_st_03_shared_factor_functions_are_deterministic(self) -> None:
        values = [1, 2, 3, 4, 5, 6]
        self.assertEqual(sma(values, 3), 5.0)
        self.assertAlmostEqual(momentum(values, 3), 1.0)
        self.assertGreaterEqual(rsi(values, 5), 50.0)
        self.assertGreater(zscore(values, 5), 0.0)

    def test_st_07_factor_functions_handle_empty_inputs_and_boundaries(self) -> None:
        self.assertEqual(ema([], 3), 0.0)
        self.assertEqual(ema([1.0, 2.0, 3.0], 0), 0.0)
        self.assertEqual(ema([5.0], 3), 5.0)
        self.assertEqual(realized_volatility([], 5), 0.0)
        self.assertEqual(realized_volatility([100.0], 5), 0.0)
        self.assertEqual(realized_volatility([100.0, 101.0], 1), 0.0)
        self.assertAlmostEqual(realized_volatility([100.0, 110.0, 121.0, 133.1], 3), 0.0)

    def test_st_08_factor_functions_match_known_expected_values(self) -> None:
        self.assertAlmostEqual(ema([10.0, 11.0, 12.0, 13.0], 3), 12.125)
        prices = [100.0, 110.0, 99.0, 108.9]
        expected = pstdev([0.10, -0.10, 0.10]) * sqrt(252)
        self.assertAlmostEqual(realized_volatility(prices, 3), expected)

    def test_st_09_signal_generation_changes_with_factor_windows(self) -> None:
        context = self._build_context(
            [100.0, 101.0, 102.0, 103.0, 104.0, 105.0],
            bias_score=0.20,
            bias_direction=Direction.LONG,
        )
        trend_following = MovingAverageSentimentStrategy(
            params={"fast_window": 2, "slow_window": 5, "volatility_cap": 100.0}
        )
        inverted_windows = MovingAverageSentimentStrategy(
            params={"fast_window": 5, "slow_window": 2, "volatility_cap": 100.0}
        )

        trend_signal = trend_following.generate_signal(context)
        filtered_signal = inverted_windows.generate_signal(context)

        self.assertEqual(trend_signal.reason, "trend_up_positive_bias")
        self.assertGreater(trend_signal.target_weight, 0.0)
        self.assertEqual(filtered_signal.reason, "signal_filtered")
        self.assertEqual(filtered_signal.target_weight, 0.0)

    def test_st_10_volatility_cap_scales_signal_weight(self) -> None:
        context = self._build_context(
            [100.0, 120.0, 90.0, 130.0, 85.0, 140.0],
            bias_score=0.20,
            bias_direction=Direction.LONG,
        )
        uncapped = MovingAverageSentimentStrategy(params={"volatility_cap": 100.0})
        capped = MovingAverageSentimentStrategy(params={"volatility_cap": 0.1})

        uncapped_signal = uncapped.generate_signal(context)
        capped_signal = capped.generate_signal(context)

        self.assertEqual(uncapped_signal.reason, "trend_up_positive_bias")
        self.assertEqual(capped_signal.reason, "trend_up_positive_bias")
        self.assertGreater(uncapped_signal.metadata["volatility"], capped.params["volatility_cap"])
        self.assertAlmostEqual(uncapped_signal.target_weight, 0.9)
        self.assertAlmostEqual(capped_signal.target_weight, 0.45)

    def test_st_06_signal_generation_is_reproducible(self) -> None:
        engine = IntelligenceEngine()
        documents = sample_documents()
        engine.ingest_documents(documents)
        bars = sample_klines()
        bias = engine.directional_bias("BTCUSDT", as_of=bars[-1].close_time)
        strategy = MovingAverageSentimentStrategy()
        context = StrategyContext(
            instrument=sample_instrument(),
            current_bar=bars[-1],
            history=tuple(bars),
            position=Position("BTCUSDT"),
            cash=100_000.0,
            equity=100_000.0,
            latest_bias=bias,
        )
        signal_1 = strategy.generate_signal(context)
        signal_2 = strategy.generate_signal(context)
        self.assertEqual(signal_1, signal_2)

    def test_st_04_future_bar_is_rejected_by_context_guard(self) -> None:
        engine = IntelligenceEngine()
        documents = sample_documents()
        engine.ingest_documents(documents)
        bars = sample_klines()
        bias = engine.directional_bias("BTCUSDT", as_of=bars[-1].close_time)
        future_history = tuple(list(bars[:-1]) + [bars[-1]])
        with self.assertRaisesRegex(ValueError, "lookahead_bias_detected"):
            StrategyContext(
                instrument=sample_instrument(),
                current_bar=bars[-2],
                history=future_history,
                position=Position("BTCUSDT"),
                cash=100_000.0,
                equity=100_000.0,
                latest_bias=bias,
            )


class FactorComprehensiveTests(unittest.TestCase):
    """Comprehensive tests for all technical analysis factor functions."""

    def test_rate_of_change(self) -> None:
        """Rate of Change calculates percentage change over window."""
        values = [100.0, 105.0, 110.0, 115.0, 120.0]
        # (120 - 100) / 100 * 100 = 20%
        self.assertAlmostEqual(rate_of_change(values, 4), 20.0)
        # (120 - 110) / 110 * 100 ≈ 9.09%
        self.assertAlmostEqual(rate_of_change(values, 2), 9.0909, places=3)

    def test_rate_of_change_edge_cases(self) -> None:
        """ROC handles empty inputs and insufficient data."""
        self.assertEqual(rate_of_change([], 3), 0.0)
        self.assertEqual(rate_of_change([100.0], 3), 0.0)
        self.assertEqual(rate_of_change([100.0, 105.0], 3), 0.0)
        self.assertEqual(rate_of_change([100.0, 0.0], 1), -100.0)  # division by zero handled

    def test_bollinger_percent_b(self) -> None:
        """Bollinger %B shows position of price relative to bands."""
        values = [100.0, 102.0, 104.0, 103.0, 101.0, 105.0, 107.0, 106.0, 108.0, 110.0,
                  112.0, 111.0, 113.0, 115.0, 114.0, 116.0, 118.0, 117.0, 119.0, 120.0]
        pb = bollinger_percent_b(values, window=20, num_std=2.0)
        # Value should be between 0 and 1 for price within bands
        self.assertGreaterEqual(pb, 0.0)
        self.assertLessEqual(pb, 1.0)

    def test_bollinger_percent_b_edge_cases(self) -> None:
        """Bollinger %B handles insufficient data."""
        self.assertEqual(bollinger_percent_b([], 20), 0.5)  # returns 0.5 when upper==lower
        self.assertEqual(bollinger_percent_b([100.0], 20), 0.5)
        self.assertEqual(bollinger_percent_b([100.0] * 10, 20), 0.5)

    def test_ewma_volatility(self) -> None:
        """EWMA volatility computes exponentially weighted volatility."""
        values = [100.0, 101.0, 102.0, 101.5, 103.0]
        vol = ewma_volatility(values, window=4, decay=0.94)
        self.assertGreaterEqual(vol, 0.0)

    def test_ewma_volatility_edge_cases(self) -> None:
        """EWMA volatility handles empty inputs."""
        self.assertEqual(ewma_volatility([], 5), 0.0)
        self.assertEqual(ewma_volatility([100.0], 5), 0.0)
        self.assertEqual(ewma_volatility([100.0, 100.0], 5), 0.0)

    def test_stochastic_k(self) -> None:
        """Stochastic %K measures position relative to high-low range."""
        highs = [110.0, 112.0, 111.0, 113.0, 115.0]
        lows = [98.0, 99.0, 97.0, 100.0, 101.0]
        closes = [105.0, 108.0, 106.0, 109.0, 112.0]
        k = stochastic_k(highs, lows, closes, window=3)
        # (112 - 97) / (115 - 97) * 100 = 83.33%
        self.assertAlmostEqual(k, 83.33, places=1)

    def test_stochastic_k_edge_cases(self) -> None:
        """Stochastic %K handles insufficient data."""
        self.assertEqual(stochastic_k([], [], [], 14), 50.0)
        self.assertEqual(stochastic_k([100.0], [99.0], [100.0], 14), 50.0)
        self.assertEqual(stochastic_k([110.0, 110.0], [100.0, 100.0], [105.0, 105.0], 2), 50.0)

    def test_williams_r(self) -> None:
        """Williams %R oscillator ranges from -100 to 0."""
        highs = [110.0, 112.0, 111.0, 113.0, 115.0]
        lows = [98.0, 99.0, 97.0, 100.0, 101.0]
        closes = [105.0, 108.0, 106.0, 109.0, 112.0]
        wr = williams_r(highs, lows, closes, window=3)
        # (115 - 112) / (115 - 97) * -100 = -16.67%
        self.assertAlmostEqual(wr, -16.67, places=1)

    def test_williams_r_edge_cases(self) -> None:
        """Williams %R handles insufficient data."""
        self.assertEqual(williams_r([], [], [], 14), -50.0)
        self.assertEqual(williams_r([100.0], [99.0], [100.0], 14), -50.0)
        self.assertEqual(williams_r([110.0, 110.0], [100.0, 100.0], [105.0, 105.0], 2), -50.0)

    def test_cci(self) -> None:
        """Commodity Channel Index measures deviation from average."""
        highs = [110.0, 112.0, 111.0, 113.0, 115.0]
        lows = [98.0, 99.0, 97.0, 100.0, 101.0]
        closes = [105.0, 108.0, 106.0, 109.0, 112.0]
        cci_val = cci(highs, lows, closes, window=4)
        # CCI should be a reasonable value (not 0 with enough data)
        self.assertNotEqual(cci_val, 0.0)

    def test_cci_edge_cases(self) -> None:
        """CCI handles insufficient data."""
        self.assertEqual(cci([], [], [], 20), 0.0)
        self.assertEqual(cci([100.0], [99.0], [100.0], 20), 0.0)

    def test_obv(self) -> None:
        """On-Balance Volume cumulates volume based on price direction."""
        closes = [100.0, 101.0, 99.5, 100.5, 102.0]
        volumes = [1000.0, 1500.0, 1200.0, 800.0, 2000.0]
        obv_val = obv(closes, volumes)
        # +1000 +1500 -1200 +800 +2000 = 3100
        self.assertEqual(obv_val, 3100.0)

    def test_obv_edge_cases(self) -> None:
        """OBV handles insufficient data."""
        self.assertEqual(obv([], []), 0.0)
        self.assertEqual(obv([100.0], [1000.0]), 0.0)

    def test_vwap(self) -> None:
        """Volume-Weighted Average Price."""
        highs = [105.0, 107.0, 106.0, 108.0, 110.0]
        lows = [95.0, 98.0, 97.0, 99.0, 101.0]
        closes = [100.0, 103.0, 102.0, 104.0, 106.0]
        volumes = [1000.0, 1500.0, 1200.0, 800.0, 2000.0]
        vwap_val = vwap(highs, lows, closes, volumes)
        self.assertGreater(vwap_val, 0.0)
        # VWAP should be between low and high prices
        self.assertGreaterEqual(vwap_val, min(closes))
        self.assertLessEqual(vwap_val, max(closes))

    def test_vwap_edge_cases(self) -> None:
        """VWAP handles empty inputs."""
        self.assertEqual(vwap([], [], [], []), 0.0)
        self.assertEqual(vwap([100.0], [99.0], [100.0], [0.0]), 0.0)

    def test_amihud_illiquidity(self) -> None:
        """Amihud illiquidity ratio measures price impact of volume."""
        closes = [100.0, 101.0, 100.5, 102.0, 101.5, 103.0, 102.5, 104.0, 103.5, 105.0]
        volumes = [1000000.0] * 10
        illiq = amihud_illiquidity(closes, volumes, window=5)
        self.assertGreaterEqual(illiq, 0.0)

    def test_amihud_illiquidity_edge_cases(self) -> None:
        """Amihud handles empty inputs."""
        self.assertEqual(amihud_illiquidity([], [], 20), 0.0)
        self.assertEqual(amihud_illiquidity([100.0], [1000.0], 20), 0.0)

    def test_pe_score(self) -> None:
        """PE score ranks P/E relative to sector median."""
        # P/E equal to sector median = 1.0 (best value per formula)
        self.assertEqual(pe_score(15.0, 15.0), 1.0)
        # Lower P/E = higher score (capped at 1.0)
        self.assertEqual(pe_score(10.0, 15.0), 1.0)
        # Higher P/E = lower score: ratio = 20/15 = 1.333, score = 1 - 0.333*0.5 = 0.833
        self.assertAlmostEqual(pe_score(20.0, 15.0), 0.8333, places=3)

    def test_pe_score_edge_cases(self) -> None:
        """PE score handles invalid inputs."""
        self.assertEqual(pe_score(0.0, 15.0), 0.5)
        self.assertEqual(pe_score(-10.0, 15.0), 0.5)
        self.assertEqual(pe_score(15.0, 0.0), 0.5)

    def test_roe_score(self) -> None:
        """ROE score normalizes return on equity to 0-1 range."""
        # ROE of 0.25 (25%) = 1.0
        self.assertEqual(roe_score(0.25), 1.0)
        # ROE of 0.125 (12.5%) = 0.5
        self.assertEqual(roe_score(0.125), 0.5)
        # ROE of 0 (break-even) = 0
        self.assertEqual(roe_score(0.0), 0.0)
        # Negative ROE should clamp to 0
        self.assertEqual(roe_score(-0.1), 0.0)

    def test_bollinger_bands(self) -> None:
        """Bollinger Bands return upper, middle, lower values."""
        values = [100.0] * 20
        upper, middle, lower = bollinger_bands(values, window=20, num_std=2.0)
        # All values same, so stddev = 0, bands collapse to mean
        self.assertEqual(upper, 100.0)
        self.assertEqual(middle, 100.0)
        self.assertEqual(lower, 100.0)

    def test_bollinger_bands_with_trend(self) -> None:
        """Bollinger Bands widen as price deviates from mean."""
        # Steady uptrend
        values = [100.0 + i for i in range(20)]
        upper, middle, lower = bollinger_bands(values, window=20, num_std=2.0)
        self.assertGreater(upper, middle)
        self.assertGreater(middle, lower)

    def test_atr(self) -> None:
        """Average True Range measures volatility."""
        highs = [110.0, 112.0, 111.0, 113.0, 115.0]
        lows = [98.0, 99.0, 97.0, 100.0, 101.0]
        closes = [105.0, 108.0, 106.0, 109.0, 112.0]
        atr_val = atr(highs, lows, closes, window=4)
        self.assertGreater(atr_val, 0.0)

    def test_atr_edge_cases(self) -> None:
        """ATR handles insufficient data."""
        self.assertEqual(atr([], [], [], 14), 0.0)
        self.assertEqual(atr([100.0], [99.0], [100.0], 14), 0.0)


if __name__ == "__main__":
    unittest.main()
