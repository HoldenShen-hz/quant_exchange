from __future__ import annotations

from datetime import timedelta
from math import sqrt
from statistics import pstdev
import unittest

from quant_exchange.core.models import Direction, DirectionalBias, Position
from quant_exchange.strategy import MovingAverageSentimentStrategy, StrategyContext, StrategyRegistry
from quant_exchange.strategy.factors import ema, momentum, realized_volatility, rsi, sma, zscore

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


if __name__ == "__main__":
    unittest.main()
