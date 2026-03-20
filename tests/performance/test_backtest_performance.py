"""Performance benchmarks for backtest engine (性能压测)."""

import time
import unittest
from datetime import datetime, timedelta, timezone

from quant_exchange.backtest.engine import BacktestEngine
from quant_exchange.core.models import Instrument, Kline, MarketType
from quant_exchange.strategy import MovingAverageSentimentStrategy


def _synth_bars(instrument_id: str, count: int) -> list[Kline]:
    now = datetime(2024, 1, 1, tzinfo=timezone.utc)
    bars = []
    price = 100.0
    for i in range(count):
        t = now + timedelta(days=i)
        close = price + (i % 3 - 1) * 0.5
        bars.append(
            Kline(
                instrument_id=instrument_id,
                timeframe="1d",
                open_time=t,
                close_time=t + timedelta(days=1),
                open=price,
                high=max(price, close) + 0.2,
                low=min(price, close) - 0.2,
                close=close,
                volume=1_000_000.0 + i * 10_000,
            )
        )
        price = close
    return bars


class BacktestPerformanceTests(unittest.TestCase):
    """Benchmarks for BacktestEngine across different data scales."""

    def test_backtest_100_bars(self) -> None:
        """Benchmark backtest with 100 bars."""
        bars = _synth_bars("PERF:100", 100)
        strategy = MovingAverageSentimentStrategy(strategy_id="p100", params={"fast_window": 3, "slow_window": 5})
        instrument = Instrument(instrument_id="PERF:100", symbol="P100", market=MarketType.STOCK)
        engine = BacktestEngine()

        start = time.perf_counter()
        result = engine.run(instrument=instrument, klines=bars, strategy=strategy, intelligence_engine=None, risk_engine=None)
        elapsed = time.perf_counter() - start

        self.assertIsNotNone(result)
        print(f"\n[PERF] Backtest 100 bars: {elapsed*1000:.2f}ms, trades={result.metrics.total_trades}")

    def test_backtest_500_bars(self) -> None:
        """Benchmark backtest with 500 bars."""
        bars = _synth_bars("PERF:500", 500)
        strategy = MovingAverageSentimentStrategy(strategy_id="p500", params={"fast_window": 3, "slow_window": 5})
        instrument = Instrument(instrument_id="PERF:500", symbol="P500", market=MarketType.STOCK)
        engine = BacktestEngine()

        start = time.perf_counter()
        result = engine.run(instrument=instrument, klines=bars, strategy=strategy, intelligence_engine=None, risk_engine=None)
        elapsed = time.perf_counter() - start

        self.assertIsNotNone(result)
        print(f"\n[PERF] Backtest 500 bars: {elapsed*1000:.2f}ms, trades={result.metrics.total_trades}")

    def test_backtest_1000_bars(self) -> None:
        """Benchmark backtest with 1000 bars (stress test)."""
        bars = _synth_bars("PERF:1000", 1000)
        strategy = MovingAverageSentimentStrategy(strategy_id="p1000", params={"fast_window": 3, "slow_window": 5})
        instrument = Instrument(instrument_id="PERF:1000", symbol="P1000", market=MarketType.STOCK)
        engine = BacktestEngine()

        start = time.perf_counter()
        result = engine.run(instrument=instrument, klines=bars, strategy=strategy, intelligence_engine=None, risk_engine=None)
        elapsed = time.perf_counter() - start

        self.assertIsNotNone(result)
        print(f"\n[PERF] Backtest 1000 bars: {elapsed*1000:.2f}ms, trades={result.metrics.total_trades}")


if __name__ == "__main__":
    unittest.main(verbosity=2)
