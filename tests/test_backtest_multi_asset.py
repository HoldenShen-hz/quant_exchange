"""Tests for multi-asset backtesting and margin/leverage simulation."""

from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timedelta, timezone

from quant_exchange.backtest import BacktestResultStore, MultiAssetBacktestEngine
from quant_exchange.core.models import (
    Direction,
    DirectionalBias,
    FundingRate,
    Instrument,
    Kline,
    MarketType,
    Position,
)
from quant_exchange.intelligence import IntelligenceEngine
from quant_exchange.risk import RiskEngine
from quant_exchange.strategy import MovingAverageSentimentStrategy


def utc_now() -> datetime:
    """Return a timezone-aware UTC timestamp."""
    return datetime.now(timezone.utc)


def sample_klines(prices: list[float], instrument_id: str = "BTCUSDT") -> list[Kline]:
    """Build a list of sample K-lines from close prices."""
    klines = []
    base_time = utc_now() - timedelta(days=len(prices))
    for i, close in enumerate(prices):
        open_time = base_time + timedelta(days=i)
        klines.append(
            Kline(
                instrument_id=instrument_id,
                timeframe="1d",
                open_time=open_time,
                close_time=open_time + timedelta(hours=23, minutes=59, seconds=59),
                open=close * 0.99,
                high=close * 1.02,
                low=close * 0.98,
                close=close,
                volume=1000.0,
            )
        )
    return klines


def sample_instrument(instrument_id: str = "BTCUSDT", instrument_type: str = "spot") -> Instrument:
    """Create a sample instrument for testing."""
    return Instrument(
        instrument_id=instrument_id,
        symbol=instrument_id,
        market=MarketType.CRYPTO,
        instrument_type=instrument_type,
        lot_size=0.001,
    )


class MultiAssetBacktestEngineTests(unittest.TestCase):
    """Test multi-asset portfolio backtesting (BT-06)."""

    def setUp(self) -> None:
        self.engine = MultiAssetBacktestEngine(fee_rate=0.001, slippage_bps=5.0)
        self.intelligence = IntelligenceEngine()

    def test_multi_asset_backtest_runs_without_error(self) -> None:
        """Verify multi-asset backtest can run to completion."""
        btc = sample_instrument("BTCUSDT", "spot")
        eth = sample_instrument("ETHUSDT", "spot")

        btc_klines = sample_klines([100.0, 105.0, 110.0, 108.0, 112.0], "BTCUSDT")
        eth_klines = sample_klines([50.0, 52.0, 55.0, 53.0, 57.0], "ETHUSDT")

        strategy = MovingAverageSentimentStrategy()

        result = self.engine.run_multi_asset(
            instruments=[btc, eth],
            klines_by_instrument={"BTCUSDT": btc_klines, "ETHUSDT": eth_klines},
            strategy=strategy,
            intelligence_engine=self.intelligence,
            risk_engine=RiskEngine(),
            initial_cash=100_000.0,
        )

        self.assertIsNotNone(result)
        self.assertEqual(result.strategy_id, strategy.strategy_id)
        self.assertGreater(len(result.equity_curve), 0)

    def test_multi_asset_backtest_with_leverage(self) -> None:
        """Verify backtest can run with leverage settings."""
        btc = sample_instrument("BTCUSDT", "perpetual")

        btc_klines = sample_klines([100.0, 105.0, 110.0, 108.0, 112.0], "BTCUSDT")

        strategy = MovingAverageSentimentStrategy()

        result = self.engine.run_multi_asset(
            instruments=[btc],
            klines_by_instrument={"BTCUSDT": btc_klines},
            strategy=strategy,
            intelligence_engine=self.intelligence,
            risk_engine=RiskEngine(),
            initial_cash=100_000.0,
            leverage_by_instrument={"BTCUSDT": 2.0},
        )

        self.assertIsNotNone(result)
        self.assertGreater(len(result.equity_curve), 0)

    def test_multi_asset_backtest_with_funding_rates(self) -> None:
        """Verify funding rate accrual works for perpetual contracts."""
        btc = sample_instrument("BTCUSDT", "perpetual")

        btc_klines = sample_klines([100.0, 105.0, 110.0, 108.0, 112.0], "BTCUSDT")

        funding_rates = {
            "BTCUSDT": FundingRate(
                instrument_id="BTCUSDT",
                timestamp=utc_now(),
                funding_rate=0.0001,
            )
        }

        strategy = MovingAverageSentimentStrategy()

        result = self.engine.run_multi_asset(
            instruments=[btc],
            klines_by_instrument={"BTCUSDT": btc_klines},
            strategy=strategy,
            intelligence_engine=self.intelligence,
            risk_engine=RiskEngine(),
            initial_cash=100_000.0,
            leverage_by_instrument={"BTCUSDT": 2.0},
            funding_rates=funding_rates,
        )

        self.assertIsNotNone(result)


class BacktestResultStoreTests(unittest.TestCase):
    """Test backtest result persistence (BT-07)."""

    def setUp(self) -> None:
        self.temp_dir = tempfile.mkdtemp()
        self.store = BacktestResultStore(storage_path=self.temp_dir)

    def tearDown(self) -> None:
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_save_and_load_result(self) -> None:
        """Verify backtest results can be saved and loaded."""
        from quant_exchange.core.models import PerformanceMetrics

        equity_curve = [(utc_now(), 100000.0), (utc_now() + timedelta(days=1), 105000.0)]
        metrics = PerformanceMetrics(
            total_return=0.05,
            annualized_return=0.12,
            max_drawdown=0.02,
            sharpe=1.5,
            sortino=1.2,
            calmar=6.0,
            win_rate=0.6,
            profit_factor=1.5,
            turnover=1.0,
        )

        from quant_exchange.core.models import BacktestResult

        result = BacktestResult(
            strategy_id="test_strategy",
            instrument_id="BTCUSDT",
            equity_curve=tuple(equity_curve),
            orders=(),
            fills=(),
            metrics=metrics,
            alerts=(),
            bias_history=(),
            risk_rejections=(),
        )

        result_id = self.store.save(result)
        loaded = self.store.load(result_id)

        self.assertIsNotNone(loaded)
        self.assertEqual(loaded.strategy_id, "test_strategy")
        self.assertEqual(loaded.instrument_id, "BTCUSDT")

    def test_list_results_returns_sorted_results(self) -> None:
        """Verify list_results returns results sorted by return."""
        from quant_exchange.core.models import PerformanceMetrics

        equity_curve = [(utc_now(), 100000.0)]

        metrics1 = PerformanceMetrics(
            total_return=0.05, annualized_return=0.1, max_drawdown=0.01,
            sharpe=1.0, sortino=0.9, calmar=5.0, win_rate=0.5,
            profit_factor=1.2, turnover=0.8,
        )
        metrics2 = PerformanceMetrics(
            total_return=0.15, annualized_return=0.3, max_drawdown=0.02,
            sharpe=2.0, sortino=1.8, calmar=15.0, win_rate=0.7,
            profit_factor=2.0, turnover=1.2,
        )

        from quant_exchange.core.models import BacktestResult

        result1 = BacktestResult(
            strategy_id="s1",
            instrument_id="BTCUSDT",
            equity_curve=tuple(equity_curve),
            orders=(),
            fills=(),
            metrics=metrics1,
            alerts=(),
            bias_history=(),
            risk_rejections=(),
        )
        result2 = BacktestResult(
            strategy_id="s2",
            instrument_id="BTCUSDT",
            equity_curve=tuple(equity_curve),
            orders=(),
            fills=(),
            metrics=metrics2,
            alerts=(),
            bias_history=(),
            risk_rejections=(),
        )

        self.store.save(result1)
        self.store.save(result2)

        results = self.store.list_results()

        self.assertEqual(len(results), 2)
        self.assertEqual(results[0].strategy_id, "s2")


if __name__ == "__main__":
    unittest.main()
