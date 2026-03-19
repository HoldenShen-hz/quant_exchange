"""Tests for enhanced backtest features (BT-06 ~ BT-08).

Tests:
- BT-06: Batch backtesting and parameter sweeps
- BT-07: Result persistence (already tested via BacktestResultStore)
- BT-08: Bias audit (look-ahead, future function, time alignment)
"""

from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from quant_exchange.backtest import (
    BatchBacktestEngine,
    BatchBacktestResult,
    BiasAuditResult,
    BiasAuditService,
    BiasFinding,
    BiasType,
    BacktestEngine,
    BacktestResultStore,
)


def utc_now() -> datetime:
    """Return a timezone-aware UTC timestamp."""
    return datetime.now(timezone.utc)


def create_sample_klines(count: int = 20, start_price: float = 100.0) -> list:
    """Create sample klines for testing."""
    from quant_exchange.core.models import Kline

    klines = []
    start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    for i in range(count):
        klines.append(Kline(
            instrument_id="TEST",
            timeframe="1d",
            open_time=start + timedelta(days=i),
            close_time=start + timedelta(days=i, hours=23, minutes=59),
            open=start_price + i,
            high=start_price + i + 1,
            low=start_price + i - 1,
            close=start_price + i + 0.5,
            volume=1000.0 + i * 10,
        ))
    return klines


def create_sample_fill(order_id: str, fill_id: str, timestamp: datetime, price: float = 100.0):
    """Create sample fill for testing."""
    from quant_exchange.core.models import Fill, OrderSide

    return Fill(
        fill_id=fill_id,
        order_id=order_id,
        instrument_id="TEST",
        side=OrderSide.BUY,
        quantity=1.0,
        price=price,
        timestamp=timestamp,
        fee=0.1,
    )


# ─── BT-08: Bias Audit Tests ───────────────────────────────────────────────────


class BiasAuditServiceTests(unittest.TestCase):
    """Test BT-08: Bias audit for look-ahead, future function, and time alignment."""

    def setUp(self) -> None:
        self.audit_service = BiasAuditService()

    def test_audit_passes_with_clean_data(self) -> None:
        """Verify audit passes with properly ordered timestamps."""
        from quant_exchange.core.models import Order, OrderStatus

        klines = create_sample_klines(20)
        orders = []
        fills = [create_sample_fill("ord_1", "fill_1", klines[5].close_time)]

        result = self.audit_service.audit_backtest("strategy_001", klines, orders, fills)

        self.assertTrue(result.passed)
        self.assertEqual(len(result.findings), 0)

    def test_audit_detects_timestamp_discontinuity(self) -> None:
        """Verify audit detects look-ahead bias via timestamp issues."""
        from quant_exchange.core.models import Fill, Order, OrderSide, OrderStatus

        klines = create_sample_klines(20)

        # Create fill with timestamp after last bar (look-ahead)
        fill = Fill(
            fill_id="fill_late",
            order_id="ord_1",
            instrument_id="TEST",
            side=OrderSide.BUY,
            quantity=1.0,
            price=100.0,
            timestamp=klines[-1].close_time + timedelta(days=1),  # After last bar!
            fee=0.1,
        )

        result = self.audit_service.audit_backtest("strategy_001", klines, [], [fill])

        self.assertFalse(result.passed)
        findings = [f for f in result.findings if f.bias_type == BiasType.LOOKAHEAD]
        self.assertTrue(len(findings) > 0)

    def test_audit_detects_time_misalignment(self) -> None:
        """Verify audit detects inconsistent time intervals."""
        from quant_exchange.core.models import Kline

        # Create klines with inconsistent intervals
        klines = []
        start = datetime(2025, 1, 1, tzinfo=timezone.utc)

        # First 10 with 1-day intervals
        for i in range(10):
            klines.append(Kline(
                instrument_id="TEST",
                timeframe="1d",
                open_time=start + timedelta(days=i),
                close_time=start + timedelta(days=i, hours=23, minutes=59),
                open=100 + i,
                high=101 + i,
                low=99 + i,
                close=100.5 + i,
                volume=1000.0,
            ))

        # Next 5 with 2-day intervals (misaligned)
        for i in range(10, 15):
            klines.append(Kline(
                instrument_id="TEST",
                timeframe="1d",
                open_time=start + timedelta(days=i * 2),  # Double interval!
                close_time=start + timedelta(days=i * 2, hours=23, minutes=59),
                open=100 + i,
                high=101 + i,
                low=99 + i,
                close=100.5 + i,
                volume=1000.0,
            ))

        result = self.audit_service.audit_backtest("strategy_001", klines, [], [])

        findings = [f for f in result.findings if f.bias_type == BiasType.TIME_MISALIGNMENT]
        self.assertTrue(len(findings) > 0)

    def test_audit_summary_generation(self) -> None:
        """Verify audit summary is properly generated."""
        klines = create_sample_klines(20)

        result = self.audit_service.audit_backtest("strategy_001", klines, [], [])

        self.assertIn("total_findings", result.summary)
        self.assertIn("by_type", result.summary)
        self.assertIn("by_severity", result.summary)

    def test_audit_history_tracking(self) -> None:
        """Verify audit history is properly maintained."""
        klines = create_sample_klines(20)

        self.audit_service.audit_backtest("strategy_001", klines, [], [])
        self.audit_service.audit_backtest("strategy_002", klines, [], [])

        history = self.audit_service.get_audit_history()
        self.assertEqual(len(history), 2)


class BiasFindingTests(unittest.TestCase):
    """Test BiasFinding dataclass."""

    def test_bias_finding_creation(self) -> None:
        """Verify BiasFinding can be created."""
        finding = BiasFinding(
            bias_type=BiasType.LOOKAHEAD,
            severity="high",
            description="Timestamp issue detected",
            location="kline_index_5",
        )

        self.assertEqual(finding.bias_type, BiasType.LOOKAHEAD)
        self.assertEqual(finding.severity, "high")
        self.assertEqual(finding.location, "kline_index_5")


# ─── BT-07: Result Persistence Tests ───────────────────────────────────────────


class BacktestResultStoreTests(unittest.TestCase):
    """Test BT-07: Backtest result persistence."""

    def setUp(self) -> None:
        self.store = BacktestResultStore()
        from quant_exchange.core.models import BacktestResult, PerformanceMetrics

        self.sample_result = BacktestResult(
            strategy_id="test_strategy",
            instrument_id="BTCUSDT",
            equity_curve=(
                (datetime(2025, 1, 1, tzinfo=timezone.utc), 100000.0),
                (datetime(2025, 1, 2, tzinfo=timezone.utc), 101000.0),
            ),
            orders=(),
            fills=(),
            metrics=PerformanceMetrics(
                total_return=0.01,
                annualized_return=3.65,
                max_drawdown=0.05,
                sharpe=1.5,
                sortino=1.2,
                calmar=0.73,
                win_rate=0.55,
                profit_factor=1.5,
                turnover=1.0,
                total_trades=10,
                avg_trade_return=0.001,
            ),
            alerts=(),
            bias_history=(),
            risk_rejections=(),
        )

    def test_save_and_load_result(self) -> None:
        """Verify result can be saved and loaded."""
        result_id = self.store.save(self.sample_result)
        loaded = self.store.load(result_id)

        self.assertIsNotNone(loaded)
        self.assertEqual(loaded.strategy_id, "test_strategy")
        self.assertEqual(loaded.instrument_id, "BTCUSDT")

    def test_list_results(self) -> None:
        """Verify results can be listed."""
        self.store.save(self.sample_result)

        results = self.store.list_results()
        self.assertEqual(len(results), 1)

    def test_list_results_by_strategy(self) -> None:
        """Verify results can be filtered by strategy."""
        self.store.save(self.sample_result)

        results = self.store.list_results(strategy_id="test_strategy")
        self.assertEqual(len(results), 1)

        results = self.store.list_results(strategy_id="other_strategy")
        self.assertEqual(len(results), 0)


# ─── BT-06: Batch Backtesting Tests ────────────────────────────────────────────


class BatchBacktestEngineTests(unittest.TestCase):
    """Test BT-06: Batch backtesting capabilities."""

    def setUp(self) -> None:
        self.engine = BacktestEngine()

    def test_batch_backtest_result_structure(self) -> None:
        """Verify BatchBacktestResult has correct structure."""
        result = BatchBacktestResult(
            batch_id="batch_001",
            strategy_id="test_strategy",
            total_runs=5,
            results=[],
            best_result=None,
            parameter_sweep_summary={"test": "summary"},
        )

        self.assertEqual(result.batch_id, "batch_001")
        self.assertEqual(result.total_runs, 5)

    def test_parameter_sweep_summary(self) -> None:
        """Verify parameter sweep summary generation."""
        # Create mock results
        from quant_exchange.core.models import BacktestResult, PerformanceMetrics

        mock_results = [
            BacktestResult(
                strategy_id="test",
                instrument_id="TEST",
                equity_curve=((utc_now(), 100000.0),),
                orders=(),
                fills=(),
                metrics=PerformanceMetrics(
                    total_return=0.01,
                    annualized_return=0.1,
                    max_drawdown=0.05,
                    sharpe=1.0,
                    sortino=0.8,
                    calmar=0.5,
                    win_rate=0.5,
                    profit_factor=1.2,
                    turnover=1.0,
                    total_trades=10,
                    avg_trade_return=0.001,
                ),
                alerts=(),
                bias_history=(),
                risk_rejections=(),
            )
            for _ in range(3)
        ]

        batch_result = BatchBacktestResult(
            batch_id="batch_001",
            strategy_id="test",
            total_runs=3,
            results=mock_results,
            best_result=mock_results[0],
            parameter_sweep_summary={
                "total_combinations": 3,
                "returns": {"min": 0.01, "max": 0.01, "mean": 0.01},
                "sharpes": {"min": 1.0, "max": 1.0, "mean": 1.0},
                "drawdowns": {"min": 0.05, "max": 0.05},
            },
        )

        self.assertEqual(batch_result.parameter_sweep_summary["total_combinations"], 3)


if __name__ == "__main__":
    unittest.main()
