"""End-to-end integration tests for complete trading workflows (端到端测试)."""

import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from quant_exchange.config import AppSettings
from quant_exchange.core.models import Instrument, Kline, MarketType, OrderSide
from quant_exchange.stocks.service import StockProfile
from quant_exchange.enhanced.services import ErrorRecoveryService, RecoveryPolicy
from quant_exchange.execution.oms import (
    AlgorithmOrder,
    ExecutionAlgorithmService,
    ExecutionAlgorithmType,
    TWAPExecutionAlgorithm,
    VWAPExecutionAlgorithm,
    IcebergOrderHandler,
    POVExecutionAlgorithm,
)
from quant_exchange.platform import QuantTradingPlatform
from quant_exchange.strategy import MovingAverageSentimentStrategy


def _synth_bars(instrument_id: str, n: int = 120) -> list[Kline]:
    now = datetime(2024, 1, 1, tzinfo=timezone.utc)
    bars = []
    price = 100.0
    for i in range(n):
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


class E2EBacktestWorkflowTest(unittest.TestCase):
    """E2E: Complete backtest workflow from data ingestion to results."""

    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        db_path = str(Path(self.temp_dir.name) / "e2e_backtest.sqlite3")
        self.platform = QuantTradingPlatform(AppSettings.from_mapping({"database": {"url": db_path}}))

    def tearDown(self) -> None:
        self.platform.close()
        self.temp_dir.cleanup()

    def test_full_backtest_workflow(self) -> None:
        """Test complete backtest: ingest bars → run backtest → verify results."""
        from quant_exchange.core.models import MarketType
        instrument = Instrument(instrument_id="E2E:001", symbol="E2E", market=MarketType.STOCK)
        bars = _synth_bars("E2E:001", 120)

        # Ingest bars into market data store
        self.platform.market_data.ingest_klines(bars)

        # Run backtest
        strategy = MovingAverageSentimentStrategy(strategy_id="e2e_ma", params={"fast_window": 3, "slow_window": 5})
        result = self.platform.backtest.run(
            instrument=instrument,
            klines=bars,
            strategy=strategy,
            intelligence_engine=None,
            risk_engine=None,
        )
        self.assertIsNotNone(result)
        self.assertGreaterEqual(result.metrics.total_trades, 0)
        print(f"\n[E2E] Backtest workflow: {result.metrics.total_trades} trades, return={result.metrics.total_return:.2%}")


class E2EBotLifecycleTest(unittest.TestCase):
    """E2E: Complete bot lifecycle via ControlPlaneAPI."""

    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        db_path = str(Path(self.temp_dir.name) / "e2e_bot.sqlite3")
        self.platform = QuantTradingPlatform(AppSettings.from_mapping({"database": {"url": db_path}}))
        # Register a test instrument in the stock directory
        instr = Instrument(instrument_id="E2E:002", symbol="E2E:002", market=MarketType.STOCK)
        profile = StockProfile(instrument_id="E2E:002", symbol="E2E:002", company_name="E2E Corp", market_region="US", exchange_code="NASDAQ", board="Test", sector="Technology", industry="Software")
        self.platform.stocks.upsert_stock(instr, profile)

    def tearDown(self) -> None:
        self.platform.close()
        self.temp_dir.cleanup()

    def test_bot_create_start_stop(self) -> None:
        """Test bot creation → start → stop lifecycle via API."""
        created = self.platform.api.create_strategy_bot(
            template_code="ma_sentiment",
            instrument_id="E2E:002",
            bot_name="E2E Test Bot",
            mode="paper",
            params={"fast_window": 3, "slow_window": 5},
        )
        self.assertEqual(created["code"], "OK")
        bot_id = created["data"]["bot_id"]
        self.assertEqual(created["data"]["status"], "draft")

        started = self.platform.api.start_strategy_bot(bot_id)
        self.assertEqual(started["code"], "OK")
        self.assertEqual(started["data"]["status"], "running")

        stopped = self.platform.api.stop_strategy_bot(bot_id)
        self.assertEqual(stopped["code"], "OK")
        self.assertEqual(stopped["data"]["status"], "stopped")
        print(f"\n[E2E] Bot lifecycle: {bot_id} created -> started -> stopped")


class E2EAlgorithmOrderTest(unittest.TestCase):
    """E2E: Algorithm order submission and slice computation."""

    def test_twap_order_flow(self) -> None:
        """Test TWAP algorithm order from submission to slice generation."""
        ems = ExecutionAlgorithmService()

        order = ems.submit_algorithm_order(
            instrument_id="E2E:FUT",
            side=OrderSide.BUY,
            quantity=1000.0,
            algo_type=ExecutionAlgorithmType.TWAP,
            limit_price=5000.0,
            params={"num_slices": 5},
        )
        self.assertIsNotNone(order.algo_order_id)
        self.assertEqual(len(order.slices), 5)
        self.assertAlmostEqual(sum(s.quantity for s in order.slices), 1000.0, places=4)

        metrics = ems.get_algorithm_metrics(order.algo_order_id)
        self.assertEqual(metrics["algo_type"], "twap")
        self.assertEqual(metrics["slice_count"], 5)
        print(f"\n[E2E] TWAP order: {order.algo_order_id}, {len(order.slices)} slices")

    def test_vwap_order_flow(self) -> None:
        """Test VWAP algorithm order."""
        ems = ExecutionAlgorithmService()

        order = ems.submit_algorithm_order(
            instrument_id="E2E:FUT",
            side=OrderSide.SELL,
            quantity=2000.0,
            algo_type=ExecutionAlgorithmType.VWAP,
        )
        self.assertIsNotNone(order.algo_order_id)
        self.assertGreater(len(order.slices), 0)
        print(f"\n[E2E] VWAP order: {order.algo_order_id}, {len(order.slices)} slices")

    def test_iceberg_order_flow(self) -> None:
        """Test Iceberg order with hidden quantity."""
        ems = ExecutionAlgorithmService()

        order = ems.submit_algorithm_order(
            instrument_id="E2E:FUT",
            side=OrderSide.BUY,
            quantity=50_000.0,
            algo_type=ExecutionAlgorithmType.ICEBERG,
            limit_price=200.0,
        )
        self.assertIsNotNone(order.algo_order_id)
        visible = sum(s.quantity for s in order.slices)
        self.assertLess(visible, 50_000.0)
        self.assertGreaterEqual(visible, 50_000.0 * 0.01)
        print(f"\n[E2E] ICEBERG order: {order.algo_order_id}, visible={visible:.2f}/50000.0")

    def test_algorithm_slice_computation(self) -> None:
        """Test TWAP, VWAP, POV, Iceberg slice computation directly."""
        # TWAP
        twap = TWAPExecutionAlgorithm(num_slices=10, interval_seconds=60)
        slices = twap.compute_slices("ORDER:TWAP", 1000.0, 5000.0, "primary")
        self.assertEqual(len(slices), 10)
        self.assertAlmostEqual(sum(s.quantity for s in slices), 1000.0, places=4)

        # VWAP
        vwap = VWAPExecutionAlgorithm(num_slices=8)
        slices = vwap.compute_slices("ORDER:VWAP", 1000.0, None, "primary")
        self.assertGreater(len(slices), 0)
        self.assertAlmostEqual(sum(s.quantity for s in slices), 1000.0, places=2)

        # POV
        pov = POVExecutionAlgorithm(participation_rate=0.15)
        qty = pov.compute_slice_quantity("ORDER:POV", 100_000.0, 500.0, None)
        self.assertGreater(qty, 0)
        self.assertLessEqual(qty, 100_000.0 * 0.15 * 1.5)  # Within reasonable bounds

        # Iceberg
        iceberg = IcebergOrderHandler(visible_ratio=0.05)
        visible = iceberg.compute_visible_quantity(50_000.0, 50_000.0)
        self.assertGreater(visible, 0)
        self.assertLess(visible, 50_000.0)
        print(f"\n[E2E] Algorithm slices: TWAP={len(slices)}, VWAP={len(slices)}, POV={qty:.2f}, Iceberg visible={visible:.2f}")


class E2EErrorRecoveryTest(unittest.TestCase):
    """E2E: Error recovery across network failures."""

    def test_retry_with_exponential_backoff(self) -> None:
        """Test that errors are retried with backoff and eventually recovered."""
        svc = ErrorRecoveryService(persistence=None)
        attempts = []

        def flaky_op():
            attempts.append(len(attempts) + 1)
            if len(attempts) < 3:
                raise ConnectionError("simulated network error")
            return "success"

        result = svc.execute_with_recovery(
            operation_name="test_network_op",
            fn=flaky_op,
            policy=RecoveryPolicy(max_retries=5, base_delay_seconds=0.001),
        )

        self.assertTrue(result.success)
        self.assertEqual(result.attempts, 3)
        self.assertTrue(result.recovered)
        print(f"\n[E2E] Retry: recovered after {result.attempts} attempts")

    def test_circuit_breaker_opens(self) -> None:
        """Test circuit breaker opens after repeated failures."""
        svc = ErrorRecoveryService(persistence=None)

        def always_fail():
            raise ConnectionError("always failing")

        result = svc.execute_with_recovery(
            operation_name="test_circuit",
            fn=always_fail,
            policy=RecoveryPolicy(max_retries=1, circuit_failure_threshold=2, circuit_timeout_seconds=30.0),
        )

        state = svc.get_circuit_state("test_circuit")
        self.assertEqual(state, "OPEN")
        print(f"\n[E2E] Circuit breaker: state={state} after repeated failures")

    def test_fallback_on_exhausted_retries(self) -> None:
        """Test fallback is used when all retries are exhausted."""
        svc = ErrorRecoveryService(persistence=None)

        def always_fail():
            raise ValueError("permanent error")

        def fallback():
            return "fallback_result"

        result = svc.execute_with_recovery(
            operation_name="test_fallback",
            fn=always_fail,
            fallback_fn=fallback,
            policy=RecoveryPolicy(max_retries=2, base_delay_seconds=0.001),
        )

        self.assertTrue(result.success)
        self.assertTrue(result.fallback_used)
        self.assertEqual(result.result, "fallback_result")
        print(f"\n[E2E] Fallback: used after exhausted retries")


if __name__ == "__main__":
    unittest.main(verbosity=2)
