from __future__ import annotations

import unittest

from quant_exchange.backtest import BacktestEngine
from quant_exchange.core.models import RiskLimits
from quant_exchange.intelligence import IntelligenceEngine
from quant_exchange.risk import RiskEngine
from quant_exchange.strategy import MovingAverageSentimentStrategy

from .fixtures import sample_documents, sample_instrument, sample_klines


class BacktestTests(unittest.TestCase):
    def setUp(self) -> None:
        self.instrument = sample_instrument()
        self.klines = sample_klines()
        self.intelligence = IntelligenceEngine()
        self.intelligence.ingest_documents(sample_documents())
        self.strategy = MovingAverageSentimentStrategy()
        self.risk = RiskEngine(
            RiskLimits(
                max_order_notional=500_000.0,
                max_single_order_quantity=100_000.0,
                max_position_notional=500_000.0,
                max_gross_notional=500_000.0,
                max_leverage=5.0,
                max_drawdown=0.5,
            )
        )

    def _canonical_orders(self, result) -> list[tuple]:
        """Normalize order records so reproducibility checks ignore runtime UUIDs."""

        return [
            (
                order.request.client_order_id,
                order.request.side.value,
                round(order.request.quantity, 8),
                order.status.value,
                round(order.filled_quantity, 8),
                round(order.average_fill_price, 8),
            )
            for order in sorted(result.orders, key=lambda item: item.request.client_order_id)
        ]

    def _canonical_fills(self, result) -> list[tuple]:
        """Normalize fills so reproducibility checks ignore generated identifiers."""

        return [
            (
                fill.timestamp.isoformat(),
                fill.side.value,
                round(fill.quantity, 8),
                round(fill.price, 8),
                round(fill.fee, 8),
            )
            for fill in sorted(result.fills, key=lambda item: item.timestamp)
        ]

    def test_bt_01_backtest_runs_and_generates_orders(self) -> None:
        result = BacktestEngine().run(
            instrument=self.instrument,
            klines=self.klines,
            strategy=self.strategy,
            intelligence_engine=self.intelligence,
            risk_engine=self.risk,
            initial_cash=100_000.0,
        )
        self.assertGreater(len(result.equity_curve), 0)
        self.assertGreater(len(result.orders), 0)
        self.assertGreaterEqual(result.metrics.total_return, -1.0)

    def test_bt_05_backtest_is_reproducible(self) -> None:
        engine = BacktestEngine()
        result_1 = engine.run(
            instrument=self.instrument,
            klines=self.klines,
            strategy=self.strategy,
            intelligence_engine=self.intelligence,
            risk_engine=self.risk,
            initial_cash=100_000.0,
        )
        result_2 = engine.run(
            instrument=self.instrument,
            klines=self.klines,
            strategy=self.strategy,
            intelligence_engine=self.intelligence,
            risk_engine=self.risk,
            initial_cash=100_000.0,
        )
        self.assertEqual(result_1.equity_curve, result_2.equity_curve)
        self.assertEqual(result_1.metrics, result_2.metrics)

    def test_bt_06_cost_sensitivity_changes_returns(self) -> None:
        baseline = BacktestEngine(fee_rate=0.0001, slippage_bps=1).run(
            instrument=self.instrument,
            klines=self.klines,
            strategy=self.strategy,
            intelligence_engine=self.intelligence,
            risk_engine=self.risk,
            initial_cash=100_000.0,
        )
        expensive = BacktestEngine(fee_rate=0.01, slippage_bps=50).run(
            instrument=self.instrument,
            klines=self.klines,
            strategy=self.strategy,
            intelligence_engine=self.intelligence,
            risk_engine=self.risk,
            initial_cash=100_000.0,
        )
        self.assertLessEqual(expensive.metrics.total_return, baseline.metrics.total_return)

    def test_bt_07_factor_driven_signal_and_execution_are_stable(self) -> None:
        engine = BacktestEngine()
        result_1 = engine.run(
            instrument=self.instrument,
            klines=self.klines,
            strategy=self.strategy,
            intelligence_engine=self.intelligence,
            risk_engine=self.risk,
            initial_cash=100_000.0,
        )
        result_2 = engine.run(
            instrument=self.instrument,
            klines=self.klines,
            strategy=self.strategy,
            intelligence_engine=self.intelligence,
            risk_engine=self.risk,
            initial_cash=100_000.0,
        )
        self.assertEqual(result_1.bias_history, result_2.bias_history)
        self.assertEqual(self._canonical_orders(result_1), self._canonical_orders(result_2))
        self.assertEqual(self._canonical_fills(result_1), self._canonical_fills(result_2))

    def test_bt_08_future_bars_do_not_change_prefix_results(self) -> None:
        prefix_klines = sample_klines([100.0, 102.0, 104.0, 107.0, 109.0, 112.0])
        extended_klines = sample_klines([100.0, 102.0, 104.0, 107.0, 109.0, 112.0, 85.0, 83.0, 80.0])
        engine = BacktestEngine()

        prefix_result = engine.run(
            instrument=self.instrument,
            klines=prefix_klines,
            strategy=self.strategy,
            intelligence_engine=self.intelligence,
            risk_engine=self.risk,
            initial_cash=100_000.0,
        )
        extended_result = engine.run(
            instrument=self.instrument,
            klines=extended_klines,
            strategy=self.strategy,
            intelligence_engine=self.intelligence,
            risk_engine=self.risk,
            initial_cash=100_000.0,
        )

        prefix_order_count = len(self._canonical_orders(prefix_result))
        prefix_fill_count = len(self._canonical_fills(prefix_result))

        self.assertEqual(prefix_result.equity_curve, extended_result.equity_curve[: len(prefix_result.equity_curve)])
        self.assertEqual(prefix_result.bias_history, extended_result.bias_history[: len(prefix_result.bias_history)])
        self.assertEqual(
            self._canonical_orders(prefix_result),
            self._canonical_orders(extended_result)[:prefix_order_count],
        )
        self.assertEqual(
            self._canonical_fills(prefix_result),
            self._canonical_fills(extended_result)[:prefix_fill_count],
        )


if __name__ == "__main__":
    unittest.main()
