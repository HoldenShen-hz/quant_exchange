from __future__ import annotations

import unittest

from quant_exchange.core.models import Kline, OrderRequest, OrderSide, OrderType, PortfolioSnapshot, RiskLimits
from quant_exchange.execution import OrderManager, PaperExecutionEngine
from quant_exchange.risk import RiskEngine

from .fixtures import sample_klines


class ExecutionAndRiskTests(unittest.TestCase):
    def setUp(self) -> None:
        self.bar = sample_klines([100.0, 101.0])[1]

    def test_ex_01_market_order_is_filled_with_slippage(self) -> None:
        oms = OrderManager()
        request = OrderRequest("market_buy", "BTCUSDT", OrderSide.BUY, 1.0)
        order = oms.submit_order(request)
        fills = PaperExecutionEngine(slippage_bps=10).execute_on_bar(order, self.bar)
        self.assertEqual(len(fills), 1)
        oms.apply_fill(fills[0])
        self.assertEqual(order.status.value, "filled")
        self.assertGreaterEqual(fills[0].price, self.bar.open)

    def test_ex_02_limit_order_can_stay_unfilled(self) -> None:
        oms = OrderManager()
        request = OrderRequest("limit_buy", "BTCUSDT", OrderSide.BUY, 1.0, order_type=OrderType.LIMIT, price=90.0)
        order = oms.submit_order(request)
        fills = PaperExecutionEngine().execute_on_bar(order, self.bar)
        self.assertEqual(fills, [])
        self.assertEqual(order.status.value, "accepted")

    def test_pp_02_partial_fill_is_supported(self) -> None:
        oms = OrderManager()
        request = OrderRequest("partial_buy", "BTCUSDT", OrderSide.BUY, 2.0)
        order = oms.submit_order(request)
        fills = PaperExecutionEngine(max_fill_ratio=0.5).execute_on_bar(order, self.bar)
        self.assertEqual(len(fills), 1)
        oms.apply_fill(fills[0])
        self.assertEqual(order.status.value, "partially_filled")
        self.assertAlmostEqual(order.filled_quantity, 1.0)

    def test_rk_01_order_notional_limit_blocks_trade(self) -> None:
        limits = RiskLimits(max_order_notional=50.0, max_gross_notional=1_000.0, max_position_notional=1_000.0)
        risk = RiskEngine(limits)
        decision = risk.evaluate_order(
            OrderRequest("too_big", "BTCUSDT", OrderSide.BUY, 1.0),
            price=100.0,
            current_position_qty=0.0,
            snapshot=PortfolioSnapshot(
                timestamp=self.bar.close_time,
                cash=1000.0,
                positions_value=0.0,
                equity=1000.0,
                gross_exposure=0.0,
                net_exposure=0.0,
                leverage=0.0,
                drawdown=0.0,
            ),
        )
        self.assertFalse(decision.approved)
        self.assertIn("order_notional_limit", decision.reasons)

    def test_rk_02_kill_switch_blocks_orders(self) -> None:
        risk = RiskEngine()
        risk.activate_kill_switch()
        decision = risk.evaluate_order(
            OrderRequest("blocked", "BTCUSDT", OrderSide.BUY, 1.0),
            price=100.0,
            current_position_qty=0.0,
            snapshot=PortfolioSnapshot(
                timestamp=self.bar.close_time,
                cash=1000.0,
                positions_value=0.0,
                equity=1000.0,
                gross_exposure=0.0,
                net_exposure=0.0,
                leverage=0.0,
                drawdown=0.0,
            ),
        )
        self.assertFalse(decision.approved)
        self.assertIn("kill_switch_active", decision.reasons)


if __name__ == "__main__":
    unittest.main()
