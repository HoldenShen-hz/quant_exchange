from __future__ import annotations

import unittest
from datetime import datetime, timezone
from datetime import timedelta

from quant_exchange.core.models import AlertSeverity, Fill, OrderSide
from quant_exchange.monitoring import MonitoringService
from quant_exchange.portfolio import PortfolioManager
from quant_exchange.reporting import ReportingService

from .fixtures import sample_instrument


class PortfolioReportingMonitoringTests(unittest.TestCase):
    def setUp(self) -> None:
        self.instrument = sample_instrument()
        self.portfolio = PortfolioManager(initial_cash=10_000.0)
        self.portfolio.register_instrument(self.instrument)

    def test_pf_01_fill_updates_position_and_equity(self) -> None:
        fill = Fill(
            fill_id="fill_1",
            order_id="ord_1",
            instrument_id="BTCUSDT",
            side=OrderSide.BUY,
            quantity=1.0,
            price=100.0,
            timestamp=datetime.now(timezone.utc),
            fee=1.0,
        )
        position = self.portfolio.apply_fill(fill)
        snapshot = self.portfolio.mark_to_market({"BTCUSDT": 110.0})
        self.assertEqual(position.quantity, 1.0)
        self.assertAlmostEqual(snapshot.equity, 10009.0)

    def test_pf_02_rebalance_generates_orders(self) -> None:
        orders = self.portfolio.rebalance_orders({"BTCUSDT": 0.5}, {"BTCUSDT": 100.0})
        self.assertEqual(len(orders), 1)
        self.assertEqual(orders[0].side, OrderSide.BUY)

    def test_mo_01_drawdown_and_stale_data_create_alerts(self) -> None:
        monitoring = MonitoringService()
        snapshot = self.portfolio.mark_to_market({"BTCUSDT": 0.0})
        drawdown_alert = monitoring.check_drawdown(snapshot, 0.0)
        stale_alert = monitoring.check_stale_data(
            as_of=snapshot.timestamp + timedelta(hours=2),
            last_update=snapshot.timestamp,
            threshold=timedelta(minutes=30),
        )
        self.assertEqual(drawdown_alert.severity, AlertSeverity.CRITICAL)
        self.assertEqual(stale_alert.severity, AlertSeverity.WARNING)

    def test_rp_01_reporting_summary_contains_key_fields(self) -> None:
        snapshot = self.portfolio.mark_to_market({"BTCUSDT": 100.0})
        summary = ReportingService().daily_summary(snapshot=snapshot)
        self.assertIn("equity", summary)
        self.assertIn("drawdown", summary)

    def test_mo_suppress_and_unsuppress_alerts(self) -> None:
        monitoring = MonitoringService()
        future = datetime.now(timezone.utc) + timedelta(hours=1)
        # Suppress drawdown_threshold alerts
        monitoring.suppress_alerts("drawdown_threshold", future)
        self.assertTrue(monitoring.is_suppressed("drawdown_threshold"))
        # add_alert should return None when suppressed
        alert = monitoring.add_alert(
            "drawdown_threshold",
            AlertSeverity.CRITICAL,
            "Should be suppressed",
        )
        self.assertIsNone(alert)
        # Unsuppress
        monitoring.unsuppress_alerts("drawdown_threshold")
        self.assertFalse(monitoring.is_suppressed("drawdown_threshold"))
        alert = monitoring.add_alert(
            "drawdown_threshold",
            AlertSeverity.CRITICAL,
            "Should not be suppressed",
        )
        self.assertIsNotNone(alert)

    def test_mo_prometheus_metrics_output(self) -> None:
        monitoring = MonitoringService()
        metrics = monitoring.prometheus_metrics()
        self.assertIsInstance(metrics, str)
        # Should contain HELP and TYPE lines
        self.assertIn("alerts_total", metrics)


if __name__ == "__main__":
    unittest.main()
