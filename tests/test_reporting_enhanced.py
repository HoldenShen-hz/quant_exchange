"""Tests for enhanced reporting features.

Tests:
- Trade detail report
- Attribution analysis
- Daily report generation
- Report scheduling
"""

from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from quant_exchange.core.models import (
    Alert,
    AlertSeverity,
    Direction,
    DirectionalBias,
    Fill,
    Order,
    OrderSide,
    OrderStatus,
    PortfolioSnapshot,
    Position,
)
from quant_exchange.reporting import ReportScheduler, ReportStatus, ReportingService


def utc_now() -> datetime:
    """Return a timezone-aware UTC timestamp."""
    return datetime.now(timezone.utc)


class TradeDetailReportTests(unittest.TestCase):
    """Test trade detail report generation (RP-01)."""

    def setUp(self) -> None:
        self.service = ReportingService()

    def test_trade_detail_report_generates_trades(self) -> None:
        """Verify trade detail report contains all fills."""
        fills = [
            Fill(
                fill_id="f1",
                order_id="o1",
                instrument_id="BTCUSDT",
                side=OrderSide.BUY,
                quantity=1.0,
                price=50000.0,
                timestamp=utc_now(),
                fee=50.0,
            ),
            Fill(
                fill_id="f2",
                order_id="o2",
                instrument_id="ETHUSDT",
                side=OrderSide.SELL,
                quantity=10.0,
                price=3000.0,
                timestamp=utc_now(),
                fee=30.0,
            ),
        ]

        report = self.service.trade_detail_report(fills=fills)

        self.assertEqual(report["total_trades"], 2)
        self.assertEqual(len(report["trades"]), 2)
        self.assertEqual(report["total_fees"], 80.0)

    def test_trade_detail_report_calculates_slippage(self) -> None:
        """Verify slippage is calculated when benchmark is provided."""
        fills = [
            Fill(
                fill_id="f1",
                order_id="o1",
                instrument_id="BTCUSDT",
                side=OrderSide.BUY,
                quantity=1.0,
                price=50100.0,  # 100 above benchmark
                timestamp=utc_now(),
                fee=50.0,
            ),
        ]

        report = self.service.trade_detail_report(fills=fills, benchmark_price=50000.0)

        self.assertEqual(report["total_trades"], 1)
        self.assertGreater(report["avg_slippage_bps"], 0.0)


class AttributionAnalysisTests(unittest.TestCase):
    """Test attribution analysis (RP-04)."""

    def setUp(self) -> None:
        self.service = ReportingService()

    def test_attribution_analysis_calculates_returns(self) -> None:
        """Verify attribution analysis calculates correct returns."""
        positions = {
            "BTCUSDT": Position(
                instrument_id="BTCUSDT",
                quantity=1.0,
                average_cost=50000.0,
                realized_pnl=1000.0,
                last_price=51000.0,
            ),
            "ETHUSDT": Position(
                instrument_id="ETHUSDT",
                quantity=10.0,
                average_cost=3000.0,
                realized_pnl=-500.0,
                last_price=2950.0,
            ),
        }

        result = self.service.attribution_analysis(positions=positions)

        self.assertEqual(result.total_return, 500.0)
        self.assertEqual(result.return_attribution["BTCUSDT"], 1000.0)
        self.assertEqual(result.return_attribution["ETHUSDT"], -500.0)

    def test_attribution_analysis_sector_attribution(self) -> None:
        """Verify sector attribution is calculated."""
        positions = {
            "BTCUSDT": Position(
                instrument_id="BTCUSDT",
                quantity=1.0,
                average_cost=50000.0,
                realized_pnl=1000.0,
                last_price=51000.0,
            ),
            "ETHUSDT": Position(
                instrument_id="ETHUSDT",
                quantity=10.0,
                average_cost=3000.0,
                realized_pnl=-500.0,
                last_price=2950.0,
            ),
        }
        sector_mapping = {
            "BTCUSDT": "Crypto",
            "ETHUSDT": "Crypto",
        }

        result = self.service.attribution_analysis(
            positions=positions,
            sector_mapping=sector_mapping,
        )

        self.assertIn("Crypto", result.sector_attribution)
        self.assertEqual(result.sector_attribution["Crypto"], 500.0)

    def test_attribution_analysis_top_contributors(self) -> None:
        """Verify top contributors and detractors are identified."""
        positions = {
            "BTCUSDT": Position(
                instrument_id="BTCUSDT",
                quantity=1.0,
                average_cost=50000.0,
                realized_pnl=1000.0,
                last_price=51000.0,
            ),
            "ETHUSDT": Position(
                instrument_id="ETHUSDT",
                quantity=10.0,
                average_cost=3000.0,
                realized_pnl=-500.0,
                last_price=2950.0,
            ),
        }

        result = self.service.attribution_analysis(positions=positions)

        self.assertEqual(len(result.top_contributors), 1)
        self.assertEqual(result.top_contributors[0]["instrument_id"], "BTCUSDT")
        self.assertEqual(len(result.top_detractors), 1)
        self.assertEqual(result.top_detractors[0]["instrument_id"], "ETHUSDT")


class DailyReportTests(unittest.TestCase):
    """Test daily report generation (RP-05)."""

    def setUp(self) -> None:
        self.service = ReportingService()

    def test_daily_report_includes_all_sections(self) -> None:
        """Verify daily report includes all required sections."""
        snapshot = PortfolioSnapshot(
            timestamp=utc_now(),
            cash=100000.0,
            positions_value=50000.0,
            equity=150000.0,
            gross_exposure=50000.0,
            net_exposure=30000.0,
            leverage=1.33,
            drawdown=0.05,
        )
        positions = {
            "BTCUSDT": Position(
                instrument_id="BTCUSDT",
                quantity=1.0,
                average_cost=50000.0,
                realized_pnl=1000.0,
                last_price=51000.0,
            ),
        }
        fills = [
            Fill(
                fill_id="f1",
                order_id="o1",
                instrument_id="BTCUSDT",
                side=OrderSide.BUY,
                quantity=1.0,
                price=50000.0,
                timestamp=utc_now(),
                fee=50.0,
            ),
        ]
        alerts = []

        report = self.service.daily_report(
            account_id="acc1",
            snapshot=snapshot,
            positions=positions,
            fills=fills,
            alerts=alerts,
        )

        self.assertIn("report_id", report)
        self.assertIn("account_id", report)
        self.assertIn("report_date", report)
        self.assertIn("account_summary", report)
        self.assertIn("positions", report)
        self.assertIn("trade_summary", report)
        self.assertIn("risk_alerts", report)

    def test_daily_report_trade_summary(self) -> None:
        """Verify daily report trade summary is correct."""
        snapshot = PortfolioSnapshot(
            timestamp=utc_now(),
            cash=100000.0,
            positions_value=0.0,
            equity=100000.0,
            gross_exposure=0.0,
            net_exposure=0.0,
            leverage=1.0,
            drawdown=0.0,
        )
        positions = {
            "BTCUSDT": Position(
                instrument_id="BTCUSDT",
                quantity=1.0,
                average_cost=50000.0,
                realized_pnl=1000.0,
                last_price=51000.0,
            ),
        }
        fills = [
            Fill(
                fill_id="f1",
                order_id="o1",
                instrument_id="BTCUSDT",
                side=OrderSide.BUY,
                quantity=1.0,
                price=50000.0,
                timestamp=utc_now(),
                fee=50.0,
            ),
        ]

        report = self.service.daily_report(
            account_id="acc1",
            snapshot=snapshot,
            positions=positions,
            fills=fills,
            alerts=[],
        )

        self.assertEqual(report["trade_summary"]["trade_count"], 1)
        self.assertEqual(report["trade_summary"]["total_pnl"], 1000.0)
        self.assertEqual(report["trade_summary"]["total_fees"], 50.0)
        self.assertEqual(report["trade_summary"]["net_pnl"], 950.0)


class ReportSchedulerTests(unittest.TestCase):
    """Test report scheduling functionality."""

    def setUp(self) -> None:
        self.scheduler = ReportScheduler()

    def test_schedule_daily_report(self) -> None:
        """Verify daily report can be scheduled."""
        task = self.scheduler.schedule_daily_report(account_id="acc1")

        self.assertIsNotNone(task.task_id)
        self.assertEqual(task.account_id, "acc1")
        self.assertEqual(task.status, ReportStatus.PENDING)

    def test_get_task(self) -> None:
        """Verify task can be retrieved by ID."""
        task = self.scheduler.schedule_daily_report(account_id="acc1")

        retrieved = self.scheduler.get_task(task.task_id)

        self.assertIsNotNone(retrieved)
        self.assertEqual(retrieved.task_id, task.task_id)

    def test_list_tasks_with_filters(self) -> None:
        """Verify tasks can be filtered."""
        self.scheduler.schedule_daily_report(account_id="acc1")
        self.scheduler.schedule_daily_report(account_id="acc2")

        acc1_tasks = self.scheduler.list_tasks(account_id="acc1")
        pending_tasks = self.scheduler.list_tasks(status=ReportStatus.PENDING)

        self.assertEqual(len(acc1_tasks), 1)
        self.assertEqual(acc1_tasks[0].account_id, "acc1")
        self.assertEqual(len(pending_tasks), 2)

    def test_update_task_status(self) -> None:
        """Verify task status can be updated."""
        task = self.scheduler.schedule_daily_report(account_id="acc1")

        self.scheduler.update_task_status(task.task_id, ReportStatus.RUNNING)

        updated = self.scheduler.get_task(task.task_id)
        self.assertEqual(updated.status, ReportStatus.RUNNING)
        self.assertIsNotNone(updated.started_at)

    def test_complete_task(self) -> None:
        """Verify task can be completed."""
        task = self.scheduler.schedule_daily_report(account_id="acc1")

        self.scheduler.update_task_status(task.task_id, ReportStatus.RUNNING)
        self.scheduler.update_task_status(task.task_id, ReportStatus.COMPLETED)

        updated = self.scheduler.get_task(task.task_id)
        self.assertEqual(updated.status, ReportStatus.COMPLETED)
        self.assertIsNotNone(updated.completed_at)


if __name__ == "__main__":
    unittest.main()
