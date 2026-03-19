"""Reporting helpers for daily account, strategy, risk, and bias reports.

Implements the documented report types (RP-01 to RP-06):
- Daily account P&L report
- Strategy-level P&L report
- Risk event report
- Backtest-vs-live bias report
- Cost analysis report
- Trade detail report (RP-01)
- Attribution analysis (RP-04)
- Daily report task (RP-05)
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any

from quant_exchange.core.models import (
    Alert,
    AlertSeverity,
    Direction,
    DirectionalBias,
    Fill,
    Order,
    OrderSide,
    PerformanceMetrics,
    PortfolioSnapshot,
    Position,
)


def utc_now() -> datetime:
    """Return a timezone-aware UTC timestamp."""
    return datetime.now(timezone.utc)


class ReportStatus(str, Enum):
    """Status of a report generation task."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class DailyReportTask:
    """Scheduled daily report generation task."""

    task_id: str
    report_date: datetime
    account_id: str
    status: ReportStatus = ReportStatus.PENDING
    created_at: datetime = field(default_factory=utc_now)
    started_at: datetime | None = None
    completed_at: datetime | None = None
    error_message: str | None = None


@dataclass
class TradeDetail:
    """Detailed information about a single trade."""

    fill_id: str
    order_id: str
    instrument_id: str
    side: str
    quantity: float
    price: float
    fee: float
    timestamp: datetime
    pnl: float = 0.0
    slippage_bps: float = 0.0
    execution_delay_ms: int = 0


@dataclass
class AttributionResult:
    """Result of attribution analysis."""

    total_return: float
    return_attribution: dict[str, float]
    risk_attribution: dict[str, float]
    sector_attribution: dict[str, float]
    top_contributors: list[dict[str, Any]]
    top_detractors: list[dict[str, Any]]


class ReportingService:
    """Build compact reporting payloads from portfolio and strategy outputs."""

    def daily_summary(
        self,
        *,
        snapshot: PortfolioSnapshot,
        metrics: PerformanceMetrics | None = None,
        alerts: list[Alert] | None = None,
        bias: DirectionalBias | None = None,
    ) -> dict:
        """Return a compact daily summary suitable for dashboards or notifications."""

        return {
            "equity": round(snapshot.equity, 6),
            "cash": round(snapshot.cash, 6),
            "gross_exposure": round(snapshot.gross_exposure, 6),
            "net_exposure": round(snapshot.net_exposure, 6),
            "drawdown": round(snapshot.drawdown, 6),
            "leverage": round(snapshot.leverage, 6),
            "positions_value": round(snapshot.positions_value, 6),
            "metrics": metrics,
            "alert_count": len(alerts or []),
            "bias_direction": bias.direction.value if bias else None,
            "bias_score": round(bias.score, 6) if bias else None,
        }

    def strategy_summary(
        self,
        *,
        strategy_id: str,
        fills: list[Fill],
        positions: dict[str, Position],
    ) -> dict:
        """Return a strategy-level P&L summary."""

        total_fees = sum(f.fee for f in fills)
        total_notional = sum(abs(f.quantity * f.price) for f in fills)
        realized_pnl = sum(p.realized_pnl for p in positions.values())
        return {
            "strategy_id": strategy_id,
            "trade_count": len(fills),
            "total_notional": round(total_notional, 6),
            "total_fees": round(total_fees, 6),
            "realized_pnl": round(realized_pnl, 6),
        }

    def risk_summary(
        self,
        *,
        alerts: list[Alert],
        risk_rejections: int = 0,
    ) -> dict:
        """Return a risk event summary."""

        by_severity: dict[str, int] = {}
        for a in alerts:
            by_severity[a.severity.value] = by_severity.get(a.severity.value, 0) + 1
        return {
            "total_alerts": len(alerts),
            "by_severity": by_severity,
            "risk_rejections": risk_rejections,
            "critical_alerts": by_severity.get(AlertSeverity.CRITICAL.value, 0),
            "emergency_alerts": by_severity.get(AlertSeverity.EMERGENCY.value, 0),
        }

    def cost_analysis(
        self,
        *,
        fills: list[Fill],
    ) -> dict:
        """Return a detailed cost breakdown from fills."""

        total_fees = sum(f.fee for f in fills)
        total_notional = sum(abs(f.quantity * f.price) for f in fills)
        avg_fee_rate = total_fees / total_notional if total_notional > 0 else 0.0
        return {
            "total_fees": round(total_fees, 6),
            "total_notional": round(total_notional, 6),
            "avg_fee_rate": round(avg_fee_rate, 8),
            "trade_count": len(fills),
        }

    def bias_report(
        self,
        *,
        backtest_equity: list[tuple[Any, float]],
        live_equity: list[tuple[Any, float]],
    ) -> dict:
        """Compare backtest vs live equity curves to measure execution bias."""

        if not backtest_equity or not live_equity:
            return {"tracking_error": 0.0, "final_deviation": 0.0}
        bt_final = backtest_equity[-1][1]
        live_final = live_equity[-1][1]
        deviation = (live_final - bt_final) / bt_final if bt_final > 0 else 0.0
        return {
            "backtest_final_equity": round(bt_final, 6),
            "live_final_equity": round(live_final, 6),
            "final_deviation": round(deviation, 6),
            "backtest_periods": len(backtest_equity),
            "live_periods": len(live_equity),
        }

    def trade_detail_report(
        self,
        *,
        fills: list[Fill],
        orders: list[Order] | None = None,
        benchmark_price: float | None = None,
    ) -> dict:
        """Generate detailed trade report with execution quality metrics (RP-01)."""

        trades: list[TradeDetail] = []
        for fill in fills:
            slippage = 0.0
            if benchmark_price and benchmark_price > 0:
                price_diff = abs(fill.price - benchmark_price)
                slippage = (price_diff / benchmark_price) * 10000  # in bps

            trade = TradeDetail(
                fill_id=fill.fill_id,
                order_id=fill.order_id,
                instrument_id=fill.instrument_id,
                side=fill.side.value,
                quantity=fill.quantity,
                price=fill.price,
                fee=fill.fee,
                timestamp=fill.timestamp,
                slippage_bps=round(slippage, 2),
            )
            trades.append(trade)

        return {
            "trades": [
                {
                    "fill_id": t.fill_id,
                    "order_id": t.order_id,
                    "instrument_id": t.instrument_id,
                    "side": t.side,
                    "quantity": t.quantity,
                    "price": t.price,
                    "fee": t.fee,
                    "timestamp": t.timestamp.isoformat(),
                    "slippage_bps": t.slippage_bps,
                }
                for t in trades
            ],
            "total_trades": len(trades),
            "total_buy_value": sum(t.price * t.quantity for t in trades if t.side == OrderSide.BUY.value),
            "total_sell_value": sum(t.price * t.quantity for t in trades if t.side == OrderSide.SELL.value),
            "total_fees": sum(t.fee for t in trades),
            "avg_slippage_bps": sum(t.slippage_bps for t in trades) / len(trades) if trades else 0.0,
        }

    def attribution_analysis(
        self,
        *,
        positions: dict[str, Position],
        sector_mapping: dict[str, str] | None = None,
        benchmark_return: float = 0.0,
    ) -> AttributionResult:
        """Perform return and risk attribution analysis (RP-04)."""

        sector_mapping = sector_mapping or {}
        total_return = sum(p.realized_pnl for p in positions.values())

        # Return attribution by instrument
        return_attribution = {}
        for pos in positions.values():
            if pos.instrument_id:
                return_attribution[pos.instrument_id] = pos.realized_pnl

        # Sector attribution
        sector_returns: dict[str, float] = {}
        for pos in positions.values():
            sector = sector_mapping.get(pos.instrument_id, "Unknown")
            sector_returns[sector] = sector_returns.get(sector, 0.0) + pos.realized_pnl

        # Risk attribution (using position values as proxy)
        risk_attribution = {}
        total_exposure = sum(abs(pos.last_price * pos.quantity) for pos in positions.values())
        for pos in positions.values():
            if pos.instrument_id:
                exposure = abs(pos.last_price * pos.quantity)
                risk_pct = exposure / total_exposure if total_exposure > 0 else 0.0
                risk_attribution[pos.instrument_id] = round(risk_pct * 100, 2)

        # Top contributors and detractors
        sorted_positions = sorted(positions.values(), key=lambda p: p.realized_pnl, reverse=True)
        top_contributors = [
            {"instrument_id": p.instrument_id, "pnl": p.realized_pnl}
            for p in sorted_positions[:5] if p.realized_pnl > 0
        ]
        top_detractors = [
            {"instrument_id": p.instrument_id, "pnl": p.realized_pnl}
            for p in sorted_positions[-5:] if p.realized_pnl < 0
        ]

        return AttributionResult(
            total_return=total_return,
            return_attribution=return_attribution,
            risk_attribution=risk_attribution,
            sector_attribution=sector_returns,
            top_contributors=top_contributors,
            top_detractors=top_detractors,
        )

    def daily_report(
        self,
        *,
        account_id: str,
        snapshot: PortfolioSnapshot,
        positions: dict[str, Position],
        fills: list[Fill],
        alerts: list[Alert],
        metrics: PerformanceMetrics | None = None,
        report_date: datetime | None = None,
    ) -> dict:
        """Generate comprehensive daily report (RP-05)."""

        report_date = report_date or utc_now()
        total_pnl = sum(p.realized_pnl for p in positions.values())
        total_fees = sum(f.fee for f in fills)

        return {
            "report_id": str(uuid.uuid4()),
            "account_id": account_id,
            "report_date": report_date.isoformat(),
            "account_summary": self.daily_summary(
                snapshot=snapshot,
                metrics=metrics,
                alerts=alerts,
            ),
            "positions": [
                {
                    "instrument_id": p.instrument_id,
                    "quantity": p.quantity,
                    "average_cost": p.average_cost,
                    "last_price": p.last_price,
                    "realized_pnl": p.realized_pnl,
                    "unrealized_pnl": (p.last_price - p.average_cost) * p.quantity if p.quantity > 0 else 0.0,
                }
                for p in positions.values()
            ],
            "trade_summary": {
                "trade_count": len(fills),
                "total_pnl": round(total_pnl, 6),
                "total_fees": round(total_fees, 6),
                "net_pnl": round(total_pnl - total_fees, 6),
            },
            "risk_alerts": self.risk_summary(alerts=alerts),
        }


class ReportScheduler:
    """Schedule and manage daily report generation tasks."""

    def __init__(self) -> None:
        self._tasks: dict[str, DailyReportTask] = {}

    def schedule_daily_report(
        self,
        account_id: str,
        report_date: datetime | None = None,
    ) -> DailyReportTask:
        """Schedule a daily report generation task."""
        report_date = report_date or utc_now()
        task = DailyReportTask(
            task_id=str(uuid.uuid4()),
            report_date=report_date,
            account_id=account_id,
        )
        self._tasks[task.task_id] = task
        return task

    def get_task(self, task_id: str) -> DailyReportTask | None:
        """Get a report task by ID."""
        return self._tasks.get(task_id)

    def list_tasks(
        self,
        account_id: str | None = None,
        status: ReportStatus | None = None,
    ) -> list[DailyReportTask]:
        """List report tasks with optional filters."""
        tasks = list(self._tasks.values())
        if account_id:
            tasks = [t for t in tasks if t.account_id == account_id]
        if status:
            tasks = [t for t in tasks if t.status == status]
        return sorted(tasks, key=lambda t: t.created_at, reverse=True)

    def update_task_status(
        self,
        task_id: str,
        status: ReportStatus,
        error_message: str | None = None,
    ) -> bool:
        """Update task status."""
        task = self._tasks.get(task_id)
        if task is None:
            return False

        task.status = status
        if status == ReportStatus.RUNNING:
            task.started_at = utc_now()
        elif status in (ReportStatus.COMPLETED, ReportStatus.FAILED):
            task.completed_at = utc_now()
        if error_message:
            task.error_message = error_message

        return True
