"""Reporting helpers for daily account, strategy, risk, and bias reports.

Implements the documented report types (RP-01 to RP-06):
- Daily account P&L report
- Strategy-level P&L report
- Risk event report
- Backtest-vs-live bias report
- Cost analysis report
- Trade detail report (RP-01)
- Attribution analysis (RP-04)
- Daily/Weekly/Monthly report task (RP-05)
- Management and compliance reports (RP-06)
"""

from __future__ import annotations

import io
import uuid
import math
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Callable

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

    # ── Drift Analysis (RP-03 + PP-06) ─────────────────────────────────────────

    def slippage_analysis(
        self,
        *,
        fills: list[Fill],
        signal_prices: dict[str, float] | None = None,
    ) -> dict:
        """Analyze per-trade slippage vs signal prices (RP-03).

        Args:
            fills: List of executed fills.
            signal_prices: Dict of instrument_id → signal/scheduled price.
                           If None, uses the arrival price (open of next bar) as benchmark.
        Returns:
            Per-trade slippage breakdown and aggregate statistics.
        """
        if not fills:
            return {"total_trades": 0, "avg_slippage_bps": 0.0, "slippage_by_trade": []}

        slippage_by_trade = []
        total_slippage_bps = 0.0
        total_slippage_abs = 0.0

        for fill in fills:
            # Use signal price if provided, otherwise mark as "benchmark unavailable"
            if signal_prices and fill.instrument_id in signal_prices:
                benchmark = signal_prices[fill.instrument_id]
                if benchmark > 0:
                    slippage_abs = fill.price - benchmark
                    slippage_bps = (slippage_abs / benchmark) * 10000
                    # Positive slippage = worse execution (paid more than signal)
                    direction = "adverse" if ((fill.side == OrderSide.BUY and slippage_abs > 0) or
                                              (fill.side == OrderSide.SELL and slippage_abs < 0)) else "favorable"
                else:
                    slippage_bps = 0.0
                    slippage_abs = 0.0
                    direction = "unknown"
            else:
                slippage_bps = 0.0
                slippage_abs = 0.0
                direction = "no_benchmark"

            total_slippage_bps += slippage_bps
            total_slippage_abs += slippage_abs
            slippage_by_trade.append({
                "fill_id": fill.fill_id,
                "instrument_id": fill.instrument_id,
                "side": fill.side.value,
                "exec_price": fill.price,
                "benchmark_price": signal_prices.get(fill.instrument_id) if signal_prices else None,
                "slippage_bps": round(slippage_bps, 2),
                "slippage_abs": round(slippage_abs, 6),
                "direction": direction,
                "fee": fill.fee,
                "timestamp": fill.timestamp.isoformat(),
            })

        n = len(fills)
        return {
            "total_trades": n,
            "avg_slippage_bps": round(total_slippage_bps / n, 2),
            "total_slippage_bps": round(total_slippage_bps, 2),
            "total_slippage_abs": round(total_slippage_abs, 6),
            "adverse_trades": sum(1 for t in slippage_by_trade if t["direction"] == "adverse"),
            "favorable_trades": sum(1 for t in slippage_by_trade if t["direction"] == "favorable"),
            "slippage_by_trade": slippage_by_trade,
        }

    def signal_divergence(
        self,
        *,
        backtest_signals: list[dict],
        live_signals: list[dict],
    ) -> dict:
        """Detect signal divergence between backtest and live (RP-03, PP-06).

        Compares signal timing and direction at matching timestamps.
        Returns per-signal and aggregate divergence metrics.
        """
        if not backtest_signals or not live_signals:
            return {"divergence_score": 0.0, "signal_count": 0, "divergent_signals": []}

        # Index live signals by instrument + timestamp for matching
        live_index: dict[tuple, dict] = {}
        for sig in live_signals:
            key = (sig.get("instrument_id", ""), sig.get("timestamp"))
            live_index[key] = sig

        divergent_signals = []
        timing_diffs = []
        direction_mismatches = 0

        for bt_sig in backtest_signals:
            key = (bt_sig.get("instrument_id", ""), bt_sig.get("timestamp"))
            matching_live = live_index.get(key)

            if matching_live is None:
                # Missing in live - signal was dropped
                divergent_signals.append({
                    "instrument_id": bt_sig.get("instrument_id"),
                    "timestamp": bt_sig.get("timestamp"),
                    "backtest_signal": bt_sig.get("direction"),
                    "live_signal": None,
                    "status": "dropped_in_live",
                    "signal_value_bt": bt_sig.get("value"),
                })
                continue

            # Compare direction
            bt_dir = bt_sig.get("direction")
            live_dir = matching_live.get("direction")
            if bt_dir != live_dir:
                direction_mismatches += 1
                divergent_signals.append({
                    "instrument_id": bt_sig.get("instrument_id"),
                    "timestamp": bt_sig.get("timestamp"),
                    "backtest_signal": bt_dir,
                    "live_signal": live_dir,
                    "status": "direction_mismatch",
                    "signal_value_bt": bt_sig.get("value"),
                    "signal_value_live": matching_live.get("value"),
                })

            # Timing diff
            if "signal_time" in bt_sig and "signal_time" in matching_live:
                try:
                    t_bt = bt_sig["signal_time"]
                    t_live = matching_live["signal_time"]
                    if isinstance(t_bt, datetime) and isinstance(t_live, datetime):
                        diff_ms = abs((t_live - t_bt).total_seconds() * 1000)
                        timing_diffs.append(diff_ms)
                except Exception:
                    pass

        n = len(backtest_signals)
        divergence_score = (direction_mismatches + len(divergent_signals)) / n if n > 0 else 0.0
        avg_timing_diff_ms = sum(timing_diffs) / len(timing_diffs) if timing_diffs else 0.0

        return {
            "divergence_score": round(divergence_score, 4),
            "signal_count": n,
            "direction_mismatches": direction_mismatches,
            "divergent_signals": divergent_signals,
            "avg_timing_diff_ms": round(avg_timing_diff_ms, 2),
            "max_timing_diff_ms": round(max(timing_diffs), 2) if timing_diffs else 0.0,
        }

    def drift_score(
        self,
        *,
        backtest_equity: list[tuple[Any, float]],
        live_equity: list[tuple[Any, float]],
        backtest_trades: list[Fill] | None = None,
        live_trades: list[Fill] | None = None,
        signal_prices: dict[str, float] | None = None,
    ) -> dict:
        """Calculate composite drift score between backtest and live (RP-03, PP-06).

        Combines equity deviation, slippage, and signal divergence into a single
        actionable drift score (0-100, higher = more drift).
        """
        if not backtest_equity or not live_equity:
            return {"drift_score": 0.0, "components": {}}

        # 1. Equity deviation component (40% weight)
        bt_values = [e for _, e in backtest_equity]
        live_values = [e for _, e in live_equity]
        min_len = min(len(bt_values), len(live_values))

        if min_len >= 2:
            bt_returns = [(bt_values[i+1] - bt_values[i]) / bt_values[i]
                          for i in range(min_len - 1) if bt_values[i] != 0]
            live_returns = [(live_values[i+1] - live_values[i]) / live_values[i]
                            for i in range(min_len - 1) if live_values[i] != 0]
            if bt_returns and live_returns:
                mean_bt = sum(bt_returns) / len(bt_returns)
                mean_live = sum(live_returns) / len(live_returns)
                return_diff = abs(mean_live - mean_bt)
                equity_drift = min(return_diff * 1000, 1.0)  # Normalize to [0,1]
            else:
                equity_drift = 0.0
        else:
            equity_drift = 0.0

        # 2. Slippage component (30% weight)
        slippage_component = 0.0
        if backtest_trades and live_trades:
            # Compare slippage between backtest and live fills
            bt_slip = self.slippage_analysis(fills=backtest_trades, signal_prices=signal_prices)
            live_slip = self.slippage_analysis(fills=live_trades, signal_prices=signal_prices)
            slippage_diff = abs(bt_slip["avg_slippage_bps"] - live_slip["avg_slippage_bps"])
            slippage_component = min(slippage_diff / 50, 1.0)  # 50bps threshold → 1.0
        elif live_trades:
            live_slip = self.slippage_analysis(fills=live_trades, signal_prices=signal_prices)
            slippage_component = min(abs(live_slip["avg_slippage_bps"]) / 50, 1.0)

        # 3. Final equity deviation (30% weight)
        bt_final = backtest_equity[-1][1]
        live_final = live_equity[-1][1]
        final_deviation = abs((live_final - bt_final) / bt_final) if bt_final > 0 else 0.0
        final_drift = min(final_deviation * 10, 1.0)  # 10% deviation → 1.0

        # Composite score (0-100)
        composite = (0.40 * equity_drift + 0.30 * slippage_component + 0.30 * final_drift) * 100

        return {
            "drift_score": round(composite, 2),
            "components": {
                "equity_drift_normalized": round(equity_drift, 4),
                "slippage_component_normalized": round(slippage_component, 4),
                "final_deviation_normalized": round(final_drift, 4),
            },
            "raw_metrics": {
                "final_deviation_pct": round(final_deviation * 100, 4),
                "avg_backtest_return": round(mean_bt * 100, 4) if 'mean_bt' in dir() else None,
                "avg_live_return": round(mean_live * 100, 4) if 'mean_live' in dir() else None,
            },
            "drift_level": (
                "LOW" if composite < 20 else
                "MEDIUM" if composite < 50 else
                "HIGH" if composite < 80 else "CRITICAL"
            ),
        }

    def drift_recommendations(
        self,
        drift_result: dict,
        slippage_result: dict | None = None,
        divergence_result: dict | None = None,
    ) -> list[dict]:
        """Generate actionable recommendations based on drift analysis (RP-03, PP-06)."""
        recommendations = []
        score = drift_result.get("drift_score", 0)
        level = drift_result.get("drift_level", "LOW")

        # Drift-level recommendations
        if score >= 80:
            recommendations.append({
                "priority": "CRITICAL",
                "action": "Suspend live trading immediately",
                "reason": f"Drift score {score} exceeds 80 — severe deviation from backtest",
            })
        elif score >= 50:
            recommendations.append({
                "priority": "HIGH",
                "action": "Reduce position sizes by 50%",
                "reason": f"Drift score {score} indicates significant execution drift",
            })

        if level in ("HIGH", "CRITICAL"):
            recommendations.append({
                "priority": "HIGH",
                "action": "Review fill allocation and commission models",
                "reason": "Execution quality has diverged significantly from backtest assumptions",
            })

        # Slippage-based recommendations
        if slippage_result:
            avg_slip = slippage_result.get("avg_slippage_bps", 0)
            adverse_count = slippage_result.get("adverse_trades", 0)
            total = slippage_result.get("total_trades", 0)
            if total > 0 and adverse_count / total > 0.5:
                recommendations.append({
                    "priority": "MEDIUM",
                    "action": "Review order routing and liquidity provider settings",
                    "reason": f"{adverse_count}/{total} trades executed adversely vs signal price",
                })
            if abs(avg_slip) > 20:  # 20bps threshold
                recommendations.append({
                    "priority": "MEDIUM",
                    "action": "Tighten spread assumptions in backtest commission model",
                    "reason": f"Average slippage {avg_slip:.2f}bps significantly exceeds backtest",
                })

        # Divergence-based recommendations
        if divergence_result:
            div_score = divergence_result.get("divergence_score", 0)
            mismatches = divergence_result.get("direction_mismatches", 0)
            if mismatches > 0:
                recommendations.append({
                    "priority": "HIGH",
                    "action": "Audit signal generation — direction mismatches detected",
                    "reason": f"{mismatches} signals changed direction between backtest and live",
                })
            if div_score > 0.3:
                recommendations.append({
                    "priority": "MEDIUM",
                    "action": "Review market data feed latency and data一致性",
                    "reason": f"Signal divergence score {div_score:.2%} indicates data or timing issues",
                })

        if not recommendations:
            recommendations.append({
                "priority": "INFO",
                "action": "No immediate action required",
                "reason": "Drift metrics within normal ranges",
            })

        return recommendations

    def bias_report(
        self,
        *,
        backtest_equity: list[tuple[Any, float]],
        live_equity: list[tuple[Any, float]],
        backtest_trades: list[Fill] | None = None,
        live_trades: list[Fill] | None = None,
        signal_prices: dict[str, float] | None = None,
        backtest_signals: list[dict] | None = None,
        live_signals: list[dict] | None = None,
    ) -> dict:
        """Compare backtest vs live with enhanced drift analysis (RP-03, PP-06).

        Now includes per-trade slippage breakdown, signal divergence detection,
        composite drift score, and actionable recommendations.
        """
        if not backtest_equity or not live_equity:
            return {"tracking_error": 0.0, "final_deviation": 0.0}

        bt_final = backtest_equity[-1][1]
        live_final = live_equity[-1][1]
        deviation = (live_final - bt_final) / bt_final if bt_final > 0 else 0.0

        # Compute drift score
        drift = self.drift_score(
            backtest_equity=backtest_equity,
            live_equity=live_equity,
            backtest_trades=backtest_trades,
            live_trades=live_trades,
            signal_prices=signal_prices,
        )

        # Slippage analysis
        slippage = None
        if live_trades:
            slippage = self.slippage_analysis(fills=live_trades, signal_prices=signal_prices)

        # Signal divergence
        divergence = None
        if backtest_signals and live_signals:
            divergence = self.signal_divergence(
                backtest_signals=backtest_signals,
                live_signals=live_signals,
            )

        # Recommendations
        recommendations = self.drift_recommendations(drift, slippage, divergence)

        return {
            "backtest_final_equity": round(bt_final, 6),
            "live_final_equity": round(live_final, 6),
            "final_deviation": round(deviation, 6),
            "backtest_periods": len(backtest_equity),
            "live_periods": len(live_equity),
            "drift_score": drift,
            "slippage_analysis": slippage,
            "signal_divergence": divergence,
            "recommendations": recommendations,
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

    # ── Export Methods ─────────────────────────────────────────────────────────

    def export_trades_to_csv(self, fills: list[Fill]) -> str:
        """Export trade details to CSV format."""
        lines = ["fill_id,order_id,instrument_id,side,quantity,price,fee,pnl,slippage_bps,timestamp"]
        for f in fills:
            lines.append(
                f"{f.fill_id},{f.order_id},{f.instrument_id},{f.side.value},"
                f"{f.quantity},{f.price},{f.fee},{getattr(f, 'pnl', 0.0)},"
                f"{getattr(f, 'slippage_bps', 0.0)},{f.timestamp.isoformat()}"
            )
        return "\n".join(lines)

    def export_positions_to_csv(self, positions: dict[str, Position]) -> str:
        """Export position summary to CSV format."""
        lines = ["instrument_id,quantity,last_price,realized_pnl,unrealized_pnl,entry_price,sector"]
        for p in positions.values():
            lines.append(
                f"{p.instrument_id},{p.quantity},{p.last_price},"
                f"{getattr(p, 'realized_pnl', 0.0)},"
                f"{getattr(p, 'unrealized_pnl', 0.0)},"
                f"{getattr(p, 'entry_price', 0.0)},"
                f"{getattr(p, 'sector', '')}"
            )
        return "\n".join(lines)

    def export_report_to_json(self, report: dict) -> str:
        """Export a complete report dictionary to formatted JSON string."""
        import json
        return json.dumps(report, indent=2, default=str)

    # ── Weekly/Monthly Report Generation ────────────────────────────────────

    def weekly_report(
        self,
        *,
        account_id: str,
        snapshots: list[PortfolioSnapshot],
        positions: dict[str, Position],
        fills: list[Fill],
        alerts: list[Alert],
        metrics: PerformanceMetrics | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> dict:
        """Generate comprehensive weekly report (RP-05)."""
        end_date = end_date or utc_now()
        start_date = start_date or (end_date - timedelta(days=7))

        total_pnl = sum(p.realized_pnl for p in positions.values())
        total_fees = sum(f.fee for f in fills)
        trading_days = len(snapshots)

        # Calculate weekly metrics
        weekly_return = 0.0
        if len(snapshots) >= 2:
            weekly_return = (snapshots[-1].equity - snapshots[0].equity) / snapshots[0].equity if snapshots[0].equity > 0 else 0.0

        # Daily average metrics
        avg_daily_pnl = total_pnl / trading_days if trading_days > 0 else 0.0
        avg_daily_fees = total_fees / trading_days if trading_days > 0 else 0.0

        return {
            "report_id": str(uuid.uuid4()),
            "account_id": account_id,
            "report_type": "weekly",
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "trading_days": trading_days,
            "account_summary": {
                "starting_equity": snapshots[0].equity if snapshots else 0.0,
                "ending_equity": snapshots[-1].equity if snapshots else 0.0,
                "weekly_return_pct": round(weekly_return * 100, 4),
                "avg_daily_pnl": round(avg_daily_pnl, 6),
                "avg_daily_fees": round(avg_daily_fees, 6),
            },
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
                "total_trades": len(fills),
                "total_pnl": round(total_pnl, 6),
                "total_fees": round(total_fees, 6),
                "net_pnl": round(total_pnl - total_fees, 6),
            },
            "risk_alerts": self.risk_summary(alerts=alerts),
            "metrics": {
                "sharpe_ratio": metrics.sharpe if metrics else None,
                "max_drawdown": metrics.max_drawdown if metrics else None,
                "win_rate": metrics.win_rate if metrics else None,
                "profit_factor": metrics.profit_factor if metrics else None,
            } if metrics else {},
        }

    def monthly_report(
        self,
        *,
        account_id: str,
        snapshots: list[PortfolioSnapshot],
        positions: dict[str, Position],
        fills: list[Fill],
        alerts: list[Alert],
        metrics: PerformanceMetrics | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> dict:
        """Generate comprehensive monthly report (RP-05)."""
        end_date = end_date or utc_now()
        start_date = start_date or (end_date - timedelta(days=30))

        total_pnl = sum(p.realized_pnl for p in positions.values())
        total_fees = sum(f.fee for f in fills)
        trading_days = len(snapshots)

        # Calculate monthly metrics
        monthly_return = 0.0
        if len(snapshots) >= 2:
            monthly_return = (snapshots[-1].equity - snapshots[0].equity) / snapshots[0].equity if snapshots[0].equity > 0 else 0.0

        # Daily average metrics
        avg_daily_pnl = total_pnl / trading_days if trading_days > 0 else 0.0
        avg_daily_fees = total_fees / trading_days if trading_days > 0 else 0.0

        # Performance metrics summary
        perf_summary = {}
        if metrics:
            perf_summary = {
                "total_return": round(metrics.total_return, 6),
                "annualized_return": round(metrics.annualized_return, 6),
                "sharpe_ratio": round(metrics.sharpe, 4),
                "sortino_ratio": round(metrics.sortino, 4),
                "calmar_ratio": round(metrics.calmar, 4),
                "max_drawdown": round(metrics.max_drawdown, 6),
                "win_rate": round(metrics.win_rate, 4),
                "profit_factor": round(metrics.profit_factor, 4),
                "total_trades": metrics.total_trades,
                "avg_trade_return": round(metrics.avg_trade_return, 6),
            }

        # Sector attribution
        sector_pnl: dict[str, float] = {}
        for p in positions.values():
            sector = getattr(p, 'sector', 'Unknown')
            sector_pnl[sector] = sector_pnl.get(sector, 0.0) + p.realized_pnl

        return {
            "report_id": str(uuid.uuid4()),
            "account_id": account_id,
            "report_type": "monthly",
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "trading_days": trading_days,
            "account_summary": {
                "starting_equity": snapshots[0].equity if snapshots else 0.0,
                "ending_equity": snapshots[-1].equity if snapshots else 0.0,
                "monthly_return_pct": round(monthly_return * 100, 4),
                "avg_daily_pnl": round(avg_daily_pnl, 6),
                "avg_daily_fees": round(avg_daily_fees, 6),
            },
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
            "sector_attribution": sector_pnl,
            "trade_summary": {
                "total_trades": len(fills),
                "total_pnl": round(total_pnl, 6),
                "total_fees": round(total_fees, 6),
                "net_pnl": round(total_pnl - total_fees, 6),
            },
            "performance_metrics": perf_summary,
            "risk_alerts": self.risk_summary(alerts=alerts),
        }

    # ── PDF Export ──────────────────────────────────────────────────────────

    def export_report_to_pdf(
        self,
        report: dict,
        title: str = "QuantExchange Report",
    ) -> bytes:
        """Export a report to PDF format (RP-05, RP-06).

        Uses ReportLab-style generation for PDF output.
        Falls back to a simplified text-based PDF if ReportLab is not available.
        """
        try:
            from reportlab.lib import colors
            from reportlab.lib.pagesizes import letter
            from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
            from reportlab.lib.units import inch
            from reportlab.platypus import (
                Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle, PageBreak
            )
            from reportlab.lib.enums import TA_CENTER, TA_RIGHT

            buffer = io.BytesIO()
            doc = SimpleDocTemplate(buffer, pagesize=letter, topMargin=0.5*inch, bottomMargin=0.5*inch)

            styles = getSampleStyleSheet()
            styles.add(ParagraphStyle(name='ReportTitle', fontSize=18, alignment=TA_CENTER, spaceAfter=20))
            styles.add(ParagraphStyle(name='SectionHeader', fontSize=14, spaceBefore=15, spaceAfter=10))
            styles.add(ParagraphStyle(name='SubsectionHeader', fontSize=11, spaceBefore=10, spaceAfter=5))
            styles.add(ParagraphStyle(name='Footer', fontSize=8, alignment=TA_RIGHT, textColor=colors.gray))

            story = []

            # Title
            story.append(Paragraph(title, styles['ReportTitle']))
            story.append(Spacer(1, 0.2*inch))

            # Report metadata
            report_type = report.get('report_type', 'daily').upper()
            report_date = report.get('report_date', report.get('end_date', utc_now().isoformat()))
            story.append(Paragraph(f"<b>Report Type:</b> {report_type}", styles['Normal']))
            story.append(Paragraph(f"<b>Generated:</b> {report_date}", styles['Normal']))
            story.append(Paragraph(f"<b>Account:</b> {report.get('account_id', 'N/A')}", styles['Normal']))
            story.append(Spacer(1, 0.3*inch))

            # Account Summary Section
            story.append(Paragraph("Account Summary", styles['SectionHeader']))
            account_summary = report.get('account_summary', {})
            summary_data = [
                ["Metric", "Value"],
                ["Equity", f"${account_summary.get('equity', 0):,.2f}"],
                ["Cash", f"${account_summary.get('cash', 0):,.2f}"],
                ["Gross Exposure", f"${account_summary.get('gross_exposure', 0):,.2f}"],
                ["Net Exposure", f"${account_summary.get('net_exposure', 0):,.2f}"],
                ["Leverage", f"{account_summary.get('leverage', 0):.2f}x"],
                ["Drawdown", f"{account_summary.get('drawdown', 0) * 100:.2f}%"],
            ]
            if 'starting_equity' in account_summary:
                summary_data = [
                    ["Metric", "Value"],
                    ["Starting Equity", f"${account_summary.get('starting_equity', 0):,.2f}"],
                    ["Ending Equity", f"${account_summary.get('ending_equity', 0):,.2f}"],
                    ["Return", f"{account_summary.get('weekly_return_pct', account_summary.get('monthly_return_pct', 0)):.2f}%"],
                ]

            summary_table = Table(summary_data, colWidths=[2.5*inch, 2.5*inch])
            summary_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#2196F3')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, -1), 10),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
                ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor('#F5F5F5')),
                ('GRID', (0, 0), (-1, -1), 1, colors.black),
            ]))
            story.append(summary_table)
            story.append(Spacer(1, 0.2*inch))

            # Performance Metrics
            if 'metrics' in report and report['metrics']:
                story.append(Paragraph("Performance Metrics", styles['SectionHeader']))
                m = report['metrics']
                metrics_data = [
                    ["Metric", "Value"],
                    ["Sharpe Ratio", f"{m.get('sharpe_ratio', m.get('sharpe', 0)):.2f}"],
                    ["Max Drawdown", f"{m.get('max_drawdown', 0) * 100:.2f}%"],
                    ["Win Rate", f"{m.get('win_rate', 0) * 100:.2f}%"],
                    ["Profit Factor", f"{m.get('profit_factor', 0):.2f}"],
                ]
                if 'total_return' in m:
                    metrics_data.insert(2, ["Total Return", f"{m['total_return'] * 100:.2f}%"])
                if 'annualized_return' in m:
                    metrics_data.insert(3, ["Annualized Return", f"{m['annualized_return'] * 100:.2f}%"])

                metrics_table = Table(metrics_data, colWidths=[2.5*inch, 2.5*inch])
                metrics_table.setStyle(TableStyle([
                    ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#4CAF50')),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                    ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                    ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                    ('FONTSIZE', (0, 0), (-1, -1), 10),
                    ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
                    ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor('#E8F5E9')),
                    ('GRID', (0, 0), (-1, -1), 1, colors.black),
                ]))
                story.append(metrics_table)
                story.append(Spacer(1, 0.2*inch))

            # Positions Section
            if 'positions' in report and report['positions']:
                story.append(Paragraph("Open Positions", styles['SectionHeader']))
                pos_data = [["Instrument", "Quantity", "Avg Cost", "Last Price", "Realized P&L"]]
                for p in report['positions']:
                    pnl = p.get('realized_pnl', 0)
                    pnl_str = f"${pnl:,.2f}" if pnl >= 0 else f"(${abs(pnl):,.2f})"
                    pos_data.append([
                        p.get('instrument_id', ''),
                        f"{p.get('quantity', 0):.2f}",
                        f"${p.get('average_cost', 0):.2f}",
                        f"${p.get('last_price', 0):.2f}",
                        pnl_str,
                    ])

                pos_table = Table(pos_data, colWidths=[1.5*inch, 1*inch, 1*inch, 1*inch, 1.2*inch])
                pos_table.setStyle(TableStyle([
                    ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#607D8B')),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                    ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                    ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                    ('FONTSIZE', (0, 0), (-1, -1), 9),
                    ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
                    ('GRID', (0, 0), (-1, -1), 1, colors.black),
                ]))
                story.append(pos_table)
                story.append(Spacer(1, 0.2*inch))

            # Trade Summary
            if 'trade_summary' in report:
                story.append(Paragraph("Trade Summary", styles['SectionHeader']))
                ts = report['trade_summary']
                trade_data = [
                    ["Metric", "Value"],
                    ["Total Trades", str(ts.get('total_trades', 0))],
                    ["Total P&L", f"${ts.get('total_pnl', 0):,.2f}"],
                    ["Total Fees", f"${ts.get('total_fees', 0):,.2f}"],
                    ["Net P&L", f"${ts.get('net_pnl', 0):,.2f}"],
                ]
                trade_table = Table(trade_data, colWidths=[2.5*inch, 2.5*inch])
                trade_table.setStyle(TableStyle([
                    ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#FF9800')),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                    ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                    ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                    ('FONTSIZE', (0, 0), (-1, -1), 10),
                    ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
                    ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor('#FFF3E0')),
                    ('GRID', (0, 0), (-1, -1), 1, colors.black),
                ]))
                story.append(trade_table)
                story.append(Spacer(1, 0.2*inch))

            # Risk Alerts
            if 'risk_alerts' in report:
                story.append(Paragraph("Risk Alerts", styles['SectionHeader']))
                ra = report['risk_alerts']
                alert_data = [
                    ["Metric", "Value"],
                    ["Total Alerts", str(ra.get('total_alerts', 0))],
                    ["Critical", str(ra.get('critical_alerts', 0))],
                    ["Emergency", str(ra.get('emergency_alerts', 0))],
                    ["Risk Rejections", str(ra.get('risk_rejections', 0))],
                ]
                alert_table = Table(alert_data, colWidths=[2.5*inch, 2.5*inch])
                alert_table.setStyle(TableStyle([
                    ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#F44336')),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                    ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                    ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                    ('FONTSIZE', (0, 0), (-1, -1), 10),
                    ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
                    ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor('#FFEBEE')),
                    ('GRID', (0, 0), (-1, -1), 1, colors.black),
                ]))
                story.append(alert_table)

            # Footer
            story.append(Spacer(1, 0.5*inch))
            story.append(Paragraph(f"Generated by QuantExchange | {utc_now().strftime('%Y-%m-%d %H:%M:%S UTC')}", styles['Footer']))

            doc.build(story)
            return buffer.getvalue()

        except ImportError:
            # Fallback: Return simple text-based PDF
            return self._generate_simple_pdf(report, title)

    def _generate_simple_pdf(self, report: dict, title: str) -> bytes:
        """Generate a simple text-based PDF when ReportLab is not available."""
        lines = [
            title,
            "=" * 60,
            f"Report Type: {report.get('report_type', 'daily').upper()}",
            f"Generated: {utc_now().isoformat()}",
            f"Account: {report.get('account_id', 'N/A')}",
            "",
            "ACCOUNT SUMMARY",
            "-" * 40,
        ]

        account_summary = report.get('account_summary', {})
        for key, value in account_summary.items():
            lines.append(f"  {key}: {value}")

        if 'positions' in report:
            lines.append("")
            lines.append("OPEN POSITIONS")
            lines.append("-" * 40)
            for p in report['positions']:
                lines.append(f"  {p.get('instrument_id')}: Qty={p.get('quantity')}, P&L=${p.get('realized_pnl', 0):.2f}")

        if 'trade_summary' in report:
            lines.append("")
            lines.append("TRADE SUMMARY")
            lines.append("-" * 40)
            for key, value in report['trade_summary'].items():
                lines.append(f"  {key}: {value}")

        lines.append("")
        lines.append("=" * 60)
        lines.append(f"Generated by QuantExchange | {utc_now().strftime('%Y-%m-%d %H:%M:%S UTC')}")

        content = "\n".join(lines)
        # Simple PDF header
        pdf_content = f"%PDF-1.4\n1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents 4 0 R /Resources << /Font << /F1 5 0 R >> >> >>\nendobj\n4 0 obj\n<< /Length {len(content) + 100} >>\nstream\nBT\n/F1 12 Tf\n50 750 Td\n{content}\nET\nendstream\nendobj\n5 0 obj\n<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>\nendobj\nxref\n0 6\n0000000000 65535 f\n0000000009 00000 n\n0000000058 00000 n\n0000000115 00000 n\n0000000266 00000 n\n0000000600 00000 n\ntrailer\n<< /Size 6 /Root 1 0 R >>\nstartxref\n674\n%%EOF"
        return pdf_content.encode('utf-8')

    # ── Management and Compliance Reports (RP-06) ─────────────────────────────────

    def generate_compliance_report(
        self,
        *,
        account_id: str,
        positions: dict[str, Position],
        fills: list[Fill],
        alerts: list[Alert],
        risk_metrics: dict | None = None,
        report_date: datetime | None = None,
    ) -> dict:
        """Generate management and compliance report (RP-06).

        Covers:
        - Regulatory requirements
        - Risk limit compliance
        - Trade surveillance
        """
        report_date = report_date or utc_now()
        total_pnl = sum(p.realized_pnl for p in positions.values())
        total_exposure = sum(abs(p.last_price * p.quantity) for p in positions.values())

        # Risk limit compliance checks
        compliance_checks = []

        # Check leverage limits
        leverage = risk_metrics.get('leverage', 1.0) if risk_metrics else 1.0
        compliance_checks.append({
            "check": "Leverage Limit",
            "limit": "3.0x",
            "current": f"{leverage:.2f}x",
            "status": "PASS" if leverage <= 3.0 else "FAIL",
        })

        # Check drawdown limits
        drawdown = risk_metrics.get('drawdown', 0.0) if risk_metrics else 0.0
        compliance_checks.append({
            "check": "Max Drawdown Limit",
            "limit": "20%",
            "current": f"{drawdown * 100:.2f}%",
            "status": "PASS" if drawdown <= 0.20 else "FAIL",
        })

        # Check concentration limits (single position)
        max_concentration = 0.0
        for p in positions.values():
            if total_exposure > 0:
                concentration = abs(p.last_price * p.quantity) / total_exposure
                max_concentration = max(max_concentration, concentration)

        compliance_checks.append({
            "check": "Position Concentration",
            "limit": "30%",
            "current": f"{max_concentration * 100:.2f}%",
            "status": "PASS" if max_concentration <= 0.30 else "FAIL",
        })

        # Overall compliance status
        all_passed = all(check['status'] == 'PASS' for check in compliance_checks)

        return {
            "report_id": str(uuid.uuid4()),
            "account_id": account_id,
            "report_type": "compliance",
            "report_date": report_date.isoformat(),
            "overall_status": "COMPLIANT" if all_passed else "NON-COMPLIANT",
            "compliance_checks": compliance_checks,
            "risk_summary": {
                "total_exposure": round(total_exposure, 2),
                "total_pnl": round(total_pnl, 2),
                "position_count": len(positions),
                "leverage": leverage,
                "drawdown": round(drawdown * 100, 2),
            },
            "trade_surveillance": {
                "total_trades": len(fills),
                "alert_count": len(alerts),
                "critical_alerts": len([a for a in alerts if a.severity == AlertSeverity.CRITICAL]),
            },
            "generated_at": utc_now().isoformat(),
        }

    def generate_management_report(
        self,
        *,
        account_id: str,
        snapshots: list[PortfolioSnapshot],
        positions: dict[str, Position],
        fills: list[Fill],
        metrics: PerformanceMetrics | None = None,
        strategy_performance: dict | None = None,
        report_date: datetime | None = None,
    ) -> dict:
        """Generate executive management report (RP-06).

        Provides high-level overview for management decision making.
        """
        report_date = report_date or utc_now()

        if len(snapshots) >= 2:
            period_return = (snapshots[-1].equity - snapshots[0].equity) / snapshots[0].equity if snapshots[0].equity > 0 else 0.0
            starting_equity = snapshots[0].equity
            ending_equity = snapshots[-1].equity
        else:
            period_return = 0.0
            starting_equity = snapshots[-1].equity if snapshots else 0.0
            ending_equity = starting_equity

        total_fees = sum(f.fee for f in fills)

        # Strategy breakdown
        strategy_breakdown = []
        if strategy_performance:
            for strategy_id, perf in strategy_performance.items():
                strategy_breakdown.append({
                    "strategy_id": strategy_id,
                    "pnl": perf.get('pnl', 0),
                    "trades": perf.get('trades', 0),
                    "return_pct": perf.get('return_pct', 0),
                })

        # Key risk indicators
        risk_indicators = {
            "var_95": metrics.max_drawdown * 1.65 if metrics else 0.0,  # Simplified VaR
            "leverage": snapshots[-1].leverage if snapshots and hasattr(snapshots[-1], 'leverage') else 1.0,
            "concentration_risk": max(
                (abs(p.last_price * p.quantity) / snapshots[-1].gross_exposure if snapshots and hasattr(snapshots[-1], 'gross_exposure') and snapshots[-1].gross_exposure > 0 else 0.0)
                for p in positions.values()
            ) if positions else 0.0,
        }

        return {
            "report_id": str(uuid.uuid4()),
            "account_id": account_id,
            "report_type": "management",
            "report_date": report_date.isoformat(),
            "executive_summary": {
                "period_return_pct": round(period_return * 100, 2),
                "starting_equity": round(starting_equity, 2),
                "ending_equity": round(ending_equity, 2),
                "total_fees_paid": round(total_fees, 2),
                "active_positions": len(positions),
                "total_trades": len(fills),
            },
            "performance_summary": {
                "sharpe_ratio": round(metrics.sharpe, 2) if metrics else None,
                "max_drawdown_pct": round(metrics.max_drawdown * 100, 2) if metrics else None,
                "win_rate_pct": round(metrics.win_rate * 100, 2) if metrics else None,
                "profit_factor": round(metrics.profit_factor, 2) if metrics else None,
                "avg_trade_return_pct": round(metrics.avg_trade_return * 100, 4) if metrics else None,
            },
            "strategy_breakdown": strategy_breakdown,
            "risk_indicators": risk_indicators,
            "top_positions": [
                {
                    "instrument_id": p.instrument_id,
                    "exposure": round(abs(p.last_price * p.quantity), 2),
                    "pnl": round(p.realized_pnl, 2),
                }
                for p in sorted(positions.values(), key=lambda x: abs(x.last_price * x.quantity), reverse=True)[:5]
            ],
            "generated_at": utc_now().isoformat(),
        }

    # ── RP-04: Anomaly Detection in Attribution Reports ──────────────────────

    def detect_return_outliers(
        self,
        returns: list[float],
        *,
        z_threshold: float = 3.0,
        window: int = 20,
    ) -> dict[str, Any]:
        """Detect statistical outliers in a return series (RP-04).

        Uses rolling z-score to flag returns that deviate significantly
        from recent history.
        """
        if len(returns) < window + 1:
            return {"outliers": [], "outlier_count": 0, "window": window}

        outliers = []
        for i in range(window, len(returns)):
            window_returns = returns[i - window:i]
            w_mean = sum(window_returns) / len(window_returns)
            variance = sum((r - w_mean) ** 2 for r in window_returns) / len(window_returns)
            w_std = variance ** 0.5
            if w_std == 0:
                continue
            z = (returns[i] - w_mean) / w_std
            if abs(z) > z_threshold:
                outliers.append({
                    "index": i,
                    "return": round(returns[i], 6),
                    "z_score": round(z, 3),
                    "direction": "positive" if returns[i] > 0 else "negative",
                    "severity": (
                        "EXTREME" if abs(z) > 5 else
                        "SEVERE" if abs(z) > 4 else
                        "MODERATE"
                    ),
                })

        return {
            "outliers": outliers,
            "outlier_count": len(outliers),
            "total_observations": len(returns),
            "outlier_rate_pct": round(len(outliers) / max(len(returns) - window, 1) * 100, 3),
            "window": window,
            "z_threshold": z_threshold,
        }

    def detect_risk_contribution_anomalies(
        self,
        positions: dict[str, Position],
        historical_risk: dict[str, float] | None = None,
        threshold_pct: float = 0.50,
    ) -> dict[str, Any]:
        """Detect when risk contributions drift from historical norms (RP-04).

        Args:
            positions: Current positions.
            historical_risk: Dict of instrument_id → historical risk pct.
            threshold_pct: Fractional change threshold to flag as anomaly.
        """
        total_exposure = sum(abs(p.last_price * p.quantity) for p in positions.values())
        if total_exposure == 0:
            return {"anomalies": [], "note": "No exposure"}

        current_risk: dict[str, float] = {}
        for pos in positions.values():
            if not pos.instrument_id:
                continue
            exposure = abs(pos.last_price * pos.quantity)
            current_risk[pos.instrument_id] = exposure / total_exposure

        anomalies = []
        if historical_risk:
            all_instruments = set(current_risk) | set(historical_risk)
            for iid in all_instruments:
                curr = current_risk.get(iid, 0.0)
                hist = historical_risk.get(iid, 0.0)
                if hist > 0:
                    change = abs(curr - hist) / hist
                    if change > threshold_pct:
                        anomalies.append({
                            "instrument_id": iid,
                            "current_risk_pct": round(curr * 100, 3),
                            "historical_risk_pct": round(hist * 100, 3),
                            "change_pct": round(change * 100, 3),
                            "direction": "increased" if curr > hist else "decreased",
                            "severity": (
                                "CRITICAL" if change > 1.0 else
                                "HIGH" if change > 0.5 else
                                "MEDIUM"
                            ),
                        })

        return {
            "anomalies": anomalies,
            "anomaly_count": len(anomalies),
            "current_risk_contribution": {
                iid: round(v * 100, 3) for iid, v in current_risk.items()
            },
            "threshold_pct": round(threshold_pct * 100, 2),
        }

    def detect_sector_drift_anomalies(
        self,
        positions: dict[str, Position],
        target_sector_allocation: dict[str, float] | None = None,
        sector_mapping: dict[str, str] | None = None,
        drift_threshold_pct: float = 0.30,
    ) -> dict[str, Any]:
        """Detect sector allocation drift from targets (RP-04).

        Args:
            positions: Current positions.
            target_sector_allocation: Dict of sector → target weight (fractions summing to 1).
            sector_mapping: instrument_id → sector name.
            drift_threshold_pct: Maximum allowed deviation from target per sector.
        """
        sector_mapping = sector_mapping or {}
        sector_exposure: dict[str, float] = {}
        total_exposure = 0.0

        for pos in positions.values():
            sector = sector_mapping.get(pos.instrument_id, "Unknown")
            exposure = abs(pos.last_price * pos.quantity)
            sector_exposure[sector] = sector_exposure.get(sector, 0.0) + exposure
            total_exposure += exposure

        if total_exposure == 0:
            return {"anomalies": [], "note": "No exposure"}

        current_allocation = {
            s: e / total_exposure for s, e in sector_exposure.items()
        }

        anomalies = []
        if target_sector_allocation:
            all_sectors = set(current_allocation) | set(target_sector_allocation)
            for sector in all_sectors:
                curr = current_allocation.get(sector, 0.0)
                target = target_sector_allocation.get(sector, 0.0)
                drift = curr - target
                if abs(drift) > drift_threshold_pct:
                    anomalies.append({
                        "sector": sector,
                        "current_allocation_pct": round(curr * 100, 3),
                        "target_allocation_pct": round(target * 100, 3),
                        "drift_pct": round(drift * 100, 3),
                        "direction": "overweight" if drift > 0 else "underweight",
                        "severity": (
                            "CRITICAL" if abs(drift) > 0.15 else
                            "HIGH" if abs(drift) > 0.10 else
                            "MEDIUM"
                        ),
                    })

        return {
            "anomalies": anomalies,
            "anomaly_count": len(anomalies),
            "current_allocation": {
                s: round(v * 100, 3) for s, v in current_allocation.items()
            },
            "target_allocation": (
                {s: round(v * 100, 3) for s, v in target_sector_allocation.items()}
                if target_sector_allocation else {}
            ),
            "drift_threshold_pct": round(drift_threshold_pct * 100, 2),
        }

    def generate_anomaly_report(
        self,
        returns: list[float] | None = None,
        positions: dict[str, Position] | None = None,
        historical_risk: dict[str, float] | None = None,
        target_sector_allocation: dict[str, float] | None = None,
        sector_mapping: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """Generate a comprehensive anomaly detection report (RP-04).

        Combines return outliers, risk contribution drift, and sector allocation
        drift into a single actionable report with severity scoring.
        """
        report_id = str(uuid.uuid4())
        sections: dict[str, Any] = {}
        critical_count = 0
        high_count = 0
        medium_count = 0

        # Return outliers
        if returns:
            result = self.detect_return_outliers(returns)
            sections["return_outliers"] = result
            for o in result.get("outliers", []):
                if o["severity"] == "EXTREME" or o["severity"] == "SEVERE":
                    critical_count += 1
                elif o["severity"] == "MODERATE":
                    medium_count += 1

        # Risk contribution drift
        if positions:
            result = self.detect_risk_contribution_anomalies(
                positions, historical_risk
            )
            sections["risk_contribution_anomalies"] = result
            for a in result.get("anomalies", []):
                if a["severity"] == "CRITICAL":
                    critical_count += 1
                elif a["severity"] == "HIGH":
                    high_count += 1
                elif a["severity"] == "MEDIUM":
                    medium_count += 1

        # Sector drift
        if positions:
            result = self.detect_sector_drift_anomalies(
                positions, target_sector_allocation, sector_mapping
            )
            sections["sector_drift_anomalies"] = result
            for a in result.get("anomalies", []):
                if a["severity"] == "CRITICAL":
                    critical_count += 1
                elif a["severity"] == "HIGH":
                    high_count += 1
                elif a["severity"] == "MEDIUM":
                    medium_count += 1

        total_anomalies = critical_count + high_count + medium_count
        overall_severity = (
            "CRITICAL" if critical_count > 0 else
            "HIGH" if high_count > 0 else
            "MEDIUM" if medium_count > 0 else
            "NORMAL"
        )

        # Actionable recommendations
        recommendations = []
        if overall_severity in ("CRITICAL", "HIGH"):
            recommendations.append({
                "priority": "HIGH",
                "action": "Review portfolio allocation immediately",
                "reason": f"Portfolio has {critical_count} critical and {high_count} high severity anomalies",
            })
        if critical_count > 0:
            recommendations.append({
                "priority": "CRITICAL",
                "action": "Consider de-risking or hedging extreme return outliers",
                "reason": f"Detected {critical_count} extreme/severe return outliers",
            })
        if high_count > 0:
            recommendations.append({
                "priority": "MEDIUM",
                "action": "Rebalance sector allocations toward targets",
                "reason": f"{high_count} sector allocations exceed drift threshold",
            })
        if total_anomalies == 0:
            recommendations.append({
                "priority": "INFO",
                "action": "No anomalies detected — portfolio within normal ranges",
                "reason": "All metrics within statistical thresholds",
            })

        return {
            "report_id": report_id,
            "generated_at": utc_now().isoformat(),
            "overall_severity": overall_severity,
            "anomaly_summary": {
                "critical": critical_count,
                "high": high_count,
                "medium": medium_count,
                "total": total_anomalies,
            },
            "sections": sections,
            "recommendations": recommendations,
        }


class ReportScheduler:
    """Schedule and manage daily report generation tasks."""

    def __init__(self) -> None:
        self._tasks: dict[str, DailyReportTask] = {}

    def schedule_report(
        self,
        account_id: str,
        period: str = "daily",
        report_date: datetime | None = None,
    ) -> DailyReportTask:
        """Schedule a report generation task for daily, weekly, or monthly period (RP-05)."""
        report_date = report_date or utc_now()
        task = DailyReportTask(
            task_id=str(uuid.uuid4()),
            report_date=report_date,
            account_id=account_id,
        )
        self._tasks[task.task_id] = task
        return task

    def schedule_daily_report(
        self,
        account_id: str,
        report_date: datetime | None = None,
    ) -> DailyReportTask:
        """Schedule a daily report generation task."""
        return self.schedule_report(account_id, "daily", report_date)

    def schedule_weekly_report(
        self,
        account_id: str,
        report_date: datetime | None = None,
    ) -> DailyReportTask:
        """Schedule a weekly report generation task (RP-05)."""
        return self.schedule_report(account_id, "weekly", report_date)

    def schedule_monthly_report(
        self,
        account_id: str,
        report_date: datetime | None = None,
    ) -> DailyReportTask:
        """Schedule a monthly report generation task (RP-05)."""
        return self.schedule_report(account_id, "monthly", report_date)

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
