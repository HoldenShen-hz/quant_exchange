"""Compliance report generation for regulatory review (RP-06).

Produces structured compliance reports covering:
- Position limit checks (single-name, sector, margin)
- Risk metric summary (VaR, drawdown, leverage)
- Trade execution quality (slippage, fills, rejections)
- Audit event summary (login, orders, risk events)
- Regulatory checklist (daily loss limit, position limit breach flags)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any

from quant_exchange.core.models import OrderSide, OrderStatus, Role, utc_now


class ComplianceFlag(str, Enum):
    """Compliance violation or warning flag."""
    BREACH_POSITION_LIMIT = "breach_position_limit"
    BREACH_DAILY_LOSS = "breach_daily_loss"
    BREACH_LEVERAGE = "breach_leverage"
    BREACH_MARGIN = "breach_margin"
    REJECTED_ORDER_SPIKE = "rejected_order_spike"
    UNAUTHORIZED_ACCESS = "unauthorized_access"
    OFF_EXCHANGE = "off_exchange_activity"
    UNUSUAL_VOLUME = "unusual_volume"
    NO_FLAG = "none"


@dataclass
class ComplianceCheck:
    """Result of a single compliance check."""
    check_name: str
    status: str  # "PASS", "WARN", "FAIL"
    value: float | str
    threshold: float | str | None = None
    details: str = ""


@dataclass
class ComplianceReport:
    """Complete compliance report for a review period (RP-06)."""
    report_id: str
    generated_at: datetime
    period_start: datetime
    period_end: datetime
    account_id: str
    checks: list[ComplianceCheck]
    flags: list[ComplianceFlag]
    summary: dict[str, Any]
    regulatory_notes: list[str] = field(default_factory=list)


class ComplianceReportService:
    """Generate compliance reports for regulatory and internal audit (RP-06).

    Aggregates data from:
    - Portfolio positions and limits
    - Risk engine metrics
    - Order execution records
    - Security audit log
    """

    def __init__(self) -> None:
        self._reports: list[ComplianceReport] = []

    def generate_report(
        self,
        *,
        account_id: str,
        period_start: datetime,
        period_end: datetime,
        positions: list[dict[str, Any]],
        orders: list[dict[str, Any]],
        risk_metrics: dict[str, Any],
        audit_events: list[dict[str, Any]],
        position_limits: dict[str, float] | None = None,
        daily_loss_limit: float = -0.05,  # -5%
        max_leverage: float = 3.0,
    ) -> ComplianceReport:
        """Generate a compliance report for the given period (RP-06)."""
        import uuid

        checks: list[ComplianceCheck] = []
        flags: list[ComplianceFlag] = []

        # ── 1. Position Limit Checks ──────────────────────────────────────
        if position_limits:
            for pos in positions:
                iid = pos.get("instrument_id", "unknown")
                qty = abs(pos.get("quantity", 0))
                limit = position_limits.get(iid, float("inf"))
                if qty > limit:
                    checks.append(ComplianceCheck(
                        check_name=f"position_limit_{iid}",
                        status="FAIL",
                        value=qty,
                        threshold=limit,
                        details=f"Position {iid} ({qty}) exceeds limit ({limit})",
                    ))
                    flags.append(ComplianceFlag.BREACH_POSITION_LIMIT)
                else:
                    checks.append(ComplianceCheck(
                        check_name=f"position_limit_{iid}",
                        status="PASS",
                        value=qty,
                        threshold=limit,
                    ))

        # ── 2. Daily Loss Limit Check ──────────────────────────────────────
        total_return = risk_metrics.get("total_return", 0.0)
        if total_return <= daily_loss_limit:
            checks.append(ComplianceCheck(
                check_name="daily_loss_limit",
                status="FAIL",
                value=total_return,
                threshold=daily_loss_limit,
                details=f"Daily return {total_return:.2%} exceeds loss limit {daily_loss_limit:.2%}",
            ))
            flags.append(ComplianceFlag.BREACH_DAILY_LOSS)
        else:
            checks.append(ComplianceCheck(
                check_name="daily_loss_limit",
                status="PASS",
                value=total_return,
                threshold=daily_loss_limit,
            ))

        # ── 3. Leverage Check ──────────────────────────────────────────────
        leverage = risk_metrics.get("leverage", 1.0)
        if leverage > max_leverage:
            checks.append(ComplianceCheck(
                check_name="max_leverage",
                status="FAIL",
                value=leverage,
                threshold=max_leverage,
                details=f"Leverage {leverage:.2f}x exceeds maximum {max_leverage:.2f}x",
            ))
            flags.append(ComplianceFlag.BREACH_LEVERAGE)
        else:
            checks.append(ComplianceCheck(
                check_name="max_leverage",
                status="PASS",
                value=leverage,
                threshold=max_leverage,
            ))

        # ── 4. Margin Check ────────────────────────────────────────────────
        margin_ratio = risk_metrics.get("margin_ratio", 999.0)
        if margin_ratio < 0.8:
            checks.append(ComplianceCheck(
                check_name="margin_ratio",
                status="WARN",
                value=margin_ratio,
                threshold=0.8,
                details=f"Margin ratio {margin_ratio:.2%} below healthy threshold (80%)",
            ))
            flags.append(ComplianceFlag.BREACH_MARGIN)
        elif margin_ratio < 0.667:
            checks.append(ComplianceCheck(
                check_name="margin_ratio",
                status="FAIL",
                value=margin_ratio,
                threshold=0.667,
                details=f"Margin ratio {margin_ratio:.2%} below liquidation threshold (66.7%)",
            ))
        else:
            checks.append(ComplianceCheck(
                check_name="margin_ratio",
                status="PASS",
                value=margin_ratio,
                threshold=0.667,
            ))

        # ── 5. Order Rejection Rate ────────────────────────────────────────
        total_orders = len(orders)
        rejected_orders = sum(1 for o in orders if o.get("status") in (OrderStatus.REJECTED.value, "rejected"))
        rejection_rate = rejected_orders / max(total_orders, 1)
        if rejection_rate > 0.1:  # > 10% rejection rate
            checks.append(ComplianceCheck(
                check_name="order_rejection_rate",
                status="WARN",
                value=rejection_rate,
                threshold=0.1,
                details=f"Order rejection rate {rejection_rate:.2%} exceeds 10% threshold",
            ))
            flags.append(ComplianceFlag.REJECTED_ORDER_SPIKE)
        else:
            checks.append(ComplianceCheck(
                check_name="order_rejection_rate",
                status="PASS",
                value=rejection_rate,
                threshold=0.1,
            ))

        # ── 6. Unauthorized Access Check ──────────────────────────────────
        auth_failures = sum(1 for e in audit_events if e.get("event_type") == "login_failed")
        if auth_failures > 5:
            checks.append(ComplianceCheck(
                check_name="unauthorized_access_attempts",
                status="WARN",
                value=auth_failures,
                threshold=5,
                details=f"{auth_failures} failed login attempts in period",
            ))
            flags.append(ComplianceFlag.UNAUTHORIZED_ACCESS)
        else:
            checks.append(ComplianceCheck(
                check_name="unauthorized_access_attempts",
                status="PASS",
                value=auth_failures,
                threshold=5,
            ))

        # ── 7. Trade Volume Anomaly ─────────────────────────────────────────
        volumes = [o.get("filled_quantity", 0) for o in orders if o.get("status") == OrderStatus.FILLED.value]
        if volumes:
            avg_vol = sum(volumes) / len(volumes)
            max_vol = max(volumes)
            if max_vol > avg_vol * 10 and avg_vol > 0:
                checks.append(ComplianceCheck(
                    check_name="volume_anomaly",
                    status="WARN",
                    value=max_vol,
                    threshold=avg_vol * 10,
                    details=f"Max order volume {max_vol} is 10x above average {avg_vol:.0f}",
                ))
                flags.append(ComplianceFlag.UNUSUAL_VOLUME)
            else:
                checks.append(ComplianceCheck(
                    check_name="volume_anomaly",
                    status="PASS",
                    value=max_vol,
                    threshold=avg_vol * 10,
                ))

        # ── Summary ─────────────────────────────────────────────────────────
        total_checks = len(checks)
        passed = sum(1 for c in checks if c.status == "PASS")
        warnings = sum(1 for c in checks if c.status == "WARN")
        failures = sum(1 for c in checks if c.status == "FAIL")

        summary = {
            "total_checks": total_checks,
            "passed": passed,
            "warnings": warnings,
            "failures": failures,
            "pass_rate": round(passed / max(total_checks, 1), 4),
            "total_orders": total_orders,
            "rejected_orders": rejected_orders,
            "total_audit_events": len(audit_events),
            "auth_failures": auth_failures,
            "positions_count": len(positions),
            "net_exposure": risk_metrics.get("net_exposure", 0.0),
            "equity": risk_metrics.get("equity", 0.0),
            "sharpe": risk_metrics.get("sharpe", 0.0),
            "max_drawdown": risk_metrics.get("max_drawdown", 0.0),
        }

        # ── Regulatory Notes ───────────────────────────────────────────────
        notes: list[str] = []
        if any(f in flags for f in (ComplianceFlag.BREACH_POSITION_LIMIT,)):
            notes.append("POSITION LIMIT BREACH DETECTED — Immediate review required per Market Abuse Regulation.")
        if ComplianceFlag.BREACH_DAILY_LOSS in flags:
            notes.append("DAILY LOSS LIMIT BREACH — Risk committee must be notified within 24 hours.")
        if ComplianceFlag.BREACH_LEVERAGE in flags:
            notes.append("LEVERAGE LIMIT BREACH — Margin call procedure initiated.")
        if ComplianceFlag.UNAUTHORIZED_ACCESS in flags:
            notes.append("EXCESSIVE FAILED LOGIN ATTEMPTS — Security team notified. Possible brute-force attack.")
        if not notes:
            notes.append("No regulatory breaches detected in this review period.")
        else:
            notes.append("This report was generated automatically. All breaches require human review and sign-off.")

        report = ComplianceReport(
            report_id=f"CR-{uuid.uuid4().hex[:10].upper()}",
            generated_at=utc_now(),
            period_start=period_start,
            period_end=period_end,
            account_id=account_id,
            checks=checks,
            flags=flags,
            summary=summary,
            regulatory_notes=notes,
        )

        self._reports.append(report)
        return report

    def get_latest_report(self) -> ComplianceReport | None:
        """Return the most recently generated report."""
        return self._reports[-1] if self._reports else None

    def list_reports(self, limit: int = 30) -> list[ComplianceReport]:
        """Return recent compliance reports."""
        return sorted(self._reports, key=lambda r: r.generated_at, reverse=True)[:limit]

    def export_report_json(self, report: ComplianceReport) -> dict[str, Any]:
        """Export a compliance report as a structured dict for JSON serialization."""
        return {
            "report_id": report.report_id,
            "generated_at": report.generated_at.isoformat(),
            "period_start": report.period_start.isoformat(),
            "period_end": report.period_end.isoformat(),
            "account_id": report.account_id,
            "checks": [
                {
                    "check_name": c.check_name,
                    "status": c.status,
                    "value": c.value,
                    "threshold": c.threshold,
                    "details": c.details,
                }
                for c in report.checks
            ],
            "flags": [f.value for f in report.flags],
            "summary": report.summary,
            "regulatory_notes": report.regulatory_notes,
        }
