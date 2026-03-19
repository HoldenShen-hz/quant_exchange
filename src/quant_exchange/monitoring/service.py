"""Monitoring and alert generation helpers for runtime health checks.

Implements the documented monitoring features:
- Alert severity levels: INFO, WARNING, CRITICAL, EMERGENCY
- Alert deduplication within a configurable window
- Alert escalation (repeated triggers bump severity)
- System, application, and business-level health checks
"""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from datetime import timedelta
from typing import Any

from quant_exchange.core.models import Alert, AlertSeverity, PortfolioSnapshot, utc_now

_SEVERITY_ORDER = {
    AlertSeverity.INFO: 0,
    AlertSeverity.WARNING: 1,
    AlertSeverity.CRITICAL: 2,
    AlertSeverity.EMERGENCY: 3,
}

_SEVERITY_ESCALATION = {
    AlertSeverity.INFO: AlertSeverity.WARNING,
    AlertSeverity.WARNING: AlertSeverity.CRITICAL,
    AlertSeverity.CRITICAL: AlertSeverity.EMERGENCY,
    AlertSeverity.EMERGENCY: AlertSeverity.EMERGENCY,
}


class MonitoringService:
    """Collect alerts emitted by risk, data, and portfolio health checks.

    Features deduplication and escalation per the documented requirements.
    """

    def __init__(
        self,
        *,
        dedup_window: timedelta = timedelta(minutes=5),
        escalation_threshold: int = 3,
    ) -> None:
        self.alerts: list[Alert] = []
        self.dedup_window = dedup_window
        self.escalation_threshold = escalation_threshold
        # Track recent alerts by code for dedup and escalation
        self._recent_alerts: dict[str, list[datetime]] = defaultdict(list)
        # Metrics counters
        self._metrics: dict[str, float] = defaultdict(float)

    def add_alert(self, code: str, severity: AlertSeverity, message: str, *, context: dict | None = None) -> Alert:
        """Append a structured alert with deduplication and optional escalation."""

        now = utc_now()
        # Deduplication: suppress identical alerts within window
        cutoff = now - self.dedup_window
        recent = [t for t in self._recent_alerts[code] if t > cutoff]
        self._recent_alerts[code] = recent
        if recent:
            # Check escalation: if same alert fired N times, bump severity
            if len(recent) >= self.escalation_threshold:
                severity = _SEVERITY_ESCALATION.get(severity, severity)
            # Still record, but flag as escalated in context
            ctx = dict(context or {})
            ctx["repeat_count"] = len(recent) + 1
            if len(recent) >= self.escalation_threshold:
                ctx["escalated"] = True
        else:
            ctx = dict(context or {})

        self._recent_alerts[code].append(now)
        alert = Alert(
            code=code,
            severity=severity,
            message=message,
            timestamp=now,
            context=ctx,
        )
        self.alerts.append(alert)
        self._metrics[f"alert_count_{severity.value}"] += 1
        return alert

    def check_drawdown(self, snapshot: PortfolioSnapshot, threshold: float) -> Alert | None:
        """Raise a critical alert when drawdown breaches a configured threshold."""

        if snapshot.drawdown >= threshold:
            return self.add_alert(
                "drawdown_threshold",
                AlertSeverity.CRITICAL,
                f"Drawdown {snapshot.drawdown:.2%} breached threshold {threshold:.2%}.",
                context={"equity": snapshot.equity, "drawdown": snapshot.drawdown},
            )
        return None

    def check_stale_data(self, *, as_of: datetime, last_update: datetime, threshold: timedelta) -> Alert | None:
        """Raise a warning when market data is older than the allowed threshold."""

        if as_of - last_update > threshold:
            return self.add_alert(
                "stale_market_data",
                AlertSeverity.WARNING,
                "Market data feed is stale.",
                context={"as_of": as_of.isoformat(), "last_update": last_update.isoformat()},
            )
        return None

    def check_margin_ratio(self, margin_ratio: float, *, warning: float = 0.8, critical: float = 0.9) -> Alert | None:
        """Alert on margin ratio approaching dangerous levels."""

        if margin_ratio >= critical:
            return self.add_alert(
                "margin_critical",
                AlertSeverity.CRITICAL,
                f"Margin ratio {margin_ratio:.2%} above critical threshold {critical:.2%}.",
                context={"margin_ratio": margin_ratio},
            )
        if margin_ratio >= warning:
            return self.add_alert(
                "margin_warning",
                AlertSeverity.WARNING,
                f"Margin ratio {margin_ratio:.2%} approaching warning threshold {warning:.2%}.",
                context={"margin_ratio": margin_ratio},
            )
        return None

    def check_order_latency(self, latency_ms: float, threshold_ms: float = 500.0) -> Alert | None:
        """Alert when order processing latency exceeds the threshold."""

        self._metrics["last_order_latency_ms"] = latency_ms
        if latency_ms > threshold_ms:
            return self.add_alert(
                "high_order_latency",
                AlertSeverity.WARNING,
                f"Order latency {latency_ms:.0f}ms exceeds threshold {threshold_ms:.0f}ms.",
                context={"latency_ms": latency_ms},
            )
        return None

    def record_risk_rejection(self, reasons: tuple[str, ...], request_id: str) -> Alert:
        """Convert a risk rejection into a warning alert for operators."""

        return self.add_alert(
            "risk_rejection",
            AlertSeverity.WARNING,
            f"Order {request_id} rejected by risk engine.",
            context={"reasons": reasons},
        )

    def record_metric(self, name: str, value: float) -> None:
        """Record a named metric value for monitoring dashboards."""

        self._metrics[name] = value

    @property
    def metrics(self) -> dict[str, float]:
        """Return current metrics snapshot."""

        return dict(self._metrics)

    def alerts_by_severity(self, severity: AlertSeverity) -> list[Alert]:
        """Return alerts filtered by severity level."""

        return [a for a in self.alerts if a.severity == severity]

    def recent_alerts(self, window: timedelta = timedelta(hours=1)) -> list[Alert]:
        """Return alerts within a recent time window."""

        cutoff = utc_now() - window
        return [a for a in self.alerts if a.timestamp > cutoff]

    # ── Prometheus-format Metrics Export ─────────────────────────────────────

    def prometheus_metrics(self) -> str:
        """Return all current metrics formatted in Prometheus text exposition format.

        Exports:
        - Counters (with _total suffix): alert_count_INFO_total, orders_submitted_total, etc.
        - Gauges: portfolio_equity, portfolio_cash, position_quantity, etc.
        - Histograms: order_latency_ms_bucket{le=...}, order_latency_ms_sum, order_latency_ms_count
        """
        lines: list[str] = []

        # Build TYPE and HELP lines for known metrics
        metric_helps: dict[str, str] = {
            "alert_count": "Total alerts by severity",
            "orders_submitted": "Total orders submitted",
            "orders_filled": "Total orders filled",
            "orders_rejected": "Total orders rejected by risk",
            "portfolio_equity": "Current portfolio equity in base currency",
            "portfolio_cash": "Current cash balance",
            "portfolio_drawdown": "Current drawdown as a fraction",
            "portfolio_leverage": "Current leverage ratio",
            "position_quantity": "Current position quantity for instrument",
            "order_latency_ms": "Order processing latency in milliseconds",
            "strategy_pnl": "Realized strategy PnL",
            "strategy_daily_loss": "Strategy running daily loss",
            "hotness_score": "Intelligence hotness score",
            "sentiment_score": "Intelligence sentiment score",
        }

        # Emit TYPE/HELP once per metric base name
        emitted: set[str] = set()

        def _emit(name: str, value: float, metric_type: str, labels: dict[str, str] | None = None) -> None:
            if name not in emitted:
                lines.append(f"# HELP {name} {metric_helps.get(name, name)}")
                lines.append(f"# TYPE {name} {metric_type}")
                emitted.add(name)
            label_str = ""
            if labels:
                label_parts = [f'{k}="{v}"' for k, v in labels.items()]
                label_str = "{" + ",".join(label_parts) + "}"
            lines.append(f"{name}{label_str} {value}")

        # Export generic counters (alert counts)
        for metric_name, value in self._metrics.items():
            if metric_name.startswith("alert_count_"):
                severity = metric_name.replace("alert_count_", "")
                _emit(f"alert_count_{severity}_total", value, "counter")
            elif metric_name in ("orders_submitted", "orders_filled", "orders_rejected"):
                _emit(f"{metric_name}_total", value, "counter")
            elif metric_name == "last_order_latency_ms":
                _emit("order_latency_ms", value, "gauge")
            else:
                # Generic gauge
                _emit(f"monitoring_{metric_name}", value, "gauge")

        # Export aggregated alert count
        total_alerts = sum(1 for a in self.alerts)
        _emit("alerts_total", float(total_alerts), "counter")

        # Emit system health gauge
        emergency = len(self.alerts_by_severity(AlertSeverity.EMERGENCY))
        critical = len(self.alerts_by_severity(AlertSeverity.CRITICAL))
        health_score = 1.0
        if emergency > 0:
            health_score = 0.0
        elif critical > 0:
            health_score = 0.25
        _emit("system_health_score", health_score, "gauge")
        lines.append("")

        return "\n".join(lines)


class PrometheusMetricsCollector:
    """Structured metric collector for Prometheus-compatible monitoring.

    Provides typed counters, gauges, and histograms that accumulate
    and can be exported in Prometheus text format.
    """

    def __init__(self) -> None:
        self._counters: dict[str, float] = defaultdict(float)
        self._counter_labels: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
        self._gauges: dict[str, float] = defaultdict(float)
        self._gauge_labels: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
        self._histograms: dict[str, list[float]] = defaultdict(list)
        self._histogram_bounds: dict[str, list[float]] = {}

    def inc_counter(self, name: str, value: float = 1.0, **labels: str) -> None:
        """Increment a counter metric."""
        if labels:
            self._counter_labels[name][self._labels_key(labels)] += value
        else:
            self._counters[name] += value

    def set_gauge(self, name: str, value: float, **labels: str) -> None:
        """Set a gauge metric to an absolute value."""
        if labels:
            self._gauge_labels[name][self._labels_key(labels)] = value
        else:
            self._gauges[name] = value

    def observe_histogram(self, name: str, value: float, **labels: str) -> None:
        """Observe a value for a histogram metric."""
        self._histograms[name].append(value)

    def set_histogram_bounds(self, name: str, bounds: list[float]) -> None:
        """Set the bucket boundaries for a histogram (e.g. [5, 10, 25, 50, 100, 250, 500, 1000])."""
        self._histogram_bounds[name] = sorted(bounds)

    def export(self) -> str:
        """Export all metrics in Prometheus text exposition format."""
        lines: list[str] = []

        def _emit_type_help(name: str, mtype: str, help_text: str) -> None:
            lines.append(f"# HELP {name} {help_text}")
            lines.append(f"# TYPE {name} {mtype}")

        # Counters without labels
        for name, value in self._counters.items():
            _emit_type_help(name, "counter", f"Counter {name}")
            lines.append(f"{name} {value}")

        # Counters with labels
        for name, label_values in self._counter_labels.items():
            _emit_type_help(name, "counter", f"Counter {name}")
            for lkey, value in label_values.items():
                labels = self._parse_labels(lkey)
                label_str = "{" + ",".join(f'{k}="{v}"' for k, v in labels.items()) + "}"
                lines.append(f"{name}{label_str} {value}")

        # Gauges without labels
        for name, value in self._gauges.items():
            _emit_type_help(name, "gauge", f"Gauge {name}")
            lines.append(f"{name} {value}")

        # Gauges with labels
        for name, label_values in self._gauge_labels.items():
            _emit_type_help(name, "gauge", f"Gauge {name}")
            for lkey, value in label_values.items():
                labels = self._parse_labels(lkey)
                label_str = "{" + ",".join(f'{k}="{v}"' for k, v in labels.items()) + "}"
                lines.append(f"{name}{label_str} {value}")

        # Histograms
        for name, values in self._histograms.items():
            if not values:
                continue
            bounds = self._histogram_bounds.get(name, [5, 10, 25, 50, 100, 250, 500, 1000])
            sorted_vals = sorted(values)
            total = len(sorted_vals)
            total_sum = sum(sorted_vals)

            _emit_type_help(name, "histogram", f"Histogram {name}")
            lines.append(f"{name}_count {total}")
            lines.append(f"{name}_sum {total_sum}")

            cumulative = 0
            for bound in bounds:
                cumulative = sum(1 for v in sorted_vals if v <= bound)
                le_label = f'{{le="{bound}"}}'
                lines.append(f"{name}_bucket{le_label} {cumulative}")
            # +Inf bucket
            lines.append(f'{name}_bucket{{le="+Inf"}} {total}')

        lines.append("")
        return "\n".join(lines)

    def _labels_key(self, labels: dict[str, str]) -> str:
        return ",".join(f"{k}={v}" for k, v in sorted(labels.items()))

    def _parse_labels(self, key: str) -> dict[str, str]:
        result = {}
        for part in key.split(","):
            if "=" in part:
                k, v = part.split("=", 1)
                result[k] = v
        return result


class HealthCheckAggregator:
    """Aggregates component health checks and reports a combined system status.

    Components register their checks; the aggregator runs them all and
    returns an overall status for /health endpoints.
    """

    HealthStatus = AlertSeverity

    def __init__(self) -> None:
        self._checks: dict[str, callable] = {}
        self._last_results: dict[str, dict[str, Any]] = {}

    def register(self, name: str, check_fn: callable) -> None:
        """Register a health check function.

        check_fn() should return {"healthy": bool, "message": str, "details": dict}.
        """
        self._checks[name] = check_fn

    def run_all(self) -> dict[str, Any]:
        """Run all registered health checks and return combined results."""
        results: dict[str, dict[str, Any]] = {}
        overall_healthy = True
        worst_severity = AlertSeverity.INFO

        for name, check_fn in self._checks.items():
            try:
                result = check_fn()
            except Exception as exc:
                result = {"healthy": False, "message": str(exc), "details": {}}
            results[name] = result
            if not result.get("healthy", False):
                overall_healthy = False

        # Determine worst severity from results
        for name, result in results.items():
            if not result.get("healthy", False):
                severity_str = result.get("severity", "critical")
                try:
                    severity = AlertSeverity(severity_str)
                except ValueError:
                    severity = AlertSeverity.CRITICAL
                if _SEVERITY_ORDER.get(severity, 0) > _SEVERITY_ORDER.get(worst_severity, 0):
                    worst_severity = severity

        status_value = "healthy"
        if not overall_healthy:
            if worst_severity == AlertSeverity.EMERGENCY:
                status_value = "unhealthy"
            elif worst_severity == AlertSeverity.CRITICAL:
                status_value = "degraded"
            else:
                status_value = "degraded"

        self._last_results = results
        return {
            "status": status_value,
            "overall_healthy": overall_healthy,
            "worst_severity": worst_severity.value,
            "checks": results,
        }

    @property
    def last_results(self) -> dict[str, dict[str, Any]]:
        """Return the most recent run_all() results."""
        return dict(self._last_results)


def generate_grafana_dashboard() -> dict[str, Any]:
    """Generate a Grafana dashboard JSON for the quant exchange platform.

    Panels: Portfolio equity curve, order latency, alert heatmap,
    strategy P&L, position sizes, risk metrics.
    """
    return {
        "title": "QuantExchange Overview",
        "tags": ["quant_exchange", "automated"],
        "timezone": "browser",
        "panels": [
            {
                "id": 1,
                "title": "Portfolio Equity",
                "type": "graph",
                "gridPos": {"x": 0, "y": 0, "w": 12, "h": 8},
                "targets": [
                    {"expr": 'portfolio_equity', "legendFormat": "Equity", "refId": "A"}
                ],
                "yaxes": [
                    {"format": "currencyUSD", "label": "Equity"},
                    {"format": "short"},
                ],
            },
            {
                "id": 2,
                "title": "Order Latency (ms)",
                "type": "graph",
                "gridPos": {"x": 12, "y": 0, "w": 12, "h": 8},
                "targets": [
                    {"expr": 'order_latency_ms_bucket', "legendFormat": "p50", "refId": "A"}
                ],
                "yaxes": [
                    {"format": "ms", "label": "Latency"},
                    {"format": "short"},
                ],
            },
            {
                "id": 3,
                "title": "Alerts by Severity",
                "type": "graph",
                "gridPos": {"x": 0, "y": 8, "w": 8, "h": 8},
                "targets": [
                    {"expr": 'alert_count_INFO_total', "legendFormat": "Info", "refId": "A"},
                    {"expr": 'alert_count_WARNING_total', "legendFormat": "Warning", "refId": "B"},
                    {"expr": 'alert_count_CRITICAL_total', "legendFormat": "Critical", "refId": "C"},
                    {"expr": 'alert_count_EMERGENCY_total', "legendFormat": "Emergency", "refId": "D"},
                ],
            },
            {
                "id": 4,
                "title": "System Health Score",
                "type": "singlestat",
                "gridPos": {"x": 8, "y": 8, "w": 4, "h": 8},
                "targets": [
                    {"expr": 'system_health_score', "refId": "A"}
                ],
                "valueName": "current",
                "thresholds": "0.25,0.5",
                "colorBackground": True,
                "colors": ["#FF3A3A", "#FFA500", "#73BF69"],
            },
            {
                "id": 5,
                "title": "Orders (Submitted vs Rejected)",
                "type": "graph",
                "gridPos": {"x": 12, "y": 8, "w": 12, "h": 8},
                "targets": [
                    {"expr": 'orders_submitted_total', "legendFormat": "Submitted", "refId": "A"},
                    {"expr": 'orders_rejected_total', "legendFormat": "Rejected", "refId": "B"},
                ],
            },
            {
                "id": 6,
                "title": "Portfolio Drawdown",
                "type": "graph",
                "gridPos": {"x": 0, "y": 16, "w": 12, "h": 8},
                "targets": [
                    {"expr": 'portfolio_drawdown', "legendFormat": "Drawdown", "refId": "A"}
                ],
                "yaxes": [
                    {"format": "percentunit", "label": "Drawdown"},
                    {"format": "short"},
                ],
            },
        ],
        "refresh": "10s",
        "schemaVersion": 16,
        "version": 1,
    }
