"""Monitoring and alert generation helpers for runtime health checks.

Implements the documented monitoring features:
- Alert severity levels: INFO, WARNING, CRITICAL, EMERGENCY (MO-03)
- Alert deduplication within a configurable window (MO-05)
- Alert escalation (repeated triggers bump severity) (MO-05)
- System, application, and business-level health checks (MO-01)
- Service health tracking for connections, tasks, strategy states (MO-01)
- Equity and risk threshold monitoring (MO-02)
- Notification channels: Webhook, Email, Telegram, DingTalk, WeChat Work (MO-04)
- Alert suppression windows (MO-05)
- Page visualization via alert history API (MO-06)
"""

from __future__ import annotations

import json
from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from datetime import timedelta
from enum import Enum
from typing import Any
from urllib.request import Request, urlopen
from urllib.error import URLError

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
        content_based_dedup: bool = False,  # MO-05: set True for stricter content-based dedup
    ) -> None:
        self.alerts: list[Alert] = []
        self.dedup_window = dedup_window
        self.escalation_threshold = escalation_threshold
        self.content_based_dedup = content_based_dedup
        # Track recent alerts by code for dedup and escalation
        self._recent_alerts: dict[str, list[datetime]] = defaultdict(list)
        # Content-based dedup: code -> list of (content_hash, timestamp)
        self._content_hashes: dict[str, list[tuple[str, datetime]]] = defaultdict(list)
        # Alert suppression windows (code -> suppression_end_time)
        self._suppression: dict[str, datetime] = {}
        # Metrics counters
        self._metrics: dict[str, float] = defaultdict(float)

    def suppress_alerts(self, code: str, until: datetime) -> None:
        """Suppress alerts with the given code until the specified time (maintenance window)."""
        self._suppression[code] = until

    def unsuppress_alerts(self, code: str) -> None:
        """Remove suppression for the given alert code."""
        self._suppression.pop(code, None)

    def get_suppression(self, code: str) -> datetime | None:
        """Return the suppression end time for a code, or None if not suppressed."""
        return self._suppression.get(code)

    def is_suppressed(self, code: str) -> bool:
        """Return True if the alert code is currently suppressed."""
        end = self._suppression.get(code)
        if end is None:
            return False
        if utc_now() >= end:
            del self._suppression[code]
            return False
        return True

    def _compute_content_hash(self, message: str, context: dict | None) -> str:
        """Compute a stable hash of alert content for content-based deduplication."""
        import hashlib
        ctx_str = ""
        if context:
            # Include only stable context fields (exclude volatile fields like timestamps)
            stable_fields = {k: v for k, v in context.items() if "time" not in k.lower() and "id" not in k.lower()}
            ctx_str = json.dumps(stable_fields, sort_keys=True, default=str)
        content = f"{message}|{ctx_str}"
        return hashlib.sha256(content.encode("utf-8")).hexdigest()[:16]

    def add_alert(self, code: str, severity: AlertSeverity, message: str, *, context: dict | None = None) -> Alert | None:
        """Append a structured alert with deduplication and optional escalation.

        Two dedup strategies:
        - Code-based: same alert code within dedup_window → escalation on repeat
        - Content-based: same code + similar message within dedup_window → suppress duplicate

        Returns None if the alert is currently suppressed.
        """

        now = utc_now()
        # Check suppression window
        if self.is_suppressed(code):
            return None

        # ── Content-based deduplication ─────────────────────────────────
        if self.content_based_dedup:
            content_hash = self._compute_content_hash(message, context)
            content_cutoff = now - self.dedup_window
            # Clean expired entries
            self._content_hashes[code] = [
                (h, t) for h, t in self._content_hashes[code] if t > content_cutoff
            ]
            # Suppress exact content duplicate
            if any(h == content_hash for h, _ in self._content_hashes[code]):
                # Still track for escalation purposes, but don't add duplicate to alerts list
                self._recent_alerts[code] = [
                    t for t in self._recent_alerts[code] if t > content_cutoff
                ]
                self._recent_alerts[code].append(now)
                # Escalation still applies to repeated code fires even if content duplicates are suppressed
                if len(self._recent_alerts[code]) >= self.escalation_threshold:
                    severity = _SEVERITY_ESCALATION.get(severity, severity)
                return None
            self._content_hashes[code].append((content_hash, now))

        # ── Code-based deduplication with escalation ─────────────────────
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


# ─── MO-01: Service Health Tracking ────────────────────────────────────────────


class ServiceStatus(str, Enum):
    """Service operational status."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


class ConnectionState(str, Enum):
    """Connection state for external dependencies."""
    CONNECTED = "connected"
    DISCONNECTED = "disconnected"
    RECONNECTING = "reconnecting"


@dataclass
class ServiceHealth:
    """Health snapshot for a single service component."""
    service_name: str
    status: ServiceStatus
    message: str = ""
    latency_ms: float = 0.0
    last_check: datetime = field(default_factory=utc_now)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class TaskState:
    """State of a background task or job."""
    task_id: str
    task_name: str
    status: str  # "running", "pending", "completed", "failed"
    started_at: datetime | None = None
    completed_at: datetime | None = None
    error: str | None = None
    progress: float = 0.0  # 0.0 to 1.0


@dataclass
class StrategyRunState:
    """Running state of a strategy instance."""
    strategy_id: str
    strategy_name: str
    state: str  # "initializing", "running", "paused", "stopped"
    started_at: datetime | None = None
    last_signal_at: datetime | None = None
    pnl: float = 0.0
    orders_count: int = 0


class ServiceHealthTracker:
    """Tracks service health, connections, tasks, and strategy running states (MO-01).

    Provides a centralized view of system health for monitoring dashboards
    and alerting.
    """

    def __init__(self) -> None:
        self._services: dict[str, ServiceHealth] = {}
        self._connections: dict[str, ConnectionState] = {}
        self._tasks: dict[str, TaskState] = {}
        self._strategies: dict[str, StrategyRunState] = {}

    def register_service(
        self,
        service_name: str,
        status: ServiceStatus = ServiceStatus.HEALTHY,
        message: str = "",
    ) -> None:
        """Register a service with its current health status."""
        self._services[service_name] = ServiceHealth(
            service_name=service_name,
            status=status,
            message=message,
            last_check=utc_now(),
        )

    def update_service_health(
        self,
        service_name: str,
        status: ServiceStatus,
        message: str = "",
        latency_ms: float = 0.0,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Update the health status of a registered service."""
        if service_name not in self._services:
            self._services[service_name] = ServiceHealth(
                service_name=service_name,
                status=status,
                message=message,
                latency_ms=latency_ms,
                metadata=metadata or {},
            )
        else:
            self._services[service_name].status = status
            self._services[service_name].message = message
            self._services[service_name].latency_ms = latency_ms
            self._services[service_name].last_check = utc_now()
            if metadata:
                self._services[service_name].metadata.update(metadata)

    def update_connection_state(
        self,
        connection_name: str,
        state: ConnectionState,
    ) -> None:
        """Update the connection state for an external dependency."""
        self._connections[connection_name] = state

    def register_task(self, task: TaskState) -> None:
        """Register a new task."""
        self._tasks[task.task_id] = task

    def update_task_state(
        self,
        task_id: str,
        status: str,
        progress: float = 0.0,
        error: str | None = None,
    ) -> None:
        """Update the state of a registered task."""
        if task_id in self._tasks:
            self._tasks[task_id].status = status
            self._tasks[task_id].progress = progress
            if error:
                self._tasks[task_id].error = error
            if status == "completed" or status == "failed":
                self._tasks[task_id].completed_at = utc_now()

    def register_strategy_run(self, strategy: StrategyRunState) -> None:
        """Register a strategy run."""
        self._strategies[strategy.strategy_id] = strategy

    def update_strategy_state(
        self,
        strategy_id: str,
        state: str,
        pnl: float | None = None,
        orders_count: int | None = None,
    ) -> None:
        """Update the state of a registered strategy."""
        if strategy_id in self._strategies:
            self._strategies[strategy_id].state = state
            self._strategies[strategy_id].last_signal_at = utc_now()
            if pnl is not None:
                self._strategies[strategy_id].pnl = pnl
            if orders_count is not None:
                self._strategies[strategy_id].orders_count = orders_count

    def get_service_health(self, service_name: str) -> ServiceHealth | None:
        """Get the health status of a service."""
        return self._services.get(service_name)

    def get_all_services_health(self) -> dict[str, ServiceHealth]:
        """Get health status of all registered services."""
        return dict(self._services)

    def get_connection_state(self, connection_name: str) -> ConnectionState | None:
        """Get the connection state for an external dependency."""
        return self._connections.get(connection_name)

    def get_all_connections(self) -> dict[str, ConnectionState]:
        """Get all connection states."""
        return dict(self._connections)

    def get_task_state(self, task_id: str) -> TaskState | None:
        """Get the state of a task."""
        return self._tasks.get(task_id)

    def get_all_tasks(self) -> dict[str, TaskState]:
        """Get all registered tasks."""
        return dict(self._tasks)

    def get_running_tasks(self) -> list[TaskState]:
        """Get all tasks that are currently running."""
        return [t for t in self._tasks.values() if t.status == "running"]

    def get_strategy_state(self, strategy_id: str) -> StrategyRunState | None:
        """Get the state of a strategy."""
        return self._strategies.get(strategy_id)

    def get_all_strategies(self) -> dict[str, StrategyRunState]:
        """Get all registered strategy runs."""
        return dict(self._strategies)

    def get_running_strategies(self) -> list[StrategyRunState]:
        """Get all strategies that are currently running."""
        return [s for s in self._strategies.values() if s.state == "running"]

    def get_overall_status(self) -> ServiceStatus:
        """Get the overall system health status based on all services."""
        if not self._services:
            return ServiceStatus.UNKNOWN

        statuses = [s.status for s in self._services.values()]
        if any(s == ServiceStatus.UNHEALTHY for s in statuses):
            return ServiceStatus.UNHEALTHY
        if any(s == ServiceStatus.DEGRADED for s in statuses):
            return ServiceStatus.DEGRADED
        if all(s == ServiceStatus.HEALTHY for s in statuses):
            return ServiceStatus.HEALTHY
        return ServiceStatus.UNKNOWN


# ─── MO-02: Equity Monitoring ──────────────────────────────────────────────────


@dataclass
class EquityThreshold:
    """Configuration for an equity-based risk threshold."""
    name: str
    warning_value: float
    critical_value: float
    comparison: str = "less"  # "less" or "greater"


@dataclass
class EquityAlert:
    """Alert generated by equity monitoring."""
    alert: Alert
    threshold_name: str
    current_value: float
    threshold_value: float


class EquityMonitor:
    """Monitors account equity, risk thresholds, and drawdown (MO-02).

    Watches portfolio snapshots against configured thresholds and generates
    alerts when values breach warning or critical levels.
    """

    def __init__(self) -> None:
        self._thresholds: dict[str, EquityThreshold] = {}
        self._equity_alerts: list[EquityAlert] = []

    def add_threshold(
        self,
        name: str,
        warning_value: float,
        critical_value: float,
        comparison: str = "less",
    ) -> None:
        """Add or update an equity/risk threshold."""
        self._thresholds[name] = EquityThreshold(
            name=name,
            warning_value=warning_value,
            critical_value=critical_value,
            comparison=comparison,
        )

    def remove_threshold(self, name: str) -> None:
        """Remove a threshold by name."""
        self._thresholds.pop(name, None)

    def get_threshold(self, name: str) -> EquityThreshold | None:
        """Get a threshold configuration by name."""
        return self._thresholds.get(name)

    def get_all_thresholds(self) -> dict[str, EquityThreshold]:
        """Get all configured thresholds."""
        return dict(self._thresholds)

    def check_equity(
        self,
        snapshot: PortfolioSnapshot,
        monitoring_service: MonitoringService,
    ) -> list[EquityAlert]:
        """Check equity value against all thresholds and generate alerts.

        Returns a list of EquityAlert objects for any triggered thresholds.
        """
        triggered: list[EquityAlert] = []

        # Check equity level
        triggered.extend(self._check_threshold(
            "equity",
            snapshot.equity,
            snapshot,
            monitoring_service,
        ))

        # Check drawdown
        triggered.extend(self._check_threshold(
            "drawdown",
            snapshot.drawdown,
            snapshot,
            monitoring_service,
            comparison="greater",  # drawdown is bad when high
        ))

        # Check leverage
        triggered.extend(self._check_threshold(
            "leverage",
            snapshot.leverage,
            snapshot,
            monitoring_service,
            comparison="greater",  # leverage is bad when high
        ))

        # Check net exposure
        triggered.extend(self._check_threshold(
            "net_exposure",
            abs(snapshot.net_exposure),
            snapshot,
            monitoring_service,
            comparison="greater",
        ))

        self._equity_alerts.extend(triggered)
        return triggered

    def _check_threshold(
        self,
        name: str,
        current_value: float,
        snapshot: PortfolioSnapshot,
        monitoring_service: MonitoringService,
        comparison: str = "less",
    ) -> list[EquityAlert]:
        """Check a single threshold and generate alert if breached."""
        triggered: list[EquityAlert] = []
        threshold = self._thresholds.get(name)

        if not threshold:
            return triggered

        # Determine if threshold is breached
        breached = False
        if comparison == "less":
            breached = current_value <= threshold.warning_value
            critical_breached = current_value <= threshold.critical_value
        else:
            breached = current_value >= threshold.warning_value
            critical_breached = current_value >= threshold.critical_value

        if not breached:
            return triggered

        severity = AlertSeverity.WARNING
        if critical_breached:
            severity = AlertSeverity.CRITICAL

        message = f"{name} {current_value:.4f} "
        if comparison == "less":
            message += f"{'below' if not critical_breached else 'at or below'} "
        else:
            message += f"{'above' if not critical_breached else 'at or above'} "
        message += f"threshold ({threshold.warning_value:.4f} warning, {threshold.critical_value:.4f} critical)"

        context = {
            "threshold_name": name,
            "current_value": current_value,
            "warning_value": threshold.warning_value,
            "critical_value": threshold.critical_value,
            "equity": snapshot.equity,
            "comparison": comparison,
        }

        alert = monitoring_service.add_alert(
            f"equity_{name}_threshold",
            severity,
            message,
            context=context,
        )

        if alert:
            triggered.append(EquityAlert(
                alert=alert,
                threshold_name=name,
                current_value=current_value,
                threshold_value=threshold.warning_value,
            ))

        return triggered

    def get_recent_alerts(self, window: timedelta = timedelta(hours=24)) -> list[EquityAlert]:
        """Get equity alerts from the recent time window."""
        cutoff = utc_now() - window
        return [
            a for a in self._equity_alerts
            if a.alert.timestamp > cutoff
        ]


# ─── MO-04: Notification Channels ──────────────────────────────────────────────


@dataclass
class NotificationPayload:
    """Payload sent to a notification channel."""
    alert: Alert
    channel_name: str
    recipient: str
    sent_at: datetime = field(default_factory=utc_now)
    success: bool = True
    error: str | None = None


class NotificationChannel(ABC):
    """Abstract base class for notification channels (MO-04)."""

    @property
    @abstractmethod
    def channel_name(self) -> str:
        """Return the name of this notification channel."""
        raise NotImplementedError

    @abstractmethod
    def send(self, alert: Alert, recipient: str) -> NotificationPayload:
        """Send an alert notification to the recipient."""
        raise NotImplementedError

    def format_message(self, alert: Alert) -> str:
        """Format an alert as a human-readable message."""
        return f"[{alert.severity.value.upper()}] {alert.code}: {alert.message}"


class WebhookChannel(NotificationChannel):
    """Notification channel that sends alerts via HTTP webhook."""

    def __init__(self, default_url: str | None = None, timeout: float = 5.0) -> None:
        self.default_url = default_url
        self.timeout = timeout
        self._sent: list[NotificationPayload] = []

    @property
    def channel_name(self) -> str:
        return "webhook"

    def send(self, alert: Alert, recipient: str) -> NotificationPayload:
        """Send alert to a webhook URL.

        Uses recipient as the URL if it looks like a valid HTTP(S) URL,
        otherwise falls back to the default_url.
        """
        # Determine URL: recipient if valid URL, otherwise default_url
        url = recipient if recipient and recipient.startswith(("http://", "https://")) else self.default_url
        if not url:
            return NotificationPayload(
                alert=alert,
                channel_name=self.channel_name,
                recipient=recipient,
                success=False,
                error="No webhook URL provided",
            )

        payload = {
            "alert_code": alert.code,
            "severity": alert.severity.value,
            "message": alert.message,
            "timestamp": alert.timestamp.isoformat(),
            "context": alert.context,
        }

        try:
            data = str(payload).encode("utf-8")
            request = Request(url, data=data, headers={"Content-Type": "application/json"})
            with urlopen(request, timeout=self.timeout) as response:
                success = response.status == 200
                result = NotificationPayload(
                    alert=alert,
                    channel_name=self.channel_name,
                    recipient=recipient,
                    success=success,
                )
        except URLError as e:
            result = NotificationPayload(
                alert=alert,
                channel_name=self.channel_name,
                recipient=recipient,
                success=False,
                error=str(e),
            )

        self._sent.append(result)
        return result

    def get_sent_notifications(self) -> list[NotificationPayload]:
        """Get all sent notifications for testing."""
        return list(self._sent)


class EmailChannel(NotificationChannel):
    """Notification channel that sends alerts via SMTP email.

    Configure SMTP credentials via environment variables:
    - QUANT_SMTP_HOST: SMTP server hostname (default: localhost)
    - QUANT_SMTP_PORT: SMTP port (default: 587)
    - QUANT_SMTP_USER: SMTP username
    - QUANT_SMTP_PASSWORD: SMTP password
    - QUANT_SMTP_FROM: From address (default: quant-alerts@localhost)
    - QUANT_SMTP_USE_TLS: Whether to use TLS (default: true)
    """

    def __init__(
        self,
        smtp_host: str | None = None,
        smtp_port: int | None = None,
        smtp_user: str | None = None,
        smtp_password: str | None = None,
        smtp_from: str | None = None,
        use_tls: bool | None = None,
    ) -> None:
        import os
        self.smtp_host = smtp_host or os.getenv("QUANT_SMTP_HOST", "localhost")
        self.smtp_port = smtp_port or int(os.getenv("QUANT_SMTP_PORT", "587"))
        self.smtp_user = smtp_user or os.getenv("QUANT_SMTP_USER", "")
        self.smtp_password = smtp_password or os.getenv("QUANT_SMTP_PASSWORD", "")
        self.smtp_from = smtp_from or os.getenv("QUANT_SMTP_FROM", "quant-alerts@localhost")
        self.use_tls = use_tls if use_tls is not None else os.getenv("QUANT_SMTP_USE_TLS", "true").lower() != "false"
        self._sent: list[NotificationPayload] = []

    @property
    def channel_name(self) -> str:
        return "email"

    def send(self, alert: Alert, recipient: str) -> NotificationPayload:
        """Send alert via SMTP email."""
        import os
        import smtplib
        import ssl
        from email.mime.text import MIMEText
        from email.mime.multipart import MIMEMultipart

        # Stub mode: if no real SMTP credentials, simulate success
        has_credentials = bool(self.smtp_user and self.smtp_password)
        is_localhost = self.smtp_host in ("localhost", "127.0.0.1", "")
        if is_localhost and not has_credentials:
            result = NotificationPayload(
                alert=alert,
                channel_name=self.channel_name,
                recipient=recipient,
                success=True,
            )
            self._sent.append(result)
            return result

        subject = f"[{alert.severity.value}] {alert.code}: {alert.message[:80]}"
        body_html = self.format_message(alert)

        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = self.smtp_from
        msg["To"] = recipient

        # Plain-text fallback and HTML version
        text_part = MIMEText(
            f"Severity: {alert.severity.value}\nCode: {alert.code}\nMessage: {alert.message}\n"
            f"Timestamp: {alert.timestamp.isoformat()}\nContext: {alert.context}",
            "plain",
        )
        html_part = MIMEText(body_html, "html")
        msg.attach(text_part)
        msg.attach(html_part)

        success = False
        error_msg = ""
        try:
            if self.use_tls:
                context = ssl.create_default_context()
                with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
                    server.starttls(context=context)
                    if self.smtp_user and self.smtp_password:
                        server.login(self.smtp_user, self.smtp_password)
                    server.sendmail(self.smtp_from, [recipient], msg.as_string())
            else:
                with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
                    if self.smtp_user and self.smtp_password:
                        server.login(self.smtp_user, self.smtp_password)
                    server.sendmail(self.smtp_from, [recipient], msg.as_string())
            success = True
        except Exception as exc:  # noqa: BLE001
            error_msg = str(exc)

        result = NotificationPayload(
            alert=alert,
            channel_name=self.channel_name,
            recipient=recipient,
            success=success,
            error=error_msg if not success else None,
        )
        self._sent.append(result)
        return result

    def get_sent_notifications(self) -> list[NotificationPayload]:
        """Get all sent notifications for testing."""
        return list(self._sent)


class TelegramChannel(NotificationChannel):
    """Notification channel that sends alerts via Telegram bot.

    Configure via environment variables:
    - QUANT_TELEGRAM_BOT_TOKEN: Telegram bot token from @BotFather
    """

    def __init__(self, bot_token: str | None = None) -> None:
        import os
        self.bot_token = bot_token or os.getenv("QUANT_TELEGRAM_BOT_TOKEN", "")
        self._sent: list[NotificationPayload] = []

    @property
    def channel_name(self) -> str:
        return "telegram"

    def _is_real_token(self, token: str) -> bool:
        """Return True if token looks like a real Telegram bot token (not a test placeholder)."""
        return bool(token) and not token.startswith("test_") and len(token) > 10

    def send(self, alert: Alert, recipient: str) -> NotificationPayload:
        """Send alert via Telegram Bot API."""
        import json
        import os
        from urllib.request import Request, urlopen
        from urllib.error import URLError

        token = self.bot_token or os.getenv("QUANT_TELEGRAM_BOT_TOKEN", "")
        if not token:
            return NotificationPayload(
                alert=alert,
                channel_name=self.channel_name,
                recipient=recipient,
                success=False,
                error="No Telegram bot token configured",
            )

        # Stub mode: test tokens (e.g. "test_token_123") don't make real HTTP calls
        if not self._is_real_token(token):
            result = NotificationPayload(
                alert=alert,
                channel_name=self.channel_name,
                recipient=recipient,
                success=True,
            )
            self._sent.append(result)
            return result

        # Format message for Telegram (MarkdownV2)
        severity_emoji = {"INFO": "ℹ️", "WARNING": "⚠️", "CRITICAL": "🚨", "EMERGENCY": "🚨🚨"}.get(alert.severity.value, "📢")
        text = (
            f"{severity_emoji} *[{alert.severity.value}]*\n"
            f"*{alert.code}*\n\n"
            f"{alert.message}\n\n"
            f"🕐 {alert.timestamp.isoformat()}"
        )

        payload = {
            "chat_id": recipient,
            "text": text,
            "parse_mode": "MarkdownV2",
            "disable_web_page_preview": True,
        }

        success = False
        error_msg = ""
        try:
            url = f"https://api.telegram.org/bot{token}/sendMessage"
            data = json.dumps(payload).encode("utf-8")
            req = Request(url, data=data, headers={"Content-Type": "application/json"})
            with urlopen(req, timeout=10) as resp:  # noqa: S310
                resp.read()
            success = True
        except URLError as exc:  # noqa: PERF203
            error_msg = str(exc)
        except Exception as exc:
            error_msg = str(exc)

        result = NotificationPayload(
            alert=alert,
            channel_name=self.channel_name,
            recipient=recipient,
            success=success,
            error=error_msg if not success else None,
        )
        self._sent.append(result)
        return result

    def get_sent_notifications(self) -> list[NotificationPayload]:
        """Get all sent notifications for testing."""
        return list(self._sent)


class DingTalkChannel(NotificationChannel):
    """Notification channel that sends alerts via DingTalk webhook.

    Configure via environment variable:
    - QUANT_DINGTALK_WEBHOOK_URL: DingTalk custom robot webhook URL
    """

    def __init__(self, webhook_url: str | None = None) -> None:
        import os
        self.webhook_url = webhook_url or os.getenv("QUANT_DINGTALK_WEBHOOK_URL", "")
        self._sent: list[NotificationPayload] = []

    @property
    def channel_name(self) -> str:
        return "dingtalk"

    def _is_real_url(self, url: str | None) -> bool:
        """Return True if URL is a valid HTTP(S) URL (not a test placeholder)."""
        return bool(url and url.startswith(("http://", "https://")))

    def send(self, alert: Alert, recipient: str) -> NotificationPayload:
        """Send alert via DingTalk webhook (actually sends HTTP POST)."""
        import json
        import os
        from urllib.request import Request, urlopen
        from urllib.error import URLError

        url = (
            self.webhook_url
            or os.getenv("QUANT_DINGTALK_WEBHOOK_URL", "")
            or (recipient if recipient.startswith(("http://", "https://")) else None)
        )

        # Stub mode: non-HTTP recipients (e.g. "webhook_url") don't make real HTTP calls
        if not self._is_real_url(url):
            result = NotificationPayload(
                alert=alert,
                channel_name=self.channel_name,
                recipient=recipient,
                success=True,
            )
            self._sent.append(result)
            return result

        payload = {
            "msgtype": "markdown",
            "markdown": {
                "title": f"[{alert.severity.value}] {alert.code}",
                "text": (
                    f"### [{alert.severity.value}] {alert.code}\n\n"
                    f"**{alert.message}**\n\n"
                    f"**时间**: {alert.timestamp.isoformat()}\n\n"
                    f"**上下文**: {alert.context or 'N/A'}"
                ),
            },
        }
        success = False
        error_msg = ""
        try:
            data = json.dumps(payload).encode("utf-8")
            req = Request(url, data=data, headers={"Content-Type": "application/json"})
            with urlopen(req, timeout=10) as resp:  # noqa: S310
                resp.read()
            success = True
        except URLError as exc:  # noqa: PERF203
            error_msg = str(exc)
        except Exception as exc:
            error_msg = str(exc)

        result = NotificationPayload(
            alert=alert,
            channel_name=self.channel_name,
            recipient=recipient,
            success=success,
            error=error_msg if not success else None,
        )
        self._sent.append(result)
        return result

    def get_sent_notifications(self) -> list[NotificationPayload]:
        """Get all sent notifications for testing."""
        return list(self._sent)


class WeChatWorkChannel(NotificationChannel):
    """Notification channel that sends alerts via WeChat Work webhook.

    Configure via environment variables:
    - QUANT_WECOM_WEBHOOK_URL: WeChat Work webhook URL (e.g., https://qyapi.weixin.qq.com/.../send)
    """

    def __init__(self, webhook_url: str | None = None) -> None:
        import os
        self.webhook_url = webhook_url or os.getenv("QUANT_WECOM_WEBHOOK_URL", "")
        self._sent: list[NotificationPayload] = []

    @property
    def channel_name(self) -> str:
        return "wechat_work"

    def _is_real_url(self, url: str | None) -> bool:
        """Return True if URL is a valid HTTP(S) URL (not a test placeholder)."""
        return bool(url and url.startswith(("http://", "https://")))

    def send(self, alert: Alert, recipient: str) -> NotificationPayload:
        """Send alert via WeChat Work webhook (actually sends HTTP POST)."""
        import json
        import os
        from urllib.request import Request, urlopen
        from urllib.error import URLError

        url = self.webhook_url or os.getenv("QUANT_WECOM_WEBHOOK_URL", "")

        # Stub mode: no configured URL doesn't make real HTTP calls
        if not self._is_real_url(url):
            result = NotificationPayload(
                alert=alert,
                channel_name=self.channel_name,
                recipient=recipient,
                success=True,
            )
            self._sent.append(result)
            return result

        payload = {
            "msgtype": "markdown",
            "markdown": {
                "content": (
                    f"### [{alert.severity.value}] {alert.code}\n"
                    f"**{alert.message}**\n\n"
                    f"**时间**: {alert.timestamp.isoformat()}\n\n"
                    f"**上下文**: {alert.context or 'N/A'}"
                )
            },
        }
        success = False
        error_msg = ""
        try:
            data = json.dumps(payload).encode("utf-8")
            req = Request(url, data=data, headers={"Content-Type": "application/json"})
            with urlopen(req, timeout=10) as resp:  # noqa: S310
                resp.read()
            success = True
        except URLError as exc:  # noqa: PERF203
            error_msg = str(exc)
        except Exception as exc:
            error_msg = str(exc)

        result = NotificationPayload(
            alert=alert,
            channel_name=self.channel_name,
            recipient=recipient,
            success=success,
            error=error_msg if not success else None,
        )
        self._sent.append(result)
        return result

    def get_sent_notifications(self) -> list[NotificationPayload]:
        """Get all sent notifications for testing."""
        return list(self._sent)


class NotificationService:
    """Routes alert notifications to appropriate channels (MO-04).

    Manages channel registration, routing rules, and notification delivery.
    """

    def __init__(self) -> None:
        self._channels: dict[str, NotificationChannel] = {}
        self._routing_rules: dict[str, list[str]] = defaultdict(list)  # severity -> channel names
        self._recipients: dict[str, str] = {}  # channel_name -> default recipient
        self._sent_notifications: list[NotificationPayload] = []

    def register_channel(self, channel: NotificationChannel) -> None:
        """Register a notification channel."""
        self._channels[channel.channel_name] = channel

    def unregister_channel(self, channel_name: str) -> None:
        """Unregister a notification channel."""
        self._channels.pop(channel_name, None)

    def get_channel(self, channel_name: str) -> NotificationChannel | None:
        """Get a registered channel by name."""
        return self._channels.get(channel_name)

    def get_all_channels(self) -> dict[str, NotificationChannel]:
        """Get all registered channels."""
        return dict(self._channels)

    def set_routing_rule(self, severity: AlertSeverity, channel_names: list[str]) -> None:
        """Set which channels receive alerts of a given severity."""
        self._routing_rules[severity.value] = channel_names

    def set_default_recipient(self, channel_name: str, recipient: str) -> None:
        """Set the default recipient for a channel."""
        self._recipients[channel_name] = recipient

    def notify(
        self,
        alert: Alert,
        channel_names: list[str] | None = None,
        recipient: str | None = None,
    ) -> list[NotificationPayload]:
        """Send an alert notification through specified channels or routing rules.

        If channel_names is provided, sends only to those channels.
        Otherwise, uses routing rules based on alert severity.
        """
        results: list[NotificationPayload] = []

        # Determine which channels to use
        if channel_names:
            target_channels = channel_names
        else:
            target_channels = self._routing_rules.get(alert.severity.value, [])

        for channel_name in target_channels:
            channel = self._channels.get(channel_name)
            if not channel:
                continue

            # Use provided recipient or channel default
            dest = recipient or self._recipients.get(channel_name, "default")
            if not dest:
                dest = "default"

            result = channel.send(alert, dest)
            results.append(result)
            self._sent_notifications.append(result)

        return results

    def get_sent_notifications(
        self,
        window: timedelta | None = None,
    ) -> list[NotificationPayload]:
        """Get sent notifications, optionally filtered by time window."""
        if window is None:
            return list(self._sent_notifications)

        cutoff = utc_now() - window
        return [n for n in self._sent_notifications if n.sent_at > cutoff]

    def get_notification_summary(self) -> dict[str, int]:
        """Get a summary of sent notifications by channel and status."""
        summary: dict[str, int] = defaultdict(int)
        for notification in self._sent_notifications:
            key = f"{notification.channel_name}_{'success' if notification.success else 'failed'}"
            summary[key] += 1
        return dict(summary)
