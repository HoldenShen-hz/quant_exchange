"""Configuration objects loaded from environment variables or mappings."""

from __future__ import annotations

import os
from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass(slots=True, frozen=True)
class DatabaseSettings:
    """Database configuration used by the persistence layer."""

    url: str = ":memory:"


@dataclass(slots=True, frozen=True)
class ApiSettings:
    """Control-plane API configuration."""

    base_path: str = "/api/v1"
    token_ttl_seconds: int = 7200


@dataclass(slots=True, frozen=True)
class SchedulerSettings:
    """Scheduler configuration for sync and reporting jobs."""

    enabled: bool = True
    default_interval_seconds: int = 60


@dataclass(slots=True, frozen=True)
class AdapterSettings:
    """Runtime configuration for market adapters."""

    environment: str = "test"
    default_exchange_code: str = "SIM"


@dataclass(slots=True, frozen=True)
class RiskSettings:
    """Risk engine configuration."""

    max_order_notional: float = 50_000.0
    max_single_order_quantity: float = 10_000.0
    max_position_notional: float = 100_000.0
    max_gross_notional: float = 200_000.0
    max_leverage: float = 2.0
    max_drawdown: float = 0.20
    kill_switch_enabled: bool = True


@dataclass(slots=True, frozen=True)
class MonitoringSettings:
    """Monitoring and alerting configuration."""

    dedup_window_seconds: int = 300
    escalation_threshold: int = 3
    stale_data_threshold_seconds: int = 60
    alert_channels: tuple[str, ...] = ("log",)


@dataclass(slots=True, frozen=True)
class BacktestSettings:
    """Backtest engine defaults."""

    default_fee_rate: float = 0.001
    default_slippage_bps: float = 5.0
    default_initial_cash: float = 100_000.0


@dataclass(slots=True, frozen=True)
class IntelligenceSettings:
    """Intelligence engine configuration."""

    bias_window_hours: int = 24
    sentiment_threshold: float = 0.15


@dataclass(slots=True, frozen=True)
class AppSettings:
    """Top-level application settings container."""

    database: DatabaseSettings = DatabaseSettings()
    api: ApiSettings = ApiSettings()
    scheduler: SchedulerSettings = SchedulerSettings()
    adapters: AdapterSettings = AdapterSettings()
    risk: RiskSettings = RiskSettings()
    monitoring: MonitoringSettings = MonitoringSettings()
    backtest: BacktestSettings = BacktestSettings()
    intelligence: IntelligenceSettings = IntelligenceSettings()

    @classmethod
    def from_env(cls) -> "AppSettings":
        """Create settings from environment variables."""

        return cls(
            database=DatabaseSettings(url=os.getenv("QUANT_DB_URL", ":memory:")),
            api=ApiSettings(
                base_path=os.getenv("QUANT_API_BASE_PATH", "/api/v1"),
                token_ttl_seconds=int(os.getenv("QUANT_API_TOKEN_TTL_SECONDS", "7200")),
            ),
            scheduler=SchedulerSettings(
                enabled=os.getenv("QUANT_SCHEDULER_ENABLED", "true").lower() == "true",
                default_interval_seconds=int(os.getenv("QUANT_SCHEDULER_DEFAULT_INTERVAL_SECONDS", "60")),
            ),
            adapters=AdapterSettings(
                environment=os.getenv("QUANT_ADAPTER_ENV", "test"),
                default_exchange_code=os.getenv("QUANT_DEFAULT_EXCHANGE_CODE", "SIM"),
            ),
            risk=RiskSettings(
                max_order_notional=float(os.getenv("QUANT_RISK_MAX_ORDER_NOTIONAL", "50000")),
                max_leverage=float(os.getenv("QUANT_RISK_MAX_LEVERAGE", "2.0")),
                max_drawdown=float(os.getenv("QUANT_RISK_MAX_DRAWDOWN", "0.20")),
            ),
            monitoring=MonitoringSettings(
                dedup_window_seconds=int(os.getenv("QUANT_MONITORING_DEDUP_SECONDS", "300")),
                escalation_threshold=int(os.getenv("QUANT_MONITORING_ESCALATION_THRESHOLD", "3")),
            ),
            backtest=BacktestSettings(
                default_fee_rate=float(os.getenv("QUANT_BACKTEST_FEE_RATE", "0.001")),
                default_slippage_bps=float(os.getenv("QUANT_BACKTEST_SLIPPAGE_BPS", "5.0")),
                default_initial_cash=float(os.getenv("QUANT_BACKTEST_INITIAL_CASH", "100000")),
            ),
            intelligence=IntelligenceSettings(
                bias_window_hours=int(os.getenv("QUANT_INTEL_BIAS_WINDOW_HOURS", "24")),
            ),
        )

    @classmethod
    def from_mapping(cls, mapping: dict[str, Any]) -> "AppSettings":
        """Create settings from a nested mapping."""

        database = DatabaseSettings(**mapping.get("database", {}))
        api = ApiSettings(**mapping.get("api", {}))
        scheduler = SchedulerSettings(**mapping.get("scheduler", {}))
        adapters = AdapterSettings(**mapping.get("adapters", {}))
        risk = RiskSettings(**mapping.get("risk", {}))
        monitoring = MonitoringSettings(**mapping.get("monitoring", {}))
        backtest = BacktestSettings(**mapping.get("backtest", {}))
        intelligence = IntelligenceSettings(**mapping.get("intelligence", {}))
        return cls(
            database=database, api=api, scheduler=scheduler, adapters=adapters,
            risk=risk, monitoring=monitoring, backtest=backtest, intelligence=intelligence,
        )

    def to_dict(self) -> dict[str, Any]:
        """Return a plain dictionary representation of the settings."""

        return asdict(self)
