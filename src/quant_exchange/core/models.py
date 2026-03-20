"""Core domain models shared across the MVP quant trading platform."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any


def utc_now() -> datetime:
    """Return a timezone-aware UTC timestamp."""

    return datetime.now(timezone.utc)


class MarketType(str, Enum):
    """Supported market families."""

    STOCK = "stock"
    FUTURES = "futures"
    CRYPTO = "crypto"
    OPTION = "option"


class InstrumentType(str, Enum):
    """Fine-grained instrument classification within a market family."""

    SPOT = "spot"
    PERPETUAL = "perpetual"
    FUTURES = "futures"
    EQUITY = "equity"
    OPTION = "option"


class InstrumentStatus(str, Enum):
    """Lifecycle status of a tradable instrument."""

    ACTIVE = "active"
    SUSPENDED = "suspended"
    DELISTED = "delisted"


class Direction(str, Enum):
    """Directional output used by signals and bias engines."""

    LONG = "long"
    SHORT = "short"
    FLAT = "flat"


class SentimentLabel(str, Enum):
    """Normalized sentiment labels produced by the intelligence engine."""

    POSITIVE = "positive"
    NEGATIVE = "negative"
    NEUTRAL = "neutral"


class OrderSide(str, Enum):
    """Trading side used by orders and fills."""

    BUY = "buy"
    SELL = "sell"


class OrderType(str, Enum):
    """Order styles supported by the execution layer."""

    MARKET = "market"
    LIMIT = "limit"
    STOP_LOSS = "stop_loss"
    TAKE_PROFIT = "take_profit"
    CONDITIONAL = "conditional"


class TimeInForce(str, Enum):
    """Order lifetime policy."""

    GTC = "gtc"
    IOC = "ioc"
    FOK = "fok"


class OrderStatus(str, Enum):
    """Order lifecycle states tracked by the OMS."""

    CREATED = "created"
    PENDING_SUBMIT = "pending_submit"
    ACCEPTED = "accepted"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    CANCELLED = "cancelled"
    REJECTED = "rejected"
    FAILED = "failed"


class AlertSeverity(str, Enum):
    """Severity levels for monitoring alerts."""

    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"
    EMERGENCY = "emergency"


class Role(str, Enum):
    """User roles used by the RBAC layer."""

    ADMIN = "admin"
    RESEARCHER = "researcher"
    TRADER = "trader"
    RISK = "risk"
    RISK_OFFICER = "risk_officer"
    AUDITOR = "auditor"
    VIEWER = "viewer"


class Action(str, Enum):
    """Protected operations that may require authorization."""

    VIEW = "view"
    RUN_BACKTEST = "run_backtest"
    SUBMIT_ORDER = "submit_order"
    CANCEL_ORDER = "cancel_order"
    CHANGE_LIMITS = "change_limits"
    TRIGGER_KILL_SWITCH = "trigger_kill_switch"
    VIEW_AUDIT = "view_audit"
    DEPLOY_STRATEGY = "deploy_strategy"
    MODIFY_RISK_RULES = "modify_risk_rules"
    DELETE_DATA = "delete_data"
    MANUAL_OVERRIDE = "manual_override"


class EventTag(str, Enum):
    """Classification tags for market intelligence events."""

    LISTING = "listing"
    DELISTING = "delisting"
    SECURITY_INCIDENT = "security_incident"
    REGULATORY = "regulatory"
    ETF_MACRO = "etf_macro"
    LIQUIDATION = "liquidation"
    PARTNERSHIP = "partnership"
    PRODUCT_LAUNCH = "product_launch"
    EARNINGS = "earnings"
    DIVIDEND = "dividend"
    SPLIT = "split"
    M_AND_A = "m_and_a"
    OTHER = "other"


class AllocationMethod(str, Enum):
    """Portfolio capital allocation methods."""

    FIXED_WEIGHT = "fixed_weight"
    VOLATILITY_TARGET = "volatility_target"
    RISK_PARITY = "risk_parity"
    MEAN_VARIANCE = "mean_variance"
    HRP = "hrp"
    KELLY = "kelly"


@dataclass(slots=True, frozen=True)
class Instrument:
    """Tradable instrument metadata normalized across markets."""

    instrument_id: str
    symbol: str
    market: MarketType
    instrument_type: str = "spot"
    market_region: str = "GLOBAL"
    tick_size: float = 0.01
    lot_size: float = 1.0
    contract_multiplier: float = 1.0
    quote_currency: str = "USD"
    base_currency: str = ""
    settlement_cycle: str | None = None
    short_sellable: bool = False
    expiry_at: datetime | None = None
    trading_sessions: tuple[tuple[str, str], ...] = ()
    trading_rules: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True, frozen=True)
class Kline:
    """Time-bucketed OHLCV market data."""

    instrument_id: str
    timeframe: str
    open_time: datetime
    close_time: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    turnover: float = 0.0


@dataclass(slots=True, frozen=True)
class Tick:
    """Tick-level trade or quote-like event used by low-latency workflows."""

    instrument_id: str
    timestamp: datetime
    price: float
    size: float
    side: str = ""
    exchange_trade_id: str = ""


@dataclass(slots=True, frozen=True)
class OrderBookLevel:
    """One price level in an order book snapshot."""

    price: float
    quantity: float


@dataclass(slots=True, frozen=True)
class OrderBookSnapshot:
    """Point-in-time order book state for one instrument."""

    instrument_id: str
    timestamp: datetime
    bid_levels: tuple[OrderBookLevel, ...] = ()
    ask_levels: tuple[OrderBookLevel, ...] = ()
    sequence_no: int = 0


@dataclass(slots=True, frozen=True)
class FundingRate:
    """Perpetual contract funding rate observation."""

    instrument_id: str
    timestamp: datetime
    funding_rate: float
    predicted_funding_rate: float = 0.0


@dataclass(slots=True, frozen=True)
class AccountSnapshot:
    """Point-in-time account valuation used by risk and portfolio modules."""

    account_id: str
    timestamp: datetime
    cash: float
    equity: float
    margin_ratio: float = 0.0
    unrealized_pnl: float = 0.0
    realized_pnl: float = 0.0


@dataclass(slots=True, frozen=True)
class AccountLedgerEntry:
    """Immutable ledger entry for fund-level accounting."""

    account_id: str
    entry_time: datetime
    entry_type: str  # TRADE, FEE, FUNDING, PNL, DEPOSIT, WITHDRAWAL
    amount: float
    currency: str = "USD"
    reference_id: str = ""


@dataclass(slots=True, frozen=True)
class MarketDocument:
    """Normalized text document used for market intelligence and sentiment analysis."""

    document_id: str
    source: str
    instrument_id: str
    published_at: datetime
    title: str
    content: str
    language: str = "unknown"
    event_tag: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True, frozen=True)
class SentimentResult:
    """Document-level sentiment output produced by the intelligence engine."""

    document_id: str
    instrument_id: str
    score: float
    label: SentimentLabel
    confidence: float
    positive_hits: int
    negative_hits: int


@dataclass(slots=True, frozen=True)
class DirectionalBias:
    """Aggregated directional view over a time window for a single instrument."""

    instrument_id: str
    as_of: datetime
    window: timedelta
    score: float
    direction: Direction
    confidence: float
    supporting_documents: int


@dataclass(slots=True, frozen=True)
class OrderRequest:
    """Intent to trade before the OMS assigns a platform order identifier."""

    client_order_id: str
    instrument_id: str
    side: OrderSide
    quantity: float
    order_type: OrderType = OrderType.MARKET
    price: float | None = None
    tif: TimeInForce = TimeInForce.GTC
    strategy_id: str = "manual"
    reduce_only: bool = False
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class Order:
    """Mutable order state managed by the in-memory order manager."""

    order_id: str
    request: OrderRequest
    status: OrderStatus = OrderStatus.CREATED
    created_at: datetime = field(default_factory=utc_now)
    updated_at: datetime = field(default_factory=utc_now)
    filled_quantity: float = 0.0
    average_fill_price: float = 0.0
    rejection_reason: str | None = None

    @property
    def remaining_quantity(self) -> float:
        """Return the quantity that still needs to be matched."""

        return max(self.request.quantity - self.filled_quantity, 0.0)


@dataclass(slots=True, frozen=True)
class Fill:
    """Executed slice of an order."""

    fill_id: str
    order_id: str
    instrument_id: str
    side: OrderSide
    quantity: float
    price: float
    timestamp: datetime
    fee: float = 0.0


@dataclass(slots=True)
class Position:
    """Current holdings and realized profit and loss for one instrument."""

    instrument_id: str
    quantity: float = 0.0
    average_cost: float = 0.0
    realized_pnl: float = 0.0
    last_price: float = 0.0
    # Leverage and margin fields
    is_leveraged: bool = False
    leverage: float = 1.0
    margin_used: float = 0.0
    # Funding fields (for perpetual contracts)
    funding_accrued: float = 0.0


@dataclass(slots=True, frozen=True)
class RiskLimits:
    """Multi-level account, strategy, and order guardrails used by the risk engine."""

    # Order-level limits
    max_order_notional: float = 50_000.0
    max_single_order_quantity: float = 10_000.0
    max_price_deviation: float = 0.05  # max % deviation from last price
    max_order_frequency: int = 100  # max orders per minute
    # Position-level limits
    max_position_notional: float = 100_000.0
    # Strategy-level limits
    max_strategy_daily_loss: float = 10_000.0
    max_strategy_drawdown: float = 0.15
    max_consecutive_losses: int = 10
    # Account-level limits
    max_gross_notional: float = 200_000.0
    max_leverage: float = 2.0
    max_drawdown: float = 0.20
    margin_warning_ratio: float = 0.8
    margin_block_ratio: float = 0.9
    margin_liquidation_ratio: float = 0.95


@dataclass(slots=True, frozen=True)
class RiskDecision:
    """Result of a pre-trade risk evaluation."""

    approved: bool
    reasons: tuple[str, ...] = ()


@dataclass(slots=True, frozen=True)
class PortfolioSnapshot:
    """Point-in-time portfolio valuation and exposure summary."""

    timestamp: datetime
    cash: float
    positions_value: float
    equity: float
    gross_exposure: float
    net_exposure: float
    leverage: float
    drawdown: float


@dataclass(slots=True, frozen=True)
class Alert:
    """Monitoring event emitted by the platform runtime."""

    code: str
    severity: AlertSeverity
    message: str
    timestamp: datetime
    context: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True, frozen=True)
class StrategySignal:
    """Normalized strategy output expressed as a target portfolio weight."""

    instrument_id: str
    timestamp: datetime
    target_weight: float
    reason: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True, frozen=True)
class CostBreakdown:
    """Detailed cost analysis for a trading period."""

    total_commission: float = 0.0
    total_slippage: float = 0.0
    total_funding: float = 0.0
    total_cost: float = 0.0


@dataclass(slots=True, frozen=True)
class PerformanceMetrics:
    """Performance statistics returned by the backtest engine."""

    total_return: float
    annualized_return: float
    max_drawdown: float
    sharpe: float
    sortino: float
    calmar: float
    win_rate: float
    profit_factor: float
    turnover: float
    total_trades: int = 0
    avg_trade_return: float = 0.0
    costs: CostBreakdown = field(default_factory=CostBreakdown)


@dataclass(slots=True, frozen=True)
class BacktestResult:
    """Full backtest output, including metrics, fills, alerts, and bias history."""

    strategy_id: str
    instrument_id: str
    equity_curve: tuple[tuple[datetime, float], ...]
    orders: tuple[Order, ...]
    fills: tuple[Fill, ...]
    metrics: PerformanceMetrics
    alerts: tuple[Alert, ...]
    bias_history: tuple[DirectionalBias, ...]
    risk_rejections: tuple[RiskDecision, ...]
    audit_result: Any = None  # BT-08: BiasAuditResult from BiasAuditService


@dataclass(slots=True, frozen=True)
class AuditEvent:
    """Immutable audit log entry recorded by the security module."""

    actor: str
    action: Action
    resource: str
    timestamp: datetime
    success: bool
    details: dict[str, Any] = field(default_factory=dict)


# ─── MD-10: Corporate Actions ──────────────────────────────────────────────────

@dataclass(slots=True, frozen=True)
class CorporateAction:
    """Represents a corporate action event (dividend, split, rights issue, merger).

    Used for:
    - Backtest price adjustments (split-adjusted, dividend-adjusted prices)
    - Fundamental event tracking (ex-date, record date, payment date)
    - Reference data (MD-10)
    """

    action_id: str
    instrument_id: str
    event_type: str  # "dividend" | "split" | "rights_issue" | "merger" | "spinoff"
    ex_date: datetime | None  # First day without the dividend/split benefit
    record_date: datetime | None
    payment_date: datetime | None
    # For dividends
    dividend_per_share: float | None = None
    # For splits
    split_ratio: tuple[int, int] | None = None  # e.g. (2, 1) = 2-for-1 split
    # For rights issues
    rights_issue_price: float | None = None
    # For all
    currency: str = "CNY"
    status: str = "announced"  # "announced" | "confirmed" | "completed" | "cancelled"
    created_at: datetime = field(default_factory=utc_now)
