"""In-memory order management and paper execution helpers.

Implements the documented OMS contract:
- Idempotent order submission (unique client_order_id)
- Full state machine with transition validation
- Order status history for audit trail
- Recoverable non-terminal order queries

Enhanced for EX-01~EX-08:
- EX-01: Multi-channel adapter abstraction and registry
- EX-02: Market, limit, cancel order types
- EX-03: Order state synchronization and persistence
- EX-04: Idempotency, retry mechanism, and compensation tasks
- EX-05: Pre-trade risk engine integration
- EX-06: Account/strategy/instrument permission control
- EX-07: Exception handling (disconnect, rate limit, reject, latency)
- EX-08: Smart order routing and multi-venue execution
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any
from uuid import uuid4

from quant_exchange.core.models import (
    Alert,
    AlertSeverity,
    Fill,
    Kline,
    Order,
    OrderRequest,
    OrderSide,
    OrderStatus,
    OrderType,
    PortfolioSnapshot,
    utc_now,
)

# Valid state transitions per the documented order lifecycle
_VALID_TRANSITIONS: dict[OrderStatus, set[OrderStatus]] = {
    OrderStatus.CREATED: {OrderStatus.PENDING_SUBMIT, OrderStatus.ACCEPTED, OrderStatus.REJECTED},
    OrderStatus.PENDING_SUBMIT: {OrderStatus.ACCEPTED, OrderStatus.REJECTED, OrderStatus.FAILED},
    OrderStatus.ACCEPTED: {OrderStatus.PARTIALLY_FILLED, OrderStatus.FILLED, OrderStatus.CANCELLED},
    OrderStatus.PARTIALLY_FILLED: {OrderStatus.PARTIALLY_FILLED, OrderStatus.FILLED, OrderStatus.CANCELLED},
    OrderStatus.FILLED: set(),
    OrderStatus.CANCELLED: set(),
    OrderStatus.REJECTED: set(),
    OrderStatus.FAILED: set(),
}


class OrderManager:
    """Track order lifecycle events and enforce idempotent submission semantics."""

    FINAL_STATUSES = {OrderStatus.FILLED, OrderStatus.CANCELLED, OrderStatus.REJECTED, OrderStatus.FAILED}

    def __init__(self) -> None:
        self.orders: dict[str, Order] = {}
        self.client_order_index: dict[str, str] = {}
        self.order_events: list[tuple[str, OrderStatus, str]] = []
        self.status_history: list[dict[str, Any]] = []
        self._fills: list[Fill] = []

    def submit_order(self, request: OrderRequest) -> Order:
        """Accept an order request or return the existing order for the same client id."""

        existing_order_id = self.client_order_index.get(request.client_order_id)
        if existing_order_id is not None:
            return self.orders[existing_order_id]
        order = Order(order_id=f"ord_{uuid4().hex[:12]}", request=request, status=OrderStatus.ACCEPTED)
        self.orders[order.order_id] = order
        self.client_order_index[request.client_order_id] = order.order_id
        self._record_transition(order, OrderStatus.ACCEPTED, "accepted")
        return order

    def reject_order(self, request: OrderRequest, reason: str) -> Order:
        """Create a terminal rejected order record."""

        order = Order(
            order_id=f"ord_{uuid4().hex[:12]}",
            request=request,
            status=OrderStatus.REJECTED,
            rejection_reason=reason,
        )
        self.orders[order.order_id] = order
        self._record_transition(order, OrderStatus.REJECTED, reason)
        return order

    def cancel_order(self, order_id: str) -> Order:
        """Cancel a live order unless it already reached a terminal state."""

        order = self.orders[order_id]
        if order.status in self.FINAL_STATUSES:
            return order
        self._transition(order, OrderStatus.CANCELLED, "cancelled")
        return order

    def fail_order(self, order_id: str, reason: str) -> Order:
        """Mark an order as failed (e.g. exchange communication error)."""

        order = self.orders[order_id]
        if order.status in self.FINAL_STATUSES:
            return order
        self._transition(order, OrderStatus.FAILED, reason)
        order.rejection_reason = reason
        return order

    def apply_fill(self, fill: Fill) -> Order:
        """Update an order with a new fill event."""

        order = self.orders[fill.order_id]
        total_notional = order.average_fill_price * order.filled_quantity + fill.price * fill.quantity
        order.filled_quantity += fill.quantity
        if order.filled_quantity > 0:
            order.average_fill_price = total_notional / order.filled_quantity
        order.updated_at = fill.timestamp
        if order.filled_quantity + 1e-12 >= order.request.quantity:
            new_status = OrderStatus.FILLED
        else:
            new_status = OrderStatus.PARTIALLY_FILLED
        self._transition(order, new_status, "fill")
        self._fills.append(fill)
        return order

    def get_open_orders(self) -> list[Order]:
        """Return all non-terminal orders for reconciliation or recovery."""

        return [o for o in self.orders.values() if o.status not in self.FINAL_STATUSES]

    def get_orders_by_strategy(self, strategy_id: str) -> list[Order]:
        """Return all orders for a given strategy."""

        return [o for o in self.orders.values() if o.request.strategy_id == strategy_id]

    def get_orders_by_instrument(self, instrument_id: str) -> list[Order]:
        """Return all orders for a given instrument."""

        return [o for o in self.orders.values() if o.request.instrument_id == instrument_id]

    @property
    def fills(self) -> list[Fill]:
        """Return all recorded fills."""

        return list(self._fills)

    def _transition(self, order: Order, new_status: OrderStatus, detail: str) -> None:
        """Apply a validated state transition to an order."""

        allowed = _VALID_TRANSITIONS.get(order.status, set())
        if new_status not in allowed:
            # Allow same-status for partial fill updates
            if new_status != order.status:
                raise ValueError(
                    f"invalid_transition:{order.status.value}->{new_status.value}"
                )
        order.status = new_status
        order.updated_at = utc_now()
        self._record_transition(order, new_status, detail)

    def _record_transition(self, order: Order, status: OrderStatus, detail: str) -> None:
        """Record a status change to both the event log and audit history."""

        self.order_events.append((order.order_id, status, detail))
        self.status_history.append({
            "order_id": order.order_id,
            "status": status.value,
            "detail": detail,
            "timestamp": order.updated_at.isoformat(),
        })


class PaperExecutionEngine:
    """Deterministic paper execution model for backtests and simulations."""

    def __init__(self, *, fee_rate: float = 0.001, slippage_bps: float = 5.0, max_fill_ratio: float = 1.0) -> None:
        self.fee_rate = fee_rate
        self.slippage_bps = slippage_bps
        self.max_fill_ratio = max_fill_ratio

    def execute_on_bar(self, order: Order, bar: Kline) -> list[Fill]:
        """Match an order against one bar and return the resulting fill list."""

        if order.status in {OrderStatus.CANCELLED, OrderStatus.REJECTED, OrderStatus.FILLED}:
            return []
        fillable_qty = order.remaining_quantity * self.max_fill_ratio
        if fillable_qty <= 0:
            return []
        fill_price = self._match_price(order, bar)
        if fill_price is None:
            return []
        fee = abs(fillable_qty * fill_price) * self.fee_rate
        return [
            Fill(
                fill_id=f"fill_{uuid4().hex[:12]}",
                order_id=order.order_id,
                instrument_id=order.request.instrument_id,
                side=order.request.side,
                quantity=fillable_qty,
                price=fill_price,
                timestamp=bar.close_time,
                fee=fee,
            )
        ]

    def _match_price(self, order: Order, bar: Kline) -> float | None:
        """Return a matched price when the bar is compatible with the order."""

        request = order.request
        slippage = self.slippage_bps / 10_000.0
        if request.order_type == OrderType.MARKET:
            if request.side == OrderSide.BUY:
                return min(bar.high, bar.open * (1.0 + slippage))
            return max(bar.low, bar.open * (1.0 - slippage))
        if request.order_type == OrderType.LIMIT and request.price is not None:
            if request.side == OrderSide.BUY and bar.low <= request.price:
                return min(request.price, bar.open)
            if request.side == OrderSide.SELL and bar.high >= request.price:
                return max(request.price, bar.open)
        return None


# ─── EX-01: Multi-Channel Adapter Abstraction ──────────────────────────────────


class ExecutionChannelState(str, Enum):
    """State of an execution channel."""
    CONNECTED = "connected"
    DISCONNECTED = "disconnected"
    RECONNECTING = "reconnecting"
    RATE_LIMITED = "rate_limited"


@dataclass
class ExecutionChannelMetrics:
    """Metrics for an execution channel."""
    channel_name: str
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    rejected_requests: int = 0
    average_latency_ms: float = 0.0
    last_request_at: datetime | None = None
    rate_limit_remaining: int = 100  # Default high limit


class ExecutionChannel(ABC):
    """Abstract base class for execution channels (EX-01).

    A channel represents a specific execution venue or broker connection.
    """

    @property
    @abstractmethod
    def channel_id(self) -> str:
        """Return unique identifier for this channel."""
        raise NotImplementedError

    @property
    @abstractmethod
    def channel_name(self) -> str:
        """Return human-readable name for this channel."""
        raise NotImplementedError

    @property
    @abstractmethod
    def supported_order_types(self) -> set[OrderType]:
        """Return set of supported order types."""
        raise NotImplementedError

    @abstractmethod
    def connect(self) -> bool:
        """Establish connection to the channel."""
        raise NotImplementedError

    @abstractmethod
    def disconnect(self) -> bool:
        """Close connection to the channel."""
        raise NotImplementedError

    @abstractmethod
    def is_connected(self) -> bool:
        """Return True if channel is connected and ready."""
        raise NotImplementedError

    @abstractmethod
    def submit_order(self, request: OrderRequest) -> dict:
        """Submit order to the channel. Returns normalized response."""
        raise NotImplementedError

    @abstractmethod
    def cancel_order(self, venue_order_id: str) -> dict:
        """Cancel order on the channel."""
        raise NotImplementedError

    @abstractmethod
    def get_account_snapshot(self) -> PortfolioSnapshot:
        """Get current account snapshot from the channel."""
        raise NotImplementedError

    def get_metrics(self) -> ExecutionChannelMetrics:
        """Return current channel metrics."""
        return self._metrics

    def __init__(self) -> None:
        self._state = ExecutionChannelState.DISCONNECTED
        self._metrics = ExecutionChannelMetrics(channel_name=self.channel_name)


class SimulatedExecutionChannel(ExecutionChannel):
    """Simulated execution channel for testing (EX-01)."""

    def __init__(
        self,
        channel_id: str = "sim_exchange",
        channel_name: str = "Simulated Exchange",
        supported_types: set[OrderType] | None = None,
    ) -> None:
        # Set attributes before calling super().__init__() since parent accesses them
        self._channel_id = channel_id
        self._channel_name = channel_name
        self._supported_types = supported_types or {OrderType.MARKET, OrderType.LIMIT, OrderType.STOP_LOSS, OrderType.TAKE_PROFIT}
        self._order_counter = 0
        super().__init__()

    @property
    def channel_id(self) -> str:
        return self._channel_id

    @property
    def channel_name(self) -> str:
        return self._channel_name

    @property
    def supported_order_types(self) -> set[OrderType]:
        return self._supported_types

    def connect(self) -> bool:
        self._state = ExecutionChannelState.CONNECTED
        return True

    def disconnect(self) -> bool:
        self._state = ExecutionChannelState.DISCONNECTED
        return True

    def is_connected(self) -> bool:
        return self._state == ExecutionChannelState.CONNECTED

    def submit_order(self, request: OrderRequest) -> dict:
        self._metrics.total_requests += 1
        self._metrics.last_request_at = utc_now()

        if not self.is_connected():
            self._metrics.failed_requests += 1
            return {
                "success": False,
                "error": "Channel disconnected",
                "channel_id": self._channel_id,
            }

        if request.order_type not in self._supported_types:
            self._metrics.rejected_requests += 1
            return {
                "success": False,
                "error": f"Order type {request.order_type} not supported",
                "channel_id": self._channel_id,
            }

        self._order_counter += 1
        self._metrics.successful_requests += 1
        return {
            "success": True,
            "exchange_order_id": f"{self._channel_id}_ord_{self._order_counter}",
            "client_order_id": request.client_order_id,
            "status": "SUBMITTED",
            "channel_id": self._channel_id,
        }

    def cancel_order(self, venue_order_id: str) -> dict:
        self._metrics.total_requests += 1
        self._metrics.last_request_at = utc_now()

        if not self.is_connected():
            self._metrics.failed_requests += 1
            return {"success": False, "error": "Channel disconnected"}

        self._metrics.successful_requests += 1
        return {
            "success": True,
            "exchange_order_id": venue_order_id,
            "status": "CANCELLED",
        }

    def get_account_snapshot(self) -> PortfolioSnapshot:
        return PortfolioSnapshot(
            timestamp=utc_now(),
            cash=100_000.0,
            positions_value=0.0,
            equity=100_000.0,
            gross_exposure=0.0,
            net_exposure=0.0,
            leverage=1.0,
            drawdown=0.0,
        )


# ─── EX-06: Permission Control ──────────────────────────────────────────────────


@dataclass
class TradingPermission:
    """Trading permission for a specific scope."""
    account_id: str
    strategy_ids: set[str] = field(default_factory=set)  # Empty = all strategies
    instrument_ids: set[str] = field(default_factory=set)  # Empty = all instruments
    allowed_sides: set[OrderSide] = field(default_factory=set)  # Empty = all sides
    max_order_value: float | None = None
    max_position_value: float | None = None
    enabled: bool = True


class PermissionController:
    """Enforces account/strategy/instrument trading permissions (EX-06)."""

    def __init__(self) -> None:
        self._permissions: dict[str, TradingPermission] = {}
        self._blocked_instruments: set[str] = set()
        self._blocked_strategies: set[str] = set()

    def set_permission(self, permission: TradingPermission) -> None:
        """Set or update trading permission for an account."""
        self._permissions[permission.account_id] = permission

    def get_permission(self, account_id: str) -> TradingPermission | None:
        """Get trading permission for an account."""
        return self._permissions.get(account_id)

    def revoke_permission(self, account_id: str) -> None:
        """Remove trading permission for an account."""
        self._permissions.pop(account_id, None)

    def block_instrument(self, instrument_id: str) -> None:
        """Block trading for a specific instrument globally."""
        self._blocked_instruments.add(instrument_id)

    def unblock_instrument(self, instrument_id: str) -> None:
        """Remove block on a specific instrument."""
        self._blocked_instruments.discard(instrument_id)

    def block_strategy(self, strategy_id: str) -> None:
        """Block a specific strategy from trading."""
        self._blocked_strategies.add(strategy_id)

    def unblock_strategy(self, strategy_id: str) -> None:
        """Remove block on a specific strategy."""
        self._blocked_strategies.discard(strategy_id)

    def check_permission(
        self,
        account_id: str,
        strategy_id: str | None,
        instrument_id: str,
        side: OrderSide,
        order_value: float,
    ) -> tuple[bool, str | None]:
        """Check if a trading request is permitted.

        Returns (allowed, reason_if_denied).
        """
        # Check if instrument is globally blocked
        if instrument_id in self._blocked_instruments:
            return False, f"Instrument {instrument_id} is blocked"

        # Check if strategy is globally blocked
        if strategy_id and strategy_id in self._blocked_strategies:
            return False, f"Strategy {strategy_id} is blocked"

        # Get account permission
        permission = self._permissions.get(account_id)
        if not permission:
            return False, f"No trading permission for account {account_id}"

        if not permission.enabled:
            return False, f"Trading disabled for account {account_id}"

        # Check strategy permission
        if permission.strategy_ids and strategy_id and strategy_id not in permission.strategy_ids:
            return False, f"Strategy {strategy_id} not allowed for account {account_id}"

        # Check instrument permission
        if permission.instrument_ids and instrument_id not in permission.instrument_ids:
            return False, f"Instrument {instrument_id} not allowed for account {account_id}"

        # Check side permission
        if permission.allowed_sides and side not in permission.allowed_sides:
            return False, f"Side {side} not allowed for account {account_id}"

        # Check order value limit
        if permission.max_order_value and order_value > permission.max_order_value:
            return False, f"Order value {order_value} exceeds limit {permission.max_order_value}"

        return True, None

    def get_blocked_instruments(self) -> set[str]:
        """Return set of globally blocked instruments."""
        return set(self._blocked_instruments)

    def get_blocked_strategies(self) -> set[str]:
        """Return set of globally blocked strategies."""
        return set(self._blocked_strategies)


# ─── EX-04: Retry and Compensation ─────────────────────────────────────────────


@dataclass
class CompensationTask:
    """Task for compensating a failed or partial execution."""
    task_id: str
    original_order_id: str
    compensation_type: str  # "retry", "cancel", "reconcile"
    status: str = "pending"  # "pending", "executing", "completed", "failed"
    attempts: int = 0
    max_attempts: int = 3
    created_at: datetime = field(default_factory=utc_now)
    last_attempt_at: datetime | None = None
    error: str | None = None


class RetryController:
    """Manages retry logic and compensation tasks (EX-04)."""

    def __init__(
        self,
        max_retries: int = 3,
        base_delay_ms: float = 100.0,
        max_delay_ms: float = 5000.0,
    ) -> None:
        self.max_retries = max_retries
        self.base_delay_ms = base_delay_ms
        self.max_delay_ms = max_delay_ms
        self._compensation_queue: list[CompensationTask] = []
        self._retry_history: dict[str, list[datetime]] = defaultdict(list)

    def calculate_delay(self, attempt: int) -> float:
        """Calculate delay for a given retry attempt with exponential backoff."""
        delay = self.base_delay_ms * (2 ** attempt)
        return min(delay, self.max_delay_ms)

    def should_retry(self, order_id: str, attempt: int) -> bool:
        """Return True if the order should be retried."""
        if attempt >= self.max_retries:
            return False
        # Check if we've already retried this order recently
        history = self._retry_history.get(order_id, [])
        if history and (utc_now() - history[-1]) < timedelta(milliseconds=self.calculate_delay(attempt)):
            return False
        return True

    def record_attempt(self, order_id: str) -> None:
        """Record a retry attempt for an order."""
        self._retry_history[order_id].append(utc_now())

    def create_compensation(
        self,
        order_id: str,
        compensation_type: str,
        max_attempts: int = 3,
    ) -> CompensationTask:
        """Create a compensation task for a failed order."""
        task = CompensationTask(
            task_id=f"comp_{uuid4().hex[:12]}",
            original_order_id=order_id,
            compensation_type=compensation_type,
            max_attempts=max_attempts,
        )
        self._compensation_queue.append(task)
        return task

    def get_pending_compensations(self) -> list[CompensationTask]:
        """Get all pending compensation tasks."""
        return [t for t in self._compensation_queue if t.status == "pending"]

    def mark_compensation_executing(self, task_id: str) -> None:
        """Mark a compensation task as executing."""
        for task in self._compensation_queue:
            if task.task_id == task_id:
                task.status = "executing"
                task.attempts += 1
                task.last_attempt_at = utc_now()

    def mark_compensation_completed(self, task_id: str) -> None:
        """Mark a compensation task as completed."""
        for task in self._compensation_queue:
            if task.task_id == task_id:
                task.status = "completed"

    def mark_compensation_failed(self, task_id: str, error: str) -> None:
        """Mark a compensation task as failed."""
        for task in self._compensation_queue:
            if task.task_id == task_id:
                task.status = "failed"
                task.error = error


# ─── EX-07: Exception Handling and Rate Limiting ────────────────────────────────


@dataclass
class RateLimitRule:
    """Rate limit configuration for an execution channel."""
    channel_id: str
    max_requests_per_second: float
    max_requests_per_minute: float
    max_orders_per_second: float
    max_orders_per_day: float


class RateLimiter:
    """Enforces rate limits on execution channels (EX-07)."""

    def __init__(self) -> None:
        self._rules: dict[str, RateLimitRule] = {}
        self._request_counts: dict[str, list[datetime]] = defaultdict(list)
        self._order_counts: dict[str, list[datetime]] = defaultdict(list)

    def add_rule(self, rule: RateLimitRule) -> None:
        """Add or update a rate limit rule for a channel."""
        self._rules[rule.channel_id] = rule

    def remove_rule(self, channel_id: str) -> None:
        """Remove rate limit rule for a channel."""
        self._rules.pop(channel_id, None)

    def check_rate_limit(self, channel_id: str, is_order: bool = False) -> tuple[bool, str | None]:
        """Check if a request would exceed rate limits.

        Returns (allowed, reason_if_limited).
        """
        rule = self._rules.get(channel_id)
        if not rule:
            return True, None  # No limit configured

        now = utc_now()
        self._cleanup_old_counts(channel_id, now)

        if is_order:
            # Check orders per second
            recent_orders = [t for t in self._order_counts[channel_id] if now - t < timedelta(seconds=1)]
            if len(recent_orders) >= rule.max_orders_per_second:
                return False, f"Order rate limit exceeded: {rule.max_orders_per_second}/sec"

            # Check orders per day
            day_orders = [t for t in self._order_counts[channel_id] if now - t < timedelta(days=1)]
            if len(day_orders) >= rule.max_orders_per_day:
                return False, f"Daily order limit exceeded: {rule.max_orders_per_day}/day"
        else:
            # Check requests per second
            recent = [t for t in self._request_counts[channel_id] if now - t < timedelta(seconds=1)]
            if len(recent) >= rule.max_requests_per_second:
                return False, f"Request rate limit exceeded: {rule.max_requests_per_second}/sec"

            # Check requests per minute
            minute_requests = [t for t in self._request_counts[channel_id] if now - t < timedelta(minutes=1)]
            if len(minute_requests) >= rule.max_requests_per_minute:
                return False, f"Minute request limit exceeded: {rule.max_requests_per_minute}/min"

        return True, None

    def record_request(self, channel_id: str, is_order: bool = False) -> None:
        """Record a request for rate limiting."""
        now = utc_now()
        if is_order:
            self._order_counts[channel_id].append(now)
        else:
            self._request_counts[channel_id].append(now)

    def _cleanup_old_counts(self, channel_id: str, now: datetime) -> None:
        """Remove old count entries to prevent memory growth."""
        # Keep only last 24 hours of order counts
        self._order_counts[channel_id] = [
            t for t in self._order_counts[channel_id] if now - t < timedelta(days=1)
        ]
        # Keep only last 5 minutes of request counts
        self._request_counts[channel_id] = [
            t for t in self._request_counts[channel_id] if now - t < timedelta(minutes=5)
        ]


# ─── EX-08: Smart Order Router ─────────────────────────────────────────────────


class SmartOrderRouter:
    """Routes orders to optimal execution channels (EX-08)."""

    def __init__(
        self,
        channels: list[ExecutionChannel] | None = None,
        rate_limiter: RateLimiter | None = None,
    ) -> None:
        self._channels: dict[str, ExecutionChannel] = {}
        self._channel_order: list[str] = []  # Priority order of channels
        self._rate_limiter = rate_limiter or RateLimiter()

        if channels:
            for channel in channels:
                self.register_channel(channel)

    def register_channel(self, channel: ExecutionChannel, priority: int = 0) -> None:
        """Register an execution channel with a priority."""
        self._channels[channel.channel_id] = channel
        # Insert at priority position
        self._channel_order.insert(priority, channel.channel_id)

    def unregister_channel(self, channel_id: str) -> None:
        """Remove an execution channel."""
        self._channels.pop(channel_id, None)
        self._channel_order = [c for c in self._channel_order if c != channel_id]

    def get_channel(self, channel_id: str) -> ExecutionChannel | None:
        """Get a channel by ID."""
        return self._channels.get(channel_id)

    def get_connected_channels(self) -> list[ExecutionChannel]:
        """Get all connected channels."""
        return [c for c in self._channels.values() if c.is_connected()]

    def route_order(
        self,
        request: OrderRequest,
        preferred_channel_id: str | None = None,
    ) -> tuple[ExecutionChannel | None, dict]:
        """Route an order to the best available channel.

        Returns (channel, response) tuple. Channel is None if no channel available.
        """
        # Try preferred channel first if specified and available
        if preferred_channel_id:
            channel = self._channels.get(preferred_channel_id)
            if channel and channel.is_connected():
                # Check rate limit
                allowed, reason = self._rate_limiter.check_rate_limit(
                    channel.channel_id, is_order=True
                )
                if allowed:
                    response = channel.submit_order(request)
                    self._rate_limiter.record_request(channel.channel_id, is_order=True)
                    return channel, response
                # Rate limited, try next channel

        # Try channels in priority order
        for channel_id in self._channel_order:
            channel = self._channels.get(channel_id)
            if not channel or not channel.is_connected():
                continue

            # Check rate limit
            allowed, reason = self._rate_limiter.check_rate_limit(
                channel.channel_id, is_order=True
            )
            if not allowed:
                continue

            # Check if channel supports the order type
            if request.order_type not in channel.supported_order_types:
                continue

            response = channel.submit_order(request)
            self._rate_limiter.record_request(channel.channel_id, is_order=True)
            return channel, response

        return None, {"success": False, "error": "No available channel"}

    def route_cancel(self, venue_order_id: str, channel_id: str) -> dict:
        """Route a cancel request to a specific channel."""
        channel = self._channels.get(channel_id)
        if not channel or not channel.is_connected():
            return {"success": False, "error": "Channel not available"}

        allowed, reason = self._rate_limiter.check_rate_limit(channel.channel_id)
        if not allowed:
            return {"success": False, "error": reason}

        response = channel.cancel_order(venue_order_id)
        self._rate_limiter.record_request(channel.channel_id)
        return response

    def get_router_metrics(self) -> dict[str, Any]:
        """Get aggregated metrics from all channels."""
        total_requests = 0
        total_success = 0
        total_failed = 0
        total_rejected = 0

        for channel in self._channels.values():
            m = channel.get_metrics()
            total_requests += m.total_requests
            total_success += m.successful_requests
            total_failed += m.failed_requests
            total_rejected += m.rejected_requests

        return {
            "total_channels": len(self._channels),
            "connected_channels": len(self.get_connected_channels()),
            "total_requests": total_requests,
            "successful_requests": total_success,
            "failed_requests": total_failed,
            "rejected_requests": total_rejected,
        }


# ─────────────────────────────────────────────────────────────────────────────
# EX-08: Advanced Execution Algorithms — TWAP / VWAP / Iceberg
# ─────────────────────────────────────────────────────────────────────────────


class ExecutionAlgorithmType(str, Enum):
    """Types of execution algorithms supported by the EMS."""

    TWAP = "twap"
    VWAP = "vwap"
    ICEBERG = "iceberg"
    POV = "pov"  # Percentage of Volume


@dataclass
class ExecutionSlice:
    """One child slice of a parent algorithm order."""

    slice_id: str
    parent_order_id: str
    quantity: float
    limit_price: float | None
    venue: str
    status: OrderStatus = OrderStatus.CREATED
    child_order_ids: list[str] = field(default_factory=list)
    filled_quantity: float = 0.0
    avg_fill_price: float = 0.0
    created_at: datetime | None = None


@dataclass
class AlgorithmOrder:
    """Parent order that manages an execution algorithm across multiple slices."""

    algo_order_id: str
    instrument_id: str
    side: OrderSide
    total_quantity: float
    algo_type: ExecutionAlgorithmType
    params: dict
    status: OrderStatus = OrderStatus.CREATED
    slices: list[ExecutionSlice] = field(default_factory=list)
    filled_quantity: float = 0.0
    avg_fill_price: float = 0.0
    created_at: datetime | None = None
    updated_at: datetime | None = None
    completed_at: datetime | None = None


class TWAPExecutionAlgorithm:
    """Time-Weighted Average Price (TWAP) execution algorithm.

    Splits a parent order into equal-sized slices released at regular time intervals.
    Suitable for large orders where market impact is a concern.
    """

    def __init__(
        self,
        num_slices: int = 10,
        interval_seconds: int = 60,
        slice_timeout_seconds: int = 30,
    ) -> None:
        self.num_slices = num_slices
        self.interval_seconds = interval_seconds
        self.slice_timeout_seconds = slice_timeout_seconds

    def compute_slices(
        self,
        order_id: str,
        total_quantity: float,
        limit_price: float | None = None,
        venue: str = "primary",
    ) -> list[ExecutionSlice]:
        """Compute equal-sized TWAP slices for the given quantity."""
        slice_qty = total_quantity / self.num_slices
        slices = []
        for i in range(self.num_slices):
            slices.append(
                ExecutionSlice(
                    slice_id=f"{order_id}:twap:{i + 1}",
                    parent_order_id=order_id,
                    quantity=round(slice_qty, 6),
                    limit_price=limit_price,
                    venue=venue,
                    created_at=utc_now(),
                )
            )
        return slices


class VWAPExecutionAlgorithm:
    """Volume-Weighted Average Price (VWAP) execution algorithm.

    Schedules child orders proportional to expected volume分布, attempting to
    achieve an average execution price close to the day's VWAP.
    """

    def __init__(
        self,
        volume_profile: dict[int, float] | None = None,  # hour -> volume fraction
        num_slices: int = 20,
        slice_timeout_seconds: int = 30,
    ) -> None:
        # Default:均匀分布 over market hours (9:30–16:00 = 6.5 hours ≈ 390 minutes)
        self.volume_profile = volume_profile or {
            9: 0.05, 10: 0.12, 11: 0.10, 12: 0.06, 13: 0.08, 14: 0.14, 15: 0.15, 16: 0.10
        }
        self.num_slices = num_slices
        self.slice_timeout_seconds = slice_timeout_seconds

    def compute_slices(
        self,
        order_id: str,
        total_quantity: float,
        limit_price: float | None = None,
        venue: str = "primary",
    ) -> list[ExecutionSlice]:
        """Compute volume-weighted VWAP slices."""
        total_volume_weight = sum(self.volume_profile.values())
        slices = []
        slice_qty_remaining = total_quantity

        sorted_hours = sorted(self.volume_profile.keys())
        for idx, hour in enumerate(sorted_hours):
            weight = self.volume_profile[hour] / total_volume_weight
            qty = round(total_quantity * weight, 6)
            # Last slice gets remainder to handle rounding
            if idx == len(sorted_hours) - 1:
                qty = round(slice_qty_remaining, 6)
            qty = max(qty, 0.0)
            slice_qty_remaining -= qty

            slices.append(
                ExecutionSlice(
                    slice_id=f"{order_id}:vwap:{hour}",
                    parent_order_id=order_id,
                    quantity=qty,
                    limit_price=limit_price,
                    venue=venue,
                    created_at=utc_now(),
                )
            )
        return slices


class POVExecutionAlgorithm:
    """Percentage of Volume (POV) execution algorithm.

    Maintains a constant participation rate relative to market volume,
    pausing when volume is low and resuming when volume increases.
    """

    def __init__(
        self,
        participation_rate: float = 0.10,  # 10% of volume
        min_slice_quantity: float = 100.0,
        max_slice_quantity: float = 10_000.0,
        slice_interval_seconds: int = 30,
    ) -> None:
        self.participation_rate = max(0.01, min(0.50, participation_rate))
        self.min_slice_quantity = min_slice_quantity
        self.max_slice_quantity = max_slice_quantity
        self.slice_interval_seconds = slice_interval_seconds

    def compute_slice_quantity(
        self,
        parent_order_id: str,
        current_volume: float,
        remaining_quantity: float,
        limit_price: float | None = None,
    ) -> float:
        """Compute one POV slice based on current volume."""
        target_qty = current_volume * self.participation_rate
        target_qty = max(self.min_slice_quantity, min(target_qty, self.max_slice_quantity))
        target_qty = min(target_qty, remaining_quantity)
        return round(target_qty, 6)


class IcebergOrderHandler:
    """Iceberg (hidden/large) order handler.

    Displays only the visible slice to the market, revealing more only
    as the current slice gets filled.
    """

    def __init__(
        self,
        visible_ratio: float = 0.05,  # 5% visible by default
        min_visible: float = 100.0,
        max_visible: float = 50_000.0,
    ) -> None:
        self.visible_ratio = max(0.01, min(0.50, visible_ratio))
        self.min_visible = min_visible
        self.max_visible = max_visible

    def compute_visible_quantity(self, total_quantity: float, remaining_quantity: float) -> float:
        """Compute the visible (displayed) quantity for the next slice."""
        visible = remaining_quantity * self.visible_ratio
        visible = max(self.min_visible, min(visible, self.max_visible))
        visible = min(visible, remaining_quantity)
        return round(visible, 6)

    def create_iceberg_slice(
        self,
        order_id: str,
        total_quantity: float,
        remaining_quantity: float,
        limit_price: float | None,
        venue: str = "primary",
    ) -> ExecutionSlice:
        """Create the next iceberg slice with computed visible quantity."""
        visible_qty = self.compute_visible_quantity(total_quantity, remaining_quantity)
        return ExecutionSlice(
            slice_id=f"{order_id}:iceberg:{uuid4().hex[:8]}",
            parent_order_id=order_id,
            quantity=visible_qty,
            limit_price=limit_price,
            venue=venue,
            created_at=utc_now(),
        )


class ExecutionAlgorithmService:
    """Service that manages execution algorithms and tracks parent/child orders (EX-08).

    Coordinates TWAP, VWAP, POV, and Iceberg algorithms across the OMS.
    Each parent algorithm order spawns child slices routed through the SmartOrderRouter.
    """

    def __init__(self, sor: SmartOrderRouter | None = None) -> None:
        self.sor = sor or SmartOrderRouter()
        self._algorithms: dict[str, AlgorithmOrder] = {}
        self._twap = TWAPExecutionAlgorithm()
        self._vwap = VWAPExecutionAlgorithm()
        self._pov = POVExecutionAlgorithm()
        self._iceberg = IcebergOrderHandler()

    def submit_algorithm_order(
        self,
        instrument_id: str,
        side: OrderSide,
        quantity: float,
        algo_type: ExecutionAlgorithmType,
        limit_price: float | None = None,
        params: dict | None = None,
        venue: str = "primary",
    ) -> AlgorithmOrder:
        """Submit a parent order with an execution algorithm (TWAP/VWAP/ICEBERG/POV)."""
        params = params or {}
        order_id = f"algo:{uuid4().hex[:12]}"

        if algo_type == ExecutionAlgorithmType.TWAP:
            twap_slices = self._twap.compute_slices(order_id, quantity, limit_price, venue)
            slices = twap_slices
        elif algo_type == ExecutionAlgorithmType.VWAP:
            vwap_slices = self._vwap.compute_slices(order_id, quantity, limit_price, venue)
            slices = vwap_slices
        elif algo_type == ExecutionAlgorithmType.POV:
            # POV creates one initial slice; more computed on the fly
            initial_qty = self._pov.compute_slice_quantity(
                order_id, params.get("current_volume", 100_000.0), quantity, limit_price
            )
            slices = [
                ExecutionSlice(
                    slice_id=f"{order_id}:pov:1",
                    parent_order_id=order_id,
                    quantity=initial_qty,
                    limit_price=limit_price,
                    venue=venue,
                    created_at=utc_now(),
                )
            ]
        elif algo_type == ExecutionAlgorithmType.ICEBERG:
            iceberg_slice = self._iceberg.create_iceberg_slice(order_id, quantity, quantity, limit_price, venue)
            slices = [iceberg_slice]
        else:
            # Fallback to single slice
            slices = [
                ExecutionSlice(
                    slice_id=f"{order_id}:single",
                    parent_order_id=order_id,
                    quantity=quantity,
                    limit_price=limit_price,
                    venue=venue,
                    created_at=utc_now(),
                )
            ]

        algo_order = AlgorithmOrder(
            algo_order_id=order_id,
            instrument_id=instrument_id,
            side=side,
            total_quantity=quantity,
            algo_type=algo_type,
            params=params,
            status=OrderStatus.ACCEPTED,
            slices=slices,
            created_at=utc_now(),
            updated_at=utc_now(),
        )
        self._algorithms[order_id] = algo_order
        return algo_order

    def get_algorithm_order(self, order_id: str) -> AlgorithmOrder | None:
        """Get an algorithm order by ID."""
        return self._algorithms.get(order_id)

    def get_algorithm_metrics(self, order_id: str) -> dict[str, Any]:
        """Get fill metrics for an algorithm order."""
        algo = self._algorithms.get(order_id)
        if not algo:
            return {"error": "Order not found"}

        total_filled = sum(s.filled_quantity for s in algo.slices)
        total_value = sum(s.filled_quantity * s.avg_fill_price for s in algo.slices)
        avg_price = total_value / total_filled if total_filled > 0 else 0.0

        return {
            "algo_order_id": order_id,
            "algo_type": algo.algo_type.value,
            "status": algo.status.value,
            "total_quantity": algo.total_quantity,
            "filled_quantity": total_filled,
            "remaining_quantity": algo.total_quantity - total_filled,
            "avg_fill_price": round(avg_price, 4),
            "slice_count": len(algo.slices),
            "filled_slices": sum(1 for s in algo.slices if s.status == OrderStatus.FILLED),
            "completion_pct": round(total_filled / algo.total_quantity * 100, 2) if algo.total_quantity > 0 else 0,
        }

    def list_algorithm_orders(self, status: OrderStatus | None = None) -> list[dict]:
        """List all algorithm orders, optionally filtered by status."""
        result = []
        for algo in self._algorithms.values():
            if status is None or algo.status == status:
                result.append({
                    "algo_order_id": algo.algo_order_id,
                    "instrument_id": algo.instrument_id,
                    "side": algo.side.value,
                    "algo_type": algo.algo_type.value,
                    "total_quantity": algo.total_quantity,
                    "filled_quantity": sum(s.filled_quantity for s in algo.slices),
                    "status": algo.status.value,
                    "created_at": algo.created_at.isoformat() if algo.created_at else None,
                })
        return result
