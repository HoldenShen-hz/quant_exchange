"""In-memory order management and paper execution helpers.

Implements the documented OMS contract:
- Idempotent order submission (unique client_order_id)
- Full state machine with transition validation
- Order status history for audit trail
- Recoverable non-terminal order queries
"""

from __future__ import annotations

from typing import Any
from uuid import uuid4

from quant_exchange.core.models import Fill, Kline, Order, OrderRequest, OrderSide, OrderStatus, OrderType, utc_now

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
