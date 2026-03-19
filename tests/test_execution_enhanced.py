"""Tests for enhanced execution features (EX-01 ~ EX-08).

Tests:
- EX-01: Multi-channel adapter abstraction
- EX-04: Retry and compensation
- EX-06: Permission control
- EX-07: Rate limiting
- EX-08: Smart order routing
"""

from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from quant_exchange.core.models import (
    OrderRequest,
    OrderSide,
    OrderStatus,
    OrderType,
    PortfolioSnapshot,
)
from quant_exchange.execution import (
    CompensationTask,
    ExecutionChannel,
    ExecutionChannelState,
    OrderManager,
    PaperExecutionEngine,
    PermissionController,
    RateLimiter,
    RateLimitRule,
    RetryController,
    SimulatedExecutionChannel,
    SmartOrderRouter,
    TradingPermission,
)


def utc_now() -> datetime:
    """Return a timezone-aware UTC timestamp."""
    return datetime.now(timezone.utc)


# ─── EX-01: Multi-Channel Adapter ─────────────────────────────────────────────


class SimulatedExecutionChannelTests(unittest.TestCase):
    """Test EX-01: Multi-channel adapter abstraction."""

    def setUp(self) -> None:
        self.channel = SimulatedExecutionChannel(
            channel_id="test_exchange",
            channel_name="Test Exchange",
        )

    def test_channel_connect_disconnect(self) -> None:
        """Verify channel connect/disconnect."""
        self.assertFalse(self.channel.is_connected())

        self.assertTrue(self.channel.connect())
        self.assertTrue(self.channel.is_connected())

        self.assertTrue(self.channel.disconnect())
        self.assertFalse(self.channel.is_connected())

    def test_submit_order_success(self) -> None:
        """Verify order submission through channel."""
        self.channel.connect()

        request = OrderRequest(
            client_order_id="test_001",
            instrument_id="BTCUSDT",
            side=OrderSide.BUY,
            quantity=1.0,
            price=50000.0,
            order_type=OrderType.LIMIT,
        )

        response = self.channel.submit_order(request)

        self.assertTrue(response["success"])
        self.assertEqual(response["client_order_id"], "test_001")
        self.assertIn("exchange_order_id", response)

    def test_submit_order_disconnected(self) -> None:
        """Verify order fails when channel is disconnected."""
        self.channel.disconnect()

        request = OrderRequest(
            client_order_id="test_001",
            instrument_id="BTCUSDT",
            side=OrderSide.BUY,
            quantity=1.0,
            price=50000.0,
        )

        response = self.channel.submit_order(request)

        self.assertFalse(response["success"])
        self.assertIn("disconnected", response["error"].lower())

    def test_submit_order_unsupported_type(self) -> None:
        """Verify unsupported order type is rejected."""
        channel = SimulatedExecutionChannel(
            channel_id="test",
            supported_types={OrderType.MARKET},  # Only market orders
        )
        channel.connect()

        request = OrderRequest(
            client_order_id="test_001",
            instrument_id="BTCUSDT",
            side=OrderSide.BUY,
            quantity=1.0,
            price=50000.0,
            order_type=OrderType.LIMIT,  # Not supported
        )

        response = channel.submit_order(request)

        self.assertFalse(response["success"])
        self.assertIn("not supported", response["error"].lower())

    def test_cancel_order(self) -> None:
        """Verify order cancellation."""
        self.channel.connect()

        response = self.channel.cancel_order("test_exchange_ord_123")

        self.assertTrue(response["success"])
        self.assertEqual(response["status"], "CANCELLED")

    def test_channel_metrics(self) -> None:
        """Verify channel metrics tracking."""
        self.channel.connect()
        self.channel.submit_order(OrderRequest(
            client_order_id="test",
            instrument_id="BTCUSDT",
            side=OrderSide.BUY,
            quantity=1.0,
            price=50000.0,
        ))

        metrics = self.channel.get_metrics()

        self.assertEqual(metrics.total_requests, 1)
        self.assertEqual(metrics.successful_requests, 1)


# ─── EX-04: Retry and Compensation ─────────────────────────────────────────────


class RetryControllerTests(unittest.TestCase):
    """Test EX-04: Retry mechanism and compensation tasks."""

    def setUp(self) -> None:
        self.retry = RetryController(max_retries=3, base_delay_ms=100.0)

    def test_calculate_delay_exponential_backoff(self) -> None:
        """Verify exponential backoff calculation."""
        delay0 = self.retry.calculate_delay(0)
        delay1 = self.retry.calculate_delay(1)
        delay2 = self.retry.calculate_delay(2)

        self.assertEqual(delay0, 100.0)  # 100 * 2^0
        self.assertEqual(delay1, 200.0)  # 100 * 2^1
        self.assertEqual(delay2, 400.0)  # 100 * 2^2

    def test_delay_caps_at_max(self) -> None:
        """Verify delay is capped at max_delay_ms."""
        delay = self.retry.calculate_delay(10)  # Would be 100 * 2^10 = 102400
        self.assertEqual(delay, 5000.0)  # Capped at max_delay_ms

    def test_should_retry_within_limit(self) -> None:
        """Verify should_retry respects max_retries."""
        self.assertTrue(self.retry.should_retry("order_1", 0))
        self.assertTrue(self.retry.should_retry("order_1", 1))
        self.assertTrue(self.retry.should_retry("order_1", 2))
        self.assertFalse(self.retry.should_retry("order_1", 3))

    def test_record_attempt(self) -> None:
        """Verify retry attempts are recorded."""
        self.retry.record_attempt("order_1")
        self.retry.record_attempt("order_1")

        history = self.retry._retry_history.get("order_1", [])
        self.assertEqual(len(history), 2)

    def test_create_compensation(self) -> None:
        """Verify compensation task creation."""
        task = self.retry.create_compensation("order_123", "retry")

        self.assertIsNotNone(task.task_id)
        self.assertEqual(task.original_order_id, "order_123")
        self.assertEqual(task.compensation_type, "retry")
        self.assertEqual(task.status, "pending")

    def test_compensation_workflow(self) -> None:
        """Verify compensation task state transitions."""
        task = self.retry.create_compensation("order_123", "retry")

        self.retry.mark_compensation_executing(task.task_id)
        self.assertEqual(task.status, "executing")
        self.assertEqual(task.attempts, 1)

        self.retry.mark_compensation_completed(task.task_id)
        self.assertEqual(task.status, "completed")

    def test_get_pending_compensations(self) -> None:
        """Verify pending compensations retrieval."""
        self.retry.create_compensation("order_1", "retry")
        self.retry.create_compensation("order_2", "cancel")

        pending = self.retry.get_pending_compensations()
        self.assertEqual(len(pending), 2)


# ─── EX-06: Permission Control ─────────────────────────────────────────────────


class PermissionControllerTests(unittest.TestCase):
    """Test EX-06: Account/strategy/instrument permission control."""

    def setUp(self) -> None:
        self.controller = PermissionController()

    def test_set_and_get_permission(self) -> None:
        """Verify permission can be set and retrieved."""
        permission = TradingPermission(
            account_id="acc_001",
            strategy_ids={"strat_a", "strat_b"},
            instrument_ids={"BTCUSDT", "ETHUSDT"},
        )

        self.controller.set_permission(permission)

        retrieved = self.controller.get_permission("acc_001")
        self.assertIsNotNone(retrieved)
        self.assertEqual(retrieved.account_id, "acc_001")
        self.assertEqual(len(retrieved.strategy_ids), 2)

    def test_revoke_permission(self) -> None:
        """Verify permission can be revoked."""
        permission = TradingPermission(account_id="acc_001")
        self.controller.set_permission(permission)

        self.controller.revoke_permission("acc_001")

        self.assertIsNone(self.controller.get_permission("acc_001"))

    def test_block_instrument(self) -> None:
        """Verify instrument can be globally blocked."""
        self.controller.block_instrument("DOGEUSDT")

        allowed, reason = self.controller.check_permission(
            account_id="acc_001",
            strategy_id=None,
            instrument_id="DOGEUSDT",
            side=OrderSide.BUY,
            order_value=1000.0,
        )

        self.assertFalse(allowed)
        self.assertIn("blocked", reason.lower())

    def test_unblock_instrument(self) -> None:
        """Verify instrument can be unblocked."""
        # Set up permission first
        permission = TradingPermission(account_id="acc_001")
        self.controller.set_permission(permission)

        self.controller.block_instrument("DOGEUSDT")
        self.controller.unblock_instrument("DOGEUSDT")

        allowed, _ = self.controller.check_permission(
            account_id="acc_001",
            strategy_id=None,
            instrument_id="DOGEUSDT",
            side=OrderSide.BUY,
            order_value=1000.0,
        )

        # Should not be blocked anymore
        self.assertTrue(allowed)

    def test_check_strategy_permission(self) -> None:
        """Verify strategy-specific permission checking."""
        permission = TradingPermission(
            account_id="acc_001",
            strategy_ids={"strat_a"},  # Only strat_a allowed
        )
        self.controller.set_permission(permission)

        # strat_a should be allowed
        allowed, _ = self.controller.check_permission(
            account_id="acc_001",
            strategy_id="strat_a",
            instrument_id="BTCUSDT",
            side=OrderSide.BUY,
            order_value=1000.0,
        )
        self.assertTrue(allowed)

        # strat_b should be denied
        allowed, reason = self.controller.check_permission(
            account_id="acc_001",
            strategy_id="strat_b",
            instrument_id="BTCUSDT",
            side=OrderSide.BUY,
            order_value=1000.0,
        )
        self.assertFalse(allowed)
        self.assertIn("not allowed", reason.lower())

    def test_check_instrument_permission(self) -> None:
        """Verify instrument-specific permission checking."""
        permission = TradingPermission(
            account_id="acc_001",
            instrument_ids={"BTCUSDT"},  # Only BTCUSDT allowed
        )
        self.controller.set_permission(permission)

        # BTCUSDT should be allowed
        allowed, _ = self.controller.check_permission(
            account_id="acc_001",
            strategy_id=None,
            instrument_id="BTCUSDT",
            side=OrderSide.BUY,
            order_value=1000.0,
        )
        self.assertTrue(allowed)

        # ETHUSDT should be denied
        allowed, reason = self.controller.check_permission(
            account_id="acc_001",
            strategy_id=None,
            instrument_id="ETHUSDT",
            side=OrderSide.BUY,
            order_value=1000.0,
        )
        self.assertFalse(allowed)
        self.assertIn("not allowed", reason.lower())

    def test_check_side_permission(self) -> None:
        """Verify side-specific permission checking."""
        permission = TradingPermission(
            account_id="acc_001",
            allowed_sides={OrderSide.BUY},  # Only buy allowed
        )
        self.controller.set_permission(permission)

        # BUY should be allowed
        allowed, _ = self.controller.check_permission(
            account_id="acc_001",
            strategy_id=None,
            instrument_id="BTCUSDT",
            side=OrderSide.BUY,
            order_value=1000.0,
        )
        self.assertTrue(allowed)

        # SELL should be denied
        allowed, reason = self.controller.check_permission(
            account_id="acc_001",
            strategy_id=None,
            instrument_id="BTCUSDT",
            side=OrderSide.SELL,
            order_value=1000.0,
        )
        self.assertFalse(allowed)
        self.assertIn("not allowed", reason.lower())

    def test_check_order_value_limit(self) -> None:
        """Verify order value limit checking."""
        permission = TradingPermission(
            account_id="acc_001",
            max_order_value=10000.0,
        )
        self.controller.set_permission(permission)

        # Within limit
        allowed, _ = self.controller.check_permission(
            account_id="acc_001",
            strategy_id=None,
            instrument_id="BTCUSDT",
            side=OrderSide.BUY,
            order_value=5000.0,
        )
        self.assertTrue(allowed)

        # Exceeds limit
        allowed, reason = self.controller.check_permission(
            account_id="acc_001",
            strategy_id=None,
            instrument_id="BTCUSDT",
            side=OrderSide.BUY,
            order_value=15000.0,
        )
        self.assertFalse(allowed)
        self.assertIn("exceeds limit", reason.lower())


# ─── EX-07: Rate Limiting ──────────────────────────────────────────────────────


class RateLimiterTests(unittest.TestCase):
    """Test EX-07: Rate limiting for execution channels."""

    def setUp(self) -> None:
        self.limiter = RateLimiter()
        self.rule = RateLimitRule(
            channel_id="test_channel",
            max_requests_per_second=10.0,
            max_requests_per_minute=100.0,
            max_orders_per_second=5.0,
            max_orders_per_day=10000.0,
        )
        self.limiter.add_rule(self.rule)

    def test_add_and_remove_rule(self) -> None:
        """Verify rate limit rules can be added and removed."""
        self.limiter.remove_rule("test_channel")

        allowed, _ = self.limiter.check_rate_limit("test_channel")
        self.assertTrue(allowed)  # No limit when rule removed

    def test_check_rate_limit_allows_within_limit(self) -> None:
        """Verify requests within limit are allowed."""
        allowed, _ = self.limiter.check_rate_limit("test_channel")
        self.assertTrue(allowed)

    def test_check_rate_limit_blocks_over_limit(self) -> None:
        """Verify requests over limit are blocked."""
        # Exhaust the per-second limit
        for _ in range(10):
            self.limiter.record_request("test_channel")
            self.limiter.check_rate_limit("test_channel")

        # Next request should be blocked
        allowed, reason = self.limiter.check_rate_limit("test_channel")
        self.assertFalse(allowed)
        self.assertIn("rate limit exceeded", reason.lower())

    def test_order_rate_limit(self) -> None:
        """Verify order-specific rate limiting."""
        # Exhaust order per-second limit
        for _ in range(5):
            self.limiter.record_request("test_channel", is_order=True)

        allowed, reason = self.limiter.check_rate_limit("test_channel", is_order=True)
        self.assertFalse(allowed)
        self.assertIn("order rate limit exceeded", reason.lower())


# ─── EX-08: Smart Order Router ────────────────────────────────────────────────


class SmartOrderRouterTests(unittest.TestCase):
    """Test EX-08: Smart order routing to multiple channels."""

    def setUp(self) -> None:
        self.channel1 = SimulatedExecutionChannel(
            channel_id="channel_1",
            channel_name="Primary Exchange",
        )
        self.channel2 = SimulatedExecutionChannel(
            channel_id="channel_2",
            channel_name="Backup Exchange",
        )
        self.router = SmartOrderRouter(channels=[self.channel1, self.channel2])

    def test_register_and_unregister_channel(self) -> None:
        """Verify channel registration and removal."""
        channel3 = SimulatedExecutionChannel(channel_id="channel_3")
        self.router.register_channel(channel3)

        self.assertIsNotNone(self.router.get_channel("channel_3"))

        self.router.unregister_channel("channel_3")
        self.assertIsNone(self.router.get_channel("channel_3"))

    def test_get_connected_channels(self) -> None:
        """Verify connected channels filtering."""
        self.channel1.connect()
        self.channel2.connect()

        connected = self.router.get_connected_channels()
        self.assertEqual(len(connected), 2)

        self.channel2.disconnect()
        connected = self.router.get_connected_channels()
        self.assertEqual(len(connected), 1)

    def test_route_order_to_available_channel(self) -> None:
        """Verify order is routed to connected channel."""
        self.channel1.connect()
        self.channel2.connect()

        request = OrderRequest(
            client_order_id="test_001",
            instrument_id="BTCUSDT",
            side=OrderSide.BUY,
            quantity=1.0,
            price=50000.0,
            order_type=OrderType.LIMIT,
        )

        channel, response = self.router.route_order(request)

        self.assertIsNotNone(channel)
        self.assertTrue(response["success"])

    def test_route_order_prefers_specified_channel(self) -> None:
        """Verify preferred channel is tried first."""
        self.channel1.connect()
        self.channel2.connect()

        request = OrderRequest(
            client_order_id="test_001",
            instrument_id="BTCUSDT",
            side=OrderSide.BUY,
            quantity=1.0,
            price=50000.0,
        )

        channel, response = self.router.route_order(request, preferred_channel_id="channel_2")

        self.assertIsNotNone(channel)
        self.assertEqual(channel.channel_id, "channel_2")

    def test_route_order_skips_disconnected_channel(self) -> None:
        """Verify disconnected channels are skipped."""
        self.channel1.connect()
        self.channel2.connect()
        self.channel2.disconnect()

        request = OrderRequest(
            client_order_id="test_001",
            instrument_id="BTCUSDT",
            side=OrderSide.BUY,
            quantity=1.0,
            price=50000.0,
        )

        channel, response = self.router.route_order(request, preferred_channel_id="channel_2")

        # Should have routed to channel_1 instead
        self.assertIsNotNone(channel)
        self.assertEqual(channel.channel_id, "channel_1")

    def test_route_order_no_available_channel(self) -> None:
        """Verify error response when no channel available."""
        self.channel1.disconnect()
        self.channel2.disconnect()

        request = OrderRequest(
            client_order_id="test_001",
            instrument_id="BTCUSDT",
            side=OrderSide.BUY,
            quantity=1.0,
            price=50000.0,
        )

        channel, response = self.router.route_order(request)

        self.assertIsNone(channel)
        self.assertFalse(response["success"])
        self.assertIn("No available channel", response["error"])

    def test_route_cancel(self) -> None:
        """Verify cancel routing to specific channel."""
        self.channel1.connect()

        response = self.router.route_cancel("venue_order_123", "channel_1")

        self.assertTrue(response["success"])

    def test_router_metrics(self) -> None:
        """Verify router aggregates channel metrics."""
        self.channel1.connect()
        self.channel2.connect()

        self.channel1.submit_order(OrderRequest(
            client_order_id="test",
            instrument_id="BTCUSDT",
            side=OrderSide.BUY,
            quantity=1.0,
            price=50000.0,
        ))

        metrics = self.router.get_router_metrics()

        self.assertEqual(metrics["total_channels"], 2)
        self.assertEqual(metrics["connected_channels"], 2)
        self.assertEqual(metrics["successful_requests"], 1)


if __name__ == "__main__":
    unittest.main()
