"""Tests for exchange REST and WebSocket adapters with rate limiting and reconnection."""

from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from quant_exchange.adapters.exchange import (
    BinanceRESTAdapter,
    BinanceWebSocketAdapter,
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitState,
    ConnectionState,
    ExchangeCredentials,
    RateLimitConfig,
    RateLimitState,
    RESTAPIError,
    SubscriptionManager,
    WebSocketSubscription,
)


class RateLimitStateTests(unittest.TestCase):
    """Test rate limit tracking."""

    def test_can_request_under_limit(self) -> None:
        """Verify request is allowed under rate limit."""
        state = RateLimitState()
        config = RateLimitConfig(requests_per_second=10.0)
        self.assertTrue(state.can_request(config))

    def test_rate_limit_enforced(self) -> None:
        """Verify rate limit is enforced after exceeding."""
        state = RateLimitState()
        config = RateLimitConfig(requests_per_second=2.0, requests_per_minute=10.0, requests_per_hour=100.0)

        for _ in range(3):
            state.record_request(config)

        self.assertFalse(state.second_window < 2.0)


class CircuitBreakerTests(unittest.TestCase):
    """Test circuit breaker functionality."""

    def test_circuit_starts_closed(self) -> None:
        """Verify circuit starts in closed state."""
        cb = CircuitBreaker()
        self.assertEqual(cb.state, CircuitState.CLOSED)

    def test_circuit_opens_after_failures(self) -> None:
        """Verify circuit opens after threshold failures."""
        cb = CircuitBreaker(config=CircuitBreakerConfig(failure_threshold=3))
        cb.record_failure()
        cb.record_failure()
        self.assertEqual(cb.state, CircuitState.CLOSED)
        cb.record_failure()
        self.assertEqual(cb.state, CircuitState.OPEN)

    def test_circuit_half_open_after_timeout(self) -> None:
        """Verify circuit transitions to half-open after timeout."""
        cb = CircuitBreaker(config=CircuitBreakerConfig(failure_threshold=1, timeout_seconds=0.01))
        cb.record_failure()
        self.assertEqual(cb.state, CircuitState.OPEN)

        import time
        time.sleep(0.02)

        self.assertTrue(cb.can_execute())
        self.assertEqual(cb.state, CircuitState.HALF_OPEN)

    def test_circuit_closes_after_successes(self) -> None:
        """Verify circuit closes after successful calls in half-open."""
        cb = CircuitBreaker(config=CircuitBreakerConfig(failure_threshold=1, success_threshold=2, timeout_seconds=0.01))
        cb.record_failure()
        import time
        time.sleep(0.02)

        cb.can_execute()
        cb.record_success()
        cb.record_success()
        self.assertEqual(cb.state, CircuitState.CLOSED)


class ExchangeCredentialsTests(unittest.TestCase):
    """Test exchange credentials."""

    def test_credentials_creation(self) -> None:
        """Verify credentials are stored correctly."""
        creds = ExchangeCredentials(api_key="test_key", secret_key="test_secret", passphrase="test_pass", testnet=True)
        self.assertEqual(creds.api_key, "test_key")
        self.assertEqual(creds.secret_key, "test_secret")
        self.assertEqual(creds.passphrase, "test_pass")
        self.assertTrue(creds.testnet)


class WebSocketSubscriptionTests(unittest.TestCase):
    """Test WebSocket subscription management."""

    def test_subscription_creation(self) -> None:
        """Verify subscription is created correctly."""
        sub = WebSocketSubscription(
            subscription_id="BTCUSDT:kline:1m",
            instrument_id="BTCUSDT",
            data_type="kline",
            timeframe="1m",
        )
        self.assertEqual(sub.instrument_id, "BTCUSDT")
        self.assertEqual(sub.data_type, "kline")
        self.assertTrue(sub.is_active)

    def test_subscription_has_default_callback(self) -> None:
        """Verify subscription defaults."""
        sub = WebSocketSubscription(
            subscription_id="test",
            instrument_id="BTCUSDT",
            data_type="tick",
        )
        self.assertIsNone(sub.callback)
        self.assertIsNotNone(sub.created_at)


class SubscriptionManagerTests(unittest.TestCase):
    """Test subscription manager."""

    def setUp(self) -> None:
        self.manager = SubscriptionManager()

    def test_register_adapter(self) -> None:
        """Verify adapter registration."""
        adapter = BinanceWebSocketAdapter()
        self.manager.register_adapter("BINANCE", adapter)
        self.assertIn("BINANCE", self.manager._adapters)

    def test_get_subscriptions_empty(self) -> None:
        """Verify empty subscriptions list."""
        subs = self.manager.get_subscriptions("BINANCE")
        self.assertEqual(subs, [])


class BinanceAdapterTests(unittest.TestCase):
    """Test Binance adapter functionality."""

    def test_binance_rest_adapter_creation(self) -> None:
        """Verify Binance REST adapter creation."""
        adapter = BinanceRESTAdapter()
        self.assertEqual(adapter.exchange_code, "BINANCE")
        self.assertIn("binance", adapter.base_url)

    def test_binance_rest_adapter_testnet(self) -> None:
        """Verify Binance testnet configuration."""
        adapter = BinanceRESTAdapter(testnet=True)
        self.assertIn("testnet", adapter.base_url)

    def test_binance_adapter_with_credentials(self) -> None:
        """Verify adapter stores credentials."""
        creds = ExchangeCredentials(api_key="key", secret_key="secret")
        adapter = BinanceRESTAdapter(credentials=creds)
        self.assertEqual(adapter.credentials.api_key, "key")


if __name__ == "__main__":
    unittest.main()
