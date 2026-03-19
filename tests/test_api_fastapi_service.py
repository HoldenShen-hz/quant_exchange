"""Tests for API service layer with auth middleware, versioning, and rate limiting."""

from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from quant_exchange.api.fastapi_service import (
    APIErrorCode,
    APIResponseFormatter,
    APIRouter,
    APIVersion,
    AuthenticatedRequest,
    ConfigHotReloader,
    DistributedScheduler,
    HealthCheckService,
    IdempotencyChecker,
    InMemorySessionStore,
    JWTAuthenticator,
    MetricsCollector,
    RateLimitConfig,
    RateLimitState,
    RequestValidator,
)


class RateLimitStateTests(unittest.TestCase):
    """Test rate limit state tracking."""

    def test_can_request_initially(self) -> None:
        """Verify request is allowed initially."""
        state = RateLimitState(client_id="test_client")
        config = RateLimitConfig(requests_per_second=100.0, requests_per_minute=1000.0)
        self.assertTrue(state.can_request(config))

    def test_rate_limit_enforced(self) -> None:
        """Verify rate limit blocks excessive requests."""
        state = RateLimitState(client_id="test_client")
        config = RateLimitConfig(requests_per_second=2.0, requests_per_minute=10.0)

        for _ in range(5):
            state.record_request()

        self.assertFalse(state.can_request(config))


class InMemorySessionStoreTests(unittest.TestCase):
    """Test in-memory session store."""

    def test_create_and_get_session(self) -> None:
        """Verify session creation and retrieval."""
        store = InMemorySessionStore(ttl_seconds=3600)
        token, expires_at = store.create_session("user1", "testuser", ["admin"])

        session = store.get_session(token)
        self.assertIsNotNone(session)
        self.assertEqual(session["user_id"], "user1")
        self.assertEqual(session["username"], "testuser")
        self.assertEqual(session["roles"], ["admin"])

    def test_expired_session_returns_none(self) -> None:
        """Verify expired sessions are rejected."""
        store = InMemorySessionStore(ttl_seconds=0)
        token, _ = store.create_session("user1", "testuser", [])

        import time
        time.sleep(0.1)

        session = store.get_session(token)
        self.assertIsNone(session)

    def test_delete_session(self) -> None:
        """Verify session deletion."""
        store = InMemorySessionStore()
        token, _ = store.create_session("user1", "testuser", [])

        result = store.delete_session(token)
        self.assertTrue(result)
        self.assertIsNone(store.get_session(token))


class JWTAuthenticatorTests(unittest.TestCase):
    """Test JWT authenticator."""

    def setUp(self) -> None:
        self.auth = JWTAuthenticator(secret_key="test_secret_key_12345")

    def test_create_and_verify_token(self) -> None:
        """Verify token creation and verification."""
        payload = {"user_id": "user1", "username": "testuser", "roles": ["admin"]}
        token = self.auth.create_token(payload)

        verified = self.auth.verify_token(token)
        self.assertIsNotNone(verified)
        self.assertEqual(verified["user_id"], "user1")
        self.assertEqual(verified["username"], "testuser")

    def test_invalid_token_returns_none(self) -> None:
        """Verify invalid tokens are rejected."""
        verified = self.auth.verify_token("invalid_token")
        self.assertIsNone(verified)


class APIRouterTests(unittest.TestCase):
    """Test API router."""

    def test_router_creation(self) -> None:
        """Verify router is created with version."""
        router = APIRouter(version=APIVersion.V1)
        self.assertEqual(router._version, APIVersion.V1)

    def test_route_decorator(self) -> None:
        """Verify route decorator registers endpoint."""
        router = APIRouter(version=APIVersion.V1)

        @router.route("/test", method="GET")
        def test_handler():
            return {"result": "ok"}

        endpoints = router.get_endpoints()
        self.assertEqual(len(endpoints), 1)
        self.assertEqual(endpoints[0].path, "/test")
        self.assertEqual(endpoints[0].method, "GET")


class APIResponseFormatterTests(unittest.TestCase):
    """Test API response formatting."""

    def test_ok_response(self) -> None:
        """Verify OK response format."""
        response = APIResponseFormatter.ok({"key": "value"})
        self.assertEqual(response["code"], "OK")
        self.assertEqual(response["data"], {"key": "value"})
        self.assertIn("request_id", response)
        self.assertIn("timestamp", response)

    def test_error_response(self) -> None:
        """Verify error response format."""
        response = APIResponseFormatter.error("NOT_FOUND", "Resource not found")
        self.assertEqual(response["code"], "NOT_FOUND")
        self.assertEqual(response["error"]["message"], "Resource not found")
        self.assertIn("request_id", response)


class RequestValidatorTests(unittest.TestCase):
    """Test request validation."""

    def test_validate_pagination_defaults(self) -> None:
        """Verify pagination defaults are applied."""
        offset, limit = RequestValidator.validate_pagination(None, None)
        self.assertEqual(offset, 0)
        self.assertEqual(limit, 100)

    def test_validate_pagination_bounds(self) -> None:
        """Verify pagination bounds are enforced."""
        offset, limit = RequestValidator.validate_pagination(-10, 2000)
        self.assertEqual(offset, 0)
        self.assertEqual(limit, 1000)

    def test_validate_date_range(self) -> None:
        """Verify date range parsing."""
        start, end = RequestValidator.validate_date_range("2024-01-01T00:00:00Z", "2024-12-31T23:59:59Z")
        self.assertIsNotNone(start)
        self.assertIsNotNone(end)


class IdempotencyCheckerTests(unittest.TestCase):
    """Test idempotency checker."""

    def test_first_request_not_duplicate(self) -> None:
        """Verify first request is not marked as duplicate."""
        checker = IdempotencyChecker(ttl_seconds=3600)
        is_dup, cached = checker.check("key1")
        self.assertFalse(is_dup)
        self.assertIsNone(cached)

    def test_duplicate_request_returns_cached(self) -> None:
        """Verify duplicate request returns cached response."""
        checker = IdempotencyChecker(ttl_seconds=3600)
        response = {"result": "original"}
        checker.record("key1", response)

        is_dup, cached = checker.check("key1")
        self.assertTrue(is_dup)
        self.assertEqual(cached, response)


class HealthCheckServiceTests(unittest.TestCase):
    """Test health check service."""

    def test_health_check_all_healthy(self) -> None:
        """Verify health check with all healthy components."""
        service = HealthCheckService()
        service.register_component("db", lambda: True)
        service.register_component("cache", lambda: True)

        result = service.check_health()
        self.assertEqual(result["status"], "healthy")
        self.assertEqual(result["components"]["db"], "healthy")

    def test_health_check_unhealthy_component(self) -> None:
        """Verify health check detects unhealthy component."""
        service = HealthCheckService()
        service.register_component("db", lambda: False)

        result = service.check_health()
        self.assertEqual(result["status"], "unhealthy")
        self.assertEqual(result["components"]["db"], "unhealthy")


class MetricsCollectorTests(unittest.TestCase):
    """Test metrics collector."""

    def test_record_and_get_metrics(self) -> None:
        """Verify metrics are recorded and retrieved."""
        collector = MetricsCollector()
        collector.record_request("/api/test", 200, 50.0)
        collector.record_request("/api/test", 200, 100.0)

        metrics = collector.get_metrics()
        self.assertEqual(metrics["requests_total"]["/api/test"], 2)
        self.assertIn("/api/test", metrics["latencies"])


class ConfigHotReloaderTests(unittest.TestCase):
    """Test config hot reloader."""

    def test_get_config_returns_empty_when_not_loaded(self) -> None:
        """Verify config returns empty dict when not loaded."""
        reloader = ConfigHotReloader("/nonexistent/path.json")
        config = reloader.get_config()
        self.assertEqual(config, {})


class DistributedSchedulerTests(unittest.TestCase):
    """Test distributed scheduler."""

    def test_schedule_job(self) -> None:
        """Verify job scheduling."""
        scheduler = DistributedScheduler()

        def dummy_handler():
            pass

        result = scheduler.schedule_job("job1", "0 * * * *", dummy_handler)
        self.assertEqual(result["job_id"], "job1")
        self.assertEqual(result["status"], "scheduled")

    def test_cancel_job(self) -> None:
        """Verify job cancellation."""
        scheduler = DistributedScheduler()

        def dummy_handler():
            pass

        scheduler.schedule_job("job1", "0 * * * *", dummy_handler)
        result = scheduler.cancel_job("job1")
        self.assertTrue(result)

    def test_list_jobs(self) -> None:
        """Verify job listing."""
        scheduler = DistributedScheduler()

        def dummy_handler():
            pass

        scheduler.schedule_job("job1", "0 * * * *", dummy_handler)
        jobs = scheduler.list_jobs()
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0]["job_id"], "job1")


if __name__ == "__main__":
    unittest.main()
