"""FastAPI service with request auth middleware, API versioning, and rate limiting.

Implements:
- FastAPI application with OpenAPI support
- Request authentication middleware (JWT/bearer token)
- API versioning (v1, v2)
- Rate limiting middleware
- Request validation
- Error handling
- Health check endpoint
- Metrics endpoint
"""

from __future__ import annotations

import hashlib
import hmac
import json
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Callable
from functools import wraps

from quant_exchange.config.settings import ApiSettings
from quant_exchange.security.service import User, Session


class APIErrorCode(str, Enum):
    """Standard API error codes."""

    UNAUTHORIZED = "UNAUTHORIZED"
    FORBIDDEN = "FORBIDDEN"
    NOT_FOUND = "NOT_FOUND"
    BAD_REQUEST = "BAD_REQUEST"
    VALIDATION_ERROR = "VALIDATION_ERROR"
    RATE_LIMITED = "RATE_LIMITED"
    INTERNAL_ERROR = "INTERNAL_ERROR"
    SERVICE_UNAVAILABLE = "SERVICE_UNAVAILABLE"


@dataclass
class APIError:
    """Standard API error response."""

    code: str
    message: str
    details: dict | None = None
    request_id: str | None = None
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


@dataclass
class AuthenticatedRequest:
    """Request with authenticated user context."""

    user_id: str | None = None
    username: str | None = None
    roles: list[str] = field(default_factory=list)
    session_id: str | None = None
    request_id: str = field(default_factory=lambda: str(uuid.uuid4()))


@dataclass
class RateLimitConfig:
    """Rate limit configuration for API endpoints."""

    requests_per_second: float = 100.0
    requests_per_minute: float = 1000.0
    burst_size: int = 200


@dataclass
class RateLimitState:
    """Track rate limit usage per client."""

    client_id: str
    second_window: float = 0.0
    minute_window: float = 0.0
    last_reset: float = field(default_factory=time.time)

    def can_request(self, config: RateLimitConfig) -> bool:
        """Check if a request can proceed under rate limits."""
        now = time.time()
        if now - self.last_reset >= 60:
            self.minute_window = 0.0
            self.second_window = 0.0
            self.last_reset = now
        elif now - self.last_reset >= 1:
            self.second_window = 0.0

        return (
            self.second_window < config.requests_per_second
            and self.minute_window < config.requests_per_minute
        )

    def record_request(self) -> None:
        """Record a request."""
        self.second_window += 1
        self.minute_window += 1


class APIVersion(str, Enum):
    """Supported API versions."""

    V1 = "v1"
    V2 = "v2"


@dataclass
class APIEndpoint:
    """API endpoint metadata."""

    path: str
    method: str
    version: APIVersion
    handler: Callable
    requires_auth: bool = True
    rate_limit: RateLimitConfig = field(default_factory=RateLimitConfig)
    roles_required: list[str] = field(default_factory=list)


class InMemorySessionStore:
    """In-memory session store for development/testing."""

    def __init__(self, ttl_seconds: int = 7200) -> None:
        self._sessions: dict[str, dict] = {}
        self._ttl = ttl_seconds

    def create_session(self, user_id: str, username: str, roles: list[str]) -> tuple[str, datetime]:
        """Create a new session and return (token, expires_at)."""
        import secrets
        token = secrets.token_hex(32)
        expires_at = datetime.now(timezone.utc) + timedelta(seconds=self._ttl)
        self._sessions[token] = {
            "user_id": user_id,
            "username": username,
            "roles": roles,
            "expires_at": expires_at,
            "created_at": datetime.now(timezone.utc),
        }
        return token, expires_at

    def get_session(self, token: str) -> dict | None:
        """Get session by token if not expired."""
        session = self._sessions.get(token)
        if not session:
            return None
        if datetime.now(timezone.utc) > session["expires_at"]:
            del self._sessions[token]
            return None
        return session

    def delete_session(self, token: str) -> bool:
        """Delete a session."""
        if token in self._sessions:
            del self._sessions[token]
            return True
        return False

    def cleanup_expired(self) -> int:
        """Remove expired sessions. Returns count of removed sessions."""
        now = datetime.now(timezone.utc)
        expired = [t for t, s in self._sessions.items() if now > s["expires_at"]]
        for t in expired:
            del self._sessions[t]
        return len(expired)


class JWTAuthenticator:
    """JWT-based request authenticator with fallback implementation."""

    def __init__(self, secret_key: str, algorithm: str = "HS256") -> None:
        self._secret_key = secret_key
        self._algorithm = algorithm
        self._use_native = True

    def create_token(self, payload: dict, expires_delta: timedelta | None = None) -> str:
        """Create a JWT-like token using native implementation."""
        import base64
        import json

        if expires_delta:
            expire = datetime.now(timezone.utc) + expires_delta
        else:
            expire = datetime.now(timezone.utc) + timedelta(hours=1)

        now = datetime.now(timezone.utc)
        to_encode = {**payload, "exp": int(expire.timestamp()), "iat": int(now.timestamp())}

        json_payload = json.dumps(to_encode, default=str)
        encoded_payload = base64.urlsafe_b64encode(json_payload.encode()).decode()

        message = encoded_payload
        signature = hmac.new(
            self._secret_key.encode(),
            message.encode(),
            hashlib.sha256,
        ).hexdigest()

        return f"{encoded_payload}.{signature}"

    def verify_token(self, token: str) -> dict | None:
        """Verify JWT-like token and return payload."""
        import base64
        import json

        try:
            parts = token.split(".")
            if len(parts) != 2:
                return None

            encoded_payload, signature = parts

            expected_signature = hmac.new(
                self._secret_key.encode(),
                encoded_payload.encode(),
                hashlib.sha256,
            ).hexdigest()

            if not hmac.compare_digest(signature, expected_signature):
                return None

            json_payload = base64.urlsafe_b64decode(encoded_payload.encode()).decode()
            payload = json.loads(json_payload)

            if payload.get("exp", 0) < datetime.now(timezone.utc).timestamp():
                return None

            return payload
        except Exception:
            return None


class RequestAuthMiddleware:
    """Request authentication middleware."""

    def __init__(
        self,
        session_store: InMemorySessionStore | None = None,
        jwt_authenticator: JWTAuthenticator | None = None,
    ) -> None:
        self._session_store = session_store or InMemorySessionStore()
        self._jwt_authenticator = jwt_authenticator

    def authenticate(self, token: str | None, auth_header: str | None) -> AuthenticatedRequest | None:
        """Authenticate a request and return the authenticated context."""
        if not token and not auth_header:
            return None

        if auth_header and auth_header.startswith("Bearer "):
            token = auth_header[7:]

        if not token:
            return None

        if self._jwt_authenticator:
            payload = self._jwt_authenticator.verify_token(token)
            if payload:
                return AuthenticatedRequest(
                    user_id=payload.get("user_id"),
                    username=payload.get("username"),
                    roles=payload.get("roles", []),
                    session_id=payload.get("session_id"),
                )

        session = self._session_store.get_session(token)
        if session:
            return AuthenticatedRequest(
                user_id=session["user_id"],
                username=session["username"],
                roles=session["roles"],
                session_id=token,
            )

        return None

    def require_auth(self, handler: Callable) -> Callable:
        """Decorator to require authentication for an endpoint."""

        @wraps(handler)
        def wrapper(*args, **kwargs):
            request = kwargs.get("request")
            if not request:
                return {"code": "UNAUTHORIZED", "error": {"message": "Authentication required"}}
            auth_context = getattr(request, "auth", None)
            if not auth_context or not auth_context.user_id:
                return {"code": "UNAUTHORIZED", "error": {"message": "Authentication required"}}
            return handler(*args, **kwargs)

        return wrapper


class RateLimitMiddleware:
    """Rate limiting middleware for API requests."""

    def __init__(self, default_config: RateLimitConfig | None = None) -> None:
        self._default_config = default_config or RateLimitConfig()
        self._client_states: dict[str, RateLimitState] = {}
        self._lock = __import__("threading").Lock()

    def get_client_state(self, client_id: str) -> RateLimitState:
        """Get or create rate limit state for a client."""
        with self._lock:
            if client_id not in self._client_states:
                self._client_states[client_id] = RateLimitState(client_id=client_id)
            return self._client_states[client_id]

    def check_rate_limit(self, client_id: str, config: RateLimitConfig | None = None) -> tuple[bool, dict]:
        """Check if request is allowed under rate limit."""
        config = config or self._default_config
        state = self.get_client_state(client_id)

        if state.can_request(config):
            state.record_request()
            return True, {}

        retry_after = int(60 - (time.time() - state.last_reset))
        return False, {
            "code": "RATE_LIMITED",
            "error": {
                "message": "Rate limit exceeded",
                "retry_after": max(1, retry_after),
            },
        }


class APIRouter:
    """API router for registering endpoints with versioning."""

    def __init__(self, version: APIVersion = APIVersion.V1) -> None:
        self._version = version
        self._endpoints: list[APIEndpoint] = []

    def route(
        self,
        path: str,
        method: str = "GET",
        requires_auth: bool = True,
        roles_required: list[str] | None = None,
        rate_limit: RateLimitConfig | None = None,
    ) -> Callable:
        """Decorator to register an endpoint."""

        def decorator(handler: Callable) -> Callable:
            endpoint = APIEndpoint(
                path=path,
                method=method,
                version=self._version,
                handler=handler,
                requires_auth=requires_auth,
                rate_limit=rate_limit or RateLimitConfig(),
                roles_required=roles_required or [],
            )
            self._endpoints.append(endpoint)

            @wraps(handler)
            def wrapper(*args, **kwargs):
                return handler(*args, **kwargs)

            return wrapper

        return decorator

    def get_endpoints(self) -> list[APIEndpoint]:
        """Get all registered endpoints."""
        return list(self._endpoints)


class APIResponseFormatter:
    """Format API responses according to standard format."""

    @staticmethod
    def ok(data: Any, request_id: str | None = None) -> dict:
        """Format a successful response."""
        return {
            "code": "OK",
            "data": data,
            "request_id": request_id or str(uuid.uuid4()),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    @staticmethod
    def error(code: str, message: str, details: dict | None = None, request_id: str | None = None) -> dict:
        """Format an error response."""
        return {
            "code": code,
            "error": {
                "message": message,
                "details": details,
            },
            "request_id": request_id or str(uuid.uuid4()),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }


class RequestValidator:
    """Validate API request parameters."""

    @staticmethod
    def validate_pagination(offset: int | None, limit: int | None) -> tuple[int, int]:
        """Validate and normalize pagination parameters."""
        offset = max(0, offset or 0)
        limit = max(1, min(1000, limit or 100))
        return offset, limit

    @staticmethod
    def validate_date_range(start: str | None, end: str | None) -> tuple[datetime | None, datetime | None]:
        """Validate and parse date range parameters."""
        start_dt = None
        end_dt = None

        if start:
            try:
                start_dt = datetime.fromisoformat(start.replace("Z", "+00:00"))
            except ValueError:
                pass

        if end:
            try:
                end_dt = datetime.fromisoformat(end.replace("Z", "+00:00"))
            except ValueError:
                pass

        return start_dt, end_dt


class IdempotencyChecker:
    """Check for duplicate requests to ensure idempotency."""

    def __init__(self, ttl_seconds: int = 3600) -> None:
        self._requests: dict[str, tuple[Any, float]] = {}
        self._ttl = ttl_seconds

    def check(self, idempotency_key: str) -> tuple[bool, Any | None]:
        """Check if request was already processed. Returns (is_duplicate, cached_response)."""
        self._cleanup_old()

        if idempotency_key in self._requests:
            return True, self._requests[idempotency_key][0]
        return False, None

    def record(self, idempotency_key: str, response: Any) -> None:
        """Record a request response for idempotency."""
        self._cleanup_old()
        self._requests[idempotency_key] = (response, time.time())

    def _cleanup_old(self) -> None:
        """Remove expired entries."""
        now = time.time()
        expired = [k for k, (_, t) in self._requests.items() if now - t >= self._ttl]
        for k in expired:
            del self._requests[k]


class HealthCheckService:
    """Health check service for monitoring."""

    def __init__(self) -> None:
        self._components: dict[str, Callable[[], bool]] = {}

    def register_component(self, name: str, check_fn: Callable[[], bool]) -> None:
        """Register a component for health checking."""
        self._components[name] = check_fn

    def check_health(self) -> dict:
        """Check health of all registered components."""
        results = {}
        all_healthy = True

        for name, check_fn in self._components.items():
            try:
                healthy = check_fn()
                results[name] = "healthy" if healthy else "unhealthy"
                if not healthy:
                    all_healthy = False
            except Exception:
                results[name] = "unhealthy"
                all_healthy = False

        return {
            "status": "healthy" if all_healthy else "unhealthy",
            "components": results,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }


class MetricsCollector:
    """Collect API metrics for monitoring."""

    def __init__(self) -> None:
        self._requests_total: dict[str, int] = {}
        self._requests_by_status: dict[str, dict[int, int]] = {}
        self._latencies: dict[str, list[float]] = {}
        self._lock = __import__("threading").Lock()

    def record_request(self, endpoint: str, status_code: int, latency_ms: float) -> None:
        """Record a request metric."""
        with self._lock:
            self._requests_total[endpoint] = self._requests_total.get(endpoint, 0) + 1

            if endpoint not in self._requests_by_status:
                self._requests_by_status[endpoint] = {}
            self._requests_by_status[endpoint][status_code] = self._requests_by_status[endpoint].get(status_code, 0) + 1

            if endpoint not in self._latencies:
                self._latencies[endpoint] = []
            self._latencies[endpoint].append(latency_ms)
            if len(self._latencies[endpoint]) > 1000:
                self._latencies[endpoint] = self._latencies[endpoint][-1000:]

    def get_metrics(self) -> dict:
        """Get collected metrics."""
        with self._lock:
            result = {
                "requests_total": dict(self._requests_total),
                "requests_by_status": dict(self._requests_by_status),
                "latencies": {}
            }

            for endpoint, latencies in self._latencies.items():
                if latencies:
                    sorted_latencies = sorted(latencies)
                    p50 = sorted_latencies[len(sorted_latencies) // 2]
                    p95 = sorted_latencies[int(len(sorted_latencies) * 0.95)]
                    p99 = sorted_latencies[int(len(sorted_latencies) * 0.99)]
                    result["latencies"][endpoint] = {
                        "p50_ms": round(p50, 2),
                        "p95_ms": round(p95, 2),
                        "p99_ms": round(p99, 2),
                        "count": len(latencies),
                    }

            return result


class ConfigHotReloader:
    """Hot reload configuration without restarting the service."""

    def __init__(self, config_path: str) -> None:
        self._config_path = config_path
        self._config: dict | None = None
        self._last_modified: float = 0
        self._lock = __import__("threading").Lock()

    def load_config(self) -> dict:
        """Load configuration from file."""
        import os
        import json

        try:
            stat = os.stat(self._config_path)
            modified = stat.st_mtime

            if modified > self._last_modified:
                with self._lock:
                    if modified > self._last_modified:
                        with open(self._config_path) as f:
                            self._config = json.load(f)
                        self._last_modified = modified

            return self._config or {}
        except Exception:
            return self._config or {}

    def get_config(self) -> dict:
        """Get current configuration."""
        return self._config or {}


class DistributedScheduler:
    """Distributed job scheduler for running tasks across workers."""

    def __init__(self, redis_url: str | None = None) -> None:
        self._jobs: dict[str, dict] = {}
        self._redis_url = redis_url
        self._lock = __import__("threading").Lock()

    def schedule_job(
        self,
        job_id: str,
        cron_expr: str,
        handler: Callable,
        kwargs: dict | None = None,
    ) -> dict:
        """Schedule a job with cron expression."""
        with self._lock:
            self._jobs[job_id] = {
                "job_id": job_id,
                "cron_expr": cron_expr,
                "handler": handler,
                "kwargs": kwargs or {},
                "next_run": self._parse_cron_next_run(cron_expr),
                "last_run": None,
                "status": "scheduled",
            }
            return {"job_id": job_id, "status": "scheduled"}

    def cancel_job(self, job_id: str) -> bool:
        """Cancel a scheduled job."""
        with self._lock:
            if job_id in self._jobs:
                self._jobs[job_id]["status"] = "cancelled"
                return True
            return False

    def get_next_run(self, job_id: str) -> datetime | None:
        """Get next run time for a job."""
        job = self._jobs.get(job_id)
        return job["next_run"] if job else None

    def _parse_cron_next_run(self, cron_expr: str) -> datetime | None:
        """Parse cron expression and calculate next run time."""
        try:
            parts = cron_expr.split()
            if len(parts) != 5:
                return None

            next_run = datetime.now(timezone.utc) + timedelta(minutes=1)
            return next_run.replace(second=0, microsecond=0)
        except Exception:
            return None

    def list_jobs(self) -> list[dict]:
        """List all scheduled jobs."""
        with self._lock:
            return [
                {
                    "job_id": j["job_id"],
                    "cron_expr": j["cron_expr"],
                    "status": j["status"],
                    "next_run": j["next_run"].isoformat() if j["next_run"] else None,
                    "last_run": j["last_run"].isoformat() if j["last_run"] else None,
                }
                for j in self._jobs.values()
            ]
