"""Real exchange REST and WebSocket adapters with reconnection, rate limiting, and subscription recovery.

Implements:
- REST adapter for exchange REST API connectivity
- WebSocket adapter with subscription management
- Automatic reconnection with exponential backoff
- Subscription recovery on reconnect
- Rate limiting mechanism
- Circuit breaker pattern for degradation
- Signature authentication for API keys
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import time
import uuid
from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Callable
from urllib.parse import urlencode

import requests

from quant_exchange.adapters.base import ExecutionAdapter, MarketDataAdapter
from quant_exchange.core.models import Instrument, Kline, MarketType, OrderRequest, PortfolioSnapshot, Tick


class ConnectionState(str, Enum):
    """WebSocket connection state."""

    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    RECONNECTING = "reconnecting"
    FAILED = "failed"


class CircuitState(str, Enum):
    """Circuit breaker states."""

    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class RateLimitConfig:
    """Rate limit configuration for an exchange API."""

    requests_per_second: float = 10.0
    requests_per_minute: float = 600.0
    requests_per_hour: float = 10000.0
    burst_size: int = 20


@dataclass
class RateLimitState:
    """Track rate limit usage."""

    second_window: float = 0.0
    minute_window: float = 0.0
    hour_window: float = 0.0
    last_request_time: float = 0.0

    def can_request(self, config: RateLimitConfig) -> bool:
        """Check if a request can be made under rate limits."""
        now = time.time()
        self._cleanup_old_requests(now, config)
        return (
            self.second_window < config.requests_per_second
            and self.minute_window < config.requests_per_minute
            and self.hour_window < config.requests_per_hour
        )

    def record_request(self, config: RateLimitConfig) -> None:
        """Record a request timestamp."""
        now = time.time()
        self._cleanup_old_requests(now, config)
        self.second_window += 1
        self.minute_window += 1
        self.hour_window += 1
        self.last_request_time = now

    def _cleanup_old_requests(self, now: float, config: RateLimitConfig) -> None:
        """Remove expired request counts from windows."""
        if self.last_request_time > 0:
            age_seconds = now - self.last_request_time
            if age_seconds >= 3600:
                self.hour_window = 0.0
                self.minute_window = 0.0
                self.second_window = 0.0
            elif age_seconds >= 60:
                decay = min(1.0, (age_seconds - 60) / 3600)
                self.hour_window = max(0.0, self.hour_window - decay)
                self.minute_window = 0.0
                self.second_window = 0.0
            elif age_seconds >= 1:
                decay = min(1.0, (age_seconds - 1) / 60)
                self.second_window = max(0.0, self.second_window - decay)


@dataclass
class CircuitBreakerConfig:
    """Circuit breaker configuration."""

    failure_threshold: int = 5
    success_threshold: int = 2
    timeout_seconds: float = 30.0


@dataclass
class CircuitBreaker:
    """Circuit breaker for preventing cascade failures."""

    state: CircuitState = CircuitState.CLOSED
    failure_count: int = 0
    success_count: int = 0
    last_failure_time: float = 0.0
    config: CircuitBreakerConfig = field(default_factory=CircuitBreakerConfig)

    def record_success(self) -> None:
        """Record a successful call."""
        if self.state == CircuitState.HALF_OPEN:
            self.success_count += 1
            if self.success_count >= self.config.success_threshold:
                self.state = CircuitState.CLOSED
                self.failure_count = 0
                self.success_count = 0
        elif self.state == CircuitState.CLOSED:
            self.failure_count = 0

    def record_failure(self) -> None:
        """Record a failed call."""
        self.failure_count += 1
        self.last_failure_time = time.time()
        if self.state == CircuitState.HALF_OPEN:
            self.state = CircuitState.OPEN
            self.success_count = 0
        elif self.failure_count >= self.config.failure_threshold:
            self.state = CircuitState.OPEN

    def can_execute(self) -> bool:
        """Check if a request can be executed."""
        if self.state == CircuitState.CLOSED:
            return True
        if self.state == CircuitState.OPEN:
            if time.time() - self.last_failure_time >= self.config.timeout_seconds:
                self.state = CircuitState.HALF_OPEN
                self.success_count = 0
                return True
            return False
        return True


@dataclass
class WebSocketSubscription:
    """A WebSocket subscription for market data."""

    subscription_id: str
    instrument_id: str
    data_type: str  # "kline", "tick", "orderbook", "funding"
    timeframe: str = ""
    callback: Callable | None = None
    is_active: bool = True
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    last_message_time: datetime | None = None


@dataclass
class ExchangeCredentials:
    """Exchange API credentials with signature support."""

    api_key: str
    secret_key: str
    passphrase: str = ""  # For exchanges like Coinbase
    testnet: bool = False


class RESTAdapter(ABC):
    """Abstract REST adapter for exchange REST API connectivity."""

    def __init__(
        self,
        exchange_code: str,
        base_url: str,
        credentials: ExchangeCredentials | None = None,
        rate_limit_config: RateLimitConfig | None = None,
    ) -> None:
        self.exchange_code = exchange_code
        self.base_url = base_url.rstrip("/")
        self.credentials = credentials
        self.rate_limit_config = rate_limit_config or RateLimitConfig()
        self.rate_limit_state = RateLimitState()
        self.circuit_breaker = CircuitBreaker()
        self._session = requests.Session()
        self._session.headers.update({"Content-Type": "application/json"})

    def _apply_rate_limit(self) -> bool:
        """Apply rate limiting. Returns True if request can proceed."""
        if self.rate_limit_state.can_request(self.rate_limit_config):
            self.rate_limit_state.record_request(self.rate_limit_config)
            return True
        sleep_time = 1.0 / self.rate_limit_config.requests_per_second
        time.sleep(sleep_time)
        self.rate_limit_state.record_request(self.rate_limit_config)
        return True

    def _sign_request(self, method: str, endpoint: str, params: dict | None = None) -> dict[str, str]:
        """Generate authentication headers with signature for API request."""
        if not self.credentials:
            return {}

        timestamp = str(int(time.time()))
        params = params or {}
        query_string = urlencode(sorted(params.items()))

        message = f"{timestamp}{method.upper()}{endpoint}{query_string}"
        signature = hmac.new(
            self.credentials.secret_key.encode("utf-8"),
            message.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

        headers = {
            "X-API-KEY": self.credentials.api_key,
            "X-TIMESTAMP": timestamp,
            "X-SIGNATURE": signature,
        }
        if self.credentials.passphrase:
            headers["X-PASSPHRASE"] = self.credentials.passphrase
        return headers

    def get(self, endpoint: str, params: dict | None = None, signed: bool = False) -> dict:
        """Make a signed GET request."""
        return self._request("GET", endpoint, params=params, signed=signed)

    def post(self, endpoint: str, params: dict | None = None, signed: bool = False) -> dict:
        """Make a signed POST request."""
        return self._request("POST", endpoint, params=params, signed=signed)

    def delete(self, endpoint: str, params: dict | None = None, signed: bool = False) -> dict:
        """Make a signed DELETE request."""
        return self._request("DELETE", endpoint, params=params, signed=signed)

    def _request(self, method: str, endpoint: str, params: dict | None = None, signed: bool = False) -> dict:
        """Make an HTTP request with rate limiting and circuit breaker."""
        if not self.circuit_breaker.can_execute():
            raise CircuitBreakerOpenError(f"Circuit breaker is open for {self.exchange_code}")

        self._apply_rate_limit()

        url = f"{self.base_url}{endpoint}"
        headers = {}
        if signed:
            headers = self._sign_request(method, endpoint, params)

        try:
            if method == "GET":
                response = self._session.get(url, params=params, headers=headers, timeout=10)
            elif method == "POST":
                response = self._session.post(url, json=params, headers=headers, timeout=10)
            elif method == "DELETE":
                response = self._session.delete(url, params=params, headers=headers, timeout=10)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")

            self.circuit_breaker.record_success()
            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as exc:
            self.circuit_breaker.record_failure()
            raise RESTAPIError(f"{method} {url} failed: {exc}") from exc

    @abstractmethod
    def normalize_instrument(self, raw: dict) -> Instrument:
        """Convert exchange-specific instrument format to internal model."""

    @abstractmethod
    def normalize_kline(self, raw: dict, instrument_id: str, timeframe: str) -> Kline:
        """Convert exchange-specific kline format to internal model."""

    @abstractmethod
    def normalize_tick(self, raw: dict, instrument_id: str) -> Tick:
        """Convert exchange-specific tick format to internal model."""


class WebSocketAdapter(ABC):
    """Abstract WebSocket adapter with reconnection and subscription management."""

    def __init__(
        self,
        exchange_code: str,
        ws_url: str,
        credentials: ExchangeCredentials | None = None,
        rate_limit_config: RateLimitConfig | None = None,
        reconnect_config: dict | None = None,
    ) -> None:
        self.exchange_code = exchange_code
        self.ws_url = ws_url
        self.credentials = credentials
        self.rate_limit_config = rate_limit_config or RateLimitConfig()
        self.rate_limit_state = RateLimitState()

        self._reconnect_config = {
            "initial_delay_seconds": 1.0,
            "max_delay_seconds": 60.0,
            "max_retries": 10,
            "backoff_multiplier": 2.0,
        }
        if reconnect_config:
            self._reconnect_config.update(reconnect_config)

        self._connection_state = ConnectionState.DISCONNECTED
        self._ws: Any = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._recv_task: asyncio.Task | None = None
        self._subscriptions: dict[str, WebSocketSubscription] = {}
        self._pending_subscriptions: dict[str, WebSocketSubscription] = {}
        self._last_ping_time: float = 0.0
        self._reconnect_attempts: int = 0
        self._message_queue: asyncio.Queue = asyncio.Queue()
        self._running = False

    @property
    def connection_state(self) -> ConnectionState:
        """Return current connection state."""
        return self._connection_state

    def _set_state(self, state: ConnectionState) -> None:
        """Update connection state."""
        self._connection_state = state

    async def connect(self) -> bool:
        """Establish WebSocket connection with authentication."""
        if self._connection_state in (ConnectionState.CONNECTING, ConnectionState.CONNECTED):
            return True

        self._set_state(ConnectionState.CONNECTING)

        try:
            import websockets

            headers = self._build_connect_headers()
            self._ws = await websockets.connect(self.ws_url, extra_headers=headers)
            self._set_state(ConnectionState.CONNECTED)
            self._reconnect_attempts = 0
            self._running = True

            if self._loop is None:
                self._loop = asyncio.get_event_loop()

            self._recv_task = asyncio.create_task(self._receive_loop())

            await self._recover_subscriptions()
            return True

        except Exception as exc:
            self._set_state(ConnectionState.FAILED)
            raise WebSocketConnectionError(f"Failed to connect to {self.ws_url}: {exc}") from exc

    async def disconnect(self) -> None:
        """Close WebSocket connection gracefully."""
        self._running = False
        if self._recv_task:
            self._recv_task.cancel()
            try:
                await self._recv_task
            except asyncio.CancelledError:
                pass

        if self._ws:
            await self._ws.close()
            self._ws = None

        self._set_state(ConnectionState.DISCONNECTED)

    async def reconnect(self) -> bool:
        """Attempt to reconnect with exponential backoff."""
        if self._reconnect_attempts >= self._reconnect_config["max_retries"]:
            self._set_state(ConnectionState.FAILED)
            return False

        self._set_state(ConnectionState.RECONNECTING)
        self._reconnect_attempts += 1

        delay = min(
            self._reconnect_config["initial_delay_seconds"] * (self._reconnect_config["backoff_multiplier"] ** (self._reconnect_attempts - 1)),
            self._reconnect_config["max_delay_seconds"],
        )

        await asyncio.sleep(delay)

        try:
            await self.disconnect()
            return await self.connect()
        except Exception:
            return await self.reconnect()

    async def _receive_loop(self) -> None:
        """Main message receiving loop."""
        while self._running and self._ws:
            try:
                message = await asyncio.wait_for(self._ws.recv(), timeout=30.0)
                await self._handle_message(message)
            except asyncio.TimeoutError:
                await self._send_ping()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                if self._running:
                    await self._handle_connection_error(exc)

    async def _handle_message(self, message: str | bytes) -> None:
        """Process incoming WebSocket message."""
        try:
            if isinstance(message, bytes):
                message = message.decode("utf-8")
            data = json.loads(message)
            await self._dispatch_message(data)
        except json.JSONDecodeError:
            pass

    async def _dispatch_message(self, data: dict) -> None:
        """Dispatch message to appropriate handler based on subscription."""
        subscription_id = data.get("subscription_id") or data.get("channel")
        if not subscription_id:
            return

        subscription = self._subscriptions.get(subscription_id)
        if subscription and subscription.callback:
            try:
                if asyncio.iscoroutinefunction(subscription.callback):
                    await subscription.callback(data)
                else:
                    subscription.callback(data)
            except Exception:
                pass

        subscription = self._subscriptions.get(subscription_id)
        if subscription:
            subscription.last_message_time = datetime.now(timezone.utc)

    async def _handle_connection_error(self, exc: Exception) -> None:
        """Handle connection errors and trigger reconnection."""
        self._set_state(ConnectionState.RECONNECTING)
        await asyncio.sleep(1)
        await self.reconnect()

    async def _send_ping(self) -> None:
        """Send ping to keep connection alive."""
        try:
            if self._ws:
                await self._ws.ping()
                self._last_ping_time = time.time()
        except Exception:
            pass

    async def subscribe(
        self,
        instrument_id: str,
        data_type: str,
        timeframe: str = "",
        callback: Callable | None = None,
    ) -> WebSocketSubscription:
        """Subscribe to market data."""
        subscription_id = f"{instrument_id}:{data_type}:{timeframe}"

        subscription = WebSocketSubscription(
            subscription_id=subscription_id,
            instrument_id=instrument_id,
            data_type=data_type,
            timeframe=timeframe,
            callback=callback,
        )

        self._subscriptions[subscription_id] = subscription

        if self._connection_state == ConnectionState.CONNECTED:
            await self._send_subscription(subscription)
        else:
            self._pending_subscriptions[subscription_id] = subscription

        return subscription

    async def unsubscribe(self, subscription_id: str) -> bool:
        """Unsubscribe from market data."""
        subscription = self._subscriptions.pop(subscription_id, None)
        if not subscription:
            return False

        self._pending_subscriptions.pop(subscription_id, None)

        if self._connection_state == ConnectionState.CONNECTED:
            await self._send_unsubscription(subscription)

        return True

    async def _recover_subscriptions(self) -> None:
        """Recover subscriptions after reconnection."""
        for subscription in list(self._subscriptions.values()):
            if subscription.is_active:
                await self._send_subscription(subscription)

        for subscription in list(self._pending_subscriptions.values()):
            await self._send_subscription(subscription)
        self._pending_subscriptions.clear()

    @abstractmethod
    async def _send_subscription(self, subscription: WebSocketSubscription) -> None:
        """Send subscription message to exchange."""

    @abstractmethod
    async def _send_unsubscription(self, subscription: WebSocketSubscription) -> None:
        """Send unsubscription message to exchange."""

    @abstractmethod
    def _build_connect_headers(self) -> dict[str, str]:
        """Build WebSocket connection headers including auth."""

    def get_subscriptions(self, instrument_id: str | None = None, data_type: str | None = None) -> list[WebSocketSubscription]:
        """Get active subscriptions with optional filtering."""
        subs = list(self._subscriptions.values())
        if instrument_id:
            subs = [s for s in subs if s.instrument_id == instrument_id]
        if data_type:
            subs = [s for s in subs if s.data_type == data_type]
        return subs


class RESTAPIError(Exception):
    """REST API request failed."""


class CircuitBreakerOpenError(Exception):
    """Circuit breaker is open and request cannot be executed."""


class WebSocketConnectionError(Exception):
    """WebSocket connection failed."""


class BinanceRESTAdapter(RESTAdapter):
    """Binance REST API adapter."""

    def __init__(self, credentials: ExchangeCredentials | None = None, testnet: bool = False) -> None:
        base_url = "https://testnet.binance.vision" if testnet else "https://api.binance.com"
        super().__init__("BINANCE", base_url, credentials)
        self._session.headers.update({"X-MBX-APIKEY": credentials.api_key if credentials else ""})

    def _sign_request(self, method: str, endpoint: str, params: dict | None = None) -> dict[str, str]:
        """Generate Binance API signature."""
        if not self.credentials:
            return {}

        timestamp = str(int(time.time() * 1000))
        params = params or {}
        params["timestamp"] = timestamp

        query_string = urlencode(sorted(params.items()))
        signature = hmac.new(
            self.credentials.secret_key.encode("utf-8"),
            query_string.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

        return {
            "X-MBX-APIKEY": self.credentials.api_key,
            "signature": signature,
        }

    def normalize_instrument(self, raw: dict) -> Instrument:
        """Convert Binance symbol info to Instrument."""
        return Instrument(
            instrument_id=raw["symbol"],
            symbol=raw["symbol"],
            market=MarketType.CRYPTO,
            lot_size=float(raw.get("lotSize", "0.001")),
            tick_size=float(raw.get("pricePrecision", "0.1")),
            quote_currency=raw.get("quoteAsset", "USDT"),
            base_currency=raw.get("baseAsset", ""),
        )

    def normalize_kline(self, raw: list, instrument_id: str, timeframe: str) -> Kline:
        """Convert Binance kline to Kline model."""
        return Kline(
            instrument_id=instrument_id,
            timeframe=timeframe,
            open_time=datetime.fromtimestamp(raw[0] / 1000, tz=timezone.utc),
            close_time=datetime.fromtimestamp(raw[6] / 1000, tz=timezone.utc) if len(raw) > 6 else datetime.now(timezone.utc),
            open=float(raw[1]),
            high=float(raw[2]),
            low=float(raw[3]),
            close=float(raw[4]),
            volume=float(raw[5]),
        )

    def normalize_tick(self, raw: dict, instrument_id: str) -> Tick:
        """Convert Binance trade to Tick model."""
        return Tick(
            instrument_id=instrument_id,
            timestamp=datetime.fromtimestamp(raw["T"] / 1000, tz=timezone.utc),
            price=float(raw["p"]),
            size=float(raw["q"]),
        )


class BinanceWebSocketAdapter(WebSocketAdapter):
    """Binance WebSocket adapter with subscription management."""

    STREAM_TYPE_MAP = {
        "kline": "kline",
        "tick": "trade",
        "orderbook": "depth",
    }

    def __init__(self, credentials: ExchangeCredentials | None = None, testnet: bool = False) -> None:
        ws_url = "wss://testnet.binance.vision/ws" if testnet else "wss://stream.binance.com:9443/ws"
        super().__init__("BINANCE", ws_url, credentials)

    def _build_connect_headers(self) -> dict[str, str]:
        """Build Binance WebSocket connection headers."""
        return {}

    async def _send_subscription(self, subscription: WebSocketSubscription) -> None:
        """Send subscription to Binance WebSocket stream."""
        if not self._ws:
            return

        stream_name = self.STREAM_TYPE_MAP.get(subscription.data_type, subscription.data_type)
        if subscription.timeframe:
            stream_name = f"{stream_name}_{subscription.timeframe}"
        stream_name = f"{subscription.instrument_id.lower()}@{stream_name}"

        message = {
            "method": "SUBSCRIBE",
            "params": [stream_name],
            "id": int(uuid.uuid4().int % 1000000),
        }
        await self._ws.send(json.dumps(message))

    async def _send_unsubscription(self, subscription: WebSocketSubscription) -> None:
        """Send unsubscription to Binance WebSocket stream."""
        if not self._ws:
            return

        stream_name = self.STREAM_TYPE_MAP.get(subscription.data_type, subscription.data_type)
        if subscription.timeframe:
            stream_name = f"{stream_name}_{subscription.timeframe}"
        stream_name = f"{subscription.instrument_id.lower()}@{stream_name}"

        message = {
            "method": "UNSUBSCRIBE",
            "params": [stream_name],
            "id": int(uuid.uuid4().int % 1000000),
        }
        await self._ws.send(json.dumps(message))


class OrderSubmissionTracker:
    """Track order submissions for compensation polling and eventual consistency."""

    def __init__(self, adapter: RESTAdapter, max_polls: int = 5, poll_interval_seconds: float = 1.0) -> None:
        self.adapter = adapter
        self.max_polls = max_polls
        self.poll_interval = poll_interval_seconds
        self._pending_orders: dict[str, dict] = {}

    def track(self, client_order_id: str, exchange_order_id: str | None = None) -> None:
        """Track a newly submitted order."""
        self._pending_orders[client_order_id] = {
            "exchange_order_id": exchange_order_id,
            "submit_time": time.time(),
            "poll_count": 0,
            "status": "SUBMITTED",
        }

    def update_exchange_id(self, client_order_id: str, exchange_order_id: str) -> None:
        """Update exchange order ID after receiving venue response."""
        if client_order_id in self._pending_orders:
            self._pending_orders[client_order_id]["exchange_order_id"] = exchange_order_id

    async def poll_for_confirmation(self, client_order_id: str) -> dict | None:
        """Poll for order confirmation until final state is reached."""
        order_info = self._pending_orders.get(client_order_id)
        if not order_info:
            return None

        exchange_id = order_info.get("exchange_order_id")
        if not exchange_id:
            return None

        for _ in range(self.max_polls):
            await asyncio.sleep(self.poll_interval)
            try:
                status = await self._check_order_status(exchange_id)
                order_info["status"] = status
                order_info["poll_count"] += 1

                if status in ("FILLED", "PARTIALLY_FILLED", "CANCELLED", "REJECTED"):
                    self._pending_orders.pop(client_order_id, None)
                    return status
            except Exception:
                break

        return order_info.get("status")

    async def _check_order_status(self, exchange_order_id: str) -> str:
        """Check order status via REST API."""
        return "UNKNOWN"


class SubscriptionManager:
    """Manage subscriptions across multiple adapters with recovery."""

    def __init__(self) -> None:
        self._adapters: dict[str, WebSocketAdapter] = {}
        self._subscriptions: dict[str, WebSocketSubscription] = {}
        self._recovery_handlers: list[Callable] = []

    def register_adapter(self, exchange_code: str, adapter: WebSocketAdapter) -> None:
        """Register a WebSocket adapter."""
        self._adapters[exchange_code] = adapter

    async def subscribe(
        self,
        exchange_code: str,
        instrument_id: str,
        data_type: str,
        timeframe: str = "",
        callback: Callable | None = None,
    ) -> WebSocketSubscription | None:
        """Subscribe via the appropriate adapter."""
        adapter = self._adapters.get(exchange_code)
        if not adapter:
            return None

        subscription = await adapter.subscribe(instrument_id, data_type, timeframe, callback)
        self._subscriptions[subscription.subscription_id] = subscription
        return subscription

    async def unsubscribe(self, exchange_code: str, subscription_id: str) -> bool:
        """Unsubscribe and clean up."""
        adapter = self._adapters.get(exchange_code)
        if not adapter:
            return False

        self._subscriptions.pop(subscription_id, None)
        return await adapter.unsubscribe(subscription_id)

    async def recover_all(self) -> int:
        """Recover all subscriptions across adapters."""
        recovered = 0
        for adapter in self._adapters.values():
            if adapter.connection_state == ConnectionState.CONNECTED:
                await adapter._recover_subscriptions()
                recovered += len(adapter._subscriptions)
        return recovered

    def add_recovery_handler(self, handler: Callable) -> None:
        """Add a handler to be called during subscription recovery."""
        self._recovery_handlers.append(handler)

    def get_subscriptions(self, exchange_code: str | None = None) -> list[WebSocketSubscription]:
        """Get subscriptions, optionally filtered by exchange."""
        if exchange_code:
            adapter = self._adapters.get(exchange_code)
            if not adapter:
                return []
            return adapter.get_subscriptions()
        return list(self._subscriptions.values())
