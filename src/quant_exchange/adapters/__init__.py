"""Market adapter abstractions and simulated reference implementations."""

from .base import ExecutionAdapter, MarketDataAdapter
from .exchange import (
    BinanceRESTAdapter,
    BinanceWebSocketAdapter,
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitState,
    ConnectionState,
    ExchangeCredentials,
    OrderSubmissionTracker,
    RESTAdapter,
    RESTAPIError,
    SubscriptionManager,
    WebSocketAdapter,
    WebSocketConnectionError,
    WebSocketSubscription,
)
from .registry import AdapterRegistry
from .simulated import (
    SimulatedCryptoExchangeAdapter,
    SimulatedEquityBrokerAdapter,
    SimulatedFuturesBrokerAdapter,
)

__all__ = [
    "AdapterRegistry",
    "ExecutionAdapter",
    "MarketDataAdapter",
    "SimulatedCryptoExchangeAdapter",
    "SimulatedEquityBrokerAdapter",
    "SimulatedFuturesBrokerAdapter",
    # Exchange adapters
    "BinanceRESTAdapter",
    "BinanceWebSocketAdapter",
    "CircuitBreaker",
    "CircuitBreakerConfig",
    "CircuitState",
    "ConnectionState",
    "ExchangeCredentials",
    "OrderSubmissionTracker",
    "RESTAdapter",
    "RESTAPIError",
    "SubscriptionManager",
    "WebSocketAdapter",
    "WebSocketConnectionError",
    "WebSocketSubscription",
]
