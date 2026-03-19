"""Abstract market data and execution adapter contracts."""

from __future__ import annotations

from abc import ABC, abstractmethod

from quant_exchange.core.models import Instrument, Kline, OrderRequest, PortfolioSnapshot


class MarketDataAdapter(ABC):
    """Contract for venue-specific market data integrations."""

    @abstractmethod
    def exchange_code(self) -> str:
        """Return the venue code represented by this adapter."""

    @abstractmethod
    def fetch_instruments(self) -> list[Instrument]:
        """Return tradable instruments normalized to internal models."""

    @abstractmethod
    def fetch_klines(self, instrument_id: str, interval: str) -> list[Kline]:
        """Return historical bars for an instrument and interval."""


class ExecutionAdapter(ABC):
    """Contract for venue-specific execution integrations."""

    @abstractmethod
    def submit_order(self, request: OrderRequest) -> dict:
        """Submit an order request to the venue and return a normalized response."""

    @abstractmethod
    def cancel_order(self, venue_order_id: str) -> dict:
        """Cancel an existing venue order."""

    @abstractmethod
    def fetch_account_snapshot(self) -> PortfolioSnapshot:
        """Return a normalized account snapshot."""
