"""Registry used to manage multiple market data and execution adapters."""

from __future__ import annotations


class AdapterRegistry:
    """Register and retrieve adapters by exchange code."""

    def __init__(self) -> None:
        self.market_data_adapters: dict[str, object] = {}
        self.execution_adapters: dict[str, object] = {}

    def register_market_data(self, exchange_code: str, adapter: object) -> None:
        """Register a market data adapter."""

        self.market_data_adapters[exchange_code] = adapter

    def register_execution(self, exchange_code: str, adapter: object) -> None:
        """Register an execution adapter."""

        self.execution_adapters[exchange_code] = adapter

    def get_market_data(self, exchange_code: str) -> object:
        """Return the market data adapter for a venue."""

        return self.market_data_adapters[exchange_code]

    def get_execution(self, exchange_code: str) -> object:
        """Return the execution adapter for a venue."""

        return self.execution_adapters[exchange_code]
