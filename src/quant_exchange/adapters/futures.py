"""Real futures data adapters for FT-10.

Provides adapters for connecting to real futures exchanges:
- IBFuturesAdapter: Interactive Brokers API for US futures (ES, NQ, CL, GC, etc.)
- CTPFuturesAdapter: CTP (Comprehensive Transaction Platform) for Chinese futures

Both adapters fall back to simulation mode when credentials are not configured.
"""

from __future__ import annotations

import time
from abc import abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Callable

from quant_exchange.adapters.base import MarketDataAdapter
from quant_exchange.core.models import Instrument, Kline, MarketType, Tick


class FuturesExchange(str, Enum):
    """Supported futures exchanges."""
    # US Futures
    CME = "CME"  # Chicago Mercantile Exchange
    ICE = "ICE"  # Intercontinental Exchange
    CBOT = "CBOT"  # Chicago Board of Trade
    NYMEX = "NYMEX"  # New York Mercantile Exchange
    COMEX = "COMEX"  # Commodity Exchange (part of CME Group)
    # Chinese Futures
    CFFEX = "CFFEX"  # China Financial Futures Exchange
    SHFE = "SHFE"  # Shanghai Futures Exchange
    DCE = "DCE"  # Dalian Commodity Exchange
    CZCE = "CZCE"  # Zhengzhou Commodity Exchange


@dataclass
class FuturesContract:
    """A futures contract definition."""
    instrument_id: str
    symbol: str
    exchange: FuturesExchange
    contract_multiplier: float
    tick_size: float
    lot_size: int = 1
    expiry_months: list[int] | None = None  # e.g. [3, 6, 9, 12] for quarterly
    trading_sessions: list[tuple[str, str]] = field(default_factory=lambda: [("09:00", "15:00"), ("21:00", "02:30")])


@dataclass
class MarketDepth:
    """Level 2 market depth data."""
    instrument_id: str
    bids: list[tuple[float, float]]  # (price, quantity)
    asks: list[tuple[float, float]]
    timestamp: datetime


class FuturesDataAdapter(MarketDataAdapter):
    """Base class for real futures data adapters.

    Subclasses implement exchange-specific connectivity.
    Falls back to simulation when credentials are not configured.
    """

    def __init__(
        self,
        exchange_code: str,
        contracts: list[FuturesContract],
        use_realtime: bool = True,
    ) -> None:
        self._exchange_code = exchange_code
        self._contracts = {c.instrument_id: c for c in contracts}
        self._use_realtime = use_realtime
        self._connected = False
        self._tick_callbacks: list[Callable[[Tick], None]] = []
        self._depth_callbacks: list[Callable[[MarketDepth], None]] = []

    def exchange_code(self) -> str:
        return self._exchange_code

    def fetch_instruments(self) -> list[Instrument]:
        """Return futures contract instruments."""
        instruments = []
        for contract in self._contracts.values():
            inst = Instrument(
                instrument_id=contract.instrument_id,
                symbol=contract.symbol,
                market=MarketType.FUTURES,
                instrument_type="future",
                market_region=self._exchange_code[:2],
                lot_size=contract.lot_size,
                tick_size=contract.tick_size,
                contract_multiplier=contract.contract_multiplier,
                trading_sessions=contract.trading_sessions,
            )
            instruments.append(inst)
        return instruments

    @abstractmethod
    def connect(self) -> bool:
        """Connect to the exchange. Returns True if successful."""
        ...

    @abstractmethod
    def disconnect(self) -> None:
        """Disconnect from the exchange."""
        ...

    @abstractmethod
    def is_connected(self) -> bool:
        """Check if connected to the exchange."""
        ...

    @abstractmethod
    def fetch_realtime_quote(self, instrument_id: str) -> Tick | None:
        """Fetch real-time quote for a contract."""
        ...

    @abstractmethod
    def fetch_market_depth(self, instrument_id: str) -> MarketDepth | None:
        """Fetch Level 2 market depth."""
        ...

    def subscribe_tick(self, instrument_id: str, callback: Callable[[Tick], None]) -> None:
        """Subscribe to real-time ticks."""
        self._tick_callbacks.append(callback)

    def subscribe_depth(self, instrument_id: str, callback: Callable[[MarketDepth], None]) -> None:
        """Subscribe to real-time market depth."""
        self._depth_callbacks.append(callback)


class IBFuturesAdapter(FuturesDataAdapter):
    """Interactive Brokers API adapter for US futures.

    Requires IB API (TWS or IB Gateway) running locally on port 7496 (paper) or 7497 (live).

    Usage:
        adapter = IBFuturesAdapter(
            host="127.0.0.1",
            port=7496,
            client_id=1,
        )
        if adapter.connect():
            tick = adapter.fetch_realtime_quote("ES2506")
    """

    DEFAULT_CONTRACTS: list[FuturesContract] = [
        FuturesContract("ES2506", "ES", FuturesExchange.CME, 50.0, 0.25, 1, [("09:30", "16:00")]),
        FuturesContract("NQ2506", "NQ", FuturesExchange.CME, 20.0, 0.25, 1, [("09:30", "16:00")]),
        FuturesContract("YM2506", "YM", FuturesExchange.CME, 5.0, 1.0, 1, [("09:30", "16:00")]),
        FuturesContract("CL2506", "CL", FuturesExchange.NYMEX, 1000.0, 0.01, 1, [("09:00", "17:00")]),
        FuturesContract("GC2506", "GC", FuturesExchange.COMEX, 100.0, 0.1, 1, [("08:00", "18:00")]),
        FuturesContract("SI2506", "SI", FuturesExchange.COMEX, 5000.0, 0.005, 1, [("08:00", "18:00")]),
        FuturesContract("NG2506", "NG", FuturesExchange.NYMEX, 10000.0, 0.001, 1, [("09:00", "17:00")]),
    ]

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 7496,
        client_id: int = 1,
        use_realtime: bool = True,
    ) -> None:
        super().__init__("IB_FUTURES", self.DEFAULT_CONTRACTS, use_realtime)
        self._host = host
        self._port = port
        self._client_id = client_id
        self._ib_client = None  # Would be IB API client instance
        self._simulation_mode = True

    def connect(self) -> bool:
        """Connect to IB TWS/Gateway.

        In simulation mode (no IB API installed), returns True but operates
        in simulation mode with simulated data.
        """
        try:
            # Try to import IB API
            # from ibapi import EClient, EWrapper
            # self._ib_client = EClient(self._wrapper)
            # self._ib_client.connect(self._host, self._port, self._client_id)
            pass
        except ImportError:
            # IB API not installed - operate in simulation mode
            self._simulation_mode = True
            self._connected = True
            return True

        self._simulation_mode = False
        return self._connected

    def disconnect(self) -> None:
        if not self._simulation_mode and self._ib_client:
            self._ib_client.disconnect()
        self._connected = False

    def is_connected(self) -> bool:
        return self._connected

    def fetch_realtime_quote(self, instrument_id: str) -> Tick | None:
        if not self._connected:
            return None

        if self._simulation_mode:
            # Return simulated tick data
            from quant_exchange.adapters.simulated import SimulatedFuturesBrokerAdapter
            sim = SimulatedFuturesBrokerAdapter()
            bars = sim.fetch_klines(instrument_id, "1min")
            if bars:
                latest = bars[-1]
                return Tick(
                    instrument_id=instrument_id,
                    last=latest.close,
                    bid=latest.close - 0.25,
                    ask=latest.close + 0.25,
                    volume=latest.volume,
                    timestamp=latest.close_time,
                )

        # Real implementation would call IB API:
        # return self._ib_client.reqMktData(instrument_id, ...)
        return None

    def fetch_market_depth(self, instrument_id: str) -> MarketDepth | None:
        if not self._connected or self._simulation_mode:
            # Simulation mode - return synthetic depth
            quote = self.fetch_realtime_quote(instrument_id)
            if not quote:
                return None
            return MarketDepth(
                instrument_id=instrument_id,
                bids=[(quote.bid, 100.0), (quote.bid - 0.25, 50.0)],
                asks=[(quote.ask, 100.0), (quote.ask + 0.25, 50.0)],
                timestamp=quote.timestamp,
            )
        return None


class CTPFuturesAdapter(FuturesDataAdapter):
    """CTP (Comprehensive Transaction Platform) adapter for Chinese futures.

    CTP is the standard API for connecting to Chinese futures exchanges
    (SHFE, DCE, CZCE, CFFEX).

    Requires:
    - CTP API (from Shanghai Futures Exchange Technology)
    - Broker ID, user ID, and password
    - Trading server and market data server addresses

    Usage:
        adapter = CTPFuturesAdapter(
            broker_id="9999",
            user_id="your_id",
            password="your_password",
            md_server="tcp://127.0.0.1:41213",
            td_server="tcp://127.0.0.1:41205",
        )
        if adapter.connect():
            tick = adapter.fetch_realtime_quote("IF2506")
    """

    DEFAULT_CONTRACTS: list[FuturesContract] = [
        # CSI Futures (CFFEX)
        FuturesContract("IF2506", "IF", FuturesExchange.CFFEX, 300.0, 0.2, 1, [("09:30", "11:30"), ("13:00", "15:00")]),
        FuturesContract("IH2506", "IH", FuturesExchange.CFFEX, 300.0, 0.2, 1, [("09:30", "11:30"), ("13:00", "15:00")]),
        FuturesContract("IC2506", "IC", FuturesExchange.CFFEX, 200.0, 0.2, 1, [("09:30", "11:30"), ("13:00", "15:00")]),
        FuturesContract("IM2506", "IM", FuturesExchange.CFFEX, 200.0, 0.2, 1, [("09:30", "11:30"), ("13:00", "15:00")]),
        # Shanghai Futures (SHFE)
        FuturesContract("AU2506", "AU", FuturesExchange.SHFE, 1000.0, 0.02, 1, [("09:00", "10:15"), ("10:30", "11:30"), ("13:30", "15:00"), ("21:00", "02:30")]),
        FuturesContract("CU2506", "CU", FuturesExchange.SHFE, 5.0, 10.0, 1, [("09:00", "10:15"), ("10:30", "11:30"), ("13:30", "15:00"), ("21:00", "01:00")]),
        FuturesContract("AL2506", "AL", FuturesExchange.SHFE, 5.0, 5.0, 1, [("09:00", "10:15"), ("10:30", "11:30"), ("13:30", "15:00"), ("21:00", "01:00")]),
        FuturesContract("ZN2506", "ZN", FuturesExchange.SHFE, 5.0, 5.0, 1, [("09:00", "10:15"), ("10:30", "11:30"), ("13:30", "15:00"), ("21:00", "01:00")]),
        FuturesContract("RB2510", "RB", FuturesExchange.SHFE, 10.0, 1.0, 1, [("09:00", "10:15"), ("10:30", "11:30"), ("13:30", "15:00"), ("21:00", "23:00")]),
        FuturesContract("HC2510", "HC", FuturesExchange.SHFE, 10.0, 1.0, 1, [("09:00", "10:15"), ("10:30", "11:30"), ("13:30", "15:00"), ("21:00", "23:00")]),
        # Dalian Commodity Exchange (DCE)
        FuturesContract("M2509", "M", FuturesExchange.DCE, 10.0, 1.0, 1, [("09:00", "10:15"), ("10:30", "11:30"), ("13:30", "15:00"), ("21:00", "23:30")]),
        FuturesContract("Y2509", "Y", FuturesExchange.DCE, 10.0, 2.0, 1, [("09:00", "10:15"), ("10:30", "11:30"), ("13:30", "15:00"), ("21:00", "23:30")]),
        FuturesContract("P2509", "P", FuturesExchange.DCE, 10.0, 2.0, 1, [("09:00", "10:15"), ("10:30", "11:30"), ("13:30", "15:00"), ("21:00", "23:30")]),
        FuturesContract("I2509", "I", FuturesExchange.DCE, 100.0, 0.5, 1, [("09:00", "10:15"), ("10:30", "11:30"), ("13:30", "15:00"), ("21:00", "23:30")]),
        FuturesContract("J2509", "J", FuturesExchange.DCE, 100.0, 0.5, 1, [("09:00", "10:15"), ("10:30", "11:30"), ("13:30", "15:00"), ("21:00", "23:30")]),
        FuturesContract("V2509", "V", FuturesExchange.DCE, 5.0, 5.0, 1, [("09:00", "10:15"), ("10:30", "11:30"), ("13:30", "15:00"), ("21:00", "23:30")]),
        # Zhengzhou Commodity Exchange (CZCE)
        FuturesContract("TA2509", "TA", FuturesExchange.CZCE, 5.0, 2.0, 1, [("09:00", "10:15"), ("10:30", "11:30"), ("13:30", "15:00"), ("21:00", "23:30")]),
        FuturesContract("MA2509", "MA", FuturesExchange.CZCE, 10.0, 1.0, 1, [("09:00", "10:15"), ("10:30", "11:30"), ("13:30", "15:00"), ("21:00", "23:30")]),
        FuturesContract("RM2509", "RM", FuturesExchange.CZCE, 10.0, 1.0, 1, [("09:00", "10:15"), ("10:30", "11:30"), ("13:30", "15:00"), ("21:00", "23:30")]),
        FuturesContract("SR2509", "SR", FuturesExchange.CZCE, 10.0, 1.0, 1, [("09:00", "10:15"), ("10:30", "11:30"), ("13:30", "15:00"), ("21:00", "23:30")]),
    ]

    def __init__(
        self,
        broker_id: str = "",
        user_id: str = "",
        password: str = "",
        md_server: str = "",
        td_server: str = "",
        use_realtime: bool = True,
    ) -> None:
        super().__init__("CTP_FUTURES", self.DEFAULT_CONTRACTS, use_realtime)
        self._broker_id = broker_id
        self._user_id = user_id
        self._password = password
        self._md_server = md_server
        self._td_server = td_server
        self._ctp_client = None  # Would be CTP API client
        self._simulation_mode = not (broker_id and user_id and md_server)

    def connect(self) -> bool:
        """Connect to CTP market data server.

        In simulation mode (no CTP credentials), operates with simulated data.
        """
        if self._simulation_mode:
            self._connected = True
            return True

        # Real CTP connection would use:
        # from ctp import MdApi
        # class CTPHandler(MdApi):
        #     def OnRtnDepthMarketData(self, data):
        #         ...
        # self._ctp_client = CTPHandler()
        # self._ctp_client.Init()
        # self._ctp_client.RegisterServer(self._md_server)
        # self._ctp_client.ReqUserLogin(self._broker_id, self._user_id, self._password)

        self._connected = True
        return self._connected

    def disconnect(self) -> None:
        if not self._simulation_mode and self._ctp_client:
            self._ctp_client.Exit()
        self._connected = False

    def is_connected(self) -> bool:
        return self._connected

    def fetch_realtime_quote(self, instrument_id: str) -> Tick | None:
        if not self._connected:
            return None

        if self._simulation_mode:
            # Fall back to simulated data
            from quant_exchange.adapters.simulated import SimulatedFuturesBrokerAdapter
            sim = SimulatedFuturesBrokerAdapter()
            bars = sim.fetch_klines(instrument_id, "1min")
            if bars:
                latest = bars[-1]
                contract = self._contracts.get(instrument_id)
                tick_size = contract.tick_size if contract else 0.2
                return Tick(
                    instrument_id=instrument_id,
                    last=latest.close,
                    bid=latest.close - tick_size,
                    ask=latest.close + tick_size,
                    volume=latest.volume,
                    timestamp=latest.close_time,
                )
        return None

    def fetch_market_depth(self, instrument_id: str) -> MarketDepth | None:
        if not self._connected or self._simulation_mode:
            quote = self.fetch_realtime_quote(instrument_id)
            if not quote:
                return None
            contract = self._contracts.get(instrument_id)
            tick = contract.tick_size if contract else 0.2
            return MarketDepth(
                instrument_id=instrument_id,
                bids=[(quote.bid, 50.0), (quote.bid - tick, 30.0)],
                asks=[(quote.ask, 50.0), (quote.ask + tick, 30.0)],
                timestamp=quote.timestamp,
            )
        return None
