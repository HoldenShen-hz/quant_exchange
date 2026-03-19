"""Simulated adapters that mimic real market integrations for testing."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from quant_exchange.adapters.base import ExecutionAdapter, MarketDataAdapter
from quant_exchange.core.models import Instrument, Kline, MarketType, OrderRequest, PortfolioSnapshot


class _BaseSimulatedAdapter(MarketDataAdapter, ExecutionAdapter):
    """Shared helper logic for simulated venue adapters."""

    def __init__(self, exchange_code: str, instruments: list[Instrument], base_price: float | dict[str, float]) -> None:
        self._exchange_code = exchange_code
        self._instruments = instruments
        self._base_price = base_price
        self._order_counter = 0

    def exchange_code(self) -> str:
        """Return the configured exchange code."""

        return self._exchange_code

    def fetch_instruments(self) -> list[Instrument]:
        """Return the static instrument universe exposed by the adapter."""

        return list(self._instruments)

    def fetch_klines(self, instrument_id: str, interval: str) -> list[Kline]:
        """Generate deterministic bars for the requested instrument."""

        start = datetime(2025, 1, 1, tzinfo=timezone.utc)
        base_price = self._base_price[instrument_id] if isinstance(self._base_price, dict) else self._base_price
        bars: list[Kline] = []
        for idx in range(180):
            open_price = base_price + idx * 0.8
            close_price = open_price + 0.5
            bars.append(
                Kline(
                    instrument_id=instrument_id,
                    timeframe=interval,
                    open_time=start + timedelta(days=idx),
                    close_time=start + timedelta(days=idx, hours=23, minutes=59),
                    open=open_price,
                    high=close_price + 1.0,
                    low=open_price - 1.0,
                    close=close_price,
                    volume=1_000 + idx * 10,
                )
            )
        return bars

    def submit_order(self, request: OrderRequest) -> dict:
        """Return a normalized venue order acknowledgement."""

        self._order_counter += 1
        return {
            "exchange_code": self._exchange_code,
            "exchange_order_id": f"{self._exchange_code}_ord_{self._order_counter}",
            "client_order_id": request.client_order_id,
            "status": "SUBMITTED",
        }

    def cancel_order(self, venue_order_id: str) -> dict:
        """Return a normalized venue cancellation acknowledgement."""

        return {
            "exchange_code": self._exchange_code,
            "exchange_order_id": venue_order_id,
            "status": "CANCELLED",
        }

    def fetch_account_snapshot(self) -> PortfolioSnapshot:
        """Return a deterministic account snapshot for testing."""

        return PortfolioSnapshot(
            timestamp=datetime.now(timezone.utc),
            cash=100_000.0,
            positions_value=0.0,
            equity=100_000.0,
            gross_exposure=0.0,
            net_exposure=0.0,
            leverage=0.0,
            drawdown=0.0,
        )


class SimulatedCryptoExchangeAdapter(_BaseSimulatedAdapter):
    """Simulated crypto exchange adapter with 24/7 trading assumptions."""

    def __init__(self) -> None:
        super().__init__(
            "SIM_CRYPTO",
            [
                Instrument(
                    instrument_id="BTCUSDT",
                    symbol="BTC/USDT",
                    market=MarketType.CRYPTO,
                    instrument_type="perpetual",
                    market_region="GLOBAL",
                    lot_size=0.001,
                    tick_size=0.1,
                    quote_currency="USDT",
                    base_currency="BTC",
                    trading_rules={"trades_24x7": True, "category": "Store of Value"},
                ),
                Instrument(
                    instrument_id="ETHUSDT",
                    symbol="ETH/USDT",
                    market=MarketType.CRYPTO,
                    instrument_type="perpetual",
                    market_region="GLOBAL",
                    lot_size=0.001,
                    tick_size=0.01,
                    quote_currency="USDT",
                    base_currency="ETH",
                    trading_rules={"trades_24x7": True, "category": "Smart Contract"},
                ),
                Instrument(
                    instrument_id="SOLUSDT",
                    symbol="SOL/USDT",
                    market=MarketType.CRYPTO,
                    instrument_type="perpetual",
                    market_region="GLOBAL",
                    lot_size=0.01,
                    tick_size=0.01,
                    quote_currency="USDT",
                    base_currency="SOL",
                    trading_rules={"trades_24x7": True, "category": "High Throughput L1"},
                ),
                Instrument(
                    instrument_id="BNBUSDT",
                    symbol="BNB/USDT",
                    market=MarketType.CRYPTO,
                    instrument_type="perpetual",
                    market_region="GLOBAL",
                    lot_size=0.01,
                    tick_size=0.01,
                    quote_currency="USDT",
                    base_currency="BNB",
                    trading_rules={"trades_24x7": True, "category": "Exchange Ecosystem"},
                ),
                Instrument(
                    instrument_id="DOGEUSDT",
                    symbol="DOGE/USDT",
                    market=MarketType.CRYPTO,
                    instrument_type="perpetual",
                    market_region="GLOBAL",
                    lot_size=1.0,
                    tick_size=0.0001,
                    quote_currency="USDT",
                    base_currency="DOGE",
                    trading_rules={"trades_24x7": True},
                ),
            ],
            base_price={
                "BTCUSDT": 50_000.0,
                "ETHUSDT": 3_200.0,
                "SOLUSDT": 140.0,
                "BNBUSDT": 580.0,
                "DOGEUSDT": 0.18,
            },
        )


class SimulatedFuturesBrokerAdapter(_BaseSimulatedAdapter):
    """Simulated futures adapter with session and expiry metadata."""

    _CN_SESSIONS = (("09:30", "11:30"), ("13:00", "15:00"))
    _US_SESSIONS = (("09:30", "16:00"),)

    def __init__(self) -> None:
        super().__init__(
            "SIM_FUTURES",
            [
                Instrument(
                    instrument_id="IF2503",
                    symbol="IF2503",
                    market=MarketType.FUTURES,
                    instrument_type="future",
                    market_region="CN",
                    lot_size=1,
                    tick_size=0.2,
                    contract_multiplier=300.0,
                    expiry_at=datetime(2025, 3, 21, tzinfo=timezone.utc),
                    trading_sessions=self._CN_SESSIONS,
                    trading_rules={"delivery_buffer_days": 3},
                ),
                Instrument(
                    instrument_id="IF2506",
                    symbol="IF2506",
                    market=MarketType.FUTURES,
                    instrument_type="future",
                    market_region="CN",
                    lot_size=1,
                    tick_size=0.2,
                    contract_multiplier=300.0,
                    expiry_at=datetime(2025, 6, 20, tzinfo=timezone.utc),
                    trading_sessions=self._CN_SESSIONS,
                    trading_rules={"delivery_buffer_days": 3},
                ),
                Instrument(
                    instrument_id="IC2506",
                    symbol="IC2506",
                    market=MarketType.FUTURES,
                    instrument_type="future",
                    market_region="CN",
                    lot_size=1,
                    tick_size=0.2,
                    contract_multiplier=200.0,
                    expiry_at=datetime(2025, 6, 20, tzinfo=timezone.utc),
                    trading_sessions=self._CN_SESSIONS,
                    trading_rules={"delivery_buffer_days": 3},
                ),
                Instrument(
                    instrument_id="IH2506",
                    symbol="IH2506",
                    market=MarketType.FUTURES,
                    instrument_type="future",
                    market_region="CN",
                    lot_size=1,
                    tick_size=0.2,
                    contract_multiplier=300.0,
                    expiry_at=datetime(2025, 6, 20, tzinfo=timezone.utc),
                    trading_sessions=self._CN_SESSIONS,
                    trading_rules={"delivery_buffer_days": 3},
                ),
                Instrument(
                    instrument_id="AU2506",
                    symbol="AU2506",
                    market=MarketType.FUTURES,
                    instrument_type="future",
                    market_region="CN",
                    lot_size=1000,
                    tick_size=0.02,
                    contract_multiplier=1000.0,
                    expiry_at=datetime(2025, 6, 16, tzinfo=timezone.utc),
                    trading_sessions=self._CN_SESSIONS,
                    trading_rules={"delivery_buffer_days": 3},
                ),
                Instrument(
                    instrument_id="CU2506",
                    symbol="CU2506",
                    market=MarketType.FUTURES,
                    instrument_type="future",
                    market_region="CN",
                    lot_size=5,
                    tick_size=10,
                    contract_multiplier=5.0,
                    expiry_at=datetime(2025, 6, 16, tzinfo=timezone.utc),
                    trading_sessions=self._CN_SESSIONS,
                    trading_rules={"delivery_buffer_days": 3},
                ),
                Instrument(
                    instrument_id="RB2510",
                    symbol="RB2510",
                    market=MarketType.FUTURES,
                    instrument_type="future",
                    market_region="CN",
                    lot_size=10,
                    tick_size=1,
                    contract_multiplier=10.0,
                    expiry_at=datetime(2025, 10, 15, tzinfo=timezone.utc),
                    trading_sessions=self._CN_SESSIONS,
                    trading_rules={"delivery_buffer_days": 3},
                ),
                Instrument(
                    instrument_id="ES2506",
                    symbol="ES2506",
                    market=MarketType.FUTURES,
                    instrument_type="future",
                    market_region="US",
                    lot_size=1,
                    tick_size=0.25,
                    contract_multiplier=50.0,
                    expiry_at=datetime(2025, 6, 20, tzinfo=timezone.utc),
                    trading_sessions=self._US_SESSIONS,
                    trading_rules={"delivery_buffer_days": 3},
                ),
                Instrument(
                    instrument_id="NQ2506",
                    symbol="NQ2506",
                    market=MarketType.FUTURES,
                    instrument_type="future",
                    market_region="US",
                    lot_size=1,
                    tick_size=0.25,
                    contract_multiplier=20.0,
                    expiry_at=datetime(2025, 6, 20, tzinfo=timezone.utc),
                    trading_sessions=self._US_SESSIONS,
                    trading_rules={"delivery_buffer_days": 3},
                ),
                Instrument(
                    instrument_id="CL2506",
                    symbol="CL2506",
                    market=MarketType.FUTURES,
                    instrument_type="future",
                    market_region="US",
                    lot_size=1000,
                    tick_size=0.01,
                    contract_multiplier=1000.0,
                    expiry_at=datetime(2025, 6, 20, tzinfo=timezone.utc),
                    trading_sessions=self._US_SESSIONS,
                    trading_rules={"delivery_buffer_days": 3},
                ),
                Instrument(
                    instrument_id="GC2506",
                    symbol="GC2506",
                    market=MarketType.FUTURES,
                    instrument_type="future",
                    market_region="US",
                    lot_size=100,
                    tick_size=0.1,
                    contract_multiplier=100.0,
                    expiry_at=datetime(2025, 6, 27, tzinfo=timezone.utc),
                    trading_sessions=self._US_SESSIONS,
                    trading_rules={"delivery_buffer_days": 3},
                ),
            ],
            base_price={
                "IF2503": 3_500.0,
                "IF2506": 3_800.0,
                "IC2506": 5_200.0,
                "IH2506": 2_600.0,
                "AU2506": 580.0,
                "CU2506": 72_000.0,
                "RB2510": 3_600.0,
                "ES2506": 5_800.0,
                "NQ2506": 20_500.0,
                "CL2506": 72.0,
                "GC2506": 2_400.0,
            },
        )


class SimulatedEquityBrokerAdapter(_BaseSimulatedAdapter):
    """Simulated equity broker adapter for A-share style trading rules."""

    def __init__(self) -> None:
        super().__init__(
            "SIM_EQUITY",
            [
                Instrument(
                    instrument_id="600000.SH",
                    symbol="600000.SH",
                    market=MarketType.STOCK,
                    instrument_type="equity",
                    market_region="CN",
                    lot_size=100,
                    tick_size=0.01,
                    settlement_cycle="T+1",
                    trading_sessions=(("09:30", "11:30"), ("13:00", "15:00")),
                    trading_rules={"board_lot": 100, "allow_extended_hours": False},
                )
            ],
            base_price=10.0,
        )
