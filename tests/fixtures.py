from __future__ import annotations

from datetime import datetime, timedelta, timezone

from quant_exchange.core.models import Instrument, Kline, MarketDocument, MarketType


def sample_instrument() -> Instrument:
    return Instrument(
        instrument_id="BTCUSDT",
        symbol="BTC/USDT",
        market=MarketType.CRYPTO,
        tick_size=0.1,
        lot_size=0.001,
        contract_multiplier=1.0,
        quote_currency="USDT",
        base_currency="BTC",
    )


def sample_klines(prices: list[float] | None = None) -> list[Kline]:
    prices = prices or [100.0, 102.0, 104.0, 107.0, 109.0, 112.0, 115.0]
    start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    bars = []
    for idx, close in enumerate(prices):
        open_price = prices[idx - 1] if idx > 0 else prices[0]
        high = max(open_price, close) + 1.0
        low = min(open_price, close) - 1.0
        open_time = start + timedelta(days=idx)
        close_time = open_time + timedelta(hours=23, minutes=59)
        bars.append(
            Kline(
                instrument_id="BTCUSDT",
                timeframe="1d",
                open_time=open_time,
                close_time=close_time,
                open=open_price,
                high=high,
                low=low,
                close=close,
                volume=1000.0 + idx * 10,
            )
        )
    return bars


def sample_documents() -> list[MarketDocument]:
    base_time = datetime(2025, 1, 5, tzinfo=timezone.utc)
    return [
        MarketDocument(
            document_id="doc_1",
            source="newswire",
            instrument_id="BTCUSDT",
            published_at=base_time,
            title="BTC breakout and strong inflow",
            content="Analysts see bullish growth and strong breakout momentum.",
        ),
        MarketDocument(
            document_id="doc_2",
            source="social",
            instrument_id="BTCUSDT",
            published_at=base_time + timedelta(hours=4),
            title="社区看多",
            content="市场情绪利好，价格上涨，资金持续流入。",
        ),
    ]
