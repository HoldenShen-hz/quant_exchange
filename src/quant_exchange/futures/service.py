"""Futures market summaries, details, and chart payloads for the web UI."""

from __future__ import annotations

import math
from collections import Counter
from datetime import datetime
from statistics import pstdev
from typing import Any, Callable

from quant_exchange.adapters.registry import AdapterRegistry
from quant_exchange.core.models import Instrument, Kline, utc_now
from quant_exchange.marketdata.service import MarketDataStore


_CONTRACT_NOTES: dict[str, dict[str, Any]] = {
    "IF2503": {"name": "沪深300股指", "summary": "跟踪沪深300指数的股指期货，是A股市场最重要的衍生品之一。", "category": "股指", "market_label": "中金所"},
    "IF2506": {"name": "沪深300股指", "summary": "沪深300指数期货2506合约，用于对冲A股大盘风险或进行指数方向性交易。", "category": "股指", "market_label": "中金所"},
    "IC2506": {"name": "中证500股指", "summary": "中证500指数期货，跟踪中小盘股票表现，波动率通常高于沪深300。", "category": "股指", "market_label": "中金所"},
    "IH2506": {"name": "上证50股指", "summary": "上证50指数期货，跟踪大盘蓝筹股，与银行、保险等金融股高度相关。", "category": "股指", "market_label": "中金所"},
    "AU2506": {"name": "沪金", "summary": "上海期货交易所黄金期货，受全球宏观环境和美元指数影响显著。", "category": "贵金属", "market_label": "上期所"},
    "CU2506": {"name": "沪铜", "summary": "上海期货交易所铜期货，是全球工业金属的风向标。", "category": "有色金属", "market_label": "上期所"},
    "RB2510": {"name": "螺纹钢", "summary": "上海期货交易所螺纹钢期货，与房地产和基建投资密切相关。", "category": "黑色系", "market_label": "上期所"},
    "ES2506": {"name": "E-mini S&P 500", "summary": "CME标普500迷你合约，全球流动性最高的股指期货之一。", "category": "股指", "market_label": "CME"},
    "NQ2506": {"name": "E-mini Nasdaq 100", "summary": "CME纳指迷你合约，跟踪科技股为主的纳斯达克100指数。", "category": "股指", "market_label": "CME"},
    "CL2506": {"name": "WTI原油", "summary": "NYMEX WTI原油期货，全球最重要的能源定价基准之一。", "category": "能源", "market_label": "NYMEX"},
    "GC2506": {"name": "COMEX黄金", "summary": "COMEX黄金期货，国际金价的主要定价合约。", "category": "贵金属", "market_label": "COMEX"},
}


class FuturesWorkbenchService:
    """Prepare futures market data for the dedicated web page."""

    def __init__(
        self,
        adapter_registry: AdapterRegistry,
        market_data_store: MarketDataStore,
        *,
        exchange_code: str = "SIM_FUTURES",
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.adapters = adapter_registry
        self.market_data_store = market_data_store
        self.exchange_code = exchange_code
        self.clock = clock or utc_now
        self._instruments: dict[str, Instrument] = {}
        self._symbol_index: dict[str, str] = {}
        self._bar_cache: dict[tuple[str, str], list[Kline]] = {}
        self._bootstrap()

    def list_contracts(self) -> list[dict[str, Any]]:
        """Return all futures contracts sorted by current turnover."""

        assets = [self._contract_payload(inst) for inst in self._instruments.values()]
        return sorted(assets, key=lambda item: (-item["turnover_24h"], item["instrument_id"]))

    def universe_summary(self, *, featured_limit: int = 6) -> dict[str, Any]:
        """Return a compact futures market overview for the UI."""

        contracts = self.list_contracts()
        category_counts = Counter(item["category"] for item in contracts)
        market_counts = Counter(item.get("market_label", "其他") for item in contracts)
        top_gainers = sorted(contracts, key=lambda item: item["change_pct"], reverse=True)[:featured_limit]
        top_losers = sorted(contracts, key=lambda item: item["change_pct"])[:featured_limit]
        most_active = sorted(contracts, key=lambda item: item["turnover_24h"], reverse=True)[:featured_limit]
        average_change = sum(item["change_pct"] for item in contracts) / len(contracts) if contracts else 0.0
        return {
            "source": "simulated_futures_exchange",
            "exchange_code": self.exchange_code,
            "as_of": self.clock().isoformat(),
            "total_count": len(contracts),
            "category_counts": dict(category_counts),
            "market_counts": dict(market_counts),
            "average_change_pct": round(average_change, 4),
            "top_gainers": top_gainers,
            "top_losers": top_losers,
            "most_active": most_active,
            "featured_contracts": most_active[:featured_limit],
        }

    def get_contract(self, instrument_id: str) -> dict[str, Any]:
        """Return a detailed contract view for one futures instrument."""

        instrument = self._instrument(instrument_id)
        contract = self._contract_payload(instrument)
        notes = _CONTRACT_NOTES.get(instrument.instrument_id, {})
        contract.update({
            "exchange_code": self.exchange_code,
            "market_region": instrument.market_region,
            "summary": notes.get("summary", "期货合约研究视图。"),
            "contract_multiplier": getattr(instrument, "contract_multiplier", 1),
            "expiry_at": instrument.expiry_at.isoformat() if getattr(instrument, "expiry_at", None) else None,
            "trading_sessions": getattr(instrument, "trading_sessions", None),
            "tick_size": instrument.tick_size,
            "lot_size": instrument.lot_size,
            "instrument_type": instrument.instrument_type,
            "margin_info": {
                "initial_margin_pct": 12.0,
                "maintenance_margin_pct": 8.0,
            },
        })
        return contract

    def get_contract_history(self, instrument_id: str, *, interval: str = "1d", limit: int = 120) -> dict[str, Any]:
        """Return OHLCV history plus chart summary for one futures contract."""

        instrument = self._instrument(instrument_id)
        bars = self._bars(instrument.instrument_id, interval)
        if limit > 0:
            bars = bars[-limit:]
        if not bars:
            return {
                "instrument_id": instrument.instrument_id,
                "symbol": instrument.symbol,
                "interval": interval,
                "source": "simulated_futures_exchange",
                "bars": [],
                "summary": {
                    "latest_close": None,
                    "change_pct": 0.0,
                    "period_high": None,
                    "period_low": None,
                    "average_volume": 0.0,
                },
            }
        latest = bars[-1]
        start = bars[0]
        closes = [bar.close for bar in bars]
        volumes = [bar.volume for bar in bars]
        previous = bars[-2].close if len(bars) > 1 else start.open
        return {
            "instrument_id": instrument.instrument_id,
            "symbol": instrument.symbol,
            "interval": interval,
            "source": "simulated_futures_exchange",
            "bars": [self._serialize_bar(bar) for bar in bars],
            "summary": {
                "latest_close": latest.close,
                "previous_close": previous,
                "change_pct": round(((latest.close - start.open) / start.open) * 100, 4) if start.open else 0.0,
                "period_high": max(closes),
                "period_low": min(closes),
                "average_volume": round(sum(volumes) / len(volumes), 4),
            },
        }

    # --- internal methods ---

    def _bootstrap(self) -> None:
        """Load instrument metadata from the configured futures adapter."""

        adapter = self.adapters.get_market_data(self.exchange_code)
        for instrument in adapter.fetch_instruments():
            self._instruments[instrument.instrument_id] = instrument
            self._symbol_index[instrument.instrument_id.upper()] = instrument.instrument_id
            self._symbol_index[instrument.symbol.upper()] = instrument.instrument_id
            self.market_data_store.add_instrument(instrument)

    def _instrument(self, instrument_id: str) -> Instrument:
        """Resolve a normalized futures instrument identifier."""

        normalized = self._symbol_index.get(str(instrument_id).upper())
        if normalized is None:
            raise KeyError(instrument_id)
        return self._instruments[normalized]

    def _bars(self, instrument_id: str, interval: str) -> list[Kline]:
        """Return cached bars from the market-data store or adapter."""

        key = (instrument_id, interval)
        if key not in self._bar_cache:
            adapter = self.adapters.get_market_data(self.exchange_code)
            bars = adapter.fetch_klines(instrument_id, interval)
            self.market_data_store.ingest_klines(bars)
            self._bar_cache[key] = sorted(
                self.market_data_store.query_klines(instrument_id, interval),
                key=lambda bar: bar.open_time,
            )
        return list(self._bar_cache[key])

    def _contract_payload(self, instrument: Instrument) -> dict[str, Any]:
        """Build a web-friendly quote summary for one contract."""

        bars = self._bars(instrument.instrument_id, "1d")
        if not bars:
            raise KeyError(instrument.instrument_id)
        live = self._live_quote(instrument, bars)
        notes = _CONTRACT_NOTES.get(instrument.instrument_id, {})
        return {
            "instrument_id": instrument.instrument_id,
            "symbol": instrument.symbol,
            "display_name": notes.get("name", instrument.symbol),
            "category": notes.get("category", "期货"),
            "market_label": notes.get("market_label", "交易所"),
            "last_price": live["last_price"],
            "change": live["change"],
            "change_pct": live["change_pct"],
            "market_status": "OPEN",
            "quote_time": live["quote_time"],
            "turnover_24h": live["turnover"],
            "volume_24h": live["volume"],
            "contract_multiplier": getattr(instrument, "contract_multiplier", 1),
            "expiry_at": instrument.expiry_at.isoformat() if getattr(instrument, "expiry_at", None) else None,
            "volatility_30d": self._realized_volatility([bar.close for bar in bars[-31:]]),
            "source": "simulated_futures_exchange",
        }

    def _live_quote(self, instrument: Instrument, bars: list[Kline]) -> dict[str, Any]:
        """Overlay a deterministic quote on top of the historical series."""

        latest = bars[-1]
        previous = bars[-2] if len(bars) > 1 else latest
        now = self.clock()
        seed = sum(ord(char) for char in instrument.instrument_id)
        phase = now.timestamp() / 300.0
        swing = math.sin(phase + seed * 0.03) * 0.008 + math.cos(phase / 2.0 + seed * 0.017) * 0.004
        drift = ((seed % 11) - 5) * 0.0008
        multiplier = 1.0 + swing + drift
        last_price = max(latest.close * 0.5, latest.close * multiplier)
        volume = latest.volume * (1.0 + abs(swing) * 8.0 + (seed % 5) * 0.06)
        turnover = last_price * volume * getattr(instrument, "contract_multiplier", 1)
        change = last_price - previous.close
        change_pct = (change / previous.close) * 100 if previous.close else 0.0
        return {
            "last_price": round(last_price, 2),
            "change": round(change, 2),
            "change_pct": round(change_pct, 4),
            "volume": round(volume, 0),
            "turnover": round(turnover, 0),
            "quote_time": now.isoformat(),
        }

    def _realized_volatility(self, closes: list[float]) -> float:
        """Compute annualized realized volatility from close-to-close returns."""

        returns = []
        for prev, curr in zip(closes[:-1], closes[1:]):
            if prev <= 0:
                continue
            returns.append((curr / prev) - 1.0)
        if len(returns) < 2:
            return 0.0
        return round(pstdev(returns) * math.sqrt(252.0) * 100.0, 4)

    def _serialize_bar(self, bar: Kline) -> dict[str, Any]:
        """Convert one kline into the JSON shape used by the chart renderer."""

        return {
            "trade_date": bar.open_time.date().isoformat(),
            "open": bar.open,
            "high": bar.high,
            "low": bar.low,
            "close": bar.close,
            "volume": bar.volume,
        }
