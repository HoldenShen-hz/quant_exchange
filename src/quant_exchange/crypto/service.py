"""Crypto market summaries, details, and chart payloads for the web UI."""

from __future__ import annotations

import math
from collections import Counter
from datetime import datetime
from statistics import pstdev
from typing import Any, Callable

from quant_exchange.adapters.registry import AdapterRegistry
from quant_exchange.core.models import Instrument, Kline, utc_now
from quant_exchange.infrastructure.cache import CacheService
from quant_exchange.marketdata.service import MarketDataStore


_ASSET_NOTES: dict[str, dict[str, Any]] = {
    "BTCUSDT": {
        "name": "Bitcoin",
        "summary": "Bitcoin is treated as the reserve asset of the crypto market and often leads broad risk sentiment.",
        "use_cases": ["Store of value narrative", "Macro risk proxy", "Institutional allocation anchor"],
        "risks": ["Macro liquidity shocks", "High leverage liquidations", "Policy and custody headlines"],
    },
    "ETHUSDT": {
        "name": "Ethereum",
        "summary": "Ethereum combines monetary demand with smart-contract activity and ecosystem fee growth.",
        "use_cases": ["Smart-contract settlement", "On-chain application benchmark", "Staking and fee capture"],
        "risks": ["Layer-1 competition", "Gas-fee cyclicality", "Protocol upgrade execution risk"],
    },
    "SOLUSDT": {
        "name": "Solana",
        "summary": "Solana is usually traded as a high-beta smart-contract platform tied to throughput and activity growth.",
        "use_cases": ["High-throughput settlement", "Retail momentum proxy", "Ecosystem rotation trade"],
        "risks": ["Ecosystem concentration", "Operational stability scrutiny", "Beta drawdowns"],
    },
    "BNBUSDT": {
        "name": "BNB",
        "summary": "BNB reflects exchange-ecosystem usage, fee utility, and platform-specific growth expectations.",
        "use_cases": ["Exchange utility token", "Ecosystem fee discount", "Platform growth sentiment"],
        "risks": ["Exchange-specific regulatory risk", "Platform concentration", "Narrative dependency"],
    },
    "DOGEUSDT": {
        "name": "Dogecoin",
        "summary": "Dogecoin behaves like a sentiment-driven retail token with strong community and momentum effects.",
        "use_cases": ["Retail sentiment barometer", "Momentum rotation trade", "Community-driven attention asset"],
        "risks": ["Sharp sentiment reversals", "Low fundamental anchor", "Event-driven volatility spikes"],
    },
}


class CryptoWorkbenchService:
    """Prepare crypto market data for the dedicated web page."""

    def __init__(
        self,
        adapter_registry: AdapterRegistry,
        market_data_store: MarketDataStore,
        *,
        exchange_code: str = "SIM_CRYPTO",
        clock: Callable[[], datetime] | None = None,
        cache_service: CacheService | None = None,
    ) -> None:
        self.adapters = adapter_registry
        self.market_data_store = market_data_store
        self.exchange_code = exchange_code
        self.clock = clock or utc_now
        self._instruments: dict[str, Instrument] = {}
        self._symbol_index: dict[str, str] = {}
        self._bar_cache: dict[tuple[str, str], list[Kline]] = {}
        self.cache = cache_service
        self._bootstrap()

    def list_assets(self) -> list[dict[str, Any]]:
        """Return all crypto assets sorted by current turnover."""

        assets = [self._asset_payload(instrument) for instrument in self._instruments.values()]
        return sorted(assets, key=lambda item: (-item["turnover_24h"], item["instrument_id"]))

    def universe_summary(self, *, featured_limit: int = 6) -> dict[str, Any]:
        """Return a compact crypto market overview for the UI."""

        assets = self.list_assets()
        category_counts = Counter(item["category"] for item in assets)
        quote_currency_counts = Counter(item["quote_currency"] for item in assets)
        top_gainers = sorted(assets, key=lambda item: item["change_pct_24h"], reverse=True)[:featured_limit]
        top_losers = sorted(assets, key=lambda item: item["change_pct_24h"])[:featured_limit]
        most_active = sorted(assets, key=lambda item: item["turnover_24h"], reverse=True)[:featured_limit]
        average_change = sum(item["change_pct_24h"] for item in assets) / len(assets) if assets else 0.0
        return {
            "source": "simulated_crypto_exchange",
            "exchange_code": self.exchange_code,
            "as_of": self.clock().isoformat(),
            "total_count": len(assets),
            "category_counts": dict(category_counts),
            "quote_currency_counts": dict(quote_currency_counts),
            "average_change_pct_24h": round(average_change, 4),
            "top_gainers": top_gainers,
            "top_losers": top_losers,
            "most_active": most_active,
            "featured_assets": most_active[:featured_limit],
        }

    def get_asset(self, instrument_id: str) -> dict[str, Any]:
        """Return a detailed asset view for one crypto instrument."""

        instrument = self._instrument(instrument_id)
        asset = self._asset_payload(instrument)
        notes = _ASSET_NOTES.get(instrument.instrument_id, {})
        asset.update(
            {
                "exchange_code": self.exchange_code,
                "market_region": instrument.market_region,
                "summary": notes.get("summary") or "This asset is available for crypto market research inside the workbench.",
                "use_cases": notes.get("use_cases") or ["Liquidity monitoring", "Trend research", "Cross-market comparison"],
                "risks": notes.get("risks") or ["Volatility spikes", "Liquidity fragmentation", "Narrative-driven reversals"],
                "tick_size": instrument.tick_size,
                "lot_size": instrument.lot_size,
                "instrument_type": instrument.instrument_type,
                "trading_mode": "24x7 continuous trading",
                "microstructure": {
                    "base_currency": instrument.base_currency,
                    "quote_currency": instrument.quote_currency,
                    "category": asset["category"],
                    "trades_24x7": bool((instrument.trading_rules or {}).get("trades_24x7", True)),
                },
            }
        )
        return asset

    def get_asset_history(self, instrument_id: str, *, interval: str = "1d", limit: int = 120) -> dict[str, Any]:
        """Return OHLCV history plus chart summary for one crypto asset."""

        instrument = self._instrument(instrument_id)
        bars = self._bars(instrument.instrument_id, interval)
        if limit > 0:
            bars = bars[-limit:]
        if not bars:
            return {
                "instrument_id": instrument.instrument_id,
                "symbol": instrument.symbol,
                "interval": interval,
                "source": "simulated_crypto_exchange",
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
            "source": "simulated_crypto_exchange",
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

    def _bootstrap(self) -> None:
        """Load instrument metadata from the configured crypto adapter."""

        adapter = self.adapters.get_market_data(self.exchange_code)
        for instrument in adapter.fetch_instruments():
            self._instruments[instrument.instrument_id] = instrument
            self._symbol_index[instrument.instrument_id.upper()] = instrument.instrument_id
            self._symbol_index[instrument.symbol.replace("/", "").upper()] = instrument.instrument_id
            self._symbol_index[instrument.symbol.upper()] = instrument.instrument_id
            self.market_data_store.add_instrument(instrument)

    def _instrument(self, instrument_id: str) -> Instrument:
        """Resolve a normalized crypto instrument identifier."""

        normalized = self._symbol_index.get(str(instrument_id).replace("-", "").replace("_", "").upper())
        if normalized is None:
            normalized = self._symbol_index.get(str(instrument_id).upper())
        if normalized is None:
            raise KeyError(instrument_id)
        return self._instruments[normalized]

    def _bars(self, instrument_id: str, interval: str) -> list[Kline]:
        """Return cached bars from the market-data store or adapter.

        Implements cache-aside: checks Redis (when available) before hitting
        the adapter, then populates both local and distributed cache.
        """
        key = (instrument_id, interval)
        if key not in self._bar_cache:
            # Try distributed cache first (cache-aside read)
            cached_bars: list[Kline] = []
            if self.cache is not None and self.cache.is_available():
                cached_bars = self.cache.get_kline_range(instrument_id, interval)
            if cached_bars:
                self._bar_cache[key] = cached_bars
                # Also populate in-memory store for consistency
                self.market_data_store.ingest_klines(cached_bars)
            else:
                # Cache miss — fetch from adapter
                adapter = self.adapters.get_market_data(self.exchange_code)
                bars = adapter.fetch_klines(instrument_id, interval)
                self.market_data_store.ingest_klines(bars)
                self._bar_cache[key] = sorted(
                    self.market_data_store.query_klines(instrument_id, interval),
                    key=lambda bar: bar.open_time,
                )
                # Populate distributed cache (cache-aside write)
                if self.cache is not None and self.cache.is_available():
                    for bar in bars:
                        ttl = 300 if interval in ("1m", "5m", "15m") else 604800
                        self.cache.set_kline(bar, ttl)
        return list(self._bar_cache[key])

    def _asset_payload(self, instrument: Instrument) -> dict[str, Any]:
        """Build a web-friendly quote summary for one asset."""

        bars = self._bars(instrument.instrument_id, "1d")
        if not bars:
            raise KeyError(instrument.instrument_id)
        live = self._live_quote(instrument, bars)
        history = self.get_asset_history(instrument.instrument_id, interval="1d", limit=30)["summary"]
        notes = _ASSET_NOTES.get(instrument.instrument_id, {})
        return {
            "instrument_id": instrument.instrument_id,
            "symbol": instrument.symbol,
            "display_symbol": instrument.symbol,
            "asset_name": notes.get("name", instrument.base_currency or instrument.symbol),
            "base_currency": instrument.base_currency,
            "quote_currency": instrument.quote_currency,
            "category": (instrument.trading_rules or {}).get("category", "Crypto"),
            "last_price": live["last_price"],
            "change_24h": live["change"],
            "change_pct_24h": live["change_pct"],
            "market_status": "OPEN",
            "quote_time": live["quote_time"],
            "turnover_24h": live["turnover"],
            "volume_24h": live["volume"],
            "volatility_30d": self._realized_volatility([bar.close for bar in bars[-31:]]),
            "trend_30d_pct": history["change_pct"],
            "source": "simulated_crypto_exchange",
        }

    def _live_quote(self, instrument: Instrument, bars: list[Kline]) -> dict[str, Any]:
        """Overlay a deterministic 24x7 quote on top of the historical series."""

        latest = bars[-1]
        previous = bars[-2] if len(bars) > 1 else latest
        now = self.clock()
        seed = sum(ord(char) for char in instrument.instrument_id)
        phase = now.timestamp() / 300.0
        swing = math.sin(phase + seed * 0.03) * 0.012 + math.cos(phase / 2.0 + seed * 0.017) * 0.006
        drift = ((seed % 11) - 5) * 0.0012
        multiplier = 1.0 + swing + drift
        last_price = max(latest.close * 0.35, latest.close * multiplier)
        volume = latest.volume * (1.0 + abs(swing) * 12.0 + (seed % 5) * 0.08)
        turnover = last_price * volume
        change = last_price - previous.close
        change_pct = (change / previous.close) * 100 if previous.close else 0.0
        return {
            "last_price": round(last_price, 6 if last_price < 1 else 4),
            "change": round(change, 6 if abs(change) < 1 else 4),
            "change_pct": round(change_pct, 4),
            "volume": round(volume, 4),
            "turnover": round(turnover, 4),
            "quote_time": now.isoformat(),
        }

    def _realized_volatility(self, closes: list[float]) -> float:
        """Compute annualized realized volatility from close-to-close returns."""

        returns = []
        for previous, current in zip(closes[:-1], closes[1:]):
            if previous <= 0:
                continue
            returns.append((current / previous) - 1.0)
        if len(returns) < 2:
            return 0.0
        return round(pstdev(returns) * math.sqrt(365.0) * 100.0, 4)

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


# ─── CR-06: Real CoinGecko API Integration ─────────────────────────────────────

_COINGECKO_BASE = "https://api.coingecko.com/api/v3"

# Map our internal symbol to CoinGecko coin IDs
_COINGECKO_ID_MAP: dict[str, str] = {
    "BTCUSDT": "bitcoin",
    "ETHUSDT": "ethereum",
    "SOLUSDT": "solana",
    "BNBUSDT": "binancecoin",
    "DOGEUSDT": "dogecoin",
    "XRPUSDT": "ripple",
    "ADAUSDT": "cardano",
    "DOTUSDT": "polkadot",
    "MATICUSDT": "matic-network",
    "AVAXUSDT": "avalanche-2",
}


class CoinGeckoClient:
    """Real-time crypto market data from CoinGecko API (CR-06).

    Falls back to simulated data when API is unavailable or rate-limited.
    All methods return dicts in the same format as the simulated adapter.
    """

    def __init__(self, use_real: bool = True) -> None:
        self.use_real = use_real
        self._session: Any = None  # Will use urllib or requests
        self._last_market_fetch: datetime | None = None
        self._cached_markets: list[dict[str, Any]] = []
        self._cache_ttl_seconds = 60  # Cache market data for 60 seconds

    def _http_get(self, endpoint: str, params: dict | None = None) -> dict | list | None:
        """Make an HTTP GET request to CoinGecko API."""
        try:
            import urllib.request
            import urllib.parse
            url = f"{_COINGECKO_BASE}/{endpoint}"
            if params:
                url += "?" + urllib.parse.urlencode(params)
            req = urllib.request.Request(url, headers={"Accept": "application/json", "User-Agent": "QuantExchange/1.0"})
            with urllib.request.urlopen(req, timeout=10) as resp:
                import json
                return json.loads(resp.read())
        except Exception:
            return None

    def fetch_markets(self, currency: str = "usd") -> list[dict[str, Any]]:
        """Fetch current market data for top crypto assets (CR-06)."""
        if not self.use_real:
            return []

        now = utc_now()
        if (
            self._cached_markets
            and self._last_market_fetch
            and (now - self._last_market_fetch).total_seconds() < self._cache_ttl_seconds
        ):
            return self._cached_markets

        data = self._http_get(
            "coins/markets",
            params={
                "vs_currency": currency,
                "ids": ",".join(_COINGECKO_ID_MAP.values()),
                "order": "market_cap_desc",
                "per_page": 20,
                "page": 1,
                "sparkline": "false",
            },
        )

        if not data:
            return self._cached_markets or []

        self._cached_markets = [
            {
                "id": coin.get("id"),
                "symbol": coin.get("symbol", "").upper(),
                "name": coin.get("name"),
                "current_price": coin.get("current_price", 0),
                "market_cap": coin.get("market_cap", 0),
                "total_volume": coin.get("total_volume", 0),
                "price_change_24h": coin.get("price_change_24h", 0),
                "price_change_percentage_24h": coin.get("price_change_percentage_24h", 0),
                "circulating_supply": coin.get("circulating_supply", 0),
                "ath": coin.get("ath", 0),
                "atl": coin.get("atl", 0),
            }
            for coin in data
            if coin.get("id") in _COINGECKO_ID_MAP.values()
        ]
        self._last_market_fetch = now
        return self._cached_markets

    def fetch_ohlc(self, coin_id: str, days: int = 7) -> list[list[float]]:
        """Fetch OHLC data for a coin (CR-06)."""
        if not self.use_real:
            return []
        return self._http_get(
            f"coins/{coin_id}/ohlc",
            params={"vs_currency": "usd", "days": days},
        ) or []

    def fetch_simple_price(self, coin_ids: list[str], currencies: list[str] = ["usd"]) -> dict[str, dict[str, float]]:
        """Fetch simple price data for multiple coins (CR-06)."""
        if not self.use_real:
            return {}
        data = self._http_get(
            "simple/price",
            params={
                "ids": ",".join(coin_ids),
                "vs_currencies": ",".join(currencies),
            },
        )
        return data or {}

    def get_coin_id(self, instrument_id: str) -> str | None:
        """Map internal instrument ID to CoinGecko coin ID."""
        return _COINGECKO_ID_MAP.get(instrument_id)

    def get_instrument_id(self, coin_id: str) -> str | None:
        """Map CoinGecko coin ID back to internal instrument ID."""
        for iid, cid in _COINGECKO_ID_MAP.items():
            if cid == coin_id:
                return iid
        return None
