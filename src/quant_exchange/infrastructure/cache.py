"""Redis-backed distributed cache with graceful in-memory fallback.

Provides a cache-aside layer for market data (klines, instruments, quotes).
Designed to be optional — when Redis is unavailable, falls back to the
built-in in-memory store without disrupting the platform.

Architecture (per quant_trading_system_architecture_design.md):
  - Real-time cache only for acceleration, NOT as source of truth
  - Event channels via Redis Pub/Sub for real-time data distribution
  - Short-lived idempotency keys for order deduplication
"""

from __future__ import annotations

import json
import logging
import threading
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

from quant_exchange.core.models import Kline, utc_now

logger = logging.getLogger(__name__)


# ─── TTL Constants ────────────────────────────────────────────────────────────

_INSTRUMENT_TTL_SECONDS = 86_400  # 24 h — instruments rarely change
_KLINE_HISTORICAL_TTL_SECONDS = 604_800  # 7 days for daily bars
_KLINE_RECENT_TTL_SECONDS = 300  # 5 min for recent bars
_TICK_TTL_SECONDS = 3_600  # 1 hour
_ORDERBOOK_TTL_SECONDS = 30  # 30 seconds
_FUNDING_RATE_TTL_SECONDS = 28_800  # 8 hours
_LATEST_PRICE_TTL_SECONDS = 60  # 1 minute


# ─── Cache Key Utilities ──────────────────────────────────────────────────────

def _kline_key(instrument_id: str, timeframe: str, open_time: datetime) -> str:
    return f"mds:kline:{instrument_id}:{timeframe}:{open_time.isoformat()}"


def _kline_index_key(instrument_id: str, timeframe: str) -> str:
    return f"mds:kline:idx:{instrument_id}:{timeframe}"


def _instrument_key(instrument_id: str) -> str:
    return f"mds:instrument:{instrument_id}"


def _instrument_list_key() -> str:
    return "mds:instrument:list"


def _latest_price_key(instrument_id: str) -> str:
    return f"mds:price:latest:{instrument_id}"


def _orderbook_key(instrument_id: str) -> str:
    return f"mds:ob:{instrument_id}"


def _funding_rate_key(instrument_id: str) -> str:
    return f"mds:funding:{instrument_id}"


# ─── Serialization Helpers ─────────────────────────────────────────────────────

def _kline_to_dict(kline: Kline) -> dict[str, Any]:
    return {
        "instrument_id": kline.instrument_id,
        "timeframe": kline.timeframe,
        "open_time": kline.open_time.isoformat() if isinstance(kline.open_time, datetime) else kline.open_time,
        "close_time": kline.close_time.isoformat() if isinstance(kline.close_time, datetime) else kline.close_time,
        "open": kline.open,
        "high": kline.high,
        "low": kline.low,
        "close": kline.close,
        "volume": kline.volume,
        "turnover": kline.turnover,
    }


def _dict_to_kline(d: dict[str, Any]) -> Kline:
    from quant_exchange.core.models import Kline as KlineModel
    open_time = d["open_time"]
    close_time = d["close_time"]
    if isinstance(open_time, str):
        open_time = datetime.fromisoformat(open_time)
    if isinstance(close_time, str):
        close_time = datetime.fromisoformat(close_time)
    return KlineModel(
        instrument_id=d["instrument_id"],
        timeframe=d["timeframe"],
        open_time=open_time,
        close_time=close_time,
        open=d["open"],
        high=d["high"],
        low=d["low"],
        close=d["close"],
        volume=d["volume"],
        turnover=d.get("turnover", 0.0),
    )


# ─── Cache Service Interface ──────────────────────────────────────────────────

class CacheService(ABC):
    """Abstract cache interface for market data caching."""

    @abstractmethod
    def is_available(self) -> bool:
        """Return True if the cache backend is reachable."""

    @abstractmethod
    def get_kline(self, instrument_id: str, timeframe: str, open_time: datetime) -> Kline | None:
        """Fetch a single kline from cache."""

    @abstractmethod
    def set_kline(self, kline: Kline, ttl_seconds: int = _KLINE_RECENT_TTL_SECONDS) -> None:
        """Store a single kline in cache."""

    @abstractmethod
    def get_kline_range(
        self,
        instrument_id: str,
        timeframe: str,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int | None = None,
    ) -> list[Kline]:
        """Fetch a range of klines from cache (cache-aside query)."""

    @abstractmethod
    def set_kline_range(self, klines: list[Kline], ttl_seconds: int = _KLINE_RECENT_TTL_SECONDS) -> None:
        """Store multiple klines in cache."""

    @abstractmethod
    def get_instrument(self, instrument_id: str) -> dict[str, Any] | None:
        """Fetch instrument metadata from cache."""

    @abstractmethod
    def set_instrument(self, instrument_data: dict[str, Any]) -> None:
        """Store instrument metadata in cache."""

    @abstractmethod
    def get_latest_price(self, instrument_id: str) -> float | None:
        """Fetch latest price for an instrument."""

    @abstractmethod
    def set_latest_price(self, instrument_id: str, price: float) -> None:
        """Store latest price for an instrument."""

    @abstractmethod
    def invalidate_instrument(self, instrument_id: str) -> None:
        """Remove instrument from cache."""

    @abstractmethod
    def ping(self) -> bool:
        """Health check — return True if cache backend is reachable."""


# ─── In-Memory Cache (Always Available) ─────────────────────────────────────

class InMemoryCacheService(CacheService):
    """Thread-safe in-memory cache used as fallback when Redis is unavailable.

    Data is stored in plain Python dicts with naive TTL tracking.
    Suitable for single-instance deployments or as fallback layer.
    """

    def __init__(self) -> None:
        self._klines: dict[str, Kline] = {}
        self._kline_indexes: dict[str, list[str]] = {}  # key → list of kline keys
        self._instruments: dict[str, dict[str, Any]] = {}
        self._latest_prices: dict[str, float] = {}
        self._expiry: dict[str, datetime] = {}
        self._lock = threading.RLock()
        self._hits = 0
        self._misses = 0

    # ── CacheService implementation ──────────────────────────────────────────

    def is_available(self) -> bool:
        return True

    def get_kline(self, instrument_id: str, timeframe: str, open_time: datetime) -> Kline | None:
        key = _kline_key(instrument_id, timeframe, open_time)
        with self._lock:
            if key in self._klines and not self._is_expired(key):
                self._hits += 1
                return self._klines[key]
            self._misses += 1
            return None

    def set_kline(self, kline: Kline, ttl_seconds: int = _KLINE_RECENT_TTL_SECONDS) -> None:
        key = _kline_key(kline.instrument_id, kline.timeframe, kline.open_time)
        idx_key = _kline_index_key(kline.instrument_id, kline.timeframe)
        with self._lock:
            self._klines[key] = kline
            if idx_key not in self._kline_indexes:
                self._kline_indexes[idx_key] = []
            if key not in self._kline_indexes[idx_key]:
                self._kline_indexes[idx_key].append(key)
            self._set_expiry(key, ttl_seconds)

    def get_kline_range(
        self,
        instrument_id: str,
        timeframe: str,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int | None = None,
    ) -> list[Kline]:
        idx_key = _kline_index_key(instrument_id, timeframe)
        with self._lock:
            keys = list(self._kline_indexes.get(idx_key, []))
        result: list[Kline] = []
        for key in keys:
            with self._lock:
                kline = self._klines.get(key)
            if kline is None:
                continue
            if self._is_expired(key):
                continue
            if start is not None and kline.open_time < start:
                continue
            if end is not None and kline.close_time > end:
                continue
            result.append(kline)
        result.sort(key=lambda k: k.open_time)
        if limit:
            result = result[-limit:]
        return result

    def set_kline_range(self, klines: list[Kline], ttl_seconds: int = _KLINE_RECENT_TTL_SECONDS) -> None:
        for kline in klines:
            self.set_kline(kline, ttl_seconds)

    def get_instrument(self, instrument_id: str) -> dict[str, Any] | None:
        key = _instrument_key(instrument_id)
        with self._lock:
            if key in self._instruments and not self._is_expired(key):
                self._hits += 1
                return self._instruments[key]
            self._misses += 1
            return None

    def set_instrument(self, instrument_data: dict[str, Any]) -> None:
        key = _instrument_key(instrument_data["instrument_id"])
        with self._lock:
            self._instruments[key] = instrument_data
            self._set_expiry(key, _INSTRUMENT_TTL_SECONDS)

    def get_latest_price(self, instrument_id: str) -> float | None:
        key = _latest_price_key(instrument_id)
        with self._lock:
            if key in self._latest_prices and not self._is_expired(key):
                self._hits += 1
                return self._latest_prices[key]
            self._misses += 1
            return None

    def set_latest_price(self, instrument_id: str, price: float) -> None:
        key = _latest_price_key(instrument_id)
        with self._lock:
            self._latest_prices[key] = price
            self._set_expiry(key, _LATEST_PRICE_TTL_SECONDS)

    def invalidate_instrument(self, instrument_id: str) -> None:
        key = _instrument_key(instrument_id)
        with self._lock:
            self._instruments.pop(key, None)
            self._expiry.pop(key, None)

    def ping(self) -> bool:
        return True

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _is_expired(self, key: str) -> bool:
        exp = self._expiry.get(key)
        if exp is None:
            return False
        return utc_now() > exp

    def _set_expiry(self, key: str, ttl_seconds: int) -> None:
        self._expiry[key] = utc_now() + timedelta(seconds=ttl_seconds)

    def stats(self) -> dict[str, int]:
        """Return cache hit/miss statistics."""
        with self._lock:
            return {"hits": self._hits, "misses": self._misses, "keys": len(self._klines)}


# ─── Redis Cache (Network Distributed) ───────────────────────────────────────

class RedisCacheService(CacheService):
    """Redis-backed distributed cache.

    Implements the full CacheService interface using Redis as the backend.
    Falls back to a local InMemoryCacheService when Redis is unavailable.

    Key design decisions:
    - All values stored as JSON strings
    - klines stored individually with TTL + indexed in a sorted set by timestamp
    - instruments stored individually with TTL
    - prices stored with short TTL
    - Graceful degradation: never raises — always returns the fallback value

    Environment variables:
      REDIS_URL   — full redis:// URL (default: redis://localhost:6379/0)
      REDIS_TTL_OVERRIDE — multiply all TTLs (for testing)
    """

    def __init__(
        self,
        redis_url: str | None = None,
        socket_timeout: float = 2.0,
        socket_connect_timeout: float = 2.0,
    ) -> None:
        import os
        self._redis_url = redis_url or os.environ.get("REDIS_URL", "redis://localhost:6379/0")
        self._socket_timeout = socket_timeout
        self._socket_connect_timeout = socket_connect_timeout
        self._redis_ttl_multiplier = float(os.environ.get("REDIS_TTL_OVERRIDE", "1.0"))

        self._redis: Any = None  # Set lazily
        self._connected = False
        self._fallback = InMemoryCacheService()
        self._connect_lock = threading.Lock()
        self._ensure_connection()

    # ── Connection Management ─────────────────────────────────────────────────

    def _ensure_connection(self) -> None:
        if self._connected:
            return
        with self._connect_lock:
            if self._connected:
                return
            try:
                import redis
            except ImportError:
                logger.debug("redis-py not installed — using in-memory fallback")
                self._connected = False
                return

            try:
                self._redis = redis.from_url(
                    self._redis_url,
                    socket_timeout=self._socket_timeout,
                    socket_connect_timeout=self._socket_connect_timeout,
                    decode_responses=True,
                )
                self._redis.ping()
                self._connected = True
                logger.info("Redis cache connected at %s", self._redis_url)
            except Exception as exc:
                logger.warning("Redis unavailable at %s — using in-memory fallback (%s)", self._redis_url, exc)
                self._connected = False

    def _reconnect(self) -> None:
        """Attempt to reconnect to Redis (called on operation failure)."""
        with self._connect_lock:
            self._connected = False
            self._ensure_connection()

    def _ttls(self, seconds: int) -> int:
        return int(seconds * self._redis_ttl_multiplier)

    # ── CacheService implementation (delegates to fallback when unavailable) ──

    def is_available(self) -> bool:
        self._ensure_connection()
        return self._connected

    def ping(self) -> bool:
        if not self._connected:
            return False
        try:
            self._redis.ping()
            return True
        except Exception:
            self._reconnect()
            return False

    def get_kline(self, instrument_id: str, timeframe: str, open_time: datetime) -> Kline | None:
        if not self._connected:
            return self._fallback.get_kline(instrument_id, timeframe, open_time)

        try:
            key = _kline_key(instrument_id, timeframe, open_time)
            data = self._redis.get(key)
            if data is None:
                return self._fallback.get_kline(instrument_id, timeframe, open_time)
            return _dict_to_kline(json.loads(data))
        except Exception:
            self._reconnect()
            return self._fallback.get_kline(instrument_id, timeframe, open_time)

    def set_kline(self, kline: Kline, ttl_seconds: int = _KLINE_RECENT_TTL_SECONDS) -> None:
        self._fallback.set_kline(kline, ttl_seconds)
        if not self._connected:
            return

        try:
            key = _kline_key(kline.instrument_id, kline.timeframe, kline.open_time)
            idx_key = _kline_index_key(kline.instrument_id, kline.timeframe)
            data = json.dumps(_kline_to_dict(kline))
            self._redis.setex(key, self._ttls(ttl_seconds), data)
            # Add to sorted set indexed by open_time timestamp for range queries
            score = kline.open_time.timestamp()
            self._redis.zadd(idx_key, {key: score})
            # Set TTL on the index too
            self._redis.expire(idx_key, self._ttls(max(ttl_seconds, _KLINE_HISTORICAL_TTL_SECONDS)))
        except Exception:
            self._reconnect()

    def get_kline_range(
        self,
        instrument_id: str,
        timeframe: str,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int | None = None,
    ) -> list[Kline]:
        if not self._connected:
            return self._fallback.get_kline_range(instrument_id, timeframe, start, end, limit)

        try:
            idx_key = _kline_index_key(instrument_id, timeframe)
            min_score = start.timestamp() if start else "-inf"
            max_score = end.timestamp() if end else "+inf"
            keys = self._redis.zrangebyscore(idx_key, min_score, max_score, start=0, num=limit or 10000)
            result: list[Kline] = []
            for key in keys:
                data = self._redis.get(key)
                if data:
                    result.append(_dict_to_kline(json.loads(data)))
            return result
        except Exception:
            self._reconnect()
            return self._fallback.get_kline_range(instrument_id, timeframe, start, end, limit)

    def set_kline_range(self, klines: list[Kline], ttl_seconds: int = _KLINE_RECENT_TTL_SECONDS) -> None:
        for kline in klines:
            self.set_kline(kline, ttl_seconds)

    def get_instrument(self, instrument_id: str) -> dict[str, Any] | None:
        if not self._connected:
            return self._fallback.get_instrument(instrument_id)

        try:
            key = _instrument_key(instrument_id)
            data = self._redis.get(key)
            if data is None:
                return self._fallback.get_instrument(instrument_id)
            return json.loads(data)
        except Exception:
            self._reconnect()
            return self._fallback.get_instrument(instrument_id)

    def set_instrument(self, instrument_data: dict[str, Any]) -> None:
        self._fallback.set_instrument(instrument_data)
        if not self._connected:
            return

        try:
            key = _instrument_key(instrument_data["instrument_id"])
            self._redis.setex(key, self._ttls(_INSTRUMENT_TTL_SECONDS), json.dumps(instrument_data))
        except Exception:
            self._reconnect()

    def get_latest_price(self, instrument_id: str) -> float | None:
        if not self._connected:
            return self._fallback.get_latest_price(instrument_id)

        try:
            key = _latest_price_key(instrument_id)
            data = self._redis.get(key)
            if data is None:
                return self._fallback.get_latest_price(instrument_id)
            return float(data)
        except Exception:
            self._reconnect()
            return self._fallback.get_latest_price(instrument_id)

    def set_latest_price(self, instrument_id: str, price: float) -> None:
        self._fallback.set_latest_price(instrument_id, price)
        if not self._connected:
            return

        try:
            key = _latest_price_key(instrument_id)
            self._redis.setex(key, self._ttls(_LATEST_PRICE_TTL_SECONDS), str(price))
        except Exception:
            self._reconnect()

    def invalidate_instrument(self, instrument_id: str) -> None:
        self._fallback.invalidate_instrument(instrument_id)
        if not self._connected:
            return

        try:
            key = _instrument_key(instrument_id)
            self._redis.delete(key)
        except Exception:
            self._reconnect()
