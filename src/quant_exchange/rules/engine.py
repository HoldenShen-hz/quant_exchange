"""Market rule validators for equity, futures, and crypto trading flows."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from quant_exchange.core.models import Instrument, MarketType, OrderRequest, OrderSide


@dataclass(slots=True, frozen=True)
class MarketRuleDecision:
    """Result of market-structure validation before an order reaches risk or execution."""

    approved: bool
    reasons: tuple[str, ...] = ()


class MarketRuleEngine:
    """Dispatch market-specific validations for equity, futures, and crypto instruments."""

    MARKET_TIMEZONES = {
        "CN": "Asia/Shanghai",
        "HK": "Asia/Hong_Kong",
        "US": "America/New_York",
    }

    def validate_order(
        self,
        instrument: Instrument,
        request: OrderRequest,
        *,
        as_of: datetime,
        available_position_qty: float = 0.0,
    ) -> MarketRuleDecision:
        """Validate an order against market-specific rules."""

        if instrument.market == MarketType.STOCK:
            return self._validate_stock(instrument, request, as_of, available_position_qty)
        if instrument.market == MarketType.FUTURES:
            return self._validate_futures(instrument, request, as_of)
        return self._validate_crypto(instrument, request)

    def _validate_stock(
        self,
        instrument: Instrument,
        request: OrderRequest,
        as_of: datetime,
        available_position_qty: float,
    ) -> MarketRuleDecision:
        reasons: list[str] = []
        board_lot = int(instrument.trading_rules.get("board_lot", instrument.lot_size))
        if board_lot > 0 and round(request.quantity) % board_lot != 0:
            reasons.append("board_lot_violation")
        if not self._is_within_session(as_of, instrument.trading_sessions, instrument.market_region):
            reasons.append("outside_trading_session")
        if (
            instrument.settlement_cycle == "T+1"
            and request.side == OrderSide.SELL
            and request.quantity > available_position_qty
        ):
            reasons.append("t_plus_one_sell_violation")
        if request.side == OrderSide.SELL and available_position_qty <= 0 and not instrument.short_sellable:
            reasons.append("short_sell_not_allowed")
        return MarketRuleDecision(not reasons, tuple(reasons))

    def _validate_futures(
        self,
        instrument: Instrument,
        request: OrderRequest,
        as_of: datetime,
    ) -> MarketRuleDecision:
        reasons: list[str] = []
        if not self._is_within_session(as_of, instrument.trading_sessions, instrument.market_region):
            reasons.append("outside_trading_session")
        if instrument.expiry_at is not None:
            if as_of >= instrument.expiry_at:
                reasons.append("contract_expired")
            buffer_days = int(instrument.trading_rules.get("delivery_buffer_days", 0))
            if request.side == OrderSide.BUY and as_of >= instrument.expiry_at - timedelta(days=buffer_days):
                reasons.append("opening_near_expiry_forbidden")
        return MarketRuleDecision(not reasons, tuple(reasons))

    def _validate_crypto(self, instrument: Instrument, request: OrderRequest) -> MarketRuleDecision:
        reasons: list[str] = []
        if instrument.lot_size > 0 and request.quantity < instrument.lot_size:
            reasons.append("min_lot_violation")
        return MarketRuleDecision(not reasons, tuple(reasons))

    def _is_within_session(
        self,
        as_of: datetime,
        sessions: tuple[tuple[str, str], ...],
        market_region: str,
    ) -> bool:
        """Return whether the timestamp falls into one of the configured trading sessions."""

        if not sessions:
            return True
        clock = self._localize_timestamp(as_of, market_region).strftime("%H:%M")
        return any(start <= clock <= end for start, end in sessions)

    def _localize_timestamp(self, as_of: datetime, market_region: str) -> datetime:
        """Normalize timestamps into the instrument's market-local timezone before session checks."""

        if as_of.tzinfo is None:
            return as_of
        timezone_name = self.MARKET_TIMEZONES.get(market_region, "UTC")
        return as_of.astimezone(ZoneInfo(timezone_name))
