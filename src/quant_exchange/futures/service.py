"""Futures market summaries, details, and chart payloads for the web UI.

FT-08: Trading calendar and session periods (day/night sessions)
FT-09: Main contract and continuous contract mapping (rollover logic)
FT-10: Futures simulated trading (order, position, mark-to-market, margin)
FT-11: Unified cross-market portfolio view (futures/stocks/crypto)
"""

from __future__ import annotations

import math
import uuid
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, time
from statistics import pstdev
from typing import Any, Callable

from quant_exchange.adapters.registry import AdapterRegistry
from quant_exchange.core.models import Instrument, Kline, MarketType, utc_now
from quant_exchange.marketdata.service import MarketDataStore


# Trading session definitions for different market regions
_TRADING_SESSIONS: dict[str, list[dict[str, str]]] = {
    "中金所": [
        {"session": "day", "start": "09:30", "end": "11:30", "label": "日盘"},
        {"session": "night", "start": "13:00", "end": "15:00", "label": "日盘"},
    ],
    "上期所": [
        {"session": "day", "start": "09:00", "end": "10:15", "label": "日盘"},
        {"session": "day", "start": "10:30", "end": "11:30", "label": "日盘"},
        {"session": "night", "start": "21:00", "end": "01:00", "label": "夜盘"},
    ],
    "CME": [
        {"session": "electronic", "start": "17:00", "end": "16:00", "label": "电子盘"},
    ],
    "NYMEX": [
        {"session": "electronic", "start": "18:00", "end": "17:00", "label": "电子盘"},
    ],
    "COMEX": [
        {"session": "electronic", "start": "18:00", "end": "17:00", "label": "电子盘"},
    ],
}

# Main contract aliases for continuous contract mapping (FT-09)
_MAIN_CONTRACT_ALIASES: dict[str, str] = {
    "IF": "IF2506",  # CSI 300 main contract
    "IC": "IC2506",  # CSI 500 main contract
    "IH": "IH2506",  # SSE 50 main contract
    "AU": "AU2506",  # Gold main contract
    "CU": "CU2506",  # Copper main contract
    "RB": "RB2510",  # Rebar main contract
    "ES": "ES2506",  # E-mini S&P 500 main contract
    "NQ": "NQ2506",  # E-mini Nasdaq main contract
    "CL": "CL2506",  # WTI Crude main contract
    "GC": "GC2506",  # Gold main contract (COMEX)
}

# Continuous contract mappings (rollover logic)
_CONTINUOUS_CONTRACTS: dict[str, list[str]] = {
    "IF": ["IF2503", "IF2506", "IF2509"],
    "IC": ["IC2503", "IC2506", "IC2509"],
    "IH": ["IH2503", "IH2506", "IH2509"],
    "AU": ["AU2504", "AU2506", "AU2508"],
    "CU": ["CU2504", "CU2506", "CU2508"],
    "RB": ["RB2506", "RB2510", "RB2512"],
    "ES": ["ES2506", "ES2509", "ES2512"],
    "NQ": ["NQ2506", "NQ2509", "NQ2512"],
    "CL": ["CL2506", "CL2507", "CL2508"],
    "GC": ["GC2506", "GC2508", "GC2510"],
}


@dataclass
class FuturesPosition:
    """Represents a futures position for simulated trading (FT-10)."""
    instrument_id: str
    direction: str  # "long" or "short"
    quantity: int
    entry_price: float
    current_price: float
    contract_multiplier: float
    margin_used: float
    unrealized_pnl: float
    realized_pnl: float
    updated_at: datetime = field(default_factory=utc_now)


@dataclass
class FuturesOrder:
    """Represents a futures order for simulated trading (FT-10)."""
    order_id: str
    instrument_id: str
    direction: str
    order_type: str  # "market" or "limit"
    quantity: int
    limit_price: float | None
    filled_quantity: int
    avg_fill_price: float
    status: str  # "submitted", "filled", "partial", "cancelled", "rejected"
    created_at: datetime = field(default_factory=utc_now)
    updated_at: datetime = field(default_factory=utc_now)


@dataclass
class FuturesAccount:
    """Represents a futures trading account for mark-to-market (FT-10)."""
    account_code: str
    initial_equity: float
    current_equity: float
    cash_available: float
    margin_used: float
    positions: dict[str, FuturesPosition] = field(default_factory=dict)
    orders: dict[str, FuturesOrder] = field(default_factory=dict)
    daily_pnl: float = 0.0
    daily_realized_pnl: float = 0.0


class FuturesTradingService:
    """Futures simulated trading service (FT-10)."""

    def __init__(self) -> None:
        self._accounts: dict[str, FuturesAccount] = {}
        self._positions: dict[str, FuturesPosition] = {}
        self._orders: dict[str, FuturesOrder] = {}

    def get_or_create_account(self, account_code: str, initial_equity: float = 1000000.0) -> FuturesAccount:
        """Get or create a futures trading account."""
        if account_code not in self._accounts:
            self._accounts[account_code] = FuturesAccount(
                account_code=account_code,
                initial_equity=initial_equity,
                current_equity=initial_equity,
                cash_available=initial_equity,
                margin_used=0.0,
            )
        return self._accounts[account_code]

    def submit_order(
        self,
        account_code: str,
        instrument_id: str,
        direction: str,
        quantity: int,
        order_type: str = "market",
        limit_price: float | None = None,
        contract_multiplier: float = 1.0,
    ) -> FuturesOrder:
        """Submit a futures order (FT-10)."""
        account = self.get_or_create_account(account_code)

        # Calculate margin required
        price = limit_price if order_type == "limit" else self._get_current_price(instrument_id)
        margin_required = price * quantity * contract_multiplier * 0.12  # 12% initial margin

        if margin_required > account.cash_available:
            order = FuturesOrder(
                order_id=str(uuid.uuid4())[:8],
                instrument_id=instrument_id,
                direction=direction,
                order_type=order_type,
                quantity=quantity,
                limit_price=limit_price,
                filled_quantity=0,
                avg_fill_price=0.0,
                status="rejected",
            )
            account.orders[order.order_id] = order
            return order

        order = FuturesOrder(
            order_id=str(uuid.uuid4())[:8],
            instrument_id=instrument_id,
            direction=direction,
            order_type=order_type,
            quantity=quantity,
            limit_price=limit_price,
            filled_quantity=quantity,
            avg_fill_price=price,
            status="filled",
        )

        account.orders[order.order_id] = order
        account.cash_available -= margin_required
        account.margin_used += margin_required

        # Update or create position
        self._update_position(account, instrument_id, direction, quantity, price, contract_multiplier)

        return order

    def _update_position(
        self,
        account: FuturesAccount,
        instrument_id: str,
        direction: str,
        quantity: int,
        price: float,
        contract_multiplier: float,
    ) -> None:
        """Update futures position after order fill (FT-10)."""
        pos_key = f"{instrument_id}_{direction}"
        if pos_key in account.positions:
            pos = account.positions[pos_key]
            total_qty = pos.quantity + quantity
            pos.entry_price = (pos.entry_price * pos.quantity + price * quantity) / total_qty
            pos.quantity = total_qty
            pos.current_price = price
            pos.unrealized_pnl = self._calculate_pnl(pos, price)
            pos.updated_at = utc_now()
        else:
            pos = FuturesPosition(
                instrument_id=instrument_id,
                direction=direction,
                quantity=quantity,
                entry_price=price,
                current_price=price,
                contract_multiplier=contract_multiplier,
                margin_used=price * quantity * contract_multiplier * 0.12,
                unrealized_pnl=0.0,
                realized_pnl=0.0,
            )
            account.positions[pos_key] = pos

        self._positions[pos_key] = account.positions[pos_key]

    def _calculate_pnl(self, pos: FuturesPosition, current_price: float) -> float:
        """Calculate unrealized PnL for a position (FT-10)."""
        if pos.direction == "long":
            return (current_price - pos.entry_price) * pos.quantity * pos.contract_multiplier
        else:
            return (pos.entry_price - current_price) * pos.quantity * pos.contract_multiplier

    def _get_current_price(self, instrument_id: str) -> float:
        """Get current price for a futures contract (simplified simulation)."""
        return 4000.0  # Simplified - would use market data store

    def mark_to_market(self, account_code: str, instrument_id: str, current_price: float) -> dict[str, Any]:
        """Mark positions to market price and update account equity (FT-10)."""
        account = self.get_or_create_account(account_code)
        total_unrealized = 0.0

        for pos_key, pos in list(account.positions.items()):
            if instrument_id in pos_key:
                old_pnl = pos.unrealized_pnl
                pos.current_price = current_price
                pos.unrealized_pnl = self._calculate_pnl(pos, current_price)
                delta = pos.unrealized_pnl - old_pnl
                total_unrealized += delta
                pos.updated_at = utc_now()

        account.current_equity = account.initial_equity + account.daily_realized_pnl + total_unrealized
        return {
            "account_code": account_code,
            "current_equity": account.current_equity,
            "daily_pnl": total_unrealized,
            "margin_used": account.margin_used,
        }

    def get_positions(self, account_code: str) -> list[dict[str, Any]]:
        """Get all positions for an account (FT-10)."""
        account = self.get_or_create_account(account_code)
        return [
            {
                "instrument_id": pos.instrument_id,
                "direction": pos.direction,
                "quantity": pos.quantity,
                "entry_price": pos.entry_price,
                "current_price": pos.current_price,
                "unrealized_pnl": pos.unrealized_pnl,
                "realized_pnl": pos.realized_pnl,
                "margin_used": pos.margin_used,
            }
            for pos in account.positions.values()
        ]

    def get_dashboard(self, account_code: str) -> dict[str, Any]:
        """Get futures trading dashboard summary (FT-10)."""
        account = self.get_or_create_account(account_code)
        positions = self.get_positions(account_code)
        total_unrealized = sum(p["unrealized_pnl"] for p in positions)
        total_realized = sum(p["realized_pnl"] for p in positions)
        return {
            "account_code": account_code,
            "initial_equity": account.initial_equity,
            "current_equity": account.current_equity,
            "cash_available": account.cash_available,
            "margin_used": account.margin_used,
            "daily_pnl": total_unrealized,
            "total_realized_pnl": total_realized,
            "position_count": len(positions),
            "positions": positions,
        }


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

    # ─── FT-08: Trading Calendar and Session Periods ────────────────────────────────

    def trading_calendar(self) -> dict[str, Any]:
        """Return futures trading calendar with session periods (FT-08).

        Returns trading sessions for each market region including day/night sessions.
        """
        return {
            "source": "simulated_futures_exchange",
            "as_of": self.clock().isoformat(),
            "markets": {
                market_label: {
                    "label": market_label,
                    "sessions": sessions,
                }
                for market_label, sessions in _TRADING_SESSIONS.items()
            },
            "session_definitions": {
                "day": {"description": "日盘 (Day Session)", "typical_hours": "09:00-15:00 CST"},
                "night": {"description": "夜盘 (Night Session)", "typical_hours": "21:00-02:00 CST"},
                "electronic": {"description": "电子盘 (Electronic Session)", "typical_hours": "CME: 17:00-16:00 EST"},
            },
            "settlement_windows": [
                {"market": "中金所", "time": "15:00-15:15 CST", "description": "当日结算"},
                {"market": "上期所", "time": "15:00-15:30 CST", "description": "当日结算"},
                {"market": "CME", "time": "16:00 EST", "description": "每日结算"},
            ],
        }

    def get_trading_sessions(self, instrument_id: str) -> dict[str, Any]:
        """Return trading sessions for a specific contract (FT-08)."""
        instrument = self._instrument(instrument_id)
        notes = _CONTRACT_NOTES.get(instrument.instrument_id, {})
        market_label = notes.get("market_label", "其他")
        sessions = _TRADING_SESSIONS.get(market_label, [])
        return {
            "instrument_id": instrument_id,
            "market_label": market_label,
            "sessions": sessions,
            "trading_hours": self._format_trading_hours(sessions),
        }

    def _format_trading_hours(self, sessions: list[dict[str, str]]) -> str:
        """Format trading sessions into readable string."""
        if not sessions:
            return "24/7"
        return ", ".join(f"{s['start']}-{s['end']} ({s['label']})" for s in sessions)

    # ─── FT-09: Main Contract and Continuous Contract Mapping ──────────────────────

    def get_main_contract(self, product_code: str) -> dict[str, Any]:
        """Return the main (front-month) contract for a product (FT-09).

        Args:
            product_code: Product code like "IF", "AU", "CL"
        """
        main_id = _MAIN_CONTRACT_ALIASES.get(product_code.upper())
        if not main_id:
            raise KeyError(f"No main contract for product: {product_code}")
        return self.get_contract(main_id)

    def get_continuous_contract(self, product_code: str) -> dict[str, Any]:
        """Return continuous contract chain for a product (FT-09).

        Returns the list of all contracts in the chain for rollover analysis.
        """
        chain = _CONTINUOUS_CONTRACTS.get(product_code.upper())
        if not chain:
            raise KeyError(f"No continuous contract chain for: {product_code}")
        return {
            "product_code": product_code.upper(),
            "main_contract": _MAIN_CONTRACT_ALIASES.get(product_code.upper()),
            "chain": [
                {
                    "instrument_id": contract_id,
                    "display_name": _CONTRACT_NOTES.get(contract_id, {}).get("name", contract_id),
                    "expiry": self._instruments[contract_id].expiry_at.isoformat() if contract_id in self._instruments and self._instruments[contract_id].expiry_at else None,
                }
                for contract_id in chain
            ],
        }

    def get_rollover_recommendation(self, product_code: str) -> dict[str, Any]:
        """Provide rollover recommendation based on position and expiry (FT-09)."""
        chain_data = self.get_continuous_contract(product_code)
        main_contract = chain_data["main_contract"]
        if main_contract and main_contract in self._instruments:
            inst = self._instruments[main_contract]
            days_to_expiry = (inst.expiry_at - self.clock()).days if inst.expiry_at else 999
            return {
                "product_code": product_code.upper(),
                "recommended_action": "roll" if days_to_expiry < 7 else "hold",
                "days_to_expiry": days_to_expiry,
                "target_contract": chain_data["chain"][0]["instrument_id"] if chain_data["chain"] else None,
                "rationale": "Near expiry - consider rolling to next contract" if days_to_expiry < 7 else "Sufficient time remaining",
            }
        return {
            "product_code": product_code.upper(),
            "recommended_action": "unknown",
            "days_to_expiry": None,
            "target_contract": None,
            "rationale": "Contract data not available",
        }

    # ─── FT-11: Unified Cross-Market Portfolio View ─────────────────────────────────

    def unified_portfolio_summary(
        self,
        stock_positions: list[dict[str, Any]] | None = None,
        crypto_positions: list[dict[str, Any]] | None = None,
        futures_positions: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """Return unified portfolio view across stocks, crypto, and futures (FT-11).

        Aggregates positions and risk metrics across all asset classes.
        """
        stock_positions = stock_positions or []
        crypto_positions = crypto_positions or []
        futures_positions = futures_positions or []

        total_equity = 0.0
        total_exposure = 0.0
        by_asset_class = {}

        # Aggregate stock positions
        stock_exposure = sum(p.get("market_value", 0) for p in stock_positions)
        stock_pnl = sum(p.get("unrealized_pnl", 0) for p in stock_positions)
        by_asset_class["stocks"] = {
            "position_count": len(stock_positions),
            "total_exposure": stock_exposure,
            "total_pnl": stock_pnl,
            "positions": stock_positions,
        }
        total_exposure += stock_exposure

        # Aggregate crypto positions
        crypto_exposure = sum(p.get("market_value", 0) for p in crypto_positions)
        crypto_pnl = sum(p.get("unrealized_pnl", 0) for p in crypto_positions)
        by_asset_class["crypto"] = {
            "position_count": len(crypto_positions),
            "total_exposure": crypto_exposure,
            "total_pnl": crypto_pnl,
            "positions": crypto_positions,
        }
        total_exposure += crypto_exposure

        # Aggregate futures positions
        futures_exposure = sum(abs(p.get("notional_value", 0)) for p in futures_positions)
        futures_pnl = sum(p.get("unrealized_pnl", 0) for p in futures_positions)
        futures_margin = sum(p.get("margin_used", 0) for p in futures_positions)
        by_asset_class["futures"] = {
            "position_count": len(futures_positions),
            "total_exposure": futures_exposure,
            "total_pnl": futures_pnl,
            "margin_used": futures_margin,
            "positions": futures_positions,
        }
        total_exposure += futures_exposure

        return {
            "source": "unified_portfolio",
            "as_of": self.clock().isoformat(),
            "total_exposure": total_exposure,
            "total_pnl": stock_pnl + crypto_pnl + futures_pnl,
            "asset_class_breakdown": by_asset_class,
            "cross_market_risk": {
                "leverage_ratio": round(futures_margin / max(total_exposure, 1), 4),
                "concentration_risk": self._calculate_concentration_risk(stock_positions, crypto_positions, futures_positions),
            },
        }

    def _calculate_concentration_risk(
        self,
        stock_positions: list[dict[str, Any]],
        crypto_positions: list[dict[str, Any]],
        futures_positions: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """Calculate concentration risk metrics across asset classes (FT-11)."""
        all_positions = len(stock_positions) + len(crypto_positions) + len(futures_positions)
        if all_positions == 0:
            return {"level": "none", "top_holdings": []}

        # Find largest positions by exposure
        all_vals = []
        for p in stock_positions:
            all_vals.append((p.get("instrument_id", ""), abs(p.get("market_value", 0)), "stock"))
        for p in crypto_positions:
            all_vals.append((p.get("instrument_id", ""), abs(p.get("market_value", 0)), "crypto"))
        for p in futures_positions:
            all_vals.append((p.get("instrument_id", ""), abs(p.get("notional_value", 0)), "futures"))

        all_vals.sort(key=lambda x: x[1], reverse=True)
        top_5 = all_vals[:5]

        largest_exposure = all_vals[0][1] if all_vals else 0
        total_exposure = sum(v[1] for v in all_vals)

        return {
            "level": "high" if (largest_exposure / max(total_exposure, 1)) > 0.3 else "medium" if (largest_exposure / max(total_exposure, 1)) > 0.15 else "low",
            "top_holdings": [{"instrument_id": v[0], "exposure": v[1], "asset_class": v[2]} for v in top_5],
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
