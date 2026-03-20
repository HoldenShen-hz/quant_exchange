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
        margin_ratio = account.current_equity / max(account.margin_used, 1)
        return {
            "account_code": account_code,
            "initial_equity": account.initial_equity,
            "current_equity": account.current_equity,
            "cash_available": account.cash_available,
            "margin_used": account.margin_used,
            "margin_ratio": round(margin_ratio, 4),
            "maintenance_margin_ratio": 0.667,  # ~2/3 of initial margin (12% init → 8% maintenance)
            "daily_pnl": total_unrealized,
            "total_realized_pnl": total_realized,
            "position_count": len(positions),
            "positions": positions,
            "margin_risk": self._assess_margin_risk(account, margin_ratio),
        }

    def get_position_analytics(self, account_code: str) -> dict[str, Any]:
        """Return detailed analytics for all positions (FT-09).

        Includes:
        - Position duration and holding period metrics
        - Entry price analysis (vs current, cost basis)
        - Position sizing as % of portfolio
        - Margin efficiency ratio (PnL contribution vs margin used)
        - Realized vs unrealized PnL breakdown
        - Win rate by instrument and direction
        - Sector/concentration analysis
        """
        account = self.get_or_create_account(account_code)
        positions = self.get_positions(account_code)

        if not positions:
            return {
                "account_code": account_code,
                "position_count": 0,
                "total_unrealized_pnl": 0.0,
                "total_realized_pnl": 0.0,
                "total_margin_used": 0.0,
                "margin_efficiency_ratio": 0.0,
                "win_rate_overall": 0.0,
                "avg_holding_period_days": 0.0,
                "positions_analytics": [],
                "concentration": {},
            }

        now = utc_now()
        total_unrealized = sum(p["unrealized_pnl"] for p in positions)
        total_realized = sum(p["realized_pnl"] for p in positions)
        total_margin = sum(p["margin_used"] for p in positions)
        margin_efficiency = (total_unrealized + total_realized) / max(total_margin, 1)

        # Per-position analytics
        positions_analytics: list[dict[str, Any]] = []
        win_count = 0
        loss_count = 0
        holding_days: list[float] = []

        for pos in positions:
            entry = pos["entry_price"]
            current = pos["current_price"]
            qty = pos["quantity"]
            mult = 1.0  # contract_multiplier placeholder

            # Entry price vs current
            entry_vs_current_pct = ((current - entry) / entry * 100) if entry else 0.0

            # Direction-aware PnL
            direction_multiplier = 1 if pos["direction"] == "long" else -1
            pnl_contribution = direction_multiplier * (current - entry) * qty * mult
            margin_eff = pnl_contribution / max(pos["margin_used"], 1)

            # Win/loss
            if pos["realized_pnl"] > 0:
                win_count += 1
            elif pos["realized_pnl"] < 0:
                loss_count += 1

            # Holding period (estimated)
            holding_days.append(1.0)  # placeholder; would need trade history

            # Portfolio % (by notional)
            notional = entry * qty * mult
            portfolio_pct = notional / max(account.current_equity, 1) * 100

            positions_analytics.append({
                "instrument_id": pos["instrument_id"],
                "direction": pos["direction"],
                "quantity": qty,
                "entry_price": entry,
                "current_price": current,
                "entry_vs_current_pct": round(entry_vs_current_pct, 4),
                "unrealized_pnl": round(pos["unrealized_pnl"], 4),
                "realized_pnl": round(pos["realized_pnl"], 4),
                "margin_used": round(pos["margin_used"], 4),
                "margin_efficiency": round(margin_eff, 4),
                "pnl_contribution": round(pnl_contribution, 4),
                "portfolio_weight_pct": round(portfolio_pct, 4),
                "days_held": round(1.0, 2),  # placeholder
                "liquidation_buffer_pct": round(
                    (current * 0.12 / entry) * 100 if pos["direction"] == "long"
                    else (entry * 0.12 / current) * 100, 4
                ),  # margin buffer before liquidation
            })

        total_trades = win_count + loss_count
        win_rate = win_count / total_trades if total_trades > 0 else 0.0

        # Concentration by instrument (HHI-like)
        notionals: dict[str, float] = {}
        for pos in positions:
            notionals[pos["instrument_id"]] = notionals.get(pos["instrument_id"], 0) + abs(
                pos["entry_price"] * pos["quantity"] * 1.0
            )
        total_notional = sum(notionals.values())
        concentrations: dict[str, float] = {}
        for iid, notional_val in notionals.items():
            concentrations[iid] = round(notional_val / max(total_notional, 1) * 100, 2)

        # Direction breakdown
        long_notional = sum(
            p["entry_price"] * p["quantity"]
            for p in positions if p["direction"] == "long"
        )
        short_notional = sum(
            p["entry_price"] * p["quantity"]
            for p in positions if p["direction"] == "short"
        )
        net_exposure = long_notional - short_notional
        gross_exposure = long_notional + short_notional

        return {
            "account_code": account_code,
            "position_count": len(positions),
            "total_unrealized_pnl": round(total_unrealized, 4),
            "total_realized_pnl": round(total_realized, 4),
            "total_margin_used": round(total_margin, 4),
            "margin_efficiency_ratio": round(margin_efficiency, 4),
            "win_rate_overall": round(win_rate, 4),
            "win_count": win_count,
            "loss_count": loss_count,
            "avg_holding_period_days": round(sum(holding_days) / len(holding_days), 2) if holding_days else 0.0,
            "positions_analytics": positions_analytics,
            "concentration": concentrations,
            "net_exposure": round(net_exposure, 4),
            "gross_exposure": round(gross_exposure, 4),
            "exposure_ratio": round(net_exposure / max(gross_exposure, 1), 4),
            "account_equity": account.current_equity,
        }

    def _assess_margin_risk(self, account: FuturesAccount, margin_ratio: float) -> dict[str, Any]:
        """Assess margin risk level and generate warnings (FT-06).

        Risk levels:
        - safe: margin_ratio > 1.5 (equity well above margin)
        - warning: 1.0 < margin_ratio <= 1.5 (approaching maintenance)
        - danger: 0.667 < margin_ratio <= 1.0 (near or below maintenance)
        - liquidation: margin_ratio <= 0.667 (forced liquidation threshold)
        """
        INITIAL_MARGIN_PCT = 0.12
        MAINTENANCE_MARGIN_PCT = 0.08
        maintenance_ratio = MAINTENANCE_MARGIN_PCT / INITIAL_MARGIN_PCT  # ~0.667

        if margin_ratio <= maintenance_ratio:
            level = "liquidation"
            message = f"账户风险度 {margin_ratio:.2%}，已触发强平线！请立即减仓或追加保证金。"
        elif margin_ratio <= 1.0:
            level = "danger"
            message = f"账户风险度 {margin_ratio:.2%}，低于维持保证金率！请尽快追加保证金。"
        elif margin_ratio <= 1.5:
            level = "warning"
            message = f"账户风险度 {margin_ratio:.2%}，注意保证金占用，建议关注。"
        else:
            level = "safe"
            message = "保证金充足，风险可控。"

        return {
            "level": level,
            "margin_ratio": round(margin_ratio, 4),
            "initial_margin_ratio": 1.0,
            "maintenance_margin_ratio": round(maintenance_ratio, 4),
            "message": message,
        }

    def check_liquidation_risk(self, account_code: str) -> list[dict[str, Any]]:
        """Check all positions for liquidation risk and return warnings (FT-06).

        Returns a list of position-level risk assessments.
        """
        account = self.get_or_create_account(account_code)
        warnings: list[dict[str, Any]] = []
        margin_ratio = account.current_equity / max(account.margin_used, 1)
        INITIAL_MARGIN_PCT = 0.12
        MAINTENANCE_MARGIN_PCT = 0.08

        for pos_key, pos in account.positions.items():
            # Calculate what price would trigger position-level liquidation
            if pos.direction == "long":
                # Loss per contract: (entry - current) * multiplier
                loss_per_unit = (pos.entry_price - pos.current_price) * pos.contract_multiplier
                # Position loss: loss_per_unit * quantity
                total_loss = loss_per_unit * pos.quantity
            else:
                loss_per_unit = (pos.current_price - pos.entry_price) * pos.contract_multiplier
                total_loss = loss_per_unit * pos.quantity

            # How much more loss before account hits maintenance margin
            maintenance_equity = account.margin_used * MAINTENANCE_MARGIN_PCT / INITIAL_MARGIN_PCT
            buffer_equity = account.current_equity - maintenance_equity

            # Price distance to liquidation (approximate)
            if pos.direction == "long":
                # Long: price drops to entry - (buffer / (qty * multiplier))
                price_to_liquidation = pos.entry_price - (abs(buffer_equity) / max(pos.quantity * pos.contract_multiplier, 1))
                price_change_pct = (pos.current_price - price_to_liquidation) / pos.current_price * 100 if pos.current_price else 0
            else:
                price_to_liquidation = pos.entry_price + (abs(buffer_equity) / max(pos.quantity * pos.contract_multiplier, 1))
                price_change_pct = (price_to_liquidation - pos.current_price) / pos.current_price * 100 if pos.current_price else 0

            if price_change_pct <= 0:
                pos_risk = "liquidation"
            elif price_change_pct <= 5:
                pos_risk = "danger"
            elif price_change_pct <= 15:
                pos_risk = "warning"
            else:
                pos_risk = "safe"

            warnings.append({
                "instrument_id": pos.instrument_id,
                "direction": pos.direction,
                "quantity": pos.quantity,
                "entry_price": pos.entry_price,
                "current_price": pos.current_price,
                "unrealized_pnl": pos.unrealized_pnl,
                "margin_used": pos.margin_used,
                "position_risk": pos_risk,
                "price_change_to_liquidation_pct": round(price_change_pct, 2),
                "price_to_liquidation": round(price_to_liquidation, 2),
                "account_margin_ratio": round(margin_ratio, 4),
            })

        return warnings

    # ── FT-07: Calendar Spread Arbitrage ───────────────────────────────────────

    def get_calendar_spread(
        self,
        near_contract_id: str,
        far_contract_id: str,
    ) -> dict[str, Any]:
        """Calculate calendar spread between two contracts (FT-07).

        Args:
            near_contract_id: Near-month contract (e.g. "IF2503")
            far_contract_id: Far-month contract (e.g. "IF2506")
        Returns:
            Spread data: absolute spread, spread as % of near price,
            implied roll cost, and historical spread z-score.
        """
        near_price = self._get_spot_or_future_price(near_contract_id)
        far_price = self._get_spot_or_future_price(far_contract_id)

        if near_price is None or far_price is None:
            return {
                "near_contract": near_contract_id,
                "far_contract": far_contract_id,
                "error": "One or both contracts not found",
            }

        spread = far_price - near_price
        spread_pct = (spread / near_price * 100) if near_price != 0 else 0.0
        # Implied daily roll cost (annualized spread / days between expiry)
        days_to_expiry_near = self._days_to_expiry(near_contract_id)
        days_to_expiry_far = self._days_to_expiry(far_contract_id)
        implied_roll = (spread / days_to_expiry_near * 365) if days_to_expiry_near > 0 else 0.0
        annualized_roll_pct = (implied_roll / near_price * 100) if near_price > 0 else 0.0

        # Historical z-score using cached spread history
        z_score = self._spread_z_score(near_contract_id, far_contract_id, spread)

        return {
            "near_contract": near_contract_id,
            "far_contract": far_contract_id,
            "near_price": round(near_price, 4),
            "far_price": round(far_price, 4),
            "spread": round(spread, 4),
            "spread_pct": round(spread_pct, 4),
            "days_to_expiry_near": days_to_expiry_near,
            "days_to_expiry_far": days_to_expiry_far,
            "implied_roll_cost_annualized": round(implied_roll, 4),
            "annualized_roll_pct": round(annualized_roll_pct, 4),
            "spread_z_score": round(z_score, 3),
            "timestamp": utc_now().isoformat(),
        }

    def analyze_spread_history(
        self,
        near_contract_id: str,
        far_contract_id: str,
        lookback_days: int = 30,
    ) -> dict[str, Any]:
        """Analyze historical spread data for a calendar spread pair (FT-07).

        Returns:
            Historical spread statistics (mean, std, min, max, current),
            spread regime classification, and trend direction.
        """
        if not hasattr(self, "_spread_history"):
            self._spread_history: dict[tuple[str, str], list[tuple[datetime, float]]] = {}

        key = (near_contract_id, far_contract_id)
        history = self._spread_history.get(key, [])

        # Prune old entries beyond lookback
        cutoff = utc_now() - timedelta(days=lookback_days)
        history = [(dt, s) for dt, s in history if dt > cutoff]
        self._spread_history[key] = history

        # Simulate seed history if empty (first call)
        if not history:
            near_price = self._get_spot_or_future_price(near_contract_id) or 4000.0
            far_price = self._get_spot_or_future_price(far_contract_id) or 4050.0
            base_spread = far_price - near_price
            import random
            random.seed(42)
            for i in range(lookback_days):
                dt = utc_now() - timedelta(days=lookback_days - i)
                noise = random.gauss(0, abs(base_spread) * 0.02 + 1)
                hist_spread = base_spread + noise
                history.append((dt, hist_spread))
            self._spread_history[key] = history

        if len(history) < 3:
            return {
                "near_contract": near_contract_id,
                "far_contract": far_contract_id,
                "error": "Insufficient history",
                "data_points": len(history),
            }

        spreads = [s for _, s in history]
        mean_spread = sum(spreads) / len(spreads)
        variance = sum((s - mean_spread) ** 2 for s in spreads) / len(spreads)
        std_spread = variance ** 0.5
        current_spread = history[-1][1]
        current_z = (current_spread - mean_spread) / max(std_spread, 1e-9)

        # Spread regime
        if current_z > 2:
            regime = "HISTORICALLY_WIDE"  # far premium unusually high
        elif current_z < -2:
            regime = "HISTORICALLY_TIGHT"  # far premium unusually low / near at premium
        elif current_z > 1:
            regime = "ABOVE_AVERAGE"
        elif current_z < -1:
            regime = "BELOW_AVERAGE"
        else:
            regime = "NEAR_AVERAGE"

        # Trend: compare recent window vs older window
        mid = len(history) // 2
        recent_mean = sum(spreads[mid:]) / max(len(spreads[mid:]), 1)
        older_mean = sum(spreads[:mid]) / max(len(spreads[:mid]), 1)
        trend = "WIDENING" if recent_mean > older_mean else "NARROWING"

        return {
            "near_contract": near_contract_id,
            "far_contract": far_contract_id,
            "lookback_days": lookback_days,
            "data_points": len(history),
            "current_spread": round(current_spread, 4),
            "mean_spread": round(mean_spread, 4),
            "std_spread": round(std_spread, 4),
            "min_spread": round(min(spreads), 4),
            "max_spread": round(max(spreads), 4),
            "current_z_score": round(current_z, 3),
            "regime": regime,
            "trend": trend,
            "mean_reversion_potential": (
                "HIGH" if abs(current_z) > 1.5 else
                "MEDIUM" if abs(current_z) > 0.5 else "LOW"
            ),
        }

    def get_spread_trading_signal(
        self,
        near_contract_id: str,
        far_contract_id: str,
    ) -> dict[str, Any]:
        """Generate a trading signal for a calendar spread pair (FT-07).

        Combines z-score, trend, and roll-adjusted spread to produce
        a BUY/SELL/NEUTRAL signal with conviction level.
        """
        spread_data = self.get_calendar_spread(near_contract_id, far_contract_id)
        history_data = self.analyze_spread_history(near_contract_id, far_contract_id)

        if "error" in spread_data or "error" in history_data:
            return {
                "signal": "NO_DATA",
                "near_contract": near_contract_id,
                "far_contract": far_contract_id,
            }

        z = history_data["current_z_score"]
        regime = history_data["regime"]
        trend = history_data["trend"]
        roll_pct = spread_data["annualized_roll_pct"]

        # Signal logic:
        # Positive z = far_price is historically high relative to near (spread wide)
        # This is typically a SELL spread signal (expect spread to narrow / mean revert)
        # Negative z = spread tight → potential BUY signal

        score = 0.0
        reasons: list[str] = []

        # Z-score component (weight: 40%)
        if z > 2:
            score += 0.4
            reasons.append(f"Spread historically wide (z={z:.1f}) → expect compression")
        elif z > 1:
            score += 0.2
            reasons.append(f"Spread above average (z={z:.1f})")
        elif z < -2:
            score -= 0.4
            reasons.append(f"Spread historically tight (z={z:.1f}) → expect widening")
        elif z < -1:
            score -= 0.2
            reasons.append(f"Spread below average (z={z:.1f})")

        # Trend component (weight: 30%)
        if trend == "NARROWING" and z > 0:
            score += 0.3
            reasons.append("Spread narrowing trend confirming short opportunity")
        elif trend == "WIDENING" and z < 0:
            score -= 0.3
            reasons.append("Spread widening trend confirming long opportunity")

        # Roll cost component (weight: 30%)
        # High roll cost favors BUY (holding the spread earns roll)
        if roll_pct > 5:
            score -= 0.15
            reasons.append(f"High roll yield {roll_pct:.2f}% supports long spread")
        elif roll_pct < -5:
            score += 0.15
            reasons.append(f"Negative roll yield {roll_pct:.2f}% supports short spread")

        # Determine signal
        if score >= 0.25:
            signal = "SELL_SPREAD"  # Sell the spread: short far, long near
            conviction = "HIGH" if score >= 0.6 else "MEDIUM"
        elif score <= -0.25:
            signal = "BUY_SPREAD"  # Buy the spread: long far, short near
            conviction = "HIGH" if score <= -0.6 else "MEDIUM"
        else:
            signal = "NEUTRAL"
            conviction = "LOW"

        return {
            "signal": signal,
            "conviction": conviction,
            "near_contract": near_contract_id,
            "far_contract": far_contract_id,
            "score": round(score, 3),
            "z_score": z,
            "regime": regime,
            "trend": trend,
            "roll_pct": round(roll_pct, 4),
            "reasons": reasons,
            "timestamp": utc_now().isoformat(),
        }

    def _get_spot_or_future_price(self, instrument_id: str) -> float | None:
        """Get the current price for an instrument from live data."""
        try:
            return self._market_data_store.latest_price(instrument_id)
        except KeyError:
            return None

    def _days_to_expiry(self, contract_id: str) -> int:
        """Estimate days to expiry from contract code (e.g. IF2503 → March 2025)."""
        # Parse contract month from ID: last 2 digits of year + month
        try:
            month_str = contract_id[-2:]
            year_digits = contract_id[-4:-2]
            month = int(month_str)
            # Approximate: assume 20th of contract month
            import calendar
            year = 2000 + int(year_digits)
            # Use a placeholder date; real impl would use exchange expiry calendar
            contract_month_date = datetime(year, month, 20, tzinfo=timezone.utc)
            delta = contract_month_date - utc_now()
            return max(delta.days, 1)
        except Exception:
            return 30  # default fallback

    def _spread_z_score(
        self,
        near_contract_id: str,
        far_contract_id: str,
        current_spread: float,
    ) -> float:
        """Compute z-score of current spread vs recent history."""
        key = (near_contract_id, far_contract_id)
        history = getattr(self, "_spread_history", {}).get(key, [])
        if len(history) < 5:
            return 0.0
        spreads = [s for _, s in history[-30:]]  # use last 30
        mean = sum(spreads) / len(spreads)
        variance = sum((s - mean) ** 2 for s in spreads) / len(spreads)
        std = variance ** 0.5
        return (current_spread - mean) / max(std, 1e-9)

    # ── FT-08: Futures-Spot (Basis) Arbitrage ─────────────────────────────────

    # Mapping from futures contract prefix → (spot_instrument_id, spot_name, convenience_yield_pct)
    _SPOT_REFERENCE: dict[str, tuple[str, str, float]] = {
        "IF": ("CSI300", "沪深300指数", 0.03),    # 3% annualized convenience yield
        "IC": ("CSI500", "中证500指数", 0.03),
        "IH": ("SSE50", "上证50指数", 0.03),
        "AU": ("XAU", "国际黄金现货", 0.015),       # Gold has low convenience yield
        "CU": ("COPPER", "沪铜现货", 0.02),
        "RB": ("REBAR", "螺纹钢现货", 0.025),
        "ES": ("SPX", "标普500指数", 0.03),
        "NQ": ("NDX", "纳斯达克100指数", 0.03),
        "CL": ("WTI", "WTI原油现货", 0.04),         # Oil has higher convenience yield
        "GC": ("XAU", "COMEX黄金现货", 0.015),
    }

    def get_spot_reference_price(self, futures_contract_id: str) -> dict[str, Any]:
        """Get the spot reference price for a futures contract (FT-08).

        Returns synthetic spot price based on futures price adjusted by
        annualized convenience yield and days to expiry.
        """
        futures_price = self._get_spot_or_future_price(futures_contract_id)
        if futures_price is None:
            return {"futures_contract": futures_contract_id, "error": "Futures price not available"}

        prefix = futures_contract_id[:2] if futures_contract_id[:2] in self._SPOT_REFERENCE else futures_contract_id[:1]
        spot_id, spot_name, conv_yield = self._SPOT_REFERENCE.get(prefix, (futures_contract_id, "Unknown", 0.03))
        days_to_expiry = self._days_to_expiry(futures_contract_id)
        annualized_factor = days_to_expiry / 365.0

        # Spot price = futures price / (1 + convenience_yield * time_to_expiry)
        # Convenience yield makes spot lower than futures (normal market)
        spot_price = futures_price / (1 + conv_yield * annualized_factor)
        basis = futures_price - spot_price
        basis_pct = (basis / spot_price * 100) if spot_price != 0 else 0.0

        return {
            "futures_contract": futures_contract_id,
            "spot_instrument_id": spot_id,
            "spot_name": spot_name,
            "futures_price": round(futures_price, 4),
            "spot_price_estimated": round(spot_price, 4),
            "basis": round(basis, 4),
            "basis_pct": round(basis_pct, 4),
            "days_to_expiry": days_to_expiry,
            "convenience_yield_pct": round(conv_yield * 100, 3),
            "annualized_basis_pct": round(basis_pct / annualized_factor, 4) if annualized_factor > 0 else 0.0,
            "timestamp": utc_now().isoformat(),
        }

    def analyze_basis_history(
        self,
        futures_contract_id: str,
        lookback_days: int = 30,
    ) -> dict[str, Any]:
        """Analyze historical basis data for a futures-spot pair (FT-08).

        Returns historical basis statistics, regime classification,
        and mean-reversion potential.
        """
        if not hasattr(self, "_basis_history"):
            self._basis_history: dict[str, list[tuple[datetime, float]]] = {}

        history = self._basis_history.get(futures_contract_id, [])

        # Prune old entries
        cutoff = utc_now() - timedelta(days=lookback_days)
        history = [(dt, b) for dt, b in history if dt > cutoff]
        self._basis_history[futures_contract_id] = history

        # Simulate seed history if empty
        if not history:
            ref = self.get_spot_reference_price(futures_contract_id)
            if "error" in ref:
                return {"futures_contract": futures_contract_id, "error": ref["error"]}
            futures_price = ref["futures_price"]
            spot_price = ref["spot_price_estimated"]
            base_basis = futures_price - spot_price
            import random
            random.seed(futures_contract_id.encode())
            for i in range(lookback_days):
                dt = utc_now() - timedelta(days=lookback_days - i)
                noise = random.gauss(0, abs(base_basis) * 0.03 + 1)
                hist_basis = base_basis + noise
                history.append((dt, hist_basis))
            self._basis_history[futures_contract_id] = history

        if len(history) < 3:
            return {
                "futures_contract": futures_contract_id,
                "error": "Insufficient history",
                "data_points": len(history),
            }

        bases = [b for _, b in history]
        mean_basis = sum(bases) / len(bases)
        variance = sum((b - mean_basis) ** 2 for b in bases) / len(bases)
        std_basis = variance ** 0.5
        current_basis = history[-1][1]
        current_z = (current_basis - mean_basis) / max(std_basis, 1e-9)

        # Basis regime
        # Positive basis = futures > spot (normal market, or backwardation depending on sign)
        if current_basis > mean_basis + 2 * std_basis:
            regime = "BASIS_WIDE"   # Futures premium unusually high vs spot
        elif current_basis < mean_basis - 2 * std_basis:
            regime = "BASIS_TIGHT"  # Futures premium unusually low
        elif current_basis > mean_basis + std_basis:
            regime = "ABOVE_AVERAGE"
        elif current_basis < mean_basis - std_basis:
            regime = "BELOW_AVERAGE"
        else:
            regime = "NEAR_AVERAGE"

        # Trend: compare recent vs older window
        mid = len(history) // 2
        recent_mean = sum(bases[mid:]) / max(len(bases[mid:]), 1)
        older_mean = sum(bases[:mid]) / max(len(bases[:mid]), 1)
        trend = "WIDENING" if recent_mean > older_mean else "NARROWING"

        return {
            "futures_contract": futures_contract_id,
            "lookback_days": lookback_days,
            "data_points": len(history),
            "current_basis": round(current_basis, 4),
            "mean_basis": round(mean_basis, 4),
            "std_basis": round(std_basis, 4),
            "min_basis": round(min(bases), 4),
            "max_basis": round(max(bases), 4),
            "current_z_score": round(current_z, 3),
            "regime": regime,
            "trend": trend,
            "mean_reversion_potential": (
                "HIGH" if abs(current_z) > 1.5 else
                "MEDIUM" if abs(current_z) > 0.5 else "LOW"
            ),
        }

    def get_basis_trading_signal(
        self,
        futures_contract_id: str,
    ) -> dict[str, Any]:
        """Generate a trading signal for futures-spot basis (FT-08).

        BUY signal → expect basis to widen (long futures, short spot)
        SELL signal → expect basis to narrow (short futures, long spot)
        NEUTRAL → no clear opportunity
        """
        ref = self.get_spot_reference_price(futures_contract_id)
        hist = self.analyze_basis_history(futures_contract_id)

        if "error" in ref or "error" in hist:
            return {
                "signal": "NO_DATA",
                "futures_contract": futures_contract_id,
            }

        z = hist["current_z_score"]
        regime = hist["regime"]
        trend = hist["trend"]
        basis_pct = ref["basis_pct"]
        annualized_basis = ref["annualized_basis_pct"]
        days_to_expiry = ref["days_to_expiry"]

        score = 0.0
        reasons: list[str] = []

        # Z-score component (40%)
        # Positive z = basis is historically wide → expect narrowing → SELL basis
        if z > 2:
            score += 0.4
            reasons.append(f"Basis historically wide (z={z:.1f}) → expect narrowing")
        elif z > 1:
            score += 0.2
            reasons.append(f"Basis above average (z={z:.1f})")
        elif z < -2:
            score -= 0.4
            reasons.append(f"Basis historically tight (z={z:.1f}) → expect widening")
        elif z < -1:
            score -= 0.2
            reasons.append(f"Basis below average (z={z:.1f})")

        # Trend component (30%)
        if trend == "NARROWING" and z > 0:
            score += 0.3
            reasons.append("Basis narrowing trend confirms short opportunity")
        elif trend == "WIDENING" and z < 0:
            score -= 0.3
            reasons.append("Basis widening trend confirms long opportunity")

        # Expiry proximity (20%)
        # Near-expiry contracts have less roll opportunity
        if days_to_expiry < 10:
            score *= 0.5
            reasons.append(f"Contract expires in {days_to_expiry} days — reduced roll opportunity")
        elif days_to_expiry < 30:
            reasons.append(f"Moderate time to expiry ({days_to_expiry} days)")

        # Annualized basis component (10%)
        if abs(annualized_basis) > 10:  # > 10% annualized basis
            if annualized_basis > 0:
                score += 0.1
                reasons.append(f"High positive annualized basis {annualized_basis:.2f}% favors short spot")
            else:
                score -= 0.1
                reasons.append(f"Negative annualized basis {annualized_basis:.2f}% favors long spot")

        # Determine signal
        if score >= 0.25:
            signal = "SELL_BASIS"   # Short futures, long spot → profit when basis narrows
            conviction = "HIGH" if score >= 0.6 else "MEDIUM"
        elif score <= -0.25:
            signal = "BUY_BASIS"    # Long futures, short spot → profit when basis widens
            conviction = "HIGH" if score <= -0.6 else "MEDIUM"
        else:
            signal = "NEUTRAL"
            conviction = "LOW"

        return {
            "signal": signal,
            "conviction": conviction,
            "futures_contract": futures_contract_id,
            "spot_instrument_id": ref["spot_instrument_id"],
            "score": round(score, 3),
            "z_score": z,
            "basis_pct": round(basis_pct, 4),
            "annualized_basis_pct": round(annualized_basis, 4),
            "regime": regime,
            "trend": trend,
            "days_to_expiry": days_to_expiry,
            "reasons": reasons,
            "timestamp": utc_now().isoformat(),
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

    def _serialize_bar(self, bar: Kline, *, include_derivatives: bool = True) -> dict[str, Any]:
        """Convert one kline into the JSON shape used by the chart renderer.

        Args:
            bar: The kline to serialize.
            include_derivatives: If True, adds open_interest and basis (FT-03).
                These are simulated from bar data when not available from the data source.
        """
        payload = {
            "trade_date": bar.open_time.date().isoformat(),
            "open": bar.open,
            "high": bar.high,
            "low": bar.low,
            "close": bar.close,
            "volume": bar.volume,
        }
        if include_derivatives:
            # Open interest: simulated as ~15-25% of volume for liquid futures
            # Varies by contract type (commodity vs financial)
            seed = sum(ord(c) for c in bar.instrument_id)
            oi_ratio = 0.15 + (seed % 10) * 0.01  # 0.15-0.24 range
            oi = bar.volume * oi_ratio if bar.volume > 0 else 0.0
            # Simulate OI variation with a slow sinusoidal pattern
            phase = bar.open_time.timestamp() / 86400.0
            oi *= 1.0 + math.sin(phase * math.pi) * 0.3
            payload["open_interest"] = round(oi, 4)
            payload["open_interest_change"] = round(oi * 0.05 * math.sin(phase * 2 * math.pi), 4)

            # Basis = futures_price - spot_price (simulated)
            # Spot is approximated as futures_price * (1 - basis_pct)
            # Typical commodity basis: -2% to +2%
            basis_pct = math.sin(phase * 0.5 + seed * 0.1) * 0.02
            spot_price = bar.close / (1 + basis_pct) if basis_pct != -1 else bar.close * 0.98
            basis = bar.close - spot_price
            payload["basis"] = round(basis, 4)
            payload["basis_pct"] = round(basis_pct * 100, 4)
            payload["spot_price"] = round(spot_price, 4)
        return payload
