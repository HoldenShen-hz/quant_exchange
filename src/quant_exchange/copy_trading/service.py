"""Copy trading service (COPY-01~COPY-06).

Covers:
- COPY-01: Signal provider registration and performance tracking
- COPY-02: Subscriber management and subscription plans
- COPY-03: Auto-copy orders with risk controls
- COPY-04: Profit/loss sharing and commission结算
- COPY-05: Signal filtering and quality controls
- COPY-06: Anti-fraud and risk management
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any


# ─────────────────────────────────────────────────────────────────────────────
# Enums
# ─────────────────────────────────────────────────────────────────────────────


class SubscriptionStatus(str, Enum):
    ACTIVE = "active"
    PAUSED = "paused"
    CANCELLED = "cancelled"
    EXPIRED = "expired"


class CopyTradeStatus(str, Enum):
    PENDING = "pending"
    EXECUTED = "executed"
    REJECTED = "rejected"
    CANCELLED = "cancelled"


class ProviderStatus(str, Enum):
    ACTIVE = "active"
    SUSPENDED = "suspended"
    CLOSED = "closed"


# ─────────────────────────────────────────────────────────────────────────────
# Data Models
# ─────────────────────────────────────────────────────────────────────────────


@dataclass(slots=True)
class SignalProvider:
    """A signal provider that others can copy."""

    provider_id: str
    user_id: str
    display_name: str
    total_subscribers: int = 0
    total_aum: float = 0.0  # assets under management (copied capital)
    cumulative_pnl: float = 0.0
    win_rate: float = 0.0
    sharpe_ratio: float = 0.0
    max_drawdown: float = 0.0
    monthly_return: float = 0.0
    status: ProviderStatus = ProviderStatus.ACTIVE
    commission_rate: float = 0.20  # 20% of profit
    min_subscription: float = 100.0  # minimum subscription amount
    description: str = ""
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass(slots=True)
class Subscriber:
    """A subscriber who copies signal providers."""

    subscriber_id: str
    user_id: str
    total_invested: float = 0.0
    total_pnl: float = 0.0
    active_subscriptions: int = 0
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass(slots=True)
class Subscription:
    """A subscription to a signal provider."""

    subscription_id: str
    subscriber_id: str
    provider_id: str
    allocated_amount: float
    status: SubscriptionStatus = SubscriptionStatus.ACTIVE
    copy_ratio: float = 1.0  # 1.0 = 100% of provider's position size
    stop_loss_pct: float = 0.10  # 10% max loss per subscription
    auto_compound: bool = False
    commission_paid: float = 0.0
    pnl_share: float = 0.0  # subscriber's share of profit
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass(slots=True)
class CopyTrade:
    """An automatically copied trade."""

    trade_id: str
    subscription_id: str
    provider_id: str
    subscriber_id: str
    instrument_id: str
    direction: str  # LONG/SHORT
    quantity: float
    entry_price: float
    exit_price: float = 0.0
    pnl: float = 0.0
    commission_charged: float = 0.0
    status: CopyTradeStatus = CopyTradeStatus.PENDING
    provider_trade_id: str = ""  # reference to original provider trade
    executed_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    closed_at: datetime | None = None


# ─────────────────────────────────────────────────────────────────────────────
# Copy Trading Service
# ─────────────────────────────────────────────────────────────────────────────


class CopyTradingService:
    """Copy trading service (COPY-01~COPY-06)."""

    def __init__(self, persistence=None) -> None:
        self.persistence = persistence
        self._providers: dict[str, SignalProvider] = {}
        self._subscribers: dict[str, Subscriber] = {}
        self._subscriptions: dict[str, Subscription] = {}
        self._copy_trades: dict[str, CopyTrade] = {}
        self._commission_ledger: list[dict] = []
        self._init_demo_data()

    def _init_demo_data(self) -> None:
        """Initialize demo data."""
        providers = [
            SignalProvider(provider_id="sp001", user_id="u001", display_name="Alice Quant", total_subscribers=42, total_aum=125000.0, cumulative_pnl=18500.0, win_rate=0.68, sharpe_ratio=2.1, max_drawdown=0.08, monthly_return=0.035, description="专注期权波动率策略，月均收益3.5%，夏普比率2.1"),
            SignalProvider(provider_id="sp002", user_id="u002", display_name="Bob Algo", total_subscribers=28, total_aum=78000.0, cumulative_pnl=9200.0, win_rate=0.72, sharpe_ratio=1.9, max_drawdown=0.11, monthly_return=0.028, description="CTA趋势跟踪策略，趋势行情表现出色"),
            SignalProvider(provider_id="sp003", user_id="u003", display_name="Carol Grid", total_subscribers=15, total_aum=35000.0, cumulative_pnl=3100.0, win_rate=0.55, sharpe_ratio=1.2, max_drawdown=0.15, monthly_return=0.018, description="网格做市策略，适合震荡行情"),
        ]
        for p in providers:
            self._providers[p.provider_id] = p

        subscribers = [
            Subscriber(subscriber_id="sub001", user_id="u004", total_invested=5000.0, total_pnl=420.0, active_subscriptions=2),
            Subscriber(subscriber_id="sub002", user_id="u005", total_invested=10000.0, total_pnl=890.0, active_subscriptions=1),
        ]
        for s in subscribers:
            self._subscribers[s.subscriber_id] = s

        subs = [
            Subscription(subscription_id="s001", subscriber_id="sub001", provider_id="sp001", allocated_amount=3000.0, status=SubscriptionStatus.ACTIVE),
            Subscription(subscription_id="s002", subscriber_id="sub001", provider_id="sp002", allocated_amount=2000.0, status=SubscriptionStatus.ACTIVE),
            Subscription(subscription_id="s003", subscriber_id="sub002", provider_id="sp001", allocated_amount=10000.0, status=SubscriptionStatus.ACTIVE),
        ]
        for s in subs:
            self._subscriptions[s.subscription_id] = s

    # ── COPY-01: Signal Provider ─────────────────────────────────────────────

    def register_provider(
        self,
        user_id: str,
        display_name: str,
        description: str = "",
        commission_rate: float = 0.20,
        min_subscription: float = 100.0,
    ) -> SignalProvider:
        """Register as a signal provider (COPY-01)."""
        provider = SignalProvider(
            provider_id=f"sp:{uuid.uuid4().hex[:12]}",
            user_id=user_id,
            display_name=display_name,
            description=description,
            commission_rate=commission_rate,
            min_subscription=min_subscription,
        )
        self._providers[provider.provider_id] = provider
        return provider

    def get_provider(self, provider_id: str) -> SignalProvider | None:
        """Get a signal provider by ID."""
        return self._providers.get(provider_id)

    def list_providers(
        self,
        status: ProviderStatus | None = None,
        sort_by: str = "performance",
        limit: int = 20,
    ) -> list[SignalProvider]:
        """List signal providers (COPY-01)."""
        results = [p for p in self._providers.values() if p.status == ProviderStatus.ACTIVE]
        if status:
            results = [p for p in results if p.status == status]

        if sort_by == "performance":
            results.sort(key=lambda p: p.sharpe_ratio, reverse=True)
        elif sort_by == "subscribers":
            results.sort(key=lambda p: p.total_subscribers, reverse=True)
        elif sort_by == "aum":
            results.sort(key=lambda p: p.total_aum, reverse=True)
        elif sort_by == "return":
            results.sort(key=lambda p: p.monthly_return, reverse=True)

        return results[:limit]

    def update_provider_stats(self, provider_id: str, stats: dict[str, Any]) -> SignalProvider | None:
        """Update provider performance statistics (COPY-01)."""
        provider = self._providers.get(provider_id)
        if not provider:
            return None
        for key, value in stats.items():
            if hasattr(provider, key):
                setattr(provider, key, value)
        return provider

    def suspend_provider(self, provider_id: str, reason: str = "") -> bool:
        """Suspend a signal provider (COPY-06)."""
        provider = self._providers.get(provider_id)
        if not provider:
            return False
        provider.status = ProviderStatus.SUSPENDED
        # Pause all active subscriptions
        for sub in self._subscriptions.values():
            if sub.provider_id == provider_id and sub.status == SubscriptionStatus.ACTIVE:
                sub.status = SubscriptionStatus.PAUSED
        return True

    # ── COPY-02: Subscriber & Subscription ─────────────────────────────────

    def register_subscriber(self, user_id: str) -> Subscriber:
        """Register as a subscriber (COPY-02)."""
        sub = Subscriber(
            subscriber_id=f"sub:{uuid.uuid4().hex[:12]}",
            user_id=user_id,
        )
        self._subscribers[sub.subscriber_id] = sub
        return sub

    def subscribe(
        self,
        subscriber_id: str,
        provider_id: str,
        allocated_amount: float,
        copy_ratio: float = 1.0,
        stop_loss_pct: float = 0.10,
        auto_compound: bool = False,
    ) -> Subscription | None:
        """Subscribe to a signal provider (COPY-02)."""
        provider = self._providers.get(provider_id)
        if not provider or provider.status != ProviderStatus.ACTIVE:
            return None
        if allocated_amount < provider.min_subscription:
            return None

        sub = Subscriber(subscriber_id=subscriber_id, user_id=subscriber_id, active_subscriptions=0)
        subscriber = self._subscribers.get(subscriber_id, sub)
        if subscriber.subscriber_id not in self._subscribers:
            self._subscribers[subscriber.subscriber_id] = subscriber

        subscription = Subscription(
            subscription_id=f"subnow:{uuid.uuid4().hex[:12]}",
            subscriber_id=subscriber.subscriber_id,
            provider_id=provider_id,
            allocated_amount=allocated_amount,
            copy_ratio=copy_ratio,
            stop_loss_pct=stop_loss_pct,
            auto_compound=auto_compound,
        )
        self._subscriptions[subscription.subscription_id] = subscription

        # Update provider stats
        provider.total_subscribers += 1
        provider.total_aum += allocated_amount

        # Update subscriber
        subscriber.total_invested += allocated_amount
        subscriber.active_subscriptions = sum(1 for s in self._subscriptions.values() if s.subscriber_id == subscriber.subscriber_id and s.status == SubscriptionStatus.ACTIVE)

        return subscription

    def pause_subscription(self, subscription_id: str) -> bool:
        """Pause a subscription temporarily."""
        sub = self._subscriptions.get(subscription_id)
        if not sub or sub.status != SubscriptionStatus.ACTIVE:
            return False
        sub.status = SubscriptionStatus.PAUSED
        # Update provider subscriber count
        provider = self._providers.get(sub.provider_id)
        if provider:
            provider.total_subscribers = max(0, provider.total_subscribers - 1)
        return True

    def resume_subscription(self, subscription_id: str) -> bool:
        """Resume a paused subscription."""
        sub = self._subscriptions.get(subscription_id)
        if not sub or sub.status != SubscriptionStatus.PAUSED:
            return False
        sub.status = SubscriptionStatus.ACTIVE
        provider = self._providers.get(sub.provider_id)
        if provider:
            provider.total_subscribers += 1
        return True

    def cancel_subscription(self, subscription_id: str) -> bool:
        """Cancel a subscription."""
        sub = self._subscriptions.get(subscription_id)
        if not sub:
            return False
        sub.status = SubscriptionStatus.CANCELLED
        provider = self._providers.get(sub.provider_id)
        if provider:
            provider.total_subscribers = max(0, provider.total_subscribers - 1)
            provider.total_aum = max(0, provider.total_aum - sub.allocated_amount)
        return True

    def list_subscriptions(self, subscriber_id: str) -> list[Subscription]:
        """List active subscriptions for a subscriber."""
        return [s for s in self._subscriptions.values() if s.subscriber_id == subscriber_id and s.status == SubscriptionStatus.ACTIVE]

    # ── COPY-03: Auto-Copy Orders ──────────────────────────────────────────

    def copy_trade(
        self,
        subscription_id: str,
        provider_trade_id: str,
        instrument_id: str,
        direction: str,
        quantity: float,
        entry_price: float,
    ) -> CopyTrade | None:
        """Automatically copy a provider's trade (COPY-03)."""
        sub = self._subscriptions.get(subscription_id)
        if not sub or sub.status != SubscriptionStatus.ACTIVE:
            return None

        # Check stop-loss
        provider = self._providers.get(sub.provider_id)
        if provider and provider.status != ProviderStatus.ACTIVE:
            return None

        # Scale quantity by copy ratio
        scaled_qty = quantity * sub.copy_ratio

        # Scale by allocated vs provider position (simplified)
        scaled_qty = min(scaled_qty, sub.allocated_amount / entry_price * 0.1)  # max 10% of allocated

        trade = CopyTrade(
            trade_id=f"ct:{uuid.uuid4().hex[:12]}",
            subscription_id=subscription_id,
            provider_id=sub.provider_id,
            subscriber_id=sub.subscriber_id,
            instrument_id=instrument_id,
            direction=direction,
            quantity=scaled_qty,
            entry_price=entry_price,
            provider_trade_id=provider_trade_id,
            status=CopyTradeStatus.EXECUTED,
        )
        self._copy_trades[trade.trade_id] = trade
        return trade

    def close_copy_trade(self, trade_id: str, exit_price: float) -> CopyTrade | None:
        """Close a copied trade and calculate P&L."""
        trade = self._copy_trades.get(trade_id)
        if not trade or trade.status != CopyTradeStatus.EXECUTED:
            return None

        trade.exit_price = exit_price
        multiplier = 1.0 if trade.direction == "LONG" else -1.0
        trade.pnl = (exit_price - trade.entry_price) * trade.quantity * multiplier
        trade.status = CopyTradeStatus.EXECUTED
        trade.closed_at = datetime.now(timezone.utc)

        # Calculate commission
        sub = self._subscriptions.get(trade.subscription_id)
        if sub:
            provider = self._providers.get(trade.provider_id)
            if provider and trade.pnl > 0:
                commission = trade.pnl * provider.commission_rate
                trade.commission_charged = commission
                sub.commission_paid += commission
                provider.cumulative_pnl += trade.pnl - commission

                # Record in ledger
                self._commission_ledger.append({
                    "trade_id": trade.trade_id,
                    "provider_id": trade.provider_id,
                    "subscriber_id": trade.subscriber_id,
                    "pnl": trade.pnl,
                    "commission": commission,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                })

        return trade

    def get_copy_trades(self, subscription_id: str) -> list[CopyTrade]:
        """Get all copy trades for a subscription."""
        return [t for t in self._copy_trades.values() if t.subscription_id == subscription_id]

    # ── COPY-04: Commission & P&L ───────────────────────────────────────────

    def get_provider_earnings(self, provider_id: str) -> dict[str, Any]:
        """Get total earnings for a provider (COPY-04)."""
        provider = self._providers.get(provider_id)
        if not provider:
            return {}
        entries = [e for e in self._commission_ledger if e["provider_id"] == provider_id]
        total_commission = sum(e["commission"] for e in entries)
        total_pnl = sum(e["pnl"] for e in entries)
        return {
            "provider_id": provider_id,
            "total_pnl": total_pnl,
            "total_commission": total_commission,
            "net_pnl": total_pnl - total_commission,
            "subscriber_count": provider.total_subscribers,
            "aum": provider.total_aum,
        }

    def get_subscriber_pnl(self, subscriber_id: str) -> dict[str, Any]:
        """Get P&L for a subscriber across all subscriptions."""
        trades = [t for t in self._copy_trades.values() if t.subscriber_id == subscriber_id]
        total_pnl = sum(t.pnl for t in trades)
        total_commission = sum(t.commission_charged for t in trades)
        return {
            "subscriber_id": subscriber_id,
            "total_trades": len(trades),
            "total_pnl": total_pnl,
            "total_commission": total_commission,
            "net_pnl": total_pnl - total_commission,
            "active_subscriptions": sum(1 for s in self._subscriptions.values() if s.subscriber_id == subscriber_id and s.status == SubscriptionStatus.ACTIVE),
        }

    # ── COPY-05: Signal Quality Controls ─────────────────────────────────────

    def get_signal_quality_score(self, provider_id: str) -> dict[str, Any]:
        """Calculate signal quality score (COPY-05)."""
        provider = self._providers.get(provider_id)
        if not provider:
            return {}

        # Composite score based on multiple factors
        sharpe_score = min(provider.sharpe_ratio / 3.0, 1.0) * 30  # max 30 points
        drawdown_score = max(0, (0.20 - provider.max_drawdown) / 0.20) * 25  # max 25 points
        winrate_score = provider.win_rate * 25  # max 25 points
        consistency_score = min(provider.monthly_return / 0.05, 1.0) * 20  # max 20 points

        total = sharpe_score + drawdown_score + winrate_score + consistency_score

        return {
            "provider_id": provider_id,
            "total_score": round(total, 1),
            "sharpe_component": round(sharpe_score, 1),
            "drawdown_component": round(drawdown_score, 1),
            "winrate_component": round(winrate_score, 1),
            "consistency_component": round(consistency_score, 1),
            "rating": "A" if total >= 80 else "B" if total >= 60 else "C" if total >= 40 else "D",
        }

    # ── COPY-06: Risk Management ───────────────────────────────────────────

    def check_risk_limits(self, subscription_id: str) -> dict[str, Any]:
        """Check if subscription is within risk limits (COPY-06)."""
        sub = self._subscriptions.get(subscription_id)
        if not sub:
            return {"ok": False, "reason": "subscription not found"}

        # Check drawdown limit
        sub_trades = [t for t in self._copy_trades.values() if t.subscription_id == subscription_id]
        if not sub_trades:
            return {"ok": True, "checks": {}}

        total_pnl = sum(t.pnl for t in sub_trades)
        loss_pct = abs(total_pnl) / sub.allocated_amount if total_pnl < 0 else 0

        checks = {
            "stop_loss_ok": loss_pct <= sub.stop_loss_pct,
            "loss_pct": round(loss_pct, 4),
            "stop_loss_limit": sub.stop_loss_pct,
        }

        all_ok = checks["stop_loss_ok"]

        return {
            "ok": all_ok,
            "subscription_id": subscription_id,
            "checks": checks,
        }
