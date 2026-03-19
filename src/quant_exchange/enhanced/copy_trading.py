"""Copy trading service (COPY-01 ~ COPY-06).

Covers:
- Signal provider access and management
- Copy mode configuration
- Deviation monitoring
- One-click stop/copy stop
- Copy trading leaderboard
"""

from __future__ import annotations

import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


# ─────────────────────────────────────────────────────────────────────────────
# Enums
# ─────────────────────────────────────────────────────────────────────────────

class CopyMode(str, Enum):
    FIXED_AMOUNT = "fixed_amount"    # Copy with fixed dollar amount
    FIXED_LOT = "fixed_lot"         # Copy with fixed lot/quantity
    PROPORTIONAL = "proportional"    # Proportional to provider (percentage)
    MIRROR = "mirror"                # Mirror exact positions


class CopyStatus(str, Enum):
    ACTIVE = "active"
    PAUSED = "paused"
    STOPPED = "stopped"
    EXPIRED = "expired"
    LIMIT_REACHED = "limit_reached"


class SignalType(str, Enum):
    ENTRY_LONG = "entry_long"
    ENTRY_SHORT = "entry_short"
    EXIT = "exit"
    CLOSE_ALL = "close_all"
    SIGNAL_ENTRY = "signal_entry"  # Generic signal from provider


@dataclass(slots=True)
class SignalProvider:
    """A signal provider that can be copied."""

    provider_id: str
    user_id: str
    name: str
    description: str = ""
    instruments: tuple[str, ...] = field(default_factory=tuple)  # instrument IDs they trade
    strategies: tuple[str, ...] = field(default_factory=tuple)   # strategy IDs
    follower_count: int = 0
    total_return_pct: float = 0.0
    sharpe_ratio: float = 0.0
    max_drawdown_pct: float = 0.0
    win_rate: float = 0.0
    avg_trade_duration_hours: float = 0.0
    monthly_return_pct: float = 0.0
    is_verified: bool = False
    is_featured: bool = False
    commission_pct: float = 0.0  # % of profit charged
    min_copy_amount: float = 100.0
    max_copy_amount: float = 100000.0
    created_at: str = field(default_factory=_now)
    updated_at: str = field(default_factory=_now)


@dataclass(slots=True)
class CopyTrade:
    """An active copy trade relationship."""

    copy_id: str
    follower_id: str
    provider_id: str
    copy_mode: CopyMode
    status: CopyStatus
    allocated_amount: float = 0.0           # Total allocated for copying
    used_amount: float = 0.0                 # Currently deployed
    available_amount: float = 0.0            # Remaining available
    unrealized_pnl: float = 0.0
    realized_pnl: float = 0.0
    total_fees_paid: float = 0.0
    # Settings
    max_slippage_pct: float = 0.5            # Max price slippage to allow
    auto_stop_loss_pct: float | None = None  # Auto-stop if loss exceeds this
    close_on_stop: bool = True              # Close positions when stopping
    notifications_enabled: bool = True
    # Limits
    max_positions: int = 10
    per_trade_limit_pct: float = 20.0       # Max % of copy allocation per trade
    # Tracking
    last_sync_at: str = field(default_factory=_now)
    started_at: str = field(default_factory=_now)
    stopped_at: str | None = None


@dataclass(slots=True)
class CopiedPosition:
    """A position opened via copy trading."""

    position_id: str
    copy_id: str
    follower_id: str
    provider_id: str
    instrument_id: str
    provider_position_id: str | None = None  # Original position on provider's side
    direction: str = "long"  # long, short
    quantity: float = 0.0
    entry_price: float = 0.0
    current_price: float = 0.0
    unrealized_pnl: float = 0.0
    opened_at: str = field(default_factory=_now)
    synced_at: str = field(default_factory=_now)


@dataclass(slots=True)
class SignalEvent:
    """A trading signal from a provider."""

    signal_id: str
    provider_id: str
    instrument_id: str
    signal_type: SignalType
    quantity: float | None = None
    price: float | None = None
    stop_loss: float | None = None
    take_profit: float | None = None
    reason: str = ""
    confidence: float = 1.0  # 0.0 - 1.0
    expires_at: str | None = None
    status: str = "pending"  # pending, executed, partially_executed, expired, cancelled
    created_at: str = field(default_factory=_now)


@dataclass(slots=True)
class DeviationAlert:
    """Alert when copy trade deviates from provider."""

    alert_id: str
    copy_id: str
    alert_type: str  # price_deviation, position_mismatch, delay, equity_drop
    message: str
    severity: str = "warning"  # info, warning, critical
    details: dict[str, Any] = field(default_factory=dict)
    acknowledged: bool = False
    created_at: str = field(default_factory=_now)


@dataclass(slots=True)
class LeaderboardEntry:
    """Entry in the copy trading leaderboard."""

    rank: int
    provider_id: str
    name: str
    total_return_pct: float
    monthly_return_pct: float
    sharpe_ratio: float
    max_drawdown_pct: float
    win_rate: float
    follower_count: int
    is_verified: bool
    is_featured: bool


# ─────────────────────────────────────────────────────────────────────────────
# Copy Trading Service
# ─────────────────────────────────────────────────────────────────────────────

class CopyTradingService:
    """Copy trading service (COPY-01 ~ COPY-06).

    Provides:
    - Signal provider access and management
    - Copy mode configuration
    - Deviation monitoring
    - One-click stop/copy stop
    - Copy trading leaderboard
    """

    def __init__(self, persistence=None) -> None:
        self.persistence = persistence
        self._providers: dict[str, SignalProvider] = {}
        self._copies: dict[str, CopyTrade] = {}
        self._positions: dict[str, CopiedPosition] = {}
        self._signals: dict[str, SignalEvent] = {}
        self._alerts: dict[str, DeviationAlert] = {}
        self._provider_signals: dict[str, list[str]] = {}  # provider_id -> signal_ids
        self._follower_copies: dict[str, list[str]] = {}   # follower_id -> copy_ids

    # ── Provider Management ─────────────────────────────────────────────────

    def register_provider(
        self,
        user_id: str,
        name: str,
        description: str = "",
        instruments: list[str] | None = None,
        strategies: list[str] | None = None,
        commission_pct: float = 0.0,
        min_copy_amount: float = 100.0,
        max_copy_amount: float = 100000.0,
    ) -> SignalProvider:
        """Register as a signal provider."""
        provider_id = f"prov:{uuid.uuid4().hex[:12]}"
        provider = SignalProvider(
            provider_id=provider_id,
            user_id=user_id,
            name=name,
            description=description,
            instruments=tuple(instruments) if instruments else (),
            strategies=tuple(strategies) if strategies else (),
            commission_pct=commission_pct,
            min_copy_amount=min_copy_amount,
            max_copy_amount=max_copy_amount,
        )
        self._providers[provider_id] = provider
        self._provider_signals[provider_id] = []
        return provider

    def get_provider(self, provider_id: str) -> SignalProvider | None:
        """Get a provider by ID."""
        return self._providers.get(provider_id)

    def get_providers(
        self,
        instrument_id: str | None = None,
        verified_only: bool = False,
        featured_only: bool = False,
        sort_by: str = "total_return_pct",
    ) -> list[SignalProvider]:
        """Get providers with filters."""
        results = list(self._providers.values())

        if instrument_id:
            results = [p for p in results if instrument_id in p.instruments]
        if verified_only:
            results = [p for p in results if p.is_verified]
        if featured_only:
            results = [p for p in results if p.is_featured]

        # Sort
        if sort_by == "total_return_pct":
            results.sort(key=lambda p: p.total_return_pct, reverse=True)
        elif sort_by == "sharpe_ratio":
            results.sort(key=lambda p: p.sharpe_ratio, reverse=True)
        elif sort_by == "follower_count":
            results.sort(key=lambda p: p.follower_count, reverse=True)
        elif sort_by == "monthly_return_pct":
            results.sort(key=lambda p: p.monthly_return_pct, reverse=True)

        return results

    def update_provider_stats(
        self,
        provider_id: str,
        total_return_pct: float | None = None,
        sharpe_ratio: float | None = None,
        max_drawdown_pct: float | None = None,
        win_rate: float | None = None,
        monthly_return_pct: float | None = None,
        avg_trade_duration_hours: float | None = None,
    ) -> SignalProvider | None:
        """Update provider statistics."""
        provider = self._providers.get(provider_id)
        if not provider:
            return None
        if total_return_pct is not None:
            provider.total_return_pct = total_return_pct
        if sharpe_ratio is not None:
            provider.sharpe_ratio = sharpe_ratio
        if max_drawdown_pct is not None:
            provider.max_drawdown_pct = max_drawdown_pct
        if win_rate is not None:
            provider.win_rate = win_rate
        if monthly_return_pct is not None:
            provider.monthly_return_pct = monthly_return_pct
        if avg_trade_duration_hours is not None:
            provider.avg_trade_duration_hours = avg_trade_duration_hours
        provider.updated_at = _now()
        return provider

    def verify_provider(self, provider_id: str, verified: bool = True) -> SignalProvider | None:
        """Verify or unverify a provider."""
        provider = self._providers.get(provider_id)
        if provider:
            provider.is_verified = verified
        return provider

    def set_featured_provider(self, provider_id: str, featured: bool = True) -> SignalProvider | None:
        """Set or unset a provider as featured."""
        provider = self._providers.get(provider_id)
        if provider:
            provider.is_featured = featured
        return provider

    # ── Copy Trade Management ───────────────────────────────────────────────

    def start_copying(
        self,
        follower_id: str,
        provider_id: str,
        copy_mode: CopyMode,
        allocated_amount: float,
    ) -> CopyTrade | None:
        """Start copying a provider."""
        provider = self._providers.get(provider_id)
        if not provider:
            return None

        if allocated_amount < provider.min_copy_amount or allocated_amount > provider.max_copy_amount:
            return None

        copy_id = f"copy:{uuid.uuid4().hex[:12]}"
        copy = CopyTrade(
            copy_id=copy_id,
            follower_id=follower_id,
            provider_id=provider_id,
            copy_mode=copy_mode,
            status=CopyStatus.ACTIVE,
            allocated_amount=allocated_amount,
            used_amount=0.0,
            available_amount=allocated_amount,
        )
        self._copies[copy_id] = copy

        # Track
        if follower_id not in self._follower_copies:
            self._follower_copies[follower_id] = []
        self._follower_copies[follower_id].append(copy_id)

        # Increment follower count
        provider.follower_count += 1

        return copy

    def get_copy_trade(self, copy_id: str) -> CopyTrade | None:
        """Get a copy trade by ID."""
        return self._copies.get(copy_id)

    def get_user_copies(self, user_id: str, status: CopyStatus | None = None) -> list[CopyTrade]:
        """Get all copy trades for a user."""
        copy_ids = self._follower_copies.get(user_id, [])
        copies = [self._copies[cid] for cid in copy_ids if cid in self._copies]
        if status:
            copies = [c for c in copies if c.status == status]
        return copies

    def pause_copying(self, copy_id: str) -> CopyTrade | None:
        """Pause an active copy trade."""
        copy = self._copies.get(copy_id)
        if copy and copy.status == CopyStatus.ACTIVE:
            copy.status = CopyStatus.PAUSED
        return copy

    def resume_copying(self, copy_id: str) -> CopyTrade | None:
        """Resume a paused copy trade."""
        copy = self._copies.get(copy_id)
        if copy and copy.status == CopyStatus.PAUSED:
            copy.status = CopyStatus.ACTIVE
        return copy

    def stop_copying(self, copy_id: str, close_positions: bool = True) -> CopyTrade | None:
        """Stop copying a provider."""
        copy = self._copies.get(copy_id)
        if not copy:
            return None

        copy.status = CopyStatus.STOPPED
        copy.stopped_at = _now()

        if close_positions:
            # Close all copied positions
            for pos in list(self._positions.values()):
                if pos.copy_id == copy_id:
                    pos.unrealized_pnl = 0.0
                    # Would trigger actual close in real implementation

        # Decrement follower count
        provider = self._providers.get(copy.provider_id)
        if provider:
            provider.follower_count = max(0, provider.follower_count - 1)

        return copy

    def update_copy_settings(
        self,
        copy_id: str,
        max_slippage_pct: float | None = None,
        auto_stop_loss_pct: float | None = None,
        close_on_stop: bool | None = None,
        notifications_enabled: bool | None = None,
        max_positions: int | None = None,
        per_trade_limit_pct: float | None = None,
    ) -> CopyTrade | None:
        """Update copy trade settings."""
        copy = self._copies.get(copy_id)
        if not copy:
            return None
        if max_slippage_pct is not None:
            copy.max_slippage_pct = max_slippage_pct
        if auto_stop_loss_pct is not None:
            copy.auto_stop_loss_pct = auto_stop_loss_pct
        if close_on_stop is not None:
            copy.close_on_stop = close_on_stop
        if notifications_enabled is not None:
            copy.notifications_enabled = notifications_enabled
        if max_positions is not None:
            copy.max_positions = max_positions
        if per_trade_limit_pct is not None:
            copy.per_trade_limit_pct = per_trade_limit_pct
        return copy

    # ── Signal Handling ────────────────────────────────────────────────────

    def emit_signal(
        self,
        provider_id: str,
        instrument_id: str,
        signal_type: SignalType,
        quantity: float | None = None,
        price: float | None = None,
        stop_loss: float | None = None,
        take_profit: float | None = None,
        reason: str = "",
        confidence: float = 1.0,
        expires_at: str | None = None,
    ) -> SignalEvent | None:
        """Emit a trading signal from a provider."""
        if provider_id not in self._providers:
            return None

        signal_id = f"sig:{uuid.uuid4().hex[:12]}"
        signal = SignalEvent(
            signal_id=signal_id,
            provider_id=provider_id,
            instrument_id=instrument_id,
            signal_type=signal_type,
            quantity=quantity,
            price=price,
            stop_loss=stop_loss,
            take_profit=take_profit,
            reason=reason,
            confidence=confidence,
            expires_at=expires_at,
        )
        self._signals[signal_id] = signal
        self._provider_signals[provider_id].append(signal_id)
        return signal

    def get_signal(self, signal_id: str) -> SignalEvent | None:
        """Get a signal by ID."""
        return self._signals.get(signal_id)

    def get_provider_signals(
        self,
        provider_id: str,
        status: str | None = None,
        limit: int = 50,
    ) -> list[SignalEvent]:
        """Get signals from a provider."""
        signal_ids = self._provider_signals.get(provider_id, [])
        signals = [self._signals[sid] for sid in reversed(signal_ids) if sid in self._signals]
        if status:
            signals = [s for s in signals if s.status == status]
        return signals[:limit]

    def update_signal_status(self, signal_id: str, status: str) -> SignalEvent | None:
        """Update signal status."""
        signal = self._signals.get(signal_id)
        if signal:
            signal.status = status
        return signal

    def execute_signal_for_copy(
        self,
        copy_id: str,
        signal: SignalEvent,
    ) -> CopiedPosition | None:
        """Execute a provider's signal for a specific copy trade."""
        copy = self._copies.get(copy_id)
        if not copy or copy.status != CopyStatus.ACTIVE:
            return None

        # Check position limits
        copy_positions = [p for p in self._positions.values() if p.copy_id == copy_id and p.unrealized_pnl != 0]
        if len(copy_positions) >= copy.max_positions:
            copy.status = CopyStatus.LIMIT_REACHED
            return None

        # Calculate quantity based on copy mode
        quantity = signal.quantity or 0.0
        if copy.copy_mode == CopyMode.PROPORTIONAL:
            quantity = (copy.allocated_amount * 0.1) / signal.price if signal.price else 0.0
        elif copy.copy_mode == CopyMode.FIXED_AMOUNT:
            quantity = min(copy.available_amount * 0.1, copy.allocated_amount * copy.per_trade_limit_pct / 100) / signal.price if signal.price else 0.0
        elif copy.copy_mode == CopyMode.FIXED_LOT:
            quantity = 1.0  # fixed lot size

        position_id = f"cpos:{uuid.uuid4().hex[:12]}"
        position = CopiedPosition(
            position_id=position_id,
            copy_id=copy_id,
            follower_id=copy.follower_id,
            provider_id=copy.provider_id,
            provider_position_id=None,
            instrument_id=signal.instrument_id,
            direction="long" if signal.signal_type in (SignalType.ENTRY_LONG, SignalType.SIGNAL_ENTRY) else "short",
            quantity=quantity,
            entry_price=signal.price or 0.0,
            current_price=signal.price or 0.0,
        )
        self._positions[position_id] = position

        # Update copy used_amount
        copy.used_amount += quantity * (signal.price or 0.0)
        copy.available_amount = copy.allocated_amount - copy.used_amount
        copy.last_sync_at = _now()

        return position

    # ── Position Tracking ─────────────────────────────────────────────────

    def get_copied_positions(self, copy_id: str) -> list[CopiedPosition]:
        """Get all positions for a copy trade."""
        return [p for p in self._positions.values() if p.copy_id == copy_id]

    def get_user_positions(self, user_id: str) -> list[CopiedPosition]:
        """Get all copy positions for a user."""
        return [p for p in self._positions.values() if p.follower_id == user_id]

    def sync_position(
        self,
        position_id: str,
        current_price: float,
    ) -> CopiedPosition | None:
        """Sync/correct a copied position's current price and PnL."""
        position = self._positions.get(position_id)
        if not position:
            return None

        position.current_price = current_price
        direction = 1 if position.direction == "long" else -1
        position.unrealized_pnl = direction * (current_price - position.entry_price) * position.quantity
        position.synced_at = _now()

        # Sync parent copy
        copy = self._copies.get(position.copy_id)
        if copy:
            copy.unrealized_pnl = sum(
                p.unrealized_pnl for p in self._positions.values() if p.copy_id == copy.copy_id
            )
            copy.last_sync_at = _now()

            # Check auto-stop-loss
            if copy.auto_stop_loss_pct and abs(copy.unrealized_pnl) / copy.allocated_amount * 100 > copy.auto_stop_loss_pct:
                self.stop_copying(copy.copy_id, close_positions=copy.close_on_stop)

        return position

    def close_copied_position(
        self,
        position_id: str,
        exit_price: float,
    ) -> CopiedPosition | None:
        """Close a copied position."""
        position = self._positions.get(position_id)
        if not position:
            return None

        direction = 1 if position.direction == "long" else -1
        position.unrealized_pnl = direction * (exit_price - position.entry_price) * position.quantity
        position.current_price = exit_price
        position.quantity = 0.0

        # Release used amount back to available
        copy = self._copies.get(position.copy_id)
        if copy:
            copy.used_amount -= position.entry_price * position.quantity
            copy.available_amount = copy.allocated_amount - copy.used_amount
            copy.realized_pnl += position.unrealized_pnl

        return position

    # ── Deviation & Alerts ──────────────────────────────────────────────────

    def check_deviations(self, copy_id: str) -> list[DeviationAlert]:
        """Check for deviations between copy and provider."""
        copy = self._copies.get(copy_id)
        if not copy:
            return []

        alerts: list[DeviationAlert] = []

        # Check if copy is paused/stopped
        if copy.status != CopyStatus.ACTIVE:
            return alerts

        # Check equity drop
        if copy.unrealized_pnl < -copy.allocated_amount * 0.1:
            alerts.append(DeviationAlert(
                alert_id=f"alert:{uuid.uuid4().hex[:12]}",
                copy_id=copy_id,
                alert_type="equity_drop",
                severity="critical" if copy.unrealized_pnl < -copy.allocated_amount * 0.2 else "warning",
                message=f"Copy equity down {abs(copy.unrealized_pnl / copy.allocated_amount * 100):.1f}%",
                details={"pnl_pct": copy.unrealized_pnl / copy.allocated_amount * 100},
            ))

        # Check if positions are out of sync (simplified)
        positions = self.get_copied_positions(copy_id)
        if len(positions) == 0 and copy.used_amount > 0:
            alerts.append(DeviationAlert(
                alert_id=f"alert:{uuid.uuid4().hex[:12]}",
                copy_id=copy_id,
                alert_type="position_mismatch",
                severity="warning",
                message="No active positions but allocated amount shows deployed capital",
                details={"used_amount": copy.used_amount},
            ))

        for alert in alerts:
            self._alerts[alert.alert_id] = alert

        return alerts

    def get_alerts(self, copy_id: str, unacknowledged_only: bool = False) -> list[DeviationAlert]:
        """Get alerts for a copy trade."""
        alerts = [a for a in self._alerts.values() if a.copy_id == copy_id]
        if unacknowledged_only:
            alerts = [a for a in alerts if not a.acknowledged]
        return alerts

    def acknowledge_alert(self, alert_id: str) -> DeviationAlert | None:
        """Acknowledge an alert."""
        alert = self._alerts.get(alert_id)
        if alert:
            alert.acknowledged = True
        return alert

    # ── Leaderboard ────────────────────────────────────────────────────────

    def get_leaderboard(
        self,
        period: str = "monthly",  # daily, weekly, monthly, all_time
        limit: int = 20,
    ) -> list[LeaderboardEntry]:
        """Get the copy trading leaderboard."""
        providers = self.get_providers(sort_by="total_return_pct")

        entries = []
        for rank, provider in enumerate(providers[:limit], 1):
            entries.append(LeaderboardEntry(
                rank=rank,
                provider_id=provider.provider_id,
                name=provider.name,
                total_return_pct=provider.total_return_pct,
                monthly_return_pct=provider.monthly_return_pct,
                sharpe_ratio=provider.sharpe_ratio,
                max_drawdown_pct=provider.max_drawdown_pct,
                win_rate=provider.win_rate,
                follower_count=provider.follower_count,
                is_verified=provider.is_verified,
                is_featured=provider.is_featured,
            ))
        return entries
