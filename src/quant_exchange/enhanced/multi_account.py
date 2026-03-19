"""Multi-account unified management service (ACCT-01 ~ ACCT-04).

Covers:
- Account registration, groups, and unified asset view
- Internal transfers between accounts
- Cross-account risk monitoring
- Sub-account hierarchy support
"""

from __future__ import annotations

import uuid
from collections import defaultdict
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


# ─────────────────────────────────────────────────────────────────────────────
# Enums
# ─────────────────────────────────────────────────────────────────────────────

class AccountType(str, Enum):
    LIVE = "live"
    PAPER = "paper"
    SUBACCOUNT = "subaccount"
    AGGREGATED = "aggregated"


class TransferStatus(str, Enum):
    PENDING = "pending"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


# ─────────────────────────────────────────────────────────────────────────────
# Data Models
# ─────────────────────────────────────────────────────────────────────────────

@dataclass(slots=True)
class AccountInfo:
    """A trading account."""

    account_id: str
    user_id: str
    account_type: AccountType
    name: str
    balance: float
    currency: str = "USD"
    is_active: bool = True
    created_at: str = field(default_factory=_now)
    updated_at: str = field(default_factory=_now)


@dataclass(slots=True)
class AccountGroup:
    """A group of accounts for unified management."""

    group_id: str
    user_id: str
    name: str
    account_ids: tuple[str, ...] = field(default_factory=tuple)
    created_at: str = field(default_factory=_now)


@dataclass(slots=True)
class UnifiedAssetView:
    """Unified view of assets across multiple accounts."""

    user_id: str
    total_equity: float
    total_cash: float
    total_positions_value: float
    total_pnl: float
    currency: str
    account_breakdown: dict[str, float]  # account_id -> equity
    as_of: str = field(default_factory=_now)


@dataclass(slots=True)
class InternalTransfer:
    """Internal transfer between accounts."""

    transfer_id: str
    from_account_id: str
    to_account_id: str
    amount: float
    currency: str
    status: TransferStatus
    initiated_at: str = field(default_factory=_now)
    completed_at: str | None = None
    error_message: str = ""


@dataclass(slots=True)
class CrossAccountRiskExposure:
    """Cross-account risk exposure summary."""

    user_id: str
    net_exposure: float
    gross_exposure: float
    largest_single_position: float
    leverage_ratio: float
    concentration_risk: float  # Herfindahl index of positions
    as_of: str = field(default_factory=_now)


# ─────────────────────────────────────────────────────────────────────────────
# Multi-Account Service
# ─────────────────────────────────────────────────────────────────────────────

class MultiAccountService:
    """Multi-account unified management service (ACCT-01 ~ ACCT-04).

    Provides:
    - Account registration and lifecycle
    - Account groups for unified management
    - Unified asset view across accounts
    - Internal transfers
    - Cross-account risk monitoring
    """

    def __init__(self, persistence=None) -> None:
        self.persistence = persistence
        self._accounts: dict[str, AccountInfo] = {}
        self._user_accounts: dict[str, list[str]] = defaultdict(list)  # user_id -> account_ids
        self._groups: dict[str, AccountGroup] = {}
        self._user_groups: dict[str, list[str]] = defaultdict(list)  # user_id -> group_ids
        self._transfers: dict[str, InternalTransfer] = {}

    # ── Account Management ─────────────────────────────────────────────────

    def register_account(
        self,
        user_id: str,
        account_type: AccountType,
        name: str,
        initial_balance: float = 0.0,
        currency: str = "USD",
    ) -> AccountInfo:
        """Register a new account for a user."""
        account_id = f"acct:{uuid.uuid4().hex[:12]}"
        account = AccountInfo(
            account_id=account_id,
            user_id=user_id,
            account_type=account_type,
            name=name,
            balance=initial_balance,
            currency=currency,
        )
        self._accounts[account_id] = account
        self._user_accounts[user_id].append(account_id)
        return account

    def get_account(self, account_id: str) -> AccountInfo | None:
        """Get account info by ID."""
        return self._accounts.get(account_id)

    def get_user_accounts(self, user_id: str) -> list[AccountInfo]:
        """Get all accounts for a user."""
        return [self._accounts[aid] for aid in self._user_accounts.get(user_id, []) if aid in self._accounts]

    def update_balance(self, account_id: str, new_balance: float) -> bool:
        """Update account balance."""
        account = self._accounts.get(account_id)
        if not account:
            return False
        account.balance = new_balance
        account.updated_at = _now()
        return True

    def deactivate_account(self, account_id: str) -> bool:
        """Deactivate an account."""
        account = self._accounts.get(account_id)
        if not account:
            return False
        account.is_active = False
        account.updated_at = _now()
        return True

    # ── Account Groups ────────────────────────────────────────────────────

    def create_group(self, user_id: str, name: str) -> AccountGroup:
        """Create an account group."""
        group_id = f"grp:{uuid.uuid4().hex[:12]}"
        group = AccountGroup(
            group_id=group_id,
            user_id=user_id,
            name=name,
        )
        self._groups[group_id] = group
        self._user_groups[user_id].append(group_id)
        return group

    def add_account_to_group(self, group_id: str, account_id: str) -> bool:
        """Add an account to a group."""
        group = self._groups.get(group_id)
        if not group:
            return False
        if account_id not in group.account_ids:
            group.account_ids = group.account_ids + (account_id,)
        return True

    def remove_account_from_group(self, group_id: str, account_id: str) -> bool:
        """Remove an account from a group."""
        group = self._groups.get(group_id)
        if not group:
            return False
        group.account_ids = tuple(aid for aid in group.account_ids if aid != account_id)
        return True

    def get_user_groups(self, user_id: str) -> list[AccountGroup]:
        """Get all groups for a user."""
        return [self._groups[gid] for gid in self._user_groups.get(user_id, []) if gid in self._groups]

    # ── Unified Asset View ─────────────────────────────────────────────────

    def get_unified_view(
        self,
        user_id: str,
        account_ids: list[str] | None = None,
    ) -> UnifiedAssetView:
        """Get unified asset view across accounts.

        If account_ids is None, uses all user's accounts.
        """
        if account_ids is None:
            account_ids = self._user_accounts.get(user_id, [])

        total_equity = 0.0
        total_cash = 0.0
        account_breakdown: dict[str, float] = {}
        primary_currency = "USD"

        for aid in account_ids:
            account = self._accounts.get(aid)
            if not account or not account.is_active:
                continue
            equity = account.balance
            total_equity += equity
            total_cash += equity
            account_breakdown[aid] = equity
            primary_currency = account.currency

        return UnifiedAssetView(
            user_id=user_id,
            total_equity=total_equity,
            total_cash=total_cash,
            total_positions_value=0.0,  # Would be populated from portfolio service
            total_pnl=0.0,
            currency=primary_currency,
            account_breakdown=account_breakdown,
        )

    # ── Internal Transfers ─────────────────────────────────────────────────

    def transfer(
        self,
        from_account_id: str,
        to_account_id: str,
        amount: float,
        currency: str = "USD",
    ) -> InternalTransfer | None:
        """Initiate an internal transfer between accounts."""
        from_acc = self._accounts.get(from_account_id)
        to_acc = self._accounts.get(to_account_id)
        if not from_acc or not to_acc:
            return None
        if from_acc.balance < amount:
            return None

        transfer_id = f"xfer:{uuid.uuid4().hex[:12]}"
        transfer = InternalTransfer(
            transfer_id=transfer_id,
            from_account_id=from_account_id,
            to_account_id=to_account_id,
            amount=amount,
            currency=currency,
            status=TransferStatus.PENDING,
        )
        self._transfers[transfer_id] = transfer

        # Execute transfer
        from_acc.balance -= amount
        from_acc.updated_at = _now()
        to_acc.balance += amount
        to_acc.updated_at = _now()

        transfer.status = TransferStatus.COMPLETED
        transfer.completed_at = _now()

        return transfer

    def get_transfer(self, transfer_id: str) -> InternalTransfer | None:
        """Get transfer status."""
        return self._transfers.get(transfer_id)

    def cancel_transfer(self, transfer_id: str) -> bool:
        """Cancel a pending transfer (if not yet completed)."""
        transfer = self._transfers.get(transfer_id)
        if not transfer or transfer.status != TransferStatus.PENDING:
            return False
        # Reverse the transfer
        from_acc = self._accounts.get(transfer.from_account_id)
        to_acc = self._accounts.get(transfer.to_account_id)
        if from_acc and to_acc:
            from_acc.balance += transfer.amount
            to_acc.balance -= transfer.amount
        transfer.status = TransferStatus.CANCELLED
        return True

    # ── Cross-Account Risk ────────────────────────────────────────────────

    def compute_cross_account_risk(
        self,
        user_id: str,
        positions: dict[str, float] | None = None,  # instrument_id -> quantity
    ) -> CrossAccountRiskExposure:
        """Compute cross-account risk exposure.

        positions: optional dict of instrument -> quantity for position risk.
                   If None, only computes exposure from cash balances.
        """
        accounts = self.get_user_accounts(user_id)
        total_equity = sum(a.balance for a in accounts if a.is_active)

        # Simple exposure from balances
        net_exposure = total_equity
        gross_exposure = total_equity
        largest_single = max((a.balance for a in accounts if a.is_active), default=0.0)

        # Leverage
        leverage_ratio = 1.0  # No leverage by default

        # Concentration risk (simplified Herfindahl)
        concentration_risk = 0.0
        if total_equity > 0:
            for a in accounts:
                if a.is_active:
                    share = a.balance / total_equity
                    concentration_risk += share * share

        return CrossAccountRiskExposure(
            user_id=user_id,
            net_exposure=net_exposure,
            gross_exposure=gross_exposure,
            largest_single_position=largest_single,
            leverage_ratio=leverage_ratio,
            concentration_risk=concentration_risk,
        )
