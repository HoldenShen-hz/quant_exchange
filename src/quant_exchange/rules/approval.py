"""Three-tier approval workflow for high-risk operations (EX-06).

Risk tiers:
  L1 — Operator: single approver (risk, trader)
  L2 — Risk Manager: requires risk manager sign-off
  L3 — Compliance: requires compliance officer + risk manager dual sign-off

Actions requiring approval:
  L1: MANUAL_OVERRIDE (non-critical), large order modifications
  L2: DEPLOY_STRATEGY, MODIFY_RISK_RULES, KILL_SWITCH (non-emergency)
  L3: KILL_SWITCH (emergency), DELETE_DATA, ROLLBACK_STRATEGY
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any

from quant_exchange.core.models import Action, utc_now


class ApprovalTier(str, Enum):
    """Required approval tier for a given action."""
    L1 = "L1"  # Operator only
    L2 = "L2"  # Risk manager required
    L3 = "L3"  # Compliance + risk manager


class ApprovalStatus(str, Enum):
    """Status of an individual approval request."""
    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"
    EXPIRED = "expired"
    CANCELLED = "cancelled"


# ─── Approval Tier Mapping ────────────────────────────────────────────────────

ACTION_TIER_MAP: dict[Action, ApprovalTier] = {
    Action.MANUAL_OVERRIDE: ApprovalTier.L1,
    Action.DEPLOY_STRATEGY: ApprovalTier.L2,
    Action.MODIFY_RISK_RULES: ApprovalTier.L2,
    Action.TRIGGER_KILL_SWITCH: ApprovalTier.L3,  # L3 if emergency
    Action.DELETE_DATA: ApprovalTier.L3,
}


# ─── Data Models ───────────────────────────────────────────────────────────────

@dataclass
class ApprovalRequest:
    """A pending or completed approval request for a high-risk action."""
    request_id: str
    actor: str  # Who initiated the request
    action: Action
    tier: ApprovalTier
    resource: str  # e.g. "strategy:ma_sentiment" or "risk_rule:daily_loss_limit"
    details: dict[str, Any]
    status: ApprovalStatus
    required_approvers: list[str]  # role names or user IDs
    approvals: dict[str, dict[str, Any]] = field(default_factory=dict)  # approver -> {status, comment, timestamp}
    created_at: datetime = field(default_factory=utc_now)
    expires_at: datetime = field(default_factory=lambda: utc_now() + timedelta(hours=24))
    completed_at: datetime | None = None


@dataclass
class ApprovalResult:
    """Result of an approval decision."""
    request_id: str
    approved: bool
    approver: str
    status: ApprovalStatus
    comment: str = ""
    timestamp: datetime = field(default_factory=utc_now)


# ─── Approval Service ─────────────────────────────────────────────────────────

class ApprovalService:
    """Three-tier approval workflow engine (EX-06).

    Manages the full lifecycle of high-risk action approval:
    - Request creation with tier determination
    - Role-based approval/rejection
    - Expiration handling
    - Audit trail
    """

    def __init__(self, default_ttl_hours: int = 24) -> None:
        self._requests: dict[str, ApprovalRequest] = {}
        self._default_ttl = timedelta(hours=default_ttl_hours)
        self._audit: list[dict[str, Any]] = []

    # ── Tier Determination ──────────────────────────────────────────────────

    def get_required_tier(self, action: Action) -> ApprovalTier:
        """Return the approval tier required for an action (EX-06)."""
        return ACTION_TIER_MAP.get(action, ApprovalTier.L1)

    def requires_approval(self, action: Action) -> bool:
        """Return True if the given action requires approval before execution."""
        return action in ACTION_TIER_MAP

    def get_required_approvers(self, tier: ApprovalTier) -> list[str]:
        """Return the list of role identifiers that must approve at each tier."""
        if tier == ApprovalTier.L1:
            return ["OPERATOR", "RISK", "TRADER", "ADMIN"]
        if tier == ApprovalTier.L2:
            return ["RISK", "RISK_OFFICER", "ADMIN"]
        if tier == ApprovalTier.L3:
            return ["RISK_OFFICER", "COMPLIANCE"]
        return []

    # ── Request Lifecycle ────────────────────────────────────────────────────

    def create_request(
        self,
        actor: str,
        action: Action,
        resource: str,
        details: dict[str, Any] | None = None,
        ttl_hours: int | None = None,
    ) -> ApprovalRequest:
        """Submit a new approval request for a high-risk action (EX-06).

        Returns the created ApprovalRequest with PENDING status.
        """
        if not self.requires_approval(action):
            raise ValueError(f"Action {action.value} does not require approval")

        tier = self.get_required_tier(action)
        required = self.get_required_approvers(tier)
        ttl = ttl_hours or 24

        request = ApprovalRequest(
            request_id=f"apr-{uuid.uuid4().hex[:12]}",
            actor=actor,
            action=action,
            tier=tier,
            resource=resource,
            details=details or {},
            status=ApprovalStatus.PENDING,
            required_approvers=required,
            approvals={},
            expires_at=utc_now() + timedelta(hours=ttl),
        )

        self._requests[request.request_id] = request
        self._audit.append({
            "event": "request_created",
            "request_id": request.request_id,
            "actor": actor,
            "action": action.value,
            "tier": tier.value,
            "resource": resource,
            "timestamp": utc_now().isoformat(),
        })

        return request

    def approve(
        self,
        request_id: str,
        approver: str,
        approver_role: str,
        comment: str = "",
    ) -> ApprovalResult:
        """Approve a pending request (EX-06).

        Approval rules:
        - L1: any single OPERATOR/RISK/TRADER/ADMIN role approves
        - L2: RISK/RISK_OFFICER/ADMIN must approve
        - L3: RISK_OFFICER + COMPLIANCE must both approve (dual approval)

        Returns ApprovalResult indicating whether the request is now fully approved.
        """
        request = self._requests.get(request_id)
        if request is None:
            return ApprovalResult(request_id=request_id, approved=False, approver=approver, status=ApprovalStatus.REJECTED, comment="Request not found")

        if request.status != ApprovalStatus.PENDING:
            return ApprovalResult(request_id=request_id, approved=False, approver=approver, status=request.status, comment=f"Request is {request.status.value}")

        # Check expiration
        if utc_now() > request.expires_at:
            request.status = ApprovalStatus.EXPIRED
            return ApprovalResult(request_id=request_id, approved=False, approver=approver, status=ApprovalStatus.EXPIRED, comment="Request expired")

        # Validate approver role for this tier
        if not self._is_valid_approver(request.tier, approver_role):
            return ApprovalResult(request_id=request_id, approved=False, approver=approver, status=ApprovalStatus.REJECTED, comment=f"Role {approver_role} cannot approve tier {request.tier.value}")

        # Record approval
        request.approvals[approver] = {
            "status": ApprovalStatus.APPROVED.value,
            "comment": comment,
            "timestamp": utc_now().isoformat(),
            "role": approver_role,
        }

        self._audit.append({
            "event": "approval_given",
            "request_id": request_id,
            "approver": approver,
            "role": approver_role,
            "tier": request.tier.value,
            "timestamp": utc_now().isoformat(),
        })

        # Check if fully approved
        fully_approved = self._is_fully_approved(request)
        if fully_approved:
            request.status = ApprovalStatus.APPROVED
            request.completed_at = utc_now()

        return ApprovalResult(
            request_id=request_id,
            approved=fully_approved,
            approver=approver,
            status=request.status,
            comment="Approved" if fully_approved else f"Approval recorded. {self._remaining_approvers_msg(request)}",
        )

    def reject(
        self,
        request_id: str,
        rejector: str,
        rejector_role: str,
        reason: str = "",
    ) -> ApprovalResult:
        """Reject a pending request (EX-06).

        Any valid approver can reject a request. Rejection is final.
        """
        request = self._requests.get(request_id)
        if request is None:
            return ApprovalResult(request_id=request_id, approved=False, approver=rejector, status=ApprovalStatus.REJECTED, comment="Request not found")

        if request.status != ApprovalStatus.PENDING:
            return ApprovalResult(request_id=request_id, approved=False, approver=rejector, status=request.status, comment=f"Request is {request.status.value}")

        if not self._is_valid_approver(request.tier, rejector_role):
            return ApprovalResult(request_id=request_id, approved=False, approver=rejector, status=ApprovalStatus.REJECTED, comment=f"Role {rejector_role} cannot approve tier {request.tier.value}")

        request.status = ApprovalStatus.REJECTED
        request.completed_at = utc_now()
        request.approvals[rejector] = {
            "status": ApprovalStatus.REJECTED.value,
            "comment": reason,
            "timestamp": utc_now().isoformat(),
            "role": rejector_role,
        }

        self._audit.append({
            "event": "request_rejected",
            "request_id": request_id,
            "rejector": rejector,
            "role": rejector_role,
            "reason": reason,
            "timestamp": utc_now().isoformat(),
        })

        return ApprovalResult(
            request_id=request_id,
            approved=False,
            approver=rejector,
            status=ApprovalStatus.REJECTED,
            comment=reason,
        )

    def cancel(self, request_id: str, actor: str) -> bool:
        """Cancel a pending request (only the original actor can cancel)."""
        request = self._requests.get(request_id)
        if request is None or request.status != ApprovalStatus.PENDING:
            return False
        if request.actor != actor:
            return False
        request.status = ApprovalStatus.CANCELLED
        request.completed_at = utc_now()
        return True

    def get_request(self, request_id: str) -> ApprovalRequest | None:
        """Get an approval request by ID."""
        return self._requests.get(request_id)

    def list_pending(self, actor: str | None = None, action: Action | None = None) -> list[ApprovalRequest]:
        """List pending approval requests, optionally filtered."""
        results = [r for r in self._requests.values() if r.status == ApprovalStatus.PENDING]
        if actor:
            results = [r for r in results if r.actor == actor]
        if action:
            results = [r for r in results if r.action == action]
        return sorted(results, key=lambda r: r.created_at)

    def list_by_approver(self, approver_role: str) -> list[ApprovalRequest]:
        """List pending requests that a given role can approve."""
        pending = self.list_pending()
        return [r for r in pending if self._is_valid_approver(r.tier, approver_role)]

    def cleanup_expired(self) -> int:
        """Mark expired requests as EXPIRED. Returns count of expired requests."""
        count = 0
        now = utc_now()
        for req in self._requests.values():
            if req.status == ApprovalStatus.PENDING and now > req.expires_at:
                req.status = ApprovalStatus.EXPIRED
                count += 1
        return count

    def get_audit_log(self, limit: int = 100) -> list[dict[str, Any]]:
        """Return the approval audit log (most recent first)."""
        return sorted(self._audit, key=lambda e: e["timestamp"], reverse=True)[-limit:]

    # ── Internal Helpers ────────────────────────────────────────────────────

    def _is_valid_approver(self, tier: ApprovalTier, role: str) -> bool:
        """Return True if the given role can approve at the specified tier."""
        if tier == ApprovalTier.L1:
            return role in ("OPERATOR", "RISK", "TRADER", "ADMIN", "RISK_OFFICER", "AUDITOR")
        if tier == ApprovalTier.L2:
            return role in ("RISK", "RISK_OFFICER", "ADMIN")
        if tier == ApprovalTier.L3:
            return role in ("RISK_OFFICER", "COMPLIANCE")
        return False

    def _is_fully_approved(self, request: ApprovalRequest) -> bool:
        """Return True if all required approvers have approved."""
        required = set(request.required_approvers)
        approved = set(
            approver
            for approver, approval in request.approvals.items()
            if approval["status"] == ApprovalStatus.APPROVED.value
        )
        # L3 requires both RISK_OFFICER and COMPLIANCE
        if request.tier == ApprovalTier.L3:
            return ("RISK_OFFICER" in approved or "COMPLIANCE" in approved) and len(approved) >= 2
        return bool(required & approved)

    def _remaining_approvers_msg(self, request: ApprovalRequest) -> str:
        approved = set(
            a for a, ap in request.approvals.items()
            if ap["status"] == ApprovalStatus.APPROVED.value
        )
        remaining = [r for r in request.required_approvers if r not in approved]
        if not remaining:
            return "All approvals received"
        return f"Still needed: {', '.join(remaining)}"
