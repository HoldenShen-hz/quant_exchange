"""Strategy marketplace service (MKT-01 ~ MKT-06).

Covers:
- Strategy template submission and listing
- Review and rating system
- Business models: free, purchase, subscription, revenue share
- Trial and verification sandbox
- Developer tools: CLI templates, sandbox, earnings dashboard
- Version management and authorization
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

class ListingStatus(str, Enum):
    DRAFT = "draft"
    PENDING_REVIEW = "pending_review"
    APPROVED = "approved"
    REJECTED = "rejected"
    RETIRED = "retired"


class BusinessModel(str, Enum):
    FREE = "free"
    PURCHASE = "purchase"    # One-time purchase
    SUBSCRIPTION = "subscription"  # Monthly/annual
    REVENUE_SHARE = "revenue_share"  # % of follower profits


class ReviewStatus(str, Enum):
    PENDING = "pending"
    APPROVED = "approved"
    FLAGGED = "flagged"
    REMOVED = "removed"


# ─────────────────────────────────────────────────────────────────────────────
# Data Models
# ─────────────────────────────────────────────────────────────────────────────

@dataclass(slots=True)
class StrategyTemplate:
    """A strategy template listed in the marketplace."""

    template_id: str
    author_id: str
    name: str
    description: str
    version: str
    listing_status: ListingStatus
    business_model: BusinessModel
    tags: tuple[str, ...]
    price: float = 0.0  # 0 for free
    subscription_price: float = 0.0  # monthly
    revenue_share_pct: float = 0.0
    download_count: int = 0
    follower_count: int = 0
    avg_rating: float = 0.0
    review_count: int = 0
    created_at: str = field(default_factory=_now)
    updated_at: str = field(default_factory=_now)


@dataclass(slots=True)
class VersionEntry:
    """A specific version of a strategy template."""

    version_id: str
    template_id: str
    version: str
    changelog: str = ""
    status: ListingStatus = ListingStatus.APPROVED
    created_at: str = field(default_factory=_now)


@dataclass(slots=True)
class StrategyReview:
    """A user review of a strategy template."""

    review_id: str
    template_id: str
    user_id: str
    rating: float  # 1-5
    title: str
    content: str
    status: ReviewStatus = ReviewStatus.APPROVED
    helpful_count: int = 0
    created_at: str = field(default_factory=_now)


@dataclass(slots=True)
class Subscription:
    """A subscription to a strategy."""

    subscription_id: str
    template_id: str
    subscriber_id: str
    started_at: str = field(default_factory=_now)
    expires_at: str | None = None
    is_active: bool = True


@dataclass(slots=True)
class DeveloperEarnings:
    """Developer earnings record."""

    period: str  # "2026-01"
    developer_id: str
    template_id: str
    revenue_type: str  # purchase, subscription, share
    gross_revenue: float
    platform_fee: float
    net_revenue: float
    paid_at: str | None = None


# ─────────────────────────────────────────────────────────────────────────────
# Strategy Marketplace Service
# ─────────────────────────────────────────────────────────────────────────────

class StrategyMarketplaceService:
    """Strategy marketplace service (MKT-01 ~ MKT-06).

    Provides:
    - Strategy template submission, review, and listing
    - Business model support: free, purchase, subscription, revenue share
    - Trial and sandbox verification
    - Developer tools: templates, earnings dashboard
    - Version management and authorization
    """

    def __init__(self, persistence=None) -> None:
        self.persistence = persistence
        self._templates: dict[str, StrategyTemplate] = {}
        self._versions: dict[str, list[VersionEntry]] = defaultdict(list)
        self._reviews: dict[str, list[StrategyReview]] = defaultdict(list)
        self._subscriptions: dict[str, Subscription] = {}
        self._purchases: set[tuple[str, str]] = set()  # (user_id, template_id)
        self._earnings: list[DeveloperEarnings] = []
        self._platform_fee_pct = 0.20  # 20% platform fee

    # ── Template Management ─────────────────────────────────────────────────

    def submit_template(
        self,
        author_id: str,
        name: str,
        description: str,
        version: str,
        tags: tuple[str, ...],
        business_model: BusinessModel = BusinessModel.FREE,
        price: float = 0.0,
        subscription_price: float = 0.0,
        revenue_share_pct: float = 0.0,
    ) -> StrategyTemplate:
        """Submit a new strategy template for review."""
        template_id = f"tpl:{uuid.uuid4().hex[:12]}"
        template = StrategyTemplate(
            template_id=template_id,
            author_id=author_id,
            name=name,
            description=description,
            version=version,
            listing_status=ListingStatus.PENDING_REVIEW,
            business_model=business_model,
            price=price,
            subscription_price=subscription_price,
            revenue_share_pct=revenue_share_pct,
            tags=tags,
        )
        self._templates[template_id] = template

        # Create first version
        version_entry = VersionEntry(
            version_id=f"{template_id}:{version}",
            template_id=template_id,
            version=version,
            status=ListingStatus.PENDING_REVIEW,
        )
        self._versions[template_id].append(version_entry)

        return template

    def approve_template(self, template_id: str) -> bool:
        """Approve a pending strategy template."""
        template = self._templates.get(template_id)
        if not template:
            return False
        template.listing_status = ListingStatus.APPROVED
        template.updated_at = _now()
        return True

    def reject_template(self, template_id: str, reason: str) -> bool:
        """Reject a pending strategy template."""
        template = self._templates.get(template_id)
        if not template:
            return False
        template.listing_status = ListingStatus.REJECTED
        template.updated_at = _now()
        return True

    def retire_template(self, template_id: str) -> bool:
        """Retire a strategy template from the marketplace."""
        template = self._templates.get(template_id)
        if not template:
            return False
        template.listing_status = ListingStatus.RETIRED
        template.updated_at = _now()
        return True

    def get_template(self, template_id: str) -> StrategyTemplate | None:
        """Get a strategy template by ID."""
        return self._templates.get(template_id)

    def list_templates(
        self,
        *,
        status: ListingStatus | None = ListingStatus.APPROVED,
        tags: list[str] | None = None,
        business_model: BusinessModel | None = None,
        author_id: str | None = None,
        sort_by: str = "download_count",
        limit: int = 20,
        offset: int = 0,
    ) -> list[StrategyTemplate]:
        """List marketplace templates with filters."""
        results = []
        for tpl in self._templates.values():
            if status and tpl.listing_status != status:
                continue
            if author_id and tpl.author_id != author_id:
                continue
            if business_model and tpl.business_model != business_model:
                continue
            if tags and not any(tag in tpl.tags for tag in tags):
                continue
            results.append(tpl)

        # Sort
        if sort_by == "download_count":
            results.sort(key=lambda t: t.download_count, reverse=True)
        elif sort_by == "avg_rating":
            results.sort(key=lambda t: t.avg_rating, reverse=True)
        elif sort_by == "follower_count":
            results.sort(key=lambda t: t.follower_count, reverse=True)
        elif sort_by == "created_at":
            results.sort(key=lambda t: t.created_at, reverse=True)

        return results[offset:offset + limit]

    # ── Version Management ─────────────────────────────────────────────────

    def publish_version(
        self,
        template_id: str,
        version: str,
        changelog: str = "",
    ) -> VersionEntry | None:
        """Publish a new version of a strategy template."""
        template = self._templates.get(template_id)
        if not template:
            return None

        entry = VersionEntry(
            version_id=f"{template_id}:{version}",
            template_id=template_id,
            version=version,
            changelog=changelog,
            status=ListingStatus.PENDING_REVIEW,
        )
        self._versions[template_id].append(entry)
        template.version = version
        template.updated_at = _now()
        return entry

    def get_versions(self, template_id: str) -> list[VersionEntry]:
        """Get all versions of a template."""
        return self._versions.get(template_id, [])

    # ── Reviews ─────────────────────────────────────────────────────────────

    def submit_review(
        self,
        template_id: str,
        user_id: str,
        rating: float,
        title: str,
        content: str,
    ) -> StrategyReview:
        """Submit a review for a strategy template."""
        review_id = f"rev:{uuid.uuid4().hex[:12]}"
        review = StrategyReview(
            review_id=review_id,
            template_id=template_id,
            user_id=user_id,
            rating=rating,
            title=title,
            content=content,
        )
        self._reviews[template_id].append(review)
        self._recalc_rating(template_id)
        return review

    def flag_review(self, review_id: str) -> bool:
        """Flag a review for moderation."""
        for reviews in self._reviews.values():
            for review in reviews:
                if review.review_id == review_id:
                    review.status = ReviewStatus.FLAGGED
                    return True
        return False

    def get_reviews(self, template_id: str) -> list[StrategyReview]:
        """Get approved reviews for a template."""
        return [
            r for r in self._reviews.get(template_id, [])
            if r.status == ReviewStatus.APPROVED
        ]

    def _recalc_rating(self, template_id: str) -> None:
        """Recalculate average rating for a template."""
        reviews = self.get_reviews(template_id)
        template = self._templates.get(template_id)
        if not reviews or not template:
            return
        template.avg_rating = sum(r.rating for r in reviews) / len(reviews)
        template.review_count = len(reviews)

    # ── Business Model: Purchase ─────────────────────────────────────────────

    def purchase_template(self, user_id: str, template_id: str) -> bool:
        """Record a one-time purchase of a template."""
        template = self._templates.get(template_id)
        if not template or template.business_model != BusinessModel.PURCHASE:
            return False
        key = (user_id, template_id)
        if key in self._purchases:
            return True  # Already purchased
        self._purchases.add(key)
        template.download_count += 1
        self._record_earnings(template.author_id, template_id, "purchase", template.price)
        return True

    def has_purchased(self, user_id: str, template_id: str) -> bool:
        """Check if user has purchased a template."""
        return (user_id, template_id) in self._purchases

    # ── Business Model: Subscription ────────────────────────────────────────

    def subscribe(
        self,
        user_id: str,
        template_id: str,
        duration_days: int = 30,
    ) -> Subscription | None:
        """Subscribe to a template with monthly billing."""
        template = self._templates.get(template_id)
        if not template:
            return None

        subscription_id = f"sub:{uuid.uuid4().hex[:12]}"
        expires_at = datetime.now(timezone.utc).timestamp() + duration_days * 86400
        subscription = Subscription(
            subscription_id=subscription_id,
            template_id=template_id,
            subscriber_id=user_id,
            expires_at=datetime.fromtimestamp(expires_at, tz=timezone.utc).isoformat(),
        )
        self._subscriptions[subscription_id] = subscription
        template.follower_count += 1
        self._record_earnings(
            template.author_id, template_id, "subscription",
            template.subscription_price * duration_days / 30,
        )
        return subscription

    def cancel_subscription(self, subscription_id: str) -> bool:
        """Cancel an active subscription."""
        sub = self._subscriptions.get(subscription_id)
        if not sub:
            return False
        sub.is_active = False
        return True

    def is_subscribed(self, user_id: str, template_id: str) -> bool:
        """Check if user has an active subscription."""
        for sub in self._subscriptions.values():
            if sub.subscriber_id == user_id and sub.template_id == template_id and sub.is_active:
                if sub.expires_at:
                    exp = datetime.fromisoformat(sub.expires_at)
                    if exp > datetime.now(timezone.utc):
                        return True
        return False

    # ── Earnings ─────────────────────────────────────────────────────────────

    def _record_earnings(
        self,
        developer_id: str,
        template_id: str,
        revenue_type: str,
        gross: float,
    ) -> None:
        """Record earnings for a developer."""
        platform_fee = gross * self._platform_fee_pct
        net = gross - platform_fee
        period = datetime.now(timezone.utc).strftime("%Y-%m")
        earnings = DeveloperEarnings(
            period=period,
            developer_id=developer_id,
            template_id=template_id,
            revenue_type=revenue_type,
            gross_revenue=gross,
            platform_fee=platform_fee,
            net_revenue=net,
        )
        self._earnings.append(earnings)

    def get_developer_earnings(
        self,
        developer_id: str,
        period: str | None = None,
    ) -> list[DeveloperEarnings]:
        """Get earnings records for a developer."""
        results = [e for e in self._earnings if e.developer_id == developer_id]
        if period:
            results = [e for e in results if e.period == period]
        return results

    def get_developer_total_earnings(self, developer_id: str) -> float:
        """Get total net earnings for a developer."""
        return sum(
            e.net_revenue
            for e in self._earnings
            if e.developer_id == developer_id
        )

    # ── Trial / Sandbox ─────────────────────────────────────────────────────

    def start_trial(self, user_id: str, template_id: str, days: int = 7) -> dict:
        """Start a free trial of a strategy template."""
        template = self._templates.get(template_id)
        if not template:
            return {"success": False, "reason": "template_not_found"}

        return {
            "success": True,
            "trial_id": f"trial:{uuid.uuid4().hex[:12]}",
            "template_id": template_id,
            "user_id": user_id,
            "duration_days": days,
            "started_at": _now(),
        }

    def verify_template(self, template_id: str) -> dict:
        """Run template through verification sandbox."""
        template = self._templates.get(template_id)
        if not template:
            return {"success": False, "reason": "not_found"}

        # Placeholder for sandbox verification
        return {
            "success": True,
            "template_id": template_id,
            "verified_at": _now(),
            "warnings": [],
            "passed": True,
        }
