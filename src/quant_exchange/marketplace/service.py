"""Strategy marketplace service (MKT-01~MKT-06).

Covers:
- MKT-01: Strategy listing and search
- MKT-02: Strategy reviews and ratings
- MKT-03: Purchase and licensing
- MKT-04: Revenue sharing and payouts
- MKT-05: Featured strategies and promotions
- MKT-06: Marketplace moderation and compliance
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any


class ListingStatus(str, Enum):
    DRAFT = "draft"
    LISTED = "listed"
    SUSPENDED = "suspended"
    DELISTED = "delisted"


class OrderStatus(str, Enum):
    PENDING = "pending"
    COMPLETED = "completed"
    REFUNDED = "refunded"
    DISPUTED = "disputed"


@dataclass(slots=True)
class StrategyListing:
    """A strategy listed on the marketplace."""

    listing_id: str
    user_id: str
    strategy_name: str
    strategy_type: str
    description: str
    version: str
    price: float  # one-time purchase price
    subscription_price: float = 0.0  # monthly subscription
    license_type: str = "single_user"  # single_user/multi_user/unlimited
    tags: list[str] = field(default_factory=list)
    downloads: int = 0
    rating_avg: float = 0.0
    rating_count: int = 0
    status: ListingStatus = ListingStatus.LISTED
    is_featured: bool = False
    is_verified: bool = False
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass(slots=True)
class Review:
    """A review for a marketplace strategy."""

    review_id: str
    listing_id: str
    buyer_id: str
    rating: int  # 1-5
    title: str
    content: str
    helpful_count: int = 0
    response_from_seller: str = ""
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass(slots=True)
class Order:
    """A purchase order for a marketplace strategy."""

    order_id: str
    listing_id: str
    buyer_id: str
    seller_id: str
    amount: float
    license_type: str
    platform_fee: float = 0.0
    seller_payout: float = 0.0
    status: OrderStatus = OrderStatus.PENDING
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


class MarketplaceService:
    """Strategy marketplace service (MKT-01~MKT-06)."""

    PLATFORM_FEE_RATE = 0.15  # 15% platform fee

    def __init__(self, persistence=None) -> None:
        self.persistence = persistence
        self._listings: dict[str, StrategyListing] = {}
        self._reviews: dict[str, Review] = {}
        self._orders: dict[str, Order] = {}
        self._init_demo_data()

    def _init_demo_data(self) -> None:
        listings = [
            StrategyListing(listing_id="mkt001", user_id="u001", strategy_name="期权波动率套利策略", strategy_type="volatility_arbitrage", description="基于期权波动率曲面套利的成熟策略，年化收益25%，夏普比率2.3", version="2.1", price=2999.0, subscription_price=299.0, tags=["期权", "波动率", "套利"], downloads=156, rating_avg=4.7, rating_count=42, is_featured=True, is_verified=True),
            StrategyListing(listing_id="mkt002", user_id="u002", strategy_name="CTA趋势跟踪系统", strategy_type="cta_trend", description="CTA趋势跟踪策略，适用于期货和外汇市场趋势行情", version="1.5", price=1999.0, subscription_price=199.0, tags=["CTA", "趋势", "期货"], downloads=234, rating_avg=4.5, rating_count=67, is_verified=True),
            StrategyListing(listing_id="mkt003", user_id="u003", strategy_name="黄金网格做市策略", strategy_type="grid_mm", description="专为黄金设计的高频网格做市策略，震荡行情月均3%", version="3.0", price=4999.0, subscription_price=499.0, tags=["网格", "黄金", "做市"], downloads=89, rating_avg=4.2, rating_count=23),
        ]
        for l in listings:
            self._listings[l.listing_id] = l

        reviews = [
            Review(review_id="r001", listing_id="mkt001", buyer_id="u004", rating=5, title="非常实用的策略", content="波动率曲面分析功能强大，回测结果与实盘接近。", helpful_count=12),
            Review(review_id="r002", listing_id="mkt001", buyer_id="u005", rating=4, title="好策略", content="参数调优有一定工作量，整体满意。", helpful_count=8),
        ]
        for r in reviews:
            self._reviews[r.review_id] = r

    # ── MKT-01: Listings ────────────────────────────────────────────────────

    def create_listing(
        self,
        user_id: str,
        strategy_name: str,
        strategy_type: str,
        description: str,
        version: str,
        price: float,
        subscription_price: float = 0.0,
        license_type: str = "single_user",
        tags: list[str] | None = None,
    ) -> StrategyListing:
        """Create a new marketplace listing (MKT-01)."""
        listing = StrategyListing(
            listing_id=f"mkt:{uuid.uuid4().hex[:12]}",
            user_id=user_id,
            strategy_name=strategy_name,
            strategy_type=strategy_type,
            description=description,
            version=version,
            price=price,
            subscription_price=subscription_price,
            license_type=license_type,
            tags=tags or [],
        )
        self._listings[listing.listing_id] = listing
        return listing

    def get_listing(self, listing_id: str) -> StrategyListing | None:
        """Get a marketplace listing."""
        return self._listings.get(listing_id)

    def list_listings(
        self,
        strategy_type: str | None = None,
        tag: str | None = None,
        sort_by: str = "popular",
        min_price: float | None = None,
        max_price: float | None = None,
        limit: int = 20,
        offset: int = 0,
    ) -> list[StrategyListing]:
        """List marketplace strategies (MKT-01)."""
        results = [l for l in self._listings.values() if l.status == ListingStatus.LISTED]
        if strategy_type:
            results = [l for l in results if l.strategy_type == strategy_type]
        if tag:
            results = [l for l in results if tag in l.tags]
        if min_price is not None:
            results = [l for l in results if l.price >= min_price]
        if max_price is not None:
            results = [l for l in results if l.price <= max_price]

        if sort_by == "popular":
            results.sort(key=lambda l: l.downloads, reverse=True)
        elif sort_by == "rating":
            results.sort(key=lambda l: l.rating_avg, reverse=True)
        elif sort_by == "newest":
            results.sort(key=lambda l: l.created_at, reverse=True)
        elif sort_by == "price_asc":
            results.sort(key=lambda l: l.price)
        elif sort_by == "price_desc":
            results.sort(key=lambda l: l.price, reverse=True)

        return results[offset : offset + limit]

    def get_featured_listings(self) -> list[StrategyListing]:
        """Get featured strategies (MKT-05)."""
        return [l for l in self._listings.values() if l.is_featured and l.status == ListingStatus.LISTED]

    def search_listings(self, query: str, limit: int = 20) -> list[StrategyListing]:
        """Search strategies by name or description."""
        q = query.lower()
        results = [l for l in self._listings.values() if l.status == ListingStatus.LISTED and (q in l.strategy_name.lower() or q in l.description.lower() or any(q in tag.lower() for tag in l.tags))]
        results.sort(key=lambda l: l.rating_avg * l.rating_count, reverse=True)
        return results[:limit]

    # ── MKT-02: Reviews ────────────────────────────────────────────────────

    def add_review(
        self,
        listing_id: str,
        buyer_id: str,
        rating: int,
        title: str,
        content: str,
    ) -> Review | None:
        """Add a review for a purchased strategy (MKT-02)."""
        listing = self._listings.get(listing_id)
        if not listing:
            return None

        review = Review(
            review_id=f"rev:{uuid.uuid4().hex[:12]}",
            listing_id=listing_id,
            buyer_id=buyer_id,
            rating=rating,
            title=title,
            content=content,
        )
        self._reviews[review.review_id] = review

        # Update listing rating
        listing_reviews = [r for r in self._reviews.values() if r.listing_id == listing_id]
        listing.rating_avg = sum(r.rating for r in listing_reviews) / len(listing_reviews)
        listing.rating_count = len(listing_reviews)

        return review

    def list_reviews(self, listing_id: str) -> list[Review]:
        """List reviews for a strategy."""
        reviews = [r for r in self._reviews.values() if r.listing_id == listing_id]
        reviews.sort(key=lambda r: r.helpful_count, reverse=True)
        return reviews

    def mark_review_helpful(self, review_id: str) -> bool:
        """Mark a review as helpful."""
        review = self._reviews.get(review_id)
        if not review:
            return False
        review.helpful_count += 1
        return True

    # ── MKT-03: Purchase ───────────────────────────────────────────────────

    def purchase_strategy(
        self,
        listing_id: str,
        buyer_id: str,
        license_type: str,
    ) -> Order | None:
        """Purchase a strategy license (MKT-03)."""
        listing = self._listings.get(listing_id)
        if not listing or listing.status != ListingStatus.LISTED:
            return None

        amount = listing.price if license_type == "one_time" else listing.subscription_price
        if amount <= 0:
            return None

        platform_fee = amount * self.PLATFORM_FEE_RATE
        seller_payout = amount - platform_fee

        order = Order(
            order_id=f"ord:{uuid.uuid4().hex[:12]}",
            listing_id=listing_id,
            buyer_id=buyer_id,
            seller_id=listing.user_id,
            amount=amount,
            license_type=license_type,
            platform_fee=platform_fee,
            seller_payout=seller_payout,
            status=OrderStatus.COMPLETED,
        )
        self._orders[order.order_id] = order
        listing.downloads += 1
        return order

    def get_order(self, order_id: str) -> Order | None:
        """Get an order by ID."""
        return self._orders.get(order_id)

    def list_user_purchases(self, user_id: str) -> list[Order]:
        """List all purchases for a user."""
        return [o for o in self._orders.values() if o.buyer_id == user_id and o.status == OrderStatus.COMPLETED]

    # ── MKT-04: Revenue & Payouts ──────────────────────────────────────────

    def get_seller_revenue(self, user_id: str) -> dict[str, Any]:
        """Get revenue summary for a seller (MKT-04)."""
        orders = [o for o in self._orders.values() if o.seller_id == user_id and o.status == OrderStatus.COMPLETED]
        total_revenue = sum(o.amount for o in orders)
        total_payout = sum(o.seller_payout for o in orders)
        total_platform_fee = sum(o.platform_fee for o in orders)
        return {
            "seller_id": user_id,
            "total_orders": len(orders),
            "total_revenue": total_revenue,
            "total_payout": total_payout,
            "total_platform_fee": total_platform_fee,
            "pending_payout": total_payout,
        }

    def get_platform_revenue(self) -> dict[str, Any]:
        """Get total platform revenue."""
        total = sum(o.platform_fee for o in self._orders.values() if o.status == OrderStatus.COMPLETED)
        return {
            "total_revenue": total,
            "order_count": len(self._orders),
            "active_listings": sum(1 for l in self._listings.values() if l.status == ListingStatus.LISTED),
        }

    # ── MKT-05: Featured ───────────────────────────────────────────────────

    def set_featured(self, listing_id: str, featured: bool) -> bool:
        """Set or unset a listing as featured (MKT-05)."""
        listing = self._listings.get(listing_id)
        if not listing:
            return False
        listing.is_featured = featured
        return True

    def verify_listing(self, listing_id: str) -> bool:
        """Mark a listing as verified (MKT-06)."""
        listing = self._listings.get(listing_id)
        if not listing:
            return False
        listing.is_verified = True
        return True

    def suspend_listing(self, listing_id: str, reason: str = "") -> bool:
        """Suspend a listing (MKT-06)."""
        listing = self._listings.get(listing_id)
        if not listing:
            return False
        listing.status = ListingStatus.SUSPENDED
        return True
