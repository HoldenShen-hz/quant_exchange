"""Social and community service (SOC-01~SOC-06).

Covers:
- SOC-01: Forum posts/feed (create/comment/like/share)
- SOC-02: Strategy sharing (templates/parameters/live records)
- SOC-03: User profile (rankings/points/follow)
- SOC-04: Content recommendation (based on follow/performance)
- SOC-05: Private messages/system notifications
- SOC-06: Content moderation (keyword/违规 detection)
"""

from __future__ import annotations

import re
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any


# ─────────────────────────────────────────────────────────────────────────────
# Enums
# ─────────────────────────────────────────────────────────────────────────────


class PostType(str, Enum):
    TEXT = "text"
    STRATEGY_SHARE = "strategy_share"
    PERFORMANCE_POST = "performance_post"
    QUESTION = "question"
    ANNOUNCEMENT = "announcement"


class PostStatus(str, Enum):
    DRAFT = "draft"
    PUBLISHED = "published"
    HIDDEN = "hidden"
    DELETED = "deleted"


class ModerationStatus(str, Enum):
    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"


# ─────────────────────────────────────────────────────────────────────────────
# Data Models
# ─────────────────────────────────────────────────────────────────────────────


@dataclass(slots=True)
class UserProfile:
    """Community user profile."""

    user_id: str
    username: str
    display_name: str
    avatar_url: str = ""
    bio: str = ""
    rank: int = 0
    points: int = 0
    followers_count: int = 0
    following_count: int = 0
    strategies_count: int = 0
    posts_count: int = 0
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass(slots=True)
class Like:
    """A like on a post or comment."""

    like_id: str
    user_id: str
    target_id: str  # post_id or comment_id
    target_type: str  # "post" or "comment"
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass(slots=True)
class Comment:
    """A comment on a post."""

    comment_id: str
    post_id: str
    user_id: str
    username: str
    content: str
    likes_count: int = 0
    parent_comment_id: str | None = None  # for nested replies
    moderation_status: ModerationStatus = ModerationStatus.APPROVED
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass(slots=True)
class CommunityPost:
    """A community post or article."""

    post_id: str
    user_id: str
    username: str
    post_type: PostType
    title: str
    content: str
    likes_count: int = 0
    comments_count: int = 0
    views_count: int = 0
    shares_count: int = 0
    strategy_id: str | None = None  # linked strategy
    tags: list[str] = field(default_factory=list)
    status: PostStatus = PostStatus.PUBLISHED
    moderation_status: ModerationStatus = ModerationStatus.APPROVED
    is_pinned: bool = False
    is_featured: bool = False
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass(slots=True)
class StrategyShare:
    """A shared strategy with parameters and performance."""

    share_id: str
    post_id: str  # linked post
    user_id: str
    strategy_name: str
    strategy_type: str  # grid/ma/momentum/mean_reversion/etc
    parameters: dict[str, Any]  # strategy parameters
    performance_summary: dict[str, Any]  # key metrics
    is_public: bool = True
    usage_count: int = 0  # how many users copied it
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass(slots=True)
class Notification:
    """A notification for a user."""

    notification_id: str
    user_id: str
    notification_type: str  # like/comment/follow/mention/system/strategy_update
    title: str
    content: str
    related_post_id: str | None = None
    related_user_id: str | None = None
    is_read: bool = False
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass(slots=True)
class FollowRelation:
    """A follow relationship between users."""

    follower_id: str
    following_id: str
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


# ─────────────────────────────────────────────────────────────────────────────
# Moderation Engine
# ─────────────────────────────────────────────────────────────────────────────


class ContentModerator:
    """Content moderation for posts and comments (SOC-06)."""

    # Sensitive keywords (simplified - real implementation would use ML)
    BLOCKED_PATTERNS: list[str] = [
        r"(?i)fake\s*(?=.{0,4}(?:id|account|document))",
        r"(?i)pill",
        r"(?i)scam",
    ]

    SPAM_PATTERNS: list[str] = [
        r"https?://\S+",  # URLs
        r"(.)\1{5,}",  # repeated characters
        r"(?i)click\s*here",
        r"(?i)DM\s*me",
    ]

    def moderate(self, content: str) -> tuple[ModerationStatus, str]:
        """Check content and return moderation status + reason."""
        # Check blocked patterns
        for pattern in self.BLOCKED_PATTERNS:
            if re.search(pattern, content):
                return ModerationStatus.REJECTED, "Content violates community guidelines"

        # Check spam patterns (URLs need manual review)
        for pattern in self.SPAM_PATTERNS:
            if re.search(pattern, content):
                return ModerationStatus.PENDING, "Content requires manual review"

        return ModerationStatus.APPROVED, ""


# ─────────────────────────────────────────────────────────────────────────────
# Social Service
# ─────────────────────────────────────────────────────────────────────────────


class SocialService:
    """Social and community service (SOC-01~SOC-06)."""

    def __init__(self, persistence=None) -> None:
        self.persistence = persistence
        self._posts: dict[str, CommunityPost] = {}
        self._comments: dict[str, Comment] = {}
        self._likes: dict[str, Like] = {}
        self._strategy_shares: dict[str, StrategyShare] = {}
        self._notifications: dict[str, list[Notification]] = {}
        self._follows: dict[str, set[str]] = {}  # user_id -> set of following_ids
        self._user_profiles: dict[str, UserProfile] = {}
        self._moderator = ContentModerator()
        self._init_demo_data()

    def _init_demo_data(self) -> None:
        """Initialize demo data for testing."""
        now = datetime.now(timezone.utc)
        demo_users = [
            UserProfile(user_id="u001", username="alice_trader", display_name="Alice Chen", rank=1, points=9800, followers_count=342, following_count=89, strategies_count=5, posts_count=42),
            UserProfile(user_id="u002", username="bob_quant", display_name="Bob Zhang", rank=2, points=8750, followers_count=256, following_count=112, strategies_count=3, posts_count=28),
            UserProfile(user_id="u003", username="carol_algo", display_name="Carol Wang", rank=3, points=7620, followers_count=189, following_count=67, strategies_count=7, posts_count=55),
        ]
        for u in demo_users:
            self._user_profiles[u.user_id] = u

        demo_posts = [
            CommunityPost(post_id="p001", user_id="u001", username="alice_trader", post_type=PostType.STRATEGY_SHARE, title="网格策略BTC/USD做市策略", content="这是一个经典的网格做市策略，适用于高流动性币种。参数：价格间隔1%，每格仓位0.001 BTC。回测年化收益12%，最大回撤8%。", likes_count=24, comments_count=5, views_count=312, tags=["网格", "做市", "BTC"]),
            CommunityPost(post_id="p002", user_id="u002", username="bob_quant", post_type=PostType.PERFORMANCE_POST, title="3月收益总结 +15.6%", content="本月上证50期权波动率策略表现优异得益于降准预期。基于布林带均值回归配合隐含波动率过滤，策略在低波环境下表现稳健。", likes_count=45, comments_count=12, views_count=891, tags=["月报", "期权", "波动率"]),
            CommunityPost(post_id="p003", user_id="u003", username="carol_algo", post_type=PostType.QUESTION, title="关于VWAP执行算法的问题", content="在回测中使用VWAP算法时，订单量占总市场成交量比例过高会导致滑点估算偏差较大。大家有什么好的处理方式吗？", likes_count=8, comments_count=15, views_count=234, tags=["VWAP", "执行算法", "提问"]),
        ]
        for p in demo_posts:
            self._posts[p.post_id] = p

        demo_shares = [
            StrategyShare(share_id="ss001", post_id="p001", user_id="u001", strategy_name="BTC网格做市", strategy_type="grid", parameters={"price_interval_pct": 1.0, "size_per_grid": 0.001, "max_position": 1.0}, performance_summary={"annual_return": 0.12, "max_drawdown": 0.08, "sharpe_ratio": 1.5}, usage_count=23),
        ]
        for s in demo_shares:
            self._strategy_shares[s.share_id] = s

    # ── SOC-01: Posts & Feed ────────────────────────────────────────────────

    def create_post(
        self,
        user_id: str,
        post_type: PostType,
        title: str,
        content: str,
        tags: list[str] | None = None,
        strategy_id: str | None = None,
    ) -> CommunityPost:
        """Create a new post (SOC-01)."""
        username = self._user_profiles.get(user_id, UserProfile(user_id=user_id, username="unknown", display_name="Unknown")).username
        moderation_status, _ = self._moderator.moderate(content)

        post = CommunityPost(
            post_id=f"post:{uuid.uuid4().hex[:12]}",
            user_id=user_id,
            username=username,
            post_type=post_type,
            title=title,
            content=content,
            tags=tags or [],
            strategy_id=strategy_id,
            moderation_status=moderation_status,
            status=PostStatus.PUBLISHED if moderation_status == ModerationStatus.APPROVED else PostStatus.DRAFT,
        )
        self._posts[post.post_id] = post

        # Update user post count
        if user_id in self._user_profiles:
            self._user_profiles[user_id].posts_count += 1

        return post

    def get_post(self, post_id: str) -> CommunityPost | None:
        """Get a post by ID."""
        return self._posts.get(post_id)

    def list_posts(
        self,
        post_type: PostType | None = None,
        user_id: str | None = None,
        tag: str | None = None,
        limit: int = 20,
        offset: int = 0,
    ) -> list[CommunityPost]:
        """List posts with optional filters (SOC-01)."""
        results = [p for p in self._posts.values() if p.status == PostStatus.PUBLISHED]
        if post_type:
            results = [p for p in results if p.post_type == post_type]
        if user_id:
            results = [p for p in results if p.user_id == user_id]
        if tag:
            results = [p for p in results if tag in p.tags]
        results.sort(key=lambda p: (not p.is_pinned, not p.is_featured, p.created_at), reverse=True)
        return results[offset : offset + limit]

    def delete_post(self, post_id: str, user_id: str) -> bool:
        """Delete a post (own post only)."""
        post = self._posts.get(post_id)
        if not post or post.user_id != user_id:
            return False
        post.status = PostStatus.DELETED
        return True

    # ── SOC-01: Comments ─────────────────────────────────────────────────────

    def add_comment(
        self,
        post_id: str,
        user_id: str,
        content: str,
        parent_comment_id: str | None = None,
    ) -> Comment | None:
        """Add a comment to a post (SOC-01)."""
        if post_id not in self._posts:
            return None
        username = self._user_profiles.get(user_id, UserProfile(user_id=user_id, username="unknown", display_name="Unknown")).username
        moderation_status, _ = self._moderator.moderate(content)

        comment = Comment(
            comment_id=f"cmt:{uuid.uuid4().hex[:12]}",
            post_id=post_id,
            user_id=user_id,
            username=username,
            content=content,
            parent_comment_id=parent_comment_id,
            moderation_status=moderation_status,
        )
        self._comments[comment.comment_id] = comment

        # Update post comment count
        self._posts[post_id].comments_count += 1

        # Send notification to post author
        post = self._posts[post_id]
        self._send_notification(
            user_id=post.user_id,
            notification_type="comment",
            title="新评论",
            content=f"{username} 评论了你的帖子: {post.title}",
            related_post_id=post_id,
            related_user_id=user_id,
        )
        return comment

    def list_comments(self, post_id: str) -> list[Comment]:
        """List comments for a post."""
        return [c for c in self._comments.values() if c.post_id == post_id and c.moderation_status == ModerationStatus.APPROVED]

    def delete_comment(self, comment_id: str, user_id: str) -> bool:
        """Delete a comment (own comment only)."""
        comment = self._comments.get(comment_id)
        if not comment or comment.user_id != user_id:
            return False
        comment.content = "[已删除]"
        if comment.post_id in self._posts:
            self._posts[comment.post_id].comments_count = max(0, self._posts[comment.post_id].comments_count - 1)
        return True

    # ── SOC-01: Likes ───────────────────────────────────────────────────────

    def like(self, user_id: str, target_id: str, target_type: str) -> bool:
        """Like a post or comment (SOC-01)."""
        like_key = f"{user_id}:{target_id}"
        if like_key in self._likes:
            return False  # already liked

        like = Like(like_id=like_key, user_id=user_id, target_id=target_id, target_type=target_type)
        self._likes[like_key] = like

        # Update counts
        if target_type == "post" and target_id in self._posts:
            self._posts[target_id].likes_count += 1
            post = self._posts[target_id]
            self._send_notification(post.user_id, "like", "收到赞", f"{self._user_profiles.get(user_id, UserProfile(user_id=user_id, username='某用户', display_name='某用户')).username} 赞了你的帖子", related_post_id=target_id, related_user_id=user_id)
        elif target_type == "comment" and target_id in self._comments:
            self._comments[target_id].likes_count += 1
        return True

    def unlike(self, user_id: str, target_id: str) -> bool:
        """Unlike a post or comment."""
        like_key = f"{user_id}:{target_id}"
        if like_key not in self._likes:
            return False
        del self._likes[like_key]

        if target_id in self._posts:
            self._posts[target_id].likes_count = max(0, self._posts[target_id].likes_count - 1)
        elif target_id in self._comments:
            self._comments[target_id].likes_count = max(0, self._comments[target_id].likes_count - 1)
        return True

    # ── SOC-02: Strategy Sharing ─────────────────────────────────────────────

    def share_strategy(
        self,
        post_id: str,
        user_id: str,
        strategy_name: str,
        strategy_type: str,
        parameters: dict[str, Any],
        performance_summary: dict[str, Any],
    ) -> StrategyShare:
        """Share a strategy with the community (SOC-02)."""
        share = StrategyShare(
            share_id=f"ss:{uuid.uuid4().hex[:12]}",
            post_id=post_id,
            user_id=user_id,
            strategy_name=strategy_name,
            strategy_type=strategy_type,
            parameters=parameters,
            performance_summary=performance_summary,
        )
        self._strategy_shares[share.share_id] = share

        if user_id in self._user_profiles:
            self._user_profiles[user_id].strategies_count += 1

        return share

    def get_strategy_share(self, share_id: str) -> StrategyShare | None:
        """Get a strategy share by ID."""
        return self._strategy_shares.get(share_id)

    def list_strategy_shares(
        self,
        strategy_type: str | None = None,
        user_id: str | None = None,
        sort_by: str = "usage",
        limit: int = 20,
    ) -> list[StrategyShare]:
        """List public strategy shares (SOC-02)."""
        results = [s for s in self._strategy_shares.values() if s.is_public]
        if strategy_type:
            results = [s for s in results if s.strategy_type == strategy_type]
        if user_id:
            results = [s for s in results if s.user_id == user_id]

        if sort_by == "usage":
            results.sort(key=lambda s: s.usage_count, reverse=True)
        elif sort_by == "performance":
            results.sort(key=lambda s: s.performance_summary.get("annual_return", 0), reverse=True)
        return results[:limit]

    def copy_strategy(self, share_id: str, copying_user_id: str) -> dict[str, Any] | None:
        """Copy a shared strategy (SOC-02)."""
        share = self._strategy_shares.get(share_id)
        if not share:
            return None
        share.usage_count += 1
        return {
            "strategy_name": share.strategy_name,
            "strategy_type": share.strategy_type,
            "parameters": share.parameters.copy(),
        }

    # ── SOC-03: User Profiles & Follow ─────────────────────────────────────

    def get_user_profile(self, user_id: str) -> UserProfile | None:
        """Get a user profile (SOC-03)."""
        return self._user_profiles.get(user_id)

    def update_user_profile(self, user_id: str, display_name: str | None = None, bio: str | None = None, avatar_url: str | None = None) -> UserProfile | None:
        """Update user profile."""
        profile = self._user_profiles.get(user_id)
        if not profile:
            return None
        if display_name is not None:
            profile.display_name = display_name
        if bio is not None:
            profile.bio = bio
        if avatar_url is not None:
            profile.avatar_url = avatar_url
        return profile

    def follow(self, follower_id: str, following_id: str) -> bool:
        """Follow a user (SOC-03)."""
        if follower_id == following_id:
            return False
        if follower_id not in self._follows:
            self._follows[follower_id] = set()
        if following_id in self._follows[follower_id]:
            return False

        self._follows[follower_id].add(following_id)

        # Update counts
        if follower_id in self._user_profiles:
            self._user_profiles[follower_id].following_count += 1
        if following_id in self._user_profiles:
            self._user_profiles[following_id].followers_count += 1

        self._send_notification(following_id, "follow", "新粉丝", f"{self._user_profiles.get(follower_id, UserProfile(user_id=follower_id, username='某用户', display_name='某用户')).display_name} 关注了你", related_user_id=follower_id)
        return True

    def unfollow(self, follower_id: str, following_id: str) -> bool:
        """Unfollow a user."""
        if follower_id not in self._follows or following_id not in self._follows[follower_id]:
            return False
        self._follows[follower_id].discard(following_id)

        if follower_id in self._user_profiles:
            self._user_profiles[follower_id].following_count = max(0, self._user_profiles[follower_id].following_count - 1)
        if following_id in self._user_profiles:
            self._user_profiles[following_id].followers_count = max(0, self._user_profiles[following_id].followers_count - 1)
        return True

    def list_followers(self, user_id: str) -> list[UserProfile]:
        """List followers of a user."""
        follower_ids = set()
        for follower, following_set in self._follows.items():
            if user_id in following_set:
                follower_ids.add(follower)
        return [self._user_profiles[fid] for fid in follower_ids if fid in self._user_profiles]

    def list_following(self, user_id: str) -> list[UserProfile]:
        """List users that a user is following."""
        following_ids = self._follows.get(user_id, set())
        return [self._user_profiles[fid] for fid in following_ids if fid in self._user_profiles]

    def get_leaderboard(self, limit: int = 20) -> list[UserProfile]:
        """Get user ranking by points (SOC-03)."""
        profiles = sorted(self._user_profiles.values(), key=lambda p: p.points, reverse=True)
        return profiles[:limit]

    # ── SOC-04: Content Recommendation ──────────────────────────────────────

    def get_recommended_posts(self, user_id: str, limit: int = 20) -> list[CommunityPost]:
        """Get personalized post recommendations (SOC-04)."""
        following = self._follows.get(user_id, set())

        # Priority 1: posts from followed users
        from_following = [p for p in self._posts.values() if p.status == PostStatus.PUBLISHED and p.user_id in following]
        from_following.sort(key=lambda p: p.created_at, reverse=True)

        # Priority 2: high-performing posts
        others = [p for p in self._posts.values() if p.status == PostStatus.PUBLISHED and p.user_id not in following and p not in from_following]
        others.sort(key=lambda p: (p.likes_count * 2 + p.comments_count * 3 + p.views_count), reverse=True)

        combined = from_following[: limit // 2] + others[: limit - len(from_following)]
        return combined

    # ── SOC-05: Notifications ───────────────────────────────────────────────

    def _send_notification(
        self,
        user_id: str,
        notification_type: str,
        title: str,
        content: str,
        related_post_id: str | None = None,
        related_user_id: str | None = None,
    ) -> None:
        """Send a notification to a user (SOC-05)."""
        if user_id not in self._notifications:
            self._notifications[user_id] = []
        notif = Notification(
            notification_id=f"notif:{uuid.uuid4().hex[:12]}",
            user_id=user_id,
            notification_type=notification_type,
            title=title,
            content=content,
            related_post_id=related_post_id,
            related_user_id=related_user_id,
        )
        self._notifications[user_id].insert(0, notif)

    def get_notifications(self, user_id: str, unread_only: bool = False) -> list[Notification]:
        """Get user notifications (SOC-05)."""
        notifs = self._notifications.get(user_id, [])
        if unread_only:
            notifs = [n for n in notifs if not n.is_read]
        return notifs

    def mark_notification_read(self, notification_id: str, user_id: str) -> bool:
        """Mark a notification as read."""
        for notif in self._notifications.get(user_id, []):
            if notif.notification_id == notification_id:
                notif.is_read = True
                return True
        return False

    def mark_all_read(self, user_id: str) -> int:
        """Mark all notifications as read."""
        count = 0
        for notif in self._notifications.get(user_id, []):
            if not notif.is_read:
                notif.is_read = True
                count += 1
        return count

    # ── SOC-06: Moderation ─────────────────────────────────────────────────

    def moderate_post(self, post_id: str, action: str) -> bool:
        """Moderate a post (approve/reject) (SOC-06)."""
        post = self._posts.get(post_id)
        if not post:
            return False
        if action == "approve":
            post.moderation_status = ModerationStatus.APPROVED
            post.status = PostStatus.PUBLISHED
        elif action == "reject":
            post.moderation_status = ModerationStatus.REJECTED
            post.status = PostStatus.HIDDEN
        return True

    def moderate_comment(self, comment_id: str, action: str) -> bool:
        """Moderate a comment (approve/reject)."""
        comment = self._comments.get(comment_id)
        if not comment:
            return False
        if action == "approve":
            comment.moderation_status = ModerationStatus.APPROVED
        elif action == "reject":
            comment.moderation_status = ModerationStatus.REJECTED
        return True
