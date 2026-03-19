"""Social trading community service (SOC-01 ~ SOC-06).

Covers:
- Trading ideas with rich content
- Interactive features: likes, comments, follows, bookmarks, reports
- Discussion forums by market/instrument/theme
- Prediction tracking with target/stop
- Reputation system
- Long-form research reports
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

class PostType(str, Enum):
    IDEA = "idea"         # Short trading idea
    REPORT = "report"    # Long-form research report
    QUESTION = "question" # Asking the community
    STRATEGY = "strategy"  # Strategy sharing


class ReactionType(str, Enum):
    LIKE = "like"
    CELEBRATE = "celebrate"   # For big wins
    INSIGHTFUL = "insightful"  # For quality analysis
    DISAGREE = "disagree"


# ─────────────────────────────────────────────────────────────────────────────
# Data Models
# ─────────────────────────────────────────────────────────────────────────────

@dataclass(slots=True)
class UserProfile:
    """Community user profile with reputation."""

    user_id: str
    username: str
    avatar_url: str = ""
    bio: str = ""
    follower_count: int = 0
    following_count: int = 0
    reputation_score: float = 0.0
    hit_rate: float = 0.0   # % of predictions that panned out
    total_ideas: int = 0
    joined_at: str = field(default_factory=_now)


@dataclass(slots=True)
class Post:
    """A community post (idea, report, question, or strategy)."""

    post_id: str
    user_id: str
    post_type: PostType
    title: str
    content: str          # Markdown supported
    instrument_id: str | None
    tags: tuple[str, ...]
    like_count: int = 0
    comment_count: int = 0
    share_count: int = 0
    is_pinned: bool = False
    is_deleted: bool = False
    created_at: str = field(default_factory=_now)
    updated_at: str = field(default_factory=_now)


@dataclass(slots=True)
class Prediction:
    """Prediction attached to a trading idea."""

    prediction_id: str
    post_id: str
    entry_price: float
    target_price: float
    stop_price: float
    current_price: float
    status: str = "active"  # active, hit, stopped, cancelled
    resolved_at: str | None = None


@dataclass(slots=True)
class Comment:
    """Comment on a post."""

    comment_id: str
    post_id: str
    user_id: str
    parent_comment_id: str | None  # None = top-level
    content: str
    like_count: int = 0
    is_deleted: bool = False
    created_at: str = field(default_factory=_now)


@dataclass(slots=True)
class FollowRelation:
    """Follow relationship between users."""

    follower_id: str
    followee_id: str
    created_at: str = field(default_factory=_now)


@dataclass(slots=True)
class Reaction:
    """Reaction to a post."""

    reaction_id: str
    post_id: str
    user_id: str
    reaction_type: ReactionType
    created_at: str = field(default_factory=_now)


# ─────────────────────────────────────────────────────────────────────────────
# Social Service
# ─────────────────────────────────────────────────────────────────────────────

class SocialService:
    """Social trading community service (SOC-01 ~ SOC-06).

    Provides:
    - Post creation (ideas, reports, questions, strategies)
    - Interactive features (likes, comments, follows, bookmarks)
    - Discussion forums
    - Prediction tracking
    - Reputation system
    """

    def __init__(self, persistence=None) -> None:
        self.persistence = persistence
        self._users: dict[str, UserProfile] = {}
        self._posts: dict[str, Post] = {}
        self._predictions: dict[str, Prediction] = {}
        self._comments: dict[str, Comment] = {}
        self._follows: set[tuple[str, str]] = set()  # (follower, followee)
        self._reactions: dict[str, Reaction] = {}
        self._bookmarks: set[tuple[str, str]] = set()  # (user_id, post_id)
        self._reports: list[dict] = []
        self._forums: dict[str, set[str]] = defaultdict(set)  # forum_key -> post_ids
        self._user_posts: dict[str, list[str]] = defaultdict(list)  # user_id -> post_ids

    # ── User Profiles ──────────────────────────────────────────────────────

    def create_profile(self, user_id: str, username: str) -> UserProfile:
        """Create a community user profile."""
        profile = UserProfile(user_id=user_id, username=username)
        self._users[user_id] = profile
        return profile

    def get_profile(self, user_id: str) -> UserProfile | None:
        """Get a user profile."""
        return self._users.get(user_id)

    def update_reputation(self, user_id: str, delta: float) -> None:
        """Update user reputation score by delta."""
        profile = self._users.get(user_id)
        if profile:
            profile.reputation_score = max(0.0, profile.reputation_score + delta)

    def record_idea_posted(self, user_id: str) -> None:
        """Increment idea count for a user."""
        profile = self._users.get(user_id)
        if profile:
            profile.total_ideas += 1

    # ── Posts ───────────────────────────────────────────────────────────────

    def create_post(
        self,
        user_id: str,
        post_type: PostType,
        title: str,
        content: str,
        *,
        instrument_id: str | None = None,
        tags: tuple[str, ...] = (),
        forum_key: str | None = None,
    ) -> Post:
        """Create a new community post."""
        post_id = f"post:{uuid.uuid4().hex[:12]}"
        post = Post(
            post_id=post_id,
            user_id=user_id,
            post_type=post_type,
            title=title,
            content=content,
            instrument_id=instrument_id,
            tags=tags,
        )
        self._posts[post_id] = post
        self._user_posts[user_id].append(post_id)
        self.record_idea_posted(user_id)

        # Add to forum
        if forum_key:
            self._forums[forum_key].add(post_id)

        return post

    def get_post(self, post_id: str) -> Post | None:
        """Get a post by ID."""
        return self._posts.get(post_id)

    def get_posts(
        self,
        *,
        user_id: str | None = None,
        instrument_id: str | None = None,
        post_type: PostType | None = None,
        forum_key: str | None = None,
        limit: int = 20,
        offset: int = 0,
    ) -> list[Post]:
        """Get posts with optional filters."""
        candidates = set(self._posts.keys())

        if forum_key:
            candidates &= self._forums.get(forum_key, set())

        results: list[Post] = []
        for pid in sorted(candidates, key=lambda p: self._posts[p].created_at, reverse=True):
            post = self._posts[pid]
            if post.is_deleted:
                continue
            if user_id and post.user_id != user_id:
                continue
            if instrument_id and post.instrument_id != instrument_id:
                continue
            if post_type and post.post_type != post_type:
                continue
            results.append(post)

        return results[offset:offset + limit]

    def delete_post(self, post_id: str, user_id: str) -> bool:
        """Delete a post (soft delete, only by author)."""
        post = self._posts.get(post_id)
        if post and post.user_id == user_id:
            post.is_deleted = True
            return True
        return False

    # ── Predictions ───────────────────────────────────────────────────────

    def create_prediction(
        self,
        post_id: str,
        entry_price: float,
        target_price: float,
        stop_price: float,
    ) -> Prediction:
        """Attach a prediction to a post."""
        prediction_id = f"pred:{uuid.uuid4().hex[:12]}"
        prediction = Prediction(
            prediction_id=prediction_id,
            post_id=post_id,
            entry_price=entry_price,
            target_price=target_price,
            stop_price=stop_price,
            current_price=entry_price,
        )
        self._predictions[prediction_id] = prediction
        return prediction

    def update_prediction_price(self, prediction_id: str, current_price: float) -> None:
        """Update current price for a prediction."""
        pred = self._predictions.get(prediction_id)
        if pred:
            pred.current_price = current_price
            # Auto-resolve
            if pred.status == "active":
                if current_price >= pred.target_price or current_price <= pred.stop_price:
                    pred.status = "hit" if current_price >= pred.target_price else "stopped"
                    pred.resolved_at = _now()

    def get_prediction(self, prediction_id: str) -> Prediction | None:
        """Get a prediction by ID."""
        return self._predictions.get(prediction_id)

    # ── Reactions ─────────────────────────────────────────────────────────

    def add_reaction(self, post_id: str, user_id: str, reaction_type: ReactionType) -> Reaction:
        """Add or toggle a reaction on a post."""
        reaction_id = f"react:{uuid.uuid4().hex[:12]}"
        reaction = Reaction(
            reaction_id=reaction_id,
            post_id=post_id,
            user_id=user_id,
            reaction_type=reaction_type,
        )
        self._reactions[reaction_id] = reaction
        post = self._posts.get(post_id)
        if post:
            post.like_count += 1
        return reaction

    def remove_reaction(self, post_id: str, user_id: str) -> bool:
        """Remove a user's reaction from a post."""
        for rid, reaction in list(self._reactions.items()):
            if reaction.post_id == post_id and reaction.user_id == user_id:
                del self._reactions[rid]
                post = self._posts.get(post_id)
                if post and post.like_count > 0:
                    post.like_count -= 1
                return True
        return False

    def get_reactions(self, post_id: str) -> list[Reaction]:
        """Get all reactions for a post."""
        return [r for r in self._reactions.values() if r.post_id == post_id]

    # ── Comments ─────────────────────────────────────────────────────────

    def add_comment(
        self,
        post_id: str,
        user_id: str,
        content: str,
        parent_comment_id: str | None = None,
    ) -> Comment:
        """Add a comment to a post."""
        comment_id = f"cmt:{uuid.uuid4().hex[:12]}"
        comment = Comment(
            comment_id=comment_id,
            post_id=post_id,
            user_id=user_id,
            parent_comment_id=parent_comment_id,
            content=content,
        )
        self._comments[comment_id] = comment
        post = self._posts.get(post_id)
        if post:
            post.comment_count += 1
        return comment

    def get_comments(self, post_id: str, limit: int = 50) -> list[Comment]:
        """Get top-level comments for a post."""
        return [
            c for c in self._comments.values()
            if c.post_id == post_id and not c.is_deleted and c.parent_comment_id is None
        ][:limit]

    def delete_comment(self, comment_id: str, user_id: str) -> bool:
        """Soft-delete a comment."""
        comment = self._comments.get(comment_id)
        if comment and comment.user_id == user_id:
            comment.is_deleted = True
            post = self._posts.get(comment.post_id)
            if post and post.comment_count > 0:
                post.comment_count -= 1
            return True
        return False

    # ── Follows ───────────────────────────────────────────────────────────

    def follow_user(self, follower_id: str, followee_id: str) -> bool:
        """Follow another user."""
        key = (follower_id, followee_id)
        if key in self._follows:
            return False
        self._follows.add(key)
        follower = self._users.get(follower_id)
        followee = self._users.get(followee_id)
        if follower:
            follower.following_count += 1
        if followee:
            followee.follower_count += 1
        return True

    def unfollow_user(self, follower_id: str, followee_id: str) -> bool:
        """Unfollow a user."""
        key = (follower_id, followee_id)
        if key not in self._follows:
            return False
        self._follows.discard(key)
        follower = self._users.get(follower_id)
        followee = self._users.get(followee_id)
        if follower and follower.following_count > 0:
            follower.following_count -= 1
        if followee and followee.follower_count > 0:
            followee.follower_count -= 1
        return True

    def is_following(self, follower_id: str, followee_id: str) -> bool:
        """Check if a user is following another."""
        return (follower_id, followee_id) in self._follows

    def get_followers(self, user_id: str) -> list[str]:
        """Get list of user IDs following this user."""
        return [f for f, _ in self._follows if f == user_id]

    def get_following(self, user_id: str) -> list[str]:
        """Get list of user IDs this user is following."""
        return [t for _, t in self._follows if _ == user_id]

    def get_followers_of_user(self, user_id: str) -> list[UserProfile]:
        """Get profiles of users following this user."""
        return [self._users[f] for f, _ in self._follows if f == user_id and f in self._users]

    def get_following_of_user(self, user_id: str) -> list[UserProfile]:
        """Get profiles of users this user is following."""
        return [self._users[t] for _, t in self._follows if _ == user_id and t in self._users]

    # ── Bookmarks ─────────────────────────────────────────────────────────

    def bookmark_post(self, user_id: str, post_id: str) -> bool:
        """Bookmark a post."""
        key = (user_id, post_id)
        if key in self._bookmarks:
            return False
        self._bookmarks.add(key)
        return True

    def remove_bookmark(self, user_id: str, post_id: str) -> bool:
        """Remove a bookmark."""
        key = (user_id, post_id)
        if key in self._bookmarks:
            self._bookmarks.discard(key)
            return True
        return False

    def get_bookmarks(self, user_id: str) -> list[Post]:
        """Get all bookmarked posts for a user."""
        return [self._posts[pid] for _, pid in self._bookmarks if _ == user_id and pid in self._posts]

    # ── Reports ───────────────────────────────────────────────────────────

    def report_content(self, reporter_id: str, target_type: str, target_id: str, reason: str) -> dict:
        """Report inappropriate content."""
        report = {
            "report_id": f"rpt:{uuid.uuid4().hex[:8]}",
            "reporter_id": reporter_id,
            "target_type": target_type,  # post, comment, user
            "target_id": target_id,
            "reason": reason,
            "status": "pending",
            "created_at": _now(),
        }
        self._reports.append(report)
        return report

    # ── Discussion Forums ─────────────────────────────────────────────────

    def create_forum(self, forum_key: str, title: str, description: str = "") -> dict:
        """Create or update a discussion forum."""
        self._forums.setdefault(forum_key, set())
        return {"forum_key": forum_key, "title": title, "description": description}

    def get_forum_posts(self, forum_key: str, limit: int = 20) -> list[Post]:
        """Get posts in a forum."""
        posts = [self._posts[pid] for pid in self._forums.get(forum_key, set())
                  if pid in self._posts and not self._posts[pid].is_deleted]
        return sorted(posts, key=lambda p: p.created_at, reverse=True)[:limit]

    # ── Reputation ───────────────────────────────────────────────────────

    def update_hit_rate(self, user_id: str) -> float:
        """Recalculate user hit rate from resolved predictions."""
        user_post_ids = self._user_posts.get(user_id, [])
        resolved = []
        for pid in user_post_ids:
            post = self._posts.get(pid)
            if not post:
                continue
            for pred in self._predictions.values():
                if pred.post_id == pid and pred.resolved_at:
                    resolved.append(pred.status == "hit")

        if not resolved:
            return 0.0
        hit_rate = sum(resolved) / len(resolved)
        profile = self._users.get(user_id)
        if profile:
            profile.hit_rate = hit_rate
        return hit_rate

    def get_leaderboard(self, limit: int = 20) -> list[UserProfile]:
        """Get top users by reputation score."""
        return sorted(
            self._users.values(),
            key=lambda p: p.reputation_score,
            reverse=True,
        )[:limit]
