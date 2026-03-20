"""Tests for SOC-01~SOC-06 social and community service."""

import unittest

from quant_exchange.social import (
    CommunityPost,
    Comment,
    Like,
    SocialService,
    StrategyShare,
    UserProfile,
)
from quant_exchange.social.service import (
    ContentModerator,
    ModerationStatus,
    PostStatus,
    PostType,
)


class TestContentModerator(unittest.TestCase):
    """Tests for content moderation (SOC-06)."""

    def setUp(self) -> None:
        self.mod = ContentModerator()

    def test_approve_clean_content(self) -> None:
        """Clean content is approved."""
        status, reason = self.mod.moderate("This is a great strategy discussion!")
        self.assertEqual(status, ModerationStatus.APPROVED)
        self.assertEqual(reason, "")

    def test_reject_blocked_keyword(self) -> None:
        """Content with blocked keywords is rejected."""
        status, reason = self.mod.moderate("fake account alert")
        self.assertEqual(status, ModerationStatus.REJECTED)
        self.assertIn("community guidelines", reason)

    def test_pending_url_content(self) -> None:
        """Content with URLs requires manual review."""
        status, reason = self.mod.moderate("Check out https://example.com for more")
        self.assertEqual(status, ModerationStatus.PENDING)


class TestSocialService(unittest.TestCase):
    """Tests for SocialService (SOC-01 ~ SOC-06)."""

    def setUp(self) -> None:
        self.svc = SocialService()

    def test_soc_01_create_post(self) -> None:
        """Test creating a community post (SOC-01)."""
        post = self.svc.create_post(
            user_id="u001",
            post_type=PostType.TEXT,
            title="My Trading Strategy",
            content="This is my new mean reversion strategy for SPY.",
            tags=["mean_reversion", "SPY"],
        )
        self.assertIsNotNone(post)
        self.assertEqual(post.user_id, "u001")
        self.assertEqual(post.title, "My Trading Strategy")
        self.assertEqual(post.status, PostStatus.PUBLISHED)
        print(f"\n[SOC-01] Post created: {post.post_id}")

    def test_soc_01_list_posts(self) -> None:
        """Test listing posts (SOC-01)."""
        self.svc.create_post("u001", PostType.TEXT, "Post 1", "Content 1")
        self.svc.create_post("u002", PostType.TEXT, "Post 2", "Content 2")
        posts = self.svc.list_posts()
        self.assertGreater(len(posts), 0)
        print(f"\n[SOC-01] Posts listed: {len(posts)}")

    def test_soc_01_add_comment(self) -> None:
        """Test adding a comment (SOC-01)."""
        post = self.svc.create_post("u001", PostType.TEXT, "Test Post", "Test content")
        comment = self.svc.add_comment(post.post_id, "u002", "Great post!")
        self.assertIsNotNone(comment)
        self.assertEqual(comment.content, "Great post!")
        self.assertEqual(comment.post_id, post.post_id)
        print(f"\n[SOC-01] Comment added: {comment.comment_id}")

    def test_soc_01_like_post(self) -> None:
        """Test liking a post (SOC-01)."""
        post = self.svc.create_post("u001", PostType.TEXT, "Like Test", "Content")
        success = self.svc.like("u002", post.post_id, "post")
        self.assertTrue(success)
        self.assertEqual(post.likes_count, 1)
        print(f"\n[SOC-01] Post liked, count: {post.likes_count}")

    def test_soc_01_unlike_post(self) -> None:
        """Test unliking a post."""
        post = self.svc.create_post("u001", PostType.TEXT, "Unlike Test", "Content")
        self.svc.like("u002", post.post_id, "post")
        self.svc.unlike("u002", post.post_id)
        self.assertEqual(post.likes_count, 0)

    def test_soc_02_share_strategy(self) -> None:
        """Test sharing a strategy (SOC-02)."""
        post = self.svc.create_post("u001", PostType.STRATEGY_SHARE, "Strategy Post", "My strategy")
        share = self.svc.share_strategy(
            post_id=post.post_id,
            user_id="u001",
            strategy_name="Grid Bot v1",
            strategy_type="grid",
            parameters={"price_interval_pct": 1.0, "size_per_grid": 0.01},
            performance_summary={"annual_return": 0.15, "sharpe_ratio": 1.8},
        )
        self.assertIsNotNone(share)
        self.assertEqual(share.strategy_name, "Grid Bot v1")
        print(f"\n[SOC-02] Strategy shared: {share.share_id}")

    def test_soc_02_list_strategies(self) -> None:
        """Test listing shared strategies (SOC-02)."""
        post = self.svc.create_post("u001", PostType.STRATEGY_SHARE, "Strategy", "desc")
        self.svc.share_strategy(post.post_id, "u001", "Grid", "grid", {}, {})
        strategies = self.svc.list_strategy_shares(sort_by="usage")
        self.assertGreater(len(strategies), 0)
        print(f"\n[SOC-02] Strategies: {len(strategies)}")

    def test_soc_02_copy_strategy(self) -> None:
        """Test copying a shared strategy (SOC-02)."""
        post = self.svc.create_post("u001", PostType.STRATEGY_SHARE, "Strategy", "desc")
        share = self.svc.share_strategy(post.post_id, "u001", "MA Cross", "ma_cross", {"fast_ma": 10, "slow_ma": 30}, {"annual_return": 0.12})
        copied = self.svc.copy_strategy(share.share_id, "u003")
        self.assertIsNotNone(copied)
        self.assertEqual(copied["strategy_name"], "MA Cross")
        self.assertEqual(share.usage_count, 1)
        print(f"\n[SOC-02] Strategy copied: {copied['strategy_name']}")

    def test_soc_03_get_profile(self) -> None:
        """Test getting a user profile (SOC-03)."""
        profile = self.svc.get_user_profile("u001")
        self.assertIsNotNone(profile)
        self.assertEqual(profile.username, "alice_trader")
        print(f"\n[SOC-03] Profile: {profile.display_name}, rank={profile.rank}")

    def test_soc_03_follow_unfollow(self) -> None:
        """Test follow/unfollow (SOC-03)."""
        success = self.svc.follow("u001", "u002")
        self.assertTrue(success)
        followers = self.svc.list_followers("u002")
        self.assertTrue(any(f.user_id == "u001" for f in followers))

        success = self.svc.unfollow("u001", "u002")
        self.assertTrue(success)

    def test_soc_03_leaderboard(self) -> None:
        """Test leaderboard (SOC-03)."""
        leaders = self.svc.get_leaderboard(limit=10)
        self.assertGreater(len(leaders), 0)
        self.assertEqual(leaders[0].rank, 1)
        print(f"\n[SOC-03] Leaderboard: {[(p.username, p.points) for p in leaders[:3]]}")

    def test_soc_04_recommended_posts(self) -> None:
        """Test post recommendations (SOC-04)."""
        posts = self.svc.get_recommended_posts("u001", limit=5)
        self.assertIsInstance(posts, list)
        print(f"\n[SOC-04] Recommended posts: {len(posts)}")

    def test_soc_05_notifications(self) -> None:
        """Test notifications (SOC-05)."""
        post = self.svc.create_post("u001", PostType.TEXT, "Test", "Content")
        self.svc.add_comment(post.post_id, "u002", "Nice!")
        notifs = self.svc.get_notifications("u001")
        self.assertGreater(len(notifs), 0)
        print(f"\n[SOC-05] Notifications: {len(notifs)}, first: {notifs[0].title}")

    def test_soc_05_mark_read(self) -> None:
        """Test marking notifications as read."""
        post = self.svc.create_post("u001", PostType.TEXT, "Test", "Content")
        self.svc.add_comment(post.post_id, "u002", "Great!")
        notifs = self.svc.get_notifications("u001", unread_only=True)
        count = self.svc.mark_all_read("u001")
        self.assertGreater(count, 0)

    def test_soc_06_moderation(self) -> None:
        """Test content moderation (SOC-06)."""
        post = self.svc.create_post("u001", PostType.TEXT, "Test", "Content with a URL https://example.com")
        # URL content should require manual review
        self.assertEqual(post.moderation_status, ModerationStatus.PENDING)
        self.svc.moderate_post(post.post_id, "approve")
        updated = self.svc.get_post(post.post_id)
        self.assertEqual(updated.moderation_status, ModerationStatus.APPROVED)
        print(f"\n[SOC-06] Post moderated: {updated.moderation_status}")


class TestSocialServiceIntegration(unittest.TestCase):
    """Integration tests for social API endpoints."""

    def setUp(self) -> None:
        from quant_exchange.platform import QuantTradingPlatform
        from quant_exchange.config import AppSettings
        import tempfile
        from pathlib import Path

        self.temp_dir = tempfile.TemporaryDirectory()
        db_path = str(Path(self.temp_dir.name) / "social_test.sqlite3")
        self.platform = QuantTradingPlatform(
            AppSettings.from_mapping({"database": {"url": db_path}})
        )

    def tearDown(self) -> None:
        self.platform.close()
        self.temp_dir.cleanup()

    def test_social_list_posts_api(self) -> None:
        """Test social_list_posts API endpoint."""
        result = self.platform.api.social_list_posts()
        self.assertEqual(result["code"], "OK")
        self.assertGreater(len(result["data"]["posts"]), 0)
        print(f"\n[SOC-01 API] Posts: {len(result['data']['posts'])}")

    def test_social_get_post_api(self) -> None:
        """Test social_get_post API endpoint."""
        result = self.platform.api.social_get_post("p001")
        self.assertEqual(result["code"], "OK")
        self.assertIn("post", result["data"])
        print(f"\n[SOC-01 API] Post: {result['data']['post']['title']}")

    def test_social_create_post_api(self) -> None:
        """Test social_create_post API endpoint."""
        result = self.platform.api.social_create_post(
            user_id="u001",
            post_type="text",
            title="API Test Post",
            content="Testing the social API from integration test.",
            tags=["test"],
        )
        self.assertEqual(result["code"], "OK")
        self.assertIn("post", result["data"])
        print(f"\n[SOC-01 API] Created: {result['data']['post']['post_id']}")

    def test_social_get_user_profile_api(self) -> None:
        """Test social_get_user_profile API endpoint."""
        result = self.platform.api.social_get_user_profile("u001")
        self.assertEqual(result["code"], "OK")
        print(f"\n[SOC-03 API] Profile: {result['data']['profile']['display_name']}")

    def test_social_leaderboard_api(self) -> None:
        """Test social_get_leaderboard API endpoint."""
        result = self.platform.api.social_get_leaderboard(limit=5)
        self.assertEqual(result["code"], "OK")
        self.assertGreater(len(result["data"]["leaderboard"]), 0)
        print(f"\n[SOC-03 API] Leaderboard: {len(result['data']['leaderboard'])} users")

    def test_social_notifications_api(self) -> None:
        """Test social_get_notifications API endpoint."""
        result = self.platform.api.social_get_notifications("u001")
        self.assertEqual(result["code"], "OK")
        print(f"\n[SOC-05 API] Notifications: {len(result['data']['notifications'])}")


if __name__ == "__main__":
    unittest.main(verbosity=2)
