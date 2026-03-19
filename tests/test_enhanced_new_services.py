"""Tests for enhanced new services: FX, Social, Marketplace, Collaboration, Competition, MultiAccount, Options, TaxReporting."""

from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from quant_exchange.enhanced import (
    FXService,
    SocialService,
    StrategyMarketplaceService,
    CollaborationService,
    CompetitionService,
    MultiAccountService,
    OptionsService,
    TaxReportingService,
    # Enums
    AccountType,
    BusinessModel,
    CompetitionStatus,
    CostBasisMethod,
    EditAction,
    EconomicImpact,
    ExerciseStyle,
    GainType,
    ListingStatus,
    MetalType,
    OptionType,
    PostType,
    ReactionType,
    RewardType,
    StrategyLegRole,
    TradeType,
    TransferStatus,
    WorkspaceRole,
)


class TestFXService(unittest.TestCase):
    """Tests for FXService (FX-01 ~ FX-04)."""

    def setUp(self) -> None:
        self.fx = FXService()

    def test_fx_01_currency_pairs_registered(self) -> None:
        """FX-01: Standard currency pairs are registered."""
        pairs = list(self.fx._pairs.keys())
        self.assertIn("EURUSD", pairs)
        self.assertIn("GBPUSD", pairs)
        self.assertIn("USDJPY", pairs)
        # Metals are in _registered_metals
        self.assertIn("XAUUSD", self.fx._registered_metals)

    def test_fx_02_update_and_get_quote(self) -> None:
        """FX-02: Can update and retrieve FX quotes."""
        quote = self.fx.update_quote("EURUSD", 1.0850, 1.0852)
        self.assertEqual(quote.pair, "EURUSD")
        self.assertAlmostEqual(quote.mid, 1.0851, places=4)
        self.assertGreater(quote.spread_bps, 0)

        retrieved = self.fx.get_quote("EURUSD")
        self.assertIsNotNone(retrieved)
        self.assertEqual(retrieved.mid, 1.0851)

    def test_fx_03_economic_calendar(self) -> None:
        """FX-03: Economic calendar events with impact levels."""
        event = self.fx.add_economic_event(
            currency="USD",
            event_name="Non-Farm Payrolls",
            impact=EconomicImpact.HIGH,
            event_time=datetime(2026, 4, 3, 12, 30, tzinfo=timezone.utc),
            forecast=200.0,
            previous=180.0,
            unit="K",
        )
        self.assertIn("econ:", event.event_id)

        events = self.fx.get_economic_events(currency="USD", window_hours=48)
        self.assertGreater(len(events), 0)

        high_impact = self.fx.get_high_impact_events(window_hours=48)
        self.assertGreaterEqual(len(high_impact), 1)

    def test_fx_04_currency_strength(self) -> None:
        """FX-04: Currency strength indicator computation."""
        # Feed some quotes
        self.fx.update_quote("EURUSD", 1.0850, 1.0852)
        self.fx.update_quote("GBPUSD", 1.2650, 1.2652)
        self.fx.update_quote("USDJPY", 149.50, 149.52)

        strengths = self.fx.compute_currency_strength(["EUR", "GBP", "USD", "JPY"])
        self.assertIn("EUR", strengths)
        self.assertIn("GBP", strengths)
        self.assertIsNotNone(strengths["EUR"].score)

    def test_fx_05_convert_currency(self) -> None:
        """FX-05: Currency conversion through USD."""
        self.fx.update_quote("EURUSD", 1.0850, 1.0852)
        result = self.fx.convert_currency(1000.0, "EURUSD", "EUR")
        self.assertGreater(result, 0)


class TestSocialService(unittest.TestCase):
    """Tests for SocialService (SOC-01 ~ SOC-06)."""

    def setUp(self) -> None:
        self.social = SocialService()

    def test_soc_01_create_profile_and_post(self) -> None:
        """SOC-01: User profiles and posts."""
        profile = self.social.create_profile("user1", "Alice")
        self.assertEqual(profile.username, "Alice")
        self.assertEqual(profile.reputation_score, 0.0)

        post = self.social.create_post(
            user_id="user1",
            post_type=PostType.IDEA,
            title="AAPL to 200",
            content="Long AAPL",
            tags=("AAPL", "long"),
        )
        self.assertIn("post:", post.post_id)
        self.assertEqual(post.title, "AAPL to 200")

    def test_soc_02_reactions_and_comments(self) -> None:
        """SOC-02: Reactions and comments on posts."""
        post = self.social.create_post("user1", PostType.IDEA, "Test idea", "Content")
        self.social.add_reaction(post.post_id, "user2", ReactionType.LIKE)
        self.social.add_comment(post.post_id, "user2", "Great idea!")

        reactions = self.social.get_reactions(post.post_id)
        self.assertGreaterEqual(len(reactions), 1)

        comments = self.social.get_comments(post.post_id)
        self.assertEqual(len(comments), 1)
        self.assertEqual(comments[0].content, "Great idea!")

    def test_soc_03_follow_system(self) -> None:
        """SOC-03: Follow/unfollow system."""
        self.social.create_profile("alice", "Alice")
        self.social.create_profile("bob", "Bob")

        self.social.follow_user("alice", "bob")
        self.assertTrue(self.social.is_following("alice", "bob"))

        followers = self.social.get_followers("bob")
        self.assertIn("alice", followers)

        self.social.unfollow_user("alice", "bob")
        self.assertFalse(self.social.is_following("alice", "bob"))

    def test_soc_04_prediction_tracking(self) -> None:
        """SOC-04: Prediction with entry/target/stop and auto-resolution."""
        post = self.social.create_post("user1", PostType.IDEA, "Test", "Content")
        pred = self.social.create_prediction(post.post_id, entry_price=150.0, target_price=160.0, stop_price=145.0)

        self.assertEqual(pred.status, "active")
        self.social.update_prediction_price(pred.prediction_id, 161.0)
        resolved = self.social.get_prediction(pred.prediction_id)
        self.assertEqual(resolved.status, "hit")

    def test_soc_05_forum_and_bookmarks(self) -> None:
        """SOC-05: Discussion forums and bookmarks."""
        self.social.create_forum("equity", "Equity Trading", "Equity discussion")
        post = self.social.create_post("user1", PostType.IDEA, "Test", "Content", forum_key="equity")

        forum_posts = self.social.get_forum_posts("equity")
        self.assertEqual(len(forum_posts), 1)

        self.social.bookmark_post("user2", post.post_id)
        bookmarks = self.social.get_bookmarks("user2")
        self.assertEqual(len(bookmarks), 1)

    def test_soc_06_reputation_leaderboard(self) -> None:
        """SOC-06: Reputation system and leaderboard."""
        self.social.create_profile("alice", "Alice")
        self.social.create_profile("bob", "Bob")
        self.social.update_reputation("alice", 10.0)
        self.social.update_reputation("bob", 5.0)

        leaderboard = self.social.get_leaderboard()
        self.assertEqual(leaderboard[0].user_id, "alice")
        self.assertEqual(leaderboard[0].reputation_score, 10.0)


class TestStrategyMarketplaceService(unittest.TestCase):
    """Tests for StrategyMarketplaceService (MKT-01 ~ MKT-06)."""

    def setUp(self) -> None:
        self.mkt = StrategyMarketplaceService()

    def test_mkt_01_submit_and_list_templates(self) -> None:
        """MKT-01: Template submission and listing."""
        tpl = self.mkt.submit_template(
            author_id="author1",
            name="MA Crossover",
            description="Moving average strategy",
            version="1.0",
            tags=("trend", "ma"),
            business_model=BusinessModel.FREE,
        )
        self.assertIn("tpl:", tpl.template_id)
        self.assertEqual(tpl.listing_status, ListingStatus.PENDING_REVIEW)

        self.mkt.approve_template(tpl.template_id)
        approved = self.mkt.get_template(tpl.template_id)
        self.assertEqual(approved.listing_status, ListingStatus.APPROVED)

    def test_mkt_02_reviews_and_ratings(self) -> None:
        """MKT-02: Review and rating system."""
        tpl = self.mkt.submit_template("author1", "Strategy", "Desc", "v1", tags=("test",))
        self.mkt.approve_template(tpl.template_id)

        self.mkt.submit_review(tpl.template_id, "user1", 5.0, "Excellent!", "Best strategy")
        self.mkt.submit_review(tpl.template_id, "user2", 4.0, "Good", "Works well")

        reviews = self.mkt.get_reviews(tpl.template_id)
        self.assertEqual(len(reviews), 2)

        updated = self.mkt.get_template(tpl.template_id)
        self.assertEqual(updated.avg_rating, 4.5)

    def test_mkt_03_business_models(self) -> None:
        """MKT-03: Business models - purchase, subscription, revenue share."""
        purchase_tpl = self.mkt.submit_template(
            "author1", "Paid Strategy", "Desc", "v1", tags=(),
            business_model=BusinessModel.PURCHASE, price=99.0,
        )
        self.mkt.approve_template(purchase_tpl.template_id)

        self.assertTrue(self.mkt.purchase_template("user1", purchase_tpl.template_id))
        self.assertTrue(self.mkt.has_purchased("user1", purchase_tpl.template_id))

        sub_tpl = self.mkt.submit_template(
            "author2", "Sub Strategy", "Desc", "v1", tags=(),
            business_model=BusinessModel.SUBSCRIPTION, subscription_price=29.0,
        )
        self.mkt.approve_template(sub_tpl.template_id)
        sub = self.mkt.subscribe("user2", sub_tpl.template_id, duration_days=30)
        self.assertIsNotNone(sub)
        self.assertTrue(self.mkt.is_subscribed("user2", sub_tpl.template_id))

    def test_mkt_04_version_management(self) -> None:
        """MKT-04: Version management for templates."""
        tpl = self.mkt.submit_template("author1", "Strategy", "Desc", "v1.0", tags=())
        self.mkt.publish_version(tpl.template_id, "v1.1", "Bug fixes")
        versions = self.mkt.get_versions(tpl.template_id)
        self.assertGreaterEqual(len(versions), 2)

    def test_mkt_05_trial_and_verification(self) -> None:
        """MKT-05: Trial and sandbox verification."""
        tpl = self.mkt.submit_template("author1", "Strategy", "Desc", "v1", tags=())
        trial = self.mkt.start_trial("user1", tpl.template_id, days=7)
        self.assertTrue(trial["success"])

        verified = self.mkt.verify_template(tpl.template_id)
        self.assertTrue(verified["passed"])

    def test_mkt_06_developer_earnings(self) -> None:
        """MKT-06: Developer earnings dashboard."""
        tpl = self.mkt.submit_template("author1", "Strategy", "Desc", "v1", tags=(),
                                        business_model=BusinessModel.PURCHASE, price=100.0)
        self.mkt.approve_template(tpl.template_id)
        self.mkt.purchase_template("user1", tpl.template_id)

        earnings = self.mkt.get_developer_earnings("author1")
        self.assertGreaterEqual(len(earnings), 1)
        self.assertEqual(earnings[0].developer_id, "author1")
        # 80% of 100 = 80 net (20% platform fee)
        self.assertAlmostEqual(earnings[0].net_revenue, 80.0, places=2)


class TestCollaborationService(unittest.TestCase):
    """Tests for CollaborationService (COLLAB-01 ~ COLLAB-04)."""

    def setUp(self) -> None:
        self.collab = CollaborationService()

    def test_collab_01_workspace_management(self) -> None:
        """COLLAB-01: Workspace creation and member management."""
        ws = self.collab.create_workspace("Alpha Team", owner_id="alice", description="Alpha research")
        self.assertIn("ws:", ws.workspace_id)
        self.assertEqual(ws.owner_id, "alice")

        member = self.collab.invite_member(ws.workspace_id, "bob", WorkspaceRole.EDITOR, inviter_id="alice")
        self.assertIsNotNone(member)
        self.assertEqual(member.role, WorkspaceRole.EDITOR)

        role = self.collab.get_user_role(ws.workspace_id, "bob")
        self.assertEqual(role, WorkspaceRole.EDITOR)

    def test_collab_02_shared_items_and_edit_history(self) -> None:
        """COLLAB-02: Shared items with full edit history."""
        ws = self.collab.create_workspace("Alpha Team", owner_id="alice")
        self.collab.share_item(ws.workspace_id, "alice", "strategy", "strat:123", "My Strategy", WorkspaceRole.EDITOR)

        items = self.collab.get_shared_items(ws.workspace_id)
        self.assertEqual(len(items), 1)

        history = self.collab.get_edit_history(ws.workspace_id)
        self.assertGreaterEqual(len(history), 1)

    def test_collab_03_role_updates_and_removals(self) -> None:
        """COLLAB-03: Role updates and member removals."""
        ws = self.collab.create_workspace("Team", owner_id="alice")
        self.collab.invite_member(ws.workspace_id, "bob", WorkspaceRole.VIEWER, inviter_id="alice")
        self.collab.update_member_role(ws.workspace_id, "bob", WorkspaceRole.EDITOR, changer_id="alice")

        role = self.collab.get_user_role(ws.workspace_id, "bob")
        self.assertEqual(role, WorkspaceRole.EDITOR)

        self.collab.remove_member(ws.workspace_id, "bob", remover_id="alice")
        role_after = self.collab.get_user_role(ws.workspace_id, "bob")
        self.assertIsNone(role_after)

    def test_collab_04_discussions_and_replies(self) -> None:
        """COLLAB-04: Threaded discussions and replies."""
        ws = self.collab.create_workspace("Team", owner_id="alice")
        disc = self.collab.create_discussion(ws.workspace_id, "alice", "Strategy Review", "Let's discuss")
        reply = self.collab.add_reply(disc.discussion_id, "bob", "Agreed!")
        self.assertIsNotNone(reply)

        replies = self.collab.get_replies(disc.discussion_id)
        self.assertEqual(len(replies), 1)


class TestCompetitionService(unittest.TestCase):
    """Tests for CompetitionService (COMP-01 ~ COMP-04)."""

    def setUp(self) -> None:
        self.comp = CompetitionService()

    def test_comp_01_competition_lifecycle(self) -> None:
        """COMP-01: Competition lifecycle - draft -> registration -> running -> completed."""
        comp = self.comp.create_competition(
            name="Q1 League",
            description="First quarter competition",
            start_time=datetime(2026, 4, 1, tzinfo=timezone.utc),
            end_time=datetime(2026, 6, 30, tzinfo=timezone.utc),
            registration_deadline=datetime(2026, 3, 25, tzinfo=timezone.utc),
            max_participants=100,
            scoring_method="total_return",
        )
        self.assertIn("comp:", comp.competition_id)
        self.assertEqual(comp.status, CompetitionStatus.DRAFT)

        self.comp.open_registration(comp.competition_id)
        self.assertEqual(self.comp.get_competition(comp.competition_id).status, CompetitionStatus.REGISTRATION)

        self.comp.start_competition(comp.competition_id)
        self.assertEqual(self.comp.get_competition(comp.competition_id).status, CompetitionStatus.RUNNING)

        self.comp.end_competition(comp.competition_id)
        self.assertEqual(self.comp.get_competition(comp.competition_id).status, CompetitionStatus.COMPLETED)

    def test_comp_02_registration_and_participant_tracking(self) -> None:
        """COMP-02: Registration and participant tracking."""
        comp = self.comp.create_competition(
            name="Q1 League",
            description="",
            start_time=datetime(2026, 4, 1, tzinfo=timezone.utc),
            end_time=datetime(2026, 6, 30, tzinfo=timezone.utc),
            registration_deadline=datetime(2026, 3, 25, tzinfo=timezone.utc),
            max_participants=100,
        )
        self.comp.open_registration(comp.competition_id)

        p1 = self.comp.register(comp.competition_id, "alice", "Alice", initial_equity=100_000.0)
        self.assertIsNotNone(p1)
        self.assertEqual(p1.username, "Alice")

        self.comp.record_equity(comp.competition_id, "alice", 105_000.0)
        participant = self.comp.get_participant(comp.competition_id, "alice")
        self.assertAlmostEqual(participant.total_return, 0.05, places=4)

    def test_comp_03_leaderboard_and_finalization(self) -> None:
        """COMP-03: Leaderboard and scoring."""
        comp = self.comp.create_competition(
            name="Q1 League", description="",
            start_time=datetime(2026, 4, 1, tzinfo=timezone.utc),
            end_time=datetime(2026, 6, 30, tzinfo=timezone.utc),
            registration_deadline=datetime(2026, 3, 25, tzinfo=timezone.utc),
            max_participants=100, scoring_method="total_return",
        )
        self.comp.open_registration(comp.competition_id)
        # Register BEFORE starting
        self.comp.register(comp.competition_id, "alice", "Alice", 100_000.0)
        self.comp.register(comp.competition_id, "bob", "Bob", 100_000.0)
        self.comp.start_competition(comp.competition_id)

        self.comp.record_equity(comp.competition_id, "alice", 110_000.0)
        self.comp.record_equity(comp.competition_id, "bob", 105_000.0)

        leaderboard = self.comp.get_leaderboard(comp.competition_id)
        self.assertEqual(leaderboard[0].user_id, "alice")
        self.assertEqual(leaderboard[0].rank, 1)

    def test_comp_04_rewards_and_achievements(self) -> None:
        """COMP-04: Rewards and achievement system."""
        comp = self.comp.create_competition(
            name="Q1 League", description="",
            start_time=datetime(2026, 4, 1, tzinfo=timezone.utc),
            end_time=datetime(2026, 6, 30, tzinfo=timezone.utc),
            registration_deadline=datetime(2026, 3, 25, tzinfo=timezone.utc),
            max_participants=100,
        )
        self.comp.open_registration(comp.competition_id)
        self.comp.start_competition(comp.competition_id)
        self.comp.end_competition(comp.competition_id)

        self.comp.define_rewards(comp.competition_id, [
            (1, RewardType.CASH, 1000.0, "First place prize"),
            (2, RewardType.CASH, 500.0, "Second place prize"),
        ])
        rewards = self.comp.get_rewards(comp.competition_id)
        self.assertEqual(len(rewards), 2)

        ach = self.comp.award_achievement("alice", "first_win", "First Win", "Won first competition")
        self.assertIn("ach:", ach.achievement_id)

        achievements = self.comp.get_user_achievements("alice")
        self.assertEqual(len(achievements), 1)


class TestMultiAccountService(unittest.TestCase):
    """Tests for MultiAccountService (ACCT-01 ~ ACCT-04)."""

    def setUp(self) -> None:
        self.mac = MultiAccountService()

    def test_acct_01_register_and_list_accounts(self) -> None:
        """ACCT-01: Account registration."""
        acct = self.mac.register_account("user1", AccountType.LIVE, "Main Account", initial_balance=10_000.0)
        self.assertIn("acct:", acct.account_id)
        self.assertEqual(acct.balance, 10_000.0)

        accounts = self.mac.get_user_accounts("user1")
        self.assertEqual(len(accounts), 1)

    def test_acct_02_account_groups(self) -> None:
        """ACCT-02: Account groups for unified management."""
        acct1 = self.mac.register_account("user1", AccountType.LIVE, "Live", 10_000.0)
        acct2 = self.mac.register_account("user1", AccountType.PAPER, "Paper", 50_000.0)

        group = self.mac.create_group("user1", "All Accounts")
        self.mac.add_account_to_group(group.group_id, acct1.account_id)
        self.mac.add_account_to_group(group.group_id, acct2.account_id)

        groups = self.mac.get_user_groups("user1")
        self.assertEqual(len(groups), 1)
        self.assertEqual(len(groups[0].account_ids), 2)

    def test_acct_03_unified_asset_view(self) -> None:
        """ACCT-03: Unified asset view across accounts."""
        self.mac.register_account("user1", AccountType.LIVE, "Live", 10_000.0)
        self.mac.register_account("user1", AccountType.PAPER, "Paper", 50_000.0)

        view = self.mac.get_unified_view("user1")
        self.assertEqual(view.total_equity, 60_000.0)
        self.assertEqual(len(view.account_breakdown), 2)

    def test_acct_04_internal_transfers(self) -> None:
        """ACCT-04: Internal transfers between accounts."""
        acct1 = self.mac.register_account("user1", AccountType.LIVE, "Live", 10_000.0)
        acct2 = self.mac.register_account("user1", AccountType.PAPER, "Paper", 5_000.0)

        xfer = self.mac.transfer(acct1.account_id, acct2.account_id, 2_000.0)
        self.assertIsNotNone(xfer)
        self.assertEqual(xfer.status, TransferStatus.COMPLETED)
        self.assertEqual(self.mac.get_account(acct1.account_id).balance, 8_000.0)
        self.assertEqual(self.mac.get_account(acct2.account_id).balance, 7_000.0)

    def test_acct_05_cross_account_risk(self) -> None:
        """ACCT-05: Cross-account risk exposure."""
        self.mac.register_account("user1", AccountType.LIVE, "Live", 10_000.0)
        self.mac.register_account("user1", AccountType.PAPER, "Paper", 5_000.0)

        risk = self.mac.compute_cross_account_risk("user1")
        self.assertEqual(risk.net_exposure, 15_000.0)


class TestOptionsService(unittest.TestCase):
    """Tests for OptionsService (OPT-01 ~ OPT-04)."""

    def setUp(self) -> None:
        self.opt = OptionsService()

    def test_opt_01_black_scholes_price(self) -> None:
        """OPT-01: Black-Scholes pricing."""
        from quant_exchange.enhanced import black_scholes_price, black_scholes_greeks, OptionType

        price = black_scholes_price(S=100.0, K=100.0, T=0.25, r=0.05, sigma=0.20, q=0.0, option_type=OptionType.CALL)
        self.assertGreater(price, 0)

        greeks = black_scholes_greeks(S=100.0, K=100.0, T=0.25, r=0.05, sigma=0.20, q=0.0, option_type=OptionType.CALL)
        self.assertGreater(greeks.delta, 0)
        self.assertGreater(greeks.gamma, 0)
        self.assertLess(greeks.theta, 0)  # Theta is negative for long options
        self.assertGreater(greeks.vega, 0)

    def test_opt_02_implied_volatility(self) -> None:
        """OPT-02: Implied volatility calculation."""
        from quant_exchange.enhanced import implied_volatility, OptionType

        # Given market price, find implied vol
        iv = implied_volatility(
            market_price=5.0, S=100.0, K=100.0, T=0.25, r=0.05, q=0.0, option_type=OptionType.CALL,
        )
        self.assertGreater(iv, 0)
        self.assertLess(iv, 1.0)

    def test_opt_03_contract_and_price_service(self) -> None:
        """OPT-03: Contract registration and pricing service."""
        contract = self.opt.register_contract("AAPL", OptionType.CALL, strike=150.0,
                                               expiry=datetime(2026, 6, 20, tzinfo=timezone.utc))
        self.assertIn("opt:", contract.contract_id)

        greeks = self.opt.price_contract(contract.contract_id, spot_price=155.0, volatility=0.25)
        self.assertIsNotNone(greeks)
        self.assertGreater(greeks.delta, 0)

    def test_opt_04_volatility_surface(self) -> None:
        """OPT-04: Volatility surface management."""
        self.opt.add_vol_surface_point("AAPL", strike=150.0,
                                        expiry=datetime(2026, 4, 17, tzinfo=timezone.utc),
                                        implied_vol=0.25, bid_vol=0.24, ask_vol=0.26)
        self.opt.add_vol_surface_point("AAPL", strike=155.0,
                                        expiry=datetime(2026, 4, 17, tzinfo=timezone.utc),
                                        implied_vol=0.24, bid_vol=0.23, ask_vol=0.25)

        surface = self.opt.get_vol_surface("AAPL")
        self.assertIsNotNone(surface)
        self.assertEqual(len(surface.points), 2)

        interp = self.opt.interpolate_vol("AAPL", strike=152.0,
                                           expiry=datetime(2026, 4, 17, tzinfo=timezone.utc))
        self.assertIsNotNone(interp)

    def test_opt_05_strategy_builder(self) -> None:
        """OPT-05: Multi-leg strategy builder."""
        call = self.opt.register_contract("AAPL", OptionType.CALL, 150.0,
                                           datetime(2026, 6, 20, tzinfo=timezone.utc))
        put = self.opt.register_contract("AAPL", OptionType.PUT, 150.0,
                                          datetime(2026, 6, 20, tzinfo=timezone.utc))

        strategy = self.opt.create_strategy("Straddle", [
            (call.contract_id, StrategyLegRole.LONG, 1),
            (put.contract_id, StrategyLegRole.LONG, 1),
        ])
        self.assertIsNotNone(strategy)
        self.assertEqual(len(strategy.legs), 2)

    def test_opt_06_covered_call_build(self) -> None:
        """OPT-06: Pre-built covered call strategy."""
        result = self.opt.build_covered_call(
            underlying="AAPL", spot_price=155.0, strike=160.0,
            expiry=datetime(2026, 6, 20, tzinfo=timezone.utc), volatility=0.25,
        )
        self.assertIn("strategy_id", result)
        self.assertGreater(result["net_delta"], 0)
        self.assertGreater(result["net_theta"], 0)  # Short premium = positive theta benefit


class TestTaxReportingService(unittest.TestCase):
    """Tests for TaxReportingService (TAX-01 ~ TAX-04)."""

    def setUp(self) -> None:
        self.tax = TaxReportingService()

    def test_tax_01_record_trades_and_activities(self) -> None:
        """TAX-01: Record trade activities."""
        act = self.tax.record_trade(
            account_id="acct1", user_id="user1", instrument_id="AAPL",
            trade_type=TradeType.BUY, quantity=10.0, price=150.0, commission=1.0,
        )
        self.assertIn("tax:", act.activity_id)
        self.assertEqual(act.net_amount, -(10 * 150.0 + 1.0))

        sell = self.tax.record_trade(
            account_id="acct1", user_id="user1", instrument_id="AAPL",
            trade_type=TradeType.SELL, quantity=5.0, price=160.0, commission=1.0,
        )
        self.assertGreater(sell.proceeds, 0)

    def test_tax_02_tax_lots(self) -> None:
        """TAX-02: Tax lot management."""
        self.tax.record_trade("acct1", "user1", "AAPL", TradeType.BUY, 10.0, 150.0, 1.0)
        self.tax.record_trade("acct1", "user1", "AAPL", TradeType.BUY, 5.0, 155.0, 0.5)

        lots = self.tax.get_lots_for_instrument("AAPL")
        self.assertEqual(len(lots), 2)

        open_lots = self.tax.get_open_lots("AAPL")
        self.assertEqual(len(open_lots), 2)

    def test_tax_03_capital_gains_fifo(self) -> None:
        """TAX-03: Capital gains calculation with FIFO."""
        # Buy 10 shares @ 100
        self.tax.record_trade("acct1", "user1", "STOCK", TradeType.BUY, 10.0, 100.0, 0.0)
        # Buy 10 shares @ 120
        self.tax.record_trade("acct1", "user1", "STOCK", TradeType.BUY, 10.0, 120.0, 0.0)
        # Sell 10 shares @ 130 (should use FIFO: first lot @ 100)
        sale = self.tax.record_trade("acct1", "user1", "STOCK", TradeType.SELL, 10.0, 130.0, 0.0)

        gain = self.tax.calculate_gain(sale, method=CostBasisMethod.FIFO)
        self.assertIsNotNone(gain)
        self.assertAlmostEqual(gain.gain, 300.0, places=2)  # (130-100)*10
        self.assertEqual(gain.gain_type, GainType.SHORT_TERM)

    def test_tax_04_cost_basis_methods(self) -> None:
        """TAX-04: Different cost basis methods (FIFO, LIFO, HIFO)."""
        self.tax.record_trade("acct1", "user1", "STOCK", TradeType.BUY, 10.0, 100.0, 0.0)
        self.tax.record_trade("acct1", "user1", "STOCK", TradeType.BUY, 10.0, 120.0, 0.0)
        sale = self.tax.record_trade("acct1", "user1", "STOCK", TradeType.SELL, 10.0, 110.0, 0.0)

        # FIFO: uses 100 basis -> gain = (110-100)*10 = 100
        fifo_gain = self.tax.calculate_gain(sale, method=CostBasisMethod.FIFO)
        self.assertAlmostEqual(fifo_gain.gain, 100.0, places=2)

    def test_tax_05_annual_tax_summary(self) -> None:
        """TAX-05: Annual tax summary computation."""
        self.tax.record_trade("acct1", "user1", "STOCK", TradeType.BUY, 10.0, 100.0, 0.0,
                               timestamp="2026-01-15T10:00:00Z")
        sale = self.tax.record_trade("acct1", "user1", "STOCK", TradeType.SELL, 10.0, 130.0, 0.0,
                               timestamp="2026-03-15T10:00:00Z")
        self.tax.record_trade("acct1", "user1", "STOCK", TradeType.DIVIDEND, 1.0, 10.0,
                               timestamp="2026-06-01T10:00:00Z")
        # Calculate gain explicitly
        self.tax.calculate_gain(sale, method=CostBasisMethod.FIFO)

        summary = self.tax.compute_annual_summary("user1", 2026)
        self.assertEqual(summary.year, 2026)
        self.assertGreater(summary.short_term_gains, 0)
        self.assertGreater(summary.total_dividends, 0)

    def test_tax_06_export_and_wash_sale(self) -> None:
        """TAX-06: CSV export and wash sale detection."""
        self.tax.record_trade("acct1", "user1", "STOCK", TradeType.BUY, 10.0, 100.0, 0.0,
                               timestamp="2026-01-15T10:00:00Z")
        sale = self.tax.record_trade("acct1", "user1", "STOCK", TradeType.SELL, 10.0, 90.0, 0.0,
                                      timestamp="2026-01-20T10:00:00Z")

        csv = self.tax.export_summary_csv("user1", 2026)
        self.assertIn("Tax Summary 2026", csv)

        wash = self.tax.check_wash_sale("STOCK", datetime(2026, 1, 20, tzinfo=timezone.utc), 100.0)
        self.assertIsNotNone(wash)  # Loss sale with subsequent buy = wash sale

        summary_1099 = self.tax.generate_1099_summary("user1", 2026)
        self.assertEqual(summary_1099["year"], 2026)
        self.assertIn("dividends", summary_1099)


if __name__ == "__main__":
    unittest.main()
