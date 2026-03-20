"""Tests for new P2/P3 services: copy_trading, marketplace, visual_editor, competitions, tax, collaboration."""

import unittest
from datetime import datetime, timedelta, timezone

from quant_exchange.copy_trading import CopyTradingService
from quant_exchange.marketplace import MarketplaceService
from quant_exchange.visual_editor import VisualEditorService
from quant_exchange.visual_editor.service import BlockType, BlockCategory
from quant_exchange.competitions import CompetitionService
from quant_exchange.tax import TaxService
from quant_exchange.tax.service import CostBasisMethod
from quant_exchange.collaboration import CollaborationService
from quant_exchange.collaboration.service import MemberRole


class TestCopyTradingService(unittest.TestCase):
    """Tests for COPY-01~COPY-06 copy trading service."""

    def setUp(self) -> None:
        self.svc = CopyTradingService()

    def test_copy_01_list_providers(self) -> None:
        """Test listing signal providers (COPY-01)."""
        providers = self.svc.list_providers(sort_by="performance")
        self.assertGreater(len(providers), 0)
        self.assertTrue(all(p.status.value == "active" for p in providers))
        print(f"\n[COPY-01] Providers: {len(providers)}, top: {providers[0].display_name}")

    def test_copy_02_subscribe(self) -> None:
        """Test subscribing to a provider (COPY-02)."""
        sub = self.svc.subscribe("sub001", "sp001", 5000.0)
        self.assertIsNotNone(sub)
        self.assertEqual(sub.allocated_amount, 5000.0)
        print(f"\n[COPY-02] Subscription: {sub.subscription_id}")

    def test_copy_03_copy_trade(self) -> None:
        """Test auto-copying a trade (COPY-03)."""
        sub = self.svc.subscribe("sub001", "sp001", 3000.0)
        trade = self.svc.copy_trade(sub.subscription_id, "prov_trade_001", "BTCUSDT", "LONG", 0.1, 50000.0)
        self.assertIsNotNone(trade)
        self.assertEqual(trade.status.value, "executed")
        print(f"\n[COPY-03] Copy trade: {trade.trade_id}, pnl={trade.pnl}")

    def test_copy_04_earnings(self) -> None:
        """Test provider earnings calculation (COPY-04)."""
        earnings = self.svc.get_provider_earnings("sp001")
        self.assertIn("total_pnl", earnings)
        print(f"\n[COPY-04] Provider earnings: {earnings}")

    def test_copy_05_quality_score(self) -> None:
        """Test signal quality scoring (COPY-05)."""
        score = self.svc.get_signal_quality_score("sp001")
        self.assertIn("total_score", score)
        self.assertIn("rating", score)
        print(f"\n[COPY-05] Quality score: {score['total_score']}, rating: {score['rating']}")

    def test_copy_06_risk_check(self) -> None:
        """Test risk limit checking (COPY-06)."""
        sub = self.svc.subscribe("sub001", "sp001", 5000.0)
        result = self.svc.check_risk_limits(sub.subscription_id)
        self.assertIn("ok", result)
        print(f"\n[COPY-06] Risk check: ok={result['ok']}")


class TestMarketplaceService(unittest.TestCase):
    """Tests for MKT-01~MKT-06 marketplace service."""

    def setUp(self) -> None:
        self.svc = MarketplaceService()

    def test_mkt_01_listings(self) -> None:
        """Test listing strategies (MKT-01)."""
        listings = self.svc.list_listings(sort_by="popular")
        self.assertGreater(len(listings), 0)
        print(f"\n[MKT-01] Listings: {len(listings)}")

    def test_mkt_01_search(self) -> None:
        """Test strategy search (MKT-01)."""
        results = self.svc.search_listings("期权")
        self.assertGreater(len(results), 0)
        print(f"\n[MKT-01] Search results: {len(results)}")

    def test_mkt_02_reviews(self) -> None:
        """Test adding reviews (MKT-02)."""
        review = self.svc.add_review("mkt001", "u004", 5, "Excellent!", "Best strategy I bought.")
        self.assertIsNotNone(review)
        self.assertEqual(review.rating, 5)
        print(f"\n[MKT-02] Review: {review.review_id}, rating={review.rating}")

    def test_mkt_03_purchase(self) -> None:
        """Test purchasing a strategy (MKT-03)."""
        order = self.svc.purchase_strategy("mkt001", "u006", "one_time")
        self.assertIsNotNone(order)
        self.assertGreater(order.platform_fee, 0)
        print(f"\n[MKT-03] Order: {order.order_id}, payout={order.seller_payout}")

    def test_mkt_04_seller_revenue(self) -> None:
        """Test seller revenue (MKT-04)."""
        revenue = self.svc.get_seller_revenue("u001")
        self.assertIn("total_revenue", revenue)
        print(f"\n[MKT-04] Seller revenue: {revenue['total_revenue']}")

    def test_mkt_05_featured(self) -> None:
        """Test featured listings (MKT-05)."""
        featured = self.svc.get_featured_listings()
        self.assertGreater(len(featured), 0)
        print(f"\n[MKT-05] Featured: {[l.strategy_name for l in featured]}")


class TestVisualEditorService(unittest.TestCase):
    """Tests for VIS-01~VIS-05 visual strategy editor."""

    def setUp(self) -> None:
        self.svc = VisualEditorService()

    def test_vis_01_palette(self) -> None:
        """Test block palette (VIS-01)."""
        palette = self.svc.get_block_palette()
        self.assertGreater(len(palette), 0)
        print(f"\n[VIS-01] Palette blocks: {len(palette)}")

    def test_vis_02_canvas(self) -> None:
        """Test canvas management (VIS-02)."""
        canvas = self.svc.create_canvas("u001", "My Strategy")
        self.assertIsNotNone(canvas)
        self.assertEqual(canvas.name, "My Strategy")
        print(f"\n[VIS-02] Canvas: {canvas.canvas_id}")

    def test_vis_03_blocks(self) -> None:
        """Test adding blocks (VIS-03)."""
        canvas = self.svc.create_canvas("u001", "Test")
        block = self.svc.add_block(canvas.canvas_id, BlockType.SMA, 100.0, 200.0, {"period": 20})
        self.assertIsNotNone(block)
        self.assertEqual(block.label, "简单移动平均")
        print(f"\n[VIS-03] Block: {block.block_id}, type={block.block_type.value}")

    def test_vis_03_connection(self) -> None:
        """Test connections between blocks (VIS-03)."""
        canvas = self.svc.create_canvas("u001", "Test")
        source = self.svc.add_block(canvas.canvas_id, BlockType.SMA, 100.0, 200.0)
        target = self.svc.add_block(canvas.canvas_id, BlockType.MARKET_ORDER, 300.0, 200.0)
        conn = self.svc.add_connection(canvas.canvas_id, source.block_id, "sma_value", target.block_id, "signal")
        self.assertIsNotNone(conn)
        print(f"\n[VIS-03] Connection: {conn.connection_id}")

    def test_vis_04_code_generation(self) -> None:
        """Test Python code generation (VIS-04)."""
        canvas = self.svc.create_canvas("u001", "Generated")
        self.svc.add_block(canvas.canvas_id, BlockType.PRICE_DATA, 0.0, 0.0)
        self.svc.add_block(canvas.canvas_id, BlockType.SMA, 100.0, 0.0)
        self.svc.add_block(canvas.canvas_id, BlockType.MARKET_ORDER, 200.0, 0.0)
        code = self.svc.generate_code(canvas.canvas_id)
        self.assertIn("GeneratedStrategy", code)
        self.assertIn("on_bar", code)
        print(f"\n[VIS-04] Generated code lines: {len(code.split())}")

    def test_vis_05_validation(self) -> None:
        """Test canvas validation (VIS-05)."""
        canvas = self.svc.create_canvas("u001", "Invalid")
        result = self.svc.validate_canvas(canvas.canvas_id)
        self.assertFalse(result["valid"])
        self.assertGreater(len(result["errors"]), 0)
        print(f"\n[VIS-05] Validation errors: {len(result['errors'])}")


class TestCompetitionService(unittest.TestCase):
    """Tests for COMP-01~COMP-04 competition platform."""

    def setUp(self) -> None:
        self.svc = CompetitionService()
        self.now = datetime.now(timezone.utc)

    def test_comp_01_list_competitions(self) -> None:
        """Test listing competitions (COMP-01)."""
        comps = self.svc.list_competitions()
        self.assertGreater(len(comps), 0)
        print(f"\n[COMP-01] Competitions: {len(comps)}")

    def test_comp_02_registration(self) -> None:
        """Test competition registration (COMP-02)."""
        comp = self.svc.list_competitions()[0]
        part = self.svc.register(comp.competition_id, "u999", "Test Team")
        self.assertIsNotNone(part)
        self.assertEqual(part.team_name, "Test Team")
        print(f"\n[COMP-02] Participant: {part.participant_id}")

    def test_comp_03_submission(self) -> None:
        """Test strategy submission (COMP-03)."""
        comp = self.svc.list_competitions()[0]
        part = self.svc.register(comp.competition_id, "u999", "TestTeam")
        sub = self.svc.submit_strategy(comp.competition_id, part.participant_id, "MyBot", "print('hello')")
        self.assertIsNotNone(sub)
        print(f"\n[COMP-03] Submission: {sub.submission_id}")

    def test_comp_03_leaderboard(self) -> None:
        """Test leaderboard (COMP-03)."""
        lb = self.svc.get_leaderboard("comp001")
        self.assertIsInstance(lb, list)
        print(f"\n[COMP-03] Leaderboard entries: {len(lb)}")

    def test_comp_04_finalize(self) -> None:
        """Test competition finalization (COMP-04)."""
        result = self.svc.finalize_competition("comp002")
        self.assertIn("prize_distribution", result)
        print(f"\n[COMP-04] Prize distribution: {result['prize_distribution']}")


class TestTaxService(unittest.TestCase):
    """Tests for TAX-01~TAX-04 tax compliance service."""

    def setUp(self) -> None:
        self.svc = TaxService()
        self.now = datetime.now(timezone.utc)

    def test_tax_01_add_lot(self) -> None:
        """Test adding tax lots (TAX-01)."""
        lot = self.svc.add_lot("BTCUSDT", 1.0, 45000.0, self.now - timedelta(days=400))
        self.assertIsNotNone(lot)
        self.assertEqual(lot.remaining_quantity, 1.0)
        print(f"\n[TAX-01] Lot: {lot.lot_id}")

    def test_tax_02_fifo_gain(self) -> None:
        """Test FIFO capital gains calculation (TAX-02)."""
        # Add two lots
        self.svc.add_lot("ETHUSDT", 2.0, 2000.0, self.now - timedelta(days=400))
        self.svc.add_lot("ETHUSDT", 1.0, 2500.0, self.now - timedelta(days=100))
        # Sell 2 units at 3000 using FIFO
        gain = self.svc.calculate_gain("ETHUSDT", 2.0, 3000.0, self.now, method=CostBasisMethod.FIFO)
        self.assertIsNotNone(gain)
        self.assertGreater(gain.gain, 0)
        self.assertEqual(gain.gain_type.value, "long_term")  # oldest lot was > 1 year
        print(f"\n[TAX-02] FIFO gain: {gain.gain}, type={gain.gain_type.value}")

    def test_tax_03_report(self) -> None:
        """Test tax report generation (TAX-03)."""
        self.svc.add_lot("BTCUSDT", 0.5, 40000.0, self.now - timedelta(days=200))
        gain = self.svc.calculate_gain("BTCUSDT", 0.5, 50000.0, self.now)
        report = self.svc.generate_tax_report("u001", self.now.year)
        self.assertIsNotNone(report)
        self.assertEqual(report.tax_year, self.now.year)
        print(f"\n[TAX-03] Report: net_gain={report.net_gains}, trades={report.total_trades}")

    def test_tax_04_wash_sale(self) -> None:
        """Test wash sale detection (TAX-04)."""
        # Buy at loss
        self.svc.add_lot("BTCUSDT", 1.0, 60000.0, self.now - timedelta(days=100))
        loss = self.svc.calculate_gain("BTCUSDT", 1.0, 50000.0, self.now)
        # Repurchase within 30 days
        self.svc.add_lot("BTCUSDT", 1.0, 51000.0, self.now + timedelta(days=5))
        wash = self.svc.detect_wash_sales(self.now.year)
        self.assertGreater(len(wash), 0)
        print(f"\n[TAX-04] Wash sales detected: {len(wash)}")


class TestCollaborationService(unittest.TestCase):
    """Tests for COLLAB-01~COLLAB-04 team collaboration."""

    def setUp(self) -> None:
        self.svc = CollaborationService()

    def test_collab_01_create_team(self) -> None:
        """Test team creation (COLLAB-01)."""
        team = self.svc.create_team("u001", "Quant Research Team", "Research group")
        self.assertIsNotNone(team)
        self.assertEqual(team.name, "Quant Research Team")
        print(f"\n[COLLAB-01] Team: {team.team_id}, members={len(team.members)}")

    def test_collab_01_list_teams(self) -> None:
        """Test listing user teams."""
        teams = self.svc.list_user_teams("u001")
        self.assertGreater(len(teams), 0)
        print(f"\n[COLLAB-01] User teams: {len(teams)}")

    def test_collab_02_workspace(self) -> None:
        """Test workspace creation (COLLAB-02)."""
        team = self.svc.list_user_teams("u001")[0]
        ws = self.svc.create_workspace(team.team_id, "Alpha Research", "Strategy research")
        self.assertIsNotNone(ws)
        print(f"\n[COLLAB-02] Workspace: {ws.workspace_id}")

    def test_collab_03_activity(self) -> None:
        """Test activity feed (COLLAB-03)."""
        team = self.svc.list_user_teams("u001")[0]
        activity = self.svc.get_team_activity(team.team_id)
        self.assertIsInstance(activity, list)
        print(f"\n[COLLAB-03] Activity entries: {len(activity)}")

    def test_collab_04_permissions(self) -> None:
        """Test permission checking (COLLAB-04)."""
        team = self.svc.list_user_teams("u001")[0]
        perms = self.svc.get_member_permissions(team.team_id, "u001")
        self.assertTrue(perms.get("can_view"))
        self.assertTrue(perms.get("can_edit"))
        print(f"\n[COLLAB-04] Permissions: {perms}")


if __name__ == "__main__":
    unittest.main(verbosity=2)
