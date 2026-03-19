"""Tests for UI API integration (UI-04, UI-05, UI-07).

UI-04: Intelligence sentiment panel API (intelligence_recent)
UI-05: Risk dashboard API (risk_dashboard)
UI-07: Quick trade API (quick_paper_trade)
"""

from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

from quant_exchange.core.models import MarketDocument


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class MockPlatform:
    """Mock platform for testing API methods."""

    def __init__(self):
        self.intelligence = MockIntelligence()
        self.risk = MockRisk()
        self.monitoring = MockMonitoring()
        self.paper_trading = MockPaperTrading()


class MockIntelligence:
    """Mock intelligence engine."""

    def __init__(self):
        self.documents = {}
        self.sentiment_results = {}
        self.event_classifications = {}

    def recent_documents(self, limit=20):
        docs = sorted(self.documents.values(), key=lambda d: d.published_at, reverse=True)
        return docs[:limit]


class MockRisk:
    """Mock risk engine."""

    def __init__(self):
        self.kill_switch_active = False


class MockMonitoring:
    """Mock monitoring service."""

    def __init__(self):
        self.alerts = []
        self.metrics = {
            "total_orders": 100,
            "total_trades": 50,
            "active_positions": 5,
        }

    def recent_alerts(self):
        return self.alerts


class MockPaperTrading:
    """Mock paper trading service."""

    def __init__(self):
        self.accounts = {
            "paper_stock_main": MockAccount(),
        }

    def dashboard(self, account_code=None, instrument_id=None):
        return {
            "account_code": account_code,
            "positions": [],
            "equity": 100000.0,
        }

    def submit_order(self, instrument_id, side, quantity, account_code, order_type, limit_price=None):
        return {
            "order_id": "mock_order_123",
            "status": "SUBMITTED",
            "instrument_id": instrument_id,
            "side": side,
            "quantity": quantity,
        }


class MockAccount:
    """Mock trading account."""

    def __init__(self):
        self.positions = {}
        self.equity = 100000.0


class ControlPlaneAPIMockTests(unittest.TestCase):
    """Test UI API methods with mocked platform."""

    def setUp(self) -> None:
        from quant_exchange.api.control_plane import ControlPlaneAPI
        from quant_exchange.persistence.database import SQLitePersistence

        self.platform = MockPlatform()
        self.persistence = MagicMock(spec=SQLitePersistence)
        from quant_exchange.adapters.registry import AdapterRegistry
        from quant_exchange.rules.engine import MarketRuleEngine
        from quant_exchange.scheduler.service import JobScheduler

        registry = AdapterRegistry()
        scheduler = JobScheduler()
        market_rules = MarketRuleEngine()

        self.api = ControlPlaneAPI(
            platform=self.platform,
            persistence=self.persistence,
            adapter_registry=registry,
            scheduler=scheduler,
            market_rules=market_rules,
        )

    # ─── UI-04: Intelligence Recent API ─────────────────────────────────────────

    def test_ui_04_intelligence_recent_returns_documents(self) -> None:
        """Verify intelligence_recent returns documents with sentiment."""
        # Setup: add some documents
        base = datetime(2025, 1, 1, tzinfo=timezone.utc)
        self.platform.intelligence.documents = {
            "doc1": MarketDocument(
                document_id="doc1",
                source="newswire",
                instrument_id="BTCUSDT",
                published_at=base,
                title="BTC breakout",
                content="Bitcoin breakout and strong growth.",
            ),
        }
        self.platform.intelligence.sentiment_results = {
            "doc1": MagicMock(label=MagicMock(value="POSITIVE"), score=0.75),
        }
        self.platform.intelligence.event_classifications = {"doc1": ""}

        result = self.api.intelligence_recent(limit=10)

        self.assertEqual(result["code"], "OK")
        self.assertIn("documents", result["data"])
        self.assertEqual(len(result["data"]["documents"]), 1)
        doc = result["data"]["documents"][0]
        self.assertEqual(doc["title"], "BTC breakout")
        self.assertEqual(doc["sentiment_label"], "POSITIVE")

    def test_ui_04_intelligence_recent_with_timestamps(self) -> None:
        """Verify intelligence_recent returns documents sorted by published_at desc."""
        base = datetime(2025, 1, 1, tzinfo=timezone.utc)
        self.platform.intelligence.documents = {
            "doc_old": MarketDocument(
                document_id="doc_old",
                source="newswire",
                instrument_id="BTCUSDT",
                published_at=base - timedelta(days=1),
                title="Old doc",
                content="Older content.",
            ),
            "doc_new": MarketDocument(
                document_id="doc_new",
                source="newswire",
                instrument_id="BTCUSDT",
                published_at=base,
                title="New doc",
                content="Newer content.",
            ),
        }
        self.platform.intelligence.sentiment_results = {
            "doc_old": MagicMock(label=MagicMock(value="NEUTRAL"), score=0.0),
            "doc_new": MagicMock(label=MagicMock(value="POSITIVE"), score=0.5),
        }
        self.platform.intelligence.event_classifications = {}

        result = self.api.intelligence_recent(limit=10)

        docs = result["data"]["documents"]
        # Newest should be first
        self.assertEqual(docs[0]["title"], "New doc")
        self.assertEqual(docs[1]["title"], "Old doc")

    def test_ui_04_intelligence_recent_respects_limit(self) -> None:
        """Verify intelligence_recent respects the limit parameter."""
        base = datetime(2025, 1, 1, tzinfo=timezone.utc)
        for i in range(5):
            self.platform.intelligence.documents[f"doc_{i}"] = MarketDocument(
                document_id=f"doc_{i}",
                source="newswire",
                instrument_id="BTCUSDT",
                published_at=base + timedelta(hours=i),
                title=f"Doc {i}",
                content=f"Content {i}.",
            )
            self.platform.intelligence.sentiment_results[f"doc_{i}"] = MagicMock(
                label=MagicMock(value="NEUTRAL"), score=0.0
            )
        self.platform.intelligence.event_classifications = {}

        result = self.api.intelligence_recent(limit=3)

        self.assertEqual(len(result["data"]["documents"]), 3)

    # ─── UI-05: Risk Dashboard API ───────────────────────────────────────────────

    def test_ui_05_risk_dashboard_structure(self) -> None:
        """Verify risk_dashboard returns correct structure."""
        result = self.api.risk_dashboard()

        self.assertEqual(result["code"], "OK")
        self.assertIn("kill_switch_active", result["data"])
        self.assertIn("alerts", result["data"])
        self.assertIn("metrics", result["data"])

    def test_ui_05_risk_dashboard_kill_switch_off(self) -> None:
        """Verify kill_switch_active is False by default."""
        result = self.api.risk_dashboard()
        self.assertEqual(result["data"]["kill_switch_active"], False)

    def test_ui_05_risk_dashboard_metrics(self) -> None:
        """Verify metrics are properly returned."""
        result = self.api.risk_dashboard()
        metrics = result["data"]["metrics"]
        self.assertGreaterEqual(metrics["total_orders"], 0)

    # ─── UI-07: Quick Paper Trade API ───────────────────────────────────────────

    def test_ui_07_quick_paper_trade_success(self) -> None:
        """Verify quick_paper_trade successfully submits order."""
        result = self.api.quick_paper_trade(
            symbol="BTCUSDT",
            side="buy",
            quantity=100,
            account_code="paper_stock_main",
        )

        self.assertEqual(result["code"], "OK")

    def test_ui_07_quick_paper_trade_empty_symbol_error(self) -> None:
        """Verify quick_paper_trade returns error for empty symbol."""
        result = self.api.quick_paper_trade(
            symbol="",
            side="buy",
            quantity=100,
        )

        self.assertEqual(result["code"], "BAD_REQUEST")
        self.assertIn("message", result["error"])

    def test_ui_07_quick_paper_trade_missing_symbol_error(self) -> None:
        """Verify quick_paper_trade returns error when symbol not provided."""
        result = self.api.quick_paper_trade(
            symbol=None,
            side="buy",
            quantity=100,
        )

        self.assertEqual(result["code"], "BAD_REQUEST")


class IntelligenceRecentEdgeCasesTests(unittest.TestCase):
    """Test edge cases for intelligence_recent."""

    def test_empty_documents_returns_empty_list(self) -> None:
        """Verify intelligence_recent returns empty list when no docs."""
        from quant_exchange.api.control_plane import ControlPlaneAPI
        from quant_exchange.persistence.database import SQLitePersistence

        platform = MockPlatform()
        platform.intelligence.documents = {}
        persistence = MagicMock(spec=SQLitePersistence)

        registry = MagicMock()
        scheduler = MagicMock()
        market_rules = MagicMock()

        api = ControlPlaneAPI(
            platform=platform,
            persistence=persistence,
            adapter_registry=registry,
            scheduler=scheduler,
            market_rules=market_rules,
        )

        result = api.intelligence_recent(limit=10)
        self.assertEqual(result["code"], "OK")
        self.assertEqual(len(result["data"]["documents"]), 0)


if __name__ == "__main__":
    unittest.main()
