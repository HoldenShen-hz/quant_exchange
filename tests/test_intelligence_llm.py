"""Tests for IN-07 LLM interpretation service."""

import unittest
from datetime import datetime, timedelta, timezone

from quant_exchange.core.models import (
    Direction,
    DirectionalBias,
    MarketDocument,
)
from quant_exchange.intelligence import IntelligenceEngine
from quant_exchange.intelligence.llm_interpretation import (
    BiasExplanation,
    EventTimeline,
    LLMInterpretationService,
    LLMSummary,
    MarketCommentary,
    MockLLMClient,
)


class TestMockLLMClient(unittest.TestCase):
    """Test the mock LLM client responses."""

    def test_complete_returns_string(self) -> None:
        client = MockLLMClient()
        result = client.complete("Summarize the market")
        self.assertIsInstance(result, str)
        self.assertGreater(len(result), 10)


class TestLLMInterpretationService(unittest.TestCase):
    """Test LLMInterpretationService methods."""

    def setUp(self) -> None:
        self.engine = IntelligenceEngine()
        self.service = LLMInterpretationService(
            intelligence_engine=self.engine, llm_client=MockLLMClient()
        )
        # Seed some test documents
        self._seed_docs()

    def _seed_docs(self) -> None:
        now = datetime.now(timezone.utc)
        docs = [
            MarketDocument(
                document_id="doc1",
                instrument_id="TEST:001",
                source="news",
                title="TEST:001 earnings beat expectations",
                content="The company reported EPS of $1.50, beating the $1.20 consensus estimate.",
                published_at=now - timedelta(hours=6),
                event_tag="earnings",
            ),
            MarketDocument(
                document_id="doc2",
                instrument_id="TEST:001",
                source="research",
                title="Analyst upgrades TEST:001 to Buy",
                content="Major institution raises target from $100 to $120 citing strong growth.",
                published_at=now - timedelta(hours=12),
                event_tag="research_upgrade",
            ),
            MarketDocument(
                document_id="doc3",
                instrument_id="TEST:001",
                source="news",
                title="Regulatory approval for TEST:001 product",
                content="FDA approves the new product, removing regulatory uncertainty.",
                published_at=now - timedelta(hours=24),
                event_tag="regulatory",
            ),
        ]
        self.engine.ingest_documents(docs)

    def test_summarize_documents_returns_llm_summary(self) -> None:
        summary = self.service.summarize_documents(
            "TEST:001", window=timedelta(days=3)
        )
        self.assertIsInstance(summary, LLMSummary)
        self.assertEqual(summary.instrument_id, "TEST:001")
        self.assertIsInstance(summary.summary_text, str)
        self.assertGreater(len(summary.summary_text), 10)
        self.assertIn(summary.overall_tone, ["bullish", "bearish", "neutral", "mixed"])
        self.assertGreaterEqual(summary.document_count, 0)
        self.assertGreaterEqual(summary.confidence, 0.0)
        print(f"\n[IN-07] Summary: tone={summary.overall_tone}, docs={summary.document_count}")

    def test_summarize_documents_no_docs(self) -> None:
        summary = self.service.summarize_documents(
            "NONEXISTENT:999", window=timedelta(days=7)
        )
        self.assertIsInstance(summary, LLMSummary)
        self.assertEqual(summary.instrument_id, "NONEXISTENT:999")
        self.assertEqual(summary.document_count, 0)
        self.assertEqual(summary.confidence, 0.0)

    def test_build_event_timeline(self) -> None:
        timeline = self.service.build_event_timeline(
            "TEST:001", window=timedelta(days=7)
        )
        self.assertIsInstance(timeline, EventTimeline)
        self.assertEqual(timeline.instrument_id, "TEST:001")
        self.assertIsInstance(timeline.clusters, list)
        self.assertIsInstance(timeline.narrative, str)
        self.assertGreater(len(timeline.narrative), 0)
        print(f"\n[IN-07] Timeline: {len(timeline.clusters)} clusters")

    def test_explain_bias(self) -> None:
        bias = DirectionalBias(
            instrument_id="TEST:001",
            as_of=datetime.now(timezone.utc),
            window=timedelta(days=7),
            score=0.35,
            direction=Direction.LONG,
            confidence=0.75,
            supporting_documents=3,
        )
        explanation = self.service.explain_bias(bias, window=timedelta(days=7))
        self.assertIsInstance(explanation, BiasExplanation)
        self.assertEqual(explanation.instrument_id, "TEST:001")
        self.assertIsInstance(explanation.explanation_text, str)
        self.assertIsInstance(explanation.key_drivers, list)
        self.assertIsInstance(explanation.confidence_factors, list)
        self.assertIsInstance(explanation.risk_cautions, list)
        self.assertIsInstance(explanation.alternative_scenarios, list)
        print(f"\n[IN-07] Bias explanation: {explanation.explanation_text[:80]}...")

    def test_generate_commentary(self) -> None:
        commentary = self.service.generate_commentary(
            "TEST:001", window=timedelta(days=7)
        )
        self.assertIsInstance(commentary, MarketCommentary)
        self.assertEqual(commentary.instrument_id, "TEST:001")
        self.assertIsInstance(commentary.headline, str)
        self.assertIsInstance(commentary.body, str)
        self.assertIsInstance(commentary.sentiment_summary, str)
        self.assertIsInstance(commentary.key_level, str)
        self.assertIsInstance(commentary.catalyst_outlook, str)
        print(f"\n[IN-07] Commentary: {commentary.headline}")


class TestLLMInterpretationServiceIntegration(unittest.TestCase):
    """Integration test with platform-level API calls."""

    def setUp(self) -> None:
        from quant_exchange.platform import QuantTradingPlatform
        from quant_exchange.config import AppSettings

        import tempfile
        from pathlib import Path

        self.temp_dir = tempfile.TemporaryDirectory()
        db_path = str(Path(self.temp_dir.name) / "llm_test.sqlite3")
        self.platform = QuantTradingPlatform(
            AppSettings.from_mapping({"database": {"url": db_path}})
        )

    def tearDown(self) -> None:
        self.platform.close()
        self.temp_dir.cleanup()

    def test_llm_summarize_api(self) -> None:
        """Test llm_summarize API endpoint."""
        result = self.platform.api.llm_summarize("BTCUSDT", window_days=7)
        self.assertEqual(result["code"], "OK")
        data = result["data"]
        self.assertIn("instrument_id", data)
        self.assertIn("summary_text", data)
        self.assertIn("overall_tone", data)
        print(f"\n[IN-07 API] Summarize: {data['overall_tone']}")

    def test_llm_event_timeline_api(self) -> None:
        """Test llm_event_timeline API endpoint."""
        result = self.platform.api.llm_event_timeline("BTCUSDT", window_days=30)
        self.assertEqual(result["code"], "OK")
        data = result["data"]
        self.assertIn("instrument_id", data)
        self.assertIn("clusters", data)
        self.assertIn("narrative", data)
        print(f"\n[IN-07 API] Timeline: {len(data['clusters'])} clusters")

    def test_llm_explain_bias_api(self) -> None:
        """Test llm_explain_bias API endpoint."""
        result = self.platform.api.llm_explain_bias("BTCUSDT", window_days=7)
        self.assertEqual(result["code"], "OK")
        data = result["data"]
        self.assertIn("instrument_id", data)
        self.assertIn("explanation_text", data)
        self.assertIn("key_drivers", data)
        print(f"\n[IN-07 API] Explain bias: {data['explanation_text'][:80]}...")

    def test_llm_market_commentary_api(self) -> None:
        """Test llm_market_commentary API endpoint."""
        result = self.platform.api.llm_market_commentary("BTCUSDT", window_days=7)
        self.assertEqual(result["code"], "OK")
        data = result["data"]
        self.assertIn("instrument_id", data)
        self.assertIn("headline", data)
        self.assertIn("body", data)
        print(f"\n[IN-07 API] Commentary: {data['headline']}")


if __name__ == "__main__":
    unittest.main(verbosity=2)
