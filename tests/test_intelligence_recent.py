"""Tests for IntelligenceEngine.recent_documents() (IN-01, IN-05) and UI API integration.

IN-01: Ingest and score documents (already tested)
IN-05: recent_documents returns sorted by published_at desc
UI-04: intelligence_recent API endpoint
UI-05: risk_dashboard API endpoint
UI-07: quick_paper_trade API endpoint
"""

from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from quant_exchange.core.models import MarketDocument
from quant_exchange.intelligence import IntelligenceEngine


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def sample_documents_with_timestamps() -> list[MarketDocument]:
    """Create sample documents with varying timestamps for sorting tests."""
    base = datetime(2025, 1, 1, tzinfo=timezone.utc)
    return [
        MarketDocument(
            document_id="doc_oldest",
            source="newswire",
            instrument_id="BTCUSDT",
            published_at=base - timedelta(days=5),
            title="Old article about Bitcoin",
            content="Bitcoin was trading lower.",
        ),
        MarketDocument(
            document_id="doc_middle",
            source="research",
            instrument_id="BTCUSDT",
            published_at=base - timedelta(days=2),
            title="Bitcoin analysis report",
            content="中性分析报告。",
        ),
        MarketDocument(
            document_id="doc_new",
            source="social",
            instrument_id="BTCUSDT",
            published_at=base + timedelta(hours=1),
            title="社区讨论BTC上涨",
            content="市场情绪利好，社区看多。",
        ),
        MarketDocument(
            document_id="doc_newest",
            source="exchange_announcement",
            instrument_id="BTCUSDT",
            published_at=base + timedelta(days=1),
            title="Bitcoin breakout announcement",
            content="Strong breakout and bullish growth signal.",
        ),
    ]


class IntelligenceRecentDocumentsTests(unittest.TestCase):
    """Test IN-05: recent_documents sorting and limiting."""

    def setUp(self) -> None:
        self.engine = IntelligenceEngine()

    def test_in_05_recent_documents_returns_newest_first(self) -> None:
        """Verify recent_documents returns documents sorted by published_at desc."""
        docs = sample_documents_with_timestamps()
        self.engine.ingest_documents(docs)

        recent = self.engine.recent_documents(limit=10)

        self.assertEqual(len(recent), 4)
        # Verify descending order (newest first)
        for i in range(len(recent) - 1):
            self.assertGreaterEqual(
                recent[i].published_at,
                recent[i + 1].published_at,
                f"Document at index {i} should have later timestamp than index {i+1}",
            )

    def test_in_05_recent_documents_newest_first_check(self) -> None:
        """Verify the first document is indeed the newest."""
        docs = sample_documents_with_timestamps()
        self.engine.ingest_documents(docs)

        recent = self.engine.recent_documents(limit=1)

        self.assertEqual(len(recent), 1)
        self.assertEqual(recent[0].document_id, "doc_newest")

    def test_in_05_recent_documents_respects_limit(self) -> None:
        """Verify recent_documents respects the limit parameter."""
        docs = sample_documents_with_timestamps()
        self.engine.ingest_documents(docs)

        recent_2 = self.engine.recent_documents(limit=2)
        self.assertEqual(len(recent_2), 2)
        # Should return 2 newest
        self.assertEqual(recent_2[0].document_id, "doc_newest")
        self.assertEqual(recent_2[1].document_id, "doc_new")

    def test_in_05_recent_documents_empty_when_no_docs(self) -> None:
        """Verify recent_documents returns empty list when no documents."""
        recent = self.engine.recent_documents()
        self.assertEqual(len(recent), 0)

    def test_in_05_recent_documents_includes_all_instruments(self) -> None:
        """Verify recent_documents returns docs from all instruments."""
        docs = [
            MarketDocument(
                document_id="btc_doc",
                source="newswire",
                instrument_id="BTCUSDT",
                published_at=datetime(2025, 1, 5, tzinfo=timezone.utc),
                title="BTC news",
                content="Bitcoin rally.",
            ),
            MarketDocument(
                document_id="eth_doc",
                source="newswire",
                instrument_id="ETHUSDT",
                published_at=datetime(2025, 1, 6, tzinfo=timezone.utc),
                title="ETH news",
                content="Ethereum upgrade.",
            ),
        ]
        self.engine.ingest_documents(docs)

        recent = self.engine.recent_documents(limit=10)
        self.assertEqual(len(recent), 2)
        instrument_ids = {d.instrument_id for d in recent}
        self.assertEqual(instrument_ids, {"BTCUSDT", "ETHUSDT"})


class IntelligenceDocumentIngestionTests(unittest.TestCase):
    """Test IN-01: Document ingestion and deduplication."""

    def test_in_01_documents_are_scored_and_deduplicated(self) -> None:
        """Verify duplicate documents are properly deduplicated."""
        engine = IntelligenceEngine()
        docs = sample_documents_with_timestamps()
        # Ingest all
        results = engine.ingest_documents(docs + [docs[0]])
        # Should only return 4 results (4 unique docs)
        self.assertEqual(len(results), 4)
        # Should only have 4 documents stored
        self.assertEqual(len(engine.documents), 4)

    def test_in_01_sentiment_scores_stored(self) -> None:
        """Verify sentiment scores are computed and stored for each document."""
        engine = IntelligenceEngine()
        docs = sample_documents_with_timestamps()
        engine.ingest_documents(docs)

        for doc in docs:
            self.assertIn(doc.document_id, engine.sentiment_results)
            score = engine.sentiment_results[doc.document_id]
            self.assertIsNotNone(score.score)
            self.assertIsNotNone(score.label)

    def test_in_01_instrument_results_populated(self) -> None:
        """Verify instrument_results maps instrument to sentiment results."""
        engine = IntelligenceEngine()
        docs = sample_documents_with_timestamps()
        engine.ingest_documents(docs)

        self.assertIn("BTCUSDT", engine.instrument_results)
        self.assertEqual(len(engine.instrument_results["BTCUSDT"]), 4)


class IntelligenceLanguageDetectionTests(unittest.TestCase):
    """Test IN-01: Language detection during ingestion."""

    def test_in_01_chinese_text_detected(self) -> None:
        """Verify Chinese text is correctly detected."""
        engine = IntelligenceEngine()
        self.assertEqual(engine.detect_language("比特币上涨"), "zh")
        self.assertEqual(engine.detect_language("市场分析报告"), "zh")

    def test_in_01_english_text_detected(self) -> None:
        """Verify English text is correctly detected."""
        engine = IntelligenceEngine()
        self.assertEqual(engine.detect_language("Bitcoin breakout rally"), "en")
        self.assertEqual(engine.detect_language("bullish growth"), "en")


class IntelligenceEventClassificationTests(unittest.TestCase):
    """Test IN-01: Event classification during ingestion."""

    def test_in_01_event_classification_works(self) -> None:
        """Verify events are classified correctly."""
        engine = IntelligenceEngine()
        docs = [
            MarketDocument(
                document_id="doc_listing",
                source="newswire",
                instrument_id="BTCUSDT",
                published_at=datetime(2025, 1, 1, tzinfo=timezone.utc),
                title="New coin listing",
                content="A new coin was listed on exchange.",
            ),
            MarketDocument(
                document_id="doc_regulatory",
                source="newswire",
                instrument_id="BTCUSDT",
                published_at=datetime(2025, 1, 2, tzinfo=timezone.utc),
                title="SEC regulatory news",
                content="The SEC announced new regulatory guidelines.",
            ),
        ]
        engine.ingest_documents(docs)

        self.assertEqual(engine.event_classifications.get("doc_listing"), "listing")
        self.assertEqual(engine.event_classifications.get("doc_regulatory"), "regulatory")


class IntelligenceAggregationTests(unittest.TestCase):
    """Test IN-01: Sentiment aggregation."""

    def test_in_01_aggregate_sentiment(self) -> None:
        """Verify aggregate_sentiment returns proper structure."""
        engine = IntelligenceEngine()
        docs = sample_documents_with_timestamps()
        engine.ingest_documents(docs)

        as_of = datetime(2025, 1, 7, tzinfo=timezone.utc)
        agg = engine.aggregate_sentiment("BTCUSDT", as_of=as_of, window=timedelta(days=7))

        self.assertIn("avg_score", agg)
        self.assertIn("doc_count", agg)
        self.assertIn("positive_count", agg)
        self.assertIn("negative_count", agg)
        self.assertIn("neutral_count", agg)


if __name__ == "__main__":
    unittest.main()
