from __future__ import annotations

import unittest
from datetime import timedelta

from quant_exchange.core.models import Direction
from quant_exchange.intelligence import IntelligenceEngine

from .fixtures import sample_documents


class IntelligenceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.engine = IntelligenceEngine()

    def test_in_01_documents_are_scored_and_deduplicated(self) -> None:
        documents = sample_documents()
        results = self.engine.ingest_documents(documents + [documents[0]])
        self.assertEqual(len(results), 2)
        self.assertEqual(len(self.engine.documents), 2)

    def test_in_02_directional_bias_outputs_long_signal(self) -> None:
        documents = sample_documents()
        self.engine.ingest_documents(documents)
        bias = self.engine.directional_bias(
            "BTCUSDT",
            as_of=documents[-1].published_at + timedelta(hours=1),
            window=timedelta(days=2),
        )
        self.assertEqual(bias.direction, Direction.LONG)
        self.assertGreater(bias.confidence, 0.3)


if __name__ == "__main__":
    unittest.main()
