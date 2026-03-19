from __future__ import annotations

import unittest

from quant_exchange import QuantTradingPlatform
from quant_exchange.core.models import Action, Role
from quant_exchange.strategy import MovingAverageSentimentStrategy

from .fixtures import sample_documents, sample_instrument, sample_klines


class PlatformIntegrationTests(unittest.TestCase):
    def test_platform_wires_core_modules_together(self) -> None:
        platform = QuantTradingPlatform()
        self.addCleanup(platform.close)
        instrument = sample_instrument()
        platform.register_instrument(instrument)
        platform.market_data.ingest_klines(sample_klines())
        platform.intelligence.ingest_documents(sample_documents())
        result = platform.backtest.run(
            instrument=instrument,
            klines=platform.market_data.query_klines("BTCUSDT", "1d"),
            strategy=MovingAverageSentimentStrategy(),
            intelligence_engine=platform.intelligence,
            risk_engine=platform.risk,
            initial_cash=100_000.0,
        )
        self.assertGreater(len(result.equity_curve), 0)
        self.assertTrue(platform.security.authorize(Role.ADMIN, Action.VIEW))


if __name__ == "__main__":
    unittest.main()
