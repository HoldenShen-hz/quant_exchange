from __future__ import annotations

import unittest
from datetime import datetime, timezone

from quant_exchange.adapters import (
    AdapterRegistry,
    SimulatedCryptoExchangeAdapter,
    SimulatedEquityBrokerAdapter,
    SimulatedFuturesBrokerAdapter,
)
from quant_exchange.core.models import Instrument, MarketType, OrderRequest, OrderSide
from quant_exchange.rules import MarketRuleEngine


class AdapterAndRulesTests(unittest.TestCase):
    def test_ad_01_registry_and_simulated_adapters_work(self) -> None:
        registry = AdapterRegistry()
        crypto = SimulatedCryptoExchangeAdapter()
        registry.register_market_data(crypto.exchange_code(), crypto)
        registry.register_execution(crypto.exchange_code(), crypto)
        instruments = registry.get_market_data("SIM_CRYPTO").fetch_instruments()
        self.assertEqual(instruments[0].market, MarketType.CRYPTO)
        response = registry.get_execution("SIM_CRYPTO").submit_order(
            OrderRequest("cid-1", "BTCUSDT", OrderSide.BUY, 0.01)
        )
        self.assertEqual(response["status"], "SUBMITTED")

    def test_eq_01_stock_t_plus_one_and_board_lot_are_enforced(self) -> None:
        instrument = SimulatedEquityBrokerAdapter().fetch_instruments()[0]
        engine = MarketRuleEngine()
        bad_lot = engine.validate_order(
            instrument,
            OrderRequest("eq-1", instrument.instrument_id, OrderSide.BUY, 50),
            as_of=datetime(2025, 1, 2, 2, 0, tzinfo=timezone.utc).replace(hour=10, minute=0),
        )
        sell_violation = engine.validate_order(
            instrument,
            OrderRequest("eq-2", instrument.instrument_id, OrderSide.SELL, 100),
            as_of=datetime(2025, 1, 2, 10, 0, tzinfo=timezone.utc),
            available_position_qty=0,
        )
        self.assertFalse(bad_lot.approved)
        self.assertIn("board_lot_violation", bad_lot.reasons)
        self.assertFalse(sell_violation.approved)
        self.assertIn("t_plus_one_sell_violation", sell_violation.reasons)

    def test_fu_02_futures_near_expiry_is_blocked(self) -> None:
        instrument = SimulatedFuturesBrokerAdapter().fetch_instruments()[0]
        engine = MarketRuleEngine()
        decision = engine.validate_order(
            instrument,
            OrderRequest("fu-1", instrument.instrument_id, OrderSide.BUY, 1),
            as_of=datetime(2025, 3, 20, 10, 0, tzinfo=timezone.utc),
        )
        self.assertFalse(decision.approved)
        self.assertIn("opening_near_expiry_forbidden", decision.reasons)

    def test_cr_01_crypto_min_lot_is_enforced(self) -> None:
        instrument = SimulatedCryptoExchangeAdapter().fetch_instruments()[0]
        engine = MarketRuleEngine()
        decision = engine.validate_order(
            instrument,
            OrderRequest("cr-1", instrument.instrument_id, OrderSide.BUY, 0.0001),
            as_of=datetime(2025, 1, 1, 0, 0, tzinfo=timezone.utc),
        )
        self.assertFalse(decision.approved)
        self.assertIn("min_lot_violation", decision.reasons)

    def test_eq_02_us_market_session_accepts_utc_timestamp_when_local_time_is_open(self) -> None:
        instrument = Instrument(
            instrument_id="MSFT.US",
            symbol="MSFT.US",
            market=MarketType.STOCK,
            instrument_type="equity",
            market_region="US",
            lot_size=1,
            settlement_cycle="T+2",
            short_sellable=True,
            trading_sessions=(("09:30", "16:00"),),
            trading_rules={"allow_extended_hours": True},
        )
        engine = MarketRuleEngine()
        decision = engine.validate_order(
            instrument,
            OrderRequest("eq-us-1", instrument.instrument_id, OrderSide.BUY, 10),
            as_of=datetime(2025, 1, 2, 15, 30, tzinfo=timezone.utc),
        )
        self.assertTrue(decision.approved)


if __name__ == "__main__":
    unittest.main()
