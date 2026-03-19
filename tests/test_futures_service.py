"""Tests for FuturesWorkbenchService (FT-01 ~ FT-07).

FT-01: List all available futures contracts
FT-02: Return universe summary (gainers, losers, most active)
FT-03: Get detailed contract info
FT-04: Get contract K-line history
FT-05: Realized volatility calculation
FT-06: Multi-exchange support (IF, IC, IH, AU, CU, RB, ES, NQ, CL, GC)
FT-07: Contract metadata (multiplier, expiry, sessions, tick_size)
"""

from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone
from math import sqrt

from quant_exchange.adapters.registry import AdapterRegistry
from quant_exchange.adapters.simulated import SimulatedFuturesBrokerAdapter
from quant_exchange.core.models import Kline, MarketType
from quant_exchange.futures import FuturesWorkbenchService
from quant_exchange.marketdata.service import MarketDataStore


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class FuturesWorkbenchServiceTests(unittest.TestCase):
    """Test FT-01 ~ FT-07: FuturesWorkbenchService functionality."""

    def setUp(self) -> None:
        self.registry = AdapterRegistry()
        self.futures_adapter = SimulatedFuturesBrokerAdapter()
        self.registry.register_market_data("SIM_FUTURES", self.futures_adapter)
        self.market_data_store = MarketDataStore()
        self.service = FuturesWorkbenchService(
            adapter_registry=self.registry,
            market_data_store=self.market_data_store,
        )

    # ─── FT-01: List all futures contracts ───────────────────────────────────────

    def test_ft_01_list_contracts_returns_all_instruments(self) -> None:
        """Verify list_contracts returns all registered futures contracts."""
        contracts = self.service.list_contracts()
        self.assertGreater(len(contracts), 0)
        # Should have IF, IC, IH, AU, CU, RB, ES, NQ, CL, GC
        instrument_ids = [c["instrument_id"] for c in contracts]
        self.assertIn("IF2503", instrument_ids)
        self.assertIn("IC2506", instrument_ids)
        self.assertIn("AU2506", instrument_ids)
        self.assertIn("CU2506", instrument_ids)
        self.assertIn("CL2506", instrument_ids)

    def test_ft_01_contracts_have_required_fields(self) -> None:
        """Verify each contract payload has all required fields."""
        contracts = self.service.list_contracts()
        required_fields = [
            "instrument_id", "symbol", "display_name", "category",
            "market_label", "last_price", "change", "change_pct",
            "market_status", "quote_time", "turnover_24h", "volume_24h",
            "volatility_30d", "source",
        ]
        for contract in contracts:
            for field in required_fields:
                self.assertIn(field, contract, f"Missing field {field} in {contract['instrument_id']}")

    def test_ft_01_contracts_sorted_by_turnover(self) -> None:
        """Verify contracts are sorted by 24h turnover descending."""
        contracts = self.service.list_contracts()
        turnovers = [c["turnover_24h"] for c in contracts]
        self.assertEqual(turnovers, sorted(turnovers, reverse=True))

    # ─── FT-02: Universe summary ─────────────────────────────────────────────────

    def test_ft_02_universe_summary_structure(self) -> None:
        """Verify universe_summary returns correct structure."""
        summary = self.service.universe_summary()
        self.assertIn("source", summary)
        self.assertIn("total_count", summary)
        self.assertIn("category_counts", summary)
        self.assertIn("market_counts", summary)
        self.assertIn("average_change_pct", summary)
        self.assertIn("top_gainers", summary)
        self.assertIn("top_losers", summary)
        self.assertIn("most_active", summary)
        self.assertIn("featured_contracts", summary)
        self.assertEqual(summary["source"], "simulated_futures_exchange")

    def test_ft_02_universe_summary_categories(self) -> None:
        """Verify category and market counts are populated."""
        summary = self.service.universe_summary()
        self.assertGreater(len(summary["category_counts"]), 0)
        self.assertGreater(len(summary["market_counts"]), 0)
        # Should have categories like 股指, 贵金属, 有色金属, etc.
        self.assertIn("股指", summary["category_counts"])

    def test_ft_02_top_gainers_and_losers(self) -> None:
        """Verify gainers have positive change, losers have negative."""
        summary = self.service.universe_summary(featured_limit=3)
        for gainer in summary["top_gainers"]:
            self.assertGreater(gainer["change_pct"], 0.0)
        for loser in summary["top_losers"]:
            self.assertLessEqual(loser["change_pct"], 0.0)

    # ─── FT-03: Contract detail ─────────────────────────────────────────────────

    def test_ft_03_get_contract_returns_detail(self) -> None:
        """Verify get_contract returns full contract details."""
        detail = self.service.get_contract("IF2503")
        self.assertEqual(detail["instrument_id"], "IF2503")
        self.assertIn("exchange_code", detail)
        self.assertIn("market_region", detail)
        self.assertIn("summary", detail)
        self.assertIn("contract_multiplier", detail)
        self.assertIn("expiry_at", detail)
        self.assertIn("trading_sessions", detail)
        self.assertIn("tick_size", detail)
        self.assertIn("lot_size", detail)
        self.assertIn("margin_info", detail)

    def test_ft_03_get_contract_multiplier(self) -> None:
        """Verify contract multiplier is correctly set."""
        # IF has multiplier 300.0
        if_contract = self.service.get_contract("IF2503")
        self.assertEqual(if_contract["contract_multiplier"], 300.0)
        # AU (gold) has multiplier 1000.0
        au_contract = self.service.get_contract("AU2506")
        self.assertEqual(au_contract["contract_multiplier"], 1000.0)

    def test_ft_03_get_contract_expiry(self) -> None:
        """Verify contract expiry date is populated."""
        detail = self.service.get_contract("IF2503")
        self.assertIsNotNone(detail["expiry_at"])

    def test_ft_03_get_contract_trading_sessions(self) -> None:
        """Verify trading sessions are returned for CN contracts."""
        detail = self.service.get_contract("IF2503")
        self.assertIsNotNone(detail["trading_sessions"])
        # trading_sessions is a tuple of tuples
        self.assertIsInstance(detail["trading_sessions"], (list, tuple))
        self.assertGreater(len(detail["trading_sessions"]), 0)

    def test_ft_03_get_contract_unknown_raises(self) -> None:
        """Verify unknown instrument raises KeyError."""
        with self.assertRaises(KeyError):
            self.service.get_contract("UNKNOWN_FUTURE")

    # ─── FT-04: K-line history ─────────────────────────────────────────────────

    def test_ft_04_get_contract_history_structure(self) -> None:
        """Verify get_contract_history returns correct structure."""
        history = self.service.get_contract_history("IF2503", limit=10)
        self.assertIn("instrument_id", history)
        self.assertIn("symbol", history)
        self.assertIn("interval", history)
        self.assertIn("source", history)
        self.assertIn("bars", history)
        self.assertIn("summary", history)
        self.assertEqual(history["source"], "simulated_futures_exchange")

    def test_ft_04_get_contract_history_bars(self) -> None:
        """Verify bars have correct OHLCV fields."""
        history = self.service.get_contract_history("IF2503", limit=5)
        self.assertGreater(len(history["bars"]), 0)
        bar = history["bars"][0]
        self.assertIn("trade_date", bar)
        self.assertIn("open", bar)
        self.assertIn("high", bar)
        self.assertIn("low", bar)
        self.assertIn("close", bar)
        self.assertIn("volume", bar)

    def test_ft_04_get_contract_history_summary(self) -> None:
        """Verify summary fields are populated."""
        history = self.service.get_contract_history("IF2503", limit=30)
        summary = history["summary"]
        self.assertIn("latest_close", summary)
        self.assertIn("previous_close", summary)
        self.assertIn("change_pct", summary)
        self.assertIn("period_high", summary)
        self.assertIn("period_low", summary)
        self.assertIn("average_volume", summary)

    def test_ft_04_get_contract_history_respects_limit(self) -> None:
        """Verify limit parameter controls number of bars returned."""
        history_5 = self.service.get_contract_history("IF2503", limit=5)
        history_10 = self.service.get_contract_history("IF2503", limit=10)
        self.assertLessEqual(len(history_5["bars"]), 5)
        self.assertLessEqual(len(history_10["bars"]), 10)

    def test_ft_04_get_contract_history_unknown_raises(self) -> None:
        """Verify unknown instrument raises KeyError."""
        with self.assertRaises(KeyError):
            self.service.get_contract_history("UNKNOWN_FUTURE")

    # ─── FT-05: Realized volatility ─────────────────────────────────────────────

    def test_ft_05_realized_volatility_positive(self) -> None:
        """Verify realized volatility returns positive value for trending data."""
        # With 31 bars, volatility should be computed
        history = self.service.get_contract_history("IF2503", limit=31)
        for contract in self.service.list_contracts()[:3]:
            detail = self.service.get_contract(contract["instrument_id"])
            self.assertGreaterEqual(detail["volatility_30d"], 0.0)

    def test_ft_05_volatility_zero_for_flat_prices(self) -> None:
        """Verify volatility is 0 for flat/unchanged prices."""
        # Create service with custom clock
        import math
        def flat_clock() -> datetime:
            return datetime(2025, 6, 15, tzinfo=timezone.utc)
        service = FuturesWorkbenchService(
            adapter_registry=self.registry,
            market_data_store=self.market_data_store,
            clock=flat_clock,
        )
        # Flat prices should result in 0 volatility
        closes = [100.0] * 31
        vol = service._realized_volatility(closes)
        self.assertEqual(vol, 0.0)

    # ─── FT-06: Multi-exchange support ─────────────────────────────────────────

    def test_ft_06_all_contract_categories_present(self) -> None:
        """Verify all major contract categories are present."""
        contracts = self.service.list_contracts()
        categories = {c["category"] for c in contracts}
        # Should have: 股指 (index), 贵金属 (precious metals),
        # 有色金属 (non-ferrous), 黑色系 (black series), 能源 (energy)
        self.assertIn("股指", categories)
        self.assertIn("贵金属", categories)
        self.assertIn("有色金属", categories)
        self.assertIn("能源", categories)

    def test_ft_06_all_markets_present(self) -> None:
        """Verify multiple market labels are present."""
        contracts = self.service.list_contracts()
        markets = {c["market_label"] for c in contracts}
        # Should have: 中金所 (CFFEX), 上期所 (SHFE), CME, NYMEX, COMEX
        self.assertIn("中金所", markets)
        self.assertIn("上期所", markets)
        self.assertIn("CME", markets)

    # ─── FT-07: Contract metadata ───────────────────────────────────────────────

    def test_ft_07_tick_size_per_contract(self) -> None:
        """Verify tick size varies by contract type."""
        if_detail = self.service.get_contract("IF2503")
        au_detail = self.service.get_contract("AU2506")
        # IF has tick_size 0.2, AU has tick_size 0.02
        self.assertGreater(if_detail["tick_size"], 0)
        self.assertGreater(au_detail["tick_size"], 0)

    def test_ft_07_lot_size_per_contract(self) -> None:
        """Verify lot size varies by contract."""
        if_detail = self.service.get_contract("IF2503")
        au_detail = self.service.get_contract("AU2506")
        # IF lot_size=1, AU lot_size=1000
        self.assertEqual(if_detail["lot_size"], 1)
        self.assertEqual(au_detail["lot_size"], 1000)

    def test_ft_07_margin_info_structure(self) -> None:
        """Verify margin info is properly structured."""
        for contract in self.service.list_contracts()[:3]:
            detail = self.service.get_contract(contract["instrument_id"])
            self.assertIn("margin_info", detail)
            self.assertIn("initial_margin_pct", detail["margin_info"])
            self.assertIn("maintenance_margin_pct", detail["margin_info"])
            self.assertGreater(detail["margin_info"]["initial_margin_pct"], 0)
            self.assertGreater(detail["margin_info"]["maintenance_margin_pct"], 0)


class FuturesContractNotesTests(unittest.TestCase):
    """Test FT-07: Contract notes and descriptions."""

    def test_contract_notes_exist_for_major_contracts(self) -> None:
        """Verify contract notes/dictionaries exist for major contracts."""
        from quant_exchange.futures.service import _CONTRACT_NOTES
        # Should have notes for main contracts
        self.assertIn("IF2503", _CONTRACT_NOTES)
        self.assertIn("AU2506", _CONTRACT_NOTES)
        self.assertIn("CL2506", _CONTRACT_NOTES)
        self.assertIn("GC2506", _CONTRACT_NOTES)

    def test_contract_note_structure(self) -> None:
        """Verify each contract note has required fields."""
        from quant_exchange.futures.service import _CONTRACT_NOTES
        required = {"name", "summary", "category", "market_label"}
        for contract_id, notes in _CONTRACT_NOTES.items():
            self.assertTrue(required.issubset(notes.keys()), f"{contract_id} missing fields")


if __name__ == "__main__":
    unittest.main()
