"""Tests for FX-01~FX-04 forex and commodities service."""

import unittest
from datetime import datetime, timedelta, timezone

from quant_exchange.forex import (
    AssetClass,
    Commodity,
    CrossAssetRisk,
    CurrencyPair,
    CurrencyStrengthIndicator,
    EconomicEvent,
    EconomicImpact,
    ForexQuote,
    ForexService,
)


class TestForexService(unittest.TestCase):
    """Tests for ForexService (FX-01 ~ FX-04)."""

    def setUp(self) -> None:
        self.svc = ForexService()

    def test_fx_01_list_pairs(self) -> None:
        """Test listing currency pairs (FX-01)."""
        pairs = self.svc.list_pairs()
        self.assertGreater(len(pairs), 0)
        self.assertTrue(any(p.instrument_id == "EURUSD" for p in pairs))
        print(f"\n[FX-01] Currency pairs: {len(pairs)}")

    def test_fx_01_get_pair(self) -> None:
        """Test getting a specific currency pair (FX-01)."""
        pair = self.svc.get_pair("EURUSD")
        self.assertIsNotNone(pair)
        self.assertEqual(pair.base_currency, "EUR")
        self.assertEqual(pair.quote_currency, "USD")
        print(f"\n[FX-01] EURUSD pair: base={pair.base_currency}, quote={pair.quote_currency}")

    def test_fx_02_list_commodities(self) -> None:
        """Test listing commodities (FX-02)."""
        commodities = self.svc.list_commodities()
        self.assertGreater(len(commodities), 0)
        self.assertTrue(any(c.instrument_id == "XAUUSD" for c in commodities))
        self.assertTrue(any(c.instrument_id == "USOIL" for c in commodities))
        print(f"\n[FX-02] Commodities: {len(commodities)}")

    def test_fx_02_simulate_quote(self) -> None:
        """Test simulating a quote (FX-02)."""
        quote = self.svc.simulate_quote("XAUUSD", base_price=2030.0)
        self.assertIsNotNone(quote)
        self.assertEqual(quote.instrument_id, "XAUUSD")
        self.assertGreater(quote.bid, 0)
        self.assertGreater(quote.ask, quote.bid)
        print(f"\n[FX-02] XAUUSD quote: bid={quote.bid:.2f}, ask={quote.ask:.2f}")

    def test_fx_02_update_quote(self) -> None:
        """Test updating a quote (FX-02)."""
        quote = self.svc.update_quote("EURUSD", 1.0850, 1.0852)
        self.assertIsNotNone(quote)
        self.assertEqual(quote.instrument_id, "EURUSD")
        self.assertGreater(quote.spread_pips, 0)
        print(f"\n[FX-02] EURUSD updated: spread={quote.spread_pips:.1f} pips")

    def test_fx_03_currency_strength(self) -> None:
        """Test computing currency strength (FX-03)."""
        # Simulate quotes first
        self.svc.simulate_quote("EURUSD")
        self.svc.simulate_quote("GBPUSD")
        self.svc.simulate_quote("USDJPY")

        strengths = self.svc.compute_currency_strength()
        self.assertIsInstance(strengths, list)
        self.assertGreater(len(strengths), 0)

        # Check ranking
        ranks = [s.rank for s in strengths]
        self.assertEqual(sorted(ranks), list(range(1, len(ranks) + 1)))
        print(f"\n[FX-03] Currency strength: {[(s.currency, s.rank, s.strength_value) for s in strengths[:3]]}")

    def test_fx_03_economic_calendar(self) -> None:
        """Test economic calendar events (FX-03)."""
        now = datetime.now(timezone.utc)
        event = self.svc.add_economic_event(
            country="US",
            currency="USD",
            event_name="Non-Farm Payrolls",
            impact=EconomicImpact.HIGH,
            release_time=now + timedelta(hours=12),
            previous_value="200K",
            forecast_value="180K",
        )
        self.assertIsNotNone(event)
        self.assertEqual(event.currency, "USD")
        self.assertEqual(event.impact, EconomicImpact.HIGH)

        upcoming = self.svc.get_upcoming_events(hours=24)
        self.assertGreater(len(upcoming), 0)
        print(f"\n[FX-03] Upcoming events: {len(upcoming)}, first: {upcoming[0].event_name}")

    def test_fx_03_correlation_matrix(self) -> None:
        """Test correlation matrix computation (FX-03)."""
        self.svc.simulate_quote("EURUSD")
        self.svc.simulate_quote("GBPUSD")
        self.svc.simulate_quote("XAUUSD")

        matrix = self.svc.get_correlation_matrix(["EURUSD", "GBPUSD", "XAUUSD"])
        self.assertIn("EURUSD", matrix)
        self.assertIn("GBPUSD", matrix["EURUSD"])
        self.assertEqual(matrix["EURUSD"]["EURUSD"], 1.0)
        print(f"\n[FX-03] Correlation EUR/GBP: {matrix['EURUSD']['GBPUSD']:.2f}")

    def test_fx_04_cross_asset_risk(self) -> None:
        """Test cross-asset risk computation (FX-04)."""
        positions = {
            "EURUSD": 100_000.0,
            "GBPUSD": 50_000.0,
            "XAUUSD": 200_000.0,
        }
        risk = self.svc.compute_cross_asset_risk(positions)
        self.assertIsInstance(risk, CrossAssetRisk)
        self.assertIn("EURUSD", risk.risk_concentration)
        total_concentration = sum(risk.risk_concentration.values())
        self.assertAlmostEqual(total_concentration, 100.0, places=1)
        print(f"\n[FX-04] Cross-asset risk: concentration={risk.risk_concentration}")


class TestForexServiceIntegration(unittest.TestCase):
    """Integration tests for forex API endpoints."""

    def setUp(self) -> None:
        from quant_exchange.platform import QuantTradingPlatform
        from quant_exchange.config import AppSettings
        import tempfile
        from pathlib import Path

        self.temp_dir = tempfile.TemporaryDirectory()
        db_path = str(Path(self.temp_dir.name) / "forex_test.sqlite3")
        self.platform = QuantTradingPlatform(
            AppSettings.from_mapping({"database": {"url": db_path}})
        )

    def tearDown(self) -> None:
        self.platform.close()
        self.temp_dir.cleanup()

    def test_fx_list_pairs_api(self) -> None:
        """Test fx_list_pairs API endpoint."""
        result = self.platform.api.fx_list_pairs()
        self.assertEqual(result["code"], "OK")
        self.assertGreater(len(result["data"]["pairs"]), 0)
        print(f"\n[FX-01 API] Pairs: {len(result['data']['pairs'])}")

    def test_fx_list_commodities_api(self) -> None:
        """Test fx_list_commodities API endpoint."""
        result = self.platform.api.fx_list_commodities()
        self.assertEqual(result["code"], "OK")
        self.assertGreater(len(result["data"]["commodities"]), 0)
        print(f"\n[FX-02 API] Commodities: {len(result['data']['commodities'])}")

    def test_fx_get_quote_api(self) -> None:
        """Test fx_get_quote API endpoint."""
        result = self.platform.api.fx_get_quote("EURUSD")
        self.assertEqual(result["code"], "OK")
        data = result["data"]
        self.assertIn("bid", data)
        self.assertIn("ask", data)
        self.assertIn("spread_pips", data)
        print(f"\n[FX-01 API] EURUSD quote: {data['bid']:.4f}/{data['ask']:.4f}")

    def test_fx_currency_strength_api(self) -> None:
        """Test fx_get_currency_strength API endpoint."""
        result = self.platform.api.fx_get_currency_strength()
        self.assertEqual(result["code"], "OK")
        self.assertIn("currencies", result["data"])
        print(f"\n[FX-03 API] Currency strengths: {len(result['data']['currencies'])}")

    def test_fx_economic_calendar_api(self) -> None:
        """Test fx_get_economic_calendar API endpoint."""
        result = self.platform.api.fx_get_economic_calendar(hours=48)
        self.assertEqual(result["code"], "OK")
        self.assertIn("events", result["data"])
        print(f"\n[FX-03 API] Calendar events: {len(result['data']['events'])}")

    def test_fx_cross_asset_risk_api(self) -> None:
        """Test fx_cross_asset_risk API endpoint."""
        result = self.platform.api.fx_cross_asset_risk({
            "EURUSD": 100000.0,
            "XAUUSD": 50000.0,
        })
        self.assertEqual(result["code"], "OK")
        data = result["data"]
        self.assertIn("currency_exposure", data)
        print(f"\n[FX-04 API] Currency exposure: {data['currency_exposure']}")


if __name__ == "__main__":
    unittest.main(verbosity=2)
