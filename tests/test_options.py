"""Tests for Options module (OPT-01 ~ OPT-04).

Tests:
- OPT-01: Black-Scholes pricing for European options
- OPT-02: Greeks calculation (delta, gamma, theta, vega, rho)
- OPT-03: Implied volatility calculation
- OPT-04: Volatility surface and multi-leg strategies
"""

from __future__ import annotations

import math
import unittest
from datetime import datetime, timezone

from quant_exchange.enhanced.options import (
    OptionContract,
    OptionType,
    ExerciseStyle,
    StrategyLeg,
    StrategyLegRole,
    OptionStrategy,
    VolSurfacePoint,
    VolSurface,
    OptionsService,
    black_scholes_price,
    black_scholes_greeks,
    implied_volatility,
)


class BlackScholesPricingTests(unittest.TestCase):
    """Test OPT-01: Black-Scholes pricing."""

    def test_opt_01_call_price_at_money(self) -> None:
        """Verify ATM call option pricing (S=K, T>0)."""
        # S=100, K=100, T=1 year, r=5%, sigma=20%, q=0
        price = black_scholes_price(100.0, 100.0, 1.0, 0.05, 0.20, 0.0, OptionType.CALL)
        # ATM price should be roughly 10 (intrinsic is 0, time value dominates)
        self.assertGreater(price, 5.0)
        self.assertLess(price, 15.0)

    def test_opt_01_put_price_at_money(self) -> None:
        """Verify ATM put option pricing."""
        price = black_scholes_price(100.0, 100.0, 1.0, 0.05, 0.20, 0.0, OptionType.PUT)
        # ATM put should be similar to call for no-dividend stock
        self.assertGreater(price, 5.0)
        self.assertLess(price, 15.0)

    def test_opt_01_call_intrinsic_at_expiry(self) -> None:
        """Verify call option returns intrinsic value at expiry."""
        # ITM call: S=110, K=100, T=0
        price = black_scholes_price(110.0, 100.0, 0.0, 0.05, 0.20, 0.0, OptionType.CALL)
        self.assertEqual(price, 10.0)

    def test_opt_01_put_intrinsic_at_expiry(self) -> None:
        """Verify put option returns intrinsic value at expiry."""
        # ITM put: S=90, K=100, T=0
        price = black_scholes_price(90.0, 100.0, 0.0, 0.05, 0.20, 0.0, OptionType.PUT)
        self.assertEqual(price, 10.0)

    def test_opt_01_call_zero_at_deep_otm(self) -> None:
        """Verify deep OTM call has zero price."""
        # Deep OTM: S=80, K=200
        price = black_scholes_price(80.0, 200.0, 1.0, 0.05, 0.20, 0.0, OptionType.CALL)
        self.assertLess(price, 0.01)

    def test_opt_01_put_zero_at_deep_otm(self) -> None:
        """Verify deep OTM put has zero price."""
        # Deep OTM put: S=200, K=50
        price = black_scholes_price(200.0, 50.0, 1.0, 0.05, 0.20, 0.0, OptionType.PUT)
        self.assertLess(price, 0.01)

    def test_opt_01_call_price_increases_with_spot(self) -> None:
        """Verify call price increases with spot price."""
        prices = [black_scholes_price(s, 100.0, 1.0, 0.05, 0.20, 0.0, OptionType.CALL) for s in [90, 100, 110]]
        for i in range(len(prices) - 1):
            self.assertGreater(prices[i + 1], prices[i])

    def test_opt_01_put_price_decreases_with_spot(self) -> None:
        """Verify put price decreases with spot price."""
        prices = [black_scholes_price(s, 100.0, 1.0, 0.05, 0.20, 0.0, OptionType.PUT) for s in [90, 100, 110]]
        for i in range(len(prices) - 1):
            self.assertLess(prices[i + 1], prices[i])

    def test_opt_01_call_price_increases_with_volatility(self) -> None:
        """Verify call price increases with volatility."""
        prices = [black_scholes_price(100.0, 100.0, 1.0, 0.05, sig, 0.0, OptionType.CALL) for sig in [0.10, 0.20, 0.30]]
        for i in range(len(prices) - 1):
            self.assertGreater(prices[i + 1], prices[i])

    def test_opt_01_put_call_parity(self) -> None:
        """Verify put-call parity: C - P = S - K*e^(-rT)."""
        S, K, T, r, sigma = 100.0, 100.0, 1.0, 0.05, 0.20
        call = black_scholes_price(S, K, T, r, sigma, 0.0, OptionType.CALL)
        put = black_scholes_price(S, K, T, r, sigma, 0.0, OptionType.PUT)
        lhs = call - put
        rhs = S - K * math.exp(-r * T)
        self.assertAlmostEqual(lhs, rhs, places=4)

    def test_opt_01_dividend_reduces_call_price(self) -> None:
        """Verify higher dividend yield reduces call price."""
        price_no_div = black_scholes_price(100.0, 100.0, 1.0, 0.05, 0.20, 0.0, OptionType.CALL)
        price_with_div = black_scholes_price(100.0, 100.0, 1.0, 0.05, 0.20, 0.03, OptionType.CALL)
        self.assertLess(price_with_div, price_no_div)


class BlackScholesGreeksTests(unittest.TestCase):
    """Test OPT-02: Greeks calculation."""

    def test_opt_02_call_delta_at_money(self) -> None:
        """Verify ATM call delta is approximately 0.5 (positive, less than 1)."""
        greeks = black_scholes_greeks(100.0, 100.0, 1.0, 0.05, 0.20, 0.0, OptionType.CALL)
        self.assertGreater(greeks.delta, 0.4)
        self.assertLess(greeks.delta, 1.0)

    def test_opt_02_call_delta_at_itm(self) -> None:
        """Verify ITM call delta approaches 1.0."""
        greeks = black_scholes_greeks(150.0, 100.0, 1.0, 0.05, 0.20, 0.0, OptionType.CALL)
        self.assertGreater(greeks.delta, 0.9)

    def test_opt_02_call_delta_at_otm(self) -> None:
        """Verify OTM call delta approaches 0.0."""
        greeks = black_scholes_greeks(50.0, 100.0, 1.0, 0.05, 0.20, 0.0, OptionType.CALL)
        self.assertLess(greeks.delta, 0.1)

    def test_opt_02_put_delta_at_money(self) -> None:
        """Verify ATM put delta is negative (between -1 and 0)."""
        greeks = black_scholes_greeks(100.0, 100.0, 1.0, 0.05, 0.20, 0.0, OptionType.PUT)
        self.assertLess(greeks.delta, 0.0)
        self.assertGreater(greeks.delta, -1.0)

    def test_opt_02_gamma_same_for_call_put(self) -> None:
        """Verify gamma is the same for calls and puts."""
        call_greeks = black_scholes_greeks(100.0, 100.0, 1.0, 0.05, 0.20, 0.0, OptionType.CALL)
        put_greeks = black_scholes_greeks(100.0, 100.0, 1.0, 0.05, 0.20, 0.0, OptionType.PUT)
        self.assertAlmostEqual(call_greeks.gamma, put_greeks.gamma, places=6)

    def test_opt_02_gamma_near_atm_higher_than_deep(self) -> None:
        """Verify gamma near ATM is higher than deep ITM/OTM options."""
        greeks_atm = black_scholes_greeks(100.0, 100.0, 1.0, 0.05, 0.20, 0.0, OptionType.CALL)
        greeks_itm = black_scholes_greeks(120.0, 100.0, 1.0, 0.05, 0.20, 0.0, OptionType.CALL)
        greeks_otm = black_scholes_greeks(80.0, 100.0, 1.0, 0.05, 0.20, 0.0, OptionType.CALL)
        # ATM gamma should be higher than deep ITM/OTM
        self.assertGreater(greeks_atm.gamma, greeks_itm.gamma)
        self.assertGreater(greeks_atm.gamma, greeks_otm.gamma)

    def test_opt_02_theta_negative_for_long_call(self) -> None:
        """Verify theta is negative (time decay) for long call."""
        greeks = black_scholes_greeks(100.0, 100.0, 1.0, 0.05, 0.20, 0.0, OptionType.CALL)
        # Theta is expressed per day, should be negative
        self.assertLess(greeks.theta, 0.0)

    def test_opt_02_vega_positive_for_long_call(self) -> None:
        """Verify vega is positive for long call."""
        greeks = black_scholes_greeks(100.0, 100.0, 1.0, 0.05, 0.20, 0.0, OptionType.CALL)
        self.assertGreater(greeks.vega, 0.0)

    def test_opt_02_rho_positive_for_call(self) -> None:
        """Verify rho is positive for call."""
        greeks = black_scholes_greeks(100.0, 100.0, 1.0, 0.05, 0.20, 0.0, OptionType.CALL)
        self.assertGreater(greeks.rho, 0.0)

    def test_opt_02_rho_negative_for_put(self) -> None:
        """Verify rho is negative for put."""
        greeks = black_scholes_greeks(100.0, 100.0, 1.0, 0.05, 0.20, 0.0, OptionType.PUT)
        self.assertLess(greeks.rho, 0.0)

    def test_opt_02_greeks_zero_at_expiry(self) -> None:
        """Verify Greeks (except premium) are handled correctly at expiry."""
        greeks = black_scholes_greeks(110.0, 100.0, 0.0, 0.05, 0.20, 0.0, OptionType.CALL)
        # At expiry, premium = intrinsic
        self.assertEqual(greeks.premium, 10.0)
        self.assertEqual(greeks.intrinsic, 10.0)


class ImpliedVolatilityTests(unittest.TestCase):
    """Test OPT-03: Implied volatility calculation."""

    def test_opt_03_iv_roundtrip(self) -> None:
        """Verify IV calculation roundtrips with known price."""
        S, K, T, r, sigma, q = 100.0, 100.0, 1.0, 0.05, 0.20, 0.0
        market_price = black_scholes_price(S, K, T, r, sigma, q, OptionType.CALL)
        iv = implied_volatility(market_price, S, K, T, r, q, OptionType.CALL)
        self.assertAlmostEqual(iv, sigma, places=3)

    def test_opt_03_iv_put_roundtrip(self) -> None:
        """Verify IV calculation roundtrips for put."""
        S, K, T, r, sigma, q = 100.0, 100.0, 1.0, 0.05, 0.20, 0.0
        market_price = black_scholes_price(S, K, T, r, sigma, q, OptionType.PUT)
        iv = implied_volatility(market_price, S, K, T, r, q, OptionType.PUT)
        self.assertAlmostEqual(iv, sigma, places=3)

    def test_opt_03_iv_zero_for_zero_price(self) -> None:
        """Verify IV returns 0 for zero price."""
        iv = implied_volatility(0.0, 100.0, 100.0, 1.0, 0.05, 0.0, OptionType.CALL)
        self.assertEqual(iv, 0.0)

    def test_opt_03_iv_bounded(self) -> None:
        """Verify IV is bounded between reasonable values."""
        # Deep ITM call with low price shouldn't give extreme IV
        iv = implied_volatility(5.0, 50.0, 100.0, 1.0, 0.05, 0.0, OptionType.CALL)
        self.assertGreaterEqual(iv, 0.01)
        self.assertLess(iv, 5.0)


class OptionsServiceTests(unittest.TestCase):
    """Test OPT-04: Options service and contracts."""

    def setUp(self) -> None:
        self.service = OptionsService()

    def test_opt_04_register_and_get_contract(self) -> None:
        """Verify contract can be registered and retrieved."""
        contract = self.service.register_contract(
            underlying="AAPL",
            option_type=OptionType.CALL,
            strike=150.0,
            expiry=datetime(2025, 1, 1, tzinfo=timezone.utc),
        )
        retrieved = self.service.get_contract(contract.contract_id)
        self.assertIsNotNone(retrieved)
        self.assertEqual(retrieved.underlying, "AAPL")
        self.assertEqual(retrieved.strike, 150.0)

    def test_opt_04_price_contract(self) -> None:
        """Verify service can price a contract (returns Greeks)."""
        contract = self.service.register_contract(
            underlying="AAPL",
            option_type=OptionType.CALL,
            strike=150.0,
            expiry=datetime(2025, 6, 1, tzinfo=timezone.utc),  # Future expiry
        )
        # Price at S=155, vol=20%
        greeks = self.service.price_contract(contract.contract_id, spot_price=155.0, volatility=0.20)
        self.assertIsNotNone(greeks)
        self.assertGreater(greeks.premium, 0.0)
        self.assertIsNotNone(greeks.delta)

    def test_opt_04_volatility_surface_add_point(self) -> None:
        """Verify volatility surface points can be added."""
        self.service.add_vol_surface_point(
            underlying="AAPL",
            strike=150.0,
            expiry=datetime(2025, 6, 1, tzinfo=timezone.utc),
            implied_vol=0.25,
            bid_vol=0.24,
            ask_vol=0.26,
        )
        surface = self.service.get_vol_surface("AAPL")
        self.assertIsNotNone(surface)
        self.assertEqual(len(surface.points), 1)
        self.assertEqual(surface.points[0].strike, 150.0)


class VolSurfaceTests(unittest.TestCase):
    """Test OPT-04: Volatility surface."""

    def test_vol_surface_point_creation(self) -> None:
        """Verify VolSurfacePoint creation."""
        point = VolSurfacePoint(
            strike=100.0,
            expiry=datetime(2025, 1, 1, tzinfo=timezone.utc),
            implied_vol=0.20,
            bid_vol=0.19,
            ask_vol=0.21,
            timestamp=datetime.now(timezone.utc),
        )
        self.assertEqual(point.strike, 100.0)
        self.assertEqual(point.implied_vol, 0.20)

    def test_vol_surface_creation(self) -> None:
        """Verify VolSurface creation."""
        surface = VolSurface(
            underlying="AAPL",
            points=[],
            atm_vol=0.25,
            term_structure={"1m": 0.22, "3m": 0.24, "1y": 0.26},
        )
        self.assertEqual(surface.underlying, "AAPL")
        self.assertEqual(len(surface.points), 0)
        self.assertIn("1m", surface.term_structure)


if __name__ == "__main__":
    unittest.main()
