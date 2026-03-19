"""Options greeks pricing and strategy service (OPT-01 ~ OPT-04).

Covers:
- Real-time Black-Scholes/Merton pricing
- Greeks: delta, gamma, theta, vega, rho
- Volatility surface (strike × expiry)
- Multi-leg strategy builder with combined Greeks
- Implied volatility calculation
"""

from __future__ import annotations

import math
import uuid
from collections import defaultdict
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


# ─────────────────────────────────────────────────────────────────────────────
# Enums
# ─────────────────────────────────────────────────────────────────────────────

class OptionType(str, Enum):
    CALL = "call"
    PUT = "put"


class ExerciseStyle(str, Enum):
    EUROPEAN = "european"
    AMERICAN = "american"


class StrategyLegRole(str, Enum):
    LONG = "long"
    SHORT = "short"


# ─────────────────────────────────────────────────────────────────────────────
# Data Models
# ─────────────────────────────────────────────────────────────────────────────

@dataclass(slots=True)
class OptionContract:
    """An option contract definition."""

    contract_id: str
    underlying: str          # e.g. "AAPL"
    option_type: OptionType   # CALL or PUT
    strike: float
    expiry: datetime
    exercise_style: ExerciseStyle = ExerciseStyle.EUROPEAN
    multiplier: float = 100.0  # Standard equity option
    currency: str = "USD"


@dataclass(slots=True)
class Greeks:
    """Option Greeks values."""

    delta: float = 0.0
    gamma: float = 0.0
    theta: float = 0.0
    vega: float = 0.0
    rho: float = 0.0
    premium: float = 0.0
    intrinsic: float = 0.0
    as_of: str = field(default_factory=_now)


@dataclass(slots=True)
class StrategyLeg:
    """A single leg in a multi-leg options strategy."""

    leg_id: str
    contract: OptionContract
    role: StrategyLegRole      # LONG or SHORT
    quantity: int = 1          # Number of contracts


@dataclass(slots=True)
class OptionStrategy:
    """A multi-leg options strategy."""

    strategy_id: str
    name: str
    legs: tuple[StrategyLeg, ...]
    net_delta: float = 0.0
    net_gamma: float = 0.0
    net_theta: float = 0.0
    net_vega: float = 0.0
    net_rho: float = 0.0
    max_profit: float = 0.0
    max_loss: float = 0.0
    breakeven: tuple[float, ...] = field(default_factory=tuple)
    created_at: str = field(default_factory=_now)


@dataclass(slots=True)
class VolSurfacePoint:
    """A point on the volatility surface."""

    strike: float
    expiry: datetime
    implied_vol: float
    bid_vol: float
    ask_vol: float
    timestamp: datetime


@dataclass(slots=True)
class VolSurface:
    """Volatility surface for an underlying."""

    underlying: str
    points: list[VolSurfacePoint]
    atm_vol: float = 0.0      # ATM vol at nearest expiry
    term_structure: dict[str, float] = field(default_factory=dict)  # expiry_str -> vol


# ─────────────────────────────────────────────────────────────────────────────
# Black-Scholes / Merton Pricing
# ─────────────────────────────────────────────────────────────────────────────

def _norm_cdf(x: float) -> float:
    """Standard normal cumulative distribution function."""
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _norm_pdf(x: float) -> float:
    """Standard normal probability density function."""
    return math.exp(-0.5 * x * x) / math.sqrt(2.0 * math.pi)


def _d1_d2(S: float, K: float, T: float, r: float, q: float, sigma: float) -> tuple[float, float]:
    """Calculate d1 and d2 for Black-Scholes."""
    if T <= 0 or sigma <= 0:
        return 0.0, 0.0
    d1 = (math.log(S / K) + (r - q + 0.5 * sigma * sigma) * T) / (sigma * math.sqrt(T))
    d2 = d1 - sigma * math.sqrt(T)
    return d1, d2


def black_scholes_price(
    S: float,      # Spot price
    K: float,      # Strike
    T: float,      # Time to expiry (years)
    r: float,      # Risk-free rate (annual)
    sigma: float,  # Volatility (annual)
    q: float = 0.0,  # Dividend yield
    option_type: OptionType = OptionType.CALL,
) -> float:
    """Black-Scholes theoretical price for European options."""
    if T <= 0:
        # At expiry
        if option_type == OptionType.CALL:
            return max(0.0, S - K)
        else:
            return max(0.0, K - S)

    d1, d2 = _d1_d2(S, K, T, r, q, sigma)
    if option_type == OptionType.CALL:
        price = S * math.exp(-q * T) * _norm_cdf(d1) - K * math.exp(-r * T) * _norm_cdf(d2)
    else:
        price = K * math.exp(-r * T) * _norm_cdf(-d2) - S * math.exp(-q * T) * _norm_cdf(-d1)
    return max(0.0, price)


def black_scholes_greeks(
    S: float,
    K: float,
    T: float,
    r: float,
    sigma: float,
    q: float = 0.0,
    option_type: OptionType = OptionType.CALL,
) -> Greeks:
    """Calculate all Greeks for a European option."""
    if T <= 0 or sigma <= 0:
        intrinsic = max(0.0, S - K) if option_type == OptionType.CALL else max(0.0, K - S)
        return Greeks(premium=intrinsic, intrinsic=intrinsic)

    d1, d2 = _d1_d2(S, K, T, r, q, sigma)
    sqrt_T = math.sqrt(T)

    # Common terms
    exp_q_T = math.exp(-q * T)
    exp_r_T = math.exp(-r * T)
    nd1 = _norm_pdf(d1)

    # Delta
    if option_type == OptionType.CALL:
        delta = exp_q_T * _norm_cdf(d1)
    else:
        delta = exp_q_T * (_norm_cdf(d1) - 1.0)

    # Gamma (same for call and put)
    gamma = exp_q_T * nd1 / (S * sigma * sqrt_T)

    # Theta (per day, so divide by 365)
    if option_type == OptionType.CALL:
        theta = (-(S * exp_q_T * nd1 * sigma) / (2 * sqrt_T)
                 - r * K * exp_r_T * _norm_cdf(d2)
                 + q * S * exp_q_T * _norm_cdf(d1)) / 365.0
    else:
        theta = (-(S * exp_q_T * nd1 * sigma) / (2 * sqrt_T)
                 + r * K * exp_r_T * _norm_cdf(-d2)
                 - q * S * exp_q_T * _norm_cdf(-d1)) / 365.0

    # Vega (per 1% move, so divide by 100)
    vega = S * exp_q_T * nd1 * sqrt_T / 100.0

    # Rho (per 1% move, so divide by 100)
    if option_type == OptionType.CALL:
        rho = K * T * exp_r_T * _norm_cdf(d2) / 100.0
    else:
        rho = -K * T * exp_r_T * _norm_cdf(-d2) / 100.0

    premium = black_scholes_price(S, K, T, r, sigma, q, option_type)
    intrinsic = max(0.0, S - K) if option_type == OptionType.CALL else max(0.0, K - S)

    return Greeks(
        delta=delta,
        gamma=gamma,
        theta=theta,
        vega=vega,
        rho=rho,
        premium=premium,
        intrinsic=intrinsic,
    )


def implied_volatility(
    market_price: float,
    S: float,
    K: float,
    T: float,
    r: float,
    q: float = 0.0,
    option_type: OptionType = OptionType.CALL,
    tol: float = 1e-6,
    max_iter: int = 100,
) -> float:
    """Newton-Raphson implied volatility calculation."""
    if market_price <= 0:
        return 0.0

    # Bracket
    sigma = 0.20  # Initial guess
    for _ in range(max_iter):
        price = black_scholes_price(S, K, T, r, sigma, q, option_type)
        diff = market_price - price
        if abs(diff) < tol:
            return sigma

        # Vega for Newton step
        greeks = black_scholes_greeks(S, K, T, r, sigma, q, option_type)
        vega = greeks.vega * 100  # Convert back from per-1% to per-1-unit
        if abs(vega) < 1e-10:
            break

        sigma += diff / vega
        sigma = max(0.01, min(sigma, 5.0))  # Bound

    return sigma


# ─────────────────────────────────────────────────────────────────────────────
# Options Service
# ─────────────────────────────────────────────────────────────────────────────

class OptionsService:
    """Options greeks pricing and strategy service (OPT-01 ~ OPT-04).

    Provides:
    - Black-Scholes/Merton pricing for European options
    - Full Greeks calculation (delta, gamma, theta, vega, rho)
    - Implied volatility solver
    - Volatility surface management
    - Multi-leg strategy builder with combined Greeks
    """

    def __init__(self, persistence=None) -> None:
        self.persistence = persistence
        self._contracts: dict[str, OptionContract] = {}
        self._strategies: dict[str, OptionStrategy] = {}
        self._vol_surfaces: dict[str, VolSurface] = {}
        self._default_r = 0.05   # 5% risk-free rate
        self._default_q = 0.0    # 0% dividend yield

    # ── Contract Management ───────────────────────────────────────────────

    def register_contract(
        self,
        underlying: str,
        option_type: OptionType,
        strike: float,
        expiry: datetime,
        exercise_style: ExerciseStyle = ExerciseStyle.EUROPEAN,
        multiplier: float = 100.0,
        currency: str = "USD",
    ) -> OptionContract:
        """Register an option contract."""
        contract_id = f"opt:{uuid.uuid4().hex[:12]}"
        contract = OptionContract(
            contract_id=contract_id,
            underlying=underlying,
            option_type=option_type,
            strike=strike,
            expiry=expiry,
            exercise_style=exercise_style,
            multiplier=multiplier,
            currency=currency,
        )
        self._contracts[contract_id] = contract
        return contract

    def get_contract(self, contract_id: str) -> OptionContract | None:
        """Get a contract by ID."""
        return self._contracts.get(contract_id)

    def get_contracts_for_underlying(
        self,
        underlying: str,
        expiry_after: datetime | None = None,
    ) -> list[OptionContract]:
        """Get all contracts for an underlying."""
        results = [
            c for c in self._contracts.values()
            if c.underlying == underlying
            and (expiry_after is None or c.expiry > expiry_after)
        ]
        return sorted(results, key=lambda c: c.expiry)

    # ── Pricing ───────────────────────────────────────────────────────────

    def price_contract(
        self,
        contract_id: str,
        spot_price: float,
        volatility: float,
        risk_free_rate: float | None = None,
        dividend_yield: float | None = None,
        as_of: datetime | None = None,
    ) -> Greeks | None:
        """Price an option contract and return Greeks."""
        contract = self._contracts.get(contract_id)
        if not contract:
            return None

        r = risk_free_rate if risk_free_rate is not None else self._default_r
        q = dividend_yield if dividend_yield is not None else self._default_q

        if as_of is None:
            as_of = datetime.now(timezone.utc)
        T = (contract.expiry - as_of).total_seconds() / (365.0 * 86400.0)
        T = max(T, 1e-6)

        return black_scholes_greeks(
            S=spot_price,
            K=contract.strike,
            T=T,
            r=r,
            sigma=volatility,
            q=q,
            option_type=contract.option_type,
        )

    def price_generic(
        self,
        spot: float,
        strike: float,
        expiry: datetime,
        volatility: float,
        option_type: OptionType,
        risk_free_rate: float = 0.05,
        dividend_yield: float = 0.0,
        as_of: datetime | None = None,
    ) -> Greeks:
        """Price a generic option without registering a contract."""
        if as_of is None:
            as_of = datetime.now(timezone.utc)
        T = (expiry - as_of).total_seconds() / (365.0 * 86400.0)
        T = max(T, 1e-6)
        return black_scholes_greeks(
            S=spot, K=strike, T=T, r=risk_free_rate,
            sigma=volatility, q=dividend_yield, option_type=option_type,
        )

    def calculate_implied_vol(
        self,
        market_price: float,
        spot: float,
        strike: float,
        expiry: datetime,
        option_type: OptionType,
        risk_free_rate: float = 0.05,
        dividend_yield: float = 0.0,
        as_of: datetime | None = None,
    ) -> float:
        """Calculate implied volatility from market price."""
        if as_of is None:
            as_of = datetime.now(timezone.utc)
        T = (expiry - as_of).total_seconds() / (365.0 * 86400.0)
        T = max(T, 1e-6)
        return implied_volatility(
            market_price=market_price,
            S=spot, K=strike, T=T,
            r=risk_free_rate, q=dividend_yield,
            option_type=option_type,
        )

    # ── Volatility Surface ────────────────────────────────────────────────

    def add_vol_surface_point(
        self,
        underlying: str,
        strike: float,
        expiry: datetime,
        implied_vol: float,
        bid_vol: float,
        ask_vol: float,
    ) -> None:
        """Add a point to the volatility surface."""
        if underlying not in self._vol_surfaces:
            self._vol_surfaces[underlying] = VolSurface(
                underlying=underlying,
                points=[],
                atm_vol=0.0,
                term_structure={},
            )
        point = VolSurfacePoint(
            strike=strike,
            expiry=expiry,
            implied_vol=implied_vol,
            bid_vol=bid_vol,
            ask_vol=ask_vol,
            timestamp=datetime.now(timezone.utc),
        )
        self._vol_surfaces[underlying].points.append(point)

    def get_vol_surface(self, underlying: str) -> VolSurface | None:
        """Get the volatility surface for an underlying."""
        return self._vol_surfaces.get(underlying)

    def interpolate_vol(
        self,
        underlying: str,
        strike: float,
        expiry: datetime,
    ) -> float | None:
        """Interpolate volatility from the surface for a strike/expiry."""
        surface = self._vol_surfaces.get(underlying)
        if not surface or not surface.points:
            return None

        # Find nearest points (simplified: nearest strike at nearest expiry)
        valid_points = [p for p in surface.points if p.expiry >= expiry]
        if not valid_points:
            valid_points = surface.points

        # Simple nearest neighbor interpolation
        nearest = min(valid_points, key=lambda p: abs(p.strike - strike) + abs((p.expiry - expiry).total_seconds()))
        return nearest.implied_vol

    # ── Strategy Builder ─────────────────────────────────────────────────

    def create_strategy(
        self,
        name: str,
        legs: list[tuple[str, StrategyLegRole, int]],
    ) -> OptionStrategy | None:
        """Create a multi-leg strategy.

        legs: [(contract_id, role, quantity), ...]
        """
        strategy_id = f"strat:{uuid.uuid4().hex[:12]}"
        parsed_legs: list[StrategyLeg] = []

        for contract_id, role, qty in legs:
            contract = self._contracts.get(contract_id)
            if not contract:
                return None
            parsed_legs.append(StrategyLeg(
                leg_id=f"leg:{uuid.uuid4().hex[:8]}",
                contract=contract,
                role=role,
                quantity=qty,
            ))

        strategy = OptionStrategy(
            strategy_id=strategy_id,
            name=name,
            legs=tuple(parsed_legs),
        )
        self._strategies[strategy_id] = strategy
        return strategy

    def get_strategy(self, strategy_id: str) -> OptionStrategy | None:
        """Get a strategy by ID."""
        return self._strategies.get(strategy_id)

    def compute_strategy_greeks(
        self,
        strategy_id: str,
        spot_prices: dict[str, float],  # underlying -> spot
        volatility: float,
        risk_free_rate: float = 0.05,
        dividend_yield: float = 0.0,
    ) -> OptionStrategy | None:
        """Compute combined Greeks for a strategy given current spot prices."""
        strategy = self._strategies.get(strategy_id)
        if not strategy:
            return None

        net_delta = 0.0
        net_gamma = 0.0
        net_theta = 0.0
        net_vega = 0.0
        net_rho = 0.0
        as_of = datetime.now(timezone.utc)

        for leg in strategy.legs:
            contract = leg.contract
            spot = spot_prices.get(contract.underlying, 0.0)
            if spot <= 0:
                continue

            T = (contract.expiry - as_of).total_seconds() / (365.0 * 86400.0)
            T = max(T, 1e-6)

            greeks = black_scholes_greeks(
                S=spot, K=contract.strike, T=T,
                r=risk_free_rate, sigma=volatility,
                q=dividend_yield, option_type=contract.option_type,
            )

            sign = 1.0 if leg.role == StrategyLegRole.LONG else -1.0
            multiplier = sign * leg.quantity * contract.multiplier

            net_delta += greeks.delta * multiplier
            net_gamma += greeks.gamma * multiplier
            net_theta += greeks.theta * multiplier
            net_vega += greeks.vega * multiplier
            net_rho += greeks.rho * multiplier

        strategy.net_delta = net_delta
        strategy.net_gamma = net_gamma
        strategy.net_theta = net_theta
        strategy.net_vega = net_vega
        strategy.net_rho = net_rho
        return strategy

    def build_covered_call(
        self,
        underlying: str,
        spot_price: float,
        strike: float,
        expiry: datetime,
        volatility: float,
    ) -> dict[str, Any]:
        """Build a covered call strategy (long 100 shares + short call)."""
        # Find or create underlying contract
        contract = self.register_contract(
            underlying=underlying,
            option_type=OptionType.CALL,
            strike=strike,
            expiry=expiry,
        )

        T = (expiry - datetime.now(timezone.utc)).total_seconds() / (365.0 * 86400.0)
        T = max(T, 1e-6)

        # Short call
        call_greeks = black_scholes_greeks(
            S=spot_price, K=strike, T=T,
            r=self._default_r, sigma=volatility,
            q=0.0, option_type=OptionType.CALL,
        )

        # Long stock equivalent delta = 1 per 100 shares
        stock_delta = 100.0

        net_delta = stock_delta + call_greeks.delta * (-100.0)  # Short call
        net_gamma = call_greeks.gamma * (-100.0)
        net_theta = call_greeks.theta * (-100.0)  # Short premium = positive theta
        net_vega = call_greeks.vega * (-100.0)

        max_profit = call_greeks.premium * 100.0
        max_loss = float('inf')  # Stock can fall indefinitely

        breakeven = spot_price - call_greeks.premium

        return {
            "strategy_id": f"strat:{uuid.uuid4().hex[:12]}",
            "name": f"Covered Call {underlying}",
            "legs": [
                {"type": "stock", "quantity": 100},
                {"type": "short_call", "contract_id": contract.contract_id, "strike": strike},
            ],
            "net_delta": net_delta,
            "net_gamma": net_gamma,
            "net_theta": net_theta,
            "net_vega": net_vega,
            "max_profit": max_profit,
            "max_loss": max_loss,
            "breakeven": breakeven,
        }

    def build_straddle(
        self,
        underlying: str,
        spot_price: float,
        strike: float,
        expiry: datetime,
        volatility: float,
    ) -> dict[str, Any]:
        """Build a long straddle (long call + long put at same strike)."""
        call = self.register_contract(underlying, OptionType.CALL, strike, expiry)
        put = self.register_contract(underlying, OptionType.PUT, strike, expiry)

        T = (expiry - datetime.now(timezone.utc)).total_seconds() / (365.0 * 86400.0)
        T = max(T, 1e-6)

        call_g = black_scholes_greeks(spot_price, strike, T, self._default_r, volatility)
        put_g = black_scholes_greeks(spot_price, strike, T, self._default_r, volatility, option_type=OptionType.PUT)

        net_delta = (call_g.delta - put_g.delta) * 100.0
        net_gamma = (call_g.gamma + put_g.gamma) * 100.0
        net_theta = (call_g.theta + put_g.theta) * 100.0
        net_vega = (call_g.vega + put_g.vega) * 100.0

        max_profit = float('inf')
        max_loss = (call_g.premium + put_g.premium) * 100.0
        breakeven = (strike - call_g.premium - put_g.premium, strike + call_g.premium + put_g.premium)

        return {
            "strategy_id": f"strat:{uuid.uuid4().hex[:12]}",
            "name": f"Long Straddle {underlying}",
            "net_delta": net_delta,
            "net_gamma": net_gamma,
            "net_theta": net_theta,
            "net_vega": net_vega,
            "max_profit": max_profit,
            "max_loss": max_loss,
            "breakeven": breakeven,
        }
