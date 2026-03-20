"""Portfolio allocation strategies (PF-01 ~ PF-06).

Implements:
- Equal-weight allocator
- Risk-parity allocator
- Mean-variance optimizer (Markowitz)
- Black-Litterman allocator
- Dynamic rebalancing with threshold bands
- Multi-strategy budget allocation
"""

from __future__ import annotations

import uuid
import math
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

class AllocatorType(str, Enum):
    EQUAL_WEIGHT = "equal_weight"
    RISK_PARITY = "risk_parity"
    MEAN_VARIANCE = "mean_variance"
    BLACK_LITTERMAN = "black_litterman"
    MIN_VARIANCE = "min_variance"
    MAX_SHARPE = "max_sharpe"
    EQUAL_RISK = "equal_risk"


class RebalanceTrigger(str, Enum):
    THRESHOLD = "threshold"     # Rebalance when drift exceeds threshold
    TIME = "time"               # Rebalance on schedule
    BOTH = "both"               # Rebalance when either triggered


# ─────────────────────────────────────────────────────────────────────────────
# Data Models
# ─────────────────────────────────────────────────────────────────────────────

@dataclass(slots=True)
class AllocationResult:
    """Result of an allocation calculation."""

    allocation_id: str
    allocator_type: AllocatorType
    weights: dict[str, float]  # instrument_id -> weight
    expected_return: float
    expected_volatility: float
    sharpe_ratio: float
    total_notional: float
    constraints_satisfied: bool
    violations: tuple[str, ...] = field(default_factory=tuple)
    created_at: str = field(default_factory=_now)


@dataclass(slots=True)
class AllocatorConfig:
    """Configuration for an allocator."""

    config_id: str
    allocator_type: AllocatorType
    name: str
    description: str = ""
    # Rebalance settings
    rebalance_trigger: RebalanceTrigger = RebalanceTrigger.THRESHOLD
    drift_threshold: float = 0.05  # 5% drift triggers rebalance
    time_frequency: str = "1d"     # daily rebalance
    # Risk settings
    target_volatility: float | None = None
    max_weight: float = 0.3         # max single position weight
    min_weight: float = 0.0         # min single position weight
    # Constraints
    allow_short: bool = False
    max_leverage: float = 1.0
    sector_constraints: dict[str, float] = field(default_factory=dict)  # sector -> max_weight
    # Black-Litterman specific
    equilibrium_returns: dict[str, float] | None = None  # instrument -> return
    market_cap_weights: dict[str, float] | None = None  # instrument -> weight
    views: dict[str, float] | None = None  # instrument -> expected return
    view_confidence: float = 0.5  # 0.0 to 1.0


@dataclass(slots=True)
class PortfolioAllocation:
    """A complete portfolio allocation with target and actual weights."""

    allocation_id: str
    user_id: str
    portfolio_id: str
    allocator_config: AllocatorConfig
    target_weights: dict[str, float]
    current_weights: dict[str, float]
    drift: dict[str, float]  # instrument -> drift amount
    needs_rebalance: bool
    last_rebalance_at: str | None = None
    rebalance_history: tuple[dict[str, Any], ...] = field(default_factory=tuple)
    created_at: str = field(default_factory=_now)


@dataclass(slots=True)
class RiskBudget:
    """Risk budget allocation across strategies or instruments."""

    budget_id: str
    user_id: str
    total_budget: float  # total VaR or volatility budget
    allocations: dict[str, float]  # strategy_id or instrument_id -> risk amount
    budget_type: str = "var"  # var, volatility, drawdown
    created_at: str = field(default_factory=_now)


@dataclass(slots=True)
class RebalancePlan:
    """A plan for rebalancing the portfolio."""

    plan_id: str
    portfolio_id: str
    current_weights: dict[str, float]
    target_weights: dict[str, float]
    trades: tuple[dict[str, Any], ...]  # instrument, direction, quantity, price
    estimated_cost: float
    drift_summary: dict[str, float]  # instrument -> drift
    created_at: str = field(default_factory=_now)


# ─────────────────────────────────────────────────────────────────────────────
# Portfolio Allocator Service
# ─────────────────────────────────────────────────────────────────────────────

class PortfolioAllocatorService:
    """Portfolio allocation service (PF-01 ~ PF-06).

    Provides:
    - Multiple allocation strategies (equal-weight, risk-parity, mean-variance, etc.)
    - Dynamic rebalancing with threshold bands
    - Risk budget allocation
    - Black-Litterman views integration
    - Multi-strategy budget allocation
    """

    def __init__(self, persistence=None) -> None:
        self.persistence = persistence
        self._allocators: dict[str, AllocatorConfig] = {}
        self._allocations: dict[str, PortfolioAllocation] = {}
        self._risk_budgets: dict[str, RiskBudget] = {}

    # ── Allocator Configuration ─────────────────────────────────────────────

    def create_allocator(
        self,
        user_id: str,
        allocator_type: AllocatorType,
        name: str,
        description: str = "",
        **kwargs,
    ) -> AllocatorConfig:
        """Create an allocator configuration."""
        config_id = f"alloc:{uuid.uuid4().hex[:12]}"
        config = AllocatorConfig(
            config_id=config_id,
            allocator_type=allocator_type,
            name=name,
            description=description,
            rebalance_trigger=kwargs.get("rebalance_trigger", RebalanceTrigger.THRESHOLD),
            drift_threshold=kwargs.get("drift_threshold", 0.05),
            time_frequency=kwargs.get("time_frequency", "1d"),
            target_volatility=kwargs.get("target_volatility"),
            max_weight=kwargs.get("max_weight", 0.3),
            min_weight=kwargs.get("min_weight", 0.0),
            allow_short=kwargs.get("allow_short", False),
            max_leverage=kwargs.get("max_leverage", 1.0),
            sector_constraints=kwargs.get("sector_constraints", {}),
            equilibrium_returns=kwargs.get("equilibrium_returns"),
            market_cap_weights=kwargs.get("market_cap_weights"),
            views=kwargs.get("views"),
            view_confidence=kwargs.get("view_confidence", 0.5),
        )
        self._allocators[config_id] = config
        return config

    def get_allocator(self, config_id: str) -> AllocatorConfig | None:
        """Get an allocator config by ID."""
        return self._allocators.get(config_id)

    def get_user_allocators(self, user_id: str) -> list[AllocatorConfig]:
        """Get all allocator configs for a user."""
        return list(self._allocators.values())

    # ── Allocation Calculation ─────────────────────────────────────────────

    def calculate_allocation(
        self,
        allocator_config: AllocatorConfig,
        expected_returns: dict[str, float],
        volatilities: dict[str, float],
        correlations: dict[tuple[str, str], float] | None = None,
        current_weights: dict[str, float] | None = None,
    ) -> AllocationResult:
        """Calculate portfolio allocation based on allocator type."""
        instruments = list(expected_returns.keys())
        n = len(instruments)

        if n == 0:
            return AllocationResult(
                allocation_id=f"ar:{uuid.uuid4().hex[:12]}",
                allocator_type=allocator_config.allocator_type,
                weights={},
                expected_return=0.0,
                expected_volatility=0.0,
                sharpe_ratio=0.0,
                total_notional=0.0,
                constraints_satisfied=True,
            )

        # Initialize weights
        weights = {i: 1.0 / n for i in instruments}
        violations: list[str] = []

        if allocator_config.allocator_type == AllocatorType.EQUAL_WEIGHT:
            weights = self._equal_weight(instruments)

        elif allocator_config.allocator_type == AllocatorType.RISK_PARITY:
            weights = self._risk_parity(instruments, volatilities)

        elif allocator_config.allocator_type == AllocatorType.MEAN_VARIANCE:
            weights = self._mean_variance(
                instruments, expected_returns, volatilities,
                correlations or {}, allocator_config
            )

        elif allocator_config.allocator_type == AllocatorType.MIN_VARIANCE:
            weights = self._min_variance(instruments, volatilities, correlations or {})

        elif allocator_config.allocator_type == AllocatorType.MAX_SHARPE:
            weights = self._max_sharpe(
                instruments, expected_returns, volatilities, correlations or {},
                allocator_config
            )

        elif allocator_config.allocator_type == AllocatorType.BLACK_LITTERMAN:
            weights = self._black_litterman(
                instruments, allocator_config, volatilities
            )

        elif allocator_config.allocator_type == AllocatorType.EQUAL_RISK:
            weights = self._equal_risk(instruments, volatilities)

        # Apply constraints
        weights, violations = self._apply_constraints(weights, allocator_config)

        # Calculate metrics
        port_return = sum(weights[i] * expected_returns.get(i, 0.0) for i in instruments)
        port_vol = self._calculate_portfolio_volatility(weights, volatilities, correlations or {})
        sharpe = port_return / port_vol if port_vol > 0 else 0.0

        return AllocationResult(
            allocation_id=f"ar:{uuid.uuid4().hex[:12]}",
            allocator_type=allocator_config.allocator_type,
            weights=weights,
            expected_return=port_return,
            expected_volatility=port_vol,
            sharpe_ratio=sharpe,
            total_notional=1.0,  # normalized
            constraints_satisfied=len(violations) == 0,
            violations=tuple(violations),
        )

    def _equal_weight(self, instruments: list[str]) -> dict[str, float]:
        """Equal weight allocation."""
        n = len(instruments)
        return {i: 1.0 / n for i in instruments}

    def _risk_parity(self, instruments: list[str], volatilities: dict[str, float]) -> dict[str, float]:
        """Risk parity allocation - each position contributes equally to portfolio risk."""
        inv_vols = {i: 1.0 / volatilities.get(i, 1.0) for i in instruments}
        total = sum(inv_vols.values())
        if total == 0:
            return {i: 1.0 / len(instruments) for i in instruments}
        return {i: inv_vols[i] / total for i in instruments}

    def _equal_risk(self, instruments: list[str], volatilities: dict[str, float]) -> dict[str, float]:
        """Equal risk contribution - each position has same volatility contribution."""
        return self._risk_parity(instruments, volatilities)

    def _min_variance(
        self,
        instruments: list[str],
        volatilities: dict[str, float],
        correlations: dict[tuple[str, str], float],
    ) -> dict[str, float]:
        """Minimum variance portfolio."""
        if len(instruments) == 0:
            return {}

        # Simplified: use inverse variance weighting
        inv_vars = {i: 1.0 / (volatilities.get(i, 1.0) ** 2) for i in instruments}
        total = sum(inv_vars.values())
        if total == 0:
            return {i: 1.0 / len(instruments) for i in instruments}

        weights = {i: inv_vars[i] / total for i in instruments}

        # Normalize for short constraints
        min_w = min(weights.values())
        if min_w < 0:
            weights = {i: w - min_w for i, w in weights.items()}
            total = sum(weights.values())
            weights = {i: w / total for i, w in weights.items()}

        return weights

    def _mean_variance(
        self,
        instruments: list[str],
        expected_returns: dict[str, float],
        volatilities: dict[str, float],
        correlations: dict[tuple[str, str], float],
        config: AllocatorConfig,
    ) -> dict[str, float]:
        """Mean-variance optimization (simplified Markowitz)."""
        # Use Sharpe ratio optimization with constraints
        return self._max_sharpe(instruments, expected_returns, volatilities, correlations, config)

    def _max_sharpe(
        self,
        instruments: list[str],
        expected_returns: dict[str, float],
        volatilities: dict[str, float],
        correlations: dict[tuple[str, str], float],
        config: AllocatorConfig,
    ) -> dict[str, float]:
        """Maximum Sharpe ratio portfolio."""
        if not expected_returns:
            return self._equal_weight(instruments)

        # Simplified: weight by Sharpe ratio
        sharpes = {
            i: (expected_returns.get(i, 0.0) / volatilities.get(i, 1.0))
            if volatilities.get(i, 0) > 0 else 0
            for i in instruments
        }

        # Only positive Sharpe weights
        pos_sharpes = {i: max(s, 0) for i, s in sharpes.items()}
        total = sum(pos_sharpes.values())

        if total == 0:
            return self._equal_weight(instruments)

        weights = {i: pos_sharpes[i] / total for i in instruments}
        return weights

    def _black_litterman(
        self,
        instruments: list[str],
        config: AllocatorConfig,
        volatilities: dict[str, float],
    ) -> dict[str, float]:
        """Black-Litterman allocation from equilibrium returns and views."""
        equilibrium = config.equilibrium_returns or {}
        views = config.views or {}
        view_conf = config.view_confidence
        mc_weights = config.market_cap_weights or {}

        # Start from equilibrium returns (market implied)
        if not equilibrium:
            # Use equal weight as equilibrium
            equilibrium = {i: 1.0 / len(instruments) for i in instruments}

        # Blend views with equilibrium
        adjusted_returns = {}
        for i in instruments:
            eq_return = equilibrium.get(i, 0.0)
            if i in views:
                # Blend view with equilibrium based on confidence
                adjusted_returns[i] = view_conf * views[i] + (1 - view_conf) * eq_return
            else:
                adjusted_returns[i] = eq_return

        # Use adjusted returns for mean-variance
        vols = {i: volatilities.get(i, 0.2) for i in instruments}
        return self._max_sharpe(instruments, adjusted_returns, vols, {}, config)

    def _calculate_portfolio_volatility(
        self,
        weights: dict[str, float],
        volatilities: dict[str, float],
        correlations: dict[tuple[str, str], float],
    ) -> float:
        """Calculate portfolio volatility given weights and correlations."""
        if not weights:
            return 0.0

        instruments = list(weights.keys())
        variance = 0.0

        for i in instruments:
            for j in instruments:
                w_i = weights.get(i, 0)
                w_j = weights.get(j, 0)
                vol_i = volatilities.get(i, 0)
                vol_j = volatilities.get(j, 0)

                if i == j:
                    variance += w_i * w_j * vol_i * vol_j
                else:
                    corr = correlations.get((i, j), correlations.get((j, i), 0.0))
                    variance += w_i * w_j * vol_i * vol_j * corr

        return math.sqrt(max(variance, 0))

    def _apply_constraints(
        self,
        weights: dict[str, float],
        config: AllocatorConfig,
    ) -> tuple[dict[str, float], list[str]]:
        """Apply weight constraints and return any violations."""
        violations: list[str] = []
        result = weights.copy()

        # Apply max/min weight constraints
        for i, w in result.items():
            if w > config.max_weight:
                violations.append(f"{i} weight {w:.2%} exceeds max {config.max_weight:.2%}")
                result[i] = config.max_weight
            if w < config.min_weight and w > 0:
                violations.append(f"{i} weight {w:.2%} below min {config.min_weight:.2%}")
                result[i] = config.min_weight

        # Normalize to sum to 1
        total = sum(result.values())
        if total > 0:
            result = {i: w / total for i, w in result.items()}

        # Leverage constraint
        gross_exposure = sum(abs(w) for w in result.values())
        if gross_exposure > config.max_leverage:
            violations.append(f"Gross exposure {gross_exposure:.2%} exceeds max leverage {config.max_leverage:.2%}")
            scale = config.max_leverage / gross_exposure
            result = {i: w * scale for i, w in result.items()}

        # Short selling constraint
        if not config.allow_short:
            result = {i: max(w, 0) for i, w in result.items()}
            total = sum(result.values())
            if total > 0:
                result = {i: w / total for i, w in result.items()}

        return result, violations

    # ── Rebalance Management ───────────────────────────────────────────────

    def check_rebalance_needed(
        self,
        allocation: PortfolioAllocation,
        current_weights: dict[str, float],
    ) -> bool:
        """Check if portfolio needs rebalancing based on drift threshold."""
        if allocation.allocator_config.rebalance_trigger in (RebalanceTrigger.THRESHOLD, RebalanceTrigger.BOTH):
            for instr, target in allocation.target_weights.items():
                current = current_weights.get(instr, 0)
                drift = abs(current - target)
                if drift > allocation.allocator_config.drift_threshold:
                    return True

        # Time-based trigger would check scheduler here
        return False

    def calculate_rebalance_plan(
        self,
        portfolio_id: str,
        target_weights: dict[str, float],
        current_weights: dict[str, float],
        current_prices: dict[str, float],
        notional: float = 100000.0,
    ) -> RebalancePlan:
        """Calculate the trades needed to rebalance."""
        trades = []

        for instr, target in target_weights.items():
            current = current_weights.get(instr, 0)
            drift = target - current

            if abs(drift) < 0.001:  # Ignore tiny drifts
                continue

            target_notional = target * notional
            current_notional = current * notional
            diff_notional = target_notional - current_notional

            price = current_prices.get(instr, 1.0)
            if price > 0:
                quantity = diff_notional / price
                direction = "buy" if quantity > 0 else "sell"

                trades.append({
                    "instrument_id": instr,
                    "direction": direction,
                    "quantity": abs(quantity),
                    "price": price,
                    "notional": abs(diff_notional),
                    "target_weight": target,
                    "current_weight": current,
                    "drift": drift,
                })

        # Sort by drift size (largest first)
        trades.sort(key=lambda t: abs(t["drift"]), reverse=True)

        estimated_cost = sum(abs(t["notional"]) * 0.001 for t in trades)  # 0.1% cost estimate

        return RebalancePlan(
            plan_id=f"rebal:{uuid.uuid4().hex[:12]}",
            portfolio_id=portfolio_id,
            current_weights=current_weights,
            target_weights=target_weights,
            trades=tuple(trades),
            estimated_cost=estimated_cost,
            drift_summary={t["instrument_id"]: t["drift"] for t in trades},
        )

    # ── Risk Budget ──────────────────────────────────────────────────────

    def allocate_risk_budget(
        self,
        user_id: str,
        total_budget: float,
        instruments: list[str],
        method: str = "equal",
    ) -> RiskBudget:
        """Allocate risk budget across instruments."""
        budget_id = f"rb:{uuid.uuid4().hex[:12]}"
        n = len(instruments)

        if method == "equal":
            allocations = {i: total_budget / n for i in instruments}
        elif method == "inverse_vol":
            # Allocate more to lower volatility
            # Simplified - would need actual vol data
            allocations = {i: total_budget / n for i in instruments}
        else:
            allocations = {i: total_budget / n for i in instruments}

        budget = RiskBudget(
            budget_id=budget_id,
            user_id=user_id,
            total_budget=total_budget,
            allocations=allocations,
        )
        self._risk_budgets[budget_id] = budget
        return budget

    def get_risk_budget(self, budget_id: str) -> RiskBudget | None:
        """Get a risk budget by ID."""
        return self._risk_budgets.get(budget_id)

    def check_risk_budget_usage(
        self,
        budget: RiskBudget,
        current_risks: dict[str, float],
    ) -> tuple[bool, list[str]]:
        """Check if current risk usage exceeds budget."""
        warnings = []
        exceeded = False

        for instr, allocated in budget.allocations.items():
            used = current_risks.get(instr, 0)
            if used > allocated:
                exceeded = True
                warnings.append(f"{instr} risk used {used:.2f} exceeds budget {allocated:.2f}")
            elif used > allocated * 0.9:
                warnings.append(f"{instr} risk approaching budget limit: {used:.2f}/{allocated:.2f}")

        return exceeded, warnings

    # ── Portfolio Allocation Tracking ────────────────────────────────────

    def create_allocation(
        self,
        user_id: str,
        portfolio_id: str,
        allocator_config: AllocatorConfig,
        target_weights: dict[str, float],
    ) -> PortfolioAllocation:
        """Create and track a portfolio allocation."""
        allocation_id = f"pa:{uuid.uuid4().hex[:12]}"
        allocation = PortfolioAllocation(
            allocation_id=allocation_id,
            user_id=user_id,
            portfolio_id=portfolio_id,
            allocator_config=allocator_config,
            target_weights=target_weights,
            current_weights={},
            drift={},
            needs_rebalance=False,
        )
        self._allocations[allocation_id] = allocation
        return allocation

    def update_allocation_weights(
        self,
        allocation_id: str,
        current_weights: dict[str, float],
    ) -> PortfolioAllocation | None:
        """Update current weights and recalculate drift."""
        allocation = self._allocations.get(allocation_id)
        if not allocation:
            return None

        allocation.current_weights = current_weights
        allocation.drift = {
            i: current_weights.get(i, 0) - allocation.target_weights.get(i, 0)
            for i in set(list(current_weights.keys()) + list(allocation.target_weights.keys()))
        }
        allocation.needs_rebalance = self.check_rebalance_needed(allocation, current_weights)
        return allocation

    def get_allocation(self, allocation_id: str) -> PortfolioAllocation | None:
        """Get an allocation by ID."""
        return self._allocations.get(allocation_id)

    def get_user_allocations(self, user_id: str) -> list[PortfolioAllocation]:
        """Get all allocations for a user."""
        return [a for a in self._allocations.values() if a.user_id == user_id]


# ─────────────────────────────────────────────────────────────────────────────
# PF-04: Inter-Strategy Risk Exposure Aggregation
# ─────────────────────────────────────────────────────────────────────────────

@dataclass(slots=True)
class StrategyExposure:
    """Risk exposure for a single strategy."""

    strategy_id: str
    gross_exposure: float
    net_exposure: float
    directional_exposure: float  # positive = long bias, negative = short bias
    sector_exposures: dict[str, float]  # sector -> exposure
    instrument_exposures: dict[str, float]  # instrument_id -> exposure
    leverage: float
    correlation_to_benchmark: float


@dataclass(slots=True)
class AggregatedExposure:
    """Aggregated risk exposure across multiple strategies."""

    exposure_id: str
    total_gross_exposure: float
    total_net_exposure: float
    net_directional_exposure: float
    strategy_exposures: dict[str, StrategyExposure]
    aggregate_sector_exposures: dict[str, float]
    aggregate_instrument_exposures: dict[str, float]
    concentration_risk: dict[str, float]  # instrument/sector -> concentration %
    leverage_ratio: float
    correlation_matrix: dict[str, dict[str, float]]
    created_at: str = field(default_factory=_now)


class RiskExposureAggregator:
    """Aggregates risk exposures across multiple strategies (PF-04).

    Provides:
    - Cross-strategy exposure aggregation
    - Sector and instrument concentration analysis
    - Correlation-based risk contribution
    - Net vs gross exposure analysis
    """

    def __init__(self, benchmark_returns: list[float] | None = None) -> None:
        self.benchmark_returns = benchmark_returns or []
        self._strategy_positions: dict[str, dict[str, float]] = {}  # strategy_id -> {instrument_id -> quantity}
        self._strategy_cash: dict[str, float] = {}
        self._strategy_returns: dict[str, list[float]] = defaultdict(list)
        self._sector_map: dict[str, str] = {}  # instrument_id -> sector

    def register_instrument_sector(self, instrument_id: str, sector: str) -> None:
        """Register the sector for an instrument."""
        self._sector_map[instrument_id] = sector

    def record_strategy_position(
        self,
        strategy_id: str,
        positions: dict[str, float],
        cash: float = 0.0,
    ) -> None:
        """Record positions for a strategy (instrument_id -> quantity)."""
        self._strategy_positions[strategy_id] = positions.copy()
        self._strategy_cash[strategy_id] = cash

    def record_strategy_return(self, strategy_id: str, return_pct: float) -> None:
        """Record a return for correlation calculation."""
        self._strategy_returns[strategy_id].append(return_pct)
        if len(self._strategy_returns[strategy_id]) > 100:
            self._strategy_returns[strategy_id] = self._strategy_returns[strategy_id][-100:]

    def aggregate_exposures(
        self,
        prices: dict[str, float],
        instrument_sectors: dict[str, str] | None = None,
    ) -> AggregatedExposure:
        """Aggregate exposures across all registered strategies."""
        if instrument_sectors:
            for iid, sector in instrument_sectors.items():
                self._sector_map[iid] = sector

        strategy_exposures: dict[str, StrategyExposure] = {}
        aggregate_sector: dict[str, float] = defaultdict(float)
        aggregate_instrument: dict[str, float] = defaultdict(float)
        total_gross = 0.0
        total_net = 0.0

        for strategy_id, positions in self._strategy_positions.items():
            gross_exp = 0.0
            net_exp = 0.0
            long_exp = 0.0
            short_exp = 0.0
            sector_exp: dict[str, float] = defaultdict(float)
            instr_exp: dict[str, float] = {}

            equity = self._strategy_cash.get(strategy_id, 0.0) + sum(
                qty * prices.get(iid, 0.0) for iid, qty in positions.items()
            )

            for iid, qty in positions.items():
                price = prices.get(iid, 0.0)
                notional = qty * price
                sector = self._sector_map.get(iid, "UNKNOWN")

                gross_exp += abs(notional)
                net_exp += notional
                if qty > 0:
                    long_exp += notional
                else:
                    short_exp += notional

                sector_exp[sector] += notional
                instr_exp[iid] = notional
                aggregate_instrument[iid] += notional

            total_gross += gross_exp
            total_net += net_exp

            directional = (long_exp - abs(short_exp)) / equity if equity > 0 else 0.0
            leverage = gross_exp / abs(equity) if equity > 0 else 0.0

            corr_benchmark = 0.0
            if self.benchmark_returns and self._strategy_returns.get(strategy_id):
                strat_rets = self._strategy_returns[strategy_id]
                min_len = min(len(strat_rets), len(self.benchmark_returns))
                if min_len >= 2:
                    mean_strat = sum(strat_rets[-min_len:]) / min_len
                    mean_bench = sum(self.benchmark_returns[-min_len:]) / min_len
                    cov = sum(
                        (strat_rets[-min_len + k] - mean_strat) * (self.benchmark_returns[-min_len + k] - mean_bench)
                        for k in range(min_len)
                    ) / min_len
                    std_strat = math.sqrt(sum((r - mean_strat) ** 2 for r in strat_rets[-min_len:]) / min_len)
                    std_bench = math.sqrt(sum((r - mean_bench) ** 2 for r in self.benchmark_returns[-min_len:]) / min_len)
                    corr_benchmark = cov / (std_strat * std_bench) if std_strat * std_bench > 0 else 0.0

            strategy_exposures[strategy_id] = StrategyExposure(
                strategy_id=strategy_id,
                gross_exposure=gross_exp,
                net_exposure=net_exp,
                directional_exposure=directional,
                sector_exposures=dict(sector_exp),
                instrument_exposures=instr_exp,
                leverage=leverage,
                correlation_to_benchmark=corr_benchmark,
            )

            for sector, exp in sector_exp.items():
                aggregate_sector[sector] += exp

        # Concentration risk
        concentration: dict[str, float] = {}
        if total_gross > 0:
            for iid, exp in aggregate_instrument.items():
                concentration[iid] = abs(exp) / total_gross

        # Correlation matrix
        corr_matrix = self._compute_correlation_matrix()

        return AggregatedExposure(
            exposure_id=f"ae:{uuid.uuid4().hex[:12]}",
            total_gross_exposure=total_gross,
            total_net_exposure=total_net,
            net_directional_exposure=(total_net / total_gross) if total_gross > 0 else 0.0,
            strategy_exposures=strategy_exposures,
            aggregate_sector_exposures=dict(aggregate_sector),
            aggregate_instrument_exposures=dict(aggregate_instrument),
            concentration_risk=concentration,
            leverage_ratio=total_gross / abs(sum(self._strategy_cash.values())) if self._strategy_cash else 0.0,
            correlation_matrix=corr_matrix,
        )

    def _compute_correlation_matrix(self) -> dict[str, dict[str, float]]:
        """Compute pairwise return correlations across strategies."""
        strategy_ids = list(self._strategy_returns.keys())
        n = len(strategy_ids)
        if n == 0:
            return {}

        result: dict[str, dict[str, float]] = {}
        for i, sid_i in enumerate(strategy_ids):
            result[sid_i] = {}
            for j, sid_j in enumerate(strategy_ids):
                if i == j:
                    result[sid_i][sid_j] = 1.0
                    continue
                rets_i = self._strategy_returns.get(sid_i, [])
                rets_j = self._strategy_returns.get(sid_j, [])
                min_len = min(len(rets_i), len(rets_j))
                if min_len < 2:
                    result[sid_i][sid_j] = 0.0
                else:
                    mean_i = sum(rets_i[-min_len:]) / min_len
                    mean_j = sum(rets_j[-min_len:]) / min_len
                    cov = sum(
                        (rets_i[-min_len + k] - mean_i) * (rets_j[-min_len + k] - mean_j)
                        for k in range(min_len)
                    ) / min_len
                    std_i = math.sqrt(sum((r - mean_i) ** 2 for r in rets_i[-min_len:]) / min_len)
                    std_j = math.sqrt(sum((r - mean_j) ** 2 for r in rets_j[-min_len:]) / min_len)
                    result[sid_i][sid_j] = cov / (std_i * std_j) if std_i * std_j > 0 else 0.0
        return result

    def get_strategy_risk_contribution(
        self,
        aggregated: AggregatedExposure,
    ) -> dict[str, float]:
        """Calculate each strategy's contribution to total portfolio risk."""
        if not aggregated.strategy_exposures:
            return {}

        total_risk = sum(
            abs(se.gross_exposure) * se.correlation_to_benchmark
            for se in aggregated.strategy_exposures.values()
        )

        if total_risk == 0:
            return {sid: 1.0 / len(aggregated.strategy_exposures) for sid in aggregated.strategy_exposures}

        contributions: dict[str, float] = {}
        for sid, se in aggregated.strategy_exposures.items():
            risk_contrib = abs(se.gross_exposure) * se.correlation_to_benchmark
            contributions[sid] = risk_contrib / total_risk

        return contributions


# ─────────────────────────────────────────────────────────────────────────────
# PF-05: Return Attribution Analysis
# ─────────────────────────────────────────────────────────────────────────────

@dataclass(slots=True)
class AttributionResult:
    """Result of return attribution analysis."""

    attribution_id: str
    total_return: float
    return_contributions: dict[str, float]  # instrument/strategy -> contribution
    sector_contributions: dict[str, float]  # sector -> contribution
    allocation_effect: float  # return from weight changes
    selection_effect: float  # return from asset selection
    interaction_effect: float  # joint effect
    active_return: float  # portfolio return - benchmark return
    created_at: str = field(default_factory=_now)


class AttributionAnalyzer:
    """Performs return attribution analysis (PF-05).

    Provides:
    - Brinson attribution (allocation, selection, interaction effects)
    - Sector attribution
    - Factor attribution
    - Contribution analysis
    """

    def __init__(self) -> None:
        self._return_history: dict[str, list[float]] = defaultdict(list)
        self._weight_history: dict[str, list[float]] = defaultdict(list)
        self._benchmark_returns: list[float] = []

    def record_period_return(self, instrument_id: str, return_pct: float) -> None:
        """Record return for an instrument."""
        self._return_history[instrument_id].append(return_pct)
        if len(self._return_history[instrument_id]) > 252:
            self._return_history[instrument_id] = self._return_history[instrument_id][-252:]

    def record_period_weights(self, weights: dict[str, float]) -> None:
        """Record portfolio weights at start of period."""
        for iid, w in weights.items():
            self._weight_history[iid].append(w)
            if len(self._weight_history[iid]) > 252:
                self._weight_history[iid] = self._weight_history[iid][-252:]

    def record_benchmark_return(self, return_pct: float) -> None:
        """Record benchmark return for the period."""
        self._benchmark_returns.append(return_pct)
        if len(self._benchmark_returns) > 252:
            self._benchmark_returns = self._benchmark_returns[-252:]

    def brinson_attribution(
        self,
        portfolio_weights: dict[str, float],
        benchmark_weights: dict[str, float],
        portfolio_returns: dict[str, float],
        benchmark_returns: dict[str, float],
    ) -> AttributionResult:
        """Perform Brinson attribution analysis.

        Breaks active return into:
        - Allocation effect: return from overweighting/underweighting sectors
        - Selection effect: return from picking assets within sectors
        - Interaction effect: joint effect of allocation and selection
        """
        all_instruments = set(portfolio_weights.keys()) | set(benchmark_weights.keys())

        allocation_effect = 0.0
        selection_effect = 0.0
        interaction_effect = 0.0
        return_contributions: dict[str, float] = {}
        sector_contributions: dict[str, float] = defaultdict(float)

        for iid in all_instruments:
            p_w = portfolio_weights.get(iid, 0.0)
            b_w = benchmark_weights.get(iid, 0.0)
            p_r = portfolio_returns.get(iid, 0.0)
            b_r = benchmark_returns.get(iid, 0.0)

            # Allocation effect: (Pw - Bw) * Br
            alloc = (p_w - b_w) * b_r
            allocation_effect += alloc

            # Selection effect: Bw * (PwR - Br)
            sel = b_w * (p_r - b_r)
            selection_effect += sel

            # Interaction effect: (Pw - Bw) * (PwR - Br)
            interact = (p_w - b_w) * (p_r - b_r)
            interaction_effect += interact

            return_contributions[iid] = p_w * p_r

        total_return = sum(return_contributions.values())
        active_return = total_return - sum(benchmark_returns.get(iid, 0.0) * b_w for iid, b_w in benchmark_weights.items())

        return AttributionResult(
            attribution_id=f"attr:{uuid.uuid4().hex[:12]}",
            total_return=total_return,
            return_contributions=return_contributions,
            sector_contributions=dict(sector_contributions),
            allocation_effect=allocation_effect,
            selection_effect=selection_effect,
            interaction_effect=interaction_effect,
            active_return=active_return,
        )

    def factor_attribution(
        self,
        instrument_factors: dict[str, dict[str, float]],
        instrument_returns: dict[str, float],
        factor_returns: dict[str, float],
    ) -> dict[str, float]:
        """Perform factor-based attribution.

        Returns the contribution of each factor to total return.
        """
        factor_contributions: dict[str, float] = defaultdict(float)

        for iid, factors in instrument_factors.items():
            ret = instrument_returns.get(iid, 0.0)
            for factor, beta in factors.items():
                factor_ret = factor_returns.get(factor, 0.0)
                factor_contributions[factor] += beta * factor_ret

        return dict(factor_contributions)

    def calculate_nav_attribution(
        self,
        positions: dict[str, float],
        prices_start: dict[str, float],
        prices_end: dict[str, float],
        benchmark_return: float,
    ) -> AttributionResult:
        """Calculate NAV-based attribution (PF-05).

        Returns contribution of each position to total PnL.
        """
        return_contributions: dict[str, float] = {}
        total_return = 0.0

        for iid, qty in positions.items():
            start_price = prices_start.get(iid, 0.0)
            end_price = prices_end.get(iid, start_price)

            if start_price > 0:
                position_return = (end_price - start_price) / start_price
                position_pnl = qty * (end_price - start_price)
                return_contributions[iid] = position_pnl
                total_return += position_return

        # Calculate active return vs benchmark
        active_return = total_return - benchmark_return

        return AttributionResult(
            attribution_id=f"nav:{uuid.uuid4().hex[:12]}",
            total_return=total_return,
            return_contributions=return_contributions,
            sector_contributions={},
            allocation_effect=0.0,
            selection_effect=active_return,
            interaction_effect=0.0,
            active_return=active_return,
        )

    def get_top_contributors(
        self,
        contributions: dict[str, float],
        n: int = 5,
    ) -> tuple[list[tuple[str, float]], list[tuple[str, float]]]:
        """Get top N contributors and detractors."""
        sorted_items = sorted(contributions.items(), key=lambda x: x[1], reverse=True)
        return sorted_items[:n], sorted_items[-n:]

    # ── PF-05: Volatility Attribution ───────────────────────────────────────

    def volatility_attribution(
        self,
        positions: dict[str, dict[str, float]],
        volatilities: dict[str, float],
        correlations: dict[tuple[str, str], float],
        weights: dict[str, float],
    ) -> dict[str, Any]:
        """Decompose portfolio volatility into contributing factors (PF-05).

        Uses the classic formula:
          portfolio_vol = sqrt(sum_ij(w_i * w_j * vol_i * vol_j * corr_ij))

        Returns per-position contribution to total portfolio volatility.
        """
        tickers = list(weights.keys())
        n = len(tickers)
        if n == 0:
            return {"total_volatility": 0.0, "contributions": {}, "marginal_contributions": {}}

        # Build correlation matrix
        corr_mat: dict[str, dict[str, float]] = {i: {} for i in tickers}
        for i in tickers:
            for j in tickers:
                if i == j:
                    corr_mat[i][j] = 1.0
                else:
                    key = (min(i, j), max(i, j))
                    corr_mat[i][j] = correlations.get(key, 0.0)

        vols = {i: volatilities.get(i, 0.0) for i in tickers}
        ws = {i: weights.get(i, 0.0) for i in tickers}

        # Total portfolio variance
        total_var = 0.0
        for i in tickers:
            for j in tickers:
                total_var += ws[i] * ws[j] * vols[i] * vols[j] * corr_mat[i][j]
        total_vol = max(total_var, 0.0) ** 0.5

        # Marginal contribution of each position to portfolio variance
        # d(portfolio_var)/d(w_i) = 2 * sum_j(w_j * vol_i * vol_j * corr_ij)
        marginal_contribs: dict[str, float] = {}
        contribs: dict[str, float] = {}
        for i in tickers:
            mc = 2.0 * sum(ws[j] * vols[i] * vols[j] * corr_mat[i][j] for j in tickers)
            marginal_contribs[i] = mc
            # Actual contribution = w_i * marginal_contrib_i (Boud第二种分解)
            contribs[i] = ws[i] * mc

        # Normalize contributions
        total_contrib = sum(contribs.values())
        if total_contrib != 0:
            normalized = {i: c / total_contrib for i, c in contribs.items()}
        else:
            normalized = {i: 0.0 for i in contribs}

        return {
            "total_volatility": round(total_vol, 6),
            "contributions": {i: round(contribs[i], 6) for i in tickers},
            "normalized_contributions": {i: round(normalized[i], 4) for i in tickers},
            "marginal_contributions": {i: round(marginal_contribs[i], 6) for i in tickers},
        }

    # ── PF-05: Drawdown Attribution ─────────────────────────────────────────

    def drawdown_attribution(
        self,
        equity_curve: list[float],
        dates: list[str],
        positions: dict[str, list[float]],
        peak: float | None = None,
    ) -> dict[str, Any]:
        """Attribute portfolio drawdown periods to contributing positions (PF-05).

        Identifies drawdown periods (high watermark to trough), then attributes
        each period's loss to positions based on their weight and return during
        the drawdown window.
        """
        if not equity_curve or len(equity_curve) < 2:
            return {"max_drawdown": 0.0, "current_drawdown": 0.0, "attribution": {}}

        # Compute drawdown series
        highs = [equity_curve[0]]
        for p in equity_curve[1:]:
            highs.append(max(highs[-1], p))

        drawdowns = [(highs[i] - equity_curve[i]) / highs[i] if highs[i] > 0 else 0.0 for i in range(len(equity_curve))]
        max_dd = max(drawdowns) if drawdowns else 0.0
        current_dd = drawdowns[-1] if drawdowns else 0.0

        # Find the peak and trough of max drawdown
        max_dd_idx = drawdowns.index(max_dd) if drawdowns else -1
        peak_idx = highs.index(highs[max_dd_idx]) if max_dd_idx >= 0 else 0

        # Attribution: compute contribution of each position during the drawdown window
        attribution: dict[str, dict[str, float]] = {}
        window_len = max(1, max_dd_idx - peak_idx)

        for iid, pos_curve in positions.items():
            if len(pos_curve) < len(equity_curve):
                # Pad with last value if shorter
                pos_curve = pos_curve + [pos_curve[-1]] * (len(equity_curve) - len(pos_curve))
            if len(pos_curve) == 0:
                continue

            window_pos_returns = []
            for t in range(peak_idx, max_dd_idx + 1):
                if pos_curve[t] != 0:
                    ret = (equity_curve[t] - pos_curve[t]) / pos_curve[t]
                else:
                    ret = 0.0
                window_pos_returns.append(ret)

            avg_ret = sum(window_pos_returns) / len(window_pos_returns) if window_pos_returns else 0.0
            weight = 1.0 / max(len(positions), 1)  # Equal weight attribution
            attribution[iid] = {
                "avg_return_during_drawdown": round(avg_ret, 6),
                "window_length": window_len,
                "drawdown_contribution": round(avg_ret * weight, 6),
            }

        return {
            "max_drawdown": round(max_dd, 6),
            "current_drawdown": round(current_dd, 6),
            "peak_date": dates[peak_idx] if dates and peak_idx < len(dates) else None,
            "trough_date": dates[max_dd_idx] if dates and max_dd_idx < len(dates) else None,
            "attribution": attribution,
        }

    # ── PF-05: Sector-Level Brinson Attribution ───────────────────────────────

    def sector_brinson_attribution(
        self,
        portfolio_sector_weights: dict[str, float],
        benchmark_sector_weights: dict[str, float],
        portfolio_sector_returns: dict[str, float],
        benchmark_sector_returns: dict[str, float],
        sector_instruments: dict[str, list[str]],
        portfolio_weights: dict[str, float],
        benchmark_weights: dict[str, float],
        portfolio_returns: dict[str, float],
        benchmark_returns: dict[str, float],
    ) -> dict[str, Any]:
        """Perform sector-level Brinson attribution (PF-05).

        True Brinson attribution operates at the sector level:
        - Sector allocation effect: sum over sectors (Pw_s - Bw_s) * Br_s
        - Sector selection effect: sum over sectors Bw_s * (PwR_s - Br_s)
        - Interaction: sum over sectors (Pw_s - Bw_s) * (PwR_s - Br_s)
        Then within each sector, also compute instrument-level allocation/selection.
        """
        sectors = set(portfolio_sector_weights.keys()) | set(benchmark_sector_weights.keys())

        sector_alloc_effect = 0.0
        sector_sel_effect = 0.0
        sector_interact_effect = 0.0
        sector_results: dict[str, dict[str, float]] = {}

        for sector in sectors:
            p_sw = portfolio_sector_weights.get(sector, 0.0)
            b_sw = benchmark_sector_weights.get(sector, 0.0)
            p_sr = portfolio_sector_returns.get(sector, 0.0)
            b_sr = benchmark_sector_returns.get(sector, 0.0)

            # Allocation: (Pw_s - Bw_s) * Br_s
            alloc = (p_sw - b_sw) * b_sr
            # Selection: Bw_s * (PwR_s - Br_s)
            sel = b_sw * (p_sr - b_sr)
            # Interaction: (Pw_s - Bw_s) * (PwR_s - Br_s)
            interact = (p_sw - b_sw) * (p_sr - b_sr)

            sector_alloc_effect += alloc
            sector_sel_effect += sel
            sector_interact_effect += interact

            # Instrument-level breakdown within this sector
            instruments = sector_instruments.get(sector, [])
            instr_breakdown: dict[str, dict[str, float]] = {}
            for iid in instruments:
                p_w = portfolio_weights.get(iid, 0.0)
                b_w = benchmark_weights.get(iid, 0.0)
                p_r = portfolio_returns.get(iid, 0.0)
                i_alloc = (p_w - b_w) * b_sr
                i_sel = b_w * (p_r - b_sr)
                i_interact = (p_w - b_w) * (p_r - b_sr)
                instr_breakdown[iid] = {
                    "allocation": round(i_alloc, 6),
                    "selection": round(i_sel, 6),
                    "interaction": round(i_interact, 6),
                    "weight_pct": round(p_w * 100, 2),
                }

            sector_results[sector] = {
                "allocation_effect": round(alloc, 6),
                "selection_effect": round(sel, 6),
                "interaction_effect": round(interact, 6),
                "portfolio_weight": round(p_sw, 4),
                "benchmark_weight": round(b_sw, 4),
                "portfolio_return": round(p_sr, 4),
                "benchmark_return": round(b_sr, 4),
                "instruments": instr_breakdown,
            }

        return {
            "total_allocation_effect": round(sector_alloc_effect, 6),
            "total_selection_effect": round(sector_sel_effect, 6),
            "total_interaction_effect": round(sector_interact_effect, 6),
            "total_active_return": round(sector_alloc_effect + sector_sel_effect + sector_interact_effect, 6),
            "sectors": sector_results,
        }


# ─────────────────────────────────────────────────────────────────────────────
# PF-06: Multi-Account Capital Allocation Interface
# ─────────────────────────────────────────────────────────────────────────────

@dataclass(slots=True)
class Account:
    """Represents a trading account."""

    account_id: str
    user_id: str
    account_type: str  # primary, sub, mirror
    parent_account_id: str | None
    cash_balance: float
    equity: float
    margin_available: float
    positions_value: float
    created_at: str = field(default_factory=_now)


@dataclass(slots=True)
class TransferRequest:
    """Request to transfer funds between accounts."""

    transfer_id: str
    from_account_id: str
    to_account_id: str
    amount: float
    transfer_type: str  # manual, automatic, rebalance
    status: str  # pending, completed, failed
    created_at: str = field(default_factory=_now)
    completed_at: str | None = None


@dataclass(slots=True)
class CapitalAllocationPlan:
    """Multi-account capital allocation plan."""

    plan_id: str
    user_id: str
    total_capital: float
    account_allocations: dict[str, float]  # account_id -> amount
    strategy_allocations: dict[str, dict[str, float]]  # account_id -> {strategy_id -> amount}
    rebalance_threshold: float
    created_at: str = field(default_factory=_now)


class MultiAccountAllocator:
    """Multi-account unified capital allocation interface (PF-06).

    Provides:
    - Account hierarchy management
    - Cross-account fund transfers
    - Unified capital allocation
    - Automatic rebalancing across accounts
    """

    def __init__(self, persistence=None) -> None:
        self.persistence = persistence
        self._accounts: dict[str, Account] = {}
        self._transfer_history: list[TransferRequest] = []
        self._allocation_plans: dict[str, CapitalAllocationPlan] = {}
        self._account_positions: dict[str, dict[str, float]] = defaultdict(dict)

    def create_account(
        self,
        user_id: str,
        account_type: str = "primary",
        parent_account_id: str | None = None,
        initial_cash: float = 0.0,
    ) -> Account:
        """Create a new account."""
        account_id = f"acc:{uuid.uuid4().hex[:12]}"
        account = Account(
            account_id=account_id,
            user_id=user_id,
            account_type=account_type,
            parent_account_id=parent_account_id,
            cash_balance=initial_cash,
            equity=initial_cash,
            margin_available=initial_cash,
            positions_value=0.0,
        )
        self._accounts[account_id] = account
        return account

    def get_account(self, account_id: str) -> Account | None:
        """Get account by ID."""
        return self._accounts.get(account_id)

    def get_user_accounts(self, user_id: str) -> list[Account]:
        """Get all accounts for a user."""
        return [a for a in self._accounts.values() if a.user_id == user_id]

    def get_child_accounts(self, parent_account_id: str) -> list[Account]:
        """Get all child accounts of a parent account."""
        return [
            a for a in self._accounts.values()
            if a.parent_account_id == parent_account_id
        ]

    def update_account_balance(
        self,
        account_id: str,
        cash_delta: float,
        positions_value: float,
    ) -> Account | None:
        """Update account cash and positions value."""
        account = self._accounts.get(account_id)
        if not account:
            return None

        account.cash_balance += cash_delta
        account.equity = account.cash_balance + positions_value
        account.positions_value = positions_value
        account.margin_available = account.equity * 0.5  # Simplified margin calc
        return account

    def transfer_funds(
        self,
        from_account_id: str,
        to_account_id: str,
        amount: float,
        transfer_type: str = "manual",
    ) -> TransferRequest | None:
        """Transfer funds between accounts."""
        from_acc = self._accounts.get(from_account_id)
        to_acc = self._accounts.get(to_account_id)

        if not from_acc or not to_acc:
            return None

        if from_acc.cash_balance < amount:
            return None

        # Execute transfer
        from_acc.cash_balance -= amount
        to_acc.cash_balance += amount

        transfer = TransferRequest(
            transfer_id=f"tr:{uuid.uuid4().hex[:12]}",
            from_account_id=from_account_id,
            to_account_id=to_account_id,
            amount=amount,
            transfer_type=transfer_type,
            status="completed",
            completed_at=_now(),
        )
        self._transfer_history.append(transfer)
        return transfer

    def get_transfer_history(
        self,
        account_id: str | None = None,
        limit: int = 50,
    ) -> list[TransferRequest]:
        """Get transfer history, optionally filtered by account."""
        if account_id:
            return [
                t for t in self._transfer_history
                if t.from_account_id == account_id or t.to_account_id == account_id
            ][:limit]
        return self._transfer_history[:limit]

    def create_allocation_plan(
        self,
        user_id: str,
        total_capital: float,
        account_weights: dict[str, float],
        rebalance_threshold: float = 0.05,
    ) -> CapitalAllocationPlan:
        """Create a capital allocation plan across accounts."""
        plan_id = f"cap:{uuid.uuid4().hex[:12]}"
        account_allocations = {
            acc_id: total_capital * weight
            for acc_id, weight in account_weights.items()
        }

        plan = CapitalAllocationPlan(
            plan_id=plan_id,
            user_id=user_id,
            total_capital=total_capital,
            account_allocations=account_allocations,
            strategy_allocations={},
            rebalance_threshold=rebalance_threshold,
        )
        self._allocation_plans[plan_id] = plan
        return plan

    def allocate_strategy_to_account(
        self,
        plan_id: str,
        account_id: str,
        strategy_id: str,
        allocation_amount: float,
    ) -> bool:
        """Allocate strategy funding to a specific account."""
        plan = self._allocation_plans.get(plan_id)
        if not plan:
            return False

        if account_id not in plan.account_allocations:
            return False

        if plan.strategy_allocations.get(account_id) is None:
            plan.strategy_allocations[account_id] = {}

        plan.strategy_allocations[account_id][strategy_id] = allocation_amount
        return True

    def check_rebalance_needed(
        self,
        plan_id: str,
        current_capitals: dict[str, float],
    ) -> tuple[bool, list[str]]:
        """Check if any account needs rebalancing."""
        plan = self._allocation_plans.get(plan_id)
        if not plan:
            return False, []

        needs_rebalance = []
        for acc_id, target_amount in plan.account_allocations.items():
            current = current_capitals.get(acc_id, 0.0)
            drift = abs(current - target_amount) / target_amount if target_amount > 0 else 0.0
            if drift > plan.rebalance_threshold:
                needs_rebalance.append(acc_id)

        return len(needs_rebalance) > 0, needs_rebalance

    def execute_rebalance(
        self,
        plan_id: str,
        current_capitals: dict[str, float],
    ) -> list[TransferRequest]:
        """Execute rebalancing transfers to bring accounts back to target."""
        plan = self._allocation_plans.get(plan_id)
        if not plan:
            return []

        transfers: list[TransferRequest] = []
        total_current = sum(current_capitals.values())
        total_target = sum(plan.account_allocations.values())

        if abs(total_current - total_target) > 0.01:
            return transfers

        for acc_id, target_amount in plan.account_allocations.items():
            current = current_capitals.get(acc_id, 0.0)
            diff = target_amount - current

            if abs(diff) < 1.0:  # Ignore tiny differences
                continue

            if diff > 0:
                # Need to fund this account - find one with excess
                for other_acc, other_current in current_capitals.items():
                    if other_acc == acc_id:
                        continue
                    other_target = plan.account_allocations.get(other_acc, 0.0)
                    excess = other_current - other_target
                    if excess > 0:
                        transfer_amount = min(diff, excess)
                        transfer = self.transfer_funds(
                            other_acc, acc_id, transfer_amount, "rebalance"
                        )
                        if transfer:
                            transfers.append(transfer)
                            diff -= transfer_amount
                            current_capitals[acc_id] = current + transfer_amount
                            current_capitals[other_acc] = other_current - transfer_amount
                        if diff <= 0:
                            break

        return transfers

    def get_account_summary(self, account_id: str) -> dict[str, Any] | None:
        """Get comprehensive account summary."""
        account = self._accounts.get(account_id)
        if not account:
            return None

        child_accounts = self.get_child_accounts(account_id)
        transfers = self.get_transfer_history(account_id, limit=10)

        return {
            "account_id": account.account_id,
            "user_id": account.user_id,
            "account_type": account.account_type,
            "cash_balance": account.cash_balance,
            "equity": account.equity,
            "positions_value": account.positions_value,
            "margin_available": account.margin_available,
            "child_accounts": [c.account_id for c in child_accounts],
            "recent_transfers": len(transfers),
            "created_at": account.created_at,
        }
