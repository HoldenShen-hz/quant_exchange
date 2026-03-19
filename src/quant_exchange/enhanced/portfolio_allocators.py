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
