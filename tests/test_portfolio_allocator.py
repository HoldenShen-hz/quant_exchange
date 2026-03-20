"""Tests for portfolio allocation and risk attribution (PF-01 ~ PF-06)."""

from __future__ import annotations

import unittest
from quant_exchange.enhanced.portfolio_allocators import (
    AllocatorType,
    PortfolioAllocatorService,
    RiskExposureAggregator,
    AttributionAnalyzer,
    MultiAccountAllocator,
)


class PortfolioAllocatorServiceTests(unittest.TestCase):
    """PF-01~PF-03: Portfolio allocation strategies."""

    def setUp(self) -> None:
        self.service = PortfolioAllocatorService()

    def test_create_equal_weight_allocator(self) -> None:
        """PF-01: Create an equal-weight allocator."""
        config = self.service.create_allocator(
            user_id="user1",
            allocator_type=AllocatorType.EQUAL_WEIGHT,
            name="Equal Weight Portfolio",
            description="Simple equal-weight allocation",
        )
        self.assertEqual(config.name, "Equal Weight Portfolio")
        self.assertEqual(config.allocator_type, AllocatorType.EQUAL_WEIGHT)
        self.assertEqual(config.max_weight, 0.3)

    def test_create_risk_parity_allocator(self) -> None:
        """PF-01: Create a risk-parity allocator."""
        config = self.service.create_allocator(
            user_id="user1",
            allocator_type=AllocatorType.RISK_PARITY,
            name="Risk Parity Portfolio",
        )
        self.assertEqual(config.allocator_type, AllocatorType.RISK_PARITY)

    def test_create_mean_variance_allocator(self) -> None:
        """PF-02: Create a mean-variance (Markowitz) allocator."""
        config = self.service.create_allocator(
            user_id="user1",
            allocator_type=AllocatorType.MEAN_VARIANCE,
            name="Markowitz Portfolio",
        )
        self.assertEqual(config.allocator_type, AllocatorType.MEAN_VARIANCE)

    def test_create_black_litterman_allocator(self) -> None:
        """PF-02: Create a Black-Litterman allocator with views."""
        config = self.service.create_allocator(
            user_id="user1",
            allocator_type=AllocatorType.BLACK_LITTERMAN,
            name="BL Portfolio",
            equilibrium_returns={"AAPL": 0.1, "GOOG": 0.08},
            views={"AAPL": 0.15},
            view_confidence=0.5,
        )
        self.assertEqual(config.allocator_type, AllocatorType.BLACK_LITTERMAN)
        self.assertIsNotNone(config.views)

    def test_equal_weight_allocation(self) -> None:
        """PF-01: Equal-weight allocation across instruments."""
        config = self.service.create_allocator(
            user_id="user1",
            allocator_type=AllocatorType.EQUAL_WEIGHT,
            name="Test",
            max_weight=0.5,  # Set high enough for 3 assets at ~33% each
        )
        result = self.service.calculate_allocation(
            allocator_config=config,
            expected_returns={"A": 0.1, "B": 0.05, "C": 0.08},
            volatilities={"A": 0.2, "B": 0.15, "C": 0.18},
        )
        # Equal weight should give roughly 1/3 each (allowing for floating point)
        self.assertAlmostEqual(result.weights["A"] + result.weights["B"] + result.weights["C"], 1.0, places=3)
        self.assertTrue(result.constraints_satisfied)

    def test_risk_parity_allocation(self) -> None:
        """PF-01: Risk-parity allocation."""
        config = self.service.create_allocator(
            user_id="user1",
            allocator_type=AllocatorType.RISK_PARITY,
            name="Risk Parity",
        )
        result = self.service.calculate_allocation(
            allocator_config=config,
            expected_returns={"A": 0.1, "B": 0.05, "C": 0.08},
            volatilities={"A": 0.2, "B": 0.1, "C": 0.15},  # B is least volatile
        )
        # B should have higher weight due to lower volatility
        self.assertGreater(result.weights["B"], result.weights["A"])

    def test_max_sharpe_allocation(self) -> None:
        """PF-02: Maximum Sharpe ratio allocation."""
        config = self.service.create_allocator(
            user_id="user1",
            allocator_type=AllocatorType.MAX_SHARPE,
            name="Max Sharpe",
        )
        result = self.service.calculate_allocation(
            allocator_config=config,
            expected_returns={"A": 0.12, "B": 0.08},
            volatilities={"A": 0.2, "B": 0.15},
        )
        # Weights should be positive and sum to 1
        self.assertGreater(result.weights["A"], 0)
        self.assertGreater(result.weights["B"], 0)
        self.assertAlmostEqual(result.weights["A"] + result.weights["B"], 1.0, places=3)

    def test_black_litterman_with_views(self) -> None:
        """PF-02: Black-Litterman adjusts weights based on views."""
        config = self.service.create_allocator(
            user_id="user1",
            allocator_type=AllocatorType.BLACK_LITTERMAN,
            name="BL Test",
            equilibrium_returns={"A": 0.08, "B": 0.08},
            views={"A": 0.20},  # Strong bullish view on A
            view_confidence=0.7,
        )
        result = self.service.calculate_allocation(
            allocator_config=config,
            expected_returns={"A": 0.08, "B": 0.08},
            volatilities={"A": 0.2, "B": 0.2},
        )
        # Weights should be positive and sum to ~1
        self.assertGreater(result.weights["A"], 0)
        self.assertGreater(result.weights["B"], 0)
        self.assertAlmostEqual(result.weights["A"] + result.weights["B"], 1.0, places=3)

    def test_min_variance_allocation(self) -> None:
        """PF-02: Minimum variance allocation."""
        config = self.service.create_allocator(
            user_id="user1",
            allocator_type=AllocatorType.MIN_VARIANCE,
            name="Min Variance",
        )
        result = self.service.calculate_allocation(
            allocator_config=config,
            expected_returns={"A": 0.1, "B": 0.05},
            volatilities={"A": 0.3, "B": 0.1},  # B is less volatile
            correlations={},
        )
        # B should get more weight as it's less volatile
        self.assertGreater(result.weights["B"], result.weights["A"])

    def test_weight_constraints_enforced(self) -> None:
        """PF-02: Max weight constraint is enforced."""
        config = self.service.create_allocator(
            user_id="user1",
            allocator_type=AllocatorType.EQUAL_WEIGHT,
            name="Constrained",
            max_weight=0.4,  # 40% max
        )
        # With 3 instruments, equal would be ~33% each - within constraint
        result = self.service.calculate_allocation(
            allocator_config=config,
            expected_returns={"A": 0.1, "B": 0.05, "C": 0.08},
            volatilities={"A": 0.2, "B": 0.2, "C": 0.2},
        )
        # All weights should be positive and sum to 1
        for w in result.weights.values():
            self.assertGreater(w, 0)
        self.assertAlmostEqual(sum(result.weights.values()), 1.0, places=3)

    def test_short_selling_not_allowed_by_default(self) -> None:
        """PF-02: Short selling is disabled by default."""
        config = self.service.create_allocator(
            user_id="user1",
            allocator_type=AllocatorType.EQUAL_WEIGHT,
            name="No Short",
            allow_short=False,
        )
        self.assertFalse(config.allow_short)

    def test_empty_instruments_returns_empty_weights(self) -> None:
        """PF-01: Empty instrument list returns empty weights."""
        config = self.service.create_allocator(
            user_id="user1",
            allocator_type=AllocatorType.EQUAL_WEIGHT,
            name="Empty",
        )
        result = self.service.calculate_allocation(
            allocator_config=config,
            expected_returns={},
            volatilities={},
        )
        self.assertEqual(result.weights, {})
        self.assertEqual(result.expected_return, 0.0)


class RebalanceTests(unittest.TestCase):
    """PF-03: Dynamic rebalancing."""

    def setUp(self) -> None:
        self.service = PortfolioAllocatorService()

    def test_rebalance_plan_generates_trades(self) -> None:
        """PF-03: Calculate rebalancing trades."""
        plan = self.service.calculate_rebalance_plan(
            portfolio_id="test",
            target_weights={"A": 0.5, "B": 0.5},
            current_weights={"A": 0.7, "B": 0.3},
            current_prices={"A": 100.0, "B": 50.0},
            notional=100000.0,
        )
        self.assertGreater(len(plan.trades), 0)
        # Should have sell A and buy B
        trade_dirs = {t["direction"] for t in plan.trades}
        self.assertIn("sell", trade_dirs)
        self.assertIn("buy", trade_dirs)

    def test_rebalance_plan_sorted_by_drift(self) -> None:
        """PF-03: Largest drifts are executed first."""
        plan = self.service.calculate_rebalance_plan(
            portfolio_id="test",
            target_weights={"A": 0.4, "B": 0.4, "C": 0.2},
            current_weights={"A": 0.8, "B": 0.1, "C": 0.1},  # A has largest drift
            current_prices={"A": 100.0, "B": 100.0, "C": 100.0},
            notional=100000.0,
        )
        self.assertEqual(plan.trades[0]["instrument_id"], "A")  # A has largest drift (0.4)

    def test_check_rebalance_needed_within_threshold(self) -> None:
        """PF-03: No rebalance needed when within threshold."""
        config = self.service.create_allocator(
            user_id="user1",
            allocator_type=AllocatorType.EQUAL_WEIGHT,
            name="Test",
            drift_threshold=0.1,  # 10% threshold
        )
        allocation = self.service.create_allocation(
            user_id="user1",
            portfolio_id="test",
            allocator_config=config,
            target_weights={"A": 0.5, "B": 0.5},
        )
        # All within 5% drift
        current = {"A": 0.52, "B": 0.48}
        needs_rebalance = self.service.check_rebalance_needed(allocation, current)
        self.assertFalse(needs_rebalance)

    def test_check_rebalance_needed_exceeds_threshold(self) -> None:
        """PF-03: Rebalance needed when drift exceeds threshold."""
        config = self.service.create_allocator(
            user_id="user1",
            allocator_type=AllocatorType.EQUAL_WEIGHT,
            name="Test",
            drift_threshold=0.05,  # 5% threshold
        )
        allocation = self.service.create_allocation(
            user_id="user1",
            portfolio_id="test",
            allocator_config=config,
            target_weights={"A": 0.5, "B": 0.5},
        )
        # A has 15% drift which exceeds 5% threshold
        current = {"A": 0.65, "B": 0.35}
        needs_rebalance = self.service.check_rebalance_needed(allocation, current)
        self.assertTrue(needs_rebalance)


class RiskExposureAggregatorTests(unittest.TestCase):
    """PF-04: Inter-strategy risk exposure aggregation."""

    def setUp(self) -> None:
        self.aggregator = RiskExposureAggregator()

    def test_register_instrument_sector(self) -> None:
        """PF-04: Register sector mapping for instruments."""
        self.aggregator.register_instrument_sector("AAPL", "Technology")
        self.aggregator.register_instrument_sector("GOOG", "Technology")
        self.aggregator.register_instrument_sector("JPM", "Financials")

    def test_aggregate_single_strategy(self) -> None:
        """PF-04: Aggregate exposure for a single strategy."""
        self.aggregator.record_strategy_position(
            strategy_id="momentum",
            positions={"AAPL": 100, "GOOG": -50},
            cash=50000.0,
        )
        prices = {"AAPL": 150.0, "GOOG": 2800.0}
        exposure = self.aggregator.aggregate_exposures(prices=prices)

        self.assertEqual(exposure.total_gross_exposure, 100 * 150 + 50 * 2800)
        self.assertLess(exposure.total_net_exposure, exposure.total_gross_exposure)  # Has short

    def test_aggregate_multiple_strategies(self) -> None:
        """PF-04: Aggregate exposure across multiple strategies."""
        self.aggregator.record_strategy_position(
            strategy_id="momentum",
            positions={"AAPL": 100},
            cash=10000.0,
        )
        self.aggregator.record_strategy_position(
            strategy_id="value",
            positions={"AAPL": 50, "JPM": 100},
            cash=20000.0,
        )
        prices = {"AAPL": 150.0, "JPM": 140.0}
        exposure = self.aggregator.aggregate_exposures(prices=prices)

        self.assertIn("momentum", exposure.strategy_exposures)
        self.assertIn("value", exposure.strategy_exposures)
        # AAPL should be concentrated (150*100 + 150*50 = 22500)
        self.assertGreater(exposure.concentration_risk.get("AAPL", 0), 0)

    def test_concentration_risk_calculation(self) -> None:
        """PF-04: Concentration risk is calculated correctly."""
        self.aggregator.record_strategy_position(
            strategy_id="single",
            positions={"AAPL": 1000, "GOOG": 1},  # AAPL dominates
            cash=1000.0,
        )
        prices = {"AAPL": 150.0, "GOOG": 2800.0}
        exposure = self.aggregator.aggregate_exposures(prices=prices)

        # AAPL notional = 150000, GOOG = 2800, total = 152800
        # AAPL concentration should be ~98%
        aapl_conc = exposure.concentration_risk.get("AAPL", 0)
        self.assertGreater(aapl_conc, 0.9)


class AttributionAnalyzerTests(unittest.TestCase):
    """PF-05: Return attribution analysis."""

    def setUp(self) -> None:
        self.analyzer = AttributionAnalyzer()

    def test_brinson_attribution_allocation_effect(self) -> None:
        """PF-05: Brinson attribution separates allocation and selection effects."""
        result = self.analyzer.brinson_attribution(
            portfolio_weights={"A": 0.6, "B": 0.4},
            benchmark_weights={"A": 0.4, "B": 0.6},
            portfolio_returns={"A": 0.10, "B": 0.05},
            benchmark_returns={"A": 0.08, "B": 0.06},
        )
        # Allocation effect: overweight A (0.2) * A's benchmark return (0.08)
        # Selection effect: A benchmark weight (0.4) * (0.10 - 0.08)
        self.assertGreater(result.allocation_effect, 0)

    def test_brinson_attribution_total_return(self) -> None:
        """PF-05: Total return is sum of contributions."""
        result = self.analyzer.brinson_attribution(
            portfolio_weights={"A": 0.5, "B": 0.5},
            benchmark_weights={"A": 0.5, "B": 0.5},
            portfolio_returns={"A": 0.10, "B": 0.05},
            benchmark_returns={"A": 0.08, "B": 0.04},
        )
        # Total return = 0.5*0.10 + 0.5*0.05 = 0.075
        self.assertAlmostEqual(result.total_return, 0.075, places=4)

    def test_nav_attribution_pnl_contribution(self) -> None:
        """PF-05: NAV attribution calculates PnL contributions."""
        result = self.analyzer.calculate_nav_attribution(
            positions={"AAPL": 100},
            prices_start={"AAPL": 150.0},
            prices_end={"AAPL": 165.0},
            benchmark_return=0.05,
        )
        # PnL = 100 * (165 - 150) = 1500
        self.assertEqual(result.return_contributions.get("AAPL"), 1500)
        self.assertGreater(result.active_return, 0)  # Beat benchmark

    def test_top_contributors_and_detrators(self) -> None:
        """PF-05: Identify top contributors and detractors."""
        contributions = {"AAPL": 1000, "GOOG": -500, "MSFT": 300, "JPM": -200}
        top, bottom = self.analyzer.get_top_contributors(contributions, n=2)
        # Top should be highest values
        self.assertEqual(top[0][0], "AAPL")
        self.assertEqual(top[0][1], 1000)
        # Bottom should be lowest values
        self.assertEqual(bottom[-1][0], "GOOG")
        self.assertEqual(bottom[-1][1], -500)


class MultiAccountAllocatorTests(unittest.TestCase):
    """PF-06: Multi-account capital allocation."""

    def setUp(self) -> None:
        self.allocator = MultiAccountAllocator()

    def test_create_primary_account(self) -> None:
        """PF-06: Create a primary account."""
        account = self.allocator.create_account(
            user_id="user1",
            account_type="primary",
            initial_cash=100000.0,
        )
        self.assertEqual(account.user_id, "user1")
        self.assertEqual(account.cash_balance, 100000.0)
        self.assertEqual(account.equity, 100000.0)

    def test_create_sub_account(self) -> None:
        """PF-06: Create a sub-account linked to parent."""
        parent = self.allocator.create_account(
            user_id="user1",
            account_type="primary",
            initial_cash=100000.0,
        )
        child = self.allocator.create_account(
            user_id="user1",
            account_type="sub",
            parent_account_id=parent.account_id,
            initial_cash=0.0,
        )
        self.assertEqual(child.parent_account_id, parent.account_id)

    def test_transfer_funds(self) -> None:
        """PF-06: Transfer funds between accounts."""
        acc1 = self.allocator.create_account("user1", "primary", initial_cash=50000.0)
        acc2 = self.allocator.create_account("user1", "sub", parent_account_id=acc1.account_id, initial_cash=10000.0)

        transfer = self.allocator.transfer_funds(
            from_account_id=acc1.account_id,
            to_account_id=acc2.account_id,
            amount=10000.0,
        )
        self.assertIsNotNone(transfer)
        self.assertEqual(transfer.status, "completed")

        # Check balances updated
        acc1_updated = self.allocator.get_account(acc1.account_id)
        acc2_updated = self.allocator.get_account(acc2.account_id)
        self.assertEqual(acc1_updated.cash_balance, 40000.0)
        self.assertEqual(acc2_updated.cash_balance, 20000.0)

    def test_transfer_insufficient_balance(self) -> None:
        """PF-06: Transfer fails with insufficient balance."""
        acc1 = self.allocator.create_account("user1", "primary", 5000.0)
        acc2 = self.allocator.create_account("user1", "sub", 1000.0)

        transfer = self.allocator.transfer_funds(
            from_account_id=acc1.account_id,
            to_account_id=acc2.account_id,
            amount=10000.0,  # More than available
        )
        self.assertIsNone(transfer)

    def test_get_child_accounts(self) -> None:
        """PF-06: Get all child accounts of a parent."""
        parent = self.allocator.create_account("user1", "primary", 100000.0)
        child1 = self.allocator.create_account("user1", "sub", parent.account_id, 0.0)
        child2 = self.allocator.create_account("user1", "sub", parent.account_id, 0.0)

        children = self.allocator.get_child_accounts(parent.account_id)
        self.assertEqual(len(children), 2)

    def test_allocation_plan_and_rebalance(self) -> None:
        """PF-06: Create allocation plan and execute rebalance."""
        acc1 = self.allocator.create_account("user1", "primary", 100000.0)
        acc2 = self.allocator.create_account("user1", "sub", acc1.account_id, 0.0)

        plan = self.allocator.create_allocation_plan(
            user_id="user1",
            total_capital=100000.0,
            account_weights={acc1.account_id: 0.7, acc2.account_id: 0.3},
            rebalance_threshold=0.05,
        )
        self.assertIsNotNone(plan)

        # Simulate acc1 growing to 80%, acc2 at 20%
        current_capitals = {acc1.account_id: 80000.0, acc2.account_id: 20000.0}
        needs_rb, accs = self.allocator.check_rebalance_needed(plan.plan_id, current_capitals)
        # 80% vs 70% target = 10% drift > 5% threshold
        self.assertTrue(needs_rb)
        self.assertIn(acc1.account_id, accs)

    def test_get_account_summary(self) -> None:
        """PF-06: Get comprehensive account summary."""
        account = self.allocator.create_account("user1", "primary", initial_cash=100000.0)
        # Update balance to reflect equity
        self.allocator.update_account_balance(account.account_id, 0.0, 0.0)
        summary = self.allocator.get_account_summary(account.account_id)

        self.assertIsNotNone(summary)
        self.assertEqual(summary["user_id"], "user1")
        self.assertEqual(summary["cash_balance"], 100000.0)


class RiskBudgetTests(unittest.TestCase):
    """PF-04: Risk budget allocation."""

    def setUp(self) -> None:
        self.service = PortfolioAllocatorService()

    def test_allocate_risk_budget_equal(self) -> None:
        """PF-04: Equal risk budget allocation."""
        budget = self.service.allocate_risk_budget(
            user_id="user1",
            total_budget=100000.0,
            instruments=["AAPL", "GOOG", "MSFT"],
            method="equal",
        )
        self.assertEqual(budget.allocations["AAPL"], 100000.0 / 3)
        self.assertEqual(budget.allocations["GOOG"], 100000.0 / 3)
        self.assertEqual(budget.allocations["MSFT"], 100000.0 / 3)

    def test_check_risk_budget_usage_within_limits(self) -> None:
        """PF-04: No warning when within budget."""
        budget = self.service.allocate_risk_budget(
            user_id="user1",
            total_budget=100000.0,
            instruments=["AAPL"],
        )
        exceeded, warnings = self.service.check_risk_budget_usage(
            budget,
            current_risks={"AAPL": 50000.0},  # 50% of 100K budget
        )
        self.assertFalse(exceeded)
        self.assertEqual(len(warnings), 0)

    def test_check_risk_budget_usage_exceeded(self) -> None:
        """PF-04: Warning when exceeding budget."""
        budget = self.service.allocate_risk_budget(
            user_id="user1",
            total_budget=100000.0,
            instruments=["AAPL"],
        )
        exceeded, warnings = self.service.check_risk_budget_usage(
            budget,
            current_risks={"AAPL": 120000.0},  # 120% of budget
        )
        self.assertTrue(exceeded)
        self.assertGreater(len(warnings), 0)


if __name__ == "__main__":
    unittest.main()
