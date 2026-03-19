"""Tests for enhanced platform services with feature store, model training, and execution state machines."""

from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from quant_exchange.enhanced.enhanced_services import (
    DEXLiquidityService,
    ExecutionState,
    ExecutionStateMachine,
    FeatureBackfillJob,
    FeatureDefinition,
    FeatureStoreState,
    FeatureValue,
    MarketMakingService,
    ModelState,
    ModelTrainingPipeline,
    OptionsStateMachine,
    ResearchLabEnvironment,
    ScalableFeatureStore,
    SmartOrderRouter,
)


class ScalableFeatureStoreTests(unittest.TestCase):
    """Test scalable feature store."""

    def setUp(self) -> None:
        self.store = ScalableFeatureStore()

    def test_register_feature(self) -> None:
        """Verify feature registration."""
        feature = self.store.register_feature(
            feature_code="test_feature",
            feature_name="Test Feature",
            expression="sma:14",
            description="A test feature",
        )
        self.assertEqual(feature.feature_code, "test_feature")
        self.assertEqual(feature.version, "v1")

    def test_publish_version(self) -> None:
        """Verify version publishing."""
        self.store.register_feature("test_feature", "Test", "sma:14")
        new_version = self.store.publish_version("test_feature")
        self.assertEqual(new_version, "v2")

    def test_compute_feature(self) -> None:
        """Verify feature computation."""
        self.store.register_feature("sma14", "SMA 14", "sma:14")
        data = [100.0 + i for i in range(20)]
        event_time = datetime.now(timezone.utc)

        result = self.store.compute_feature("sma14", "BTCUSDT", data, event_time)
        self.assertIsInstance(result, FeatureValue)
        self.assertEqual(result.feature_code, "sma14")

    def test_start_backfill(self) -> None:
        """Verify backfill job creation."""
        self.store.register_feature("test_feature", "Test", "sma:14")
        job = self.store.start_backfill(
            feature_code="test_feature",
            instrument_ids=["BTCUSDT", "ETHUSDT"],
            start_time=datetime.now(timezone.utc) - timedelta(days=30),
            end_time=datetime.now(timezone.utc),
        )
        self.assertIsInstance(job, FeatureBackfillJob)
        self.assertEqual(job.state, FeatureStoreState.PENDING)


class ExecutionStateMachineTests(unittest.TestCase):
    """Test execution state machine."""

    def setUp(self) -> None:
        self.sm = ExecutionStateMachine()

    def test_create_order(self) -> None:
        """Verify order creation."""
        order = self.sm.create_order(
            order_id="order1",
            instrument_id="BTCUSDT",
            side="buy",
            quantity=1.0,
        )
        self.assertEqual(order["state"], ExecutionState.PENDING.value)

    def test_valid_transition(self) -> None:
        """Verify valid state transition."""
        self.sm.create_order("order1", "BTCUSDT", "buy", 1.0)
        result = self.sm.transition("order1", ExecutionState.SUBMITTED)
        self.assertTrue(result)
        self.assertEqual(self.sm.get_order("order1")["state"], ExecutionState.SUBMITTED.value)

    def test_invalid_transition(self) -> None:
        """Verify invalid state transition is rejected."""
        self.sm.create_order("order1", "BTCUSDT", "buy", 1.0)
        result = self.sm.transition("order1", ExecutionState.FILLED)
        self.assertFalse(result)

    def test_get_order_history(self) -> None:
        """Verify order history tracking."""
        self.sm.create_order("order1", "BTCUSDT", "buy", 1.0)
        self.sm.transition("order1", ExecutionState.SUBMITTED)
        history = self.sm.get_order_history("order1")
        self.assertGreaterEqual(len(history), 2)


class SmartOrderRouterTests(unittest.TestCase):
    """Test smart order router."""

    def setUp(self) -> None:
        self.router = SmartOrderRouter()

    def test_register_policy(self) -> None:
        """Verify policy registration."""
        policy = self.router.register_policy(
            policy_code="best_price",
            venues=["venue1", "venue2"],
        )
        self.assertEqual(policy["policy_code"], "best_price")

    def test_select_venue(self) -> None:
        """Verify venue selection."""
        self.router.register_policy("best_price", ["venue1", "venue2"])
        self.router.record_venue_performance("venue1", latency_ms=50, fill_rate=0.95)
        self.router.record_venue_performance("venue2", latency_ms=100, fill_rate=0.90)

        decision = self.router.select_venue("best_price", "BTCUSDT", "buy", 1.0)
        self.assertEqual(decision["selected_venue"], "venue1")


class ModelTrainingPipelineTests(unittest.TestCase):
    """Test model training pipeline."""

    def setUp(self) -> None:
        self.pipeline = ModelTrainingPipeline()

    def test_register_model(self) -> None:
        """Verify model registration."""
        model = self.pipeline.register_model(
            model_code="model1",
            model_name="Test Model",
            model_type="regression",
        )
        self.assertEqual(model["model_code"], "model1")
        self.assertEqual(model["state"], ModelState.REGISTERED.value)

    def test_deploy_model(self) -> None:
        """Verify model deployment."""
        self.pipeline.register_model("model1", "Test Model", "regression")
        deployment = self.pipeline.deploy_model("model1", "v1", "production")
        self.assertEqual(deployment["status"], "active")

    def test_record_drift(self) -> None:
        """Verify drift recording."""
        self.pipeline.register_model("model1", "Test Model", "regression")
        drift = self.pipeline.record_drift("model1", drift_score=0.5)
        self.assertFalse(drift["is_drifted"])

        drift_high = self.pipeline.record_drift("model1", drift_score=0.8)
        self.assertTrue(drift_high["is_drifted"])


class ResearchLabEnvironmentTests(unittest.TestCase):
    """Test research lab environment."""

    def setUp(self) -> None:
        self.lab = ResearchLabEnvironment()

    def test_create_project(self) -> None:
        """Verify project creation."""
        project = self.lab.create_project("proj1", "Test Project")
        self.assertEqual(project["project_code"], "proj1")

    def test_create_notebook(self) -> None:
        """Verify notebook creation."""
        self.lab.create_project("proj1", "Test Project")
        notebook = self.lab.create_notebook("proj1", "test_notebook")
        self.assertEqual(notebook["notebook_name"], "test_notebook")


class OptionsStateMachineTests(unittest.TestCase):
    """Test options state machine."""

    def setUp(self) -> None:
        self.options_sm = OptionsStateMachine()

    def test_register_option_chain(self) -> None:
        """Verify option chain registration."""
        chain = self.options_sm.register_option_chain(
            underlying="BTC",
            expiry="2024-12-31",
            strikes=[40000, 50000, 60000],
        )
        self.assertEqual(chain["underlying"], "BTC")

    def test_open_position(self) -> None:
        """Verify position opening."""
        self.options_sm.register_option_chain("BTC", "2024-12-31", [50000])
        position = self.options_sm.open_position(
            position_id="pos1",
            underlying="BTC",
            expiry="2024-12-31",
            strike=50000,
            option_type="call",
            side="buy",
            quantity=1.0,
            premium=1000.0,
        )
        self.assertEqual(position["state"], "open")


class MarketMakingServiceTests(unittest.TestCase):
    """Test market making service."""

    def setUp(self) -> None:
        self.mm = MarketMakingService()

    def test_create_config(self) -> None:
        """Verify market making config creation."""
        config = self.mm.create_config(
            symbol="BTCUSDT",
            base_spread_bps=10,
            max_inventory=10.0,
        )
        self.assertEqual(config["base_spread_bps"], 10)

    def test_calculate_bid_ask(self) -> None:
        """Verify bid/ask calculation."""
        self.mm.create_config("BTCUSDT", base_spread_bps=10, max_inventory=10.0)
        self.mm.update_inventory("BTCUSDT", position=5.0, avg_cost=50000.0)

        bid, ask = self.mm.calculate_bid_ask("BTCUSDT", mid_price=50000.0)
        self.assertLess(bid, ask)
        self.assertLess(bid, 50000.0)
        self.assertGreater(ask, 50000.0)


class DEXLiquidityServiceTests(unittest.TestCase):
    """Test DEX liquidity service."""

    def setUp(self) -> None:
        self.dex = DEXLiquidityService()

    def test_register_pool(self) -> None:
        """Verify pool registration."""
        pool = self.dex.register_pool(
            pool_code="pool1",
            token0="BTC",
            token1="ETH",
        )
        self.assertEqual(pool["pool_code"], "pool1")

    def test_add_liquidity(self) -> None:
        """Verify liquidity addition."""
        self.dex.register_pool("pool1", "BTC", "ETH")
        position = self.dex.add_liquidity(
            position_id="pos1",
            pool_code="pool1",
            amount0=1.0,
            amount1=10.0,
            share_ratio=0.01,
        )
        self.assertEqual(position["state"], "active")


if __name__ == "__main__":
    unittest.main()
