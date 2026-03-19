from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from quant_exchange.enhanced import (
    AdvancedExecutionService,
    AlternativeDataService,
    BiasAuditService,
    DerivativesDexService,
    FeatureStoreService,
    LedgerService,
    ReplayService,
    ResearchMlService,
    UniverseService,
)
from quant_exchange.enhanced.enhanced_services import (
    ExecutionStateMachine,
    ExecutionState,
    SmartOrderRouter,
    OptionsStateMachine,
    MarketMakingService,
    DEXLiquidityService,
    ScalableFeatureStore,
    ResearchKernel,
    ResearchLabEnvironment,
    ModelTrainingPipeline,
    FeatureBackfillJob,
    FeatureDefinition,
    FeatureStoreState,
    ModelState,
)
from quant_exchange.persistence import SQLitePersistence

from .fixtures import sample_instrument, sample_klines


class EnhancedLayerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.db = SQLitePersistence()

    def tearDown(self) -> None:
        self.db.close()

    def test_enh_01_universe_and_feature_store(self) -> None:
        universe = UniverseService(self.db)
        feature_store = FeatureStoreService(self.db)
        universe.create_universe("crypto_global", "Crypto Global", "CRYPTO")
        universe.add_rule("crypto_global", "market_region", "eq", "GLOBAL")
        snapshot = universe.rebuild_snapshot("crypto_global", [sample_instrument()])
        feature_store.create_feature("mom_3", "Momentum 3", "momentum:3")
        feature_store.publish_version("mom_3", "v1")
        value = feature_store.compute_and_store("mom_3", "BTCUSDT", sample_klines())
        self.assertEqual(snapshot["instrument_ids"], ["BTCUSDT"])
        self.assertIn("value", value)
        self.assertEqual(self.db.count("universe_universes"), 1)
        self.assertEqual(self.db.count("feature_values"), 1)

    def test_enh_02_research_ml_audit_replay_and_ledger(self) -> None:
        research = ResearchMlService(self.db)
        audit = BiasAuditService(self.db)
        replay = ReplayService(self.db)
        ledger = LedgerService(self.db)
        research.create_project("proj1", "Project 1")
        research.register_notebook("proj1", "alpha.ipynb")
        research.create_experiment("exp1", "Experiment 1")
        research.create_experiment_run("exp1", {"sharpe": 1.2})
        research.register_model("mdl1", "Model 1")
        research.publish_model_version("mdl1", "v1")
        research.deploy_model("mdl1", "paper")
        research.record_drift("mdl1", 0.12)
        job = audit.create_job("LOOKAHEAD", "STRATEGY", "strategy:ma_sentiment")
        result = audit.run_lookahead_audit(
            job["audit_job_code"],
            [
                datetime(2025, 1, 1, tzinfo=timezone.utc),
                datetime(2025, 1, 2, tzinfo=timezone.utc),
            ],
        )
        replay.append_event("market.kline.closed", {"instrument_id": "BTCUSDT"})
        replay.create_snapshot("portfolio", {"equity": 100000})
        replay.create_replay_job("event_log")
        replay.create_shadow_deployment("baseline", "candidate", 0.01)
        ledger.create_virtual_account("VA1", "USD", 1000.0)
        ledger.create_virtual_account("VA2", "USD", 100.0)
        transfer = ledger.transfer("VA1", "VA2", 50.0)
        self.assertEqual(result["status"], "PASSED")
        self.assertEqual(transfer["amount"], 50.0)
        self.assertEqual(self.db.count("ml_models"), 1)
        self.assertEqual(self.db.count("audit_results"), 1)
        self.assertEqual(self.db.count("ledger_transfers"), 1)

    def test_enh_03_alt_execution_and_derivatives_modules(self) -> None:
        alt = AlternativeDataService(self.db)
        execution = AdvancedExecutionService(self.db)
        derivatives = DerivativesDexService(self.db)
        alt.create_source("alt-news", "Alternative news feed")
        alt.create_dataset("dataset1", "alt-news")
        alt.add_record("dataset1", {"headline": "hello"})
        execution.register_algorithm("twap", {"slice_interval_seconds": 60})
        execution.create_router_policy("best_all", ["SIM_CRYPTO", "SIM_EQUITY"])
        execution.create_order_basket("basket1", [{"instrument_id": "BTCUSDT", "qty": 1.0}])
        execution.record_router_decision("best_all", {"venue": "SIM_CRYPTO", "expected_cost": 1.2})
        derivatives.register_option_chain("btc-chain", "BTC", ["2025-03-28"])
        derivatives.create_market_making_config("mm-btc", "BTCUSDT", 5.0)
        derivatives.upsert_dex_position("lp-1", "ETH/USDC", 10_000.0)
        self.assertEqual(self.db.count("alt_dataset_records"), 1)
        self.assertEqual(self.db.count("ems_router_decisions"), 1)
        self.assertEqual(self.db.count("dex_liquidity_positions"), 1)


# ─────────────────────────────────────────────────────────────────────────────
# Enhanced Services Tests (from enhanced_services.py)
# ─────────────────────────────────────────────────────────────────────────────

class TestExecutionStateMachine(unittest.TestCase):
    def setUp(self):
        self.ems = ExecutionStateMachine()

    def test_create_order(self):
        order = self.ems.create_order("ord-1", "BTCUSDT", "BUY", 1.0)
        self.assertEqual(order["order_id"], "ord-1")
        self.assertEqual(order["state"], ExecutionState.PENDING.value)

    def test_valid_transition(self):
        self.ems.create_order("ord-2", "ETHUSDT", "BUY", 2.0)
        result = self.ems.transition("ord-2", ExecutionState.SUBMITTED)
        self.assertTrue(result)
        order = self.ems.get_order("ord-2")
        self.assertEqual(order["state"], ExecutionState.SUBMITTED.value)

    def test_invalid_transition(self):
        self.ems.create_order("ord-3", "ETHUSDT", "SELL", 3.0)
        # Cannot go directly from PENDING to FILLED
        result = self.ems.transition("ord-3", ExecutionState.FILLED)
        self.assertFalse(result)

    def test_order_history(self):
        self.ems.create_order("ord-4", "BTCUSDT", "BUY", 1.0)
        self.ems.transition("ord-4", ExecutionState.SUBMITTED)
        self.ems.transition("ord-4", ExecutionState.FILLED, filled_quantity=1.0)
        history = self.ems.get_order_history("ord-4")
        self.assertTrue(len(history) >= 3)


class TestSmartOrderRouter(unittest.TestCase):
    def setUp(self):
        self.sor = SmartOrderRouter()

    def test_register_policy(self):
        policy = self.sor.register_policy("best_price", ["VENUE_A", "VENUE_B"])
        self.assertEqual(policy["policy_code"], "best_price")
        self.assertEqual(policy["venues"], ["VENUE_A", "VENUE_B"])

    def test_select_venue(self):
        self.sor.register_policy("low_latency", ["VENUE_A", "VENUE_B", "VENUE_C"])
        self.sor.record_venue_performance("VENUE_A", latency_ms=50.0, fill_rate=0.95)
        self.sor.record_venue_performance("VENUE_B", latency_ms=30.0, fill_rate=0.90)
        decision = self.sor.select_venue("low_latency", "BTCUSDT", "BUY", 1.0)
        self.assertEqual(decision["instrument_id"], "BTCUSDT")
        self.assertIn(decision["selected_venue"], ["VENUE_A", "VENUE_B", "VENUE_C"])

    def test_venue_performance_recording(self):
        self.sor.record_venue_performance("VENUE_X", latency_ms=100.0, fill_rate=0.85)
        self.sor.record_venue_performance("VENUE_X", latency_ms=50.0, fill_rate=0.90)
        # Average should be updated
        self.assertIn("VENUE_X", self.sor._venue_latencies)


class TestOptionsStateMachine(unittest.TestCase):
    def setUp(self):
        self.options = OptionsStateMachine()

    def test_register_option_chain(self):
        chain = self.options.register_option_chain("BTC", "2025-03-28", [45000, 46000, 47000])
        self.assertEqual(chain["underlying"], "BTC")
        self.assertIn(45000, chain["strikes"])

    def test_open_position(self):
        pos = self.options.open_position(
            "pos-1", "BTC", "2025-03-28", 45000.0, "call", "BUY", 1.0, 500.0
        )
        self.assertEqual(pos["position_id"], "pos-1")
        self.assertEqual(pos["state"], "open")

    def test_update_greeks(self):
        self.options.open_position("pos-2", "ETH", "2025-04-01", 2000.0, "put", "SELL", 2.0, 100.0)
        self.options.update_greeks("pos-2", delta=0.3, gamma=0.02, theta=-0.05, vega=0.15, rho=-0.01, current_price=110.0)
        pos = self.options._positions["pos-2"]
        self.assertEqual(pos["delta"], 0.3)
        self.assertEqual(pos["vega"], 0.15)

    def test_exercise_option(self):
        self.options.open_position("pos-3", "BTC", "2025-03-28", 45000.0, "call", "BUY", 1.0, 500.0)
        result = self.options.exercise_option("pos-3", spot_price=46000.0)
        self.assertEqual(result["position_id"], "pos-3")


class TestMarketMakingService(unittest.TestCase):
    def setUp(self):
        self.mm = MarketMakingService()

    def test_create_config(self):
        cfg = self.mm.create_config("BTCUSDT", base_spread_bps=10.0, max_inventory=10.0)
        self.assertEqual(cfg["symbol"], "BTCUSDT")
        self.assertEqual(cfg["base_spread_bps"], 10.0)

    def test_calculate_bid_ask(self):
        self.mm.create_config("ETHUSDT", base_spread_bps=20.0, max_inventory=5.0)
        bid, ask = self.mm.calculate_bid_ask("ETHUSDT", mid_price=2000.0)
        self.assertTrue(bid < ask)
        self.assertTrue(ask - bid > 0)  # Spread exists

    def test_update_inventory(self):
        self.mm.update_inventory("BTCUSDT", position=2.5, avg_cost=42000.0)
        inv = self.mm._inventories.get("BTCUSDT")
        self.assertIsNotNone(inv)
        self.assertEqual(inv["position"], 2.5)


class TestDEXLiquidityService(unittest.TestCase):
    def setUp(self):
        self.dex = DEXLiquidityService()

    def test_register_pool(self):
        pool = self.dex.register_pool("ETH/USDC", "ETH", "USDC", fee_tier="0.30")
        self.assertEqual(pool["token0"], "ETH")
        self.assertEqual(pool["fee_tier"], "0.30")

    def test_add_remove_liquidity(self):
        self.dex.register_pool("ETH/USDC", "ETH", "USDC")
        lp = self.dex.add_liquidity("lp-1", "ETH/USDC", 1.0, 2000.0, 0.005)
        self.assertEqual(lp["state"], "active")
        removed = self.dex.remove_liquidity("lp-1")
        self.assertEqual(removed["state"], "removed")

    def test_record_swap(self):
        self.dex.register_pool("ETH/USDC", "ETH", "USDC")
        swap = self.dex.record_swap("swap-1", "ETH/USDC", "BUY", 1.0, 2000.0, 2.0)
        self.assertEqual(swap["amount_in"], 1.0)


class TestScalableFeatureStore(unittest.TestCase):
    def setUp(self):
        self.db = SQLitePersistence()
        self.fs = ScalableFeatureStore(self.db)

    def tearDown(self):
        self.db.close()

    def test_register_and_get_feature(self):
        feature = self.fs.register_feature("rsi_14", "RSI 14-period", "TA:RSI:14")
        self.assertEqual(feature.feature_code, "rsi_14")
        self.assertIn("rsi_14", self.fs._definitions)

    def test_publish_version(self):
        self.fs.register_feature("vol_20", "Volatility 20", "TA:VOL:20")
        version = self.fs.publish_version("vol_20")
        self.assertTrue(version.startswith("v"))


class TestResearchKernel(unittest.TestCase):
    def setUp(self):
        self.rk = ResearchKernel()

    def test_execute_simple_code(self):
        result = self.rk.execute("print('hello')")
        self.assertIn("output", result)


class TestResearchLabEnvironment(unittest.TestCase):
    def setUp(self):
        self.db = SQLitePersistence()
        self.lab = ResearchLabEnvironment(self.db)

    def tearDown(self):
        self.db.close()

    def test_create_project(self):
        proj = self.lab.create_project("proj-1", "Research Project 1")
        self.assertEqual(proj["project_code"], "proj-1")

    def test_create_notebook(self):
        self.lab.create_project("proj-2", "Research Project 2")
        nb = self.lab.create_notebook("proj-2", "notebook-1")
        self.assertIn("notebook_key", nb)


class TestModelTrainingPipeline(unittest.TestCase):
    def setUp(self):
        self.db = SQLitePersistence()
        self.pipeline = ModelTrainingPipeline(self.db)

    def tearDown(self):
        self.db.close()

    def test_register_model(self):
        model = self.pipeline.register_model("model-1", "Test Model", "LIGHTGBM")
        self.assertEqual(model["model_code"], "model-1")
        self.assertEqual(model["model_type"], "LIGHTGBM")

    def test_start_training(self):
        self.pipeline.register_model("model-2", "Test Model 2", "XGBOOST")
        job = self.pipeline.start_training("model-2", {"n_estimators": 100}, "data-1")
        self.assertIn("job_id", job)


if __name__ == "__main__":
    unittest.main()
