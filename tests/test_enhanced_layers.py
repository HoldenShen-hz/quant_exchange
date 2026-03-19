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


if __name__ == "__main__":
    unittest.main()
