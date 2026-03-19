"""Tests for strategy configuration, versioning, and parameter persistence."""

from __future__ import annotations

import json
import os
import tempfile
import unittest
from datetime import timedelta

from quant_exchange.strategy.config import (
    StrategyConfigLoader,
    StrategyParameterSet,
    StrategyParameterStore,
    StrategyRun,
    StrategyRunRecorder,
    StrategyVersion,
    StrategyVersionManager,
)


class StrategyVersionManagerTests(unittest.TestCase):
    """Test strategy version management functionality (ST-04)."""

    def setUp(self) -> None:
        self.manager = StrategyVersionManager()

    def test_register_version_creates_new_version(self) -> None:
        params = {"fast_window": 10, "slow_window": 30}
        ver = self.manager.register_version("strategy_001", params, description="initial version")

        self.assertEqual(ver.version, "1.0")
        self.assertEqual(ver.strategy_id, "strategy_001")
        self.assertEqual(ver.params_hash, self.manager._versions[("strategy_001", "1.0")].params_hash)

    def test_register_multiple_versions_increments_version_number(self) -> None:
        params1 = {"fast_window": 5, "slow_window": 20}
        params2 = {"fast_window": 10, "slow_window": 50}

        ver1 = self.manager.register_version("strategy_001", params1)
        ver2 = self.manager.register_version("strategy_001", params2)

        self.assertEqual(ver1.version, "1.0")
        self.assertEqual(ver2.version, "1.1")

    def test_get_version_returns_registered_version(self) -> None:
        params = {"fast_window": 10, "slow_window": 30}
        created = self.manager.register_version("strategy_001", params, version="2.0")

        retrieved = self.manager.get_version("strategy_001", "2.0")

        self.assertIsNotNone(retrieved)
        self.assertEqual(retrieved.version, "2.0")
        self.assertEqual(retrieved.strategy_id, "strategy_001")

    def test_list_versions_returns_all_versions_in_order(self) -> None:
        params = {"fast_window": 10}
        self.manager.register_version("strategy_001", params, version="1.0")
        self.manager.register_version("strategy_001", params, version="2.0")
        self.manager.register_version("strategy_001", params, version="3.0")

        versions = self.manager.list_versions("strategy_001")

        self.assertEqual(len(versions), 3)
        self.assertEqual(versions[0].version, "1.0")
        self.assertEqual(versions[1].version, "2.0")
        self.assertEqual(versions[2].version, "3.0")

    def test_same_params_produce_same_hash(self) -> None:
        params = {"fast_window": 10, "slow_window": 30}
        ver1 = self.manager.register_version("strategy_001", params)
        ver2 = self.manager.register_version("strategy_001", params)

        self.assertEqual(ver1.params_hash, ver2.params_hash)

    def test_different_params_produce_different_hash(self) -> None:
        params1 = {"fast_window": 10}
        params2 = {"fast_window": 20}

        ver1 = self.manager.register_version("strategy_001", params1)
        ver2 = self.manager.register_version("strategy_001", params2)

        self.assertNotEqual(ver1.params_hash, ver2.params_hash)


class StrategyParameterSetTests(unittest.TestCase):
    """Test strategy parameter set functionality (ST-03)."""

    def test_parameter_set_creation(self) -> None:
        params = {"fast_window": 10, "slow_window": 30, "volatility_cap": 0.5}
        ps = StrategyParameterSet(
            strategy_id="strategy_001",
            name="default",
            params=params,
        )

        self.assertEqual(ps.strategy_id, "strategy_001")
        self.assertEqual(ps.name, "default")
        self.assertEqual(ps.params, params)
        self.assertFalse(ps.is_frozen)

    def test_freeze_prevents_modification(self) -> None:
        ps = StrategyParameterSet(
            strategy_id="strategy_001",
            name="default",
            params={"fast_window": 10},
        )
        ps.freeze()

        self.assertTrue(ps.is_frozen)

    def test_clone_with_overrides_creates_new_set(self) -> None:
        ps = StrategyParameterSet(
            strategy_id="strategy_001",
            name="default",
            params={"fast_window": 10, "slow_window": 30},
        )

        cloned = ps.clone_with_overrides({"fast_window": 20})

        self.assertEqual(cloned.name, "default_override")
        self.assertEqual(cloned.params["fast_window"], 20)
        self.assertEqual(cloned.params["slow_window"], 30)

    def test_clone_on_frozen_set_raises_error(self) -> None:
        ps = StrategyParameterSet(
            strategy_id="strategy_001",
            name="default",
            params={"fast_window": 10},
        )
        ps.freeze()

        with self.assertRaises(ValueError):
            ps.clone_with_overrides({"fast_window": 20})


class StrategyParameterStoreTests(unittest.TestCase):
    """Test strategy parameter set persistence (ST-03)."""

    def setUp(self) -> None:
        self.temp_dir = tempfile.mkdtemp()
        self.store = StrategyParameterStore(storage_path=self.temp_dir)

    def tearDown(self) -> None:
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_save_and_load_parameter_set(self) -> None:
        params = {"fast_window": 10, "slow_window": 30}
        ps = StrategyParameterSet(
            strategy_id="strategy_001",
            name="default",
            params=params,
        )

        self.store.save(ps)
        loaded = self.store.load("strategy_001", "default")

        self.assertIsNotNone(loaded)
        self.assertEqual(loaded.strategy_id, "strategy_001")
        self.assertEqual(loaded.name, "default")
        self.assertEqual(loaded.params, params)

    def test_list_param_sets(self) -> None:
        ps1 = StrategyParameterSet(strategy_id="strategy_001", name="set1", params={"w": 10})
        ps2 = StrategyParameterSet(strategy_id="strategy_001", name="set2", params={"w": 20})
        ps3 = StrategyParameterSet(strategy_id="strategy_002", name="set3", params={"w": 30})

        self.store.save(ps1)
        self.store.save(ps2)
        self.store.save(ps3)

        sets = self.store.list_param_sets("strategy_001")

        self.assertEqual(len(sets), 2)

    def test_delete_parameter_set(self) -> None:
        ps = StrategyParameterSet(strategy_id="strategy_001", name="default", params={"w": 10})
        self.store.save(ps)

        result = self.store.delete("strategy_001", "default")

        self.assertTrue(result)
        self.assertIsNone(self.store.load("strategy_001", "default"))


class StrategyRunRecorderTests(unittest.TestCase):
    """Test strategy run recording functionality (ST-04, ST-06)."""

    def setUp(self) -> None:
        self.temp_dir = tempfile.mkdtemp()
        self.recorder = StrategyRunRecorder(storage_path=self.temp_dir)

    def tearDown(self) -> None:
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_start_run_creates_new_run_record(self) -> None:
        run = self.recorder.start_run(
            strategy_id="strategy_001",
            version="1.0",
            params={"fast_window": 10},
            instrument_ids=("BTCUSDT",),
        )

        self.assertIsNotNone(run.run_id)
        self.assertEqual(run.strategy_id, "strategy_001")
        self.assertEqual(run.version, "1.0")
        self.assertIsNone(run.ended_at)
        self.assertIsNone(run.final_equity)

    def test_complete_run_updates_run_record(self) -> None:
        run = self.recorder.start_run(
            strategy_id="strategy_001",
            version="1.0",
            params={"fast_window": 10},
        )

        completed = self.recorder.complete_run(
            run_id=run.run_id,
            final_equity=110_000.0,
            total_return=0.10,
            sharpe=1.5,
            max_drawdown=0.05,
        )

        self.assertIsNotNone(completed)
        self.assertIsNotNone(completed.ended_at)
        self.assertEqual(completed.final_equity, 110_000.0)
        self.assertEqual(completed.total_return, 0.10)
        self.assertEqual(completed.sharpe, 1.5)
        self.assertEqual(completed.max_drawdown, 0.05)

    def test_list_runs_returns_runs_in_order(self) -> None:
        run1 = self.recorder.start_run("s1", "1.0", {})
        run2 = self.recorder.start_run("s1", "1.0", {})
        run3 = self.recorder.start_run("s2", "1.0", {})

        runs = self.recorder.list_runs(strategy_id="s1")

        self.assertEqual(len(runs), 2)

    def test_duration_seconds_calculates_correctly(self) -> None:
        from datetime import datetime, timezone
        run = StrategyRun(
            run_id="test",
            strategy_id="s1",
            version="1.0",
            params={},
            started_at=datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc),
            ended_at=datetime(2024, 1, 1, 10, 1, 0, tzinfo=timezone.utc),
        )

        self.assertEqual(run.duration_seconds, 60.0)


class StrategyConfigLoaderTests(unittest.TestCase):
    """Test YAML/TOML configuration loading (ST-03)."""

    def setUp(self) -> None:
        self.temp_dir = tempfile.mkdtemp()
        self.loader = StrategyConfigLoader()

    def tearDown(self) -> None:
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_load_yaml_config(self) -> None:
        yaml_path = os.path.join(self.temp_dir, "strategy.yaml")
        with open(yaml_path, "w", encoding="utf-8") as f:
            f.write("fast_window: 10\nslow_window: 30\nvolatility_cap: 0.5\n")

        config = self.loader.load(yaml_path)

        self.assertEqual(config["fast_window"], 10)
        self.assertEqual(config["slow_window"], 30)
        self.assertEqual(config["volatility_cap"], 0.5)

    def test_load_toml_config(self) -> None:
        toml_path = os.path.join(self.temp_dir, "strategy.toml")
        with open(toml_path, "w", encoding="utf-8") as f:
            f.write('fast_window = 10\nslow_window = 30\nvolatility_cap = 0.5\n')

        config = self.loader.load(toml_path)

        self.assertEqual(config["fast_window"], 10)
        self.assertEqual(config["slow_window"], 30)
        self.assertEqual(config["volatility_cap"], 0.5)

    def test_extract_params_from_config(self) -> None:
        config = {
            "strategy_id": "my_strategy",
            "params": {
                "fast_window": 10,
                "slow_window": 30,
            },
        }

        params = self.loader.extract_params(config)

        self.assertEqual(params["fast_window"], 10)
        self.assertEqual(params["slow_window"], 30)

    def test_clear_cache(self) -> None:
        yaml_path = os.path.join(self.temp_dir, "strategy.yaml")
        with open(yaml_path, "w", encoding="utf-8") as f:
            f.write("fast_window: 10\n")

        self.loader.load(yaml_path)
        self.assertEqual(len(self.loader._cache), 1)

        self.loader.clear_cache()
        self.assertEqual(len(self.loader._cache), 0)


if __name__ == "__main__":
    unittest.main()
