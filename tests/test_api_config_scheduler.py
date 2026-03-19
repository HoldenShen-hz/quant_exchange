from __future__ import annotations

import os
import tempfile
import unittest

from quant_exchange.config import AppSettings
from quant_exchange.platform import QuantTradingPlatform


class ApiConfigSchedulerTests(unittest.TestCase):
    def test_cfg_01_settings_load_from_mapping(self) -> None:
        settings = AppSettings.from_mapping(
            {
                "database": {"url": ":memory:"},
                "api": {"base_path": "/api/v99"},
                "scheduler": {"default_interval_seconds": 15},
                "adapters": {"default_exchange_code": "SIM_CRYPTO"},
            }
        )
        self.assertEqual(settings.api.base_path, "/api/v99")
        self.assertEqual(settings.scheduler.default_interval_seconds, 15)
        self.assertEqual(settings.adapters.default_exchange_code, "SIM_CRYPTO")

    def test_api_01_login_sync_and_backtest_flow(self) -> None:
        platform = QuantTradingPlatform(AppSettings.from_mapping({"database": {"url": ":memory:"}}))
        self.addCleanup(platform.close)
        api = platform.api
        api.create_user("alice", "secret", role=__import__("quant_exchange.core.models", fromlist=["Role"]).Role.ADMIN)
        login = api.login("alice", "secret")
        self.assertEqual(login["code"], "OK")
        api.create_exchange("SIM_CRYPTO", "Sim Crypto", "CRYPTO")
        instruments = api.sync_instruments("SIM_CRYPTO")
        self.assertEqual(instruments["code"], "OK")
        api.sync_klines("SIM_CRYPTO", "BTCUSDT", "1d")
        api.create_strategy("ma_sentiment", "MA Sentiment", "trend")
        backtest = api.run_backtest("ma_sentiment", "BTCUSDT", "1d")
        self.assertEqual(backtest["code"], "OK")
        self.assertGreaterEqual(backtest["data"]["order_count"], 0)

    def test_api_02_scheduler_job_is_registered_and_run(self) -> None:
        platform = QuantTradingPlatform(AppSettings.from_mapping({"database": {"url": ":memory:"}}))
        self.addCleanup(platform.close)
        api = platform.api
        called = []

        def callback(payload):
            called.append(payload)
            return {"synced": True}

        api.register_job("sync_markets", "Sync Markets", "DATA_SYNC", 0, callback)
        result = api.run_jobs()
        self.assertEqual(result["code"], "OK")
        self.assertEqual(len(called), 1)
        self.assertEqual(platform.persistence.count("ops_scheduled_jobs"), 1)
        self.assertEqual(platform.persistence.count("ops_job_runs"), 1)


if __name__ == "__main__":
    unittest.main()
