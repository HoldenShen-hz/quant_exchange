from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from quant_exchange.config import AppSettings
from quant_exchange.platform import QuantTradingPlatform


class StrategyBotCenterTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        database_path = str(Path(self.temp_dir.name) / "bot-center.sqlite3")
        self.platform = QuantTradingPlatform(AppSettings.from_mapping({"database": {"url": database_path}}))

    def tearDown(self) -> None:
        self.platform.close()
        self.temp_dir.cleanup()

    def test_bot_templates_can_create_start_and_refresh_runtime(self) -> None:
        templates = self.platform.api.list_strategy_templates()
        self.assertEqual(templates["code"], "OK")
        self.assertGreaterEqual(len(templates["data"]), 3)

        created = self.platform.api.create_strategy_bot(
            template_code="ma_sentiment",
            instrument_id="600519.SH",
            bot_name="贵州茅台趋势机器人",
        )
        self.assertEqual(created["code"], "OK")
        bot_id = created["data"]["bot_id"]
        self.assertEqual(created["data"]["status"], "draft")

        started = self.platform.api.start_strategy_bot(bot_id)
        self.assertEqual(started["code"], "OK")
        self.assertEqual(started["data"]["status"], "running")
        self.assertIsNotNone(started["data"]["metrics"]["heartbeat_at"])
        self.assertIn("target_weight", started["data"]["last_signal"])

        listed = self.platform.api.list_strategy_bots()
        self.assertEqual(listed["code"], "OK")
        self.assertEqual(len(listed["data"]), 1)
        self.assertEqual(listed["data"][0]["bot_id"], bot_id)

    def test_bot_interactions_normalize_params_and_emit_notifications(self) -> None:
        created = self.platform.api.create_strategy_bot(
            template_code="ma_breakout",
            instrument_id="MSFT.US",
            bot_name="Microsoft Breakout",
        )
        self.assertEqual(created["code"], "OK")
        bot_id = created["data"]["bot_id"]

        updated = self.platform.api.interact_strategy_bot(
            bot_id,
            "set_param",
            {"updates": {"fast_window": "4", "slow_window": "9", "max_weight": "0.66"}},
        )
        self.assertEqual(updated["code"], "OK")
        self.assertEqual(updated["data"]["params"]["fast_window"], 4)
        self.assertEqual(updated["data"]["params"]["slow_window"], 9)
        self.assertAlmostEqual(updated["data"]["params"]["max_weight"], 0.66)

        liquidated = self.platform.api.interact_strategy_bot(bot_id, "liquidate")
        self.assertEqual(liquidated["code"], "OK")
        self.assertEqual(liquidated["data"]["metrics"]["signal_weight"], 0.0)
        self.assertEqual(liquidated["data"]["metrics"]["signal_reason"], "manual_liquidate")

        notifications = self.platform.api.list_strategy_notifications(limit=10)
        self.assertEqual(notifications["code"], "OK")
        self.assertGreaterEqual(len(notifications["data"]["notifications"]), 3)
        self.assertTrue(
            any(item["event_type"] == "bot_command_set_param" for item in notifications["data"]["notifications"])
        )
        self.assertTrue(
            any(item["event_type"] == "bot_command_liquidate" for item in notifications["data"]["notifications"])
        )

    def test_invalid_bot_param_payload_returns_bad_request(self) -> None:
        created = self.platform.api.create_strategy_bot(
            template_code="ma_defensive",
            instrument_id="0700.HK",
            bot_name="Tencent Defensive",
        )
        self.assertEqual(created["code"], "OK")
        bot_id = created["data"]["bot_id"]

        invalid = self.platform.api.interact_strategy_bot(
            bot_id,
            "set_param",
            {"updates": {"fast_window": "invalid-number"}},
        )
        self.assertEqual(invalid["code"], "BAD_REQUEST")
        self.assertIn("invalid_param:fast_window", invalid["error"]["message"])


if __name__ == "__main__":
    unittest.main()
