from __future__ import annotations

import unittest

from quant_exchange.core.models import Action, Role
from quant_exchange.security import SecurityService


class SecurityTests(unittest.TestCase):
    def test_se_01_role_permissions_are_enforced(self) -> None:
        security = SecurityService()
        self.assertTrue(security.authorize(Role.ADMIN, Action.SUBMIT_ORDER))
        self.assertFalse(security.authorize(Role.VIEWER, Action.SUBMIT_ORDER))

    def test_se_02_audit_log_is_recorded(self) -> None:
        security = SecurityService()
        event = security.record_event("alice", Action.RUN_BACKTEST, "strategy:ma_sentiment", True, run_id="run_1")
        self.assertEqual(len(security.audit_log), 1)
        self.assertEqual(event.details["run_id"], "run_1")


if __name__ == "__main__":
    unittest.main()
