"""Tests for enhanced risk control features.

Tests:
- Market interruption auto-stop
- Margin warning and critical alerts
- Duplicate signal detection
"""

from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from quant_exchange.core.models import (
    Alert,
    AlertSeverity,
    OrderRequest,
    OrderSide,
    OrderType,
    PortfolioSnapshot,
    RiskDecision,
    RiskLimits,
)
from quant_exchange.risk import RiskEngine
from quant_exchange.risk.service import (
    InstrumentRiskFilter,
    InstrumentRiskState,
    RiskAuditLogger,
    RiskAuditEntry,
    RiskReasonCode,
)


def utc_now() -> datetime:
    """Return a timezone-aware UTC timestamp."""
    return datetime.now(timezone.utc)


class MarketInterruptionTests(unittest.TestCase):
    """Test market interruption auto-stop functionality."""

    def setUp(self) -> None:
        self.engine = RiskEngine()

    def test_mark_market_interrupted_triggers_auto_stop(self) -> None:
        """Verify marking market as interrupted triggers kill switch."""
        self.assertFalse(self.engine.kill_switch_active)

        triggered = self.engine.mark_market_interrupted(
            interrupted=True,
            reason="Exchange API down",
        )

        self.assertTrue(triggered)
        self.assertTrue(self.engine.kill_switch_active)
        self.assertTrue(self.engine.market_interruption_state.auto_stop_triggered)

    def test_mark_market_healthy_releases_kill_switch(self) -> None:
        """Verify marking market as healthy clears interruption but doesn't auto-release kill switch."""
        self.engine.mark_market_interrupted(interrupted=True, reason="API down")
        self.assertTrue(self.engine.kill_switch_active)

        self.engine.mark_market_interrupted(interrupted=False)
        # Market interruption is cleared
        self.assertFalse(self.engine.market_interruption_state.isInterrupted)
        # But kill switch remains active - must be manually released
        self.assertTrue(self.engine.kill_switch_active)

        self.engine.release_kill_switch()
        self.assertFalse(self.engine.kill_switch_active)

    def test_check_market_interruption_auto_stop_detects_stale_data(self) -> None:
        """Verify auto-stop is triggered when no market data received."""
        old_time = utc_now() - timedelta(minutes=10)
        self.engine.update_market_health(old_time)

        triggered = self.engine.check_market_interruption_auto_stop()

        self.assertTrue(triggered)
        self.assertTrue(self.engine.kill_switch_active)

    def test_update_market_health_clears_interruption_and_releases_kill_switch(self) -> None:
        """Verify updating market health clears interruption and releases kill switch if active."""
        self.engine.mark_market_interrupted(interrupted=True, reason="API down")
        self.assertTrue(self.engine.market_interruption_state.isInterrupted)
        self.assertTrue(self.engine.kill_switch_active)

        self.engine.update_market_health()

        # Both interruption state and kill switch should be cleared
        self.assertFalse(self.engine.market_interruption_state.isInterrupted)
        self.assertFalse(self.engine.market_interruption_state.auto_stop_triggered)
        self.assertFalse(self.engine.kill_switch_active)


class MarginWarningTests(unittest.TestCase):
    """Test margin warning and critical alert functionality."""

    def setUp(self) -> None:
        self.engine = RiskEngine()

    def test_margin_warning_triggered_at_warning_ratio(self) -> None:
        """Verify warning is triggered when margin ratio hits warning level."""
        # margin_warning_ratio default is 0.8, so 0.85 should trigger warning
        is_warned, is_critical = self.engine.check_margin_warning(
            instrument_id="BTCUSDT",
            margin_ratio=0.85,
            position_value=10000.0,
        )

        self.assertTrue(is_warned)
        self.assertFalse(is_critical)
        self.assertEqual(len(self.engine.alerts), 1)
        self.assertEqual(self.engine.alerts[0].code, "margin_warning")

    def test_margin_critical_triggered_at_block_ratio(self) -> None:
        """Verify critical alert is triggered when margin ratio hits block level."""
        # margin_block_ratio default is 0.9, so 0.92 should trigger critical
        is_warned, is_critical = self.engine.check_margin_warning(
            instrument_id="BTCUSDT",
            margin_ratio=0.92,
            position_value=10000.0,
        )

        self.assertTrue(is_warned)
        self.assertTrue(is_critical)
        critical_alerts = [a for a in self.engine.alerts if a.code == "margin_critical"]
        self.assertEqual(len(critical_alerts), 1)

    def test_margin_warning_cleared_when_ratio_recovers(self) -> None:
        """Verify warning is cleared when margin ratio recovers."""
        self.engine.check_margin_warning("BTCUSDT", 0.85, 10000.0)
        self.assertTrue(self.engine.margin_warning_states["BTCUSDT"].is_warned)

        is_warned, is_critical = self.engine.check_margin_warning("BTCUSDT", 0.50, 10000.0)

        self.assertFalse(is_warned)
        self.assertFalse(is_critical)


class DuplicateSignalTests(unittest.TestCase):
    """Test duplicate signal detection functionality."""

    def setUp(self) -> None:
        self.engine = RiskEngine()

    def test_duplicate_signal_detected_after_max_repeats(self) -> None:
        """Verify duplicate signal is flagged after max repeats."""
        direction = "long"

        for i in range(4):
            is_dup, count = self.engine.check_duplicate_signal("BTCUSDT", direction)

        self.assertTrue(is_dup)
        self.assertEqual(count, 4)
        # Alerts are generated when count >= max_repeats (3), so count=3 and count=4 both generate alerts
        duplicate_alerts = [a for a in self.engine.alerts if a.code == "duplicate_signal"]
        self.assertEqual(len(duplicate_alerts), 2)

    def test_duplicate_signal_resets_on_direction_change(self) -> None:
        """Verify duplicate count resets when signal direction changes."""
        self.engine.check_duplicate_signal("BTCUSDT", "long")
        self.engine.check_duplicate_signal("BTCUSDT", "long")
        is_dup1, count1 = self.engine.check_duplicate_signal("BTCUSDT", "long")

        self.assertTrue(is_dup1)
        self.assertEqual(count1, 3)

        is_dup2, count2 = self.engine.check_duplicate_signal("BTCUSDT", "short")

        self.assertFalse(is_dup2)
        self.assertEqual(count2, 1)

    def test_reset_duplicate_signal_tracking(self) -> None:
        """Verify duplicate signal tracking can be reset."""
        for _ in range(3):
            self.engine.check_duplicate_signal("BTCUSDT", "long")

        self.engine.reset_duplicate_signal_tracking("BTCUSDT")

        is_dup, count = self.engine.check_duplicate_signal("BTCUSDT", "long")
        self.assertFalse(is_dup)
        self.assertEqual(count, 1)


class EnhancedEvaluateOrderTests(unittest.TestCase):
    """Test enhanced order evaluation with new risk checks."""

    def setUp(self) -> None:
        self.engine = RiskEngine()
        self.snapshot = PortfolioSnapshot(
            timestamp=utc_now(),
            cash=100000.0,
            positions_value=0.0,
            equity=100000.0,
            gross_exposure=0.0,
            net_exposure=0.0,
            leverage=1.0,
            drawdown=0.0,
        )

    def test_order_rejected_when_market_interrupted(self) -> None:
        """Verify order is rejected when market is interrupted."""
        self.engine.mark_market_interrupted(interrupted=True, reason="API down")

        request = OrderRequest(
            client_order_id="test_1",
            instrument_id="BTCUSDT",
            side=OrderSide.BUY,
            quantity=1.0,
            price=50000.0,
        )

        decision = self.engine.evaluate_order(
            request,
            price=50000.0,
            current_position_qty=0.0,
            snapshot=self.snapshot,
        )

        self.assertFalse(decision.approved)
        self.assertIn("market_interrupted", decision.reasons)

    def test_order_with_duplicate_signal_rejected(self) -> None:
        """Verify order is rejected when duplicate signal limit is exceeded."""
        for _ in range(4):
            self.engine.check_duplicate_signal("BTCUSDT", "long")

        request = OrderRequest(
            client_order_id="test_1",
            instrument_id="BTCUSDT",
            side=OrderSide.BUY,
            quantity=1.0,
            price=50000.0,
        )

        decision = self.engine.evaluate_order(
            request,
            price=50000.0,
            current_position_qty=0.0,
            snapshot=self.snapshot,
            check_duplicate_signal=True,
            signal_direction="long",
        )

        self.assertFalse(decision.approved)
        self.assertIn("duplicate_signal_limit", decision.reasons)

    def test_order_with_margin_critical_rejected(self) -> None:
        """Verify order is rejected when margin is at critical level."""
        self.engine.check_margin_warning("BTCUSDT", 0.95, 50000.0)

        request = OrderRequest(
            client_order_id="test_1",
            instrument_id="BTCUSDT",
            side=OrderSide.BUY,
            quantity=1.0,
            price=50000.0,
        )

        decision = self.engine.evaluate_order(
            request,
            price=50000.0,
            current_position_qty=0.0,
            snapshot=self.snapshot,
            margin_ratio=0.95,
        )

        self.assertFalse(decision.approved)
        self.assertIn("margin_critical", decision.reasons)


class KillSwitchAlertTests(unittest.TestCase):
    """Test kill switch alert generation."""

    def setUp(self) -> None:
        self.engine = RiskEngine()

    def test_activate_kill_switch_generates_alert(self) -> None:
        """Verify activating kill switch generates emergency alert."""
        self.engine.activate_kill_switch()

        self.assertEqual(len(self.engine.alerts), 1)
        self.assertEqual(self.engine.alerts[0].code, "kill_switch_activated")
        self.assertEqual(self.engine.alerts[0].severity, AlertSeverity.EMERGENCY)

    def test_release_kill_switch_generates_info_alert(self) -> None:
        """Verify releasing kill switch generates info alert."""
        self.engine.activate_kill_switch()
        self.engine.release_kill_switch()

        alert_codes = [a.code for a in self.engine.alerts]
        self.assertIn("kill_switch_activated", alert_codes)
        self.assertIn("kill_switch_released", alert_codes)


class InstrumentRiskFilterTests(unittest.TestCase):
    """Test RK-03: Instrument-level risk filtering."""

    def setUp(self) -> None:
        self.filter = InstrumentRiskFilter(
            max_volatility=0.30,
            min_volume=1000.0,
            min_liquidity_score=0.15,
        )

    def test_update_instrument_data(self) -> None:
        """Verify instrument data is updated correctly."""
        state = self.filter.update_instrument_data(
            instrument_id="BTCUSDT",
            current_price=50000.0,
            previous_price=49500.0,
            volume=50000.0,
            high=50500.0,
            low=49000.0,
        )

        self.assertIsInstance(state, InstrumentRiskState)
        self.assertEqual(state.instrument_id, "BTCUSDT")
        self.assertTrue(state.current_volatility > 0)
        self.assertTrue(state.is_tradeable)

    def test_volatility_filter_blocks_high_volatility(self) -> None:
        """Verify high volatility instrument is blocked."""
        # 60% volatility should be blocked (exceeds 30% max)
        state = self.filter.update_instrument_data(
            instrument_id="DOGEUSDT",
            current_price=0.10,
            previous_price=0.06,
            volume=100000.0,
        )

        self.assertFalse(state.is_tradeable)
        self.assertIn("volatility", state.block_reason.lower())

    def test_volume_filter_blocks_low_volume(self) -> None:
        """Verify low volume instrument is blocked."""
        # Create filter with higher min volume
        filter_high_min = InstrumentRiskFilter(min_volume=100000.0)

        state = filter_high_min.update_instrument_data(
            instrument_id="SHIBUSDT",
            current_price=0.00001,
            previous_price=0.0000098,
            volume=100.0,  # Very low volume
        )

        self.assertFalse(state.is_tradeable)
        self.assertIn("volume", state.block_reason.lower())

    def test_check_instrument_tradeable(self) -> None:
        """Verify tradeable check works correctly."""
        self.filter.update_instrument_data(
            instrument_id="ETHUSDT",
            current_price=3000.0,
            previous_price=2950.0,
            volume=50000.0,
        )

        is_tradeable, reason = self.filter.check_instrument_tradeable("ETHUSDT")
        self.assertTrue(is_tradeable)

    def test_get_all_blocked_instruments(self) -> None:
        """Verify blocked instruments list is correct."""
        # Add a blocked instrument
        filter_high_min = InstrumentRiskFilter(min_volume=100000.0)
        filter_high_min.update_instrument_data(
            instrument_id="SHIBUSDT",
            current_price=0.00001,
            previous_price=0.0000098,
            volume=100.0,
        )

        blocked = filter_high_min.get_all_blocked_instruments()
        self.assertTrue(len(blocked) >= 1)
        self.assertTrue(any("SHIBUSDT" in item[0] for item in blocked))


class RiskAuditLoggerTests(unittest.TestCase):
    """Test RK-07: Enhanced risk audit trail with reason codes."""

    def setUp(self) -> None:
        self.logger = RiskAuditLogger()

    def test_log_evaluation(self) -> None:
        """Verify evaluation is logged correctly."""
        from quant_exchange.core.models import RiskDecision

        request = OrderRequest(
            client_order_id="test_123",
            instrument_id="BTCUSDT",
            side=OrderSide.BUY,
            quantity=1.0,
            price=50000.0,
            strategy_id="test_strategy",
        )

        snapshot = PortfolioSnapshot(
            timestamp=utc_now(),
            cash=100000.0,
            positions_value=0.0,
            equity=100000.0,
            gross_exposure=0.0,
            net_exposure=0.0,
            leverage=1.0,
            drawdown=0.0,
        )

        decision = RiskDecision(approved=False, reasons=("kill_switch_active",))
        entry = self.logger.log_evaluation(decision, request, 50000.0, snapshot)

        self.assertIsInstance(entry, RiskAuditEntry)
        self.assertEqual(entry.decision, "rejected")
        self.assertEqual(entry.primary_reason_code, RiskReasonCode.S_KILL_SWITCH)

    def test_get_rejection_summary(self) -> None:
        """Verify rejection summary is computed correctly."""
        from quant_exchange.core.models import RiskDecision

        request = OrderRequest(
            client_order_id="test_456",
            instrument_id="ETHUSDT",
            side=OrderSide.BUY,
            quantity=1.0,
            price=3000.0,
            strategy_id="test_strategy",
        )

        snapshot = PortfolioSnapshot(
            timestamp=utc_now(),
            cash=100000.0,
            positions_value=0.0,
            equity=100000.0,
            gross_exposure=0.0,
            net_exposure=0.0,
            leverage=1.0,
            drawdown=0.0,
        )

        # Log a rejection
        decision = RiskDecision(approved=False, reasons=("leverage_limit",))
        self.logger.log_evaluation(decision, request, 3000.0, snapshot)

        summary = self.logger.get_rejection_summary()

        self.assertEqual(summary.total_evaluations, 1)
        self.assertEqual(summary.total_rejections, 1)
        self.assertIn(RiskReasonCode.A_LEVERAGE_EXCEEDED, summary.rejections_by_code)

    def test_export_audit_csv(self) -> None:
        """Verify CSV export works."""
        from quant_exchange.core.models import RiskDecision

        request = OrderRequest(
            client_order_id="test_csv",
            instrument_id="BTCUSDT",
            side=OrderSide.BUY,
            quantity=1.0,
            price=50000.0,
            strategy_id="test",
        )

        snapshot = PortfolioSnapshot(
            timestamp=utc_now(),
            cash=100000.0,
            positions_value=0.0,
            equity=100000.0,
            gross_exposure=0.0,
            net_exposure=0.0,
            leverage=1.0,
            drawdown=0.0,
        )

        decision = RiskDecision(approved=False, reasons=("market_interrupted",))
        self.logger.log_evaluation(decision, request, 50000.0, snapshot)

        csv_output = self.logger.export_audit_csv()
        self.assertIn("entry_id", csv_output)
        self.assertIn("BTCUSDT", csv_output)
        self.assertIn(RiskReasonCode.S_MARKET_INTERRUPTED, csv_output)


if __name__ == "__main__":
    unittest.main()
