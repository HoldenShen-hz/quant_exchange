"""Tests for HOOK-01~HOOK-05 webhook automation service."""

import unittest
from datetime import datetime, timezone

from quant_exchange.webhooks import (
    OutboundWebhookService,
    WebhookAction,
    WebhookActionType,
    WebhookDeliveryStatus,
    WebhookService,
    WebhookTrigger,
    WebhookTriggerType,
    WebhookWorkflow,
)


class TestWebhookService(unittest.TestCase):
    """Tests for WebhookService inbound processing and workflow automation."""

    def setUp(self) -> None:
        self.svc = WebhookService()

    def test_receive_webhook_no_secret(self) -> None:
        """Test receiving a webhook without HMAC verification."""
        event = self.svc.receive_webhook(
            trigger_type=WebhookTriggerType.CUSTOM,
            payload={"alert": "price_spike", "symbol": "BTCUSDT", "price": 50000},
        )
        self.assertIsNotNone(event)
        self.assertEqual(event.verified, True)  # No secret = trusted
        self.assertEqual(event.trigger_type, WebhookTriggerType.CUSTOM)
        print(f"\n[HOOK] Webhook received: {event.event_id}, verified={event.verified}")

    def test_receive_webhook_with_valid_signature(self) -> None:
        """Test receiving a webhook with valid HMAC signature (HOOK-03)."""
        secret = "my_webhook_secret"
        payload = {"alert": "price_spike", "symbol": "ETHUSDT", "price": 3000}
        signature = self.svc.generate_signature(payload, secret)

        event = self.svc.receive_webhook(
            trigger_type=WebhookTriggerType.CUSTOM,
            payload=payload,
            headers={"X-Signature": signature},
            secret=secret,
        )
        self.assertTrue(event.verified)
        print(f"\n[HOOK] Valid signature verified: {event.verified}")

    def test_receive_webhook_with_invalid_signature(self) -> None:
        """Test that invalid HMAC signatures are rejected (HOOK-03)."""
        secret = "my_webhook_secret"
        payload = {"alert": "price_spike", "symbol": "ETHUSDT", "price": 3000}

        event = self.svc.receive_webhook(
            trigger_type=WebhookTriggerType.CUSTOM,
            payload=payload,
            headers={"X-Signature": "invalid_signature_here"},
            secret=secret,
        )
        self.assertFalse(event.verified)
        print(f"\n[HOOK] Invalid signature rejected: {event.verified}")

    def test_create_workflow(self) -> None:
        """Test creating a webhook workflow (HOOK-04)."""
        workflow = self.svc.create_workflow(
            name="Price Alert Workflow",
            description="Notify on significant price moves",
            triggers=[
                {
                    "name": "Price Spike Alert",
                    "trigger_type": "custom",
                    "conditions": {"alert": "price_spike"},
                }
            ],
            actions=[
                {
                    "name": "Send HTTP POST",
                    "action_type": "http_post",
                    "config": {"url": "https://example.com/webhook", "retry_count": 3},
                    "order": 0,
                }
            ],
        )
        self.assertIsNotNone(workflow)
        self.assertEqual(workflow.name, "Price Alert Workflow")
        self.assertEqual(len(workflow.triggers), 1)
        self.assertEqual(len(workflow.actions), 1)
        self.assertTrue(workflow.enabled)
        print(f"\n[HOOK] Workflow created: {workflow.workflow_id}")

    def test_workflow_triggered_by_event(self) -> None:
        """Test that a workflow is triggered by a matching event."""
        workflow = self.svc.create_workflow(
            name="Risk Alert Workflow",
            triggers=[
                {
                    "name": "Risk Alert",
                    "trigger_type": "risk_alert",
                    "conditions": {"severity": "high"},
                }
            ],
            actions=[
                {
                    "name": "Log Alert",
                    "action_type": "send_alert",
                    "config": {"channel": "log", "template": "Risk alert!"},
                    "order": 0,
                }
            ],
        )

        event = self.svc.receive_webhook(
            trigger_type=WebhookTriggerType.RISK_ALERT,
            payload={"severity": "high", "message": "VaR breach"},
        )
        self.assertTrue(event.verified)
        self.assertEqual(workflow.execution_count, 1)
        print(f"\n[HOOK] Workflow triggered: count={workflow.execution_count}")

    def test_workflow_conditions_filtering(self) -> None:
        """Test that workflow conditions correctly filter events."""
        workflow = self.svc.create_workflow(
            name="BTC Only Workflow",
            triggers=[
                {
                    "name": "BTC Alert",
                    "trigger_type": "custom",
                    "conditions": {"symbol": "BTCUSDT"},
                }
            ],
            actions=[
                {
                    "name": "Notify",
                    "action_type": "send_alert",
                    "config": {"channel": "log"},
                    "order": 0,
                }
            ],
        )

        # Matching event
        self.svc.receive_webhook(
            trigger_type=WebhookTriggerType.CUSTOM,
            payload={"symbol": "BTCUSDT", "alert": "price_change"},
        )
        self.assertEqual(workflow.execution_count, 1)

        # Non-matching event (different symbol)
        self.svc.receive_webhook(
            trigger_type=WebhookTriggerType.CUSTOM,
            payload={"symbol": "ETHUSDT", "alert": "price_change"},
        )
        # Should still be 1 since ETH doesn't match BTC condition
        self.assertEqual(workflow.execution_count, 1)
        print(f"\n[HOOK] Condition filtering works: count={workflow.execution_count}")

    def test_list_workflows(self) -> None:
        """Test listing workflows."""
        self.svc.create_workflow(name="Workflow 1")
        self.svc.create_workflow(name="Workflow 2")

        all_workflows = self.svc.list_workflows()
        self.assertGreaterEqual(len(all_workflows), 2)

        enabled_workflows = self.svc.list_workflows(enabled=True)
        self.assertGreaterEqual(len(enabled_workflows), 2)
        print(f"\n[HOOK] Workflows listed: {len(all_workflows)}")

    def test_delete_workflow(self) -> None:
        """Test deleting a workflow."""
        workflow = self.svc.create_workflow(name="To Delete")
        workflow_id = workflow.workflow_id

        deleted = self.svc.delete_workflow(workflow_id)
        self.assertTrue(deleted)

        retrieved = self.svc.get_workflow(workflow_id)
        self.assertIsNone(retrieved)
        print(f"\n[HOOK] Workflow deleted: {workflow_id}")

    def test_webhook_signature_generation(self) -> None:
        """Test HMAC signature generation (HOOK-03)."""
        secret = "test_secret"
        payload = {"key": "value", "number": 42}
        sig = self.svc.generate_signature(payload, secret)
        self.assertIsInstance(sig, str)
        self.assertEqual(len(sig), 64)  # SHA256 hex = 64 chars
        print(f"\n[HOOK] Signature generated: {sig[:16]}...")


class TestOutboundWebhookService(unittest.TestCase):
    """Tests for OutboundWebhookService delivery."""

    def setUp(self) -> None:
        self.svc = OutboundWebhookService()

    def test_enqueue_delivery(self) -> None:
        """Test enqueuing an outbound webhook (HOOK-02)."""
        delivery = self.svc.enqueue(
            url="https://example.com/hook",
            payload={"event": "test", "value": 123},
            secret="secret123",
            method="POST",
        )
        self.assertIsNotNone(delivery)
        self.assertEqual(delivery.status, WebhookDeliveryStatus.PENDING)
        self.assertIn("X-Signature", delivery.headers)
        print(f"\n[HOOK] Delivery enqueued: {delivery.delivery_id}")

    def test_signature_added_when_secret_provided(self) -> None:
        """Test that HMAC signature is added when secret is provided (HOOK-03)."""
        delivery = self.svc.enqueue(
            url="https://example.com/webhook",
            payload={"data": "test"},
            secret="my_secret",
        )
        self.assertIn("X-Signature", delivery.headers)
        self.assertNotEqual(delivery.headers["X-Signature"], "")

    def test_no_signature_without_secret(self) -> None:
        """Test that no signature is added when secret is empty."""
        delivery = self.svc.enqueue(
            url="https://example.com/webhook",
            payload={"data": "test"},
        )
        self.assertNotIn("X-Signature", delivery.headers)

    def test_list_recent_deliveries(self) -> None:
        """Test listing recent deliveries."""
        self.svc.enqueue(url="https://example.com/1", payload={})
        self.svc.enqueue(url="https://example.com/2", payload={})

        recent = self.svc.list_recent(limit=10)
        self.assertGreaterEqual(len(recent), 2)
        print(f"\n[HOOK] Recent deliveries: {len(recent)}")


class TestWebhookServiceIntegration(unittest.TestCase):
    """Integration tests for webhook API endpoints."""

    def setUp(self) -> None:
        from quant_exchange.platform import QuantTradingPlatform
        from quant_exchange.config import AppSettings
        import tempfile
        from pathlib import Path

        self.temp_dir = tempfile.TemporaryDirectory()
        db_path = str(Path(self.temp_dir.name) / "hook_test.sqlite3")
        self.platform = QuantTradingPlatform(
            AppSettings.from_mapping({"database": {"url": db_path}})
        )

    def tearDown(self) -> None:
        self.platform.close()
        self.temp_dir.cleanup()

    def test_webhook_create_workflow_api(self) -> None:
        """Test webhook workflow creation API."""
        result = self.platform.api.webhook_create_workflow(
            name="Test Workflow",
            description="API test workflow",
            triggers=[{"name": "Test Trigger", "trigger_type": "custom", "conditions": {}}],
            actions=[{"name": "Test Action", "action_type": "http_post", "config": {"url": "https://test.com"}, "order": 0}],
        )
        self.assertEqual(result["code"], "OK")
        self.assertIn("workflow_id", result["data"])
        print(f"\n[HOOK API] Workflow created: {result['data']['workflow_id']}")

    def test_webhook_list_workflows_api(self) -> None:
        """Test listing webhook workflows."""
        self.platform.api.webhook_create_workflow(name="WF1")
        self.platform.api.webhook_create_workflow(name="WF2")

        result = self.platform.api.webhook_list_workflows()
        self.assertEqual(result["code"], "OK")
        self.assertGreaterEqual(len(result["data"]["workflows"]), 2)
        print(f"\n[HOOK API] Workflows: {len(result['data']['workflows'])}")

    def test_webhook_receive_api(self) -> None:
        """Test receiving an inbound webhook via API."""
        result = self.platform.api.webhook_receive(
            trigger_type="custom",
            payload={"alert": "test_alert", "symbol": "BTCUSDT"},
        )
        self.assertEqual(result["code"], "OK")
        self.assertIn("event_id", result["data"])
        self.assertTrue(result["data"]["verified"])
        print(f"\n[HOOK API] Webhook received: {result['data']['event_id']}")

    def test_webhook_send_api(self) -> None:
        """Test sending an outbound webhook via API."""
        result = self.platform.api.webhook_send(
            url="https://httpbin.org/post",
            payload={"test": "data", "timestamp": "2024-01-01T00:00:00Z"},
            method="POST",
        )
        self.assertEqual(result["code"], "OK")
        self.assertIn("delivery_id", result["data"])
        print(f"\n[HOOK API] Webhook sent: {result['data']['delivery_id']}")


if __name__ == "__main__":
    unittest.main(verbosity=2)
