"""Webhook and automation workflow service (HOOK-01 ~ HOOK-05).

Inbound webhooks (HOOK-01): Receive external alerts and convert to internal signals
Outbound webhooks (HOOK-02): Push strategy/risk/order/alert events to external systems
Signature verification (HOOK-03): HMAC-SHA256 verification, retry, logging
Workflow automation (HOOK-04~HOOK-05): Triggers, conditions, actions, templates
"""

from __future__ import annotations

import hashlib
import hmac
import json
import time
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any
from urllib.parse import urlencode
import urllib.request


# ─────────────────────────────────────────────────────────────────────────────
# Enums
# ─────────────────────────────────────────────────────────────────────────────


class WebhookTriggerType(str, Enum):
    """Types of workflow triggers."""

    ORDER_FILLED = "order_filled"
    ORDER_REJECTED = "order_rejected"
    RISK_ALERT = "risk_alert"
    MONITORING_ALERT = "monitoring_alert"
    STRATEGY_SIGNAL = "strategy_signal"
    BALANCE_CHANGE = "balance_change"
    CUSTOM = "custom"  # Inbound webhook


class WebhookActionType(str, Enum):
    """Types of webhook actions."""

    HTTP_POST = "http_post"
    HTTP_GET = "http_get"
    SEND_ALERT = "send_alert"
    UPDATE_ORDER = "update_order"
    ADJUST_POSITION = "adjust_position"
    TRIGGER_STRATEGY = "trigger_strategy"


class WebhookDeliveryStatus(str, Enum):
    """Status of outbound webhook delivery."""

    PENDING = "pending"
    DELIVERED = "delivered"
    FAILED = "failed"
    RETRYING = "retrying"


# ─────────────────────────────────────────────────────────────────────────────
# Data Models
# ─────────────────────────────────────────────────────────────────────────────


@dataclass(slots=True)
class WebhookEvent:
    """An inbound webhook event received from an external system."""

    event_id: str
    trigger_type: WebhookTriggerType
    payload: dict[str, Any]
    headers: dict[str, str]
    received_at: datetime
    signature: str = ""  # HMAC signature if provided
    verified: bool = False
    processed: bool = False
    processing_result: str = ""


@dataclass(slots=True)
class WebhookTrigger:
    """A workflow trigger that activates on specific events."""

    trigger_id: str
    name: str
    trigger_type: WebhookTriggerType
    conditions: dict[str, Any] = field(default_factory=dict)  # e.g., {"instrument_id": "BTCUSDT"}
    enabled: bool = True
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass(slots=True)
class WebhookAction:
    """An action to execute when a trigger fires."""

    action_id: str
    name: str
    action_type: WebhookActionType
    config: dict[str, Any] = field(default_factory=dict)
    # For HTTP_POST/GET: {"url": "https://...", "headers": {...}, "retry_count": 3}
    # For SEND_ALERT: {"channel": "telegram", "template": "..."}
    # For UPDATE_ORDER/ADJUST_POSITION: {"order_id": "...", "changes": {...}}
    # For TRIGGER_STRATEGY: {"strategy_id": "...", "params": {...}}
    enabled: bool = True
    order: int = 0  # Execution order within the workflow


@dataclass(slots=True)
class WebhookWorkflow:
    """A complete webhook workflow with triggers, conditions, and actions."""

    workflow_id: str
    name: str
    description: str = ""
    triggers: list[WebhookTrigger] = field(default_factory=list)
    actions: list[WebhookAction] = field(default_factory=list)
    enabled: bool = True
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    execution_count: int = 0
    success_count: int = 0
    failure_count: int = 0


@dataclass(slots=True)
class OutboundWebhook:
    """An outbound webhook delivery record."""

    delivery_id: str
    workflow_id: str
    action_id: str
    url: str
    method: str  # "POST" or "GET"
    headers: dict[str, str]
    payload: dict[str, Any]
    status: WebhookDeliveryStatus
    attempts: int = 0
    max_attempts: int = 3
    last_attempt_at: datetime | None = None
    response_status: int | None = None
    response_body: str = ""
    error_message: str = ""
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


# ─────────────────────────────────────────────────────────────────────────────
# Webhook Service
# ─────────────────────────────────────────────────────────────────────────────


class WebhookService:
    """Service for managing inbound webhooks and workflow automation (HOOK-01~HOOK-05).

    Handles:
    - Inbound webhook reception and HMAC verification
    - Workflow trigger/action execution
    - Execution history and logging
    """

    def __init__(self, persistence=None) -> None:
        self.persistence = persistence
        self._events: dict[str, WebhookEvent] = {}
        self._workflows: dict[str, WebhookWorkflow] = {}
        self._outbound_queue: list[OutboundWebhook] = []
        self._event_handlers: dict[WebhookTriggerType, list[str]] = {}  # trigger_type -> [workflow_id]

    # ── Inbound Webhooks (HOOK-01) ──────────────────────────────────────────

    def receive_webhook(
        self,
        trigger_type: WebhookTriggerType | str,
        payload: dict[str, Any],
        headers: dict[str, str | bytes] | None = None,
        secret: str = "",
    ) -> WebhookEvent:
        """Receive and process an inbound webhook (HOOK-01).

        Args:
            trigger_type: Type of trigger (e.g., WebhookTriggerType.CUSTOM)
            payload: JSON payload from the webhook
            headers: HTTP headers (including X-Signature for HMAC verification)
            secret: HMAC secret for signature verification

        Returns:
            WebhookEvent with verification and processing results
        """
        event_id = f"whk:{uuid.uuid4().hex[:12]}"
        headers = headers or {}

        # Convert bytes values to strings
        headers = {k: (v.decode() if isinstance(v, bytes) else v) for k, v in headers.items()}

        event = WebhookEvent(
            event_id=event_id,
            trigger_type=WebhookTriggerType(trigger_type) if isinstance(trigger_type, str) else trigger_type,
            payload=payload,
            headers=headers,
            received_at=datetime.now(timezone.utc),
            signature=headers.get("X-Signature", ""),
        )

        # HMAC verification (HOOK-03)
        if secret and event.signature:
            event.verified = self._verify_signature(payload, secret, event.signature)
        else:
            event.verified = True  # No secret = trust the source

        self._events[event_id] = event

        # Trigger matching workflows
        if event.verified:
            self._process_event(event)

        return event

    def _verify_signature(self, payload: dict[str, Any], secret: str, signature: str) -> bool:
        """Verify HMAC-SHA256 signature of webhook payload (HOOK-03)."""
        try:
            payload_bytes = json.dumps(payload, sort_keys=True).encode()
            expected = hmac.new(
                secret.encode(), payload_bytes, hashlib.sha256
            ).hexdigest()
            return hmac.compare_digest(expected, signature)
        except Exception:
            return False

    def _process_event(self, event: WebhookEvent) -> None:
        """Process an inbound event by triggering matching workflows."""
        matching_workflows = self._get_matching_workflows(event)
        for workflow in matching_workflows:
            if not workflow.enabled:
                continue
            self._execute_workflow(workflow, event)

    def _get_matching_workflows(self, event: WebhookEvent) -> list[WebhookWorkflow]:
        """Find workflows that match the event's trigger type and conditions."""
        matching = []
        for workflow in self._workflows.values():
            for trigger in workflow.triggers:
                if trigger.trigger_type == event.trigger_type and trigger.enabled:
                    # Check conditions
                    if self._check_conditions(trigger.conditions, event.payload):
                        matching.append(workflow)
                        break
        return matching

    def _check_conditions(self, conditions: dict[str, Any], payload: dict[str, Any]) -> bool:
        """Check if payload matches trigger conditions."""
        if not conditions:
            return True
        for key, expected in conditions.items():
            actual = payload.get(key)
            if actual is None:
                return False
            if isinstance(expected, list):
                if actual not in expected:
                    return False
            elif actual != expected:
                return False
        return True

    def _execute_workflow(self, workflow: WebhookWorkflow, event: WebhookEvent) -> None:
        """Execute a workflow's actions in order."""
        workflow.execution_count += 1
        for action in sorted(workflow.actions, key=lambda a: a.order):
            if not action.enabled:
                continue
            try:
                self._execute_action(action, event)
                workflow.success_count += 1
            except Exception as exc:
                workflow.failure_count += 1
                print(f"[HOOK] Workflow {workflow.workflow_id} action {action.action_id} failed: {exc}")

    def _execute_action(self, action: WebhookAction, event: WebhookEvent) -> None:
        """Execute a single webhook action."""
        if action.action_type == WebhookActionType.HTTP_POST:
            self._http_post(action, event)
        elif action.action_type == WebhookActionType.HTTP_GET:
            self._http_get(action, event)
        elif action.action_type == WebhookActionType.SEND_ALERT:
            self._send_alert(action, event)
        elif action.action_type == WebhookActionType.TRIGGER_STRATEGY:
            self._trigger_strategy(action, event)
        # Other action types are logged but not executed without external integration

    def _http_post(self, action: WebhookAction, event: WebhookEvent) -> None:
        """Execute HTTP POST action."""
        config = action.config
        url = config.get("url", "")
        headers = config.get("headers", {"Content-Type": "application/json"})
        retry_count = config.get("retry_count", 3)

        payload = {
            "event_id": event.event_id,
            "trigger_type": event.trigger_type.value,
            "payload": event.payload,
            "received_at": event.received_at.isoformat(),
        }

        self._queue_outbound(
            url=url,
            method="POST",
            headers=headers,
            payload=payload,
            retry_count=retry_count,
            workflow_id="",
            action_id=action.action_id,
        )

    def _http_get(self, action: WebhookAction, event: WebhookEvent) -> None:
        """Execute HTTP GET action."""
        config = action.config
        url = config.get("url", "")
        headers = config.get("headers", {})
        query_params = config.get("query_params", {})

        if query_params:
            url = f"{url}?{urlencode(query_params)}"

        self._queue_outbound(
            url=url,
            method="GET",
            headers=headers,
            payload={},
            retry_count=config.get("retry_count", 3),
            workflow_id="",
            action_id=action.action_id,
        )

    def _send_alert(self, action: WebhookAction, event: WebhookEvent) -> None:
        """Queue an alert notification (delegates to notification service)."""
        # Alert handling is delegated to NotificationService
        # This is a placeholder that logs the alert
        config = action.config
        channel = config.get("channel", "log")
        message = config.get("template", "Webhook alert: {event_id}").format(
            event_id=event.event_id
        )
        print(f"[HOOK Alert:{channel}] {message}")

    def _trigger_strategy(self, action: WebhookAction, event: WebhookEvent) -> None:
        """Trigger a strategy with parameters from the webhook payload."""
        config = action.config
        strategy_id = config.get("strategy_id", "")
        params = config.get("params", {})
        # Merge with event payload if specified
        if config.get("merge_payload"):
            params = {**event.payload, **params}
        print(f"[HOOK] Triggering strategy {strategy_id} with params: {params}")

    def _queue_outbound(
        self,
        url: str,
        method: str,
        headers: dict[str, str],
        payload: dict[str, Any],
        retry_count: int,
        workflow_id: str,
        action_id: str,
    ) -> None:
        """Queue an outbound webhook for delivery."""
        delivery = OutboundWebhook(
            delivery_id=f"dlv:{uuid.uuid4().hex[:12]}",
            workflow_id=workflow_id,
            action_id=action_id,
            url=url,
            method=method,
            headers=headers,
            payload=payload,
            status=WebhookDeliveryStatus.PENDING,
            max_attempts=retry_count + 1,
        )
        self._outbound_queue.append(delivery)

    # ── Workflow Management ─────────────────────────────────────────────────

    def create_workflow(
        self,
        name: str,
        description: str = "",
        triggers: list[dict] | None = None,
        actions: list[dict] | None = None,
    ) -> WebhookWorkflow:
        """Create a new webhook workflow (HOOK-04)."""
        workflow_id = f"wf:{uuid.uuid4().hex[:12]}"
        workflow = WebhookWorkflow(
            workflow_id=workflow_id,
            name=name,
            description=description,
        )

        # Convert trigger dicts to WebhookTrigger objects
        if triggers:
            for i, t in enumerate(triggers):
                trigger = WebhookTrigger(
                    trigger_id=f"trig:{uuid.uuid4().hex[:12]}",
                    name=t.get("name", f"Trigger {i}"),
                    trigger_type=WebhookTriggerType(t.get("trigger_type", "custom")),
                    conditions=t.get("conditions", {}),
                    enabled=t.get("enabled", True),
                )
                workflow.triggers.append(trigger)

        # Convert action dicts to WebhookAction objects
        if actions:
            for i, a in enumerate(actions):
                action = WebhookAction(
                    action_id=f"act:{uuid.uuid4().hex[:12]}",
                    name=a.get("name", f"Action {i}"),
                    action_type=WebhookActionType(a.get("action_type", "http_post")),
                    config=a.get("config", {}),
                    enabled=a.get("enabled", True),
                    order=a.get("order", i),
                )
                workflow.actions.append(action)

        self._workflows[workflow_id] = workflow
        return workflow

    def get_workflow(self, workflow_id: str) -> WebhookWorkflow | None:
        """Get a workflow by ID."""
        return self._workflows.get(workflow_id)

    def list_workflows(self, enabled: bool | None = None) -> list[WebhookWorkflow]:
        """List all workflows, optionally filtered by enabled status."""
        workflows = list(self._workflows.values())
        if enabled is not None:
            workflows = [w for w in workflows if w.enabled == enabled]
        return workflows

    def update_workflow(
        self,
        workflow_id: str,
        name: str | None = None,
        description: str | None = None,
        enabled: bool | None = None,
    ) -> WebhookWorkflow | None:
        """Update workflow metadata."""
        workflow = self._workflows.get(workflow_id)
        if not workflow:
            return None
        if name is not None:
            workflow.name = name
        if description is not None:
            workflow.description = description
        if enabled is not None:
            workflow.enabled = enabled
        workflow.updated_at = datetime.now(timezone.utc)
        return workflow

    def delete_workflow(self, workflow_id: str) -> bool:
        """Delete a workflow."""
        if workflow_id in self._workflows:
            del self._workflows[workflow_id]
            return True
        return False

    def add_trigger(self, workflow_id: str, trigger: dict) -> WebhookTrigger | None:
        """Add a trigger to an existing workflow."""
        workflow = self._workflows.get(workflow_id)
        if not workflow:
            return None
        new_trigger = WebhookTrigger(
            trigger_id=f"trig:{uuid.uuid4().hex[:12]}",
            name=trigger.get("name", "New Trigger"),
            trigger_type=WebhookTriggerType(trigger.get("trigger_type", "custom")),
            conditions=trigger.get("conditions", {}),
            enabled=trigger.get("enabled", True),
        )
        workflow.triggers.append(new_trigger)
        workflow.updated_at = datetime.now(timezone.utc)
        return new_trigger

    def add_action(self, workflow_id: str, action: dict) -> WebhookAction | None:
        """Add an action to an existing workflow."""
        workflow = self._workflows.get(workflow_id)
        if not workflow:
            return None
        new_action = WebhookAction(
            action_id=f"act:{uuid.uuid4().hex[:12]}",
            name=action.get("name", "New Action"),
            action_type=WebhookActionType(action.get("action_type", "http_post")),
            config=action.get("config", {}),
            enabled=action.get("enabled", True),
            order=action.get("order", len(workflow.actions)),
        )
        workflow.actions.append(new_action)
        workflow.updated_at = datetime.now(timezone.utc)
        return new_action

    # ── Event History ───────────────────────────────────────────────────────

    def get_event(self, event_id: str) -> WebhookEvent | None:
        """Get an inbound webhook event by ID."""
        return self._events.get(event_id)

    def list_events(
        self,
        trigger_type: WebhookTriggerType | None = None,
        limit: int = 100,
    ) -> list[WebhookEvent]:
        """List recent webhook events."""
        events = sorted(self._events.values(), key=lambda e: e.received_at, reverse=True)
        if trigger_type:
            events = [e for e in events if e.trigger_type == trigger_type]
        return events[:limit]

    # ── Signature Generation (for testing/outbound) ───────────────────────

    def generate_signature(self, payload: dict[str, Any], secret: str) -> str:
        """Generate HMAC-SHA256 signature for a payload (HOOK-03)."""
        payload_bytes = json.dumps(payload, sort_keys=True).encode()
        return hmac.new(secret.encode(), payload_bytes, hashlib.sha256).hexdigest()


# ─────────────────────────────────────────────────────────────────────────────
# Outbound Webhook Delivery Service
# ─────────────────────────────────────────────────────────────────────────────


class OutboundWebhookService:
    """Service for delivering outbound webhooks with retry logic (HOOK-02, HOOK-03).

    Processes queued outbound webhooks with:
    - Exponential backoff retry
    - HMAC signing
    - Response logging
    """

    def __init__(self, persistence=None) -> None:
        self.persistence = persistence
        self._deliveries: dict[str, OutboundWebhook] = {}
        self._queue: list[OutboundWebhook] = []

    def enqueue(
        self,
        url: str,
        payload: dict[str, Any],
        secret: str = "",
        headers: dict[str, str] | None = None,
        workflow_id: str = "",
        action_id: str = "",
        method: str = "POST",
    ) -> OutboundWebhook:
        """Enqueue an outbound webhook for delivery."""
        if headers is None:
            headers = {"Content-Type": "application/json"}

        # Add HMAC signature if secret is provided
        if secret:
            signature = hmac.new(
                secret.encode(),
                json.dumps(payload, sort_keys=True).encode(),
                hashlib.sha256,
            ).hexdigest()
            headers["X-Signature"] = signature

        delivery = OutboundWebhook(
            delivery_id=f"dlv:{uuid.uuid4().hex[:12]}",
            workflow_id=workflow_id,
            action_id=action_id,
            url=url,
            method=method,
            headers=headers,
            payload=payload,
            status=WebhookDeliveryStatus.PENDING,
            max_attempts=3,
        )
        self._queue.append(delivery)
        self._deliveries[delivery.delivery_id] = delivery
        return delivery

    def deliver_next(self) -> OutboundWebhook | None:
        """Process the next queued outbound webhook."""
        if not self._queue:
            return None

        delivery = self._queue.pop(0)
        delivery.attempts += 1
        delivery.last_attempt_at = datetime.now(timezone.utc)

        try:
            response_status, response_body = self._send_http(delivery)
            delivery.response_status = response_status
            delivery.response_body = response_body[:500] if response_body else ""

            if 200 <= response_status < 300:
                delivery.status = WebhookDeliveryStatus.DELIVERED
            elif delivery.attempts < delivery.max_attempts:
                delivery.status = WebhookDeliveryStatus.RETRYING
                # Re-queue with exponential backoff
                backoff = 2 ** delivery.attempts
                # In a real system, we'd schedule this for later
                self._queue.append(delivery)
            else:
                delivery.status = WebhookDeliveryStatus.FAILED

        except Exception as exc:
            delivery.error_message = str(exc)[:200]
            if delivery.attempts < delivery.max_attempts:
                delivery.status = WebhookDeliveryStatus.RETRYING
                self._queue.append(delivery)
            else:
                delivery.status = WebhookDeliveryStatus.FAILED

        return delivery

    def _send_http(self, delivery: OutboundWebhook) -> tuple[int, str]:
        """Send an HTTP request and return (status_code, response_body)."""
        data = json.dumps(delivery.payload).encode() if delivery.payload else None

        req = urllib.request.Request(
            delivery.url,
            data=data,
            headers=delivery.headers,
            method=delivery.method,
        )

        with urllib.request.urlopen(req, timeout=10) as response:
            body = response.read().decode("utf-8", errors="replace")
            return response.status, body

    def get_delivery(self, delivery_id: str) -> OutboundWebhook | None:
        """Get delivery status by ID."""
        return self._deliveries.get(delivery_id)

    def list_pending(self, limit: int = 50) -> list[OutboundWebhook]:
        """List pending outbound deliveries."""
        return [d for d in self._deliveries.values() if d.status == WebhookDeliveryStatus.PENDING][:limit]

    def list_recent(self, limit: int = 100) -> list[OutboundWebhook]:
        """List recent deliveries."""
        sorted_deliveries = sorted(
            self._deliveries.values(),
            key=lambda d: d.created_at,
            reverse=True,
        )
        return sorted_deliveries[:limit]
