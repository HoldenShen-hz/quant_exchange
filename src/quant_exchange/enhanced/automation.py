"""Webhook and automation workflow services (HOOK-01 ~ HOOK-05)."""

from __future__ import annotations

import hashlib
import hmac
import json
import time
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable


# ─────────────────────────────────────────────────────────────────────────────
# Enums
# ─────────────────────────────────────────────────────────────────────────────

class WebhookDirection(Enum):
    """Inbound vs outbound webhook."""

    INBOUND = "inbound"
    OUTBOUND = "outbound"


class WebhookMethod(Enum):
    """HTTP methods supported by webhooks."""

    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    PATCH = "PATCH"
    DELETE = "DELETE"


class WebhookEventType(Enum):
    """Standard webhook event types."""

    ORDER_SUBMITTED = "order.submitted"
    ORDER_FILLED = "order.filled"
    ORDER_CANCELLED = "order.cancelled"
    ORDER_REJECTED = "order.rejected"
    POSITION_OPENED = "position.opened"
    POSITION_CLOSED = "position.closed"
    ALERT_TRIGGERED = "alert.triggered"
    RISK_LIMIT_BREACH = "risk.limit_breach"
    BALANCE_LOW = "balance.low"
    CUSTOM = "custom"


class WorkflowStatus(Enum):
    """Workflow execution status."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class TriggerType(Enum):
    """Workflow trigger types."""

    WEBHOOK = "webhook"
    SCHEDULE = "schedule"
    EVENT = "event"
    MANUAL = "manual"


# ─────────────────────────────────────────────────────────────────────────────
# Core Webhook Models
# ─────────────────────────────────────────────────────────────────────────────

@dataclass(slots=True)
class WebhookEndpoint:
    """A registered webhook endpoint."""

    endpoint_id: str
    name: str
    url: str
    direction: WebhookDirection
    secret: str  # HMAC secret for signature validation
    event_types: tuple[str, ...] = ()
    enabled: bool = True
    headers: dict[str, str] = field(default_factory=dict)
    timeout_seconds: int = 30
    retry_count: int = 3
    created_at: str | None = None
    updated_at: str | None = None


@dataclass(slots=True)
class WebhookDelivery:
    """Record of a single webhook delivery attempt."""

    delivery_id: str
    endpoint_id: str
    event_type: str
    payload: dict[str, Any]
    method: WebhookMethod
    headers: dict[str, str]
    response_status: int | None = None
    response_body: str | None = None
    attempt_number: int = 1
    status: str = "pending"  # pending, delivered, failed
    error_message: str | None = None
    created_at: str | None = None
    delivered_at: str | None = None


@dataclass(slots=True)
class WebhookSignature:
    """HMAC signature validation result."""

    valid: bool
    algorithm: str = "sha256"
    provided_signature: str | None = None
    computed_signature: str | None = None
    error: str | None = None


# ─────────────────────────────────────────────────────────────────────────────
# Workflow Models
# ─────────────────────────────────────────────────────────────────────────────

@dataclass(slots=True)
class WorkflowCondition:
    """A single condition within a workflow rule."""

    field: str  # e.g., "payload.price", "event.type", "header.X-Token"
    operator: str  # eq, ne, gt, lt, gte, lte, contains, starts_with, ends_with, regex
    value: Any  # comparison value
    logic: str = "and"  # and, or (for chaining with next condition)


@dataclass(slots=True)
class WorkflowAction:
    """A single action executed when workflow triggers."""

    action_type: str  # http_request, send_email, send_webhook, update_record, log, notify
    params: dict[str, Any] = field(default_factory=dict)
    continue_on_error: bool = True
    order: int = 0


@dataclass(slots=True)
class WorkflowTrigger:
    """Trigger configuration for a workflow."""

    trigger_type: TriggerType
    source: str | None = None  # webhook endpoint id, schedule expression, event type
    conditions: tuple[WorkflowCondition, ...] = ()
    enabled: bool = True


@dataclass(slots=True)
class WorkflowDefinition:
    """A complete workflow definition."""

    workflow_id: str
    name: str
    trigger: WorkflowTrigger
    description: str = ""
    actions: tuple[WorkflowAction, ...] = ()
    enabled: bool = True
    user_id: str | None = None
    tags: tuple[str, ...] = ()
    version: int = 1
    created_at: str | None = None
    updated_at: str | None = None


@dataclass(slots=True)
class WorkflowExecution:
    """Record of a single workflow execution."""

    execution_id: str
    workflow_id: str
    trigger_type: TriggerType
    trigger_payload: dict[str, Any]
    status: WorkflowStatus
    started_at: str | None = None
    completed_at: str | None = None
    action_results: tuple[dict[str, Any], ...] = ()
    error_message: str | None = None
    retry_count: int = 0


# ─────────────────────────────────────────────────────────────────────────────
# WebhookService
# ─────────────────────────────────────────────────────────────────────────────

class WebhookService:
    """Webhook management and delivery service (HOOK-01, HOOK-02, HOOK-03).

    Provides:
    - Register inbound/outbound webhook endpoints
    - HMAC signature generation and validation
    - Retry logic with exponential backoff
    - Delivery logging and status tracking
    """

    def __init__(self, persistence=None) -> None:
        self.persistence = persistence
        self._endpoints: dict[str, WebhookEndpoint] = {}
        self._deliveries: dict[str, list[WebhookDelivery]] = {}
        self._secret_cache: dict[str, str] = {}

    # ── Endpoint Management ──────────────────────────────────────────────────

    def register_endpoint(
        self,
        name: str,
        url: str,
        direction: WebhookDirection,
        event_types: tuple[str, ...] = (),
        *,
        secret: str | None = None,
        headers: dict[str, str] | None = None,
        timeout_seconds: int = 30,
        retry_count: int = 3,
    ) -> WebhookEndpoint:
        """Register a new webhook endpoint (HOOK-01 / HOOK-02)."""
        endpoint_id = f"wh_{uuid.uuid4().hex[:16]}"
        if secret is None:
            secret = uuid.uuid4().hex
        endpoint = WebhookEndpoint(
            endpoint_id=endpoint_id,
            name=name,
            url=url,
            direction=direction,
            secret=secret,
            event_types=event_types,
            headers=headers or {},
            timeout_seconds=timeout_seconds,
            retry_count=retry_count,
            created_at=datetime.now(timezone.utc).isoformat(),
            updated_at=datetime.now(timezone.utc).isoformat(),
        )
        self._endpoints[endpoint_id] = endpoint
        self._secret_cache[endpoint_id] = secret
        self._persist_endpoint(endpoint)
        return endpoint

    def get_endpoint(self, endpoint_id: str) -> WebhookEndpoint | None:
        """Retrieve a webhook endpoint by ID."""
        return self._endpoints.get(endpoint_id)

    def list_endpoints(
        self, direction: WebhookDirection | None = None, enabled_only: bool = False
    ) -> list[WebhookEndpoint]:
        """List all webhook endpoints, optionally filtered."""
        results = list(self._endpoints.values())
        if direction is not None:
            results = [e for e in results if e.direction == direction]
        if enabled_only:
            results = [e for e in results if e.enabled]
        return results

    def update_endpoint(
        self,
        endpoint_id: str,
        *,
        name: str | None = None,
        url: str | None = None,
        enabled: bool | None = None,
        event_types: tuple[str, ...] | None = None,
        headers: dict[str, str] | None = None,
        timeout_seconds: int | None = None,
        retry_count: int | None = None,
    ) -> WebhookEndpoint | None:
        """Update an existing webhook endpoint."""
        endpoint = self._endpoints.get(endpoint_id)
        if endpoint is None:
            return None
        updated = WebhookEndpoint(
            endpoint_id=endpoint.endpoint_id,
            name=name if name is not None else endpoint.name,
            url=url if url is not None else endpoint.url,
            direction=endpoint.direction,
            secret=endpoint.secret,
            event_types=event_types if event_types is not None else endpoint.event_types,
            enabled=enabled if enabled is not None else endpoint.enabled,
            headers=headers if headers is not None else endpoint.headers,
            timeout_seconds=timeout_seconds if timeout_seconds is not None else endpoint.timeout_seconds,
            retry_count=retry_count if retry_count is not None else endpoint.retry_count,
            created_at=endpoint.created_at,
            updated_at=datetime.now(timezone.utc).isoformat(),
        )
        self._endpoints[endpoint_id] = updated
        self._persist_endpoint(updated)
        return updated

    def delete_endpoint(self, endpoint_id: str) -> bool:
        """Delete a webhook endpoint."""
        if endpoint_id in self._endpoints:
            del self._endpoints[endpoint_id]
            return True
        return False

    def rotate_secret(self, endpoint_id: str) -> str | None:
        """Rotate the HMAC secret for an endpoint."""
        endpoint = self._endpoints.get(endpoint_id)
        if endpoint is None:
            return None
        new_secret = uuid.uuid4().hex
        updated = WebhookEndpoint(
            endpoint_id=endpoint.endpoint_id,
            name=endpoint.name,
            url=endpoint.url,
            direction=endpoint.direction,
            secret=new_secret,
            event_types=endpoint.event_types,
            enabled=endpoint.enabled,
            headers=endpoint.headers,
            timeout_seconds=endpoint.timeout_seconds,
            retry_count=endpoint.retry_count,
            created_at=endpoint.created_at,
            updated_at=datetime.now(timezone.utc).isoformat(),
        )
        self._endpoints[endpoint_id] = updated
        self._secret_cache[endpoint_id] = new_secret
        self._persist_endpoint(updated)
        return new_secret

    # ── Signature Operations ──────────────────────────────────────────────────

    def generate_signature(self, endpoint_id: str, payload: str | bytes) -> WebhookSignature:
        """Generate HMAC-SHA256 signature for a payload (HOOK-03)."""
        endpoint = self._endpoints.get(endpoint_id)
        if endpoint is None:
            return WebhookSignature(valid=False, error="Endpoint not found")
        secret = self._secret_cache.get(endpoint_id, endpoint.secret)
        if isinstance(payload, str):
            payload = payload.encode("utf-8")
        signature = hmac.new(secret.encode("utf-8"), payload, hashlib.sha256).hexdigest()
        return WebhookSignature(
            valid=True,
            algorithm="sha256",
            provided_signature=signature,
            computed_signature=signature,
        )

    def validate_signature(
        self, endpoint_id: str, payload: str | bytes, signature: str
    ) -> WebhookSignature:
        """Validate HMAC signature from an inbound webhook (HOOK-03)."""
        endpoint = self._endpoints.get(endpoint_id)
        if endpoint is None:
            return WebhookSignature(valid=False, error="Endpoint not found")
        secret = self._secret_cache.get(endpoint_id, endpoint.secret)
        if isinstance(payload, str):
            payload = payload.encode("utf-8")
        expected = hmac.new(secret.encode("utf-8"), payload, hashlib.sha256).hexdigest()
        valid = hmac.compare_digest(expected, signature)
        return WebhookSignature(
            valid=valid,
            algorithm="sha256",
            provided_signature=signature,
            computed_signature=expected,
        )

    def validate_request_headers(
        self, endpoint_id: str, headers: dict[str, str], body: str | bytes
    ) -> WebhookSignature:
        """Validate webhook request using signature header (HOOK-03)."""
        endpoint = self._endpoints.get(endpoint_id)
        if endpoint is None:
            return WebhookSignature(valid=False, error="Endpoint not found")
        # Try common signature header names
        sig_header = (
            headers.get("X-Webhook-Signature")
            or headers.get("X-Hub-Signature-256")
            or headers.get("Authorization")
            or ""
        )
        if sig_header.startswith("sha256="):
            sig = sig_header[7:]
        else:
            sig = sig_header
        return self.validate_signature(endpoint_id, body, sig)

    # ── Delivery ─────────────────────────────────────────────────────────────

    def record_delivery(
        self,
        endpoint_id: str,
        event_type: str,
        payload: dict[str, Any],
        method: WebhookMethod = WebhookMethod.POST,
    ) -> WebhookDelivery:
        """Record a webhook delivery attempt."""
        delivery_id = f"del_{uuid.uuid4().hex[:16]}"
        delivery = WebhookDelivery(
            delivery_id=delivery_id,
            endpoint_id=endpoint_id,
            event_type=event_type,
            payload=payload,
            method=method,
            headers={"Content-Type": "application/json"},
            created_at=datetime.now(timezone.utc).isoformat(),
        )
        if endpoint_id not in self._deliveries:
            self._deliveries[endpoint_id] = []
        self._deliveries[endpoint_id].append(delivery)
        return delivery

    def update_delivery_status(
        self,
        delivery_id: str,
        endpoint_id: str,
        *,
        status: str,
        response_status: int | None = None,
        response_body: str | None = None,
        error_message: str | None = None,
        attempt_number: int | None = None,
    ) -> bool:
        """Update the status of a webhook delivery."""
        deliveries = self._deliveries.get(endpoint_id, [])
        for delivery in deliveries:
            if delivery.delivery_id == delivery_id:
                delivery.response_status = response_status
                delivery.response_body = response_body
                delivery.status = status
                delivery.error_message = error_message
                if status == "delivered":
                    delivery.delivered_at = datetime.now(timezone.utc).isoformat()
                if attempt_number is not None:
                    delivery.attempt_number = attempt_number
                return True
        return False

    def get_deliveries(self, endpoint_id: str, limit: int = 50) -> list[WebhookDelivery]:
        """Get recent deliveries for an endpoint."""
        return (self._deliveries.get(endpoint_id, []) or [])[-limit:]

    # ── Persistence ───────────────────────────────────────────────────────────

    def _persist_endpoint(self, endpoint: WebhookEndpoint) -> None:
        """Persist endpoint to storage."""
        if self.persistence is not None:
            self.persistence.upsert_record(
                "webhook_endpoints", "endpoint_id", endpoint.endpoint_id, asdict(endpoint)
            )


# ─────────────────────────────────────────────────────────────────────────────
# AutomationWorkflowService
# ─────────────────────────────────────────────────────────────────────────────

class AutomationWorkflowService:
    """Workflow automation engine (HOOK-04, HOOK-05).

    Provides:
    - Workflow definition and management
    - Trigger evaluation and condition matching
    - Action execution with error handling
    - Execution logging and history
    - Workflow import/export
    """

    def __init__(self, persistence=None) -> None:
        self.persistence = persistence
        self._workflows: dict[str, WorkflowDefinition] = {}
        self._executions: dict[str, list[WorkflowExecution]] = {}
        self._action_handlers: dict[str, Callable[..., dict[str, Any]]] = {}
        self._register_default_handlers()

    def _register_default_handlers(self) -> None:
        """Register built-in action handlers."""
        self._action_handlers["log"] = self._handler_log
        self._action_handlers["notify"] = self._handler_notify
        self._action_handlers["http_request"] = self._handler_http_request
        self._action_handlers["update_record"] = self._handler_update_record

    # ── Workflow CRUD ────────────────────────────────────────────────────────

    def create_workflow(
        self,
        name: str,
        trigger: WorkflowTrigger,
        actions: tuple[WorkflowAction, ...],
        *,
        description: str = "",
        user_id: str | None = None,
        tags: tuple[str, ...] = (),
    ) -> WorkflowDefinition:
        """Create a new workflow definition (HOOK-04)."""
        workflow_id = f"wf_{uuid.uuid4().hex[:16]}"
        workflow = WorkflowDefinition(
            workflow_id=workflow_id,
            name=name,
            description=description,
            trigger=trigger,
            actions=actions,
            user_id=user_id,
            tags=tags,
            created_at=datetime.now(timezone.utc).isoformat(),
            updated_at=datetime.now(timezone.utc).isoformat(),
        )
        self._workflows[workflow_id] = workflow
        self._persist_workflow(workflow)
        return workflow

    def get_workflow(self, workflow_id: str) -> WorkflowDefinition | None:
        """Get a workflow by ID."""
        return self._workflows.get(workflow_id)

    def update_workflow(
        self,
        workflow_id: str,
        *,
        name: str | None = None,
        description: str | None = None,
        trigger: WorkflowTrigger | None = None,
        actions: tuple[WorkflowAction, ...] | None = None,
        enabled: bool | None = None,
        tags: tuple[str, ...] | None = None,
    ) -> WorkflowDefinition | None:
        """Update an existing workflow."""
        workflow = self._workflows.get(workflow_id)
        if workflow is None:
            return None
        updated = WorkflowDefinition(
            workflow_id=workflow.workflow_id,
            name=name if name is not None else workflow.name,
            description=description if description is not None else workflow.description,
            trigger=trigger if trigger is not None else workflow.trigger,
            actions=actions if actions is not None else workflow.actions,
            enabled=enabled if enabled is not None else workflow.enabled,
            user_id=workflow.user_id,
            tags=tags if tags is not None else workflow.tags,
            version=workflow.version + 1,
            created_at=workflow.created_at,
            updated_at=datetime.now(timezone.utc).isoformat(),
        )
        self._workflows[workflow_id] = updated
        self._persist_workflow(updated)
        return updated

    def delete_workflow(self, workflow_id: str) -> bool:
        """Delete a workflow."""
        if workflow_id in self._workflows:
            del self._workflows[workflow_id]
            return True
        return False

    def list_workflows(
        self,
        user_id: str | None = None,
        enabled_only: bool = False,
        tag: str | None = None,
    ) -> list[WorkflowDefinition]:
        """List workflows with optional filters."""
        results = list(self._workflows.values())
        if user_id is not None:
            results = [w for w in results if w.user_id == user_id]
        if enabled_only:
            results = [w for w in results if w.enabled]
        if tag is not None:
            results = [w for w in results if tag in w.tags]
        return results

    # ── Trigger Evaluation ──────────────────────────────────────────────────

    def evaluate_trigger(
        self, workflow: WorkflowDefinition, event_payload: dict[str, Any]
    ) -> bool:
        """Evaluate whether a workflow trigger should fire (HOOK-04)."""
        trigger = workflow.trigger
        if not trigger.enabled:
            return False

        if trigger.trigger_type == TriggerType.WEBHOOK:
            # Webhook triggers always fire when called
            return True

        if trigger.trigger_type == TriggerType.EVENT:
            if trigger.source and trigger.source != event_payload.get("event_type"):
                return False

        if trigger.conditions:
            return self._evaluate_conditions(trigger.conditions, event_payload)
        return True

    def _evaluate_conditions(
        self, conditions: tuple[WorkflowCondition, ...], payload: dict[str, Any]
    ) -> bool:
        """Evaluate workflow conditions against payload."""
        if not conditions:
            return True

        results: list[bool] = []
        for i, cond in enumerate(conditions):
            value = self._get_nested_field(payload, cond.field)
            result = self._compare_values(value, cond.operator, cond.value)
            if i == 0:
                results.append(result)
            else:
                if cond.logic == "and":
                    results.append(results[-1] and result)
                else:
                    results.append(results[-1] or result)
        return results[-1] if results else True

    def _get_nested_field(self, data: dict[str, Any], field: str) -> Any:
        """Get a nested field from data using dot notation."""
        parts = field.split(".")
        value = data
        for part in parts:
            if isinstance(value, dict):
                value = value.get(part)
            else:
                return None
        return value

    def _compare_values(self, actual: Any, operator: str, expected: Any) -> bool:
        """Compare values using the specified operator."""
        if operator == "eq":
            return actual == expected
        if operator == "ne":
            return actual != expected
        if operator == "gt":
            return actual is not None and float(actual) > float(expected)
        if operator == "lt":
            return actual is not None and float(actual) < float(expected)
        if operator == "gte":
            return actual is not None and float(actual) >= float(expected)
        if operator == "lte":
            return actual is not None and float(actual) <= float(expected)
        if operator == "contains":
            return actual is not None and str(expected) in str(actual)
        if operator == "starts_with":
            return actual is not None and str(actual).startswith(str(expected))
        if operator == "ends_with":
            return actual is not None and str(actual).endswith(str(expected))
        if operator == "regex":
            import re
            return actual is not None and bool(re.match(str(expected), str(actual)))
        return False

    # ── Execution ────────────────────────────────────────────────────────────

    def execute_workflow(
        self,
        workflow_id: str,
        trigger_payload: dict[str, Any],
        *,
        user_id: str | None = None,
    ) -> WorkflowExecution:
        """Execute a workflow with the given trigger payload (HOOK-05)."""
        workflow = self._workflows.get(workflow_id)
        if workflow is None:
            raise ValueError(f"Workflow {workflow_id} not found")

        execution_id = f"exec_{uuid.uuid4().hex[:16]}"
        execution = WorkflowExecution(
            execution_id=execution_id,
            workflow_id=workflow_id,
            trigger_type=workflow.trigger.trigger_type,
            trigger_payload=trigger_payload,
            status=WorkflowStatus.PENDING,
            started_at=datetime.now(timezone.utc).isoformat(),
        )

        if workflow_id not in self._executions:
            self._executions[workflow_id] = []
        self._executions[workflow_id].append(execution)
        self._persist_execution(execution)

        # Evaluate trigger conditions
        if not self.evaluate_trigger(workflow, trigger_payload):
            execution.status = WorkflowStatus.CANCELLED
            execution.error_message = "Trigger conditions not met"
            self._persist_execution(execution)
            return execution

        execution.status = WorkflowStatus.RUNNING
        self._persist_execution(execution)

        # Execute actions in order
        action_results: list[dict[str, Any]] = []
        for action in sorted(workflow.actions, key=lambda a: a.order):
            try:
                result = self._execute_action(action, trigger_payload)
                action_results.append(result)
            except Exception as e:
                error_result = {"action_type": action.action_type, "error": str(e), "success": False}
                action_results.append(error_result)
                if not action.continue_on_error:
                    execution.status = WorkflowStatus.FAILED
                    execution.error_message = str(e)
                    break

        execution.action_results = tuple(action_results)
        if execution.status == WorkflowStatus.RUNNING:
            execution.status = WorkflowStatus.COMPLETED
        execution.completed_at = datetime.now(timezone.utc).isoformat()
        self._persist_execution(execution)
        return execution

    def _execute_action(
        self, action: WorkflowAction, context: dict[str, Any]
    ) -> dict[str, Any]:
        """Execute a single workflow action."""
        handler = self._action_handlers.get(action.action_type)
        if handler is None:
            return {"action_type": action.action_type, "error": "Unknown action type", "success": False}
        return handler(action.params, context)

    # ── Built-in Action Handlers ────────────────────────────────────────────

    def _handler_log(self, params: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
        """Log action handler."""
        message = params.get("message", "").format(**context)
        level = params.get("level", "info")
        return {"action_type": "log", "level": level, "message": message, "success": True}

    def _handler_notify(self, params: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
        """Notification action handler (stub)."""
        channel = params.get("channel", "system")
        message = params.get("message", "").format(**context)
        return {"action_type": "notify", "channel": channel, "message": message, "success": True}

    def _handler_http_request(self, params: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
        """HTTP request action handler (stub)."""
        url = params.get("url", "").format(**context)
        method = params.get("method", "POST")
        headers = params.get("headers", {})
        body = params.get("body", {}).format(**context) if isinstance(params.get("body"), str) else params.get("body", {})
        return {"action_type": "http_request", "url": url, "method": method, "success": True, "response_code": 200}

    def _handler_update_record(self, params: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
        """Update record action handler (stub)."""
        table = params.get("table", "")
        record_id = params.get("record_id", "").format(**context) if isinstance(params.get("record_id"), str) else params.get("record_id")
        updates = params.get("updates", {})
        return {"action_type": "update_record", "table": table, "record_id": record_id, "success": True}

    # ── Execution History ────────────────────────────────────────────────────

    def get_executions(
        self, workflow_id: str, limit: int = 50
    ) -> list[WorkflowExecution]:
        """Get recent executions for a workflow."""
        return (self._executions.get(workflow_id, []) or [])[-limit:]

    def get_execution(self, execution_id: str) -> WorkflowExecution | None:
        """Get a specific execution by ID."""
        for executions in self._executions.values():
            for exec_ in executions:
                if exec_.execution_id == execution_id:
                    return exec_
        return None

    # ── Import / Export ──────────────────────────────────────────────────────

    def export_workflow(self, workflow_id: str) -> dict[str, Any] | None:
        """Export a workflow definition as a portable dict (HOOK-05)."""
        workflow = self._workflows.get(workflow_id)
        if workflow is None:
            return None
        return {
            "name": workflow.name,
            "description": workflow.description,
            "trigger": asdict(workflow.trigger),
            "actions": [asdict(a) for a in workflow.actions],
            "tags": list(workflow.tags),
            "version": workflow.version,
        }

    def import_workflow(
        self, data: dict[str, Any], user_id: str | None = None
    ) -> WorkflowDefinition:
        """Import a workflow from a portable dict (HOOK-05)."""
        trigger_data = data.get("trigger", {})
        trigger = WorkflowTrigger(
            trigger_type=TriggerType(trigger_data.get("trigger_type", "manual")),
            source=trigger_data.get("source"),
            conditions=tuple(WorkflowCondition(**c) for c in trigger_data.get("conditions", [])),
            enabled=trigger_data.get("enabled", True),
        )
        actions = tuple(
            WorkflowAction(
                action_type=a.get("action_type", "log"),
                params=a.get("params", {}),
                continue_on_error=a.get("continue_on_error", True),
                order=a.get("order", 0),
            )
            for a in data.get("actions", [])
        )
        return self.create_workflow(
            name=data.get("name", "Imported Workflow"),
            description=data.get("description", ""),
            trigger=trigger,
            actions=actions,
            user_id=user_id,
            tags=tuple(data.get("tags", [])),
        )

    # ── Persistence ─────────────────────────────────────────────────────────

    def _persist_workflow(self, workflow: WorkflowDefinition) -> None:
        """Persist workflow to storage."""
        if self.persistence is not None:
            self.persistence.upsert_record(
                "automation_workflows", "workflow_id", workflow.workflow_id, asdict(workflow)
            )

    def _persist_execution(self, execution: WorkflowExecution) -> None:
        """Persist execution to storage."""
        if self.persistence is not None:
            self.persistence.upsert_record(
                "automation_executions", "execution_id", execution.execution_id, asdict(execution)
            )
