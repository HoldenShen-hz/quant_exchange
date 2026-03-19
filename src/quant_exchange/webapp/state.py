"""Persistent workspace state and activity logging for the stock screener web UI."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from quant_exchange.persistence.database import SQLitePersistence


class WebWorkspaceService:
    """Persist user workspace state and UI activity events for the web application."""

    def __init__(self, persistence: SQLitePersistence, workspace_code: str = "stock_screener") -> None:
        self.persistence = persistence
        self.workspace_code = workspace_code

    def load_state(
        self,
        principal_id: str,
        *,
        principal_type: str = "client",
        client_id: str | None = None,
        username: str | None = None,
    ) -> dict[str, Any]:
        """Load the most recent saved workspace state for one browser client or authenticated user."""

        row = self.persistence.fetch_one(
            "web_saved_workspaces",
            where="workspace_key = :workspace_key",
            params={"workspace_key": self._workspace_key(principal_type, principal_id)},
        )
        if row is None and principal_type == "client":
            row = self.persistence.fetch_one(
                "web_saved_workspaces",
                where="workspace_key = :workspace_key",
                params={"workspace_key": self._legacy_workspace_key(principal_id)},
            )
        if row is None:
            return {
                "client_id": client_id or principal_id,
                "principal_type": principal_type,
                "principal_id": principal_id,
                "username": username,
                "workspace_code": self.workspace_code,
                "state": self.default_state(),
                "updated_at": None,
            }
        payload = row["payload"]
        return {
            "client_id": payload.get("client_id") or client_id or principal_id,
            "principal_type": payload.get("principal_type") or principal_type,
            "principal_id": payload.get("principal_id") or principal_id,
            "username": payload.get("username") or username,
            "workspace_code": self.workspace_code,
            "state": payload.get("state", self.default_state()),
            "updated_at": row.get("updated_at"),
        }

    def save_state(
        self,
        principal_id: str,
        state: dict[str, Any],
        *,
        principal_type: str = "client",
        client_id: str | None = None,
        username: str | None = None,
    ) -> dict[str, Any]:
        """Save the current workspace state for one browser client or authenticated user."""

        normalized_state = self._normalize_state(state)
        payload = {
            "client_id": client_id or principal_id,
            "principal_type": principal_type,
            "principal_id": principal_id,
            "username": username,
            "workspace_code": self.workspace_code,
            "state": normalized_state,
        }
        self.persistence.upsert_record(
            "web_saved_workspaces",
            "workspace_key",
            self._workspace_key(principal_type, principal_id),
            payload,
            extra_columns={
                "client_id": client_id or principal_id,
                "principal_type": principal_type,
                "principal_id": principal_id,
                "username": username,
                "workspace_code": self.workspace_code,
                "last_active_instrument_id": normalized_state["active_instrument_id"],
                "compare_left": normalized_state["compare"]["left"],
                "compare_right": normalized_state["compare"]["right"],
            },
        )
        self._sync_learning_progress(
            principal_id,
            current_lesson_id=(normalized_state.get("learning") or {}).get("selected_lesson_id"),
            principal_type=principal_type,
            client_id=client_id,
            username=username,
        )
        return self.load_state(principal_id, principal_type=principal_type, client_id=client_id, username=username)

    def log_event(
        self,
        principal_id: str,
        event_type: str,
        *,
        payload: dict[str, Any] | None = None,
        path: str | None = None,
        principal_type: str = "client",
        client_id: str | None = None,
        username: str | None = None,
    ) -> dict[str, Any]:
        """Record one user activity event for later auditing and restore assistance."""

        event_payload = {
            "event_id": uuid4().hex,
            "client_id": client_id or principal_id,
            "principal_type": principal_type,
            "principal_id": principal_id,
            "username": username,
            "workspace_code": self.workspace_code,
            "event_type": event_type,
            "path": path or "/",
            "details": payload or {},
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        self.persistence.insert_record(
            "web_activity_logs",
            event_payload,
            extra_columns={
                "event_id": event_payload["event_id"],
                "client_id": client_id or principal_id,
                "principal_type": principal_type,
                "principal_id": principal_id,
                "username": username,
                "workspace_code": self.workspace_code,
                "event_type": event_type,
                "path": event_payload["path"],
                "created_at": event_payload["created_at"],
            },
        )
        return event_payload

    def list_events(
        self,
        principal_id: str,
        *,
        principal_type: str = "client",
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        """Return recent activity events for one principal in reverse chronological order."""

        rows = self.persistence.raw_fetchall(
            """
            SELECT *
            FROM web_activity_logs
            WHERE principal_type = ? AND principal_id = ? AND workspace_code = ?
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (principal_type, principal_id, self.workspace_code, max(1, min(limit, 200))),
        )
        if not rows and principal_type == "client":
            rows = self.persistence.raw_fetchall(
                """
                SELECT *
                FROM web_activity_logs
                WHERE client_id = ? AND workspace_code = ?
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (principal_id, self.workspace_code, max(1, min(limit, 200))),
            )
        events = []
        for row in rows:
            payload = self.persistence._deserialize(row["payload"])
            events.append(payload)
        return events

    def load_learning_progress(
        self,
        principal_id: str,
        *,
        principal_type: str = "client",
        client_id: str | None = None,
        username: str | None = None,
    ) -> dict[str, Any]:
        """Return structured learning-progress data for one principal."""

        row = self.persistence.fetch_one(
            "web_learning_progress",
            where="progress_key = :progress_key",
            params={"progress_key": self._learning_progress_key(principal_type, principal_id)},
        )
        if row is None:
            return {
                "client_id": client_id or principal_id,
                "principal_type": principal_type,
                "principal_id": principal_id,
                "username": username,
                "current_lesson_id": None,
                "best_score": None,
                "last_score": None,
                "quiz_attempts": 0,
                "recent_attempts": [],
                "updated_at": None,
            }
        payload = row["payload"]
        payload.setdefault("client_id", client_id or principal_id)
        payload.setdefault("principal_type", principal_type)
        payload.setdefault("principal_id", principal_id)
        payload.setdefault("username", username)
        payload.setdefault("recent_attempts", [])
        return payload | {"updated_at": row.get("updated_at")}

    def record_learning_attempt(
        self,
        principal_id: str,
        result: dict[str, Any],
        *,
        principal_type: str = "client",
        client_id: str | None = None,
        username: str | None = None,
        current_lesson_id: str | None = None,
    ) -> dict[str, Any]:
        """Persist one quiz attempt and update aggregate learning progress."""

        progress = self.load_learning_progress(
            principal_id,
            principal_type=principal_type,
            client_id=client_id,
            username=username,
        )
        summary = {
            "attempt_id": uuid4().hex,
            "score": result.get("score"),
            "passed": result.get("passed"),
            "submitted_at": datetime.now(timezone.utc).isoformat(),
        }
        recent_attempts = [summary, *list(progress.get("recent_attempts") or [])][:8]
        updated_payload = {
            "client_id": client_id or principal_id,
            "principal_type": principal_type,
            "principal_id": principal_id,
            "username": username,
            "current_lesson_id": current_lesson_id or progress.get("current_lesson_id"),
            "best_score": max(
                [score for score in [progress.get("best_score"), result.get("score")] if score is not None],
                default=None,
            ),
            "last_score": result.get("score"),
            "quiz_attempts": int(progress.get("quiz_attempts") or 0) + 1,
            "recent_attempts": recent_attempts,
        }
        self.persistence.upsert_record(
            "web_learning_progress",
            "progress_key",
            self._learning_progress_key(principal_type, principal_id),
            updated_payload,
            extra_columns={
                "principal_type": principal_type,
                "principal_id": principal_id,
                "username": username,
                "current_lesson_id": updated_payload["current_lesson_id"],
                "best_score": updated_payload["best_score"],
                "last_score": updated_payload["last_score"],
                "quiz_attempts": updated_payload["quiz_attempts"],
            },
        )
        self.persistence.insert_record(
            "web_learning_attempts",
            {
                **updated_payload,
                "attempt_id": summary["attempt_id"],
                "result": result,
                "submitted_at": summary["submitted_at"],
            },
            extra_columns={
                "attempt_id": summary["attempt_id"],
                "principal_type": principal_type,
                "principal_id": principal_id,
                "username": username,
                "score": result.get("score") or 0,
                "passed": 1 if result.get("passed") else 0,
                "created_at": summary["submitted_at"],
            },
        )
        return self.load_learning_progress(
            principal_id,
            principal_type=principal_type,
            client_id=client_id,
            username=username,
        )

    def default_state(self) -> dict[str, Any]:
        """Return the empty workspace state used for first-time visits."""

        return {
            "filters": {},
            "compare": {"left": None, "right": None},
            "active_instrument_id": None,
            "watchlist": [],
            "chart": {"range": 120, "mode": "candles"},
            "crypto": {
                "active_instrument_id": "BTCUSDT",
                "chart": {"range": 120, "mode": "candles"},
            },
            "preset": None,
            "active_tab": "overview",
            "learning": {"selected_lesson_id": None, "search_query": ""},
        }

    def _workspace_key(self, principal_type: str, principal_id: str) -> str:
        """Build the persistence key for one client-specific or user-specific workspace."""

        return f"{self.workspace_code}:{principal_type}:{principal_id}"

    def _learning_progress_key(self, principal_type: str, principal_id: str) -> str:
        """Build the persistence key for one principal's learning progress."""

        return f"learning:{self.workspace_code}:{principal_type}:{principal_id}"

    def _legacy_workspace_key(self, client_id: str) -> str:
        """Build the pre-multi-user workspace key for backward compatibility."""

        return f"{self.workspace_code}:{client_id}"

    def _sync_learning_progress(
        self,
        principal_id: str,
        *,
        current_lesson_id: str | None,
        principal_type: str = "client",
        client_id: str | None = None,
        username: str | None = None,
    ) -> None:
        """Update the latest selected lesson without incrementing quiz attempt counters."""

        progress = self.load_learning_progress(
            principal_id,
            principal_type=principal_type,
            client_id=client_id,
            username=username,
        )
        payload = {
            "client_id": client_id or principal_id,
            "principal_type": principal_type,
            "principal_id": principal_id,
            "username": username,
            "current_lesson_id": current_lesson_id,
            "best_score": progress.get("best_score"),
            "last_score": progress.get("last_score"),
            "quiz_attempts": int(progress.get("quiz_attempts") or 0),
            "recent_attempts": list(progress.get("recent_attempts") or []),
        }
        self.persistence.upsert_record(
            "web_learning_progress",
            "progress_key",
            self._learning_progress_key(principal_type, principal_id),
            payload,
            extra_columns={
                "principal_type": principal_type,
                "principal_id": principal_id,
                "username": username,
                "current_lesson_id": current_lesson_id,
                "best_score": payload["best_score"],
                "last_score": payload["last_score"],
                "quiz_attempts": payload["quiz_attempts"],
            },
        )

    def _normalize_state(self, state: dict[str, Any] | None) -> dict[str, Any]:
        """Sanitize client-provided workspace state into a stable shape."""

        state = state or {}
        filters = state.get("filters") or {}
        compare = state.get("compare") or {}
        crypto = state.get("crypto") or {}
        return {
            "filters": {str(key): value for key, value in filters.items() if value not in (None, "")},
            "compare": {
                "left": compare.get("left"),
                "right": compare.get("right"),
            },
            "active_instrument_id": state.get("active_instrument_id"),
            "watchlist": [item for item in state.get("watchlist", []) if item],
            "chart": {
                "range": int((state.get("chart") or {}).get("range", 120)),
                "mode": (state.get("chart") or {}).get("mode", "candles"),
            },
            "crypto": {
                "active_instrument_id": crypto.get("active_instrument_id") or "BTCUSDT",
                "chart": {
                    "range": int((crypto.get("chart") or {}).get("range", 120)),
                    "mode": (crypto.get("chart") or {}).get("mode", "candles"),
                },
            },
            "preset": state.get("preset"),
            "active_tab": state.get("active_tab") or "overview",
            "learning": {
                "selected_lesson_id": (state.get("learning") or {}).get("selected_lesson_id"),
                "search_query": (state.get("learning") or {}).get("search_query", ""),
            },
        }
