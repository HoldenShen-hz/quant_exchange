"""Mobile PWA service layer (MOB-01 ~ MOB-05)."""

from __future__ import annotations

import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any


# ─────────────────────────────────────────────────────────────────────────────
# Enums
# ─────────────────────────────────────────────────────────────────────────────

class BiometricType(Enum):
    """Supported biometric authentication types."""

    NONE = "none"
    FINGERPRINT = "fingerprint"
    FACE_ID = "face_id"
    TOUCH_ID = "touch_id"
    IRIS = "iris"


class NotificationCategory(Enum):
    """Push notification categories for mobile (MOB-03)."""

    TRADE_EXECUTION = "trade_execution"  # Order fills, cancellations
    PRICE_ALERTS = "price_alerts"  # Price alerts
    PORTFOLIO = "portfolio"  # Portfolio updates, P&L
    NEWS = "news"  # Market news, events
    SYSTEM = "system"  # System notifications, maintenance
    SOCIAL = "social"  # Social features, follows
    RESEARCH = "research"  # Research alerts, reports
    ALL = "all"


class GestureAction(Enum):
    """Supported gesture-based quick actions (MOB-04)."""

    SWIPE_LEFT_ORDER = "swipe_left_order"
    SWIPE_RIGHT_CANCEL = "swipe_right_cancel"
    LONG_PRESS_QUOTE = "long_press_quote"
    DOUBLE_TAP_WATCHLIST = "double_tap_watchlist"
    PINCH_CHART = "pinch_chart"
    TWO_FINGER_SCROLL = "two_finger_scroll"


# ─────────────────────────────────────────────────────────────────────────────
# Mobile Configuration Models
# ─────────────────────────────────────────────────────────────────────────────

@dataclass(slots=True)
class PWAManifest:
    """PWA manifest configuration for mobile installation (MOB-01)."""

    app_name: str = "QuantExchange"
    short_name: str = "QuantEx"
    version: str = "1.0.0"
    start_url: str = "/"
    display: str = "standalone"  # standalone, fullscreen, minimal-ui
    orientation: str = "portrait"  # portrait, landscape
    background_color: str = "#0D1117"
    theme_color: str = "#1C2128"
    icons: tuple[dict[str, str], ...] = field(default_factory=lambda: (
        {"src": "/icons/icon-72.png", "sizes": "72x72", "type": "image/png"},
        {"src": "/icons/icon-96.png", "sizes": "96x96", "type": "image/png"},
        {"src": "/icons/icon-128.png", "sizes": "128x128", "type": "image/png"},
        {"src": "/icons/icon-144.png", "sizes": "144x144", "type": "image/png"},
        {"src": "/icons/icon-152.png", "sizes": "152x152", "type": "image/png"},
        {"src": "/icons/icon-192.png", "sizes": "192x192", "type": "image/png"},
        {"src": "/icons/icon-384.png", "sizes": "384x384", "type": "image/png"},
        {"src": "/icons/icon-512.png", "sizes": "512x512", "type": "image/png"},
    ))
    categories: tuple[str, ...] = ("finance", "trading")
    language: str = "en"
    cache_strategy: str = "stale-while-revalidate"


@dataclass(slots=True)
class OfflineCacheConfig:
    """Offline caching configuration (MOB-01)."""

    enabled: bool = True
    max_cache_size_mb: int = 100
    cache_ttl_hours: int = 24
    cached_endpoints: tuple[str, ...] = (
        "/api/v1/watchlist",
        "/api/v1/portfolio",
        "/api/v1/positions",
    )
    cached_data_types: tuple[str, ...] = (
        "stock_profile",
        "intraday_quote",
        "portfolio_summary",
    )


@dataclass(slots=True)
class NotificationPreferences:
    """Mobile push notification preferences (MOB-03)."""

    user_id: str
    enabled: bool = True
    categories: tuple[NotificationCategory, ...] = (NotificationCategory.ALL,)
    sound_enabled: bool = True
    vibration_enabled: bool = True
    badge_enabled: bool = True
    quiet_hours_enabled: bool = False
    quiet_hours_start: str = "22:00"
    quiet_hours_end: str = "08:00"
    language: str = "en"


@dataclass(slots=True)
class GesturePreferences:
    """Gesture interaction preferences (MOB-04)."""

    user_id: str
    enabled_gestures: tuple[GestureAction, ...] = (
        GestureAction.SWIPE_LEFT_ORDER,
        GestureAction.SWIPE_RIGHT_CANCEL,
        GestureAction.LONG_PRESS_QUOTE,
        GestureAction.DOUBLE_TAP_WATCHLIST,
    )
    haptic_feedback: bool = True
    gesture_sensitivity: float = 1.0  # 0.5 - 2.0


@dataclass(slots=True)
class BiometricSettings:
    """Biometric authentication settings (MOB-05)."""

    user_id: str
    biometric_type: BiometricType = BiometricType.NONE
    enabled: bool = False
    require_for_withdrawal: bool = True
    require_for_login: bool = False
    last_verified: str | None = None


@dataclass(slots=True)
class MobileSession:
    """Mobile app session state."""

    session_id: str
    user_id: str
    device_id: str
    device_name: str
    os_version: str
    app_version: str
    push_token: str | None = None
    language: str = "en"
    timezone: str = "UTC"
    created_at: str | None = None
    last_active_at: str | None = None


# ─────────────────────────────────────────────────────────────────────────────
# MobileService
# ─────────────────────────────────────────────────────────────────────────────

class MobileService:
    """Mobile PWA backend service (MOB-01 ~ MOB-05).

    Provides:
    - PWA manifest and offline cache configuration
    - Push notification preferences and device registration
    - Gesture and biometric authentication settings
    - Mobile session management
    """

    def __init__(self, persistence=None) -> None:
        self.persistence = persistence
        self._sessions: dict[str, MobileSession] = {}
        self._notification_prefs: dict[str, NotificationPreferences] = {}
        self._gesture_prefs: dict[str, GesturePreferences] = {}
        self._biometric_settings: dict[str, BiometricSettings] = {}

    # ── PWA Configuration ────────────────────────────────────────────────────

    def get_pwa_manifest(self, user_id: str | None = None) -> PWAManifest:
        """Return PWA manifest for mobile installation (MOB-01)."""
        return PWAManifest()

    def get_offline_cache_config(self, user_id: str | None = None) -> OfflineCacheConfig:
        """Return offline cache configuration (MOB-01)."""
        return OfflineCacheConfig()

    def generate_service_worker_config(self) -> dict[str, Any]:
        """Generate service worker configuration for offline support."""
        return {
            "version": "1.0.0",
            "strategies": {
                "static": {
                    "handler": "cacheFirst",
                    "urls": ["/icons/*", "/static/*", "/*.png", "/*.svg"],
                },
                "api": {
                    "handler": "staleWhileRevalidate",
                    "urls": [
                        "/api/v1/watchlist",
                        "/api/v1/portfolio",
                        "/api/v1/positions",
                    ],
                },
                "market_data": {
                    "handler": "networkFirst",
                    "urls": ["/api/v1/quotes/*"],
                    "networkTimeoutSeconds": 5,
                },
            },
            "skip_waiting": True,
            "clients_claim": True,
        }

    # ── Push Notifications ─────────────────────────────────────────────────

    def get_notification_preferences(self, user_id: str) -> NotificationPreferences:
        """Get notification preferences for a user (MOB-03)."""
        if user_id in self._notification_prefs:
            return self._notification_prefs[user_id]
        prefs = NotificationPreferences(user_id=user_id)
        self._notification_prefs[user_id] = prefs
        return prefs

    def update_notification_preferences(
        self,
        user_id: str,
        *,
        enabled: bool | None = None,
        categories: tuple[NotificationCategory, ...] | None = None,
        sound_enabled: bool | None = None,
        vibration_enabled: bool | None = None,
        badge_enabled: bool | None = None,
        quiet_hours_enabled: bool | None = None,
        quiet_hours_start: str | None = None,
        quiet_hours_end: str | None = None,
    ) -> NotificationPreferences:
        """Update notification preferences for a user."""
        prefs = self.get_notification_preferences(user_id)
        updated = NotificationPreferences(
            user_id=user_id,
            enabled=enabled if enabled is not None else prefs.enabled,
            categories=categories if categories is not None else prefs.categories,
            sound_enabled=sound_enabled if sound_enabled is not None else prefs.sound_enabled,
            vibration_enabled=vibration_enabled if vibration_enabled is not None else prefs.vibration_enabled,
            badge_enabled=badge_enabled if badge_enabled is not None else prefs.badge_enabled,
            quiet_hours_enabled=quiet_hours_enabled if quiet_hours_enabled is not None else prefs.quiet_hours_enabled,
            quiet_hours_start=quiet_hours_start if quiet_hours_start is not None else prefs.quiet_hours_start,
            quiet_hours_end=quiet_hours_end if quiet_hours_end is not None else prefs.quiet_hours_end,
        )
        self._notification_prefs[user_id] = updated
        self._persist_mobile_config(user_id, "notification_preferences", asdict(updated))
        return updated

    def should_send_notification(
        self, user_id: str, category: NotificationCategory
    ) -> bool:
        """Check if a notification should be sent based on user preferences."""
        prefs = self.get_notification_preferences(user_id)
        if not prefs.enabled:
            return False
        if NotificationCategory.ALL in prefs.categories:
            return True
        if category not in prefs.categories:
            return False
        # Check quiet hours
        if prefs.quiet_hours_enabled:
            now = datetime.now(timezone.utc)
            current_time = now.strftime("%H:%M")
            if prefs.quiet_hours_start <= current_time <= prefs.quiet_hours_end:
                return False
        return True

    # ── Device Registration ─────────────────────────────────────────────────

    def register_device(
        self,
        user_id: str,
        device_id: str,
        device_name: str,
        os_version: str,
        app_version: str,
        push_token: str | None = None,
    ) -> MobileSession:
        """Register a mobile device for push notifications."""
        session_id = f"mob_{uuid.uuid4().hex[:16]}"
        session = MobileSession(
            session_id=session_id,
            user_id=user_id,
            device_id=device_id,
            device_name=device_name,
            os_version=os_version,
            app_version=app_version,
            push_token=push_token,
            created_at=datetime.now(timezone.utc).isoformat(),
            last_active_at=datetime.now(timezone.utc).isoformat(),
        )
        self._sessions[session_id] = session
        self._persist_mobile_config(user_id, "sessions", [asdict(s) for s in self._sessions.values() if s.user_id == user_id])
        return session

    def update_device_token(self, session_id: str, push_token: str) -> bool:
        """Update push token for a device."""
        session = self._sessions.get(session_id)
        if session is None:
            return False
        session.push_token = push_token
        return True

    def get_user_sessions(self, user_id: str) -> list[MobileSession]:
        """Get all registered sessions for a user."""
        return [s for s in self._sessions.values() if s.user_id == user_id]

    # ── Gesture Preferences ─────────────────────────────────────────────────

    def get_gesture_preferences(self, user_id: str) -> GesturePreferences:
        """Get gesture preferences for a user (MOB-04)."""
        if user_id in self._gesture_prefs:
            return self._gesture_prefs[user_id]
        prefs = GesturePreferences(user_id=user_id)
        self._gesture_prefs[user_id] = prefs
        return prefs

    def update_gesture_preferences(
        self,
        user_id: str,
        *,
        enabled_gestures: tuple[GestureAction, ...] | None = None,
        haptic_feedback: bool | None = None,
        gesture_sensitivity: float | None = None,
    ) -> GesturePreferences:
        """Update gesture preferences for a user."""
        prefs = self.get_gesture_preferences(user_id)
        updated = GesturePreferences(
            user_id=user_id,
            enabled_gestures=enabled_gestures if enabled_gestures is not None else prefs.enabled_gestures,
            haptic_feedback=haptic_feedback if haptic_feedback is not None else prefs.haptic_feedback,
            gesture_sensitivity=gesture_sensitivity if gesture_sensitivity is not None else prefs.gesture_sensitivity,
        )
        self._gesture_prefs[user_id] = updated
        self._persist_mobile_config(user_id, "gesture_preferences", asdict(updated))
        return updated

    # ── Biometric Settings ─────────────────────────────────────────────────

    def get_biometric_settings(self, user_id: str) -> BiometricSettings:
        """Get biometric authentication settings for a user (MOB-05)."""
        if user_id in self._biometric_settings:
            return self._biometric_settings[user_id]
        settings = BiometricSettings(user_id=user_id)
        self._biometric_settings[user_id] = settings
        return settings

    def update_biometric_settings(
        self,
        user_id: str,
        *,
        biometric_type: BiometricType | None = None,
        enabled: bool | None = None,
        require_for_withdrawal: bool | None = None,
        require_for_login: bool | None = None,
    ) -> BiometricSettings:
        """Update biometric authentication settings for a user."""
        settings = self.get_biometric_settings(user_id)
        updated = BiometricSettings(
            user_id=user_id,
            biometric_type=biometric_type if biometric_type is not None else settings.biometric_type,
            enabled=enabled if enabled is not None else settings.enabled,
            require_for_withdrawal=require_for_withdrawal if require_for_withdrawal is not None else settings.require_for_withdrawal,
            require_for_login=require_for_login if require_for_login is not None else settings.require_for_login,
            last_verified=settings.last_verified,
        )
        self._biometric_settings[user_id] = updated
        self._persist_mobile_config(user_id, "biometric_settings", asdict(updated))
        return updated

    def verify_biometric(self, user_id: str, biometric_type: BiometricType) -> bool:
        """Record a successful biometric verification."""
        settings = self.get_biometric_settings(user_id)
        if settings.biometric_type != biometric_type or not settings.enabled:
            return False
        settings.last_verified = datetime.now(timezone.utc).isoformat()
        return True

    def requires_biometric(self, user_id: str, action: str) -> bool:
        """Check if an action requires biometric verification."""
        settings = self.get_biometric_settings(user_id)
        if not settings.enabled:
            return False
        if action == "withdrawal" and settings.require_for_withdrawal:
            return True
        if action == "login" and settings.require_for_login:
            return True
        return False

    # ── Persistence ─────────────────────────────────────────────────────────

    def _persist_mobile_config(self, user_id: str, config_type: str, data: Any) -> None:
        """Persist mobile configuration to storage."""
        if self.persistence is not None:
            self.persistence.upsert_record(
                "mobile_configs", "config_key", f"{user_id}:{config_type}", data
            )
