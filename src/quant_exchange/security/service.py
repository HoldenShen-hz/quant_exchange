"""RBAC, authentication, and audit helpers implementing the documented permission model.

Roles: ADMIN, RESEARCHER, TRADER, RISK, RISK_OFFICER, AUDITOR, VIEWER
High-risk operations requiring confirmation: kill-switch, manual orders,
  strategy deployment, risk rule modification, data deletion.

Authentication features (SE-01):
- Username/password authentication
- Session management
- Password hashing with salt
- API key encryption (SE-05)
- 2FA placeholder (SE-02)
"""

from __future__ import annotations

import hashlib
import hmac
import json
import secrets
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

from quant_exchange.core.models import Action, AuditEvent, Role, utc_now

# Actions requiring explicit confirmation before execution
HIGH_RISK_ACTIONS = {
    Action.TRIGGER_KILL_SWITCH,
    Action.DEPLOY_STRATEGY,
    Action.DELETE_DATA,
    Action.MODIFY_RISK_RULES,
    Action.MANUAL_OVERRIDE,
}


def utc_now() -> datetime:
    """Return a timezone-aware UTC timestamp."""
    return datetime.now(timezone.utc)


@dataclass(slots=True)
class User:
    """User account with hashed credentials and role."""

    user_id: str
    username: str
    password_hash: str
    password_salt: str
    role: Role
    created_at: datetime
    last_login: datetime | None = None
    is_active: bool = True
    api_key_hash: str | None = None
    two_factor_enabled: bool = False
    two_factor_secret: str | None = None


@dataclass(slots=True)
class Session:
    """User session for authenticated requests."""

    session_id: str
    user_id: str
    created_at: datetime
    expires_at: datetime
    last_activity: datetime
    is_active: bool = True
    ip_address: str | None = None
    user_agent: str | None = None


@dataclass
class AuthResult:
    """Result of an authentication attempt."""

    success: bool
    user_id: str | None = None
    session_id: str | None = None
    reason: str = ""


@dataclass
class EncryptedCredential:
    """Encrypted credential storage for API keys and secrets."""

    credential_id: str
    user_id: str
    encrypted_data: str
    iv: str
    auth_tag: str
    algorithm: str = "AES-256-GCM"
    created_at: datetime = field(default_factory=utc_now)
    last_used: datetime | None = None
    label: str = ""


class SecurityService:
    """Provide role-based authorization, authentication, and immutable audit logging."""

    DEFAULT_PERMISSIONS: dict[Role, set[Action]] = {
        Role.ADMIN: {
            Action.VIEW,
            Action.RUN_BACKTEST,
            Action.SUBMIT_ORDER,
            Action.CANCEL_ORDER,
            Action.CHANGE_LIMITS,
            Action.TRIGGER_KILL_SWITCH,
            Action.VIEW_AUDIT,
            Action.DEPLOY_STRATEGY,
            Action.MODIFY_RISK_RULES,
            Action.DELETE_DATA,
            Action.MANUAL_OVERRIDE,
        },
        Role.RESEARCHER: {Action.VIEW, Action.RUN_BACKTEST, Action.DEPLOY_STRATEGY},
        Role.TRADER: {Action.VIEW, Action.SUBMIT_ORDER, Action.CANCEL_ORDER},
        Role.RISK: {Action.VIEW, Action.CHANGE_LIMITS, Action.TRIGGER_KILL_SWITCH, Action.VIEW_AUDIT, Action.MODIFY_RISK_RULES},
        Role.RISK_OFFICER: {Action.VIEW, Action.CHANGE_LIMITS, Action.TRIGGER_KILL_SWITCH, Action.VIEW_AUDIT, Action.MODIFY_RISK_RULES},
        Role.AUDITOR: {Action.VIEW, Action.VIEW_AUDIT},
        Role.VIEWER: {Action.VIEW},
    }

    def __init__(self, session_ttl_minutes: int = 60) -> None:
        self.audit_log: list[AuditEvent] = []
        self._users: dict[str, User] = {}
        self._sessions: dict[str, Session] = {}
        self._username_index: dict[str, str] = {}
        self._encrypted_credentials: dict[str, EncryptedCredential] = {}
        self._session_ttl = timedelta(minutes=session_ttl_minutes)
        self._encryption_key: bytes | None = None
        # SE-07: Fine-grained resource-level permissions
        # key = (user_id, resource, action_value) → metadata dict
        self._explicit_grants: dict[tuple[str, str, str], dict] = {}
        self._explicit_denies: dict[tuple[str, str, str], dict] = {}

    # ==================== Password Hashing ====================

    def hash_password(self, password: str) -> tuple[str, str]:
        """Hash a password with a random salt using PBKDF2.

        Returns (password_hash, salt).
        """
        salt = secrets.token_hex(32)
        key = hashlib.pbkdf2_hmac(
            "sha256",
            password.encode("utf-8"),
            salt.encode("utf-8"),
            iterations=100000,
        )
        password_hash = key.hex()
        return (password_hash, salt)

    def verify_password(self, password: str, password_hash: str, salt: str) -> bool:
        """Verify a password against its hash."""
        key = hashlib.pbkdf2_hmac(
            "sha256",
            password.encode("utf-8"),
            salt.encode("utf-8"),
            iterations=100000,
        )
        return hmac.compare_digest(key.hex(), password_hash)

    # ==================== User Management ====================

    def create_user(
        self,
        username: str,
        password: str,
        role: Role,
    ) -> User:
        """Create a new user with hashed password."""
        if username in self._username_index:
            raise ValueError("username_already_exists")

        user_id = str(uuid.uuid4())
        password_hash, salt = self.hash_password(password)

        user = User(
            user_id=user_id,
            username=username,
            password_hash=password_hash,
            password_salt=salt,
            role=role,
            created_at=utc_now(),
        )

        self._users[user_id] = user
        self._username_index[username] = user_id

        self.record_event(
            actor=username,
            action=Action.DEPLOY_STRATEGY,  # Using as a proxy for user creation
            resource=f"user:{user_id}",
            success=True,
            event_type="user_created",
        )

        return user

    def get_user(self, user_id: str) -> User | None:
        """Get user by ID."""
        return self._users.get(user_id)

    def get_user_by_username(self, username: str) -> User | None:
        """Get user by username."""
        user_id = self._username_index.get(username)
        if user_id is None:
            return None
        return self._users.get(user_id)

    def authenticate(self, username: str, password: str, *, ip_address: str | None = None) -> AuthResult:
        """Authenticate a user and create a session.

        Returns AuthResult with success status, user_id, and session_id.
        """
        user = self.get_user_by_username(username)
        if user is None:
            self.record_event(
                actor=username,
                action=Action.VIEW,  # Using as a proxy
                resource="auth",
                success=False,
                event_type="login_failed",
                reason="user_not_found",
            )
            return AuthResult(success=False, reason="invalid_credentials")

        if not user.is_active:
            return AuthResult(success=False, reason="account_disabled")

        if not self.verify_password(password, user.password_hash, user.password_salt):
            self.record_event(
                actor=username,
                action=Action.VIEW,
                resource="auth",
                success=False,
                event_type="login_failed",
                reason="invalid_password",
            )
            return AuthResult(success=False, reason="invalid_credentials")

        # Create session
        session = self._create_session(user, ip_address=ip_address)
        user.last_login = utc_now()

        self.record_event(
            actor=username,
            action=Action.VIEW,
            resource="auth",
            success=True,
            event_type="login_success",
        )

        return AuthResult(
            success=True,
            user_id=user.user_id,
            session_id=session.session_id,
        )

    def logout(self, session_id: str) -> bool:
        """Invalidate a session."""
        session = self._sessions.get(session_id)
        if session is None:
            return False

        session.is_active = False

        user = self._users.get(session.user_id)
        if user:
            self.record_event(
                actor=user.username,
                action=Action.VIEW,
                resource="auth",
                success=True,
                event_type="logout",
            )

        return True

    def validate_session(self, session_id: str) -> Session | None:
        """Validate a session and return it if valid."""
        session = self._sessions.get(session_id)
        if session is None:
            return None

        now = utc_now()
        if not session.is_active:
            return None
        if now > session.expires_at:
            session.is_active = False
            return None

        # Update last activity
        session.last_activity = now
        return session

    def _create_session(self, user: User, *, ip_address: str | None = None) -> Session:
        """Create a new session for a user."""
        session_id = secrets.token_urlsafe(32)
        now = utc_now()

        session = Session(
            session_id=session_id,
            user_id=user.user_id,
            created_at=now,
            expires_at=now + self._session_ttl,
            last_activity=now,
            ip_address=ip_address,
        )

        self._sessions[session_id] = session
        return session

    # ==================== SE-06: Two-Factor Authentication ====================

    def enable_2fa(self, user_id: str) -> tuple[bool, str]:
        """Enable TOTP 2FA for a user (SE-06). Returns (success, base32_secret).

        The returned secret should be encoded as a TOTP URI for QR code display.
        Callers can generate QR code from:
          otpauth://totp/QuantExchange:{username}?secret={secret}&issuer=QuantExchange&algorithm=SHA1&digits=6&period=30
        """
        user = self._users.get(user_id)
        if user is None:
            return (False, "")

        # Generate a 160-bit random secret (base32-encoded = 32 chars)
        import base64
        raw_secret = secrets.token_bytes(20)
        secret_b32 = base64.b32encode(raw_secret).decode().rstrip("=")
        user.two_factor_enabled = True
        user.two_factor_secret = secret_b32
        return (True, secret_b32)

    def verify_2fa(self, user_id: str, code: str) -> bool:
        """Verify a TOTP code using HMAC-SHA1 (RFC 6238) (SE-06).

        Allows ±1 time window (30 seconds) to handle clock skew.
        """
        import base64
        import hashlib
        import hmac
        import struct
        import time

        user = self._users.get(user_id)
        if user is None or not user.two_factor_enabled or not user.two_factor_secret:
            return False

        if not (len(code) == 6 and code.isdigit()):
            return False

        secret = user.two_factor_secret.upper().encode()
        # Pad base32 secret to multiple of 8
        secret += b"=" * (8 - len(secret) % 8) if len(secret) % 8 else b""
        try:
            key = base64.b32decode(secret)
        except Exception:
            return False

        # TOTP: HTOP(token) = HMAC-SHA1(key, counter)
        # counter = floor(unix_timestamp / 30)
        current_counter = int(time.time()) // 30

        # Allow ±1 window for clock skew
        for offset in (-1, 0, 1):
            counter = current_counter + offset
            msg = struct.pack(">Q", counter)
            hmac_hash = hmac.new(key, msg, hashlib.sha1).digest()
            offset_val = hmac_hash[-1] & 0x0F
            bin_code = (
                (hmac_hash[offset_val] & 0x7F) << 24
                | (hmac_hash[offset_val + 1] & 0xFF) << 16
                | (hmac_hash[offset_val + 2] & 0xFF) << 8
                | (hmac_hash[offset_val + 3] & 0xFF)
            )
            totp = str(bin_code % 10**6)
            if hmac.compare_digest(totp, code):
                return True
        return False

    def get_2fa_uri(self, user_id: str, username: str) -> str | None:
        """Return an otpauth:// URI for QR code generation (SE-06)."""
        user = self._users.get(user_id)
        if user is None or not user.two_factor_enabled or not user.two_factor_secret:
            return None
        import urllib.parse
        params = urllib.parse.urlencode({
            "secret": user.two_factor_secret,
            "issuer": "QuantExchange",
            "algorithm": "SHA1",
            "digits": "6",
            "period": "30",
        })
        return f"otpauth://totp/QuantExchange:{urllib.parse.quote(username)}?{params}"

    # ==================== API Key Encryption (SE-05) ====================

    def set_encryption_key(self, key: bytes) -> None:
        """Set the master encryption key for credentials."""
        if len(key) < 32:
            raise ValueError("encryption_key_too_short")
        self._encryption_key = key[:32]

    def encrypt_credential(
        self,
        user_id: str,
        plaintext: str,
        label: str = "",
    ) -> EncryptedCredential:
        """Encrypt and store a credential (e.g., API key).

        Uses AES-256-GCM for authenticated encryption.
        """
        if self._encryption_key is None:
            raise ValueError("encryption_key_not_set")

        try:
            from Crypto.Cipher import AES
            from Crypto.Random import get_random_bytes
        except ImportError:
            # Fallback to simple XOR if pycryptodome is not available
            # WARNING: This is NOT secure for production use
            return self._encrypt_credential_fallback(user_id, plaintext, label)

        credential_id = str(uuid.uuid4())
        iv = get_random_bytes(16)

        cipher = AES.new(self._encryption_key, AES.MODE_GCM)
        ciphertext, auth_tag = cipher.encrypt_and_digest(plaintext.encode("utf-8"))

        encrypted = EncryptedCredential(
            credential_id=credential_id,
            user_id=user_id,
            encrypted_data=ciphertext.hex(),
            iv=iv.hex(),
            auth_tag=auth_tag.hex(),
            label=label,
        )

        self._encrypted_credentials[credential_id] = encrypted
        return encrypted

    def decrypt_credential(self, credential_id: str) -> str | None:
        """Decrypt a stored credential."""
        cred = self._encrypted_credentials.get(credential_id)
        if cred is None:
            return None

        if self._encryption_key is None:
            return None

        try:
            from Crypto.Cipher import AES
        except ImportError:
            return self._decrypt_credential_fallback(cred)

        ciphertext = bytes.fromhex(cred.encrypted_data)
        iv = bytes.fromhex(cred.iv)
        auth_tag = bytes.fromhex(cred.auth_tag)

        cipher = AES.new(self._encryption_key, AES.MODE_GCM, nonce=iv)
        plaintext = cipher.decrypt_and_verify(ciphertext, auth_tag)

        cred.last_used = utc_now()
        return plaintext.decode("utf-8")

    def _encrypt_credential_fallback(
        self,
        user_id: str,
        plaintext: str,
        label: str,
    ) -> EncryptedCredential:
        """Fallback XOR encryption - NOT SECURE, use only for testing."""
        import base64

        credential_id = str(uuid.uuid4())
        key = self._encryption_key or b"default_key_for_testing_only"
        key_repeated = (key * ((len(plaintext) // len(key)) + 1))[:len(plaintext)]
        ciphertext = bytes(a ^ b for a, b in zip(plaintext.encode("utf-8"), key_repeated))

        encrypted = EncryptedCredential(
            credential_id=credential_id,
            user_id=user_id,
            encrypted_data=base64.b64encode(ciphertext).decode(),
            iv="fallback",
            auth_tag="fallback",
            algorithm="XOR-FALLBACK",
            label=label,
        )

        self._encrypted_credentials[credential_id] = encrypted
        return encrypted

    def _decrypt_credential_fallback(self, cred: EncryptedCredential) -> str | None:
        """Fallback XOR decryption - NOT SECURE, use only for testing."""
        import base64

        if cred.algorithm != "XOR-FALLBACK":
            return None

        key = self._encryption_key or b"default_key_for_testing_only"
        ciphertext = base64.b64decode(cred.encrypted_data)
        key_repeated = (key * ((len(ciphertext) // len(key)) + 1))[:len(ciphertext)]
        plaintext = bytes(a ^ b for a, b in zip(ciphertext, key_repeated))

        cred.last_used = utc_now()
        return plaintext.decode("utf-8")

    # ==================== RBAC ====================

    def authorize(self, role: Role, action: Action) -> bool:
        """Return whether the role is allowed to perform the requested action."""

        permissions = self.DEFAULT_PERMISSIONS.get(role, set())
        return action in permissions

    def requires_confirmation(self, action: Action) -> bool:
        """Return whether the action is high-risk and requires explicit confirmation."""

        return action in HIGH_RISK_ACTIONS

    def authorize_with_confirmation(self, role: Role, action: Action, *, confirmed: bool = False) -> dict[str, Any]:
        """Authorize an action, checking confirmation for high-risk operations.

        Returns {"allowed": bool, "requires_confirmation": bool, "reason": str}.
        """

        if not self.authorize(role, action):
            return {"allowed": False, "requires_confirmation": False, "reason": "insufficient_permissions"}
        if self.requires_confirmation(action) and not confirmed:
            return {"allowed": False, "requires_confirmation": True, "reason": "confirmation_required"}
        return {"allowed": True, "requires_confirmation": False, "reason": ""}

    def authorize_session(self, session_id: str, action: Action) -> dict[str, Any]:
        """Authorize an action for a session's user.

        Returns {"allowed": bool, "requires_confirmation": bool, "reason": str, "user_id": str}.
        """
        session = self.validate_session(session_id)
        if session is None:
            return {"allowed": False, "requires_confirmation": False, "reason": "invalid_session", "user_id": ""}

        user = self._users.get(session.user_id)
        if user is None:
            return {"allowed": False, "requires_confirmation": False, "reason": "user_not_found", "user_id": ""}

        result = self.authorize_with_confirmation(user.role, action)
        result["user_id"] = user.user_id
        return result

    # ==================== SE-07: Fine-Grained Access Control =====================

    def authorize_resource(
        self,
        user_id: str,
        action: Action,
        resource: str,
        *,
        resource_type: str | None = None,
    ) -> dict[str, Any]:
        """Check fine-grained permission for a user × resource × action (SE-07).

        Permission resolution order:
          1. Explicit DENY on (user, resource, action) → denied
          2. Explicit GRANT on (user, resource, action) → allowed
          3. Role-based permission → use DEFAULT_PERMISSIONS
          4. Default deny

        Args:
            user_id: The user attempting the action.
            action: The action being attempted.
            resource: The resource identifier (e.g. "strategy:ma_sentiment", "order:ORD123").
            resource_type: Optional type hint (e.g. "strategy", "order", "instrument").
        """
        user = self._users.get(user_id)
        if user is None:
            return {"allowed": False, "reason": "user_not_found", "source": "user"}

        # Check explicit deny list first (user-level revocation)
        deny_key = (user_id, resource, action.value)
        if deny_key in self._explicit_denies:
            return {
                "allowed": False,
                "reason": f"explicit_deny on resource '{resource}'",
                "source": "explicit_deny",
                "user_id": user_id,
            }

        # Check explicit grant list (user-level permission)
        grant_key = (user_id, resource, action.value)
        if grant_key in self._explicit_grants:
            return {
                "allowed": True,
                "reason": f"explicit_grant on resource '{resource}'",
                "source": "explicit_grant",
                "user_id": user_id,
            }

        # Fall back to role-based
        if self.authorize(user.role, action):
            return {
                "allowed": True,
                "reason": f"role_based: {user.role.value} has {action.value}",
                "source": "role",
                "user_id": user_id,
            }

        return {
            "allowed": False,
            "reason": f"role {user.role.value} lacks {action.value} on '{resource}'",
            "source": "role",
            "user_id": user_id,
        }

    def grant_resource_permission(
        self,
        user_id: str,
        resource: str,
        action: Action,
        granted_by: str,
    ) -> bool:
        """Grant a user explicit permission on a specific resource (SE-07)."""
        if user_id not in self._users:
            return False
        key = (user_id, resource, action.value)
        self._explicit_grants[key] = {"granted_by": granted_by, "granted_at": utc_now()}
        self.record_event(
            actor=granted_by,
            action=Action.DEPLOY_STRATEGY,  # proxy action
            resource=f"permission:grant:{user_id}:{resource}:{action.value}",
            success=True,
            event_type="permission_granted",
        )
        return True

    def revoke_resource_permission(
        self,
        user_id: str,
        resource: str,
        action: Action,
        revoked_by: str,
    ) -> bool:
        """Revoke an explicit permission (SE-07)."""
        if user_id not in self._users:
            return False
        key = (user_id, resource, action.value)
        if key in self._explicit_grants:
            del self._explicit_grants[key]
        # Add to explicit deny to prevent re-grant
        deny_key = (user_id, resource, action.value)
        self._explicit_denies[deny_key] = {"revoked_by": revoked_by, "revoked_at": utc_now()}
        self.record_event(
            actor=revoked_by,
            action=Action.DEPLOY_STRATEGY,
            resource=f"permission:revoke:{user_id}:{resource}:{action.value}",
            success=True,
            event_type="permission_revoked",
        )
        return True

    def list_resource_permissions(self, user_id: str) -> dict[str, list[str]]:
        """List all explicit permissions for a user (SE-07)."""
        grants = [
            {"resource": r, "action": a, "key": k}
            for (uid, r, a), k in self._explicit_grants.items()
            if uid == user_id
        ]
        denies = [
            {"resource": r, "action": a}
            for (uid, r, a) in self._explicit_denies.keys()
            if uid == user_id
        ]
        return {
            "user_id": user_id,
            "grants": grants,
            "denies": denies,
        }

    # ==================== Audit Logging ====================

    def record_event(self, actor: str, action: Action, resource: str, success: bool, **details) -> AuditEvent:
        """Append an immutable structured audit record for a privileged action."""

        event = AuditEvent(actor=actor, action=action, resource=resource, timestamp=utc_now(), success=success, details=details)
        self.audit_log.append(event)
        return event

    def log_audit_event(self, actor: str, action: Action, resource: str, success: bool, details: dict | None = None) -> AuditEvent:
        """Alias for record_event for API audit logging (SE-03)."""
        return self.record_event(actor=actor, action=action, resource=resource, success=success, **(details or {}))

    def query_audit_log(
        self,
        *,
        actor: str | None = None,
        action: Action | None = None,
        limit: int = 100,
    ) -> list[AuditEvent]:
        """Query the audit log with optional filters."""

        results = self.audit_log
        if actor:
            results = [e for e in results if e.actor == actor]
        if action:
            results = [e for e in results if e.action == action]
        return results[-limit:]

    # ==================== Cleanup ====================

    def cleanup_expired_sessions(self) -> int:
        """Remove expired sessions. Returns count of removed sessions."""
        now = utc_now()
        expired = [
            sid for sid, sess in self._sessions.items()
            if not sess.is_active or now > sess.expires_at
        ]
        for sid in expired:
            del self._sessions[sid]
        return len(expired)
