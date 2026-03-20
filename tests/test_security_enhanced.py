"""Tests for enhanced security features.

Tests:
- Password hashing and verification
- User creation and authentication
- Session management
- 2FA placeholder
- Credential encryption
"""

from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from quant_exchange.core.models import Action, Role
from quant_exchange.security import SecurityService


def utc_now() -> datetime:
    """Return a timezone-aware UTC timestamp."""
    return datetime.now(timezone.utc)


class PasswordHashingTests(unittest.TestCase):
    """Test password hashing and verification."""

    def setUp(self) -> None:
        self.service = SecurityService()

    def test_hash_password_returns_hash_and_salt(self) -> None:
        """Verify hash_password returns both hash and salt."""
        password_hash, salt = self.service.hash_password("test_password")

        self.assertIsInstance(password_hash, str)
        self.assertIsInstance(salt, str)
        self.assertGreater(len(password_hash), 0)
        self.assertGreater(len(salt), 0)

    def test_verify_password_correct_password(self) -> None:
        """Verify correct password is accepted."""
        password_hash, salt = self.service.hash_password("test_password")

        result = self.service.verify_password("test_password", password_hash, salt)

        self.assertTrue(result)

    def test_verify_password_wrong_password(self) -> None:
        """Verify wrong password is rejected."""
        password_hash, salt = self.service.hash_password("test_password")

        result = self.service.verify_password("wrong_password", password_hash, salt)

        self.assertFalse(result)

    def test_different_passwords_produce_different_hashes(self) -> None:
        """Verify different passwords produce different hashes."""
        hash1, salt1 = self.service.hash_password("password1")
        hash2, salt2 = self.service.hash_password("password2")

        self.assertNotEqual(hash1, hash2)


class UserManagementTests(unittest.TestCase):
    """Test user creation and management."""

    def setUp(self) -> None:
        self.service = SecurityService()

    def test_create_user(self) -> None:
        """Verify user is created with hashed password."""
        user = self.service.create_user(
            username="testuser",
            password="secure_password",
            role=Role.TRADER,
        )

        self.assertEqual(user.username, "testuser")
        self.assertEqual(user.role, Role.TRADER)
        self.assertNotEqual(user.password_hash, "secure_password")
        self.assertTrue(user.is_active)

    def test_create_duplicate_username_raises(self) -> None:
        """Verify creating duplicate username raises error."""
        self.service.create_user("testuser", "password", Role.TRADER)

        with self.assertRaises(ValueError):
            self.service.create_user("testuser", "password2", Role.TRADER)

    def test_get_user_by_username(self) -> None:
        """Verify getting user by username works."""
        self.service.create_user("testuser", "password", Role.TRADER)

        user = self.service.get_user_by_username("testuser")

        self.assertIsNotNone(user)
        self.assertEqual(user.username, "testuser")

    def test_get_user_by_username_not_found(self) -> None:
        """Verify non-existent username returns None."""
        user = self.service.get_user_by_username("nonexistent")

        self.assertIsNone(user)


class AuthenticationTests(unittest.TestCase):
    """Test authentication and session management."""

    def setUp(self) -> None:
        self.service = SecurityService()
        self.service.create_user("testuser", "password123", Role.TRADER)

    def test_authenticate_success(self) -> None:
        """Verify successful authentication returns session."""
        result = self.service.authenticate("testuser", "password123")

        self.assertTrue(result.success)
        self.assertIsNotNone(result.user_id)
        self.assertIsNotNone(result.session_id)

    def test_authenticate_wrong_password(self) -> None:
        """Verify wrong password is rejected."""
        result = self.service.authenticate("testuser", "wrong_password")

        self.assertFalse(result.success)
        self.assertEqual(result.reason, "invalid_credentials")

    def test_authenticate_nonexistent_user(self) -> None:
        """Verify nonexistent user is rejected."""
        result = self.service.authenticate("nonexistent", "password")

        self.assertFalse(result.success)
        self.assertEqual(result.reason, "invalid_credentials")

    def test_authenticate_inactive_user(self) -> None:
        """Verify inactive user is rejected."""
        user = self.service.get_user_by_username("testuser")
        user.is_active = False

        result = self.service.authenticate("testuser", "password123")

        self.assertFalse(result.success)
        self.assertEqual(result.reason, "account_disabled")

    def test_validate_session(self) -> None:
        """Verify session validation works."""
        auth_result = self.service.authenticate("testuser", "password123")
        session = self.service.validate_session(auth_result.session_id)

        self.assertIsNotNone(session)
        self.assertEqual(session.user_id, auth_result.user_id)

    def test_validate_expired_session(self) -> None:
        """Verify expired session is invalidated."""
        auth_result = self.service.authenticate("testuser", "password123")
        session = self.service._sessions[auth_result.session_id]
        session.expires_at = utc_now() - timedelta(hours=1)

        validated = self.service.validate_session(auth_result.session_id)

        self.assertIsNone(validated)

    def test_logout(self) -> None:
        """Verify logout invalidates session."""
        auth_result = self.service.authenticate("testuser", "password123")

        result = self.service.logout(auth_result.session_id)

        self.assertTrue(result)
        self.assertFalse(self.service._sessions[auth_result.session_id].is_active)

    def test_authorize_session_success(self) -> None:
        """Verify authorize_session allows valid session with correct role."""
        auth_result = self.service.authenticate("testuser", "password123")

        result = self.service.authorize_session(
            auth_result.session_id,
            Action.SUBMIT_ORDER,
        )

        self.assertTrue(result["allowed"])
        self.assertEqual(result["user_id"], auth_result.user_id)

    def test_authorize_session_invalid(self) -> None:
        """Verify authorize_session rejects invalid session."""
        result = self.service.authorize_session(
            "invalid_session_id",
            Action.SUBMIT_ORDER,
        )

        self.assertFalse(result["allowed"])
        self.assertEqual(result["reason"], "invalid_session")


class TwoFactorAuthTests(unittest.TestCase):
    """Test 2FA functionality (placeholder implementation)."""

    def setUp(self) -> None:
        self.service = SecurityService()
        self.user = self.service.create_user("testuser", "password", Role.TRADER)

    def test_enable_2fa_returns_secret(self) -> None:
        """Verify enabling 2FA returns a secret."""
        success, secret = self.service.enable_2fa(self.user.user_id)

        self.assertTrue(success)
        self.assertGreater(len(secret), 0)

    def test_verify_2fa_accepts_valid_code(self) -> None:
        """Verify 2FA accepts a real TOTP code generated from the user's secret."""
        success, secret = self.service.enable_2fa(self.user.user_id)
        self.assertTrue(success)
        self.assertGreater(len(secret), 0)

        # Generate a real TOTP code from the user's secret (same algorithm as service)
        import base64
        import hashlib
        import hmac
        import struct
        import time
        raw = secret.upper().encode()
        # Pad to multiple of 8
        raw += b"=" * ((8 - len(raw) % 8) % 8)
        key = base64.b32decode(raw)
        counter = int(time.time()) // 30
        msg = struct.pack(">Q", counter)
        hmac_hash = hmac.new(key, msg, hashlib.sha1).digest()
        offset_val = hmac_hash[-1] & 0x0F
        bin_code = (
            (hmac_hash[offset_val] & 0x7F) << 24
            | (hmac_hash[offset_val + 1] & 0xFF) << 16
            | (hmac_hash[offset_val + 2] & 0xFF) << 8
            | (hmac_hash[offset_val + 3] & 0xFF)
        )
        real_totp = str(bin_code % 10**6)
        result = self.service.verify_2fa(self.user.user_id, real_totp)
        self.assertTrue(result)

    def test_verify_2fa_rejects_invalid_code(self) -> None:
        """Verify 2FA rejects non-6-digit code."""
        self.service.enable_2fa(self.user.user_id)

        result = self.service.verify_2fa(self.user.user_id, "12345")  # Too short

        self.assertFalse(result)


class CredentialEncryptionTests(unittest.TestCase):
    """Test API key encryption functionality."""

    def setUp(self) -> None:
        self.service = SecurityService()
        # 32 bytes exactly
        self.service.set_encryption_key(b"12345678901234567890123456789012")

    def test_encrypt_decrypt_credential(self) -> None:
        """Verify credential can be encrypted and decrypted."""
        user = self.service.create_user("testuser", "password", Role.TRADER)

        encrypted = self.service.encrypt_credential(
            user.user_id,
            "my_secret_api_key",
            label="Binance API",
        )

        self.assertIsNotNone(encrypted.credential_id)
        self.assertEqual(encrypted.label, "Binance API")

        decrypted = self.service.decrypt_credential(encrypted.credential_id)

        self.assertEqual(decrypted, "my_secret_api_key")

    def test_decrypt_nonexistent_credential(self) -> None:
        """Verify decrypting nonexistent credential returns None."""
        result = self.service.decrypt_credential("nonexistent_id")

        self.assertIsNone(result)

    def test_encrypt_without_key_raises(self) -> None:
        """Verify encrypting without key raises error."""
        service_no_key = SecurityService()
        user = service_no_key.create_user("testuser", "password", Role.TRADER)

        with self.assertRaises(ValueError):
            service_no_key.encrypt_credential(user.user_id, "secret")


class CleanupTests(unittest.TestCase):
    """Test session cleanup functionality."""

    def setUp(self) -> None:
        self.service = SecurityService()
        self.service.create_user("testuser", "password", Role.TRADER)

    def test_cleanup_expired_sessions(self) -> None:
        """Verify expired sessions are removed."""
        auth_result = self.service.authenticate("testuser", "password")
        session = self.service._sessions[auth_result.session_id]
        session.expires_at = utc_now() - timedelta(hours=1)

        count = self.service.cleanup_expired_sessions()

        self.assertEqual(count, 1)
        self.assertNotIn(auth_result.session_id, self.service._sessions)


if __name__ == "__main__":
    unittest.main()
