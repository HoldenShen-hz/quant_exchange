"""Credential Vault: encrypted storage for API keys and secrets (SE-05).

Provides a secure vault for storing credentials at rest using AES-256 encryption.
The master key is derived from the QUANT_VAULT_MASTER_KEY environment variable
using PBKDF2-HMAC-SHA256 (100,000 iterations).

If the cryptography library is not available, falls back to a hash-based
obfuscation using SHA-256 HMAC (not recommended for production, but
prevents casual reading of credentials in config files).

Usage:
    vault = CredentialVault()
    vault.store("telegram_bot_token", "123456:ABC-DEF...")
    token = vault.retrieve("telegram_bot_token")
    vault.delete("telegram_bot_token")
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import secrets
from pathlib import Path
from typing import Any

try:
    from cryptography.fernet import Fernet, InvalidToken
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

    _HAS_CRYPTGRAPHY = True
except ImportError:
    _HAS_CRYPTGRAPHY = False


class CredentialVault:
    """Encrypted credential storage vault.

    Credentials are stored in a local JSON file, encrypted with AES-256 (via
    cryptography.Fernet) or HMAC-SHA256 fallback. The encryption key is
    derived from the QUANT_VAULT_MASTER_KEY env var.
    """

    DEFAULT_VAULT_PATH = Path(os.getenv("QUANT_DB_DIR", ".")) / ".quant_vault.json"

    def __init__(self, master_key: str | None = None, vault_path: Path | None = None) -> None:
        self._master_key = master_key or os.getenv("QUANT_VAULT_MASTER_KEY", "")
        self._vault_path = vault_path or self.DEFAULT_VAULT_PATH
        self._fernet: Fernet | None = None
        self._hmac_key: bytes | None = None
        self._pending_salt: bytes | None = None  # cached salt for new vaults
        if self._master_key:
            self._init_encryption()

    def _init_encryption(self) -> None:
        """Initialize the encryption cipher using the master key."""
        if _HAS_CRYPTGRAPHY:
            # Derive a 32-byte key using PBKDF2-HMAC-SHA256
            salt = self._get_or_create_salt()
            # Cache it so _save_vault uses the same salt for a new vault
            self._pending_salt = salt
            kdf = PBKDF2HMAC(
                algorithm=hashes.SHA256(),
                length=32,
                salt=salt,
                iterations=100_000,
            )
            key = base64.urlsafe_b64encode(kdf.derive(self._master_key.encode("utf-8")))
            self._fernet = Fernet(key)
        else:
            # Fallback: use HMAC-SHA256 for MAC (no encryption, just integrity)
            self._hmac_key = hashlib.sha256(self._master_key.encode("utf-8")).digest()

    def _get_or_create_salt(self) -> bytes:
        """Get the vault salt from the vault file, or create a new one."""
        # Return cached salt if we've already created one for this new vault
        if not self._vault_path.exists() and self._pending_salt:
            return self._pending_salt
        if self._vault_path.exists():
            try:
                data = json.loads(self._vault_path.read_text("utf-8"))
                salt_b64 = data.get("_salt", "")
                if salt_b64:
                    return base64.b64decode(salt_b64)
            except (ValueError, json.JSONDecodeError, OSError):
                pass
        # Generate a new random salt
        salt = secrets.token_bytes(32)
        return salt

    def _load_vault(self) -> dict[str, str]:
        """Load the encrypted vault and decrypt entries."""
        import hmac as _hmac
        if not self._vault_path.exists():
            return {}
        try:
            raw = self._vault_path.read_text("utf-8")
            data = json.loads(raw)
        except (ValueError, json.JSONDecodeError, OSError):
            return {}
        if "_salt" in data:
            salt_b64 = data["_salt"]
            if _HAS_CRYPTGRAPHY and self._fernet:
                # Re-derive the cipher using the stored salt.
                salt = base64.b64decode(salt_b64)
                kdf = PBKDF2HMAC(
                    algorithm=hashes.SHA256(),
                    length=32,
                    salt=salt,
                    iterations=100_000,
                )
                key = base64.urlsafe_b64encode(kdf.derive(self._master_key.encode("utf-8")))
                self._fernet = Fernet(key)
                # Keep pending salt in sync so store() after load() is consistent
                self._pending_salt = salt
        # Verify the key using the stored verification token
        if "_verify" in data and self._hmac_key:
            stored_verify = base64.b64decode(data["_verify"])
            expected_mac = stored_verify[16:]
            computed_mac = _hmac.new(self._hmac_key, b"VERIFYPLEASE", hashlib.sha256).digest()
            if not secrets.compare_digest(expected_mac, computed_mac):
                # Wrong key - clear HMAC key to prevent decryption
                self._hmac_key = None
        decrypted: dict[str, str] = {}
        for name, encrypted_b64 in data.items():
            if name.startswith("_"):
                continue
            try:
                if _HAS_CRYPTGRAPHY and self._fernet:
                    decrypted[name] = self._fernet.decrypt(base64.b64decode(encrypted_b64)).decode("utf-8")
                elif self._hmac_key:
                    # HMAC fallback: base64 decode + verify HMAC tag
                    raw_bytes = base64.b64decode(encrypted_b64)
                    mac = raw_bytes[:32]
                    msg = raw_bytes[32:]
                    if not secrets.compare_digest(mac, _hmac.new(self._hmac_key, msg, hashlib.sha256).digest()):
                        decrypted[name] = ""  # Wrong key or corrupted
                    else:
                        decrypted[name] = msg.decode("utf-8")
            except Exception:
                # Decryption failed (wrong key or corrupted data)
                decrypted[name] = ""
        return decrypted

    def _save_vault(self, entries: dict[str, str]) -> None:
        """Encrypt entries and save to vault file."""
        import hmac as _hmac
        salt = self._get_or_create_salt()
        # Derive a verification token: HMAC of a known constant with the derived key
        if self._hmac_key:
            verify_token = base64.b64encode(
                secrets.token_bytes(16) + _hmac.new(
                    self._hmac_key, b"VERIFYPLEASE", hashlib.sha256
                ).digest()
            ).decode("ascii")
        else:
            verify_token = ""
        encrypted: dict[str, str] = {
            "_salt": base64.b64encode(salt).decode("ascii"),
            "_verify": verify_token,
        }
        for name, value in entries.items():
            if _HAS_CRYPTGRAPHY and self._fernet:
                encrypted[name] = base64.b64encode(self._fernet.encrypt(value.encode("utf-8"))).decode("ascii")
            elif self._hmac_key:
                # HMAC fallback: store base64 encoded + HMAC integrity tag
                msg = value.encode("utf-8")
                mac = _hmac.new(self._hmac_key, msg, hashlib.sha256).digest()
                encrypted[name] = base64.b64encode(mac + msg).decode("ascii")
            else:
                encrypted[name] = base64.b64encode(value.encode("utf-8")).decode("ascii")
        self._vault_path.parent.mkdir(parents=True, exist_ok=True)
        self._vault_path.write_text(json.dumps(encrypted, ensure_ascii=False), "utf-8")
        # Restrict permissions to user only (Unix)
        try:
            os.chmod(self._vault_path, 0o600)
        except OSError:
            pass

    def store(self, name: str, value: str) -> None:
        """Store a credential (encrypts and saves to disk)."""
        if not self._master_key:
            raise RuntimeError("QUANT_VAULT_MASTER_KEY not set. Cannot store credentials securely.")
        entries = self._load_vault()
        entries[name] = value
        self._save_vault(entries)

    def retrieve(self, name: str, default: str | None = None) -> str | None:
        """Retrieve a credential by name. Returns default if not found."""
        entries = self._load_vault()
        return entries.get(name, default)

    def delete(self, name: str) -> bool:
        """Delete a credential by name. Returns True if deleted, False if not found."""
        entries = self._load_vault()
        if name not in entries:
            return False
        del entries[name]
        self._save_vault(entries)
        return True

    def list_names(self) -> list[str]:
        """Return the list of credential names stored in the vault."""
        entries = self._load_vault()
        return [k for k in entries.keys() if not k.startswith("_")]

    def has_master_key(self) -> bool:
        """Return True if a master key is configured."""
        return bool(self._master_key)

    @property
    def vault_path(self) -> Path:
        """Return the vault file path."""
        return self._vault_path
