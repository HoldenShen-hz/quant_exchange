"""Tests for SE-05: Credential Vault (encryption at rest)."""

from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path

from quant_exchange.core.vault import CredentialVault


class CredentialVaultTests(unittest.TestCase):
    """Test SE-05: Credential encrypted storage."""

    def setUp(self) -> None:
        self.tmpdir = tempfile.mkdtemp()
        self.vault_path = Path(self.tmpdir) / ".test_vault.json"

    def tearDown(self) -> None:
        import shutil
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_store_and_retrieve(self) -> None:
        """Verify credentials are stored encrypted and retrieved correctly."""
        vault = CredentialVault(master_key="test_master_password_123", vault_path=self.vault_path)
        vault.store("telegram_bot", "123456:ABC-DEF")
        vault.store("smtp_password", "my_secret_smtp_pass")

        # Retrieve
        self.assertEqual(vault.retrieve("telegram_bot"), "123456:ABC-DEF")
        self.assertEqual(vault.retrieve("smtp_password"), "my_secret_smtp_pass")

    def test_retrieve_missing_returns_default(self) -> None:
        """Verify missing credentials return the default value."""
        vault = CredentialVault(master_key="test_key", vault_path=self.vault_path)
        self.assertIsNone(vault.retrieve("nonexistent"))
        self.assertEqual(vault.retrieve("nonexistent", "default_val"), "default_val")

    def test_delete(self) -> None:
        """Verify credentials can be deleted."""
        vault = CredentialVault(master_key="test_key", vault_path=self.vault_path)
        vault.store("api_key", "secret_key_value")
        self.assertEqual(vault.retrieve("api_key"), "secret_key_value")

        deleted = vault.delete("api_key")
        self.assertTrue(deleted)
        self.assertIsNone(vault.retrieve("api_key"))

        # Delete nonexistent returns False
        self.assertFalse(vault.delete("nonexistent"))

    def test_list_names(self) -> None:
        """Verify list_names only returns credential names (not metadata)."""
        vault = CredentialVault(master_key="test_key", vault_path=self.vault_path)
        vault.store("key1", "val1")
        vault.store("key2", "val2")

        names = vault.list_names()
        self.assertEqual(set(names), {"key1", "key2"})
        self.assertNotIn("_salt", names)

    def test_vault_file_is_encrypted(self) -> None:
        """Verify the vault file on disk is not plaintext."""
        vault = CredentialVault(master_key="test_key", vault_path=self.vault_path)
        vault.store("api_key", "super_secret_value")

        content = self.vault_path.read_text("utf-8")
        self.assertNotIn("super_secret_value", content)
        self.assertIn("_salt", content)

    def test_wrong_key_cannot_decrypt(self) -> None:
        """Verify using the wrong master key cannot decrypt the vault."""
        vault1 = CredentialVault(master_key="correct_key", vault_path=self.vault_path)
        vault1.store("api_key", "secret_value")

        # Try to read with wrong key
        vault2 = CredentialVault(master_key="wrong_key", vault_path=self.vault_path)
        result = vault2.retrieve("api_key")
        # With wrong key, decryption fails and returns empty string
        self.assertIn(result, ("", None))

    def test_store_without_master_key_raises(self) -> None:
        """Verify storing without a master key raises RuntimeError."""
        vault = CredentialVault(master_key="", vault_path=self.vault_path)
        with self.assertRaises(RuntimeError) as ctx:
            vault.store("key", "value")
        self.assertIn("QUANT_VAULT_MASTER_KEY", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
