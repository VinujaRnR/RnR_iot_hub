"""
Secure Configuration & Encrypted Secrets Repository for RnR Linux IoT Hub.
File: storage/config_repository.py

Implements Customization 2 & 3 specification:
- Stores persistent configuration in SQLite table `config_settings`.
- Protects sensitive credentials (e.g. rabbitmq.password) via AES-256-GCM encryption.
- Authenticates metadata (key name, key version) as Associated Authenticated Data (AAD).
- Tracks all configuration changes in `config_audit` without exposing secret values.
- Masks encrypted secrets on read/show commands (e.g. '********').
"""

from __future__ import annotations
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from core.crypto import CryptoManager
from storage.database import DatabaseManager

logger = logging.getLogger("iothub.config_repo")

# Settings that must always be treated as encrypted secrets by default
SENSITIVE_CONFIG_KEYS = {
    "rabbitmq.password",
    "jwt.secret",
    "auth.token",
    "api.key",
}


class ConfigRepository:
    """Manages secure runtime settings, secret encryption, and audit tracking in SQLite."""

    def __init__(self, db: DatabaseManager, crypto: Optional[CryptoManager] = None):
        self.db = db
        self.crypto = crypto or CryptoManager()

    async def set_setting(
        self,
        key: str,
        value: Any,
        is_secret: Optional[bool] = None,
        actor: str = "cli",
    ) -> None:
        """
        Sets a configuration setting.
        If is_secret is True (or key is in SENSITIVE_CONFIG_KEYS), value is encrypted
        with AES-256-GCM and stored as BLOB with random nonce.
        """
        if is_secret is None:
            is_secret = key in SENSITIVE_CONFIG_KEYS

        val_str = str(value) if value is not None else ""
        now = datetime.now(timezone.utc).isoformat()
        conn = self.db.connection

        if is_secret:
            encrypted_bytes, nonce = self.crypto.encrypt_secret(
                plaintext=val_str,
                config_key=key,
                key_version=1,
            )
            storage_type = "ENCRYPTED"
            val_text = None
        else:
            encrypted_bytes = None
            nonce = None
            storage_type = "PLAINTEXT"
            val_text = val_str

        async with self.db.transaction() as tx:
            await tx.execute(
                """
                INSERT INTO config_settings (
                    config_key, storage_type, value_text, encrypted_value, nonce, key_version, updated_at
                ) VALUES (?, ?, ?, ?, ?, 1, ?)
                ON CONFLICT(config_key) DO UPDATE SET
                    storage_type = excluded.storage_type,
                    value_text = excluded.value_text,
                    encrypted_value = excluded.encrypted_value,
                    nonce = excluded.nonce,
                    key_version = excluded.key_version,
                    updated_at = excluded.updated_at
                """,
                (key, storage_type, val_text, encrypted_bytes, nonce, now),
            )

            # Record audit event (sensitive values are masked in audit trail)
            operation = "SET_SECRET" if is_secret else "SET"
            await tx.execute(
                """
                INSERT INTO config_audit (config_key, operation, actor, changed_at)
                VALUES (?, ?, ?, ?)
                """,
                (key, operation, actor, now),
            )

        logger.info("Configuration key '%s' updated (storage_type=%s, actor=%s)", key, storage_type, actor)

    async def get_setting(self, key: str, default: Any = None) -> Any:
        """
        Retrieves setting value. If encrypted, decrypts using master key and returns plaintext.
        Returns default if key is not found.
        """
        conn = self.db.connection
        async with conn.execute(
            """
            SELECT storage_type, value_text, encrypted_value, nonce, key_version
            FROM config_settings WHERE config_key = ?
            """,
            (key,),
        ) as cur:
            row = await cur.fetchone()
            if not row:
                return default

            storage_type = row["storage_type"]
            if storage_type == "ENCRYPTED":
                enc_val = row["encrypted_value"]
                nonce = row["nonce"]
                key_ver = row["key_version"] or 1
                if not enc_val or not nonce:
                    return default
                return self.crypto.decrypt_secret(
                    ciphertext_with_tag=enc_val,
                    nonce=nonce,
                    config_key=key,
                    key_version=key_ver,
                )
            else:
                return row["value_text"]

    async def delete_setting(self, key: str, actor: str = "cli") -> bool:
        """Removes a setting and records audit event."""
        now = datetime.now(timezone.utc).isoformat()
        async with self.db.transaction() as tx:
            cur = await tx.execute("DELETE FROM config_settings WHERE config_key = ?", (key,))
            deleted = cur.rowcount > 0
            if deleted:
                await tx.execute(
                    """
                    INSERT INTO config_audit (config_key, operation, actor, changed_at)
                    VALUES (?, 'DELETE', ?, ?)
                    """,
                    (key, actor, now),
                )
        return deleted

    async def list_settings(self, mask_secrets: bool = True) -> Dict[str, Any]:
        """
        Returns all configuration settings as a dictionary.
        If mask_secrets is True, encrypted settings return '********'.
        """
        conn = self.db.connection
        result: Dict[str, Any] = {}
        async with conn.execute(
            "SELECT config_key, storage_type, value_text, encrypted_value, nonce, key_version FROM config_settings ORDER BY config_key ASC"
        ) as cur:
            rows = await cur.fetchall()
            for r in rows:
                key = r["config_key"]
                stype = r["storage_type"]
                if stype == "ENCRYPTED":
                    if mask_secrets:
                        result[key] = "********"
                    else:
                        try:
                            result[key] = self.crypto.decrypt_secret(
                                ciphertext_with_tag=r["encrypted_value"],
                                nonce=r["nonce"],
                                config_key=key,
                                key_version=r["key_version"] or 1,
                            )
                        except Exception:
                            result[key] = "[DECRYPTION_FAILED]"
                else:
                    result[key] = r["value_text"]
        return result

    async def get_audit_trail(self, key: Optional[str] = None, limit: int = 50) -> List[Dict[str, Any]]:
        """Returns configuration change history."""
        conn = self.db.connection
        query = "SELECT audit_id, config_key, operation, actor, changed_at FROM config_audit"
        params: List[Any] = []
        if key:
            query += " WHERE config_key = ?"
            params.append(key)
        query += " ORDER BY audit_id DESC LIMIT ?"
        params.append(limit)

        async with conn.execute(query, params) as cur:
            rows = await cur.fetchall()
            return [dict(r) for r in rows]

    async def seed_default_settings(self, defaults: Optional[Dict[str, Any]] = None) -> None:
        """Seeds standard hub settings if not already present in the database."""
        base_defaults = {
            "hub.id": "hub-01",
            "rabbitmq.host": "localhost",
            "rabbitmq.port": "5672",
            "rabbitmq.username": "guest",
            "rabbitmq.tls": "false",
            "usb.baud_rate": "115200",
            "schedule.timezone": "Asia/Kolkata",
            "usb.auto_reconnect": "true",
            "logging.level": "INFO",
            "schedule.replacement_policy": "replace",
            "schedule.duplicate_policy": "ignore",
            "schedule.late_action_policy": "reconcile",
            "schedule.conflict_policy": "reject",
        }
        if defaults:
            base_defaults.update(defaults)

        for k, v in base_defaults.items():
            existing = await self.get_setting(k)
            if existing is None:
                await self.set_setting(k, v, is_secret=False, actor="system_init")
