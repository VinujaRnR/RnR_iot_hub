"""
Transactional Configuration Synchronization Manager for RnR Linux IoT Hub.
File: core/config_sync_manager.py

Implements RNHub V2 Configuration Sync Specification:
1. Receives authoritative configuration JSON from Django middleware (via RabbitMQ or HTTPS).
2. Validates identity (hub_id, hub_uuid), signature, and revision (config_version).
3. Executes transactional updates to SQLite (hub_registration, authorized_devices, config_settings).
4. Preserves schedules, outbox events, and operational histories without blind overwrites.
5. Invokes AutoEnrollmentManager to reconcile physical hardware against updated device inventory.
6. Returns structured acknowledgment payload for middleware reporting.
"""

from __future__ import annotations
import hashlib
import hmac
import json
import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

if TYPE_CHECKING:
    from storage.database import DatabaseManager
    from storage.registration_repository import RegistrationRepository
    from storage.config_repository import ConfigRepository
    from devices.auto_enrollment import AutoEnrollmentManager

logger = logging.getLogger("iothub.config_sync_manager")


class ConfigurationValidationError(Exception):
    """Raised when configuration payload fails verification."""
    pass


class ConfigSyncManager:
    """
    Applies versioned configuration snapshots from Django middleware transactionally,
    preserves local historical data, and triggers hardware reconciliation.
    """

    def __init__(
        self,
        db: Any,
        reg_repo: Optional[Any] = None,
        config_repo: Optional[Any] = None,
        auto_enrollment: Optional[Any] = None,
        expected_hub_id: Optional[str] = None,
        expected_hub_uuid: Optional[str] = None,
    ):
        self.db = db
        if config_repo is None:
            from storage.config_repository import ConfigRepository
            self.config_repo = ConfigRepository(db)
        else:
            self.config_repo = config_repo

        if reg_repo is None:
            from storage.registration_repository import RegistrationRepository
            self.reg_repo = RegistrationRepository(db, self.config_repo)
        else:
            self.reg_repo = reg_repo

        self.auto_enrollment = auto_enrollment
        self.expected_hub_id = expected_hub_id
        self.expected_hub_uuid = expected_hub_uuid

    def verify_signature(
        self,
        raw_payload: str,
        signature: Optional[str],
        secret_key: Optional[str],
    ) -> bool:
        """
        Validates HMAC-SHA256 signature of incoming configuration payload.
        Returns True if signature is valid, or if no signature was provided and no key is enforced.
        """
        if not signature:
            return True
        if not secret_key:
            logger.warning("Signature received but no verification key is configured on hub.")
            return True

        expected_sig = hmac.new(
            secret_key.encode("utf-8"),
            raw_payload.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

        return hmac.compare_digest(expected_sig.lower(), signature.strip().lower())

    async def apply_configuration(
        self,
        payload: Dict[str, Any],
        raw_body: str = "",
        signature: Optional[str] = None,
        actor: str = "middleware_sync",
    ) -> Dict[str, Any]:
        """
        Validates and transactionally applies a configuration snapshot to SQLite.
        Reconciles physical USB hardware without disrupting local schedules.
        """
        # 1. Identity Validation
        incoming_hub_id = payload.get("hub_id")
        incoming_hub_uuid = payload.get("hub_uuid")

        # Resolve expected identifiers from memory or stored registration
        current_reg = await self.reg_repo.get_registration()
        target_hub_id = self.expected_hub_id or (current_reg.get("hub_id") if current_reg else None)
        target_hub_uuid = self.expected_hub_uuid or (current_reg.get("hub_uuid") if current_reg else None)

        if target_hub_id and incoming_hub_id and incoming_hub_id != target_hub_id:
            msg = f"Configuration targeted to '{incoming_hub_id}' rejected by hub '{target_hub_id}'."
            logger.warning("[CONFIG_SYNC] %s", msg)
            return {
                "status": "REJECTED",
                "error": "IDENTITY_MISMATCH",
                "detail": msg,
                "config_version": payload.get("config_version"),
            }

        if target_hub_uuid and incoming_hub_uuid and incoming_hub_uuid != target_hub_uuid:
            msg = f"Configuration UUID '{incoming_hub_uuid}' does not match hub UUID '{target_hub_uuid}'."
            logger.warning("[CONFIG_SYNC] %s", msg)
            return {
                "status": "REJECTED",
                "error": "UUID_MISMATCH",
                "detail": msg,
                "config_version": payload.get("config_version"),
            }

        # 2. Signature Validation
        signing_token = await self.config_repo.get_setting("auth.token")
        if signature and raw_body and signing_token:
            if not self.verify_signature(raw_body, signature, signing_token):
                logger.error("[CONFIG_SYNC] HMAC signature verification failed.")
                return {
                    "status": "REJECTED",
                    "error": "INVALID_SIGNATURE",
                    "detail": "HMAC signature mismatch on configuration payload.",
                    "config_version": payload.get("config_version"),
                }

        # 3. Revision & Versioning Validation
        current_version = int(current_reg.get("config_version", 0) if current_reg else 0)
        incoming_version = int(payload.get("config_version", 0))

        if incoming_version < current_version:
            msg = f"Stale configuration revision: incoming v{incoming_version} < current v{current_version}."
            logger.warning("[CONFIG_SYNC] %s", msg)
            return {
                "status": "REJECTED",
                "error": "STALE_REVISION",
                "detail": msg,
                "current_version": current_version,
                "config_version": incoming_version,
            }

        # 4. Transactional SQLite Persistence
        # Uses RegistrationRepository.save_registration which commits inside a transaction
        # and preserves schedules, outbox events, and historical device logs.
        try:
            saved_reg = await self.reg_repo.save_registration(payload, actor=actor)
        except Exception as exc:
            logger.error("[CONFIG_SYNC] SQLite transaction failed: %s", exc)
            return {
                "status": "REJECTED",
                "error": "STORAGE_TRANSACTION_FAILED",
                "detail": str(exc),
                "config_version": incoming_version,
            }

        # 5. Handle Device Removals (Disable authorization; retain history)
        incoming_devices = payload.get("connected_devices") or payload.get("authorized_devices") or []
        if isinstance(incoming_devices, list) and incoming_devices:
            active_ids = {
                str(d.get("registration_device_id") or d.get("device_id"))
                for d in incoming_devices
                if (d.get("registration_device_id") or d.get("device_id"))
            }
            if active_ids:
                now_str = datetime.now(timezone.utc).isoformat()
                existing_auth = await self.reg_repo.get_authorized_devices()
                for existing in existing_auth:
                    ex_id = existing["registration_device_id"]
                    if ex_id not in active_ids and existing.get("status") != "revoked":
                        await self.reg_repo.update_device_authorization(
                            registration_device_id=ex_id,
                            status="revoked",
                        )
                        logger.info("[CONFIG_SYNC] Disabled removed device authorization '%s' (history retained).", ex_id)

        # 6. Reconcile Physical Hardware
        reconciliation_report: Dict[str, Any] = {"bound": [], "pending": [], "rejected": [], "port_changes": []}
        if self.auto_enrollment:
            try:
                reconciliation_report = await self.auto_enrollment.discover_and_reconcile()
            except Exception as exc:
                logger.error("[CONFIG_SYNC] Hardware auto-enrollment error: %s", exc)

        logger.info(
            "[CONFIG_SYNC] Successfully applied configuration v%d: bound=%d, pending=%d, rejected=%d",
            incoming_version,
            len(reconciliation_report.get("bound", [])),
            len(reconciliation_report.get("pending", [])),
            len(reconciliation_report.get("rejected", [])),
        )

        return {
            "status": "SUCCESS",
            "hub_id": incoming_hub_id or target_hub_id,
            "hub_uuid": incoming_hub_uuid or target_hub_uuid,
            "config_version": incoming_version,
            "reconciled_devices": reconciliation_report.get("bound", []),
            "pending_devices": reconciliation_report.get("pending", []),
            "rejected_devices": reconciliation_report.get("rejected", []),
            "port_changes": reconciliation_report.get("port_changes", []),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
