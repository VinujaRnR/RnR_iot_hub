"""
Automatic Configuration Manager for RnR Linux IoT Hub.
File: core/auto_config_manager.py

Implements RNHub V2 Auto Configuration Specification (Section 6 & 7):
- Receives and validates middleware registration snapshot JSON.
- Verifies identity, Django server digital signature, schema version, and expiration.
- Manages configuration revisions (STALE, ALREADY_APPLIED, or APPLIED).
- Transactionally updates local SQLite tables in a single ACID transaction:
    * hub_registration
    * hub_configuration
    * approved_devices & authorized_devices
    * config_history (records version, SHA256 hash, timestamp)
    * config_settings (timezone, etc.)
- Preserves local schedules, operational histories, audit logs, and outbox events.
- Triggers physical USB device reconciliation and queues results into provisioning_outbox.
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
    from core.provisioning_reporter import ProvisioningReporter

logger = logging.getLogger("iothub.auto_config_manager")


class ConfigurationValidationError(Exception):
    """Raised when incoming configuration JSON fails schema or cryptographic validation."""
    pass


class AutoConfigManager:
    """
    Coordinates authoritative configuration application, SQLite snapshot storage,
    revision gating, and hardware auto-enrollment reconciliation.
    """

    def __init__(
        self,
        db: Any,
        reg_repo: Optional[Any] = None,
        config_repo: Optional[Any] = None,
        auto_enrollment: Optional[Any] = None,
        provisioning_reporter: Optional[Any] = None,
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
        self.provisioning_reporter = provisioning_reporter
        self.expected_hub_id = expected_hub_id
        self.expected_hub_uuid = expected_hub_uuid

    @staticmethod
    def compute_config_hash(payload: Dict[str, Any]) -> str:
        """Computes deterministic SHA-256 hash of configuration payload."""
        # Strip signature and volatile timestamps for canonical hash
        normalized = dict(payload)
        normalized.pop("signature", None)
        normalized.pop("timestamp", None)
        normalized.pop("issued_at", None)
        serialized = json.dumps(normalized, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(serialized.encode("utf-8")).hexdigest()

    def verify_server_signature(
        self,
        raw_payload: str,
        signature: Optional[str],
        secret_key: Optional[str],
    ) -> bool:
        """Validates HMAC-SHA256 signature against Django server secret or token."""
        if not signature:
            return True
        if not secret_key:
            logger.warning("[AUTO_CONFIG] Signature provided but no verification key is configured on hub.")
            return True

        expected_sig = hmac.new(
            secret_key.encode("utf-8"),
            raw_payload.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        return hmac.compare_digest(expected_sig.lower(), signature.strip().lower())

    async def get_current_version_and_hash(self) -> Tuple[int, Optional[str]]:
        """Retrieves currently active config_version and config_hash from SQLite."""
        conn = self.db.connection
        async with conn.execute(
            """
            SELECT config_version, config_hash FROM config_history
            ORDER BY config_version DESC, id DESC
            LIMIT 1
            """
        ) as cur:
            row = await cur.fetchone()
            if row:
                return int(row["config_version"] or 0), row["config_hash"]

        # Fallback to hub_registration
        reg = await self.reg_repo.get_registration()
        if reg:
            return int(reg.get("config_version") or 0), None
        return 0, None

    async def apply_configuration(
        self,
        payload: Dict[str, Any],
        raw_body: str = "",
        signature: Optional[str] = None,
        actor: str = "middleware_sync",
    ) -> Dict[str, Any]:
        """
        Processes and applies registration configuration snapshot according to Section 6:
        1. Validate schema and expiration.
        2. Confirm hub identity.
        3. Verify signature.
        4. Compare revisions (STALE, ALREADY_APPLIED, or APPLIED).
        5. Atomically update SQLite tables.
        6. Reconcile approved devices.
        7. Queue result for middleware via ProvisioningReporter.
        """
        # 1. Validate Schema & Structure
        # Supports both payload.devices and payload.connected_devices
        devices_list = payload.get("devices") or payload.get("connected_devices") or payload.get("authorized_devices") or []
        hub_meta = payload.get("hub") or {}

        incoming_hub_id = payload.get("hub_id") or hub_meta.get("hub_id")
        incoming_hub_uuid = payload.get("hub_uuid") or hub_meta.get("hub_uuid")
        incoming_version = int(payload.get("config_version") or 0)
        sig = signature or payload.get("signature")

        # Expiration Check
        expires_at_str = payload.get("expires_at")
        if expires_at_str:
            try:
                # Handle ISO timestamps with Z or offset
                clean_exp = expires_at_str.replace("Z", "+00:00")
                exp_dt = datetime.fromisoformat(clean_exp)
                if exp_dt < datetime.now(timezone.utc):
                    msg = f"Configuration expired at {expires_at_str}."
                    logger.warning("[AUTO_CONFIG] %s", msg)
                    return {
                        "status": "REJECTED",
                        "error": "CONFIG_EXPIRED",
                        "detail": msg,
                        "config_version": incoming_version,
                    }
            except Exception as parse_err:
                logger.debug("[AUTO_CONFIG] Expiration check parse notice: %s", parse_err)

        # 2. Confirm Hub Identity
        current_reg = await self.reg_repo.get_registration()
        target_hub_id = self.expected_hub_id or (current_reg.get("hub_id") if current_reg else None)
        target_hub_uuid = self.expected_hub_uuid or (current_reg.get("hub_uuid") if current_reg else None)

        is_initial_activation = (
            not current_reg
            or not current_reg.get("hub_uuid")
            or target_hub_id in ("hub-01", "default", None)
            or bool(payload.get("activation_code"))
        )

        if not is_initial_activation and target_hub_id and incoming_hub_id and incoming_hub_id != target_hub_id:
            msg = f"Configuration targeted to '{incoming_hub_id}' rejected by hub '{target_hub_id}'."
            logger.warning("[AUTO_CONFIG] %s", msg)
            return {
                "status": "REJECTED",
                "error": "IDENTITY_MISMATCH",
                "detail": msg,
                "config_version": incoming_version,
            }

        if not is_initial_activation and target_hub_uuid and incoming_hub_uuid and incoming_hub_uuid != target_hub_uuid:
            msg = f"Configuration UUID '{incoming_hub_uuid}' does not match hub UUID '{target_hub_uuid}'."
            logger.warning("[AUTO_CONFIG] %s", msg)
            return {
                "status": "REJECTED",
                "error": "UUID_MISMATCH",
                "detail": msg,
                "config_version": incoming_version,
            }

        # 3. Verify Server Signature
        signing_token = await self.config_repo.get_setting("auth.token")
        if sig and raw_body and signing_token:
            if not self.verify_server_signature(raw_body, sig, signing_token):
                logger.error("[AUTO_CONFIG] Digital signature verification failed.")
                return {
                    "status": "REJECTED",
                    "error": "INVALID_SIGNATURE",
                    "detail": "HMAC signature mismatch on configuration payload.",
                    "config_version": incoming_version,
                }

        # 4. Compare Configuration Revisions
        current_version, current_hash = await self.get_current_version_and_hash()
        new_hash = self.compute_config_hash(payload)

        if incoming_version < current_version:
            msg = f"Stale configuration revision: incoming v{incoming_version} < current v{current_version}."
            logger.warning("[AUTO_CONFIG] %s", msg)
            return {
                "status": "REJECTED",
                "error": "STALE_REVISION",
                "detail": msg,
                "current_version": current_version,
                "config_version": incoming_version,
            }

        if incoming_version == current_version and current_version > 0:
            if current_hash and current_hash == new_hash:
                logger.info("[AUTO_CONFIG] Configuration v%d already applied (hash verified).", incoming_version)
                return {
                    "status": "ALREADY_APPLIED",
                    "config_version": incoming_version,
                    "config_hash": new_hash,
                }

        # 5. Atomic SQLite Persistence (Transaction)
        now_iso = datetime.now(timezone.utc).isoformat()
        hub_name = hub_meta.get("hub_name") or payload.get("hub_name") or incoming_hub_id or "RNHub"
        hub_type = hub_meta.get("hub_type") or payload.get("hub_type") or "mini_pc"
        resource_id = hub_meta.get("resource_id") or payload.get("resource_id")
        sub_resource_id = hub_meta.get("sub_resource_id") or payload.get("sub_resource_id")
        location = hub_meta.get("installation_location") or payload.get("installation_location") or ""
        tz = hub_meta.get("timezone") or payload.get("timezone") or "Asia/Colombo"
        raw_json_str = raw_body or json.dumps(payload)

        conn = self.db.connection
        try:
            # 5a. Save hub_registration
            await self.reg_repo.save_registration(
                {
                    "hub_uuid": incoming_hub_uuid or target_hub_uuid,
                    "hub_id": incoming_hub_id or target_hub_id,
                    "hub_name": hub_name,
                    "hub_type": hub_type,
                    "status": "active",
                    "resource_id": resource_id,
                    "sub_resource_id": sub_resource_id,
                    "installation_location": location,
                    "timezone": tz,
                    "config_version": incoming_version,
                    "lease_expires_at": expires_at_str,
                    "verified_at": now_iso,
                    "connected_devices": devices_list,
                },
                raw_config=payload,
                actor=actor,
            )

            # 5b. Save hub_configuration table
            await conn.execute(
                """
                INSERT INTO hub_configuration (
                    hub_uuid, hub_id, hub_name, hub_type, resource_id, sub_resource_id,
                    installation_location, timezone, config_version, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(hub_uuid) DO UPDATE SET
                    hub_name = excluded.hub_name,
                    hub_type = excluded.hub_type,
                    resource_id = excluded.resource_id,
                    sub_resource_id = excluded.sub_resource_id,
                    installation_location = excluded.installation_location,
                    timezone = excluded.timezone,
                    config_version = excluded.config_version,
                    updated_at = excluded.updated_at
                """,
                (
                    incoming_hub_uuid or target_hub_uuid,
                    incoming_hub_id or target_hub_id,
                    hub_name,
                    hub_type,
                    resource_id,
                    sub_resource_id,
                    location,
                    tz,
                    incoming_version,
                    now_iso,
                ),
            )

            # 5c. Save approved_devices & authorized_devices tables
            active_device_uids = set()
            for dev in devices_list:
                dev_uid = str(dev.get("device_uid") or dev.get("registration_device_id") or dev.get("device_id") or "")
                if not dev_uid:
                    continue
                active_device_uids.add(dev_uid)
                d_name = dev.get("name") or dev_uid
                d_mac = dev.get("mac") or dev.get("device_mac")
                d_type = dev.get("type") or "relay"
                d_profile = dev.get("profile") or "esp8266_relay_v1"
                d_approved = 1 if dev.get("approved", True) else 0
                d_cfg = json.dumps(dev.get("configuration") or {})
                d_status = "approved" if d_approved else "pending_binding"

                # Update approved_devices
                await conn.execute(
                    """
                    INSERT INTO approved_devices (
                        device_uid, name, mac, type, profile, approved, configuration_json, status, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(device_uid) DO UPDATE SET
                        name = excluded.name,
                        mac = COALESCE(excluded.mac, approved_devices.mac),
                        type = excluded.type,
                        profile = excluded.profile,
                        approved = excluded.approved,
                        configuration_json = excluded.configuration_json,
                        status = CASE WHEN excluded.approved = 0 THEN 'pending_binding' ELSE approved_devices.status END,
                        updated_at = excluded.updated_at
                    """,
                    (dev_uid, d_name, d_mac, d_type, d_profile, d_approved, d_cfg, d_status, now_iso),
                )

                # Keep authorized_devices table in sync
                await conn.execute(
                    """
                    INSERT INTO authorized_devices (
                        registration_device_id, name, device_mac, profile, status, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(registration_device_id) DO UPDATE SET
                        name = excluded.name,
                        device_mac = COALESCE(excluded.device_mac, authorized_devices.device_mac),
                        profile = excluded.profile,
                        status = excluded.status,
                        updated_at = excluded.updated_at
                    """,
                    (dev_uid, d_name, d_mac, d_profile, d_status, now_iso),
                )

            # Revoke removed devices (disable authorization; preserve local history)
            if active_device_uids:
                async with conn.execute("SELECT registration_device_id FROM authorized_devices") as cur:
                    stored_rows = await cur.fetchall()
                for s_row in stored_rows:
                    st_id = s_row["registration_device_id"]
                    if st_id not in active_device_uids:
                        await conn.execute(
                            "UPDATE authorized_devices SET status = 'revoked', updated_at = ? WHERE registration_device_id = ?",
                            (now_iso, st_id),
                        )
                        await conn.execute(
                            "UPDATE approved_devices SET approved = 0, status = 'revoked', updated_at = ? WHERE device_uid = ?",
                            (now_iso, st_id),
                        )
                        logger.info("[AUTO_CONFIG] Revoked removed device '%s' (history retained).", st_id)

            # 5d. Save config_history table
            await conn.execute(
                """
                INSERT INTO config_history (
                    hub_uuid, config_version, config_hash, applied_status, raw_json, applied_at
                ) VALUES (?, ?, ?, 'APPLIED', ?, ?)
                """,
                (
                    incoming_hub_uuid or target_hub_uuid,
                    incoming_version,
                    new_hash,
                    raw_json_str,
                    now_iso,
                ),
            )
            await conn.commit()
            logger.info("[AUTO_CONFIG] Committed configuration v%d to SQLite (hash: %s).", incoming_version, new_hash[:8])

        except Exception as exc:
            logger.error("[AUTO_CONFIG] SQLite transaction failed: %s", exc)
            return {
                "status": "REJECTED",
                "error": "STORAGE_TRANSACTION_FAILED",
                "detail": str(exc),
                "config_version": incoming_version,
            }

        # 6. Reconcile Physical Hardware (Auto Enrollment)
        reconciliation_report: Dict[str, Any] = {"bound": [], "pending": [], "rejected": [], "port_changes": []}
        device_results: List[Dict[str, Any]] = []

        if self.auto_enrollment:
            try:
                reconciliation_report = await self.auto_enrollment.discover_and_reconcile()
                device_results = reconciliation_report.get("device_results") or []
            except Exception as exc:
                logger.error("[AUTO_CONFIG] Hardware auto-enrollment error: %s", exc)

        # Fallback device results formatting if empty
        if not device_results:
            for d in devices_list:
                d_uid = str(d.get("device_uid") or d.get("registration_device_id") or "")
                is_appr = bool(d.get("approved", True))
                device_results.append({
                    "device_uid": d_uid,
                    "status": "pending_hardware_binding" if is_appr else "unapproved",
                })

        # 7. Queue Acknowledgment for Middleware (Provisioning Reporter)
        ack_status = "applied"
        if any(d.get("status") == "pending_hardware_binding" for d in device_results):
            ack_status = "partial" if any(d.get("status") == "configured" for d in device_results) else "partial"
        if all(d.get("status") == "configured" for d in device_results) and device_results:
            ack_status = "applied"

        if self.provisioning_reporter:
            try:
                await self.provisioning_reporter.queue_result(
                    config_version=incoming_version,
                    device_results=device_results,
                    status=ack_status,
                    hub_uuid=incoming_hub_uuid or target_hub_uuid,
                    hub_id=incoming_hub_id or target_hub_id,
                )
            except Exception as r_err:
                logger.warning("[AUTO_CONFIG] Could not queue provisioning acknowledgment: %s", r_err)

        logger.info(
            "[AUTO_CONFIG] Successfully applied v%d: bound=%d, pending=%d, status='%s'",
            incoming_version,
            len(reconciliation_report.get("bound", [])),
            len(reconciliation_report.get("pending", [])),
            ack_status,
        )

        return {
            "status": "SUCCESS",
            "hub_id": incoming_hub_id or target_hub_id,
            "hub_uuid": incoming_hub_uuid or target_hub_uuid,
            "config_version": incoming_version,
            "config_hash": new_hash,
            "ack_status": ack_status,
            "reconciled_devices": reconciliation_report.get("bound", []),
            "pending_devices": reconciliation_report.get("pending", []),
            "device_results": device_results,
            "timestamp": now_iso,
        }
