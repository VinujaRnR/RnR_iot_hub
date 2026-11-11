"""
Persistent Registration Repository for RnR Linux IoT Hub.
File: storage/registration_repository.py

Implements RNHub V2: Middleware-Integrated Hub Registration Specification:
- Persists verified hub registration snapshot received from Django middleware.
- Maintains authoritative hub identity (hub_uuid, hub_id, resource mapping, timezone).
- Manages middleware-authorized device inventory (authorized_devices).
- Synchronizes status and lease timestamps with local SQLite storage.
- Encrypts machine authentication tokens and provisioned broker credentials via ConfigRepository.
"""

from __future__ import annotations
import json
import logging
from datetime import datetime, timezone, timedelta
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from storage.database import DatabaseManager

if TYPE_CHECKING:
    from storage.config_repository import ConfigRepository

logger = logging.getLogger("iothub.registration_repo")


class RegistrationRepository:
    """Manages verified middleware registration records and authorized device inventory in SQLite."""

    def __init__(self, db: DatabaseManager, config_repo: Optional[Any] = None):
        self.db = db
        if config_repo is None:
            from storage.config_repository import ConfigRepository
            self.config_repo = ConfigRepository(db)
        else:
            self.config_repo = config_repo

    async def get_registration(self) -> Optional[Dict[str, Any]]:
        """
        Retrieves the verified hub registration snapshot.
        Returns None if the hub has not yet been activated against middleware.
        """
        conn = self.db.connection
        async with conn.execute(
            """
            SELECT hub_uuid, hub_id, hub_name, hub_type, status,
                   resource_id, sub_resource_id, installation_location,
                   timezone, config_version, lease_expires_at, verified_at,
                   api_token, middleware_url, raw_config_json, updated_at
            FROM hub_registration
            ORDER BY updated_at DESC
            LIMIT 1
            """
        ) as cur:
            row = await cur.fetchone()
            if not row:
                return None
            return {
                "hub_uuid": row["hub_uuid"],
                "hub_id": row["hub_id"],
                "hub_name": row["hub_name"],
                "hub_type": row["hub_type"] or "mini_pc",
                "status": row["status"] or "unregistered",
                "resource_id": row["resource_id"],
                "sub_resource_id": row["sub_resource_id"],
                "installation_location": row["installation_location"] or "",
                "timezone": row["timezone"] or "UTC",
                "config_version": int(row["config_version"] or 0),
                "lease_expires_at": row["lease_expires_at"],
                "verified_at": row["verified_at"],
                "api_token": row["api_token"],
                "middleware_url": row["middleware_url"] or "",
                "raw_config": json.loads(row["raw_config_json"]) if row["raw_config_json"] else {},
                "updated_at": row["updated_at"],
            }

    async def save_registration(
        self,
        reg_data: Dict[str, Any],
        raw_config: Optional[Dict[str, Any]] = None,
        actor: str = "installer",
    ) -> Dict[str, Any]:
        """
        Persists authoritative registration snapshot to SQLite.
        Updates both hub_registration table and standard config_settings keys.
        Encrypts sensitive credentials (api_token, broker password) using AES-256-GCM.
        """
        now = datetime.now(timezone.utc).isoformat()
        hub_uuid = str(reg_data.get("hub_uuid") or "")
        hub_id = str(reg_data.get("hub_id") or "")
        hub_name = str(reg_data.get("hub_name") or hub_id or "RNHub")
        hub_type = str(reg_data.get("hub_type") or "mini_pc")
        status = str(reg_data.get("status") or "active").lower()
        resource_id = reg_data.get("resource_id")
        sub_resource_id = reg_data.get("sub_resource_id")
        installation_location = str(reg_data.get("installation_location") or "")
        tz = str(reg_data.get("timezone") or "Asia/Colombo")
        config_version = int(reg_data.get("config_version") or 1)
        lease_expires_at = reg_data.get("lease_expires_at")
        verified_at = reg_data.get("verified_at") or now
        api_token = reg_data.get("api_token")
        middleware_url = str(reg_data.get("middleware_url") or "")
        raw_json_str = json.dumps(raw_config or reg_data)

        # 1. Store in hub_registration table
        async with self.db.transaction() as tx:
            await tx.execute(
                """
                INSERT INTO hub_registration (
                    hub_uuid, hub_id, hub_name, hub_type, status,
                    resource_id, sub_resource_id, installation_location,
                    timezone, config_version, lease_expires_at, verified_at,
                    api_token, middleware_url, raw_config_json, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(hub_uuid) DO UPDATE SET
                    hub_id = excluded.hub_id,
                    hub_name = excluded.hub_name,
                    hub_type = excluded.hub_type,
                    status = excluded.status,
                    resource_id = excluded.resource_id,
                    sub_resource_id = excluded.sub_resource_id,
                    installation_location = excluded.installation_location,
                    timezone = excluded.timezone,
                    config_version = excluded.config_version,
                    lease_expires_at = excluded.lease_expires_at,
                    verified_at = excluded.verified_at,
                    api_token = excluded.api_token,
                    middleware_url = excluded.middleware_url,
                    raw_config_json = excluded.raw_config_json,
                    updated_at = excluded.updated_at
                """,
                (
                    hub_uuid, hub_id, hub_name, hub_type, status,
                    resource_id, sub_resource_id, installation_location,
                    tz, config_version, lease_expires_at, verified_at,
                    api_token, middleware_url, raw_json_str, now
                ),
            )

        # 2. Synchronize config_settings repository keys
        await self.config_repo.set_setting("registration.hub_uuid", hub_uuid, is_secret=False, actor=actor)
        await self.config_repo.set_setting("registration.hub_id", hub_id, is_secret=False, actor=actor)
        await self.config_repo.set_setting("registration.status", status, is_secret=False, actor=actor)
        await self.config_repo.set_setting("registration.config_version", config_version, is_secret=False, actor=actor)
        await self.config_repo.set_setting("registration.verified_at", verified_at, is_secret=False, actor=actor)
        if resource_id is not None:
            await self.config_repo.set_setting("registration.resource_id", resource_id, is_secret=False, actor=actor)
        if sub_resource_id is not None:
            await self.config_repo.set_setting("registration.sub_resource_id", sub_resource_id, is_secret=False, actor=actor)
        if lease_expires_at:
            await self.config_repo.set_setting("registration.lease_expires_at", lease_expires_at, is_secret=False, actor=actor)
        if middleware_url:
            await self.config_repo.set_setting("registration.middleware_url", middleware_url, is_secret=False, actor=actor)

        # 3. Securely store machine credentials (api_token & broker password)
        if api_token:
            await self.config_repo.set_setting("auth.token", api_token, is_secret=True, actor=actor)

        broker_creds = reg_data.get("broker_credentials")
        if broker_creds and isinstance(broker_creds, dict):
            if "host" in broker_creds:
                await self.config_repo.set_setting("rabbitmq.host", broker_creds["host"], is_secret=False, actor=actor)
            if "port" in broker_creds:
                await self.config_repo.set_setting("rabbitmq.port", broker_creds["port"], is_secret=False, actor=actor)
            if "user" in broker_creds:
                await self.config_repo.set_setting("rabbitmq.user", broker_creds["user"], is_secret=False, actor=actor)
            if "password" in broker_creds:
                await self.config_repo.set_setting("rabbitmq.password", broker_creds["password"], is_secret=True, actor=actor)
            if "vhost" in broker_creds:
                await self.config_repo.set_setting("rabbitmq.vhost", broker_creds["vhost"], is_secret=False, actor=actor)

        # 4. Save connected / authorized devices if present
        devices = reg_data.get("connected_devices") or reg_data.get("authorized_devices")
        if devices and isinstance(devices, list):
            await self.set_authorized_devices(devices)

        logger.info("Saved verified registration for hub '%s' (UUID: %s, version: %d, status: %s)", hub_id, hub_uuid, config_version, status)
        return (await self.get_registration()) or {}

    async def update_status(self, status: str, actor: str = "supervisor") -> None:
        """Updates registration status ('active', 'suspended', 'retired')."""
        now = datetime.now(timezone.utc).isoformat()
        clean_status = status.strip().lower()
        async with self.db.transaction() as tx:
            await tx.execute(
                "UPDATE hub_registration SET status = ?, updated_at = ?",
                (clean_status, now),
            )
        await self.config_repo.set_setting("registration.status", clean_status, is_secret=False, actor=actor)
        logger.info("Hub registration status updated to '%s' by %s", clean_status, actor)

    async def update_lease(self, lease_expires_at: str, actor: str = "supervisor") -> None:
        """Updates the operating authorization lease expiration timestamp."""
        now = datetime.now(timezone.utc).isoformat()
        async with self.db.transaction() as tx:
            await tx.execute(
                "UPDATE hub_registration SET lease_expires_at = ?, verified_at = ?, updated_at = ?",
                (lease_expires_at, now, now),
            )
        await self.config_repo.set_setting("registration.lease_expires_at", lease_expires_at, is_secret=False, actor=actor)
        await self.config_repo.set_setting("registration.verified_at", now, is_secret=False, actor=actor)
        logger.info("Hub lease renewed until '%s'", lease_expires_at)

    async def set_authorized_devices(self, devices: List[Dict[str, Any]]) -> None:
        """
        Synchronizes the middleware-approved device inventory into SQLite table `authorized_devices`.
        Preserves local bound ports and MACs if already bound and matches.
        """
        now = datetime.now(timezone.utc).isoformat()
        async with self.db.transaction() as tx:
            for dev in devices:
                reg_id = str(dev.get("registration_device_id") or dev.get("device_id") or "")
                if not reg_id:
                    continue
                name = str(dev.get("name") or dev.get("device_name") or reg_id)
                mac = dev.get("device_mac") or dev.get("mac")
                mac_clean = str(mac).strip().lower() if mac else None
                profile = dev.get("profile") or dev.get("profile_id") or "plant_controller_v1"
                status = str(dev.get("status") or "pending_binding").lower()
                port = dev.get("bound_port") or dev.get("port")

                await tx.execute(
                    """
                    INSERT INTO authorized_devices (
                        registration_device_id, name, device_mac, profile, status, bound_port, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(registration_device_id) DO UPDATE SET
                        name = excluded.name,
                        device_mac = COALESCE(excluded.device_mac, authorized_devices.device_mac),
                        profile = excluded.profile,
                        status = excluded.status,
                        bound_port = COALESCE(excluded.bound_port, authorized_devices.bound_port),
                        updated_at = excluded.updated_at
                    """,
                    (reg_id, name, mac_clean, profile, status, port, now),
                )
        logger.info("Synchronized %d authorized device(s) from middleware.", len(devices))

    async def get_authorized_devices(self) -> List[Dict[str, Any]]:
        """Retrieves all middleware-approved device inventory records."""
        conn = self.db.connection
        async with conn.execute(
            """
            SELECT registration_device_id, name, device_mac, profile, status, bound_port, updated_at
            FROM authorized_devices
            ORDER BY registration_device_id ASC
            """
        ) as cur:
            rows = await cur.fetchall()
            return [
                {
                    "registration_device_id": r["registration_device_id"],
                    "name": r["name"],
                    "device_mac": r["device_mac"],
                    "profile": r["profile"],
                    "status": r["status"],
                    "bound_port": r["bound_port"],
                    "updated_at": r["updated_at"],
                }
                for r in rows
            ]

    async def get_authorized_device(self, identifier: str) -> Optional[Dict[str, Any]]:
        """
        Looks up an authorized device by registration_device_id, device_mac, or name.
        """
        clean_id = str(identifier).strip().lower()
        conn = self.db.connection
        async with conn.execute(
            """
            SELECT registration_device_id, name, device_mac, profile, status, bound_port, updated_at
            FROM authorized_devices
            WHERE LOWER(registration_device_id) = ?
               OR LOWER(COALESCE(device_mac, '')) = ?
               OR LOWER(name) = ?
            LIMIT 1
            """,
            (clean_id, clean_id, str(identifier).strip().lower()),
        ) as cur:
            row = await cur.fetchone()
            if not row:
                return None
            return {
                "registration_device_id": row["registration_device_id"],
                "name": row["name"],
                "device_mac": row["device_mac"],
                "profile": row["profile"],
                "status": row["status"],
                "bound_port": row["bound_port"],
                "updated_at": row["updated_at"],
            }

    async def update_device_authorization(
        self,
        registration_device_id: str,
        device_mac: Optional[str] = None,
        status: str = "approved",
        bound_port: Optional[str] = None,
    ) -> None:
        """Updates device authorization status, physical MAC binding, and USB port."""
        now = datetime.now(timezone.utc).isoformat()
        mac_clean = str(device_mac).strip().lower() if device_mac else None
        async with self.db.transaction() as tx:
            await tx.execute(
                """
                UPDATE authorized_devices
                SET device_mac = COALESCE(?, device_mac),
                    status = ?,
                    bound_port = COALESCE(?, bound_port),
                    updated_at = ?
                WHERE registration_device_id = ?
                """,
                (mac_clean, status.lower(), bound_port, now, registration_device_id),
            )
        logger.info("Updated authorization for device '%s': status=%s, mac=%s, port=%s", registration_device_id, status, mac_clean, bound_port)

    async def is_activated(self) -> bool:
        """Returns True if the hub is locally registered with status 'active'."""
        reg = await self.get_registration()
        if not reg:
            return False
        return reg.get("status") == "active" and bool(reg.get("hub_uuid"))

    async def is_lease_valid(self, grace_hours: int = 72) -> bool:
        """
        Checks whether the operating authorization lease has not expired.
        Allows offline operation within grace_hours if middleware is unreachable.
        """
        reg = await self.get_registration()
        if not reg:
            return False
        if reg.get("status") != "active":
            return False

        lease_str = reg.get("lease_expires_at")
        if not lease_str:
            # If no lease is set yet, check verified_at + grace_hours
            verified_str = reg.get("verified_at") or reg.get("updated_at")
            if not verified_str:
                return True
            try:
                verified_dt = datetime.fromisoformat(verified_str.replace("Z", "+00:00"))
                return datetime.now(timezone.utc) <= (verified_dt + timedelta(hours=grace_hours))
            except Exception:
                return True

        try:
            lease_dt = datetime.fromisoformat(lease_str.replace("Z", "+00:00"))
            return datetime.now(timezone.utc) <= (lease_dt + timedelta(hours=grace_hours))
        except Exception:
            return True
