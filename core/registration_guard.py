"""
Runtime Registration & Authorization Guard for RnR Linux IoT Hub.
File: core/registration_guard.py

Implements RNHub V2: Middleware-Integrated Hub Registration:
- Enforces hub identity and operational authorization before sensitive actions.
- Governs the runtime states:
    1. UNREGISTERED    - Block activation and production operations.
    2. REGISTERED      - Setup/diagnostics allowed, but hardware actuation blocked until activated.
    3. ACTIVATED       - Full production operations permitted within lease.
    4. SUSPENDED       - Hub revoked by middleware; cease all actuation immediately.
    5. LEASE_EXPIRED   - Offline lease grace period expired; block unsafe device actions.
- Validates that target devices belong to the middleware-approved device inventory.
"""

from __future__ import annotations
import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

if TYPE_CHECKING:
    from storage.registration_repository import RegistrationRepository

logger = logging.getLogger("iothub.registration_guard")


class HubOperatingState:
    """Canonical operating authorization states."""
    UNREGISTERED = "UNREGISTERED"
    REGISTERED_INACTIVE = "REGISTERED_INACTIVE"
    ACTIVATED = "ACTIVATED"
    SUSPENDED = "SUSPENDED"
    RETIRED = "RETIRED"
    LEASE_EXPIRED = "LEASE_EXPIRED"


class RegistrationGuard:
    """
    Enforces middleware registration, lease validity, and device authorization
    at the hub, command router, and hardware execution layers.
    """

    def __init__(
        self,
        reg_repo: RegistrationRepository,
        lease_grace_hours: int = 72,
        strict_device_enforcement: bool = True,
    ):
        self.reg_repo = reg_repo
        self.lease_grace_hours = lease_grace_hours
        self.strict_device_enforcement = strict_device_enforcement

    async def get_operating_state(self) -> str:
        """Determines the current operating authorization state of the hub."""
        reg = await self.reg_repo.get_registration()
        if not reg or not reg.get("hub_uuid"):
            return HubOperatingState.UNREGISTERED

        status = str(reg.get("status") or "unregistered").lower()
        if status in ("suspended",):
            return HubOperatingState.SUSPENDED
        if status in ("retired", "deactivated"):
            return HubOperatingState.RETIRED

        if status != "active":
            return HubOperatingState.REGISTERED_INACTIVE

        is_valid = await self.reg_repo.is_lease_valid(grace_hours=self.lease_grace_hours)
        if not is_valid:
            return HubOperatingState.LEASE_EXPIRED

        return HubOperatingState.ACTIVATED

    async def check_hub_authorized(self) -> Tuple[bool, str]:
        """
        Verifies that the hub has valid, active authorization to execute IoT workloads.
        Returns (True, "Authorized") or (False, reason).
        """
        state = await self.get_operating_state()

        if state == HubOperatingState.UNREGISTERED:
            return False, "Hub is not registered. Run 'iothubctl hub activate' to claim your registration."

        if state == HubOperatingState.REGISTERED_INACTIVE:
            return False, "Hub is registered but not activated in middleware."

        if state == HubOperatingState.SUSPENDED:
            return False, "Hub operations have been SUSPENDED by Django middleware."

        if state == HubOperatingState.RETIRED:
            return False, "Hub has been RETIRED or permanently deactivated."

        if state == HubOperatingState.LEASE_EXPIRED:
            return False, "Operating authorization lease expired without middleware renewal."

        return True, "Hub authorized and active."

    async def is_device_approved(self, device_identifier: str) -> bool:
        """
        Checks if a device is authorized in the middleware inventory.
        Matches by registration_device_id, physical MAC, port, or registered name.
        """
        if not self.strict_device_enforcement:
            return True

        clean_id = str(device_identifier).strip().lower()

        # Check in authorized_devices table
        auth_dev = await self.reg_repo.get_authorized_device(clean_id)
        if auth_dev:
            # Device found in middleware inventory; must be approved
            return auth_dev.get("status") in ("approved", "active", "bound")

        # Fallback check: check if any authorized device matches as prefix or alias
        all_auth = await self.reg_repo.get_authorized_devices()
        if not all_auth:
            # If no devices are explicitly registered in middleware yet, return True
            # to allow initial discovery unless strict enforcement is mandatory
            return True

        for d in all_auth:
            reg_id = str(d.get("registration_device_id") or "").lower()
            mac = str(d.get("device_mac") or "").lower()
            name = str(d.get("name") or "").lower()
            port = str(d.get("bound_port") or "").lower()
            if clean_id in (reg_id, mac, name, port):
                return d.get("status") in ("approved", "active", "bound")

        return False

    async def validate_inbound_command(
        self,
        target_hub_id: str,
        target_device_id: str,
    ) -> Tuple[bool, str]:
        """
        Validates an incoming command against hub identity, registration state,
        and device authorization inventory.
        """
        reg = await self.reg_repo.get_registration()
        local_hub_id = reg.get("hub_id") if reg else None
        local_hub_uuid = reg.get("hub_uuid") if reg else None

        # 1. Hub identity verification
        if local_hub_id or local_hub_uuid:
            clean_target = str(target_hub_id).strip()
            valid_targets = {str(local_hub_id).strip(), str(local_hub_uuid).strip()}
            if clean_target not in valid_targets:
                return False, f"Target hub '{target_hub_id}' does not match registered hub '{local_hub_id}'"

        # 2. Hub authorization check
        authorized, reason = await self.check_hub_authorized()
        if not authorized:
            return False, reason

        # 3. Device approval check
        if not await self.is_device_approved(target_device_id):
            return False, f"Device '{target_device_id}' is not authorized by Django middleware inventory."

        return True, ""
