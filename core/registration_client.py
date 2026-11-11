"""
Django Middleware Registration & Synchronization Client for RnR Linux IoT Hub.
File: core/registration_client.py

Implements RNHub V2: Middleware-Integrated Hub Registration:
- HTTPS client for one-time activation credential redemption (/api/rnhub-agent/activate/).
- Authoritative configuration retrieval & updates (/api/rnhub-agent/config/).
- Hardware device binding authorization (/api/rnhub-agent/devices/authorize/).
- Periodic heartbeat & status reporting (/api/rnhub-agent/heartbeat/).
- Configuration application acknowledgment (/api/rnhub-agent/config/ack/).
"""

from __future__ import annotations
import hashlib
import logging
import os
import platform
import socket
import sys
import uuid
from typing import Any, Dict, List, Optional
import httpx

logger = logging.getLogger("iothub.registration_client")


# =====================================================================
# Registration Exceptions
# =====================================================================

class RegistrationError(Exception):
    """Base exception for all middleware registration and synchronization errors."""
    pass


class ActivationError(RegistrationError):
    """Raised when claiming or activating a hub against middleware fails."""
    pass


class InvalidActivationCodeError(ActivationError):
    """Raised when an activation code is invalid, expired, or already redeemed."""
    pass


class DuplicateClaimError(ActivationError):
    """Raised when the hub has already been claimed and bound to another installation."""
    pass


class RegistrationOfflineError(RegistrationError):
    """Raised when the Django middleware is unreachable (network outage, DNS, timeout)."""
    pass


class HubSuspendedError(RegistrationError):
    """Raised when the middleware reports the hub is suspended or retired."""
    pass


class DeviceAuthorizationError(RegistrationError):
    """Raised when a physical device binding is rejected by middleware."""
    pass


# =====================================================================
# Helper: Gather System Fingerprint
# =====================================================================

def get_system_fingerprint() -> Dict[str, Any]:
    """Collects machine metadata for secure installation binding in Django."""
    machine_id = "unknown"
    for p in ("/etc/machine-id", "/var/lib/dbus/machine-id"):
        try:
            if os.path.exists(p) and os.access(p, os.R_OK):
                with open(p, "r", encoding="utf-8") as f:
                    machine_id = f.read().strip()
                    break
        except Exception:
            pass

    return {
        "hostname": socket.gethostname(),
        "machine_id": machine_id,
        "platform": platform.platform(),
        "architecture": platform.machine(),
        "python_version": platform.python_version(),
    }


# =====================================================================
# Registration Client Implementation
# =====================================================================

class RegistrationClient:
    """
    Communicates securely over HTTPS with the central Django middleware.
    Supports asynchronous operations for iothubd and synchronous calls for install.py / CLI.
    """

    def __init__(
        self,
        middleware_url: str = "https://api.example.com",
        hub_uuid: Optional[str] = None,
        api_token: Optional[str] = None,
        verify_ssl: bool = True,
        timeout: float = 10.0,
        transport: Optional[httpx.AsyncBaseTransport] = None,
    ):
        self.middleware_url = middleware_url.rstrip("/")
        self.hub_uuid = hub_uuid
        self.api_token = api_token
        self.verify_ssl = verify_ssl
        self.timeout = timeout
        self.transport = transport

    def _get_headers(self) -> Dict[str, str]:
        """Builds standard authentication headers for machine-facing APIs."""
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "RnR-IoT-Hub-Agent/2.0",
        }
        if self.api_token:
            headers["Authorization"] = f"Bearer {self.api_token}"
        if self.hub_uuid:
            headers["X-Hub-UUID"] = self.hub_uuid
        return headers

    def _create_client(self) -> httpx.AsyncClient:
        """Instantiates configured httpx.AsyncClient."""
        return httpx.AsyncClient(
            base_url=self.middleware_url,
            verify=self.verify_ssl,
            timeout=self.timeout,
            transport=self.transport,
        )

    # -----------------------------------------------------------------
    # Step 4: Verify and Activate Hub against Django Middleware
    # -----------------------------------------------------------------
    async def activate(
        self,
        activation_code: str,
        system_info: Optional[Dict[str, Any]] = None,
        client_public_key: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Claims an existing registered Django hub using a one-time activation code.
        Sends machine fingerprint and receives canonical hub identity and configuration.
        """
        code_clean = activation_code.strip()
        if not code_clean:
            raise InvalidActivationCodeError("Activation code cannot be empty.")

        payload = {
            "activation_code": code_clean,
            "system_info": system_info or get_system_fingerprint(),
            "client_public_key": client_public_key or uuid.uuid4().hex,
        }

        url = "/api/rnhub-agent/activate/"
        logger.info("Attempting hub activation with middleware at %s%s...", self.middleware_url, url)

        async with self._create_client() as client:
            try:
                resp = await client.post(url, json=payload, headers={"Content-Type": "application/json"})
            except (httpx.ConnectError, httpx.TimeoutException, httpx.NetworkError) as net_err:
                logger.error("Connection failed during activation: %s", net_err)
                raise RegistrationOfflineError(f"Middleware unreachable at {self.middleware_url}: {net_err}") from net_err

        # Check status codes and diagnostic responses
        if resp.status_code == 200 or resp.status_code == 201:
            data = resp.json()
            # Update client credentials from response
            self.hub_uuid = data.get("hub_uuid") or self.hub_uuid
            self.api_token = data.get("api_token") or self.api_token
            logger.info("Successfully activated hub: '%s' (UUID: %s)", data.get("hub_id"), self.hub_uuid)
            return data

        err_body = resp.text
        try:
            err_json = resp.json()
            err_msg = err_json.get("error") or err_json.get("detail") or err_json.get("message") or err_body
        except Exception:
            err_msg = err_body

        if resp.status_code in (400, 404):
            if "expired" in str(err_msg).lower() or "invalid" in str(err_msg).lower():
                raise InvalidActivationCodeError(f"Activation code invalid or expired: {err_msg}")
            raise ActivationError(f"Activation rejected (HTTP {resp.status_code}): {err_msg}")

        if resp.status_code == 409:
            raise DuplicateClaimError(f"Hub has already been claimed or duplicate installation detected: {err_msg}")

        if resp.status_code == 403:
            raise ActivationError(f"Activation permission denied: {err_msg}")

        raise ActivationError(f"Activation failed with HTTP {resp.status_code}: {err_msg}")

    # -----------------------------------------------------------------
    # Step 5 & 7: Retrieve Authoritative Hub Configuration
    # -----------------------------------------------------------------
    async def get_configuration(self) -> Dict[str, Any]:
        """
        Retrieves the authoritative hub configuration snapshot from Django.
        Includes resource associations, timezone, authorized device inventory, and version.
        """
        url = "/api/rnhub-agent/config/"
        async with self._create_client() as client:
            try:
                resp = await client.get(url, headers=self._get_headers())
            except (httpx.ConnectError, httpx.TimeoutException, httpx.NetworkError) as net_err:
                raise RegistrationOfflineError(f"Could not reach middleware for configuration: {net_err}") from net_err

        if resp.status_code == 200:
            data = resp.json()
            if data.get("status") in ("suspended", "retired"):
                raise HubSuspendedError(f"Hub has been marked '{data.get('status')}' by middleware.")
            return data

        if resp.status_code == 401 or resp.status_code == 403:
            raise RegistrationError(f"Middleware authentication failed (HTTP {resp.status_code}). Token may be revoked.")

        raise RegistrationError(f"Failed to fetch configuration (HTTP {resp.status_code}): {resp.text}")

    # -----------------------------------------------------------------
    # Step 6: Verify Proposed Physical Device Enrollment
    # -----------------------------------------------------------------
    async def authorize_device(
        self,
        registration_device_id: str,
        device_mac: str,
        port: Optional[str] = None,
        profile: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Checks middleware approval before enrolling physical hardware.
        If MAC is already known, middleware returns approved=True.
        If MAC is newly paired, middleware places binding in pending_approval.
        """
        url = "/api/rnhub-agent/devices/authorize/"
        payload = {
            "registration_device_id": registration_device_id,
            "device_mac": device_mac.strip().lower(),
            "port": port,
            "profile": profile,
        }

        async with self._create_client() as client:
            try:
                resp = await client.post(url, json=payload, headers=self._get_headers())
            except (httpx.ConnectError, httpx.TimeoutException, httpx.NetworkError) as net_err:
                raise RegistrationOfflineError(f"Could not reach middleware to authorize device: {net_err}") from net_err

        if resp.status_code in (200, 201):
            return resp.json()

        if resp.status_code == 400:
            raise DeviceAuthorizationError(f"Device enrollment rejected: {resp.text}")

        raise DeviceAuthorizationError(f"Device authorization failed (HTTP {resp.status_code}): {resp.text}")

    # -----------------------------------------------------------------
    # Step 7: Periodic Heartbeat & Health Telemetry
    # -----------------------------------------------------------------
    async def send_heartbeat(self, status_data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Reports hub health, active devices, and current applied config version.
        Returns status update, lease renewal, or notification of pending config changes.
        """
        url = "/api/rnhub-agent/heartbeat/"
        payload = status_data or {"status": "ONLINE"}

        async with self._create_client() as client:
            try:
                resp = await client.post(url, json=payload, headers=self._get_headers())
            except (httpx.ConnectError, httpx.TimeoutException, httpx.NetworkError) as net_err:
                raise RegistrationOfflineError(f"Heartbeat failed to reach middleware: {net_err}") from net_err

        if resp.status_code == 200:
            data = resp.json()
            if data.get("status") in ("suspended", "retired"):
                raise HubSuspendedError(f"Hub has been marked '{data.get('status')}' by middleware.")
            return data

        raise RegistrationError(f"Heartbeat rejected (HTTP {resp.status_code}): {resp.text}")

    # -----------------------------------------------------------------
    # Step 7: Confirm Configuration Version Applied
    # -----------------------------------------------------------------
    async def acknowledge_config(self, config_version: int, status: str = "APPLIED") -> Dict[str, Any]:
        """Confirms that a newer configuration revision from Django was applied locally."""
        url = "/api/rnhub-agent/config/ack/"
        payload = {
            "config_version": config_version,
            "status": status,
        }

        async with self._create_client() as client:
            try:
                resp = await client.post(url, json=payload, headers=self._get_headers())
            except Exception as exc:
                logger.warning("Failed to acknowledge config version %d: %s", config_version, exc)
                return {"acknowledged": False, "error": str(exc)}

        if resp.status_code == 200:
            return resp.json()
        return {"acknowledged": False, "status_code": resp.status_code}

    # -----------------------------------------------------------------
    # Synchronous Helpers for installer & CLI scripts
    # -----------------------------------------------------------------
    def activate_sync(
        self,
        activation_code: str,
        system_info: Optional[Dict[str, Any]] = None,
        client_public_key: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Synchronous wrapper for activation during CLI installation."""
        import asyncio
        return asyncio.run(self.activate(activation_code, system_info, client_public_key))

    def get_configuration_sync(self) -> Dict[str, Any]:
        """Synchronous wrapper for configuration retrieval."""
        import asyncio
        return asyncio.run(self.get_configuration())

    def authorize_device_sync(
        self,
        registration_device_id: str,
        device_mac: str,
        port: Optional[str] = None,
        profile: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Synchronous wrapper for device enrollment authorization."""
        import asyncio
        return asyncio.run(self.authorize_device(registration_device_id, device_mac, port, profile))
