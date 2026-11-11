"""
Automatic USB Device Discovery and Approval Enrollment for RnR Linux IoT Hub.
File: devices/auto_enrollment.py

Implements RNHub V2 Auto-Enrollment Specification:
1. Discovers connected USB serial interfaces (/dev/ttyUSB*, /dev/ttyACM*).
2. Queries devices using standard protocol handshake (HELLO/HANDSHAKE).
3. Reads the hardware MAC / unique device identifier.
4. Compares discovered hardware with middleware-approved device records (authorized_devices).
5. Validates hardware communication profile (e.g., plant_controller_v1).
6. Automatically binds approved matching hardware to local SQLite registry without manual CLI commands.
7. Handles USB port renumbering dynamically (/dev/ttyUSB0 -> /dev/ttyUSB1) using stable MAC identity.
8. Rejects or marks unapproved/unknown devices as pending.
9. Separates device configuration from actuation: ensures safe startup states (outputs OFF).
"""

from __future__ import annotations
import asyncio
import logging
import os
import re
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

from storage.database import DatabaseManager
from storage.registration_repository import RegistrationRepository
from devices.device_registry import DeviceRegistry, ScannedPortInfo, DeviceBinding
from devices.device_profile import DeviceProfileRegistry
from devices.device_manager import DeviceManager

logger = logging.getLogger("iothub.auto_enrollment")


@dataclass
class AutoEnrollmentReport:
    """Summary of hardware auto-enrollment and reconciliation."""
    bound: List[Dict[str, Any]] = field(default_factory=list)
    pending: List[Dict[str, Any]] = field(default_factory=list)
    rejected: List[Dict[str, Any]] = field(default_factory=list)
    port_changes: List[Dict[str, Any]] = field(default_factory=list)
    device_results: List[Dict[str, Any]] = field(default_factory=list)
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        if not d.get("device_results"):
            d["device_results"] = self.to_middleware_device_results()
        return d

    def to_middleware_device_results(self) -> List[Dict[str, Any]]:
        results = []
        for b in self.bound:
            results.append({
                "device_uid": b.get("registration_device_id") or b.get("device_uid") or b.get("mac"),
                "status": "configured",
                "port": b.get("port"),
                "mac": b.get("mac"),
            })
        for p in self.pending:
            results.append({
                "device_uid": p.get("registration_device_id") or p.get("device_uid") or p.get("name"),
                "status": "pending_hardware_binding",
                "reason": p.get("reason"),
            })
        for r in self.rejected:
            results.append({
                "device_uid": r.get("registration_device_id") or r.get("mac") or "unknown",
                "status": "unapproved",
                "reason": r.get("reason"),
            })
        return results


class AutoEnrollmentManager:
    """
    Coordinates automatic discovery, middleware matching, and safe enrollment
    of physical USB IoT devices against authorized Django records.
    """

    def __init__(
        self,
        db: DatabaseManager,
        device_registry: Optional[DeviceRegistry] = None,
        device_manager: Optional[DeviceManager] = None,
        reg_repo: Optional[RegistrationRepository] = None,
        profile_registry: Optional[DeviceProfileRegistry] = None,
    ):
        self.db = db
        self.device_registry = device_registry or DeviceRegistry(db)
        self.device_manager = device_manager
        self.reg_repo = reg_repo or RegistrationRepository(db)
        self.profile_registry = profile_registry or DeviceProfileRegistry(db)

    @staticmethod
    def normalize_mac(mac: Optional[str]) -> Optional[str]:
        """Normalizes MAC address to 12-char lowercase hex string without delimiters."""
        if not mac:
            return None
        cleaned = re.sub(r"[^0-9a-fA-F]", "", str(mac)).strip().lower()
        if len(cleaned) == 12:
            return cleaned
        return None

    async def identify_device_on_port(
        self,
        port: str,
        scanned_info: Optional[ScannedPortInfo] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Interrogates a physical or simulated port to identify its hardware MAC address
        and capabilities without performing any actuation.
        """
        # 1. Check if scanned info already has explicit reported MAC
        if scanned_info and scanned_info.reported_mac:
            norm = self.normalize_mac(scanned_info.reported_mac)
            if norm:
                return {
                    "mac": norm,
                    "port": port,
                    "vid": scanned_info.vid,
                    "pid": scanned_info.pid,
                    "serial": scanned_info.serial_number,
                    "source": "scanned_info",
                }

        # 2. Check if device_manager has a registered device driver for this port or alias
        if self.device_manager:
            driver = self.device_manager.get_device(port)
            if driver:
                # Query device identity via protocol handshake without actuating
                if hasattr(driver, "execute_command"):
                    try:
                        res = await driver.execute_command("HANDSHAKE", {})
                        if isinstance(res, dict):
                            dev_id = res.get("device_id") or res.get("mac")
                            norm = self.normalize_mac(dev_id)
                            if norm:
                                return {
                                    "mac": norm,
                                    "port": port,
                                    "source": "handshake",
                                    "driver": driver,
                                }
                    except Exception as exc:
                        logger.debug("Handshake query failed on driver for '%s': %s", port, exc)

                # Check device_id attribute directly
                raw_id = getattr(driver, "device_id", None)
                if raw_id and not str(raw_id).startswith("/dev/"):
                    norm = self.normalize_mac(raw_id)
                    if norm:
                        return {"mac": norm, "port": port, "source": "driver_id"}

        # 3. Check USB serial number if it represents a 12-character MAC hex string
        if scanned_info and scanned_info.serial_number:
            norm = self.normalize_mac(scanned_info.serial_number)
            if norm and len(norm) == 12:
                return {
                    "mac": norm,
                    "port": port,
                    "vid": scanned_info.vid,
                    "pid": scanned_info.pid,
                    "serial": scanned_info.serial_number,
                    "source": "usb_serial",
                }

        # 4. Attempt direct serial probe if port exists and pySerial is available
        if os.path.exists(port):
            try:
                import serial
                # Non-blocking probe with short timeout
                def _probe_sync():
                    try:
                        with serial.Serial(port=port, baudrate=9600, timeout=0.8) as ser:
                            ser.write(b'{"type":"handshake"}\n')
                            line = ser.readline().decode("utf-8", errors="ignore").strip()
                            if line:
                                import json
                                data = json.loads(line)
                                return data.get("device_id") or data.get("mac")
                    except Exception:
                        return None
                    return None

                loop = asyncio.get_running_loop()
                probed_id = await loop.run_in_executor(None, _probe_sync)
                if probed_id:
                    norm = self.normalize_mac(probed_id)
                    if norm:
                        return {"mac": norm, "port": port, "source": "serial_probe"}
            except Exception as exc:
                logger.debug("Serial probe error on %s: %s", port, exc)

        return None

    async def discover_and_reconcile(
        self,
        candidate_ports: Optional[List[ScannedPortInfo]] = None,
    ) -> Dict[str, Any]:
        """
        Discovers connected USB devices, matches them against the middleware-approved
        inventory in SQLite, and automatically updates device bindings.
        """
        report = AutoEnrollmentReport()

        # 1. Retrieve authorized device inventory from Django middleware snapshot
        authorized_inventory = await self.reg_repo.get_authorized_devices()
        try:
            conn = self.db.connection
            async with conn.execute("SELECT device_uid, name, mac, profile, approved, status FROM approved_devices") as cur:
                appr_rows = await cur.fetchall()
            existing_ids = {str(d.get("registration_device_id") or d.get("device_uid")) for d in authorized_inventory}
            for ar in appr_rows:
                duid = ar["device_uid"]
                if duid not in existing_ids:
                    authorized_inventory.append({
                        "registration_device_id": duid,
                        "device_uid": duid,
                        "name": ar["name"],
                        "device_mac": ar["mac"],
                        "profile": ar["profile"],
                        "status": "approved" if ar["approved"] else "revoked",
                    })
        except Exception:
            pass

        auth_by_mac: Dict[str, Dict[str, Any]] = {}
        for dev in authorized_inventory:
            mac = self.normalize_mac(dev.get("device_mac") or dev.get("mac"))
            if mac:
                auth_by_mac[mac] = dev

        # 2. Scan physical/serial ports
        if candidate_ports is not None:
            scanned_ports = candidate_ports
        else:
            scanned_ports = await self.device_registry.scan_serial_ports()

        discovered_hardware: List[Dict[str, Any]] = []
        for port_info in scanned_ports:
            ident = await self.identify_device_on_port(port_info.port, port_info)
            if ident:
                discovered_hardware.append(ident)
            else:
                logger.debug("Could not determine hardware MAC for port '%s'", port_info.port)

        matched_macs: Set[str] = set()

        # 3. Match discovered hardware with middleware-approved records
        for hw in discovered_hardware:
            mac = hw["mac"]
            port = hw["port"]

            auth_record = auth_by_mac.get(mac)
            if auth_record:
                reg_status = str(auth_record.get("status") or "").lower()
                if reg_status == "revoked":
                    logger.warning("Rejected connection from revoked device MAC '%s' on %s", mac, port)
                    report.rejected.append({
                        "mac": mac,
                        "port": port,
                        "reason": "DEVICE_REVOKED",
                        "registration_device_id": auth_record.get("registration_device_id"),
                    })
                    continue

                matched_macs.add(mac)
                reg_id = auth_record["registration_device_id"]
                dev_name = auth_record.get("name") or reg_id
                profile_id = auth_record.get("profile") or "plant_controller_v1"

                # Check profile validity
                profile_obj = await self.profile_registry.get_profile(profile_id)
                if not profile_obj:
                    logger.warning("Profile '%s' not found for device '%s'; falling back to plant_controller_v1", profile_id, reg_id)
                    profile_id = "plant_controller_v1"

                # Check existing persistent binding for port renumbering
                existing_binding = await self.device_registry.get_binding(mac)
                if existing_binding and existing_binding.port_hint and existing_binding.port_hint != port:
                    logger.info(
                        "Detected port renumbering for device '%s' (%s): %s -> %s",
                        dev_name,
                        mac,
                        existing_binding.port_hint,
                        port,
                    )
                    report.port_changes.append({
                        "mac": mac,
                        "registration_device_id": reg_id,
                        "old_port": existing_binding.port_hint,
                        "new_port": port,
                    })

                # Automatically create or update binding in SQLite
                binding = await self.device_registry.enroll_device(
                    device_mac=mac,
                    device_name=dev_name,
                    port_hint=port,
                    usb_vid=hw.get("vid", ""),
                    usb_pid=hw.get("pid", ""),
                    usb_serial=hw.get("serial", ""),
                    profile_id=profile_id,
                    connection_mode="auto",
                    approved=True,
                    registration_device_id=reg_id,
                )

                # Update authorized_devices record to 'bound'
                await self.reg_repo.update_device_authorization(
                    registration_device_id=reg_id,
                    device_mac=mac,
                    status="bound",
                    bound_port=port,
                )
                try:
                    conn = self.db.connection
                    await conn.execute(
                        "UPDATE approved_devices SET status = 'bound', bound_port = ?, updated_at = ? WHERE device_uid = ? OR mac = ?",
                        (port, datetime.now(timezone.utc).isoformat(), reg_id, mac),
                    )
                    await conn.commit()
                except Exception:
                    pass

                # Register device with DeviceManager in SAFE startup state (outputs OFF)
                if self.device_manager:
                    self._register_driver_safe(reg_id, dev_name, mac, port, profile_id)

                logger.info(
                    "Automatically bound approved device '%s' [ID: %s, MAC: %s] to %s",
                    dev_name,
                    reg_id,
                    mac,
                    port,
                )
                report.bound.append({
                    "registration_device_id": reg_id,
                    "name": dev_name,
                    "mac": mac,
                    "port": port,
                    "profile": profile_id,
                    "status": "BOUND",
                })

            else:
                # Discovered device is not in middleware-approved inventory
                logger.warning("Unregistered hardware controller with MAC '%s' on port '%s' rejected.", mac, port)
                report.rejected.append({
                    "mac": mac,
                    "port": port,
                    "reason": "NOT_REGISTERED_IN_MIDDLEWARE",
                })

        # 4. Check authorized devices that were NOT matched
        for auth_dev in authorized_inventory:
            auth_mac = self.normalize_mac(auth_dev.get("device_mac"))
            reg_id = auth_dev.get("registration_device_id", "")
            if not auth_mac or auth_mac not in matched_macs:
                reg_status = str(auth_dev.get("status") or "").lower()
                if reg_status != "revoked":
                    report.pending.append({
                        "registration_device_id": reg_id,
                        "name": auth_dev.get("name"),
                        "device_mac": auth_dev.get("device_mac"),
                        "reason": "AWAITING_MATCHING_HARDWARE_OR_MAC",
                    })

        report.device_results = report.to_middleware_device_results()
        return report.to_dict()

    def _register_driver_safe(
        self,
        reg_id: str,
        name: str,
        mac: str,
        port: str,
        profile_id: str,
    ) -> None:
        """
        Registers device driver in DeviceManager ensuring safe initial state.
        Never executes any actuation command during registration.
        """
        if not self.device_manager:
            return

        existing = self.device_manager.get_device(reg_id)
        if not existing:
            # Look for existing driver on the same port to reuse or update
            existing = self.device_manager.get_device(port)

        if not existing:
            try:
                from devices.arduino_nano import ArduinoNanoDevice
                driver = ArduinoNanoDevice(port=port, device_id=reg_id, name=name)
                self.device_manager.register_device(reg_id, driver)
            except Exception as exc:
                logger.debug("Could not instantiate ArduinoNanoDevice for %s: %s", port, exc)
                return
        else:
            driver = existing
            if hasattr(driver, "port") and driver.port != port:
                driver.port = port
            if hasattr(driver, "device_id"):
                driver.device_id = reg_id

        # Register standard aliases
        self.device_manager.register_alias(reg_id, reg_id)
        if name and name != reg_id:
            self.device_manager.register_alias(name, reg_id)
        if mac and mac != reg_id:
            self.device_manager.register_alias(mac, reg_id)
        if port:
            self.device_manager.register_alias(port, reg_id)
            self.device_manager.register_alias(port.replace("/dev/", ""), reg_id)
