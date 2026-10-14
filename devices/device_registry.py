"""
Interactive USB Port Enrollment & Dynamic MAC Binding Registry for RnR Linux IoT Hub.
File: devices/device_registry.py

Implements Customization 4 specification:
- Stores persistent device MAC identities and hardware bindings in SQLite table `device_bindings`.
- Treats MAC as the durable logical identity; Linux serial port (/dev/ttyUSB*) as transient connection.
- Port Renumbering Survival:
  When a device changes from /dev/ttyUSB0 to /dev/ttyUSB1 across reboots or replugs,
  the registry dynamically re-resolves the port without losing schedule execution or state.
- Interactive scanning and enrollment for CLI `iothubctl devices` subcommands.
"""

from __future__ import annotations
import glob
import logging
import os
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from storage.database import DatabaseManager

logger = logging.getLogger("iothub.device_registry")


@dataclass
class ScannedPortInfo:
    """Represents a discovered serial interface."""
    port: str
    vid: str = ""
    pid: str = ""
    serial_number: str = ""
    description: str = ""
    reported_mac: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class DeviceBinding:
    """Persistent binding between a hardware device identity (MAC) and hub configuration."""
    device_mac: str
    device_name: str
    port_hint: str
    usb_vid: str = ""
    usb_pid: str = ""
    usb_serial: str = ""
    connection_mode: str = "auto"
    approved: bool = True
    profile_id: str = "plant_controller_v1"
    created_at: str = ""
    updated_at: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class DeviceRegistry:
    """Manages persistent device enrollments, MAC identity resolution, and dynamic port rebinding."""

    def __init__(self, db: DatabaseManager):
        self.db = db

    async def scan_serial_ports(self) -> List[ScannedPortInfo]:
        """
        Scans system serial interfaces (/dev/ttyUSB*, /dev/ttyACM*).
        Extracts USB VID/PID and device metadata via pyudev or sysfs.
        """
        scanned: List[ScannedPortInfo] = []
        candidate_ports = sorted(glob.glob("/dev/ttyUSB*") + glob.glob("/dev/ttyACM*"))

        # Try pyudev for rich USB metadata if available
        udev_devices: Dict[str, Any] = {}
        try:
            import pyudev
            context = pyudev.Context()
            for device in context.list_devices(subsystem="tty"):
                dev_node = device.device_node
                if dev_node:
                    udev_devices[dev_node] = device
        except Exception:
            pass

        for port in candidate_ports:
            vid, pid, serial_no, desc = "", "", "", ""
            udev_dev = udev_devices.get(port)
            if udev_dev:
                props = getattr(udev_dev, "properties", udev_dev)
                vid = str(props.get("ID_VENDOR_ID", "") or "")
                pid = str(props.get("ID_MODEL_ID", "") or "")
                serial_no = str(props.get("ID_SERIAL_SHORT", "") or "")
                desc = str(props.get("ID_MODEL_FROM_DATABASE", "") or props.get("ID_MODEL", "USB Serial Device"))
            else:
                desc = "Serial Port Interface"

            scanned.append(
                ScannedPortInfo(
                    port=port,
                    vid=vid,
                    pid=pid,
                    serial_number=serial_no,
                    description=desc,
                )
            )
        return scanned

    async def enroll_device(
        self,
        device_mac: str,
        device_name: str,
        port_hint: str = "",
        usb_vid: str = "",
        usb_pid: str = "",
        usb_serial: str = "",
        profile_id: str = "plant_controller_v1",
        connection_mode: str = "auto",
        approved: bool = True,
    ) -> DeviceBinding:
        """
        Enrolls a device MAC identity in the persistent SQLite registry.
        """
        now = datetime.now(timezone.utc).isoformat()
        clean_mac = device_mac.strip().lower()
        conn = self.db.connection

        await conn.execute(
            """
            INSERT INTO device_bindings (
                device_mac, device_name, port_hint, usb_vid, usb_pid, usb_serial,
                connection_mode, approved, profile_id, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(device_mac) DO UPDATE SET
                device_name = excluded.device_name,
                port_hint = excluded.port_hint,
                usb_vid = excluded.usb_vid,
                usb_pid = excluded.usb_pid,
                usb_serial = excluded.usb_serial,
                connection_mode = excluded.connection_mode,
                approved = excluded.approved,
                profile_id = excluded.profile_id,
                updated_at = excluded.updated_at
            """,
            (
                clean_mac,
                device_name,
                port_hint,
                usb_vid,
                usb_pid,
                usb_serial,
                connection_mode,
                1 if approved else 0,
                profile_id,
                now,
                now,
            ),
        )
        await conn.commit()
        logger.info("Enrolled device '%s' (%s) bound to port '%s'", clean_mac, device_name, port_hint)

        return DeviceBinding(
            device_mac=clean_mac,
            device_name=device_name,
            port_hint=port_hint,
            usb_vid=usb_vid,
            usb_pid=usb_pid,
            usb_serial=usb_serial,
            connection_mode=connection_mode,
            approved=approved,
            profile_id=profile_id,
            created_at=now,
            updated_at=now,
        )

    async def unbind_device(self, device_mac: str) -> bool:
        """Removes an enrollment binding for a device MAC."""
        clean_mac = device_mac.strip().lower()
        conn = self.db.connection
        cur = await conn.execute("DELETE FROM device_bindings WHERE device_mac = ?", (clean_mac,))
        await conn.commit()
        return cur.rowcount > 0

    async def get_binding(self, device_mac: str) -> Optional[DeviceBinding]:
        """Retrieves persistent binding by MAC."""
        clean_mac = device_mac.strip().lower()
        conn = self.db.connection
        async with conn.execute(
            """
            SELECT device_mac, device_name, port_hint, usb_vid, usb_pid, usb_serial,
                   connection_mode, approved, profile_id, created_at, updated_at
            FROM device_bindings WHERE device_mac = ?
            """,
            (clean_mac,),
        ) as cur:
            row = await cur.fetchone()
            if not row:
                return None
            return DeviceBinding(
                device_mac=row["device_mac"],
                device_name=row["device_name"],
                port_hint=row["port_hint"],
                usb_vid=row["usb_vid"] or "",
                usb_pid=row["usb_pid"] or "",
                usb_serial=row["usb_serial"] or "",
                connection_mode=row["connection_mode"] or "auto",
                approved=bool(row["approved"]),
                profile_id=row["profile_id"] or "plant_controller_v1",
                created_at=row["created_at"],
                updated_at=row["updated_at"],
            )

    async def list_bindings(self, approved_only: bool = False) -> List[DeviceBinding]:
        """Lists all registered device bindings."""
        conn = self.db.connection
        query = "SELECT * FROM device_bindings"
        params: List[Any] = []
        if approved_only:
            query += " WHERE approved = 1"
        query += " ORDER BY device_name ASC"

        async with conn.execute(query, params) as cur:
            rows = await cur.fetchall()
            return [
                DeviceBinding(
                    device_mac=r["device_mac"],
                    device_name=r["device_name"],
                    port_hint=r["port_hint"],
                    usb_vid=r["usb_vid"] or "",
                    usb_pid=r["usb_pid"] or "",
                    usb_serial=r["usb_serial"] or "",
                    connection_mode=r["connection_mode"] or "auto",
                    approved=bool(r["approved"]),
                    profile_id=r["profile_id"] or "plant_controller_v1",
                    created_at=r["created_at"],
                    updated_at=r["updated_at"],
                )
                for r in rows
            ]

    async def resolve_active_port(self, device_mac: str) -> Optional[str]:
        """
        Dynamically locates the current Linux port for the specified device MAC.
        Handles USB port renumbering (e.g. /dev/ttyUSB0 -> /dev/ttyUSB1):
        1. Checks if current port_hint exists and is accessible.
        2. If port_hint is unavailable or port changed, searches scanned serial ports.
        3. Updates the persistent port_hint in SQLite so future commands use the resolved port.
        """
        binding = await self.get_binding(device_mac)
        if not binding or not binding.approved:
            return None

        hint = binding.port_hint

        # 1. Search active ports for direct hardware identity match (USB serial)
        scanned = await self.scan_serial_ports()
        for s in scanned:
            if binding.usb_serial and s.serial_number and binding.usb_serial == s.serial_number:
                if s.port != hint:
                    await self.update_port_hint(device_mac, s.port)
                    logger.info("Resolved renumbered port for '%s': %s -> %s", device_mac, hint, s.port)
                return s.port

        # 2. Check if current port_hint exists on system
        if hint and os.path.exists(hint):
            return hint

        # 3. If exactly one serial port is active and port_hint was default, assign it
        if len(scanned) == 1:
            active_port = scanned[0].port
            await self.update_port_hint(device_mac, active_port)
            return active_port

        return hint if (hint and os.path.exists(hint)) else None

    async def update_port_hint(self, device_mac: str, new_port: str) -> None:
        """Updates the transient port hint for an enrolled device."""
        now = datetime.now(timezone.utc).isoformat()
        conn = self.db.connection
        await conn.execute(
            "UPDATE device_bindings SET port_hint = ?, updated_at = ? WHERE device_mac = ?",
            (new_port, now, device_mac.strip().lower()),
        )
        await conn.commit()
