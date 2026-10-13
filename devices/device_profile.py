"""
IoT Device Profiles and Channel Mapping for RnR Linux IoT Hub.
File: devices/device_profile.py

Implements Customization 5 specification:
- Extensible device profiles defining supported channels, permitted actions, and safety limits.
- Validates actions BEFORE any command is sent to hardware:
  * Rejects unsupported channel keys (e.g. relay:fan when only relay:light is defined).
  * Rejects unauthorized/unsupported actions (e.g. DIM_LIGHT when only LIGHTS_ON/OFF allowed).
  * Enforces safety parameters (e.g. max_on_seconds).
- Profiles persisted in SQLite `device_profiles` table.
"""

from __future__ import annotations
import json
import logging
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from storage.database import DatabaseManager

logger = logging.getLogger("iothub.device_profile")


@dataclass
class ChannelConfig:
    actions: List[str]
    max_on_seconds: Optional[int] = None
    failure_policy: str = "require_safe_state"


@dataclass
class DeviceProfile:
    profile_id: str
    name: str
    transport_type: str = "usb_serial"
    baud_rate: int = 115200
    channels: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    protocol: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "profile_id": self.profile_id,
            "name": self.name,
            "transport": {
                "type": self.transport_type,
                "baud_rate": self.baud_rate,
            },
            "channels": self.channels,
            "protocol": self.protocol,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> DeviceProfile:
        transport = data.get("transport", {})
        return cls(
            profile_id=data.get("profile_id", "default"),
            name=data.get("name", "Default Profile"),
            transport_type=transport.get("type", "usb_serial"),
            baud_rate=int(transport.get("baud_rate", 115200)),
            channels=data.get("channels", {}),
            protocol=data.get("protocol", {}),
        )


# Default built-in profile: Plant Controller V1 (Relays & Valves)
DEFAULT_PLANT_CONTROLLER_PROFILE = DeviceProfile(
    profile_id="plant_controller_v1",
    name="Plant Controller (Lighting & Irrigation Relays)",
    transport_type="usb_serial",
    baud_rate=115200,
    channels={
        "relay:light": {
            "actions": ["LIGHTS_ON", "LIGHTS_OFF", "STATUS"],
        },
        "relay:water_valve": {
            "actions": ["WATER_VALVE_ON", "WATER_VALVE_OFF", "STATUS"],
            "max_on_seconds": 90,
            "failure_policy": "require_safe_state",
        },
    },
    protocol={
        "encoding": "utf-8",
        "framing": "newline_json",
        "require_acknowledgment": True,
        "command_id_required": True,
    },
)

# Standard Arduino Nano LED / Relay profile
DEFAULT_ARDUINO_NANO_PROFILE = DeviceProfile(
    profile_id="arduino_nano_v1",
    name="Arduino Nano (Built-in LED / Relay Driver)",
    transport_type="usb_serial",
    baud_rate=9600,
    channels={
        "relay:light": {
            "actions": ["LIGHTS_ON", "LIGHTS_OFF", "STATUS", "HELLO", "HANDSHAKE"],
        },
    },
    protocol={
        "encoding": "ascii",
        "framing": "newline",
        "require_acknowledgment": True,
    },
)


class DeviceProfileRegistry:
    """Manages storage, retrieval, and validation of device profiles in SQLite."""

    def __init__(self, db: DatabaseManager):
        self.db = db
        self._cache: Dict[str, DeviceProfile] = {}

    async def initialize_defaults(self) -> None:
        """Seeds standard built-in profiles into SQLite if not already stored."""
        for profile in (DEFAULT_PLANT_CONTROLLER_PROFILE, DEFAULT_ARDUINO_NANO_PROFILE):
            existing = await self.get_profile(profile.profile_id)
            if existing is None:
                await self.save_profile(profile)

    async def save_profile(self, profile: DeviceProfile) -> None:
        """Saves or updates a device profile in SQLite."""
        now = datetime.now(timezone.utc).isoformat()
        conn = self.db.connection
        p_json = json.dumps(profile.to_dict())

        await conn.execute(
            """
            INSERT INTO device_profiles (profile_id, name, profile_json, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(profile_id) DO UPDATE SET
                name = excluded.name,
                profile_json = excluded.profile_json,
                updated_at = excluded.updated_at
            """,
            (profile.profile_id, profile.name, p_json, now, now),
        )
        await conn.commit()
        self._cache[profile.profile_id] = profile
        logger.info("Saved device profile '%s' (%s)", profile.profile_id, profile.name)

    async def get_profile(self, profile_id: str) -> Optional[DeviceProfile]:
        """Retrieves profile by ID from cache or database."""
        if profile_id in self._cache:
            return self._cache[profile_id]

        conn = self.db.connection
        async with conn.execute(
            "SELECT profile_id, name, profile_json FROM device_profiles WHERE profile_id = ?",
            (profile_id,),
        ) as cur:
            row = await cur.fetchone()
            if not row:
                return None
            data = json.loads(row["profile_json"])
            prof = DeviceProfile.from_dict(data)
            self._cache[profile_id] = prof
            return prof

    async def list_profiles(self) -> List[DeviceProfile]:
        """Lists all registered profiles."""
        conn = self.db.connection
        async with conn.execute("SELECT profile_json FROM device_profiles ORDER BY profile_id ASC") as cur:
            rows = await cur.fetchall()
            profiles = []
            for r in rows:
                p = DeviceProfile.from_dict(json.loads(r["profile_json"]))
                profiles.append(p)
                self._cache[p.profile_id] = p
            return profiles

    @staticmethod
    def validate_action(
        profile: DeviceProfile,
        channel_key: str,
        action: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> Tuple[bool, Optional[str]]:
        """
        Validates if an action is permitted by the device profile.
        Returns:
            Tuple[is_valid: bool, error_message: Optional[str]]
        """
        # 1. Does the device expose the requested channel?
        if channel_key not in profile.channels:
            avail = list(profile.channels.keys())
            return False, f"Channel '{channel_key}' not supported by profile '{profile.profile_id}'. Available: {avail}"

        channel_def = profile.channels[channel_key]
        allowed_actions = channel_def.get("actions", [])

        # 2. Is the command permitted for that channel?
        if action not in allowed_actions:
            return False, f"Action '{action}' not permitted for channel '{channel_key}'. Allowed actions: {allowed_actions}"

        # 3. Are safety limits satisfied?
        params = params or {}
        max_on = channel_def.get("max_on_seconds")
        if max_on is not None and "duration_seconds" in params:
            try:
                requested_duration = float(params["duration_seconds"])
                if requested_duration > max_on:
                    return False, f"Requested duration {requested_duration}s exceeds safety limit of {max_on}s on {channel_key}"
            except (ValueError, TypeError):
                pass

        return True, None
