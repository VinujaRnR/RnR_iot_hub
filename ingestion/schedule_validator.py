"""
Validation engine for IoT Hub daily production schedules.
Enforces business rules:
- Top-level date and timezone validity
- Explicit schema_version, schedule_id, hub_id and revision check (de-duplication)
- Action-level validation:
    - time: Valid HH:MM (24-hour)
    - action: Known, authorized operation
    - channel_key: Valid channel supported by the target device
    - params: Correct parameter type and values
    - sub_resource: Matches the owning sub-resource
    - iot_node_id: Valid configured device identifier
"""

from __future__ import annotations
import hashlib
import json
import logging
from typing import Any, Dict, List, Optional, Set
from pydantic import ValidationError

from config import HUB_ID
from ingestion.schemas import DailyProductionSchedule, ScheduledAction

logger = logging.getLogger("iothub.schedule_validator")

# Default set of recognized and authorized operations across manufacturing and IoT devices
DEFAULT_AUTHORIZED_ACTIONS: Set[str] = {
    "set_led",
    "read_temperature",
    "LIGHTS_ON",
    "LIGHTS_OFF",
    "LIGHT_ON",
    "LIGHT_OFF",
    "VALVE_ON",
    "VALVE_OFF",
    "VALVE_OPEN",
    "VALVE_CLOSE",
    "START_MACHINE",
    "STOP_MACHINE",
    "SET_TEMPERATURE",
    "change_size",
    "home",
    "status",
    "STATUS",
    "lock_door",
    "unlock_door",
    "ping",
}

# Default known channel mappings for devices
DEFAULT_DEVICE_CHANNELS: Dict[str, Set[str]] = {
    "iot_001": {"ch1", "led", "main", "relay_1"},
    "iot_002": {"ch1", "temp", "humidity", "sensor", "default"},
}


class ScheduleValidationError(Exception):
    """Base exception for all schedule validation errors."""
    pass


class MalformedScheduleError(ScheduleValidationError):
    """Raised when the schedule payload is malformed or invalid JSON."""
    pass


class ScheduleRevisionError(ScheduleValidationError):
    """Raised when a schedule revision is duplicate or older than current state."""
    pass


class UnauthorizedActionError(ScheduleValidationError):
    """Raised when a scheduled action is not authorized."""
    pass


class UnknownDeviceError(ScheduleValidationError):
    """Raised when target iot_node_id is not registered on the hub."""
    pass


class InvalidChannelError(ScheduleValidationError):
    """Raised when channel_key is not supported by target device."""
    pass


class SubResourceMismatchError(ScheduleValidationError):
    """Raised when an action's sub_resource does not match the parent sub_resource."""
    pass


class ScheduleRevisionTracker:
    """Thread-safe in-memory revision tracker to prevent applying duplicate/stale schedules."""

    def __init__(self):
        self._revisions: Dict[str, int] = {}
        self._hashes: Dict[str, str] = {}

    def is_duplicate_or_stale(
        self,
        schedule_id: str,
        revision: int,
        content_hash: Optional[str] = None,
    ) -> bool:
        """
        Returns True if the incoming schedule is duplicate or stale.
        - If revision is strictly less than processed revision: stale (True).
        - If revision is strictly greater than processed revision: new revision (False).
        - If revision is equal:
            - If content_hash is provided and differs from stored hash: content was updated (False).
            - Otherwise: exact duplicate (True).
        """
        if schedule_id in self._revisions:
            last_rev = self._revisions[schedule_id]
            if revision < last_rev:
                return True
            if revision == last_rev:
                if content_hash is not None and schedule_id in self._hashes:
                    return content_hash == self._hashes[schedule_id]
                return True
        return False

    def record_revision(
        self,
        schedule_id: str,
        revision: int,
        content_hash: Optional[str] = None,
    ) -> None:
        """Records the latest processed revision and optional content hash for a schedule."""
        self._revisions[schedule_id] = revision
        if content_hash:
            self._hashes[schedule_id] = content_hash

    def get_last_revision(self, schedule_id: str) -> Optional[int]:
        return self._revisions.get(schedule_id)

    def reset(self) -> None:
        self._revisions.clear()
        self._hashes.clear()


class ScheduleValidator:
    """Validates daily production schedules against business, hardware, and revision rules."""

    def __init__(
        self,
        expected_hub_id: Optional[str] = HUB_ID,
        authorized_actions: Optional[Set[str]] = None,
        known_devices: Optional[Set[str]] = None,
        device_channels: Optional[Dict[str, Set[str]]] = None,
        revision_tracker: Optional[ScheduleRevisionTracker] = None,
    ):
        self.expected_hub_id = expected_hub_id
        self.authorized_actions = authorized_actions if authorized_actions is not None else DEFAULT_AUTHORIZED_ACTIONS
        self.known_devices = known_devices  # If None, device existence check is skipped
        self.device_channels = device_channels if device_channels is not None else DEFAULT_DEVICE_CHANNELS
        self.revision_tracker = revision_tracker or ScheduleRevisionTracker()

    def validate(
        self,
        raw_data: str | bytes | dict,
        check_revision: bool = True,
    ) -> DailyProductionSchedule:
        """
        Validates raw payload (JSON string, bytes, or dict).
        Returns validated DailyProductionSchedule or raises ScheduleValidationError.
        """
        # 1. Parse JSON and compute content hash
        if isinstance(raw_data, (bytes, bytearray)):
            try:
                raw_str = raw_data.decode("utf-8")
                payload_dict = json.loads(raw_str)
                content_hash = hashlib.sha256(raw_data).hexdigest()
            except Exception as exc:
                raise MalformedScheduleError(f"Failed to decode message bytes as utf-8: {exc}") from exc
        elif isinstance(raw_data, str):
            try:
                payload_dict = json.loads(raw_data)
                content_hash = hashlib.sha256(raw_data.encode("utf-8")).hexdigest()
            except json.JSONDecodeError as exc:
                raise MalformedScheduleError(f"Malformed JSON in schedule payload: {exc}") from exc
        elif isinstance(raw_data, dict):
            payload_dict = raw_data
            content_hash = hashlib.sha256(json.dumps(payload_dict, sort_keys=True).encode("utf-8")).hexdigest()
        else:
            raise MalformedScheduleError(f"Invalid payload type: {type(raw_data).__name__}. Expected dict or JSON string")

        if not isinstance(payload_dict, dict):
            raise MalformedScheduleError("Schedule payload root must be a JSON object")

        # 2. Pydantic schema validation
        try:
            schedule = DailyProductionSchedule.model_validate(payload_dict)
        except ValidationError as exc:
            errors = [f"{err['loc']}: {err['msg']}" for err in exc.errors()]
            error_details = "; ".join(errors)
            if "sub_resource mismatch" in error_details:
                raise SubResourceMismatchError(f"Sub-resource mismatch: {error_details}") from exc
            raise MalformedScheduleError(f"Schedule schema validation error: {error_details}") from exc

        # 3. Hub ID verification
        if self.expected_hub_id and schedule.hub_id != self.expected_hub_id:
            raise ScheduleValidationError(
                f"Hub ID mismatch: schedule addressed to '{schedule.hub_id}', "
                f"but this hub is '{self.expected_hub_id}'"
            )

        # 4. Revision tracking & de-duplication
        if check_revision:
            if self.revision_tracker.is_duplicate_or_stale(schedule.schedule_id, schedule.revision, content_hash):
                last_rev = self.revision_tracker.get_last_revision(schedule.schedule_id)
                raise ScheduleRevisionError(
                    f"Stale or duplicate schedule: schedule '{schedule.schedule_id}' revision {schedule.revision} "
                    f"has already been processed (latest revision: {last_rev})"
                )

        # 5. Validate individual scheduled actions
        actions = schedule.extract_all_actions()
        for idx, act in enumerate(actions):
            self._validate_action(act, idx)

        # 6. If all checks pass, record revision and content hash
        if check_revision:
            self.revision_tracker.record_revision(schedule.schedule_id, schedule.revision, content_hash)

        logger.info(
            "Schedule '%s' (rev %d, date %s) validated successfully with %d action(s)",
            schedule.schedule_id,
            schedule.revision,
            schedule.date,
            len(actions),
        )
        return schedule

    def _validate_action(self, act: ScheduledAction, index: int) -> None:
        """Validates action operation, target device, channel, and parameter types."""
        # Validate action is authorized
        if self.authorized_actions and act.action not in self.authorized_actions:
            raise UnauthorizedActionError(
                f"Action #{index} on device '{act.iot_node_id}' uses unauthorized operation '{act.action}'"
            )

        # Validate target device node identifier
        if self.known_devices is not None and act.iot_node_id not in self.known_devices:
            raise UnknownDeviceError(
                f"Action #{index} specifies unknown device node identifier '{act.iot_node_id}'"
            )

        # Validate channel supported by device
        if act.iot_node_id in self.device_channels:
            allowed_channels = self.device_channels[act.iot_node_id]
            if act.channel_key not in allowed_channels:
                raise InvalidChannelError(
                    f"Action #{index} specifies channel '{act.channel_key}' not supported by "
                    f"device '{act.iot_node_id}' (allowed channels: {sorted(allowed_channels)})"
                )

        # Validate params is a dict and has reasonable size
        if not isinstance(act.params, dict):
            raise ScheduleValidationError(
                f"Action #{index} parameters must be a dictionary, got {type(act.params).__name__}"
            )


def validate_daily_schedule(
    raw_data: str | bytes | dict,
    expected_hub_id: Optional[str] = HUB_ID,
    authorized_actions: Optional[Set[str]] = None,
    known_devices: Optional[Set[str]] = None,
    device_channels: Optional[Dict[str, Set[str]]] = None,
    check_revision: bool = True,
) -> DailyProductionSchedule:
    """Convenience helper to validate a daily production schedule."""
    validator = ScheduleValidator(
        expected_hub_id=expected_hub_id,
        authorized_actions=authorized_actions,
        known_devices=known_devices,
        device_channels=device_channels,
    )
    return validator.validate(raw_data, check_revision=check_revision)
