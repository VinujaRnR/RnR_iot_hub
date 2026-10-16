"""
Schedule Normalizer for RnR Linux IoT Hub.
File: ingestion/schedule_normalizer.py

Normalizes nested backend JSON (production orders -> resources -> sub_resources -> actions)
into flat, normalized action records for efficient execution by the scheduler.
Uses sub_resources[].actions as the canonical source of timed device actions.
Identifies conflicting instructions and enforces explicit priority/rejection policies.
"""

from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
import json
import logging
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
import zoneinfo

from pydantic import BaseModel, Field

from config import HUB_ID, SCHEDULE_TIMEZONE

logger = logging.getLogger("iothub.schedule_normalizer")


class ConflictPolicy(str, Enum):
    """Policy for handling conflicting actions on the same device channel at the same time."""
    REJECT = "REJECT"                    # Raise ScheduleConflictError immediately
    PRIORITY_SAFETY = "PRIORITY_SAFETY"  # Safety action (OFF, STOP) takes precedence over (ON, START)
    HIGHER_STAGE = "HIGHER_STAGE"        # Action with higher stage_id wins
    LATEST_ORDER = "LATEST_ORDER"        # Action with higher production_order_id wins
    KEEP_FIRST = "KEEP_FIRST"            # Keep the first encountered action
    KEEP_LAST = "KEEP_LAST"              # Keep the last encountered action


class ScheduleConflictError(Exception):
    """Raised when conflicting scheduled instructions are detected under REJECT policy."""
    pass


class NormalizedActionRecord(BaseModel):
    """
    Flat normalized record representing a single executable device action.
    Ready for immediate execution by the scheduler without traversing nested JSON.
    """
    schedule_id: str = Field(..., description="Schedule identifier")
    action_id: str = Field(..., description="Unique action identifier")
    production_order_id: Optional[Any] = Field(default=None, description="Owning production order identifier")
    resource_id: Optional[Any] = Field(default=None, description="Owning resource identifier")
    sub_resource_id: Optional[Any] = Field(default=None, description="Owning sub-resource identifier")
    iot_node_id: str = Field(..., description="Target device/node identifier")
    stage_id: Optional[Any] = Field(default=None, description="Production stage identifier")
    scheduled_at: str = Field(..., description="Full ISO 8601 execution timestamp (e.g. 2026-10-09T09:56:00+05:30)")
    action: str = Field(..., description="Device operation (e.g. LIGHTS_ON, LIGHTS_OFF)")
    channel_key: str = Field(..., description="Device channel/relay (e.g. relay:light)")
    device_id: Optional[str] = Field(default=None, description="Assigned device (e.g. light_glow_1)")
    device_type: Optional[str] = Field(default=None, description="Assigned device type (e.g. light_glow)")
    device_number: Optional[int] = Field(default=None, description="Assigned device number (e.g. 1)")
    device_name: Optional[str] = Field(default=None, description="Assigned device display name")
    params: Optional[Dict[str, Any]] = Field(default=None, description="Action parameters if any")
    status: str = Field(default="PENDING", description="Execution status")

    def to_dict(self) -> Dict[str, Any]:
        return self.model_dump()

    def __getitem__(self, item: str) -> Any:
        return getattr(self, item)

    def __contains__(self, item: str) -> bool:
        return hasattr(self, item)

    def get(self, item: str, default: Any = None) -> Any:
        return getattr(self, item, default)


@dataclass
class ConflictDetail:
    iot_node_id: str
    channel_key: str
    scheduled_at: str
    conflicting_actions: List[NormalizedActionRecord]
    reason: str


def format_iso_scheduled_at(date_str: str, time_str: str, tz_name_or_offset: str) -> str:
    """
    Combines date ('YYYY-MM-DD'), time ('HH:MM' or 'HH:MM:SS'), and timezone into an ISO 8601 string.
    If time_str is already a complete ISO timestamp, returns it directly.
    """
    time_clean = time_str.strip()
    if "T" in time_clean:
        return time_clean

    # Parse time components
    parts = time_clean.split(":")
    hour = int(parts[0])
    minute = int(parts[1]) if len(parts) > 1 else 0
    second = int(parts[2]) if len(parts) > 2 else 0

    # Parse calendar date
    d = datetime.strptime(date_str.strip(), "%Y-%m-%d").date()

    # Determine timezone info
    tz_clean = tz_name_or_offset.strip()
    if tz_clean.startswith("+") or tz_clean.startswith("-"):
        hours_offset = int(tz_clean[1:3])
        mins_offset = int(tz_clean[4:6]) if len(tz_clean) >= 6 else int(tz_clean[3:5])
        sign = 1 if tz_clean[0] == "+" else -1
        delta = timedelta(hours=hours_offset, minutes=mins_offset) * sign
        tz = timezone(delta)
    else:
        try:
            tz = zoneinfo.ZoneInfo(tz_clean)
        except Exception:
            tz = timezone.utc

    dt = datetime(d.year, d.month, d.day, hour, minute, second, tzinfo=tz)
    return dt.isoformat()


class ScheduleNormalizer:
    """
    Extracts, normalizes, de-duplicates, and resolves conflicts for timed device actions.
    Uses sub_resources[].actions as the canonical source.
    """

    def __init__(
        self,
        default_hub_id: str = HUB_ID,
        default_timezone: str = SCHEDULE_TIMEZONE,
        conflict_policy: ConflictPolicy = ConflictPolicy.REJECT,
    ):
        self.default_hub_id = default_hub_id
        self.default_timezone = default_timezone
        self.conflict_policy = conflict_policy

    def normalize(
        self,
        payload: str | bytes | dict,
        conflict_policy: Optional[ConflictPolicy] = None,
    ) -> List[NormalizedActionRecord]:
        """
        Main entry point: parses raw payload, extracts all sub_resources[].actions,
        normalizes each action into a NormalizedActionRecord, detects/resolves conflicts,
        and returns a flat list of unique executable actions.
        """
        policy = conflict_policy or self.conflict_policy

        if isinstance(payload, (bytes, bytearray)):
            payload = payload.decode("utf-8")
        if isinstance(payload, str):
            payload = json.loads(payload)
        if not isinstance(payload, dict):
            return []

        # Extract top-level metadata
        schedule_tz = payload.get("timezone") or self.default_timezone
        try:
            today_local = datetime.now(zoneinfo.ZoneInfo(schedule_tz)).strftime("%Y-%m-%d")
        except Exception:
            today_local = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        schedule_date = payload.get("date") or today_local
        hub_id = payload.get("hub_id") or self.default_hub_id
        schedule_id = payload.get("schedule_id") or f"{hub_id}-{schedule_date}"

        # Extract raw actions from sub_resources[].actions
        raw_records = self._extract_sub_resource_actions(
            payload=payload,
            schedule_id=schedule_id,
            schedule_date=schedule_date,
            schedule_tz=schedule_tz,
        )

        if not raw_records:
            return []

        # Resolve conflicts and deduplicate
        return self._resolve_conflicts_and_deduplicate(raw_records, policy)

    def _extract_sub_resource_actions(
        self,
        payload: Dict[str, Any],
        schedule_id: str,
        schedule_date: str,
        schedule_tz: str,
    ) -> List[NormalizedActionRecord]:
        """Traverses production orders, resources, and sub-resources to collect actions."""
        records: List[NormalizedActionRecord] = []
        action_counter = 0

        # Helper to process a single sub-resource dictionary
        def process_sub_resource(
            sub_res: Dict[str, Any],
            po_id: Optional[Any],
            res_id: Optional[Any],
            parent_stage_id: Optional[Any],
        ):
            nonlocal action_counter
            sub_id = sub_res.get("sub_resource_id") or sub_res.get("id")
            sub_stage_id = sub_res.get("stage_id", parent_stage_id)
            sub_node_id = sub_res.get("iot_node_id") or sub_res.get("node_id") or sub_res.get("device_id")
            sub_channel = sub_res.get("channel_key")

            actions = sub_res.get("actions", [])
            for act in actions:
                action_counter += 1
                # Stable action identifier or generated default
                act_id = (
                    act.get("action_id")
                    or act.get("id")
                    or act.get("command_id")
                    or f"action-{action_counter:03d}"
                )
                raw_params = act.get("params") or act.get("parameters") or {}
                if not isinstance(raw_params, dict):
                    raw_params = {}
                dev_id = act.get("device_id") or raw_params.get("device") or raw_params.get("device_id")
                dev_type = act.get("device_type") or raw_params.get("device_type")
                dev_num = act.get("device_number") if act.get("device_number") is not None else raw_params.get("device_number")
                dev_name = act.get("device_name") or raw_params.get("device_name")
                if not dev_id and dev_type and dev_num is not None:
                    dev_id = f"{dev_type}_{dev_num}"
                act_params = {
                    k: v for k, v in raw_params.items()
                    if k not in ("device", "device_type", "device_number", "device_id", "device_name")
                }
                node_id = (
                    dev_id
                    or act.get("iot_node_id")
                    or sub_node_id
                    or "unknown"
                )
                channel_key = act.get("channel_key") or sub_channel or "default"
                action_name = act.get("action") or act.get("command") or "NOOP"
                stage_id = act.get("stage_id", sub_stage_id)

                # Format timestamp
                time_val = act.get("scheduled_at") or act.get("time") or act.get("scheduled_time") or "00:00:00"
                iso_timestamp = format_iso_scheduled_at(schedule_date, str(time_val), schedule_tz)

                record = NormalizedActionRecord(
                    schedule_id=schedule_id,
                    action_id=str(act_id),
                    production_order_id=po_id,
                    resource_id=res_id,
                    sub_resource_id=sub_id,
                    iot_node_id=str(node_id),
                    stage_id=stage_id,
                    scheduled_at=iso_timestamp,
                    action=str(action_name),
                    channel_key=str(channel_key),
                    device_id=str(dev_id) if dev_id is not None else None,
                    device_type=str(dev_type) if dev_type is not None else None,
                    device_number=int(dev_num) if dev_num is not None and str(dev_num).lstrip('-').isdigit() else None,
                    device_name=str(dev_name) if dev_name is not None else None,
                    params=act_params if act_params else None,
                    status="PENDING",
                )
                records.append(record)

        # 1. Traverse production_orders -> resources -> sub_resources
        for po in payload.get("production_orders", []):
            po_id = po.get("production_order_id") or po.get("id") or po.get("order_id")
            po_stage_id = po.get("stage_id")
            for res in po.get("resources", []):
                res_id = res.get("resource_id") or res.get("id")
                res_stage_id = res.get("stage_id", po_stage_id)
                for sub in res.get("sub_resources", []):
                    process_sub_resource(sub, po_id, res_id, res_stage_id)

        # 2. Traverse top-level resources -> sub_resources (if no production_orders wrapper)
        for res in payload.get("resources", []):
            res_id = res.get("resource_id") or res.get("id")
            res_stage_id = res.get("stage_id")
            for sub in res.get("sub_resources", []):
                process_sub_resource(sub, None, res_id, res_stage_id)

        # 3. Traverse top-level sub_resources directly
        for sub in payload.get("sub_resources", []):
            process_sub_resource(sub, None, None, None)

        # 4. Traverse schedules array if it contains sub_resources or actions
        for sched_entry in payload.get("schedules", []):
            po_info = sched_entry.get("production_order")
            po_id = sched_entry.get("production_order_id")
            if not po_id and isinstance(po_info, dict):
                po_id = po_info.get("id") or po_info.get("production_order_id")

            res_info = sched_entry.get("resource")
            res_id = sched_entry.get("resource_id")
            if not res_id and isinstance(res_info, dict):
                res_id = res_info.get("id") or res_info.get("resource_id")

            if "sub_resources" in sched_entry:
                for sub in sched_entry.get("sub_resources", []):
                    process_sub_resource(
                        sub,
                        po_id,
                        res_id,
                        sched_entry.get("stage_id"),
                    )
            elif "actions" in sched_entry:
                process_sub_resource(
                    sched_entry,
                    po_id,
                    res_id,
                    sched_entry.get("stage_id"),
                )
            elif "action" in sched_entry and ("time" in sched_entry or "scheduled_at" in sched_entry):
                # Flat schedule entry
                action_counter += 1
                act_id = sched_entry.get("action_id") or f"action-{action_counter:03d}"
                time_val = sched_entry.get("scheduled_at") or sched_entry.get("time") or "00:00:00"
                iso_timestamp = format_iso_scheduled_at(schedule_date, str(time_val), schedule_tz)
                raw_params = sched_entry.get("params") or sched_entry.get("parameters") or {}
                if not isinstance(raw_params, dict):
                    raw_params = {}
                dev_id = sched_entry.get("device_id") or raw_params.get("device") or raw_params.get("device_id")
                dev_type = sched_entry.get("device_type") or raw_params.get("device_type")
                dev_num = sched_entry.get("device_number") if sched_entry.get("device_number") is not None else raw_params.get("device_number")
                dev_name = sched_entry.get("device_name") or raw_params.get("device_name")
                if not dev_id and dev_type and dev_num is not None:
                    dev_id = f"{dev_type}_{dev_num}"
                act_params = {
                    k: v for k, v in raw_params.items()
                    if k not in ("device", "device_type", "device_number", "device_id", "device_name")
                }
                flat_node = (
                    dev_id
                    or sched_entry.get("iot_node_id")
                    or "unknown"
                )
                records.append(
                    NormalizedActionRecord(
                        schedule_id=schedule_id,
                        action_id=str(act_id),
                        production_order_id=sched_entry.get("production_order_id"),
                        resource_id=sched_entry.get("resource_id"),
                        sub_resource_id=sched_entry.get("sub_resource_id") or sched_entry.get("sub_resource"),
                        iot_node_id=str(flat_node),
                        stage_id=sched_entry.get("stage_id"),
                        scheduled_at=iso_timestamp,
                        action=str(sched_entry.get("action")),
                        channel_key=str(sched_entry.get("channel_key", "default")),
                        device_id=str(dev_id) if dev_id is not None else None,
                        device_type=str(dev_type) if dev_type is not None else None,
                        device_number=int(dev_num) if dev_num is not None and str(dev_num).lstrip('-').isdigit() else None,
                        device_name=str(dev_name) if dev_name is not None else None,
                        params=act_params if act_params else None,
                        status="PENDING",
                    )
                )

        # 5. If no records extracted yet, check production_list inside schedules
        if not records:
            for sched_entry in payload.get("schedules", []):
                po_info = sched_entry.get("production_order") or {}
                po_id = po_info.get("id") or sched_entry.get("production_order_id")
                res_info = sched_entry.get("resource") or {}
                res_id = res_info.get("id") or sched_entry.get("resource_id")
                sub_res_map = {s.get("id"): s for s in sched_entry.get("sub_resources", []) if isinstance(s, dict)}
                for prod_item in sched_entry.get("production_list", []):
                    stage_id = prod_item.get("id") or prod_item.get("stage_id")
                    time_val = prod_item.get("time") or "00:00:00"
                    iso_timestamp = format_iso_scheduled_at(schedule_date, str(time_val), schedule_tz)
                    for act in prod_item.get("sub_resource_actions", []):
                        action_counter += 1
                        act_id = act.get("action_id") or act.get("id") or f"action-{action_counter:03d}"
                        sub_id = act.get("sub_resource")
                        sub_info = sub_res_map.get(sub_id, {})
                        sub_node_id = sub_info.get("iot_node_id") or "unknown"
                        raw_params = act.get("params") or act.get("parameters") or {}
                        if not isinstance(raw_params, dict):
                            raw_params = {}
                        dev_id = act.get("device_id") or raw_params.get("device") or raw_params.get("device_id")
                        dev_type = act.get("device_type") or raw_params.get("device_type")
                        dev_num = act.get("device_number") if act.get("device_number") is not None else raw_params.get("device_number")
                        dev_name = act.get("device_name") or raw_params.get("device_name")
                        if not dev_id and dev_type and dev_num is not None:
                            dev_id = f"{dev_type}_{dev_num}"
                        act_params = {
                            k: v for k, v in raw_params.items()
                            if k not in ("device", "device_type", "device_number", "device_id", "device_name")
                        }
                        node_id = dev_id or act.get("iot_node_id") or sub_node_id or "unknown"
                        records.append(
                            NormalizedActionRecord(
                                schedule_id=schedule_id,
                                action_id=str(act_id),
                                production_order_id=po_id,
                                resource_id=res_id,
                                sub_resource_id=sub_id,
                                iot_node_id=str(node_id),
                                stage_id=stage_id,
                                scheduled_at=iso_timestamp,
                                action=str(act.get("action") or "NOOP"),
                                channel_key=str(act.get("channel_key") or "default"),
                                device_id=str(dev_id) if dev_id is not None else None,
                                device_type=str(dev_type) if dev_type is not None else None,
                                device_number=int(dev_num) if dev_num is not None and str(dev_num).lstrip('-').isdigit() else None,
                                device_name=str(dev_name) if dev_name is not None else None,
                                params=act_params if act_params else None,
                                status="PENDING",
                            )
                        )

        return records

    def _resolve_conflicts_and_deduplicate(
        self,
        records: List[NormalizedActionRecord],
        policy: ConflictPolicy,
    ) -> List[NormalizedActionRecord]:
        """
        Groups actions targeting the exact same device, channel, and second.
        Deduplicates identical actions and resolves or rejects conflicting instructions.
        """
        # Group actions by (iot_node_id, channel_key, scheduled_at)
        slot_groups: Dict[Tuple[str, str, str], List[NormalizedActionRecord]] = {}
        for r in records:
            key = (r.iot_node_id, r.channel_key, r.scheduled_at)
            slot_groups.setdefault(key, []).append(r)

        result_actions: List[NormalizedActionRecord] = []
        conflicts_detected: List[ConflictDetail] = []

        for (node_id, channel, sched_at), group in slot_groups.items():
            if len(group) == 1:
                result_actions.append(group[0])
                continue

            # Check if all actions in group are identical (duplicates)
            first_action = group[0].action
            first_params = (group[0].device_id, group[0].device_type, group[0].device_number)
            is_identical = all(
                g.action == first_action and (g.device_id, g.device_type, g.device_number) == first_params
                for g in group
            )

            if is_identical:
                # Deduplicate: keep one unique action
                result_actions.append(group[0])
                continue

            # Conflicting actions detected on the same relay at the same time
            conflict_desc = (
                f"Conflicting actions at '{sched_at}' on device '{node_id}' channel '{channel}': "
                + ", ".join([f"'{g.action}' (order: {g.production_order_id})" for g in group])
            )
            conflicts_detected.append(
                ConflictDetail(
                    iot_node_id=node_id,
                    channel_key=channel,
                    scheduled_at=sched_at,
                    conflicting_actions=group,
                    reason=conflict_desc,
                )
            )

            if policy == ConflictPolicy.REJECT:
                logger.error("Schedule conflict: %s", conflict_desc)
                raise ScheduleConflictError(conflict_desc)

            elif policy == ConflictPolicy.PRIORITY_SAFETY:
                # Safety/OFF/STOP operations take precedence over ON/START operations
                safety_keywords = {"LIGHTS_OFF", "STOP", "STOP_MACHINE", "OFF", "SHUTDOWN", "CLOSE"}
                safety_matches = [g for g in group if g.action.upper() in safety_keywords]
                if safety_matches:
                    chosen = safety_matches[0]
                else:
                    chosen = group[0]
                logger.warning("Resolved conflict using PRIORITY_SAFETY: selected '%s'", chosen.action)
                result_actions.append(chosen)

            elif policy == ConflictPolicy.HIGHER_STAGE:
                # Max stage_id wins
                chosen = max(group, key=lambda g: int(g.stage_id) if g.stage_id is not None else -1)
                result_actions.append(chosen)

            elif policy == ConflictPolicy.LATEST_ORDER:
                # Max production_order_id wins
                chosen = max(group, key=lambda g: int(g.production_order_id) if isinstance(g.production_order_id, (int, str)) and str(g.production_order_id).isdigit() else -1)
                result_actions.append(chosen)

            elif policy == ConflictPolicy.KEEP_FIRST:
                result_actions.append(group[0])

            elif policy == ConflictPolicy.KEEP_LAST:
                result_actions.append(group[-1])

        # Sort chronological
        result_actions.sort(key=lambda r: (r.scheduled_at, r.iot_node_id, r.channel_key))
        return result_actions


def normalize_schedule(
    payload: str | bytes | dict,
    conflict_policy: ConflictPolicy = ConflictPolicy.REJECT,
) -> List[NormalizedActionRecord]:
    """Convenience helper to normalize a schedule into flat executable action records."""
    normalizer = ScheduleNormalizer(conflict_policy=conflict_policy)
    return normalizer.normalize(payload)
