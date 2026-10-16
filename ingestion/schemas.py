"""
Pydantic Schemas for IoT Hub Daily Production Schedules.
Defines hierarchy: DailyProductionSchedule -> Production Orders -> Resources -> Sub-resources -> Actions
as well as the top-level schedules array.
"""

from __future__ import annotations
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
import zoneinfo

from pydantic import BaseModel, Field, field_validator, model_validator

TIME_HH_MM_REGEX = re.compile(r"^(?:[01]\d|2[0-3]):[0-5]\d(?::[0-5]\d)?$")
DATE_YYYY_MM_DD_REGEX = re.compile(r"^\d{4}-(?:0[1-9]|1[0-2])-(?:0[1-9]|[12]\d|3[01])$")


class ScheduledAction(BaseModel):
    """Represents an individual scheduled operation targeting a hardware node/device."""

    model_config = {"extra": "ignore"}

    time: str = Field(..., description="Execution time in 24-hour HH:MM format (e.g. '08:30')")
    action: str = Field(..., min_length=1, description="Known, authorized operation to execute")
    channel_key: str = Field(..., min_length=1, description="Target device channel key (e.g. 'ch1', 'relay_1')")
    params: Optional[Dict[str, Any]] = Field(default_factory=dict, description="Action parameter dictionary")
    sub_resource: str = Field(..., min_length=1, description="Identifier of owning sub-resource")
    iot_node_id: str = Field(..., min_length=1, description="Configured target IoT device/node identifier")
    action_id: Optional[str] = Field(default=None, description="Optional action identifier")

    @field_validator("params", mode="before")
    @classmethod
    def coerce_params(cls, v: Any) -> Dict[str, Any]:
        if v is None:
            return {}
        if not isinstance(v, dict):
            raise ValueError(f"Parameters must be a dictionary or null, got {type(v).__name__}")
        return v

    @field_validator("time")
    @classmethod
    def validate_time_format(cls, v: str) -> str:
        if not isinstance(v, str) or not TIME_HH_MM_REGEX.match(v.strip()):
            raise ValueError(f"Invalid time format '{v}'. Must be a valid 24-hour 'HH:MM' string (00:00 to 23:59)")
        parts = v.strip().split(":")
        return f"{parts[0]}:{parts[1]}"

    @field_validator("action", "channel_key", "sub_resource", "iot_node_id")
    @classmethod
    def validate_non_empty_strings(cls, v: str, info) -> str:
        if not isinstance(v, str) or not v.strip():
            raise ValueError(f"Field '{info.field_name}' must be a non-empty string")
        return v.strip()


class SubResource(BaseModel):
    """A sub-resource unit (e.g., machine component, line section) containing actions."""

    model_config = {"extra": "ignore"}

    sub_resource_id: str = Field(..., min_length=1, description="Unique sub-resource identifier")
    actions: List[ScheduledAction] = Field(default_factory=list, description="Scheduled actions for this sub-resource")

    @field_validator("sub_resource_id", mode="before")
    @classmethod
    def coerce_sub_resource_id(cls, v: Any) -> str:
        if v is None:
            raise ValueError("sub_resource_id cannot be None")
        return str(v).strip()

    @model_validator(mode="before")
    @classmethod
    def propagate_sub_resource_id_to_actions(cls, data: Any) -> Any:
        if isinstance(data, dict):
            sub_id = str(data.get("sub_resource_id") or data.get("id") or "").strip()
            if sub_id:
                data.setdefault("sub_resource_id", sub_id)
            node_id = str(data.get("iot_node_id") or data.get("device_id") or "").strip()
            actions = data.get("actions", [])
            if isinstance(actions, list):
                for act in actions:
                    if isinstance(act, dict):
                        if sub_id and "sub_resource" not in act:
                            act["sub_resource"] = sub_id
                        if node_id and "iot_node_id" not in act:
                            act["iot_node_id"] = node_id
        return data

    @model_validator(mode="after")
    def validate_actions_sub_resource_match(self) -> SubResource:
        """Enforces rule: 'sub_resource: Matches the owning sub-resource'."""
        for idx, act in enumerate(self.actions):
            if act.sub_resource != self.sub_resource_id:
                raise ValueError(
                    f"Action #{idx} sub_resource mismatch: action specifies '{act.sub_resource}' "
                    f"but owning sub_resource is '{self.sub_resource_id}'"
                )
        return self


class Resource(BaseModel):
    """A resource unit (e.g., assembly line, machine) containing sub-resources."""

    model_config = {"extra": "ignore"}

    resource_id: str = Field(..., min_length=1, description="Unique resource identifier")
    sub_resources: List[SubResource] = Field(default_factory=list, description="List of owning sub-resources")

    @field_validator("resource_id", mode="before")
    @classmethod
    def coerce_resource_id(cls, v: Any) -> str:
        if v is None:
            raise ValueError("resource_id cannot be None")
        return str(v).strip()


class ProductionOrder(BaseModel):
    """A production order grouping scheduled resources."""

    model_config = {"extra": "ignore"}

    production_order_id: str = Field(..., min_length=1, description="Production order identifier")
    resources: List[Resource] = Field(default_factory=list, description="Assigned resources")

    @field_validator("production_order_id", mode="before")
    @classmethod
    def coerce_production_order_id(cls, v: Any) -> str:
        if v is None:
            raise ValueError("production_order_id cannot be None")
        return str(v).strip()


class ScheduleEntry(BaseModel):
    """An entry in the top-level schedules array representing a scheduled action."""

    model_config = {"extra": "ignore"}

    time: str = Field(..., description="Execution time in 24-hour HH:MM format")
    action: str = Field(..., min_length=1, description="Operation to execute")
    channel_key: str = Field(..., min_length=1, description="Device channel key")
    params: Optional[Dict[str, Any]] = Field(default_factory=dict, description="Action parameters")
    sub_resource: str = Field(..., min_length=1, description="Owning sub-resource identifier")
    iot_node_id: str = Field(..., min_length=1, description="Target IoT node identifier")

    production_order_id: Optional[str] = Field(default=None, description="Optional production order identifier")
    resource_id: Optional[str] = Field(default=None, description="Optional resource identifier")

    @field_validator("params", mode="before")
    @classmethod
    def coerce_params(cls, v: Any) -> Dict[str, Any]:
        if v is None:
            return {}
        if not isinstance(v, dict):
            raise ValueError(f"Parameters must be a dictionary or null, got {type(v).__name__}")
        return v

    @field_validator("time")
    @classmethod
    def validate_time_format(cls, v: str) -> str:
        if not isinstance(v, str) or not TIME_HH_MM_REGEX.match(v.strip()):
            raise ValueError(f"Invalid time format '{v}'. Must be a valid 24-hour 'HH:MM' string (00:00 to 23:59)")
        parts = v.strip().split(":")
        return f"{parts[0]}:{parts[1]}"

    @field_validator("production_order_id", "resource_id", mode="before")
    @classmethod
    def coerce_optional_ids(cls, v: Any) -> Optional[str]:
        if v is None:
            return None
        return str(v).strip()

    def to_scheduled_action(self) -> ScheduledAction:
        return ScheduledAction(
            time=self.time,
            action=self.action,
            channel_key=self.channel_key,
            params=self.params,
            sub_resource=self.sub_resource,
            iot_node_id=self.iot_node_id,
        )


class DailyProductionSchedule(BaseModel):
    """Top-level daily production schedule received from backend system."""

    schema_version: int = Field(default=1, ge=1, description="Protocol schema version")
    schedule_id: str = Field(..., min_length=1, description="Unique identifier for this schedule")
    hub_id: str = Field(..., min_length=1, description="Target Linux IoT Hub identifier")
    revision: int = Field(default=1, ge=0, description="Schedule revision number (prevents duplicate application)")
    date: str = Field(..., description="Target schedule date in YYYY-MM-DD format")
    timezone: str = Field(..., description="IANA timezone name (e.g. 'Asia/Kolkata')")

    schedules: List[ScheduleEntry] = Field(default_factory=list, description="Top-level schedule actions")
    production_orders: List[ProductionOrder] = Field(default_factory=list, description="Hierarchical production orders")

    @model_validator(mode="before")
    @classmethod
    def normalize_incoming_schedule_payload(cls, data: Any) -> Any:
        if isinstance(data, dict):
            po_data = data.get("production_order")
            # 1. Default timezone if missing
            data.setdefault("timezone", data.get("timezone") or "Asia/Kolkata")

            # 2. Extract or default date
            # If payload explicitly provides a date, use it; otherwise default to today's local date
            # rather than the order's historical start_date (which could be weeks in the past).
            tz_str = data.get("timezone") or "Asia/Kolkata"
            try:
                today_local = datetime.now(zoneinfo.ZoneInfo(tz_str)).strftime("%Y-%m-%d")
            except Exception:
                today_local = datetime.now(timezone.utc).strftime("%Y-%m-%d")

            date = data.get("date") or today_local
            data.setdefault("date", date)

            # 3. Default hub_id and schedule_id
            hub_id = data.get("hub_id") or "hub-01"
            data.setdefault("hub_id", hub_id)
            data.setdefault("schedule_id", f"{hub_id}-{date}")
            data.setdefault("revision", 1)

            # 4. Check if root contains production_order / sub_resources without production_orders wrapper
            if "production_orders" not in data:
                if "sub_resources" in data or "production_order" in data or "resource" in data:
                    po_id = po_data.get("id") if isinstance(po_data, dict) else data.get("production_order_id", "PO-1")
                    res_info = data.get("resource") or {}
                    res_id = res_info.get("id") if isinstance(res_info, dict) else data.get("resource_id", "RES-1")
                    sub_res_list = data.get("sub_resources", [])
                    data["production_orders"] = [
                        {
                            "production_order_id": str(po_id or "PO-1"),
                            "resources": [
                                {
                                    "resource_id": str(res_id or "RES-1"),
                                    "sub_resources": sub_res_list,
                                }
                            ],
                        }
                    ]

            # 5. Check if 'schedules' contains nested production order / sub_resource blocks
            schedules = data.get("schedules")
            if isinstance(schedules, list):
                converted_orders = []
                regular_entries = []
                for entry in schedules:
                    if isinstance(entry, dict) and ("sub_resources" in entry or "production_order" in entry or "resource" in entry):
                        po_info = entry.get("production_order") or {}
                        po_id = po_info.get("id") if isinstance(po_info, dict) else entry.get("production_order_id", "PO-1")
                        res_info = entry.get("resource") or {}
                        res_id = res_info.get("id") if isinstance(res_info, dict) else entry.get("resource_id", "RES-1")
                        sub_res_list = entry.get("sub_resources", [])

                        converted_orders.append({
                            "production_order_id": str(po_id),
                            "resources": [
                                {
                                    "resource_id": str(res_id),
                                    "sub_resources": sub_res_list,
                                }
                            ]
                        })
                    else:
                        regular_entries.append(entry)

                if converted_orders:
                    data.setdefault("production_orders", []).extend(converted_orders)
                    data["schedules"] = regular_entries
        return data

    @field_validator("date")
    @classmethod
    def validate_date_format(cls, v: str) -> str:
        v_clean = v.strip()
        if not DATE_YYYY_MM_DD_REGEX.match(v_clean):
            raise ValueError(f"Invalid date '{v}'. Must be in 'YYYY-MM-DD' format")
        try:
            datetime.strptime(v_clean, "%Y-%m-%d")
        except ValueError as exc:
            raise ValueError(f"Invalid calendar date '{v}': {exc}")
        return v_clean

    @field_validator("timezone")
    @classmethod
    def validate_timezone(cls, v: str) -> str:
        v_clean = v.strip()
        try:
            zoneinfo.ZoneInfo(v_clean)
        except Exception as exc:
            raise ValueError(f"Invalid timezone identifier '{v}': {exc}")
        return v_clean

    @model_validator(mode="after")
    def validate_contains_actions(self) -> DailyProductionSchedule:
        actions = self.extract_all_actions()
        if not actions:
            raise ValueError("Daily schedule must contain at least one scheduled action (in 'schedules' or 'production_orders')")
        return self

    def extract_all_actions(self) -> List[ScheduledAction]:
        """Extracts and normalizes all scheduled actions from both schedules and production_orders."""
        all_actions: List[ScheduledAction] = []

        # From schedules array
        for entry in self.schedules:
            all_actions.append(entry.to_scheduled_action())

        # From nested production orders
        for po in self.production_orders:
            for res in po.resources:
                for sub in res.sub_resources:
                    for act in sub.actions:
                        all_actions.append(act)

        return all_actions
