import json
import logging
import uuid
from typing import Any, Dict
from config import HUB_ID

logger = logging.getLogger("iothub.normalizer")


def normalize_payload(raw_data: str | bytes | dict, default_hub_id: str = HUB_ID) -> dict:
    """
    Normalizes incoming message payloads into the canonical IoTMessage structure.
    Supports:
    1. Canonical format with 'commands' list.
    2. Nested command format (e.g. {'command': {...}}).
    3. Flat command format (e.g. {'command_id': ..., 'device_id': ..., 'actions': 'LIGHT_ON', ...}).
       Auto-fills missing 'hub_id', 'message_id', 'schema_version' if absent.
    """
    if isinstance(raw_data, (bytes, bytearray)):
        raw_data = raw_data.decode("utf-8")

    if isinstance(raw_data, str):
        try:
            data = json.loads(raw_data)
        except json.JSONDecodeError:
            return raw_data
    elif isinstance(raw_data, dict):
        data = raw_data
    else:
        return raw_data

    if not isinstance(data, dict):
        return data

    # If it's already canonical format (has 'commands' key), return as is
    if "commands" in data:
        return data

    # Check if command is nested under "command" key (e.g. {"command": {...}})
    target = data.get("command") if isinstance(data.get("command"), dict) else data

    # Check if it matches command structure (supports "action" or "actions")
    cmd_id = target.get("command_id") or data.get("command_id") or str(uuid.uuid4())
    dev_id = target.get("device_id") or data.get("device_id") or target.get("device") or target.get("target_device")
    action_val = target.get("action") or target.get("actions") or data.get("action") or data.get("actions")

    if dev_id and action_val:
        # If action is a list (e.g. ["LIGHT_ON"]), take first action string
        if isinstance(action_val, list) and action_val:
            action_str = str(action_val[0])
        else:
            action_str = str(action_val)

        # Extract parameters or build them from known fields
        params: Dict[str, Any] = {}
        if "parameters" in target and isinstance(target["parameters"], dict):
            params = dict(target["parameters"])
        elif "params" in target and isinstance(target["params"], dict):
            params = dict(target["params"])
        elif "parameters" in data and isinstance(data["parameters"], dict):
            params = dict(data["parameters"])
        elif "params" in data and isinstance(data["params"], dict):
            params = dict(data["params"])
        elif "state" in target:
            params = {"state": target["state"]}

        # Attach additional metadata fields so downstream execution and reporting can use them
        for field in ("channel_key", "device_type", "device_number", "device_name"):
            val = target.get(field) or data.get(field)
            if val is not None and field not in params:
                params[field] = val

        return {
            "schema_version": int(data.get("schema_version") or target.get("schema_version") or 1),
            "message_id": str(data.get("message_id") or target.get("message_id") or f"msg_{cmd_id}"),
            "hub_id": str(data.get("hub_id") or target.get("hub_id") or default_hub_id),
            "commands": [
                {
                    "command_id": str(cmd_id),
                    "device_id": str(dev_id),
                    "action": action_str,
                    "parameters": params,
                }
            ],
        }

    return data
