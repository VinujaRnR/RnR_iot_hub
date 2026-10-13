import json
import logging
from typing import Any, Dict
from config import HUB_ID

logger = logging.getLogger("iothub.normalizer")


def normalize_payload(raw_data: str | bytes | dict, default_hub_id: str = HUB_ID) -> dict:
    """
    Normalizes incoming message payloads into the canonical IoTMessage structure.
    Supports:
    1. Canonical format with 'commands' list.
    2. Flat/legacy single-command format (e.g., {'command_id': ..., 'device_id': ..., 'action': ...}).
    """
    if isinstance(raw_data, (bytes, bytearray)):
        raw_data = raw_data.decode("utf-8")

    if isinstance(raw_data, str):
        try:
            data = json.loads(raw_data)
        except json.JSONDecodeError:
            # Leave invalid JSON as-is to let the validator raise appropriate error
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

    # Check if it matches a flat command structure
    if "command_id" in data and "device_id" in data and "action" in data:
        # Extract parameters or build them from known fields
        params: Dict[str, Any] = {}
        if "parameters" in data and isinstance(data["parameters"], dict):
            params = data["parameters"]
        elif "params" in data and isinstance(data["params"], dict):
            params = data["params"]
        elif "state" in data:
            params = {"state": data["state"]}

        return {
            "schema_version": data.get("schema_version", 1),
            "message_id": data.get("message_id", f"msg_{data['command_id']}"),
            "hub_id": data.get("hub_id", default_hub_id),
            "commands": [
                {
                    "command_id": data["command_id"],
                    "device_id": data["device_id"],
                    "action": data["action"],
                    "parameters": params,
                }
            ],
        }

    return data
