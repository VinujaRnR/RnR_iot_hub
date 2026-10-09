import json
import logging
from typing import Any, Dict, List
from pydantic import BaseModel, Field, field_validator, ValidationError

logger = logging.getLogger("iothub.validator")


class CommandItem(BaseModel):
    command_id: str = Field(..., description="Unique command ID (e.g. UUID)")
    device_id: str = Field(..., description="Target device identifier")
    action: str = Field(..., description="Action to execute on target device")
    parameters: Dict[str, Any] = Field(default_factory=dict, description="Parameters for action")


class IoTMessage(BaseModel):
    schema_version: int = Field(1, description="Schema version of message protocol")
    message_id: str = Field(..., description="Unique message ID")
    hub_id: str = Field(..., description="Identifier of destination hub")
    commands: List[CommandItem] = Field(..., min_length=1, max_length=50, description="List of 1-50 commands")

    @field_validator("schema_version", mode="before")
    @classmethod
    def parse_schema_version(cls, v: Any) -> int:
        try:
            val = int(v)
            if val != 1:
                raise ValueError("Unsupported schema version")
            return val
        except (TypeError, ValueError):
            raise ValueError(f"Unsupported schema version: {v}")


class MessageValidationError(Exception):
    """Raised when message validation fails."""
    pass


def validate_incoming_message(raw_data: str | bytes | dict, expected_hub_id: str | None = None) -> IoTMessage:
    """
    Validates raw payload (JSON string, bytes, or dict) using Pydantic.
    Optionally verifies hub_id match.
    """
    if isinstance(raw_data, (bytes, bytearray)):
        raw_data = raw_data.decode("utf-8")

    if isinstance(raw_data, str):
        try:
            payload_dict = json.loads(raw_data)
        except json.JSONDecodeError as exc:
            raise MessageValidationError(f"Malformed JSON: {exc}") from exc
    elif isinstance(raw_data, dict):
        payload_dict = raw_data
    else:
        raise MessageValidationError(f"Invalid payload type: {type(raw_data).__name__}")

    if not isinstance(payload_dict, dict):
        raise MessageValidationError("Message payload must be a JSON object")

    try:
        model = IoTMessage.model_validate(payload_dict)
    except ValidationError as exc:
        # Extract clean error message
        errors = [f"{err['loc']}: {err['msg']}" for err in exc.errors()]
        raise MessageValidationError(f"Validation error: {'; '.join(errors)}") from exc

    if expected_hub_id and model.hub_id != expected_hub_id:
        raise MessageValidationError(f"Incorrect hub_id: expected '{expected_hub_id}', got '{model.hub_id}'")

    return model
