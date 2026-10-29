import json
import logging
import uuid
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field, field_validator, ValidationError
from config import HUB_ID

logger = logging.getLogger("iothub.validator")


class CommandItem(BaseModel):
    command_id: str = Field(default_factory=lambda: str(uuid.uuid4()), description="Unique command ID (e.g. UUID)")
    device_id: str = Field(..., description="Target device identifier")
    action: str = Field(..., description="Action to execute on target device")
    parameters: Dict[str, Any] = Field(default_factory=dict, description="Parameters for action")


class IoTMessage(BaseModel):
    schema_version: int = Field(1, description="Schema version of message protocol")
    message_id: str = Field(default_factory=lambda: f"msg_{uuid.uuid4()}", description="Unique message ID")
    hub_id: str = Field(default=HUB_ID, description="Identifier of destination hub")
    commands: List[CommandItem] = Field(default_factory=list, min_length=1, max_length=50, description="List of 1-50 commands")

    @field_validator("schema_version", mode="before")
    @classmethod
    def parse_schema_version(cls, v: Any) -> int:
        if v is None:
            return 1
        try:
            val = int(v)
            if val != 1:
                raise ValueError("Unsupported schema version")
            return val
        except (TypeError, ValueError):
            raise ValueError(f"Unsupported schema version: {v}")

    @field_validator("message_id", mode="before")
    @classmethod
    def parse_message_id(cls, v: Any) -> str:
        if not v:
            return f"msg_{uuid.uuid4()}"
        return str(v)

    @field_validator("hub_id", mode="before")
    @classmethod
    def parse_hub_id(cls, v: Any) -> str:
        if not v:
            return HUB_ID
        return str(v)


class MessageValidationError(Exception):
    """Raised when message validation fails."""
    pass


def validate_incoming_message(raw_data: str | bytes | dict, expected_hub_id: str | None = None) -> IoTMessage:
    """
    Validates raw payload (JSON string, bytes, or dict) using Pydantic.
    Automatically normalizes flat / single command structures if needed.
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

    # If payload is a flat command or missing 'commands', normalize it first
    if "commands" not in payload_dict:
        from ingestion.normalizer import normalize_payload
        payload_dict = normalize_payload(payload_dict, default_hub_id=expected_hub_id or HUB_ID)

    try:
        model = IoTMessage.model_validate(payload_dict)
    except ValidationError as exc:
        errors = [f"{err['loc']}: {err['msg']}" for err in exc.errors()]
        raise MessageValidationError(f"Validation error: {'; '.join(errors)}") from exc

    if expected_hub_id and model.hub_id and model.hub_id != expected_hub_id:
        raise MessageValidationError(f"Incorrect hub_id: expected '{expected_hub_id}', got '{model.hub_id}'")

    return model
