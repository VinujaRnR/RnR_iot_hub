"""
USB Device Communication Protocol for RnR Linux IoT Hub.
File: devices/device_protocol.py

Implements Step 9 specification:
- Encoding: UTF-8
- Payload format: JSON
- Message framing: Newline-delimited ('\\n')
- Correlation: Unique command/action ID
- Mandatory execution response: Validates device execution status
- Handshake & Device Identification
"""

from __future__ import annotations
from datetime import datetime, timezone
from enum import Enum
import json
import logging
from typing import Any, Dict, List, Optional, Union
from pydantic import BaseModel, Field, field_validator

logger = logging.getLogger("iothub.protocol")


class MessageType(str, Enum):
    """Recognized protocol message types."""
    HANDSHAKE = "handshake"
    HELLO = "hello"
    COMMAND = "command"
    RESULT = "result"
    PING = "ping"
    PONG = "pong"
    ALERT = "alert"


class DeviceStatus(str, Enum):
    """Device execution outcome status."""
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    ERROR = "ERROR"
    BUSY = "BUSY"


# =====================================================================
# Protocol Exceptions
# =====================================================================

class DeviceProtocolError(Exception):
    """Base exception for USB IoT device protocol errors."""
    pass


class DeviceNotConnectedError(DeviceProtocolError):
    """Raised when attempting an operation on a closed/disconnected serial port."""
    pass


class DeviceTimeoutError(DeviceProtocolError):
    """Raised when the device fails to respond within the configured timeout."""
    def __init__(self, message: str, command_id: Optional[str] = None, timeout: Optional[float] = None):
        super().__init__(message)
        self.command_id = command_id
        self.timeout = timeout


class DeviceExecutionError(DeviceProtocolError):
    """Raised when device explicitly reports execution failure (status != SUCCESS)."""
    def __init__(
        self,
        message: str,
        command_id: str,
        device_id: Optional[str] = None,
        status: str = "FAILED",
        error_detail: Optional[str] = None,
        raw_response: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(message)
        self.command_id = command_id
        self.device_id = device_id
        self.status = status
        self.error_detail = error_detail
        self.raw_response = raw_response or {}


class DeviceFramingError(DeviceProtocolError):
    """Raised when an unparseable or corrupted frame is received from serial."""
    def __init__(self, message: str, raw_line: str = ""):
        super().__init__(message)
        self.raw_line = raw_line


class DeviceCorrelationError(DeviceProtocolError):
    """Raised when response correlation ID does not match the active command ID."""
    pass


# =====================================================================
# Protocol Data Models
# =====================================================================

class CommandMessage(BaseModel):
    """
    Command dispatched by the IoT Hub to a USB device.
    Example:
    {
      "type": "command",
      "command_id": "action-001",
      "action": "LIGHTS_ON",
      "channel_key": "relay:light",
      "params": {}
    }
    """
    model_config = {"extra": "ignore"}

    type: str = Field(default=MessageType.COMMAND.value, description="Message type identifier")
    command_id: str = Field(..., min_length=1, description="Unique action/command correlation ID")
    action: str = Field(..., min_length=1, description="Operation to execute (e.g. LIGHTS_ON)")
    channel_key: str = Field(..., min_length=1, description="Target channel key (e.g. relay:light)")
    params: Dict[str, Any] = Field(default_factory=dict, description="Action parameter dictionary")
    timestamp: Optional[str] = Field(default=None, description="ISO timestamp of command creation")

    @field_validator("type")
    @classmethod
    def validate_type(cls, v: str) -> str:
        if v != MessageType.COMMAND.value:
            raise ValueError(f"Command message must have type='{MessageType.COMMAND.value}', got '{v}'")
        return v

    def to_json(self) -> str:
        """Serializes command to JSON string."""
        data = self.model_dump(exclude_none=True)
        if not data.get("timestamp"):
            data["timestamp"] = datetime.now(timezone.utc).isoformat()
        return json.dumps(data)

    def to_bytes(self) -> bytes:
        """Serializes command to UTF-8 newline-delimited bytes."""
        return (self.to_json() + "\n").encode("utf-8")


class ExecutionResultMessage(BaseModel):
    """
    Execution result reported by the USB device.
    Example:
    {
      "type": "result",
      "command_id": "action-001",
      "device_id": "98a316e6b658",
      "status": "SUCCESS"
    }
    """
    model_config = {"extra": "ignore"}

    type: str = Field(default=MessageType.RESULT.value, description="Message type identifier")
    command_id: str = Field(..., min_length=1, description="Matching correlation command ID")
    device_id: str = Field(..., min_length=1, description="Responding device hardware ID")
    status: str = Field(..., description="Execution status: SUCCESS, FAILED, ERROR, etc.")
    result: Dict[str, Any] = Field(default_factory=dict, description="Optional execution telemetry/data")
    error: Optional[str] = Field(default=None, description="Error explanation if status != SUCCESS")
    timestamp: Optional[str] = Field(default=None, description="Device response timestamp")

    @property
    def is_success(self) -> bool:
        """Returns True if status is SUCCESS."""
        return self.status.strip().upper() == DeviceStatus.SUCCESS.value

    def to_json(self) -> str:
        return json.dumps(self.model_dump(exclude_none=True))

    def to_bytes(self) -> bytes:
        return (self.to_json() + "\n").encode("utf-8")


class HandshakeMessage(BaseModel):
    """
    Handshake inquiry dispatched by hub to discover and authenticate connected device.
    """
    model_config = {"extra": "ignore"}

    type: str = Field(default=MessageType.HANDSHAKE.value)
    hub_id: str = Field(..., min_length=1, description="Linux IoT Hub ID")
    timestamp: Optional[str] = Field(default=None)

    def to_json(self) -> str:
        data = self.model_dump(exclude_none=True)
        if not data.get("timestamp"):
            data["timestamp"] = datetime.now(timezone.utc).isoformat()
        return json.dumps(data)

    def to_bytes(self) -> bytes:
        return (self.to_json() + "\n").encode("utf-8")


class HelloMessage(BaseModel):
    """
    Device identification announcement returned during handshake.
    Example:
    {
      "type": "hello",
      "device_id": "98a316e6b658",
      "firmware_version": "1.0.0",
      "channels": ["relay:light"]
    }
    """
    model_config = {"extra": "ignore"}

    type: str = Field(default=MessageType.HELLO.value)
    device_id: str = Field(..., min_length=1, description="Unique physical node ID (e.g. MAC address or MCU UID)")
    firmware_version: str = Field(default="1.0.0", description="Firmware build version")
    channels: List[str] = Field(default_factory=list, description="Supported device hardware channels")
    capabilities: Dict[str, Any] = Field(default_factory=dict, description="Hardware capabilities")

    def to_json(self) -> str:
        return json.dumps(self.model_dump(exclude_none=True))

    def to_bytes(self) -> bytes:
        return (self.to_json() + "\n").encode("utf-8")


class PingMessage(BaseModel):
    """Heartbeat check message."""
    type: str = Field(default=MessageType.PING.value)

    def to_bytes(self) -> bytes:
        return (json.dumps({"type": "ping"}) + "\n").encode("utf-8")


class PongMessage(BaseModel):
    """Heartbeat reply message."""
    type: str = Field(default=MessageType.PONG.value)
    device_id: Optional[str] = None


# =====================================================================
# Framing & Codec Utilities
# =====================================================================

def frame_message(payload: Union[BaseModel, Dict[str, Any]]) -> bytes:
    """Encodes a payload dictionary or BaseModel into newline-delimited UTF-8 bytes."""
    if isinstance(payload, BaseModel):
        if hasattr(payload, "to_bytes"):
            return payload.to_bytes()
        json_str = payload.model_dump_json(exclude_none=True)
    elif isinstance(payload, dict):
        json_str = json.dumps(payload)
    else:
        raise TypeError(f"Unsupported payload type for serial framing: {type(payload)}")

    return (json_str.strip() + "\n").encode("utf-8")


def parse_line(line: Union[bytes, str]) -> Dict[str, Any]:
    """
    Decodes and parses a single newline-delimited JSON line from serial.
    Raises DeviceFramingError on decoding or JSON parsing failures.
    """
    if isinstance(line, bytes):
        try:
            line_str = line.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise DeviceFramingError(f"UTF-8 decode error: {exc}", raw_line=repr(line)) from exc
    else:
        line_str = str(line)

    clean_line = line_str.strip()
    if not clean_line:
        raise DeviceFramingError("Empty line received from serial", raw_line=line_str)

    try:
        data = json.loads(clean_line)
    except json.JSONDecodeError as exc:
        raise DeviceFramingError(f"Malformed JSON frame: {exc}", raw_line=clean_line) from exc

    if not isinstance(data, dict):
        raise DeviceFramingError(f"Expected JSON object, got {type(data).__name__}", raw_line=clean_line)

    return data
