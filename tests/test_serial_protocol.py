"""
Tests for USB Communication Protocol and Serial Transport (Step 9).
Verifies:
- UTF-8 newline-delimited JSON framing
- Command dispatch and correlation by command_id
- Mandatory execution response (write alone is not success)
- Device handshake and identification
- Error handling, timeouts, framing noise resilience, and correlation checks
"""

from __future__ import annotations
import asyncio
import json
from typing import Any, Dict, List, Optional, Union
from unittest.mock import MagicMock
import pytest

from devices.device_protocol import (
    CommandMessage,
    ExecutionResultMessage,
    HandshakeMessage,
    HelloMessage,
    PingMessage,
    PongMessage,
    MessageType,
    DeviceStatus,
    DeviceProtocolError,
    DeviceExecutionError,
    DeviceTimeoutError,
    DeviceNotConnectedError,
    DeviceFramingError,
    frame_message,
    parse_line,
)
from devices.serial_transport import SerialTransport


class MockSerialPort:
    """Simulates an in-memory serial.Serial object for testing."""

    def __init__(self, port: str = "/dev/ttyUSB0", baudrate: int = 115200):
        self.port = port
        self.baudrate = baudrate
        self.is_open = True
        self.written_bytes = bytearray()
        self.incoming_bytes = bytearray()
        self.read_chunk_size = 1024

    @property
    def in_waiting(self) -> int:
        return len(self.incoming_bytes)

    def write(self, data: bytes) -> int:
        if not self.is_open:
            raise RuntimeError("Port closed")
        self.written_bytes.extend(data)
        return len(data)

    def flush(self) -> None:
        pass

    def read(self, size: int = 1) -> bytes:
        if not self.is_open:
            raise RuntimeError("Port closed")
        if not self.incoming_bytes:
            return b""
        actual_size = min(size, len(self.incoming_bytes))
        chunk = bytes(self.incoming_bytes[:actual_size])
        del self.incoming_bytes[:actual_size]
        return chunk

    def reset_input_buffer(self) -> None:
        pass

    def reset_output_buffer(self) -> None:
        self.written_bytes.clear()

    def close(self) -> None:
        self.is_open = False

    def open(self) -> None:
        self.is_open = True

    def queue_incoming_line(self, line: Union[str, bytes, dict]) -> None:
        """Helper to stage an incoming serial line from the simulated device."""
        if isinstance(line, dict):
            raw = (json.dumps(line) + "\n").encode("utf-8")
        elif isinstance(line, str):
            raw = (line.rstrip("\n") + "\n").encode("utf-8")
        else:
            raw = line if line.endswith(b"\n") else line + b"\n"
        self.incoming_bytes.extend(raw)


# =====================================================================
# Protocol Framing & Model Tests
# =====================================================================

def test_command_message_framing():
    """Verifies command serialization to UTF-8 newline-delimited JSON."""
    cmd = CommandMessage(
        command_id="action-001",
        action="LIGHTS_ON",
        channel_key="relay:light",
        params={"intensity": 100},
    )
    raw_bytes = cmd.to_bytes()
    assert raw_bytes.endswith(b"\n")
    decoded = json.loads(raw_bytes.decode("utf-8").strip())
    assert decoded["type"] == "command"
    assert decoded["command_id"] == "action-001"
    assert decoded["action"] == "LIGHTS_ON"
    assert decoded["channel_key"] == "relay:light"
    assert decoded["params"] == {"intensity": 100}


def test_execution_result_parsing():
    """Verifies result message parsing and success status checking."""
    success_raw = {
        "type": "result",
        "command_id": "action-001",
        "device_id": "98a316e6b658",
        "status": "SUCCESS",
        "result": {"state": 1},
    }
    msg = ExecutionResultMessage.model_validate(success_raw)
    assert msg.is_success is True
    assert msg.command_id == "action-001"
    assert msg.device_id == "98a316e6b658"

    failed_raw = {
        "type": "result",
        "command_id": "action-002",
        "device_id": "98a316e6b658",
        "status": "FAILED",
        "error": "Relay coil disconnected",
    }
    fail_msg = ExecutionResultMessage.model_validate(failed_raw)
    assert fail_msg.is_success is False
    assert fail_msg.error == "Relay coil disconnected"


def test_parse_line_validation():
    """Verifies parse_line correctly handles valid and corrupt frames."""
    # Valid frame
    line = b'{"type":"pong","device_id":"98a316e6b658"}\n'
    parsed = parse_line(line)
    assert parsed["type"] == "pong"
    assert parsed["device_id"] == "98a316e6b658"

    # Malformed JSON
    with pytest.raises(DeviceFramingError):
        parse_line(b"BOOTLOADER STARTING...\n")

    # Empty frame
    with pytest.raises(DeviceFramingError):
        parse_line(b"   \n")


# =====================================================================
# Serial Transport Tests (Step 9 Specification)
# =====================================================================

@pytest.mark.asyncio
async def test_handshake_success():
    """
    Step 9: Hub initiates handshake, device returns hello with node ID and channels.
    """
    mock_serial = MockSerialPort(port="/dev/ttyUSB0")
    transport = SerialTransport(port="/dev/ttyUSB0", hub_id="hub-01", serial_instance=mock_serial)

    # Stage device response to handshake
    mock_serial.queue_incoming_line(
        {
            "type": "hello",
            "device_id": "98a316e6b658",
            "firmware_version": "1.0.0",
            "channels": ["relay:light"],
        }
    )

    hello = await transport.perform_handshake(timeout=1.0)
    assert hello.device_id == "98a316e6b658"
    assert hello.firmware_version == "1.0.0"
    assert hello.channels == ["relay:light"]
    assert transport.device_id == "98a316e6b658"

    # Verify hub transmitted valid handshake frame
    written_str = mock_serial.written_bytes.decode("utf-8")
    assert '"type": "handshake"' in written_str
    assert '"hub_id": "hub-01"' in written_str


@pytest.mark.asyncio
async def test_send_command_success():
    """
    Step 9: Dispatches command, awaits correlated result response with SUCCESS status.
    """
    mock_serial = MockSerialPort(port="/dev/ttyUSB0")
    transport = SerialTransport(port="/dev/ttyUSB0", hub_id="hub-01", serial_instance=mock_serial)

    # Stage device response matching command_id 'action-001'
    mock_serial.queue_incoming_line(
        {
            "type": "result",
            "command_id": "action-001",
            "device_id": "98a316e6b658",
            "status": "SUCCESS",
        }
    )

    cmd = {
        "type": "command",
        "command_id": "action-001",
        "action": "LIGHTS_ON",
        "channel_key": "relay:light",
        "params": {},
    }

    result = await transport.send_command(cmd, timeout=1.0)
    assert result.command_id == "action-001"
    assert result.device_id == "98a316e6b658"
    assert result.status == "SUCCESS"
    assert result.is_success is True

    # Verify command written to serial port
    written_str = mock_serial.written_bytes.decode("utf-8")
    assert '"command_id": "action-001"' in written_str
    assert '"action": "LIGHTS_ON"' in written_str


@pytest.mark.asyncio
async def test_successful_write_alone_is_not_execution_success_on_timeout():
    """
    Step 9 Rule: A successful USB write alone must NOT be interpreted
    as successful device execution.
    If the device fails to respond with a result, a DeviceTimeoutError is raised.
    """
    mock_serial = MockSerialPort(port="/dev/ttyUSB0")
    transport = SerialTransport(port="/dev/ttyUSB0", hub_id="hub-01", serial_instance=mock_serial)

    cmd = {
        "type": "command",
        "command_id": "action-timeout-test",
        "action": "LIGHTS_ON",
        "channel_key": "relay:light",
        "params": {},
    }

    # Serial write will succeed, but NO response is queued
    with pytest.raises(DeviceTimeoutError) as exc_info:
        await transport.send_command(cmd, timeout=0.1)

    assert "action-timeout-test" in str(exc_info.value)
    # Confirm bytes were indeed written
    assert len(mock_serial.written_bytes) > 0


@pytest.mark.asyncio
async def test_device_execution_failure_handling():
    """
    Verifies that when device reports FAILED or ERROR:
    - Default returns ExecutionResultMessage with is_success=False
    - raise_on_failure=True raises DeviceExecutionError
    """
    mock_serial = MockSerialPort(port="/dev/ttyUSB0")
    transport = SerialTransport(port="/dev/ttyUSB0", hub_id="hub-01", serial_instance=mock_serial)

    mock_serial.queue_incoming_line(
        {
            "type": "result",
            "command_id": "action-fail-01",
            "device_id": "98a316e6b658",
            "status": "FAILED",
            "error": "Overcurrent protection tripped",
        }
    )

    cmd = {
        "command_id": "action-fail-01",
        "action": "START_MACHINE",
        "channel_key": "main",
    }

    # Case A: raise_on_failure=False returns result object
    res = await transport.send_command(cmd, timeout=1.0, raise_on_failure=False)
    assert res.is_success is False
    assert res.status == "FAILED"
    assert res.error == "Overcurrent protection tripped"

    # Case B: raise_on_failure=True raises DeviceExecutionError
    mock_serial.queue_incoming_line(
        {
            "type": "result",
            "command_id": "action-fail-02",
            "device_id": "98a316e6b658",
            "status": "ERROR",
            "error": "Hardware fault",
        }
    )
    cmd2 = dict(cmd, command_id="action-fail-02")
    with pytest.raises(DeviceExecutionError) as exc_info:
        await transport.send_command(cmd2, timeout=1.0, raise_on_failure=True)

    assert exc_info.value.command_id == "action-fail-02"
    assert exc_info.value.status == "ERROR"
    assert "Hardware fault" in exc_info.value.error_detail


@pytest.mark.asyncio
async def test_correlation_and_noise_filtering():
    """
    Verifies that the transport:
    1. Ignores random serial boot noise/unformatted lines
    2. Ignores responses for mismatched command IDs
    3. Successfully picks up the correlated response
    """
    mock_serial = MockSerialPort(port="/dev/ttyUSB0")
    transport = SerialTransport(port="/dev/ttyUSB0", hub_id="hub-01", serial_instance=mock_serial)

    # 1. Unformatted boot log noise
    mock_serial.queue_incoming_line("rst:0x1 (POWERON_RESET),boot:0x13 (SPI_FAST_FLASH_BOOT)")
    mock_serial.queue_incoming_line("configsip: 0, SPIWP:0xee")
    # 2. Out-of-order response for an old command
    mock_serial.queue_incoming_line(
        {"type": "result", "command_id": "old-stale-cmd", "device_id": "98a316e6b658", "status": "SUCCESS"}
    )
    # 3. Valid correlated response for current command
    mock_serial.queue_incoming_line(
        {"type": "result", "command_id": "target-cmd-99", "device_id": "98a316e6b658", "status": "SUCCESS"}
    )

    cmd = {
        "command_id": "target-cmd-99",
        "action": "LIGHTS_ON",
        "channel_key": "relay:light",
    }

    res = await transport.send_command(cmd, timeout=1.0)
    assert res.command_id == "target-cmd-99"
    assert res.is_success is True


@pytest.mark.asyncio
async def test_ping_pong():
    """Verifies ping/pong heartbeat mechanism."""
    mock_serial = MockSerialPort(port="/dev/ttyUSB0")
    transport = SerialTransport(port="/dev/ttyUSB0", hub_id="hub-01", serial_instance=mock_serial)

    mock_serial.queue_incoming_line({"type": "pong", "device_id": "98a316e6b658"})
    assert await transport.ping(timeout=1.0) is True


@pytest.mark.asyncio
async def test_disconnected_port_error():
    """Verifies that operations on disconnected port raise DeviceNotConnectedError."""
    mock_serial = MockSerialPort(port="/dev/ttyUSB0")
    mock_serial.close()
    transport = SerialTransport(port="/dev/ttyUSB0", serial_instance=mock_serial)

    # Prevent auto-reopen in mock
    mock_serial.open = MagicMock(side_effect=RuntimeError("Cannot open port"))

    with pytest.raises(DeviceNotConnectedError):
        await transport.send_command({"command_id": "c1", "action": "test", "channel_key": "k"})
