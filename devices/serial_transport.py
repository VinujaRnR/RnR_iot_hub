"""
Asynchronous USB Serial Transport for RnR Linux IoT Hub.
File: devices/serial_transport.py

Implements Step 9 specification:
- Common communication protocol for all compatible USB IoT devices
- Encoding: UTF-8, Payload: JSON, Framing: Newline-delimited ('\\n')
- Baud rate: 115200 (configurable via settings)
- Identification: Device handshake
- Correlation: Unique command_id
- Acknowledgment: Mandatory execution response (write alone is not success)
- Thread-safe serialization per port (asyncio.Lock)
"""

from __future__ import annotations
import asyncio
import logging
import time
from typing import Any, Dict, Optional, Union
import serial

from config import USB_BAUD_RATE, COMMAND_TIMEOUT_SECONDS, HUB_ID
from devices.device_protocol import (
    CommandMessage,
    ExecutionResultMessage,
    HandshakeMessage,
    HelloMessage,
    PingMessage,
    PongMessage,
    MessageType,
    DeviceStatus,
    DeviceNotConnectedError,
    DeviceTimeoutError,
    DeviceExecutionError,
    DeviceFramingError,
    frame_message,
    parse_line,
)

logger = logging.getLogger("iothub.serial_transport")


class SerialTransport:
    """
    Manages robust serial transport to a single physical USB microcontroller.
    Enforces atomic command dispatch, correlation matching, and execution confirmation.
    """

    def __init__(
        self,
        port: str,
        baudrate: int = USB_BAUD_RATE,
        timeout: float = float(COMMAND_TIMEOUT_SECONDS),
        hub_id: str = HUB_ID,
        serial_instance: Optional[serial.Serial] = None,
    ):
        self.port = port
        self.baudrate = baudrate
        self.default_timeout = timeout
        self.hub_id = hub_id
        self._serial: Optional[serial.Serial] = serial_instance
        self._lock = asyncio.Lock()
        self._device_id: Optional[str] = None
        self._last_seen: float = 0.0
        self._read_buffer = bytearray()

    @property
    def is_connected(self) -> bool:
        """Returns True if the serial port is currently open."""
        return bool(self._serial and getattr(self._serial, "is_open", False))

    @property
    def device_id(self) -> Optional[str]:
        """Returns the identified iot_node_id if authenticated via handshake."""
        return self._device_id

    def connect(self) -> bool:
        """
        Synchronously opens the serial port with the configured baudrate.
        Clears pending buffer garbage.
        """
        if self.is_connected:
            return True

        try:
            if not self._serial:
                self._serial = serial.Serial(
                    port=self.port,
                    baudrate=self.baudrate,
                    timeout=0.1,        # Short read timeout to allow non-blocking async loops
                    write_timeout=2.0,  # Write timeout
                )
            elif not self._serial.is_open:
                self._serial.open()

            # Flush residual hardware buffers
            try:
                self._serial.reset_input_buffer()
                self._serial.reset_output_buffer()
            except Exception:
                pass

            self._last_seen = time.time()
            logger.info("Opened serial connection on %s @ %d baud", self.port, self.baudrate)
            return True
        except Exception as exc:
            logger.error("Failed to open serial port %s: %s", self.port, exc)
            return False

    def disconnect(self) -> None:
        """Closes the underlying serial port."""
        if self._serial and getattr(self._serial, "is_open", False):
            try:
                self._serial.close()
            except Exception as exc:
                logger.warning("Error closing serial port %s: %s", self.port, exc)
            finally:
                logger.info("Closed serial connection on %s", self.port)

    async def __aenter__(self) -> SerialTransport:
        self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        self.disconnect()

    # =====================================================================
    # Handshake & Device Identification
    # =====================================================================

    async def perform_handshake(self, timeout: Optional[float] = None) -> HelloMessage:
        """
        Executes Step 9 Device Handshake:
        1. Hub sends: {"type": "handshake", "hub_id": "hub-01"}
        2. Device replies: {"type": "hello", "device_id": "...", "firmware_version": "...", "channels": [...]}
        3. Identifies and caches device_id.
        """
        effective_timeout = timeout if timeout is not None else self.default_timeout

        async with self._lock:
            if not self.is_connected:
                if not self.connect():
                    raise DeviceNotConnectedError(f"Cannot perform handshake: port {self.port} is closed")

            handshake_cmd = HandshakeMessage(hub_id=self.hub_id)
            payload_bytes = handshake_cmd.to_bytes()

            # Flush input buffer before transmission
            await self._run_blocking(self._flush_buffers)

            # Write handshake frame
            await self._run_blocking(self._write_bytes, payload_bytes)
            logger.debug("Dispatched handshake to %s: %s", self.port, handshake_cmd.to_json())

            deadline = time.time() + effective_timeout
            while time.time() < deadline:
                remaining = max(0.01, deadline - time.time())
                try:
                    frame = await self._read_json_frame(timeout=remaining)
                    if not frame:
                        continue

                    msg_type = frame.get("type")
                    if msg_type == MessageType.HELLO.value:
                        hello = HelloMessage.model_validate(frame)
                        self._device_id = hello.device_id
                        self._last_seen = time.time()
                        logger.info(
                            "Handshake SUCCESS on %s: Node ID '%s', Firmware v%s, Channels: %s",
                            self.port,
                            hello.device_id,
                            hello.firmware_version,
                            hello.channels,
                        )
                        return hello
                    else:
                        logger.debug("Ignoring non-hello frame during handshake on %s: %s", self.port, frame)
                except DeviceFramingError as fe:
                    logger.debug("Ignoring framing noise during handshake on %s: %s", self.port, fe)
                except Exception as exc:
                    logger.warning("Error processing handshake line on %s: %s", self.port, exc)

            raise DeviceTimeoutError(
                f"Handshake timed out on port {self.port} after {effective_timeout:.1f}s",
                timeout=effective_timeout,
            )

    # =====================================================================
    # Command Dispatch & Execution Validation
    # =====================================================================

    async def send_command(
        self,
        command: Union[CommandMessage, Dict[str, Any]],
        timeout: Optional[float] = None,
        raise_on_failure: bool = False,
    ) -> ExecutionResultMessage:
        """
        Executes Step 9 USB Command Protocol:
        1. Encodes command into newline-delimited UTF-8 JSON.
        2. Flushes buffers and transmits to device.
        3. Mandatory Rule: Successful write alone is NOT success.
        4. Listens for correlated {"type": "result", "command_id": command_id, ...}.
        5. Validates device status.
        6. If raise_on_failure=True and status != SUCCESS, raises DeviceExecutionError.
        """
        effective_timeout = timeout if timeout is not None else self.default_timeout

        if isinstance(command, dict):
            cmd_obj = CommandMessage.model_validate(command)
        elif isinstance(command, CommandMessage):
            cmd_obj = command
        else:
            raise TypeError(f"Expected CommandMessage or dict, got {type(command)}")

        async with self._lock:
            if not self.is_connected:
                if not self.connect():
                    raise DeviceNotConnectedError(f"Cannot dispatch command: port {self.port} is closed")

            payload_bytes = cmd_obj.to_bytes()

            # Clear OS input buffer so stale responses don't corrupt this command
            await self._run_blocking(self._flush_buffers)

            # Transmit command frame
            await self._run_blocking(self._write_bytes, payload_bytes)
            logger.info("Transmitted command '%s' (%s) to %s", cmd_obj.command_id, cmd_obj.action, self.port)

            # Await mandatory response with matching correlation ID
            deadline = time.time() + effective_timeout
            while time.time() < deadline:
                remaining = max(0.01, deadline - time.time())
                try:
                    frame = await self._read_json_frame(timeout=remaining)
                    if not frame:
                        continue

                    # Check if response is a result
                    if frame.get("type") == MessageType.RESULT.value:
                        resp_cmd_id = frame.get("command_id")

                        # Verify correlation ID
                        if resp_cmd_id == cmd_obj.command_id:
                            result_msg = ExecutionResultMessage.model_validate(frame)
                            self._last_seen = time.time()

                            if result_msg.is_success:
                                logger.info(
                                    "Command '%s' SUCCESS on device '%s' (%s)",
                                    cmd_obj.command_id,
                                    result_msg.device_id,
                                    self.port,
                                )
                            else:
                                logger.warning(
                                    "Command '%s' FAILED on device '%s': status=%s, error=%s",
                                    cmd_obj.command_id,
                                    result_msg.device_id,
                                    result_msg.status,
                                    result_msg.error,
                                )
                                if raise_on_failure:
                                    raise DeviceExecutionError(
                                        message=f"Device execution failed: {result_msg.error or result_msg.status}",
                                        command_id=cmd_obj.command_id,
                                        device_id=result_msg.device_id,
                                        status=result_msg.status,
                                        error_detail=result_msg.error,
                                        raw_response=frame,
                                    )

                            return result_msg
                        else:
                            logger.warning(
                                "Mismatched correlation on %s: expected '%s', received '%s'",
                                self.port,
                                cmd_obj.command_id,
                                resp_cmd_id,
                            )
                    else:
                        # Unsolicited frame (e.g. alert or status)
                        logger.debug("Received non-result frame on %s while awaiting '%s': %s", self.port, cmd_obj.command_id, frame)

                except DeviceFramingError as fe:
                    # Ignore random noise/boot chars
                    logger.debug("Ignoring framing noise on %s: %s", self.port, fe)

            # Timeout expired without valid result response
            raise DeviceTimeoutError(
                f"Command '{cmd_obj.command_id}' timed out after {effective_timeout:.1f}s on {self.port}",
                command_id=cmd_obj.command_id,
                timeout=effective_timeout,
            )

    # =====================================================================
    # Heartbeat / Ping
    # =====================================================================

    async def ping(self, timeout: float = 2.0) -> bool:
        """Sends a ping message and awaits a pong response."""
        async with self._lock:
            if not self.is_connected:
                return False

            try:
                ping_msg = PingMessage()
                await self._run_blocking(self._write_bytes, ping_msg.to_bytes())
                deadline = time.time() + timeout
                while time.time() < deadline:
                    remaining = max(0.01, deadline - time.time())
                    frame = await self._read_json_frame(timeout=remaining)
                    if frame and frame.get("type") == MessageType.PONG.value:
                        self._last_seen = time.time()
                        return True
                return False
            except Exception as exc:
                logger.warning("Ping failed on %s: %s", self.port, exc)
                return False

    # =====================================================================
    # Internal I/O Helpers
    # =====================================================================

    async def _run_blocking(self, func, *args):
        """Runs blocking serial function in background thread executor."""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, func, *args)

    def _flush_buffers(self) -> None:
        """Flushes serial input buffer and internal read buffer."""
        self._read_buffer.clear()
        if self._serial and getattr(self._serial, "is_open", False):
            try:
                self._serial.reset_input_buffer()
            except Exception:
                pass

    def _write_bytes(self, data: bytes) -> None:
        """Synchronously writes data to serial port and flushes OS buffer."""
        if not self._serial or not getattr(self._serial, "is_open", False):
            raise DeviceNotConnectedError(f"Serial port {self.port} is not open for writing")
        self._serial.write(data)
        self._serial.flush()

    async def _read_json_frame(self, timeout: float) -> Optional[Dict[str, Any]]:
        """
        Reads lines from serial port within timeout and parses JSON.
        Maintains an internal buffer across calls to prevent frame fragmentation.
        Returns parsed dict or None if timeout expired.
        """
        deadline = time.time() + timeout

        while time.time() < deadline:
            # First check if we already have a complete line buffered
            if b"\n" in self._read_buffer:
                line_bytes, _, remaining = self._read_buffer.partition(b"\n")
                self._read_buffer = bytearray(remaining)

                line_str = line_bytes.decode("utf-8", errors="replace").strip()
                if not line_str:
                    continue  # Ignore blank lines

                return parse_line(line_str)

            loop = asyncio.get_running_loop()

            def _blocking_read_chunk() -> bytes:
                if not self._serial or not getattr(self._serial, "is_open", False):
                    return b""
                in_waiting = getattr(self._serial, "in_waiting", 0)
                if in_waiting > 0:
                    return self._serial.read(in_waiting)
                return self._serial.read(1)

            chunk = await loop.run_in_executor(None, _blocking_read_chunk)

            if not chunk:
                await asyncio.sleep(0.01)
                continue

            self._read_buffer.extend(chunk)

            if b"\n" in self._read_buffer:
                line_bytes, _, remaining = self._read_buffer.partition(b"\n")
                self._read_buffer = bytearray(remaining)

                line_str = line_bytes.decode("utf-8", errors="replace").strip()
                if not line_str:
                    continue

                return parse_line(line_str)

        return None
