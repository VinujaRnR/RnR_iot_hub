"""
Physical Arduino Nano USB Device Driver for RnR Linux IoT Hub.
File: devices/arduino_nano.py

Controls the connected Arduino Nano on /dev/ttyUSB0.
Maps:
- LIGHTS_ON  -> sends '1' to serial (Built-in LED / Relay ON)
- LIGHTS_OFF -> sends '0' to serial (Built-in LED / Relay OFF)
"""

from __future__ import annotations
import asyncio
import logging
import os
import time
from typing import Any, Dict, Optional
import serial

logger = logging.getLogger("iothub.devices.arduino_nano")


class ArduinoNanoDevice:
    """Device driver controlling physical Arduino Nano over USB serial."""

    def __init__(
        self,
        port: str = "/dev/ttyUSB0",
        device_id: str = "/dev/ttyUSB0",
        baudrate: int = 9600,
        name: str = "Arduino Nano (LED / Relay Light)",
    ):
        self.port = port
        self.device_id = device_id
        self.baudrate = baudrate
        self.name = name
        self.channels = ["relay:light"]
        self.supported_actions = ["LIGHTS_ON", "LIGHTS_OFF", "STATUS", "HELLO", "HANDSHAKE"]
        self._state: Dict[str, Any] = {"led": "OFF"}
        self._last_command_id: Optional[str] = None
        self._serial: Optional[serial.Serial] = None
        self._lock = asyncio.Lock()

    def _ensure_port_open(self) -> serial.Serial:
        """Opens or reuses serial connection."""
        if self._serial and self._serial.is_open:
            return self._serial

        if not os.path.exists(self.port):
            raise FileNotFoundError(f"Serial port '{self.port}' not found. Ensure Arduino Nano is plugged in.")

        self._serial = serial.Serial(
            port=self.port,
            baudrate=self.baudrate,
            timeout=1.0,
            write_timeout=2.0,
        )
        time.sleep(1.8)  # Settling delay for ATmega328 bootloader reset
        logger.info("Opened serial connection to Arduino Nano on %s @ %d baud", self.port, self.baudrate)
        return self._serial

    def close(self) -> None:
        """Closes serial connection."""
        if self._serial and self._serial.is_open:
            try:
                self._serial.close()
            except Exception as exc:
                logger.warning("Error closing serial port: %s", exc)
            finally:
                self._serial = None

    async def execute_command(self, action: str, parameters: Dict[str, Any]) -> Dict[str, Any]:
        """
        Executes physical lighting actions on the Arduino Nano.
        Sends:
          '1' for LIGHTS_ON
          '0' for LIGHTS_OFF
        """
        action_upper = action.strip().upper()
        cmd_id = parameters.get("command_id") or parameters.get("action_id")

        # Duplicate command suppression
        if cmd_id and cmd_id == self._last_command_id:
            logger.info("Duplicate command '%s' suppressed on %s", cmd_id, self.port)
            return {
                "status": "SUCCESS",
                "action": action_upper,
                "state": self._state.get("led", "OFF"),
                "port": self.port,
                "duplicate": True,
            }

        if action_upper in ("HELLO", "HANDSHAKE"):
            return {
                "status": "SUCCESS",
                "type": "hello",
                "device_id": self.device_id,
                "channels": self.channels,
                "actions": self.supported_actions,
                "state": self._state.get("led", "OFF"),
                "port": self.port,
            }

        async with self._lock:
            loop = asyncio.get_running_loop()

            def _serial_io() -> Dict[str, Any]:
                ser = self._ensure_port_open()

                if action_upper in ("LIGHTS_ON", "LIGHT_ON", "ON") or (
                    action_upper == "SET_LED" and parameters.get("state", "").lower() == "on"
                ):
                    ser.write(b"1")
                    ser.flush()
                    self._state["led"] = "ON"
                    if cmd_id:
                        self._last_command_id = cmd_id
                    logger.info(">>> Physical Arduino Nano [%s]: Sent '1' -> LED / LIGHT IS ON", self.port)
                    return {"status": "SUCCESS", "action": "LIGHTS_ON", "state": "ON", "port": self.port}

                elif action_upper in ("LIGHTS_OFF", "LIGHT_OFF", "OFF") or (
                    action_upper == "SET_LED" and parameters.get("state", "").lower() == "off"
                ):
                    ser.write(b"0")
                    ser.flush()
                    self._state["led"] = "OFF"
                    if cmd_id:
                        self._last_command_id = cmd_id
                    logger.info(">>> Physical Arduino Nano [%s]: Sent '0' -> LED / LIGHT IS OFF", self.port)
                    return {"status": "SUCCESS", "action": "LIGHTS_OFF", "state": "OFF", "port": self.port}

                elif action_upper == "STATUS":
                    return {"status": "SUCCESS", "action": "STATUS", "state": self._state.get("led", "UNKNOWN")}

                else:
                    raise ValueError(f"Unsupported action '{action}' for Arduino Nano. Supported: {self.supported_actions}")

            return await loop.run_in_executor(None, _serial_io)
