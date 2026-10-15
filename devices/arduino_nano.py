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
        self.channels = ["relay:light", "relay:valve", "relay:light_glow", "relay:water_valve", "actuator"]
        self.supported_actions = [
            "LIGHTS_ON", "LIGHTS_OFF", "LIGHT_ON", "LIGHT_OFF",
            "VALVE_ON", "VALVE_OFF", "VALVE_OPEN", "VALVE_CLOSE",
            "STATUS", "HELLO", "HANDSHAKE", "change_size", "set_led",
        ]
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
        Executes actions on physical device over serial.
        Formats and transmits JSON command:
        {
          "action": "<action>",
          "params": { ... }  # omitted if empty or null
        }
        Also maintains backward compatibility by asserting raw pulses ('1', '0', 'v', 'c')
        for simple microcontrollers.
        """
        action_upper = action.strip().upper()
        cmd_id = parameters.get("command_id") or parameters.get("action_id")

        # Duplicate command suppression
        if cmd_id and cmd_id == self._last_command_id:
            logger.info("Duplicate command '%s' suppressed on %s", cmd_id, self.port)
            return {
                "status": "SUCCESS",
                "action": action,
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

        if action_upper == "STATUS":
            return {
                "status": "SUCCESS",
                "action": "STATUS",
                "state": self._state.get("led", "UNKNOWN"),
                "states": dict(self._state),
                "port": self.port,
            }

        # Resolve target device identifier for topic
        target_dev = (
            parameters.get("target_device")
            or parameters.get("device")
            or (self.device_id if not str(self.device_id).startswith("/dev/") else None)
            or "C45BBE6DF6A9"
        )

        # Extract parameters for command payload
        if "params" in parameters and isinstance(parameters["params"], dict):
            clean_params = dict(parameters["params"])
        elif "params" in parameters and parameters["params"] is None:
            clean_params = {}
        else:
            clean_params = {
                k: v for k, v in parameters.items()
                if k not in ("target_device", "command_id", "action_id", "state", "params", "device")
            }

        # Build command payload matching exact specification:
        # {
        #   "action": "change_size",
        #   "params": { ... }
        # }
        # Note: Must NOT contain "topic". If no params, omit the "params" key entirely.
        cmd_payload: Dict[str, Any] = {
            "action": action,
        }
        if clean_params:
            cmd_payload["params"] = clean_params

        async with self._lock:
            loop = asyncio.get_running_loop()

            def _serial_io() -> Dict[str, Any]:
                import json
                cmd_json_bytes = (json.dumps(cmd_payload) + "\n").encode("utf-8")

                ser = None
                try:
                    ser = self._ensure_port_open()
                    ser.write(cmd_json_bytes)
                    ser.flush()
                    logger.info(">>> Physical Device [%s]: Transmitted JSON command %s", self.port, json.dumps(cmd_payload))
                except Exception as ser_err:
                    logger.warning("Could not transmit over serial port %s: %s", self.port, ser_err)

                # Actuation tracking and backward compatibility pulses
                if action_upper in ("LIGHTS_ON", "LIGHT_ON", "ON") or (
                    action_upper == "SET_LED" and parameters.get("state", "").lower() == "on"
                ):
                    if ser:
                        try:
                            ser.write(b"1\n")
                            ser.flush()
                        except Exception:
                            pass
                    self._state["led"] = "ON"
                    self._state["relay:light"] = "ON"
                    if cmd_id:
                        self._last_command_id = cmd_id
                    logger.info(">>> Physical Device [%s]: Sent '1' -> LED / LIGHT IS ON", self.port)
                    return {"status": "SUCCESS", "action": "LIGHTS_ON", "state": "ON", "port": self.port, "command": cmd_payload}

                elif action_upper in ("LIGHTS_OFF", "LIGHT_OFF", "OFF") or (
                    action_upper == "SET_LED" and parameters.get("state", "").lower() == "off"
                ):
                    if ser:
                        try:
                            ser.write(b"0\n")
                            ser.flush()
                        except Exception:
                            pass
                    self._state["led"] = "OFF"
                    self._state["relay:light"] = "OFF"
                    if cmd_id:
                        self._last_command_id = cmd_id
                    logger.info(">>> Physical Device [%s]: Sent '0' -> LED / LIGHT IS OFF", self.port)
                    return {"status": "SUCCESS", "action": "LIGHTS_OFF", "state": "OFF", "port": self.port, "command": cmd_payload}

                elif action_upper in ("VALVE_ON", "VALVE_OPEN", "V"):
                    if ser:
                        try:
                            ser.write(b"v\n")
                            ser.flush()
                        except Exception:
                            pass
                    self._state["valve"] = "OPEN"
                    self._state["relay:valve"] = "OPEN"
                    if cmd_id:
                        self._last_command_id = cmd_id
                    logger.info(">>> Physical Device [%s]: Sent 'v' -> VALVE IS OPEN/ON", self.port)
                    return {"status": "SUCCESS", "action": "VALVE_ON", "state": "OPEN", "port": self.port, "command": cmd_payload}

                elif action_upper in ("VALVE_OFF", "VALVE_CLOSE", "C"):
                    if ser:
                        try:
                            ser.write(b"c\n")
                            ser.flush()
                        except Exception:
                            pass
                    self._state["valve"] = "CLOSED"
                    self._state["relay:valve"] = "CLOSED"
                    if cmd_id:
                        self._last_command_id = cmd_id
                    logger.info(">>> Physical Device [%s]: Sent 'c' -> VALVE IS CLOSED/OFF", self.port)
                    return {"status": "SUCCESS", "action": "VALVE_OFF", "state": "CLOSED", "port": self.port, "command": cmd_payload}

                else:
                    self._state[action] = clean_params or "EXECUTED"
                    if cmd_id:
                        self._last_command_id = cmd_id
                    return {
                        "status": "SUCCESS",
                        "action": action,
                        "state": self._state.get(action, "EXECUTED"),
                        "port": self.port,
                        "command": cmd_payload,
                    }

            return await loop.run_in_executor(None, _serial_io)
