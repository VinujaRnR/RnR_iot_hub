import asyncio
import logging
from typing import Any, Dict, Optional

from devices.serial_transport import SerialTransport
from devices.device_protocol import CommandMessage, ExecutionResultMessage

logger = logging.getLogger("iothub.serial")


class SerialWorker:
    """Handles serial communication and command framing with microcontrollers (ESP32/Arduino)."""

    def __init__(self, port: str, baudrate: int = 115200, timeout: float = 2.0):
        self.port = port
        self.baudrate = baudrate
        self.timeout = timeout
        self.transport = SerialTransport(port=port, baudrate=baudrate, timeout=timeout)

    def connect(self) -> bool:
        """Opens serial port connection."""
        return self.transport.connect()

    def close(self) -> None:
        """Closes serial port connection."""
        self.transport.disconnect()

    async def send_command(
        self,
        action: str,
        parameters: Dict[str, Any],
        command_id: Optional[str] = None,
        channel_key: str = "default",
    ) -> Dict[str, Any]:
        """Sends a JSON-delimited command over serial and awaits JSON response."""
        cid = command_id or f"cmd-{id(parameters)}"
        cmd = CommandMessage(
            command_id=cid,
            action=action,
            channel_key=channel_key,
            params=parameters,
        )
        res = await self.transport.send_command(cmd, timeout=self.timeout)
        return {
            "status": res.status.lower(),
            "device_id": res.device_id,
            "command_id": res.command_id,
            "result": res.result,
            "error": res.error,
        }
