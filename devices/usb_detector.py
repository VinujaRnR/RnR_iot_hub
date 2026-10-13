import logging
from typing import Dict, List, Optional

logger = logging.getLogger("iothub.usb")


class USBDetector:
    """Detects connected USB serial hardware using pyudev."""

    def __init__(self):
        self._context = None
        try:
            import pyudev
            self._context = pyudev.Context()
        except Exception as exc:
            logger.warning("pyudev initialization failed or not available: %s", exc)

    def scan_serial_ports(self) -> List[Dict[str, str]]:
        """Scans system for connected USB serial adapters (e.g. ESP32, Arduino, CH340, CP210x, FTDI)."""
        ports: List[Dict[str, str]] = []
        if not self._context:
            return ports

        try:
            for device in self._context.list_devices(subsystem="tty"):
                dev_node = device.device_node
                if not dev_node:
                    continue

                # Filter for USB serial devices
                props = getattr(device, "properties", device)
                id_bus = props.get("ID_BUS")
                if id_bus == "usb" or "ttyUSB" in dev_node or "ttyACM" in dev_node:
                    ports.append(
                        {
                            "port": dev_node,
                            "vendor_id": props.get("ID_VENDOR_ID", "unknown"),
                            "model_id": props.get("ID_MODEL_ID", "unknown"),
                            "vendor": props.get("ID_VENDOR", "unknown"),
                            "model": props.get("ID_MODEL", "unknown"),
                            "serial_short": props.get("ID_SERIAL_SHORT", "unknown"),
                        }
                    )
        except Exception as exc:
            logger.error("Error scanning USB serial ports: %s", exc)

        return ports
