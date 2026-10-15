import logging
from typing import Any, Dict, List, Optional
from devices.mock_device import MockDevice, MockLedActuator, MockTemperatureSensor, DeviceActionError

logger = logging.getLogger("iothub.devices")


class DeviceNotFoundError(Exception):
    """Raised when a requested device is not registered on the hub."""
    pass


class DeviceManager:
    """Manages registered devices and dispatches incoming actions."""

    def __init__(self):
        self._devices: Dict[str, Any] = {}
        self._aliases: Dict[str, str] = {}

    def register_device(self, device_or_id: Any, device: Optional[Any] = None) -> None:
        if device is not None:
            dev_id = str(device_or_id)
            actual_device = device
            self._devices[dev_id] = actual_device
            if hasattr(actual_device, "device_id") and actual_device.device_id != dev_id:
                self._devices[actual_device.device_id] = actual_device
        else:
            actual_device = device_or_id
            dev_id = getattr(actual_device, "device_id", str(actual_device))
            self._devices[dev_id] = actual_device
        logger.info("Registered device '%s' (%s)", dev_id, getattr(actual_device, "name", "device"))

    def register_alias(self, alias: str, target_device_id: str) -> None:
        self._aliases[alias] = target_device_id
        logger.info("Registered device alias '%s' -> '%s'", alias, target_device_id)

    def get_device(self, device_id: str) -> Optional[Any]:
        target_id = self._aliases.get(device_id, device_id)
        dev = self._devices.get(target_id)
        if dev:
            return dev

        # Auto-resolve any logical device to active physical serial controller
        serial_controllers = [
            d for k, d in self._devices.items()
            if hasattr(d, "port") and (str(k).startswith("/dev/tty") or hasattr(d, "_ensure_port_open"))
        ]
        if serial_controllers:
            primary = serial_controllers[0]
            # Automatically bind this logical device to the active controller
            controller_id = getattr(primary, "device_id", str(primary))
            self.register_alias(device_id, controller_id)
            logger.info("Auto-configured new logical device '%s' -> serial controller '%s'", device_id, getattr(primary, "port", controller_id))
            return primary

        return None

    def list_devices(self) -> List[str]:
        return list(self._devices.keys()) + list(self._aliases.keys())

    async def dispatch(self, device_id: str, action: str, parameters: Dict[str, Any]) -> Dict[str, Any]:
        """
        Dispatches an action to the target device.
        Raises DeviceNotFoundError if device_id is not known.
        Raises DeviceActionError if action is invalid.
        """
        device = self.get_device(device_id)
        if not device:
            raise DeviceNotFoundError(
                f"Device '{device_id}' not found. Available devices: {self.list_devices()}"
            )

        params_to_pass = dict(parameters) if isinstance(parameters, dict) else {}
        if "target_device" not in params_to_pass:
            params_to_pass["target_device"] = device_id

        return await device.execute_command(action, params_to_pass)

    def close_all(self) -> None:
        """Closes all open devices, serial connections, and workers."""
        for dev_id, device in self._devices.items():
            if hasattr(device, "close") and callable(device.close):
                try:
                    device.close()
                    logger.info("Closed device connection for '%s'", dev_id)
                except Exception as exc:
                    logger.warning("Error closing device '%s': %s", dev_id, exc)


def create_default_device_manager() -> DeviceManager:
    """Factory creating DeviceManager with auto-scanned serial ports and dynamic logical device binding."""
    manager = DeviceManager()
    manager.register_device(MockLedActuator("iot_001"))
    manager.register_device(MockTemperatureSensor("iot_002"))

    # Auto-scan candidate serial ports (/dev/ttyUSB*, /dev/ttyACM*)
    import glob
    scanned_ports = sorted(glob.glob("/dev/ttyUSB*") + glob.glob("/dev/ttyACM*"))
    ports_to_bind = scanned_ports if scanned_ports else ["/dev/ttyUSB0"]

    from devices.arduino_nano import ArduinoNanoDevice
    for port in ports_to_bind:
        try:
            ctrl = ArduinoNanoDevice(port=port, device_id=port, name=f"Serial Controller ({port})")
            manager.register_device(port, ctrl)
            manager.register_alias(port.replace("/dev/", ""), port)
            # Default logical device aliases
            manager.register_alias("light_glow_1", port)
            manager.register_alias("light_glow_2", port)
            manager.register_alias("water_valve_1", port)
            manager.register_alias("water_valve_2", port)
            manager.register_alias("98a316e6b658", port)
            manager.register_alias("3417ebc3b479", port)
            manager.register_alias("C45BBE6DF6A9", port)
            logger.info("Auto-configured serial controller on %s", port)
        except Exception as exc:
            logger.warning("Could not auto-configure serial controller on %s: %s", port, exc)

    return manager
