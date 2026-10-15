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
        # Fallback for dynamic named devices like light_glow_*, or MACs
        if (device_id.startswith("light_glow") or len(device_id) == 12) and "/dev/ttyUSB0" in self._devices:
            return self._devices.get("/dev/ttyUSB0")
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
    """Factory creating DeviceManager pre-populated with standard devices and physical Arduino Nano."""
    manager = DeviceManager()
    manager.register_device(MockLedActuator("iot_001"))
    manager.register_device(MockTemperatureSensor("iot_002"))

    # Register physical Arduino Nano on /dev/ttyUSB0
    try:
        from devices.arduino_nano import ArduinoNanoDevice
        nano = ArduinoNanoDevice(port="/dev/ttyUSB0", device_id="light_glow_1", name="light_glow_1")
        manager.register_device(nano)
        manager.register_alias("/dev/ttyUSB0", "light_glow_1")
        manager.register_alias("ttyUSB0", "light_glow_1")
        manager.register_alias("98a316e6b658", "light_glow_1")
        manager.register_alias("3417ebc3b479", "light_glow_1")
        manager.register_alias("C45BBE6DF6A9", "light_glow_1")
        manager.register_alias("light_glow_2", "light_glow_1")
    except Exception as exc:
        logger.warning("Could not register Arduino Nano: %s", exc)

    return manager
