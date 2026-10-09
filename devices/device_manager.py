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
        self._devices: Dict[str, MockDevice] = {}

    def register_device(self, device: MockDevice) -> None:
        self._devices[device.device_id] = device
        logger.info("Registered device '%s' (%s)", device.device_id, device.name)

    def get_device(self, device_id: str) -> Optional[MockDevice]:
        return self._devices.get(device_id)

    def list_devices(self) -> List[str]:
        return list(self._devices.keys())

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

        return await device.execute_command(action, parameters)


def create_default_device_manager() -> DeviceManager:
    """Factory creating DeviceManager pre-populated with standard mock devices."""
    manager = DeviceManager()
    manager.register_device(MockLedActuator("iot_001"))
    manager.register_device(MockTemperatureSensor("iot_002"))
    return manager
