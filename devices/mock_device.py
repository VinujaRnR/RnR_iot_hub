import asyncio
import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger("iothub.devices")


class DeviceActionError(Exception):
    """Raised when an action fails or is unsupported on a device."""
    pass


class MockDevice:
    """Simulated IoT device that executes actions and returns JSON responses."""

    def __init__(
        self,
        device_id: str,
        name: str = "Mock Device",
        supported_actions: Optional[List[str]] = None,
        port: Optional[str] = None,
        simulated_latency: float = 0.0,
    ):
        self.device_id = device_id
        self.name = name
        self.supported_actions = supported_actions or []
        self.port = port
        self.simulated_latency = simulated_latency
        self.command_history: List[Dict[str, Any]] = []
        self.connected: bool = True
        self._state: Dict[str, Any] = {"online": True}

    def close(self) -> None:
        self.connected = False
        self._state["online"] = False

    async def execute_command(self, action: str, parameters: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute an action on this simulated device.
        Returns a JSON response dictionary.
        Raises DeviceActionError if action is not supported.
        """
        if self.supported_actions and action not in self.supported_actions:
            raise DeviceActionError(
                f"Unsupported action '{action}' on device '{self.device_id}'. "
                f"Supported actions: {self.supported_actions}"
            )

        if self.simulated_latency > 0:
            await asyncio.sleep(self.simulated_latency)

        self.command_history.append({
            "action": action,
            "parameters": parameters,
            "action_id": parameters.get("action_id"),
        })

        # Built-in handler for specific actions
        handler_name = f"_handle_{action}"
        handler = getattr(self, handler_name, None)

        if handler:
            data = await handler(parameters)
        else:
            # Generic simulated action response
            data = {"action_executed": action, "received_params": parameters}

        return {
            "status": "success",
            "device_id": self.device_id,
            "action": action,
            "result": data,
        }


class MockLedActuator(MockDevice):
    """Mock device for iot_001: LED actuator."""

    def __init__(self, device_id: str = "iot_001"):
        super().__init__(
            device_id=device_id,
            name="Smart LED Actuator",
            supported_actions=[
                "set_led", "get_status", "toggle",
                "LIGHTS_ON", "LIGHTS_OFF", "lights_on", "lights_off",
                "LIGHT_ON", "LIGHT_OFF", "light_on", "light_off",
            ],
        )
        self._state["led"] = "off"

    @property
    def led_state(self) -> str:
        return self._state.get("led", "off").upper()

    async def _handle_LIGHTS_ON(self, params: Dict[str, Any]) -> Dict[str, Any]:
        self._state["led"] = "on"
        return {"led_state": "ON"}

    async def _handle_LIGHT_ON(self, params: Dict[str, Any]) -> Dict[str, Any]:
        return await self._handle_LIGHTS_ON(params)

    async def _handle_LIGHTS_OFF(self, params: Dict[str, Any]) -> Dict[str, Any]:
        self._state["led"] = "off"
        return {"led_state": "OFF"}

    async def _handle_LIGHT_OFF(self, params: Dict[str, Any]) -> Dict[str, Any]:
        return await self._handle_LIGHTS_OFF(params)

    async def _handle_set_led(self, params: Dict[str, Any]) -> Dict[str, Any]:
        state = params.get("state", "off").lower()
        if state not in ("on", "off"):
            raise DeviceActionError(f"Invalid LED state '{state}'. Expected 'on' or 'off'")
        self._state["led"] = state
        return {"led_state": self._state["led"], "brightness": params.get("brightness", 100)}

    async def _handle_toggle(self, params: Dict[str, Any]) -> Dict[str, Any]:
        current = self._state.get("led", "off")
        self._state["led"] = "off" if current == "on" else "on"
        return {"led_state": self._state["led"]}

    async def _handle_get_status(self, params: Dict[str, Any]) -> Dict[str, Any]:
        return dict(self._state)


class MockTemperatureSensor(MockDevice):
    """Mock device for iot_002: Environmental sensor."""

    def __init__(self, device_id: str = "iot_002"):
        super().__init__(
            device_id=device_id,
            name="Environmental Temp/Humidity Sensor",
            supported_actions=["read_temperature", "read_humidity", "get_status"],
        )
        self._temperature_c = 24.5
        self._humidity_pct = 58.0

    async def _handle_read_temperature(self, params: Dict[str, Any]) -> Dict[str, Any]:
        unit = params.get("unit", "C").upper()
        temp = self._temperature_c
        if unit == "F":
            temp = (temp * 9 / 5) + 32
        return {"temperature": round(temp, 2), "unit": unit}

    async def _handle_read_humidity(self, params: Dict[str, Any]) -> Dict[str, Any]:
        return {"humidity_percentage": self._humidity_pct}

    async def _handle_get_status(self, params: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "online": True,
            "temperature_c": self._temperature_c,
            "humidity_pct": self._humidity_pct,
        }
