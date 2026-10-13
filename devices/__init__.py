"""Devices module: USB detection, device registry and serial workers."""

from devices.device_manager import (
    DeviceManager,
    DeviceNotFoundError,
    create_default_device_manager,
)
from devices.mock_device import (
    MockDevice,
    MockLedActuator,
    MockTemperatureSensor,
    DeviceActionError,
)
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
from devices.usb_detector import USBDetector
from devices.serial_worker import SerialWorker

__all__ = [
    "DeviceManager",
    "DeviceNotFoundError",
    "create_default_device_manager",
    "MockDevice",
    "MockLedActuator",
    "MockTemperatureSensor",
    "DeviceActionError",
    "SerialTransport",
    "CommandMessage",
    "ExecutionResultMessage",
    "HandshakeMessage",
    "HelloMessage",
    "PingMessage",
    "PongMessage",
    "MessageType",
    "DeviceStatus",
    "DeviceProtocolError",
    "DeviceExecutionError",
    "DeviceTimeoutError",
    "DeviceNotConnectedError",
    "DeviceFramingError",
    "frame_message",
    "parse_line",
    "USBDetector",
    "SerialWorker",
]
