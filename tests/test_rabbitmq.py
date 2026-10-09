import json
from unittest.mock import AsyncMock, MagicMock
import pytest
from core.rabbitmq_consumer import RabbitMQConsumer
from core.router import CommandRouter
from devices.device_manager import create_default_device_manager


class FakeIncomingMessage:
    def __init__(self, body: bytes):
        self.body = body
        self.acked = False
        self.rejected = False

    def process(self, requeue: bool = False):
        class ProcessContext:
            def __init__(self, outer):
                self.outer = outer

            async def __aenter__(self):
                return self.outer

            async def __aexit__(self, exc_type, exc_val, exc_tb):
                if exc_type is None:
                    self.outer.acked = True
                else:
                    self.outer.rejected = True
                return False

        return ProcessContext(self)


@pytest.fixture
def consumer():
    router = CommandRouter(hub_id="hub-01", device_manager=create_default_device_manager())
    return RabbitMQConsumer(router=router)


@pytest.mark.asyncio
async def test_consumer_pipeline_valid_message(consumer):
    payload = {
        "schema_version": 1,
        "message_id": "test-msg-01",
        "hub_id": "hub-01",
        "commands": [
            {
                "command_id": "cmd-01",
                "device_id": "iot_001",
                "action": "set_led",
                "parameters": {"state": "on"},
            }
        ],
    }
    msg = FakeIncomingMessage(body=json.dumps(payload).encode("utf-8"))
    await consumer._on_message(msg)

    assert msg.acked is True
    # Verify device state changed
    led_dev = consumer.router.device_manager.get_device("iot_001")
    assert led_dev._state["led"] == "on"


@pytest.mark.asyncio
async def test_consumer_pipeline_invalid_schema(consumer):
    payload = {
        "schema_version": 99,
        "message_id": "test-msg-02",
        "hub_id": "hub-01",
        "commands": [],
    }
    msg = FakeIncomingMessage(body=json.dumps(payload).encode("utf-8"))
    await consumer._on_message(msg)

    # Caught gracefully and acknowledged/discarded
    assert msg.acked is True


@pytest.mark.asyncio
async def test_consumer_pipeline_malformed_json(consumer):
    msg = FakeIncomingMessage(body=b"NOT_JSON_BODY")
    await consumer._on_message(msg)

    assert msg.acked is True
