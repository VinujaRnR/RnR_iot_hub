import pytest
from core.message_validator import validate_incoming_message
from core.router import CommandRouter, CommandRoutingError
from devices.device_manager import create_default_device_manager


@pytest.fixture
def router():
    manager = create_default_device_manager()
    return CommandRouter(hub_id="hub-01", device_manager=manager)


@pytest.mark.asyncio
async def test_route_successful_commands(router):
    payload = {
        "schema_version": 1,
        "message_id": "test-batch",
        "hub_id": "hub-01",
        "commands": [
            {
                "command_id": "c1",
                "device_id": "iot_001",
                "action": "set_led",
                "parameters": {"state": "on"},
            },
            {
                "command_id": "c2",
                "device_id": "iot_002",
                "action": "read_temperature",
                "parameters": {"unit": "C"},
            },
        ],
    }
    msg = validate_incoming_message(payload, expected_hub_id="hub-01")
    results = await router.route_message(msg)

    assert len(results) == 2
    assert results[0]["status"] == "success"
    assert results[0]["result"]["led_state"] == "on"

    assert results[1]["status"] == "success"
    assert results[1]["result"]["unit"] == "C"
    assert isinstance(results[1]["result"]["temperature"], (int, float))


@pytest.mark.asyncio
async def test_route_unknown_device_id(router):
    payload = {
        "schema_version": 1,
        "message_id": "unknown-dev-test",
        "hub_id": "hub-01",
        "commands": [
            {
                "command_id": "c3",
                "device_id": "iot_unknown_999",
                "action": "status",
                "parameters": {},
            }
        ],
    }
    msg = validate_incoming_message(payload, expected_hub_id="hub-01")
    results = await router.route_message(msg)

    assert len(results) == 1
    assert results[0]["status"] == "error"
    assert results[0]["error"] == "DEVICE_NOT_FOUND"


@pytest.mark.asyncio
async def test_route_unsupported_action(router):
    payload = {
        "schema_version": 1,
        "message_id": "unsupported-action-test",
        "hub_id": "hub-01",
        "commands": [
            {
                "command_id": "c4",
                "device_id": "iot_001",
                "action": "explode_device",
                "parameters": {},
            }
        ],
    }
    msg = validate_incoming_message(payload, expected_hub_id="hub-01")
    results = await router.route_message(msg)

    assert len(results) == 1
    assert results[0]["status"] == "error"
    assert results[0]["error"] == "ACTION_ERROR"


@pytest.mark.asyncio
async def test_route_hub_mismatch(router):
    payload = {
        "schema_version": 1,
        "message_id": "hub-mismatch",
        "hub_id": "other-hub",
        "commands": [
            {
                "command_id": "c5",
                "device_id": "iot_001",
                "action": "set_led",
                "parameters": {"state": "off"},
            }
        ],
    }
    # Validate without expected hub to create message object
    msg = validate_incoming_message(payload)
    with pytest.raises(CommandRoutingError, match="Hub ID mismatch"):
        await router.route_message(msg)
