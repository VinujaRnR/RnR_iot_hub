import pytest
from core.message_validator import (
    validate_incoming_message,
    MessageValidationError,
    IoTMessage,
)


def test_valid_message():
    payload = {
        "schema_version": 1,
        "message_id": "msg-001",
        "hub_id": "hub-01",
        "commands": [
            {
                "command_id": "cmd-001",
                "device_id": "iot_001",
                "action": "set_led",
                "parameters": {"state": "on"},
            }
        ],
    }
    model = validate_incoming_message(payload, expected_hub_id="hub-01")
    assert isinstance(model, IoTMessage)
    assert model.message_id == "msg-001"
    assert len(model.commands) == 1
    assert model.commands[0].action == "set_led"


def test_malformed_json_string():
    with pytest.raises(MessageValidationError, match="Malformed JSON"):
        validate_incoming_message("NOT_VALID_JSON{:::}")


def test_unsupported_schema_version():
    payload = {
        "schema_version": 2,
        "message_id": "msg-002",
        "hub_id": "hub-01",
        "commands": [{"command_id": "c1", "device_id": "d1", "action": "a1"}],
    }
    with pytest.raises(MessageValidationError, match="Unsupported schema version"):
        validate_incoming_message(payload)


def test_incorrect_hub_id():
    payload = {
        "schema_version": 1,
        "message_id": "msg-003",
        "hub_id": "other-hub",
        "commands": [{"command_id": "c1", "device_id": "d1", "action": "a1"}],
    }
    with pytest.raises(MessageValidationError, match="Incorrect hub_id"):
        validate_incoming_message(payload, expected_hub_id="hub-01")


def test_missing_message_id():
    payload = {
        "schema_version": 1,
        "hub_id": "hub-01",
        "commands": [{"command_id": "c1", "device_id": "d1", "action": "a1"}],
    }
    with pytest.raises(MessageValidationError, match="message_id"):
        validate_incoming_message(payload)


def test_empty_commands_list():
    payload = {
        "schema_version": 1,
        "message_id": "msg-004",
        "hub_id": "hub-01",
        "commands": [],
    }
    with pytest.raises(MessageValidationError, match="commands"):
        validate_incoming_message(payload)


def test_too_many_commands():
    payload = {
        "schema_version": 1,
        "message_id": "msg-005",
        "hub_id": "hub-01",
        "commands": [
            {"command_id": f"c{i}", "device_id": "d1", "action": "a1"}
            for i in range(51)
        ],
    }
    with pytest.raises(MessageValidationError, match="commands"):
        validate_incoming_message(payload)
