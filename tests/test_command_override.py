import asyncio
import json
import pytest
from unittest.mock import AsyncMock, MagicMock

from config import HUB_ID
from storage.database import DatabaseManager
from storage.schedule_repository import ScheduleRepository
from ingestion.rabbitmq_consumer import RabbitMQConsumer
from ingestion.normalizer import normalize_payload
from core.router import CommandRouter
from devices.device_manager import DeviceManager
from devices.mock_device import MockLedActuator


@pytest.mark.asyncio
async def test_direct_command_clears_schedules_and_executes_action(tmp_path):
    """
    Verifies that when a direct command JSON of the format:
    {
        "command": {
            "command_id": "057dff9a-d8cd-4961-bc38-d07fd0dd2574",
            "device_id": "light_glow_1",
            "actions": "LIGHT_ON",
            "params": {},
            "channel_key": "relay:light_glow",
            "device_type": "light_glow",
            "device_number": 1,
            "device_name": "light_glow #1"
        }
    }
    is received:
    1. It normalizes properly to canonical format.
    2. All saved schedules and scheduled actions in the database are cleared.
    3. The action is executed on target device (light_glow_1).
    4. The action_status table is updated.
    5. The RabbitMQ message is acknowledged.
    """
    raw_payload = {
        "command": {
            "command_id": "057dff9a-d8cd-4961-bc38-d07fd0dd2574",
            "device_id": "light_glow_1",
            "actions": "LIGHT_ON",
            "params": {},
            "channel_key": "relay:light_glow",
            "device_type": "light_glow",
            "device_number": 1,
            "device_name": "light_glow #1",
        }
    }

    # 1. Test normalization
    norm = normalize_payload(raw_payload, default_hub_id=HUB_ID)
    assert norm["schema_version"] == 1
    assert norm["hub_id"] == HUB_ID
    assert len(norm["commands"]) == 1
    cmd0 = norm["commands"][0]
    assert cmd0["command_id"] == "057dff9a-d8cd-4961-bc38-d07fd0dd2574"
    assert cmd0["device_id"] == "light_glow_1"
    assert cmd0["action"] == "LIGHT_ON"
    assert cmd0["parameters"]["device_name"] == "light_glow #1"

    # 2. Setup SQLite DB and populate pre-existing active schedule
    db_path = str(tmp_path / "test_cmd_override.db")
    db = DatabaseManager(db_path=db_path)
    await db.connect()
    repo = ScheduleRepository(db)

    # Save initial schedule
    sample_actions = [
        {
            "action_id": "act-old-1",
            "device_id": "light_glow_1",
            "channel_key": "relay:light_glow",
            "action": "LIGHT_OFF",
            "scheduled_time": "2026-10-29T14:00:00+05:30",
            "scheduled_time_utc": "2026-10-29T08:30:00Z",
            "priority": 1,
            "status": "PENDING",
            "raw_action_json": "{}",
        }
    ]
    await repo.save_schedule_with_actions(
        schedule_id="sched-pre-existing-123",
        date="2026-10-29",
        revision=1,
        actions=sample_actions,
    )

    # Ensure pre-existing schedule exists
    pre_scheds = await repo.list_schedules()
    assert len(pre_scheds) == 1
    pre_acts = await repo.get_scheduled_actions(schedule_id="sched-pre-existing-123")
    assert len(pre_acts) == 1

    # 3. Setup DeviceManager with Mock LED for light_glow_1
    dev_mgr = DeviceManager()
    led_mock = MockLedActuator("light_glow_1")
    dev_mgr.register_device("light_glow_1", led_mock)
    router = CommandRouter(hub_id=HUB_ID, device_manager=dev_mgr)

    # Mock ScheduleEngine & Publisher
    mock_schedule_engine = MagicMock()
    mock_publisher = MagicMock()
    mock_publisher.publish_action_result = AsyncMock()

    consumer = RabbitMQConsumer(
        router=router,
        db=db,
        schedule_repo=repo,
        publisher=mock_publisher,
        schedule_engine=mock_schedule_engine,
        hub_id=HUB_ID,
    )

    # 4. Construct mock incoming RabbitMQ message
    mock_msg = MagicMock()
    mock_msg.body = json.dumps(raw_payload).encode("utf-8")
    mock_msg.routing_key = "iot.command"
    mock_msg.ack = AsyncMock()
    mock_msg.reject = AsyncMock()
    mock_msg.nack = AsyncMock()

    # 5. Process message through consumer
    await consumer._on_message(mock_msg)

    # Verification:
    # A. All saved schedules are removed
    active_scheds = await repo.list_schedules()
    assert len(active_scheds) == 0, "All saved schedules must be removed"
    remaining_acts = await repo.get_scheduled_actions(schedule_id="sched-pre-existing-123")
    assert len(remaining_acts) == 0, "All scheduled actions must be removed"

    # B. ScheduleEngine was notified
    mock_schedule_engine.notify_schedule_updated.assert_called()

    # C. Device executed the action
    assert led_mock.led_state == "ON"

    # D. action_status table was updated
    status_rec = await repo.get_device_action_status("light_glow #1")
    if not status_rec:
        status_rec = await repo.get_device_action_status("light_glow_1")
    assert status_rec is not None
    assert status_rec["action"] == "LIGHT_ON"
    assert status_rec["status"] == "successfull"

    # E. Message was acknowledged (responsibility transfer)
    mock_msg.ack.assert_called_once()

    await db.close()


@pytest.mark.asyncio
async def test_flat_and_list_action_formats():
    """Verifies normalizer handles flat command with 'actions' list and singular key."""
    # Flat with 'actions'
    p1 = {
        "command_id": "cmd-001",
        "device_id": "light_glow_1",
        "actions": "LIGHT_ON",
        "params": {"test": 1},
        "device_name": "light_glow #1",
    }
    n1 = normalize_payload(p1, default_hub_id=HUB_ID)
    assert n1["commands"][0]["action"] == "LIGHT_ON"
    assert n1["commands"][0]["parameters"]["device_name"] == "light_glow #1"

    # With 'actions' as list
    p2 = {
        "command": {
            "command_id": "cmd-002",
            "device_id": "light_glow_1",
            "actions": ["LIGHT_ON"],
            "channel_key": "relay:light_glow",
        }
    }
    n2 = normalize_payload(p2, default_hub_id=HUB_ID)
    assert n2["commands"][0]["action"] == "LIGHT_ON"
    assert n2["commands"][0]["parameters"]["channel_key"] == "relay:light_glow"
