"""
Unit tests verifying the action execution result JSON format requested by the user:
{
  "device name": "light_glow_1",
  "device_name": "light_glow_1",
  "status": "successfull",
  "statue": "successfull",
  "action": "LIGHT_ON"
}
"""

import json
from unittest.mock import AsyncMock, patch
import pytest
import pytest_asyncio

from devices.device_manager import DeviceManager
from devices.mock_device import MockLedActuator
from ingestion.rabbitmq_publisher import RabbitMQPublisher
from scheduler.action_dispatcher import ActionDispatcher
from storage.database import DatabaseManager
from storage.event_outbox import EventOutbox
from storage.schedule_repository import ScheduleRepository


@pytest.mark.asyncio
async def test_action_result_json_outbox_and_publisher(tmp_path):
    db_path = str(tmp_path / "test_result_json.db")
    db = DatabaseManager(db_path=db_path)
    await db.connect()
    outbox = EventOutbox(db)
    publisher = RabbitMQPublisher(outbox=outbox, hub_id="hub-01")
    publisher.publish_event = AsyncMock(return_value=True)

    # 1. Direct publish_action_result test
    await publisher.publish_action_result(
        action_id="act-test-01",
        schedule_id="sched-test",
        device_id="light_glow_1",
        device_name="light_glow_1",
        action="LIGHT_ON",
        status="successfull",
    )

    publisher.publish_event.assert_called_once()
    payload = publisher.publish_event.call_args.kwargs["payload"]

    assert payload["device_name"] == "light_glow_1"
    assert payload["device name"] == "light_glow_1"
    assert payload["status"] == "successfull"
    assert payload["statue"] == "successfull"
    assert payload["action"] == "LIGHT_ON"

    await db.close()


@pytest.mark.asyncio
async def test_action_dispatcher_generates_user_json(tmp_path):
    db_path = str(tmp_path / "test_dispatcher_json.db")
    db = DatabaseManager(db_path=db_path)
    await db.connect()
    repo = ScheduleRepository(db)
    outbox = EventOutbox(db)
    publisher = RabbitMQPublisher(outbox=outbox, hub_id="hub-01")
    publisher.publish_event = AsyncMock(return_value=True)

    mgr = DeviceManager()
    led_mock = MockLedActuator("light_glow_1")
    mgr.register_device(led_mock)

    dispatcher = ActionDispatcher(
        device_manager=mgr,
        schedule_repo=repo,
        outbox=outbox,
        publisher=publisher,
    )

    action = {
        "schedule_id": "sched-user-test",
        "action_id": "act-user-01",
        "device_id": "light_glow_1",
        "device_name": "light_glow_1",
        "action": "LIGHT_ON",
        "scheduled_at": "2026-10-19T10:00:00+00:00",
        "params": {},
    }

    res = await dispatcher._execute_action("light_glow_1", action)
    assert res.get("status", "").lower() in ("success", "successfull")

    pending = await outbox.get_pending()
    assert len(pending) == 1
    event_payload = pending[0]["payload"]

    assert event_payload["device_name"] == "light_glow_1"
    assert event_payload["device name"] == "light_glow_1"
    assert event_payload["status"] == "successfull"
    assert event_payload["statue"] == "successfull"
    assert event_payload["action"] == "LIGHT_ON"

    await db.close()
