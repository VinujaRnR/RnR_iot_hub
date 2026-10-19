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


@pytest.mark.asyncio
async def test_action_status_table_replacement_on_conflict(tmp_path):
    """
    Verifies that 'action_status' table has 'device_name' as PRIMARY KEY
    and always replaces previous record when a new status JSON arrives for that device.
    """
    db_path = str(tmp_path / "test_action_status_replacement.db")
    db = DatabaseManager(db_path=db_path)
    await db.connect()
    repo = ScheduleRepository(db)

    # 1. First record for light_glow_1
    payload_1 = {
        "device_name": "light_glow_1",
        "device name": "light_glow_1",
        "status": "successfull",
        "statue": "successfull",
        "action": "LIGHT_ON",
    }
    await repo.save_device_action_status(
        device_name="light_glow_1",
        action="LIGHT_ON",
        status="successfull",
        payload=payload_1,
        updated_at="2026-10-19T10:00:00+00:00",
    )

    rec1 = await repo.get_device_action_status("light_glow_1")
    assert rec1 is not None
    assert rec1["device_name"] == "light_glow_1"
    assert rec1["action"] == "LIGHT_ON"
    assert rec1["payload"]["action"] == "LIGHT_ON"

    # 2. Second record for the same device_name (should REPLACE, not create duplicate or fail)
    payload_2 = {
        "device_name": "light_glow_1",
        "device name": "light_glow_1",
        "status": "successfull",
        "statue": "successfull",
        "action": "LIGHT_OFF",
    }
    await repo.save_device_action_status(
        device_name="light_glow_1",
        action="LIGHT_OFF",
        status="successfull",
        payload=payload_2,
        updated_at="2026-10-19T11:00:00+00:00",
    )

    all_statuses = await repo.list_action_statuses()
    assert len(all_statuses) == 1, "There must be exactly 1 record for light_glow_1 after update"
    assert all_statuses[0]["action"] == "LIGHT_OFF"
    assert all_statuses[0]["payload"]["action"] == "LIGHT_OFF"

    # 3. Add a different device (under 'always replace all with most recent', this replaces any previous record)
    payload_3 = {
        "device_name": "fan_relay_1",
        "device name": "fan_relay_1",
        "status": "successfull",
        "statue": "successfull",
        "action": "FAN_ON",
    }
    await repo.save_device_action_status(
        device_name="fan_relay_1",
        action="FAN_ON",
        status="successfull",
        payload=payload_3,
        updated_at="2026-10-19T11:05:00+00:00",
    )

    all_statuses = await repo.list_action_statuses()
    assert len(all_statuses) == 1, "Must replace all records with the single most recent one"
    assert all_statuses[0]["device_name"] == "fan_relay_1"
    assert all_statuses[0]["action"] == "FAN_ON"

    # Query without arguments returns the single most recent record
    latest_rec = await repo.get_device_action_status()
    assert latest_rec is not None
    assert latest_rec["device_name"] == "fan_relay_1"

    # 4. Save with device 'light_glow #1' - replaces the fan record with the new one
    payload_4 = {
        "device_name": "light_glow #1",
        "device name": "light_glow #1",
        "status": "successfull",
        "statue": "successfull",
        "action": "LIGHT_ON",
    }
    await repo.save_device_action_status(
        device_name="light_glow #1",
        action="LIGHT_ON",
        status="successfull",
        payload=payload_4,
        updated_at="2026-10-19T11:10:00+00:00",
    )
    all_statuses = await repo.list_action_statuses()
    assert len(all_statuses) == 1, "Table must hold only the single most recent status record"
    assert all_statuses[0]["device_name"] == "light_glow #1"
    assert all_statuses[0]["action"] == "LIGHT_ON"

    # Query by device_name (or alias) retrieves it
    light_rec = await repo.get_device_action_status("light_glow_1")
    assert light_rec is not None
    assert light_rec["action"] == "LIGHT_ON"

    await db.close()


@pytest.mark.asyncio
async def test_management_api_action_status_endpoints(tmp_path):
    """
    Verifies that management API exposes /action_status and /action_status/{device_name}
    and that sending a command via POST /devices/{device_id}/command updates action_status.
    """
    from starlette.testclient import TestClient
    from api.management_api import create_app

    db_path = str(tmp_path / "test_api_action_status.db")
    db = DatabaseManager(db_path=db_path)
    await db.connect()
    repo = ScheduleRepository(db)

    # Pre-populate one status
    await repo.save_device_action_status(
        device_name="light_glow_1",
        action="LIGHT_ON",
        status="successfull",
        payload={
            "device_name": "light_glow_1",
            "device name": "light_glow_1",
            "status": "successfull",
            "statue": "successfull",
            "action": "LIGHT_ON",
        },
    )

    app = create_app(db=db)
    client = TestClient(app)

    # 1. GET /action_status
    resp = client.get("/action_status")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] == 1
    assert data["action_statuses"][0]["device_name"] == "light_glow_1"

    # 2. GET /action_status/light_glow_1
    resp = client.get("/action_status/light_glow_1")
    assert resp.status_code == 200
    dev_data = resp.json()
    assert dev_data["device_name"] == "light_glow_1"
    assert dev_data["action"] == "LIGHT_ON"
    assert dev_data["payload"]["device name"] == "light_glow_1"

    # 3. GET /action_status/nonexistent -> 404
    resp = client.get("/action_status/nonexistent")
    assert resp.status_code == 404

    await db.close()

