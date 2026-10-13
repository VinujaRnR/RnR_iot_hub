"""
Automated PyTest Suite for Step 1 to Step 10 Verification using schedule.json.
File: tests/test_step1_to_step10.py
"""

from __future__ import annotations
import json
import os
import platform
import sys
from pathlib import Path
import pytest
import pytest_asyncio

from config import (
    HUB_ID,
    SCHEDULE_TIMEZONE,
    DATABASE_PATH,
    COMMAND_EXCHANGE,
    QUEUE_NAME,
    RESULTS_EXCHANGE,
    DEADLETTER_EXCHANGE,
)
from ingestion.schedule_validator import ScheduleValidator, MalformedScheduleError
from ingestion.schedule_normalizer import ScheduleNormalizer
from storage.database import DatabaseManager
from storage.schedule_repository import ScheduleRepository
from storage.event_outbox import EventOutbox
from devices.usb_detector import USBDetector
from devices.device_manager import create_default_device_manager
from scheduler.scheduler import TaskScheduler


SCHEDULE_PATH = Path(__file__).resolve().parent.parent / "schedule.json"
TARGET_PORT = "/dev/ttyUSB0"


@pytest.fixture(scope="module")
def schedule_raw_data():
    assert SCHEDULE_PATH.exists(), f"schedule.json must exist at {SCHEDULE_PATH}"
    with open(SCHEDULE_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def test_step1_ubuntu_pc_environment():
    """Step 1: Verify Ubuntu PC Environment and Python >= 3.11."""
    assert sys.version_info >= (3, 11), f"Python 3.11+ required, current: {platform.python_version()}"
    assert os.name == "posix", "Operating system must be Linux/POSIX"


def test_step2_project_architecture():
    """Step 2: Verify project architectural layout and modules."""
    root = SCHEDULE_PATH.parent
    for m in [
        "config.py",
        "main.py",
        "ingestion/schemas.py",
        "ingestion/schedule_validator.py",
        "ingestion/schedule_normalizer.py",
        "storage/database.py",
        "storage/schedule_repository.py",
        "storage/event_outbox.py",
        "devices/device_manager.py",
        "devices/arduino_nano.py",
        "scheduler/scheduler.py",
    ]:
        assert (root / m).exists(), f"Missing architectural component: {m}"


def test_step3_central_configuration():
    """Step 3: Verify centralized configuration system."""
    assert HUB_ID == "hub-01"
    assert SCHEDULE_TIMEZONE == "Asia/Kolkata"
    assert DATABASE_PATH == "iothub.db"
    assert RESULTS_EXCHANGE == "iot.events"


def test_step4_schedule_validation(schedule_raw_data):
    """Step 4: Verify schedule.json validation and malformed rejection."""
    validator = ScheduleValidator(expected_hub_id=HUB_ID)
    validated = validator.validate(schedule_raw_data)
    assert validated.schedule_id == schedule_raw_data.get("schedule_id", f"{HUB_ID}-{validated.date}")
    assert validated.hub_id == HUB_ID
    assert validated.date == schedule_raw_data["date"]

    # Negative test
    with pytest.raises(MalformedScheduleError):
        validator.validate({"hub_id": "other-hub", "date": "invalid"})


def test_step5_schedule_normalization(schedule_raw_data):
    """Step 5: Verify schedule normalizer extracts 16 actions with /dev/ttyUSB0."""
    normalizer = ScheduleNormalizer(default_hub_id=HUB_ID, default_timezone=SCHEDULE_TIMEZONE)
    actions = normalizer.normalize(schedule_raw_data)
    assert len(actions) == 16, f"Expected 16 actions, got {len(actions)}"
    for act in actions:
        assert act.iot_node_id == TARGET_PORT
        assert act.channel_key == "relay:light"
        assert act.action in ("LIGHTS_ON", "LIGHTS_OFF")


@pytest.mark.asyncio
async def test_step6_sqlite_persistence(schedule_raw_data, tmp_path):
    """Step 6: Verify SQLite atomic persistence and retrieval."""
    test_db_path = str(tmp_path / "test_step6.db")
    db = DatabaseManager(db_path=test_db_path)
    await db.connect()

    async with db.connection.execute("PRAGMA journal_mode") as cur:
        row = await cur.fetchone()
        assert row[0].lower() == "wal"

    repo = ScheduleRepository(db)
    normalizer = ScheduleNormalizer(default_hub_id=HUB_ID, default_timezone=SCHEDULE_TIMEZONE)
    actions = normalizer.normalize(schedule_raw_data)
    sched_id = schedule_raw_data.get("schedule_id", "hub-01-sched")
    sched_date = schedule_raw_data.get("date", "2026-10-12")

    await repo.save_schedule_with_actions(
        schedule_id=sched_id,
        date=sched_date,
        revision=1,
        actions=actions,
    )

    retrieved = await repo.get_scheduled_actions(sched_id)
    assert len(retrieved) == 16
    assert all(r["status"] == "PENDING" for r in retrieved)
    assert all(r["iot_node_id"] == TARGET_PORT for r in retrieved)
    await db.close()


@pytest.mark.asyncio
async def test_step7_rabbitmq_outbox_event(tmp_path):
    """Step 7: Verify acceptance event generation and durable outbox persistence."""
    test_db_path = str(tmp_path / "test_step7.db")
    db = DatabaseManager(db_path=test_db_path)
    await db.connect()
    outbox = EventOutbox(db)

    event_id = await outbox.enqueue(
        event_type="schedule.accepted",
        routing_key=f"hub.{HUB_ID}.schedule.accepted",
        payload={"schedule_id": "hub-01-acceptance", "actions": 16},
    )
    assert event_id > 0

    pending = await outbox.get_pending()
    assert len(pending) == 1
    assert pending[0]["event_type"] == "schedule.accepted"
    await db.close()


def test_step8_usb_device_registry():
    """Step 8: Verify device manager registry with /dev/ttyUSB0."""
    manager = create_default_device_manager()
    devices = manager.list_devices()
    assert TARGET_PORT in devices or "ttyUSB0" in devices
    dev = manager.get_device(TARGET_PORT)
    assert dev is not None


def test_step9_serial_transport_protocol():
    """Step 9: Verify USB serial protocol and baudrate configuration."""
    manager = create_default_device_manager()
    dev = manager.get_device(TARGET_PORT)
    assert dev.port == TARGET_PORT
    assert dev.baudrate == 9600


@pytest.mark.asyncio
async def test_step10_hardware_actions_dispatch():
    """Step 10: Verify physical hardware execution of LIGHTS_ON and LIGHTS_OFF."""
    manager = create_default_device_manager()
    if not os.path.exists(TARGET_PORT):
        pytest.skip(f"{TARGET_PORT} not connected on this environment")

    # Dispatch LIGHTS_ON
    res_on = await manager.dispatch(TARGET_PORT, "LIGHTS_ON", {})
    assert res_on.get("status") == "SUCCESS"
    assert res_on.get("state") == "ON"

    # Dispatch LIGHTS_OFF
    res_off = await manager.dispatch(TARGET_PORT, "LIGHTS_OFF", {})
    assert res_off.get("status") == "SUCCESS"
    assert res_off.get("state") == "OFF"

    dev = manager.get_device(TARGET_PORT)
    if hasattr(dev, "close"):
        dev.close()
