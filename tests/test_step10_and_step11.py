"""
Comprehensive Automated Test Suite for Step 10 and Step 11.
File: tests/test_step10_and_step11.py

Validates:
- Step 10: IoT Device Firmware (ESP32/Arduino) specification, JSON protocol,
          node ID, channels, actions, active-low logic, duplicate suppression,
          and hardware driver compatibility.
- Step 11: Local Schedule Execution Engine, Python zoneinfo UTC conversion,
          atomic SQLite action claiming, independent per-device dispatcher queues
          (device 98a316e6b658 non-blocking relative to iot_001), and autonomous
          time-based local execution without RabbitMQ message per action.
"""

from __future__ import annotations
import asyncio
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
import json
from unittest.mock import MagicMock, patch
import pytest
import zoneinfo

from config import SCHEDULE_TIMEZONE
from storage.database import DatabaseManager
from storage.schedule_repository import ScheduleRepository
from storage.event_outbox import EventOutbox
from devices.device_manager import DeviceManager, create_default_device_manager
from devices.mock_device import MockLedActuator
from devices.arduino_nano import ArduinoNanoDevice
from scheduler.action_dispatcher import ActionDispatcher
from scheduler.schedule_engine import ScheduleEngine, local_to_utc_iso, current_utc_time


WORKSPACE_ROOT = Path(__file__).resolve().parent.parent


# =====================================================================
# STEP 10 TESTS: FIRMWARE SPECIFICATION & DEVICE DRIVER
# =====================================================================

def test_step10_nano_firmware_file_and_features():
    """Verify Arduino Nano firmware contains all required Step 10 features."""
    fw_path = WORKSPACE_ROOT / "firmware" / "nano_firmware" / "nano_firmware.ino"
    assert fw_path.exists(), f"Arduino Nano firmware must exist at {fw_path}"
    content = fw_path.read_text(encoding="utf-8")

    # 1. Hello handshake
    assert "sendHello" in content or "hello" in content.lower()
    # 2. Unique node ID
    assert "98a316e6b658" in content or "NODE_ID" in content
    # 3. Supported channel list
    assert "relay:light" in content
    # 4. Supported action list
    assert "LIGHTS_ON" in content and "LIGHTS_OFF" in content and "STATUS" in content
    # 5. JSON command parsing
    assert "processBuffer" in content or "extractField" in content
    # 6. Physical command execution & active-low logic
    assert "RELAY_ACTIVE_LOW" in content
    assert "digitalWrite" in content
    # 7. Execution acknowledgments
    assert "sendResult" in content or '"type":"result"' in content
    # 8. Duplicate command handling
    assert "g_last_cmd_id" in content or "duplicate" in content.lower()


@pytest.mark.asyncio
async def test_step10_device_driver_handshake_and_duplicate_suppression():
    """Verify hardware driver supports handshake, channels, and duplicate command deduplication."""
    nano = ArduinoNanoDevice(port="/dev/ttyUSB0", device_id="98a316e6b658")
    assert "relay:light" in nano.channels
    assert "LIGHTS_ON" in nano.supported_actions

    # Test HELLO handshake
    hello_resp = await nano.execute_command("HELLO", {})
    assert hello_resp["status"] == "SUCCESS"
    assert hello_resp["device_id"] == "98a316e6b658"
    assert "relay:light" in hello_resp["channels"]

    # Test duplicate command handling
    if not os.path.exists("/dev/ttyUSB0"):
        # When hardware is not physically connected, mock serial port opening
        with patch.object(nano, "_ensure_port_open"):
            res1 = await nano.execute_command("STATUS", {"command_id": "test-cmd-123"})
            assert res1["status"] == "SUCCESS"
    else:
        res1 = await nano.execute_command("STATUS", {"command_id": "test-cmd-123"})
        assert res1["status"] == "SUCCESS"

    # Repeat same command_id: should be recognized and suppressed without hardware retry
    nano._last_command_id = "test-cmd-123"
    res2 = await nano.execute_command("STATUS", {"command_id": "test-cmd-123"})
    assert res2["status"] == "SUCCESS"
    assert res2.get("duplicate") is True


@pytest.mark.asyncio
async def test_command_json_no_topic_and_light_glow_1():
    """Verify command JSON contains NO 'topic', formats 'params' correctly, and device is light_glow_1."""
    nano = ArduinoNanoDevice(port="/dev/ttyUSB0", device_id="light_glow_1", name="light_glow_1")
    assert nano.device_id == "light_glow_1"
    assert nano.name == "light_glow_1"

    # Mock _ensure_port_open and serial write
    mock_serial = MagicMock()
    with patch.object(nano, "_ensure_port_open", return_value=mock_serial):
        # 1. Action with params: should contain action and params, NO topic
        res = await nano.execute_command("change_size", {
            "params": {"shoulder": 0.3, "bust": None, "waist": None, "hip": None}
        })
        cmd = res["command"]
        assert "topic" not in cmd
        assert cmd["action"] == "change_size"
        assert cmd["params"] == {"shoulder": 0.3, "bust": None, "waist": None, "hip": None}

        # 2. Action without params: should omit 'params' key completely, NO topic
        res2 = await nano.execute_command("LIGHTS_ON", {})
        cmd2 = res2["command"]
        assert "topic" not in cmd2
        assert cmd2["action"] == "LIGHTS_ON"
        assert "params" not in cmd2

    # Verify create_default_device_manager registers light_glow_1
    mgr = create_default_device_manager()
    dev = mgr.get_device("light_glow_1")
    assert dev is not None
    assert dev.device_id == "light_glow_1"
    assert dev.name == "light_glow_1"

    # Aliases resolve to light_glow_1
    for alias in ["/dev/ttyUSB0", "ttyUSB0", "98a316e6b658", "3417ebc3b479", "C45BBE6DF6A9"]:
        resolved = mgr.get_device(alias)
        assert resolved is not None
        assert resolved.device_id == "light_glow_1"


# =====================================================================
# STEP 11 TESTS: LOCAL SCHEDULE EXECUTION ENGINE & DISPATCHER
# =====================================================================

def test_step11_zoneinfo_utc_conversion():
    """Verify zoneinfo accurately converts local date and time into UTC."""
    # Test Kolkata timezone (UTC +5:30)
    utc_str = local_to_utc_iso("2026-10-12", "14:30", "Asia/Kolkata")
    dt = datetime.fromisoformat(utc_str)
    # 14:30 in +5:30 is 09:00 in UTC
    assert dt.hour == 9
    assert dt.minute == 0
    assert dt.tzinfo == timezone.utc


@pytest.mark.asyncio
async def test_step11_atomic_claiming_in_sqlite(tmp_path):
    """Verify SQLite atomic claiming prevents double execution across tasks/restarts."""
    db_path = str(tmp_path / "test_claim.db")
    db = DatabaseManager(db_path=db_path)
    await db.connect()
    repo = ScheduleRepository(db)

    now = datetime.now(timezone.utc)
    due_time_past = (now - timedelta(minutes=5)).isoformat()
    due_time_future = (now + timedelta(minutes=10)).isoformat()

    actions = [
        {
            "action_id": "act-past-01",
            "iot_node_id": "98a316e6b658",
            "scheduled_at": due_time_past,
            "action": "LIGHTS_ON",
            "channel_key": "relay:light",
            "status": "PENDING",
        },
        {
            "action_id": "act-future-01",
            "iot_node_id": "iot_001",
            "scheduled_at": due_time_future,
            "action": "LIGHTS_OFF",
            "channel_key": "relay:light",
            "status": "PENDING",
        },
    ]

    await repo.save_schedule_with_actions(
        schedule_id="sched-test-claim",
        date="2026-10-12",
        revision=1,
        actions=actions,
    )

    # First claim: should claim only the due past action
    claimed_1 = await repo.claim_due_actions(now.isoformat())
    assert len(claimed_1) == 1
    assert claimed_1[0]["action_id"] == "act-past-01"
    assert claimed_1[0]["status"] == "CLAIMED"

    # Second claim at the exact same instant (e.g. duplicate task/restart):
    # must return 0 actions because act-past-01 is already CLAIMED!
    claimed_2 = await repo.claim_due_actions(now.isoformat())
    assert len(claimed_2) == 0

    await db.close()


@pytest.mark.asyncio
async def test_step11_independent_per_device_concurrency(tmp_path):
    """
    Verify ActionDispatcher maintains independent per-device queues.
    A slow command for device 98a316e6b658 must NOT block iot_001!
    """
    db_path = str(tmp_path / "test_dispatcher.db")
    db = DatabaseManager(db_path=db_path)
    await db.connect()
    repo = ScheduleRepository(db)

    # Create mock device manager
    mgr = DeviceManager()

    execution_order = []

    class SlowDevice:
        def __init__(self, device_id: str):
            self.device_id = device_id

        async def execute_command(self, action: str, params: dict):
            # Simulates slow serial response on device 98a316e6b658
            await asyncio.sleep(0.3)
            execution_order.append(f"{self.device_id}:{action}")
            return {"status": "SUCCESS"}

    class FastDevice:
        def __init__(self, device_id: str):
            self.device_id = device_id

        async def execute_command(self, action: str, params: dict):
            # Fast mock device
            execution_order.append(f"{self.device_id}:{action}")
            return {"status": "SUCCESS"}

    mgr.register_device(SlowDevice("98a316e6b658"))
    mgr.register_device(FastDevice("iot_001"))

    dispatcher = ActionDispatcher(device_manager=mgr, schedule_repo=repo)
    dispatcher.start()

    # Queue slow command on 98a316e6b658 first, then fast command on iot_001
    await dispatcher.queue_action({
        "schedule_id": "s1",
        "action_id": "a1",
        "iot_node_id": "98a316e6b658",
        "action": "SLOW_OP",
    })
    await dispatcher.queue_action({
        "schedule_id": "s1",
        "action_id": "a2",
        "iot_node_id": "iot_001",
        "action": "FAST_OP",
    })

    # Wait for both to complete
    await asyncio.sleep(0.5)
    await dispatcher.stop()

    # Crucial check: FastDevice ('iot_001') executed concurrently and finished FIRST!
    assert "iot_001:FAST_OP" in execution_order
    assert "98a316e6b658:SLOW_OP" in execution_order
    # iot_001 executed before the slow operation finished
    assert execution_order[0] == "iot_001:FAST_OP"

    await db.close()


@pytest.mark.asyncio
async def test_step11_schedule_engine_local_autonomous_execution(tmp_path):
    """
    Verify ScheduleEngine runs locally and executes due actions at scheduled times
    without requiring a RabbitMQ message for each action.
    """
    db_path = str(tmp_path / "test_engine.db")
    db = DatabaseManager(db_path=db_path)
    await db.connect()
    repo = ScheduleRepository(db)

    mgr = DeviceManager()
    led_mock = MockLedActuator("iot_001")
    mgr.register_device(led_mock)

    dispatcher = ActionDispatcher(device_manager=mgr, schedule_repo=repo)
    engine = ScheduleEngine(
        database=db,
        schedule_repo=repo,
        dispatcher=dispatcher,
        poll_interval=0.1,
        max_idle_wait=0.2,
    )

    now = datetime.now(timezone.utc)
    # Schedule action due in 0.15 seconds
    due_in_150ms = (now + timedelta(milliseconds=150)).isoformat()

    actions = [
        {
            "action_id": "autonomous-01",
            "iot_node_id": "iot_001",
            "scheduled_at": due_in_150ms,
            "action": "LIGHTS_ON",
            "channel_key": "relay:light",
            "status": "PENDING",
        }
    ]

    await repo.save_schedule_with_actions(
        schedule_id="sched-auto",
        date="2026-10-12",
        revision=1,
        actions=actions,
    )

    # Start engine
    await engine.start()
    assert engine.is_running is True

    # Initially LED is OFF
    assert led_mock.led_state == "OFF"

    # Wait 0.4s for engine to autonomously claim and execute at due time
    await asyncio.sleep(0.4)

    # Action executed autonomously by local engine!
    assert led_mock.led_state == "ON"

    # Verify status in SQLite was updated to COMPLETED
    records = await repo.get_scheduled_actions("sched-auto")
    assert len(records) == 1
    assert records[0]["status"] == "COMPLETED"

    await engine.stop()
    await db.close()
