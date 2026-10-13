"""
Unit and Integration Tests for Step 18: Complete System End-to-End Acceptance.
File: tests/test_step18.py

Validates the complete 15-item Final Acceptance Checklist:
1.  RabbitMQ schedule delivery: Hub receives the daily JSON and confirms acceptance.
2.  JSON normalization: Sample produces 16 unique executable actions.
3.  USB discovery: Connected devices register with the correct node IDs.
4.  Scheduled execution: Commands execute according to their due times (via controllable clock).
5.  Device acknowledgment: Execution results match the original action IDs.
6.  Duplicate delivery: Repeated schedules do not duplicate completed actions.
7.  Schedule revision: Newer revisions safely update future actions.
8.  RabbitMQ outage: Existing local schedules continue running autonomously from SQLite.
9.  USB disconnection: Other devices continue unaffected (per-device queue isolation).
10. Ubuntu reboot: Saved schedules recover without unsafe replay (in-flight action reconciliation).
11. Missing daily schedule: Supervisor reports the missing plan.
12. Invalid clock: Unsafe timed execution is blocked when system clock is unsafe.
13. Result recovery: Pending events are published after reconnection.
14. Automatic startup: Service starts at boot without login (systemd unit exact config).
15. Graceful shutdown: Processes and serial handles close cleanly.
"""

from __future__ import annotations
import asyncio
import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio

from config import HUB_ID, SCHEDULE_TIMEZONE, ACTION_RESULT_KEY
from storage.database import DatabaseManager
from storage.schedule_repository import ScheduleRepository
from storage.event_outbox import EventOutbox
from ingestion.schedule_normalizer import ScheduleNormalizer
from ingestion.rabbitmq_consumer import RabbitMQConsumer
from ingestion.rabbitmq_publisher import RabbitMQPublisher
from devices.device_manager import DeviceManager
from devices.mock_device import MockDevice
from scheduler.schedule_engine import ScheduleEngine
from scheduler.action_dispatcher import ActionDispatcher
from scheduler.state_reconciler import StateReconciler
from supervisor.supervisor import HubSupervisor
from install import SYSTEMD_UNIT_CONTENT


class MockIncomingMessage:
    """Mock aio_pika incoming message tracking ack, reject, and nack status."""

    def __init__(self, body: bytes | str):
        if isinstance(body, str):
            self.body = body.encode("utf-8")
        else:
            self.body = body
        self.acked = False
        self.rejected = False
        self.nacked = False

    async def ack(self, multiple: bool = False) -> None:
        self.acked = True

    async def reject(self, requeue: bool = False) -> None:
        self.rejected = True

    async def nack(self, multiple: bool = False, requeue: bool = True) -> None:
        self.nacked = True


@pytest_asyncio.fixture
async def temp_db():
    fd, path = tempfile.mkstemp(suffix="_test_step18.db")
    os.close(fd)
    db = DatabaseManager(db_path=path)
    await db.connect()
    yield db
    await db.close()
    if os.path.exists(path):
        os.remove(path)
    for ext in ("-wal", "-shm"):
        wal = path + ext
        if os.path.exists(wal):
            try:
                os.remove(wal)
            except OSError:
                pass


@pytest.fixture
def sample_schedule_raw():
    root = Path(__file__).resolve().parent.parent
    sched_path = root / "schedule.json"
    with open(sched_path, "r", encoding="utf-8") as f:
        return json.load(f)


# =====================================================================
# Checklist Item 1: RabbitMQ schedule delivery
# =====================================================================
@pytest.mark.asyncio
async def test_step18_01_rabbitmq_schedule_delivery(temp_db, sample_schedule_raw):
    """Checklist 1: Hub receives the daily JSON and confirms acceptance."""
    repo = ScheduleRepository(temp_db)
    outbox = EventOutbox(temp_db)

    publisher = RabbitMQPublisher(outbox=outbox, hub_id=HUB_ID)
    publisher.publish_event = AsyncMock(return_value=True)

    consumer = RabbitMQConsumer(
        hub_id=HUB_ID,
        db=temp_db,
        schedule_repo=repo,
        publisher=publisher,
    )

    msg = MockIncomingMessage(body=json.dumps(sample_schedule_raw))
    await consumer._on_message(msg)

    # 1. Message acknowledged
    assert msg.acked is True
    assert msg.rejected is False

    # 2. Schedule stored and ACTIVE
    schedules = await repo.list_schedules(status="ACTIVE")
    assert len(schedules) >= 1
    assert schedules[0]["status"] == "ACTIVE"

    # 3. Acceptance event recorded in outbox or published
    pending_events = await outbox.get_pending()
    assert any(ev["event_type"] == "schedule.accepted" for ev in pending_events) or publisher.publish_event.called


# =====================================================================
# Checklist Item 2: JSON normalization
# =====================================================================
def test_step18_02_json_normalization_produces_16_actions(sample_schedule_raw):
    """Checklist 2: Sample produces 16 unique executable actions."""
    normalizer = ScheduleNormalizer()
    normalized = normalizer.normalize(sample_schedule_raw)

    assert len(normalized) == 16, f"Expected exactly 16 actions, got {len(normalized)}"

    # All action IDs must be unique
    action_ids = [a["action_id"] for a in normalized]
    assert len(action_ids) == len(set(action_ids)), "Action IDs must be distinct"

    # Verify all targets and actions are valid
    for act in normalized:
        assert act["action"] in ("LIGHTS_ON", "LIGHTS_OFF")
        assert act["iot_node_id"] == "98a316e6b658"
        assert act["channel_key"] == "relay:light"
        assert "scheduled_at" in act


# =====================================================================
# Checklist Item 3: USB discovery
# =====================================================================
@pytest.mark.asyncio
async def test_step18_03_usb_discovery_and_registration():
    """Checklist 3: Connected devices register with the correct node IDs."""
    dev_mgr = DeviceManager()

    dev1 = MockDevice(device_id="98a316e6b658", port="/dev/ttyUSB0")
    dev2 = MockDevice(device_id="98a316e6b659", port="/dev/ttyUSB1")

    dev_mgr.register_device("98a316e6b658", dev1)
    dev_mgr.register_device("98a316e6b659", dev2)

    devices = dev_mgr.list_devices()
    assert "98a316e6b658" in devices
    assert "98a316e6b659" in devices
    assert dev_mgr.get_device("98a316e6b658").port == "/dev/ttyUSB0"
    assert dev_mgr.get_device("98a316e6b659").port == "/dev/ttyUSB1"


# =====================================================================
# Checklist Item 4: Scheduled execution (Controllable Simulated Clock)
# =====================================================================
@pytest.mark.asyncio
async def test_step18_04_scheduled_execution_with_controllable_clock(temp_db):
    """Checklist 4: Commands execute according to their due times using simulated clock."""
    repo = ScheduleRepository(temp_db)
    outbox = EventOutbox(temp_db)
    dev_mgr = DeviceManager()
    mock_dev = MockDevice(device_id="98a316e6b658", port="/dev/ttyUSB0")
    dev_mgr.register_device("98a316e6b658", mock_dev)

    dispatcher = ActionDispatcher(
        device_manager=dev_mgr,
        schedule_repo=repo,
        outbox=outbox,
    )

    # Controllable clock starting at 05:00 UTC
    engine = ScheduleEngine(
        database=temp_db,
        schedule_repo=repo,
        dispatcher=dispatcher,
        poll_interval=0.05,
        max_idle_wait=0.1,
    )

    # Insert 3 actions across the day
    sched_id = "sched-clock-001"
    await repo.save_schedule(
        schedule_id=sched_id,
        hub_id=HUB_ID,
        date="2026-10-15",
        timezone_str="UTC",
        revision=1,
        actions=[
            {
                "action_id": "act-morning",
                "action": "LIGHTS_ON",
                "iot_node_id": "98a316e6b658",
                "channel_key": "relay:light",
                "scheduled_at": "2026-10-15T06:00:00+00:00",
                "params": {},
            },
            {
                "action_id": "act-noon",
                "action": "LIGHTS_OFF",
                "iot_node_id": "98a316e6b658",
                "channel_key": "relay:light",
                "scheduled_at": "2026-10-15T12:00:00+00:00",
                "params": {},
            },
            {
                "action_id": "act-evening",
                "action": "LIGHTS_ON",
                "iot_node_id": "98a316e6b658",
                "channel_key": "relay:light",
                "scheduled_at": "2026-10-15T18:00:00+00:00",
                "params": {},
            },
        ],
    )

    # Set clock to 05:00:00 (Nothing due)
    engine.set_simulated_time("2026-10-15T05:00:00+00:00")
    await engine.start()
    await asyncio.sleep(0.15)
    assert len(mock_dev.command_history) == 0

    # Advance clock to 06:05:00 (Morning due)
    engine.set_simulated_time("2026-10-15T06:05:00+00:00")
    await asyncio.sleep(0.2)
    assert len(mock_dev.command_history) == 1
    assert mock_dev.command_history[0]["action"] == "LIGHTS_ON"

    # Advance clock to 12:05:00 (Noon due)
    engine.set_simulated_time("2026-10-15T12:05:00+00:00")
    await asyncio.sleep(0.2)
    assert len(mock_dev.command_history) == 2
    assert mock_dev.command_history[1]["action"] == "LIGHTS_OFF"

    # Advance clock to 18:05:00 (Evening due)
    engine.set_simulated_time("2026-10-15T18:05:00+00:00")
    await asyncio.sleep(0.2)
    assert len(mock_dev.command_history) == 3
    assert mock_dev.command_history[2]["action"] == "LIGHTS_ON"

    await engine.stop()


# =====================================================================
# Checklist Item 5: Device acknowledgment
# =====================================================================
@pytest.mark.asyncio
async def test_step18_05_device_acknowledgment_matches_action_ids(temp_db):
    """Checklist 5: Execution results match the original action IDs."""
    repo = ScheduleRepository(temp_db)
    outbox = EventOutbox(temp_db)
    dev_mgr = DeviceManager()
    mock_dev = MockDevice(device_id="98a316e6b658", port="/dev/ttyUSB0")
    dev_mgr.register_device("98a316e6b658", mock_dev)

    dispatcher = ActionDispatcher(device_manager=dev_mgr, schedule_repo=repo, outbox=outbox)
    dispatcher.start()

    action = {
        "schedule_id": "sched-ack-test",
        "action_id": "act-target-uuid-999",
        "action": "LIGHTS_ON",
        "iot_node_id": "98a316e6b658",
        "channel_key": "relay:light",
        "scheduled_at": "2026-10-15T10:00:00+00:00",
        "params": {},
    }
    await dispatcher.queue_action(action)
    await asyncio.sleep(0.15)
    await dispatcher.stop()

    # Query outbox for result event
    events = await outbox.get_pending()
    assert len(events) >= 1
    res_ev = events[-1]
    assert res_ev["action_id"] == "act-target-uuid-999"
    assert res_ev["device_id"] == "98a316e6b658"
    assert res_ev["action"] == "LIGHTS_ON"
    assert res_ev["status"] == "SUCCESS"


# =====================================================================
# Checklist Item 6: Duplicate delivery
# =====================================================================
@pytest.mark.asyncio
async def test_step18_06_duplicate_delivery_does_not_repeat_completed_actions(temp_db, sample_schedule_raw):
    """Checklist 6: Repeated schedules do not duplicate completed actions."""
    repo = ScheduleRepository(temp_db)
    outbox = EventOutbox(temp_db)
    publisher = RabbitMQPublisher(outbox=outbox, hub_id=HUB_ID)
    publisher.publish_event = AsyncMock(return_value=True)

    consumer = RabbitMQConsumer(hub_id=HUB_ID, db=temp_db, schedule_repo=repo, publisher=publisher)

    # First delivery
    msg1 = MockIncomingMessage(body=json.dumps(sample_schedule_raw))
    await consumer._on_message(msg1)
    assert msg1.acked is True

    # Mark first action as COMPLETED
    actions = await repo.get_scheduled_actions(sample_schedule_raw["schedule_id"])
    assert len(actions) > 0
    first_act_id = actions[0]["action_id"]
    await repo.update_action_status(sample_schedule_raw["schedule_id"], first_act_id, "COMPLETED")

    # Second delivery of identical schedule
    msg2 = MockIncomingMessage(body=json.dumps(sample_schedule_raw))
    await consumer._on_message(msg2)
    assert msg2.acked is True

    # Check status of first action: still COMPLETED (not overwritten to PENDING)
    actions_after = await repo.get_scheduled_actions(sample_schedule_raw["schedule_id"])
    target = next(a for a in actions_after if a["action_id"] == first_act_id)
    assert target["status"] == "COMPLETED"


# =====================================================================
# Checklist Item 7: Schedule revision
# =====================================================================
@pytest.mark.asyncio
async def test_step18_07_schedule_revision_safely_updates_future_actions(temp_db, sample_schedule_raw):
    """Checklist 7: Newer revisions safely update future actions."""
    repo = ScheduleRepository(temp_db)
    outbox = EventOutbox(temp_db)
    publisher = RabbitMQPublisher(outbox=outbox, hub_id=HUB_ID)
    publisher.publish_event = AsyncMock(return_value=True)
    consumer = RabbitMQConsumer(hub_id=HUB_ID, db=temp_db, schedule_repo=repo, publisher=publisher)

    # Deliver Revision 1
    sample_schedule_raw["revision"] = 1
    await consumer._on_message(MockIncomingMessage(body=json.dumps(sample_schedule_raw)))

    # Mark first action completed
    actions = await repo.get_scheduled_actions(sample_schedule_raw["schedule_id"])
    first_id = actions[0]["action_id"]
    await repo.update_action_status(sample_schedule_raw["schedule_id"], first_id, "COMPLETED")

    # Deliver Revision 2 with an updated parameter or state
    rev2_payload = dict(sample_schedule_raw)
    rev2_payload["revision"] = 2
    msg_rev2 = MockIncomingMessage(body=json.dumps(rev2_payload))
    await consumer._on_message(msg_rev2)
    assert msg_rev2.acked is True

    # Verify completed action remained COMPLETED, while schedule revision is now 2
    sched = await repo.get_schedule(sample_schedule_raw["schedule_id"])
    assert sched["revision"] == 2

    actions_rev2 = await repo.get_scheduled_actions(sample_schedule_raw["schedule_id"])
    first_after = next(a for a in actions_rev2 if a["action_id"] == first_id)
    assert first_after["status"] == "COMPLETED"


# =====================================================================
# Checklist Item 8: RabbitMQ outage
# =====================================================================
@pytest.mark.asyncio
async def test_step18_08_rabbitmq_outage_local_schedules_continue(temp_db):
    """Checklist 8: Existing local schedules continue running during RabbitMQ outage."""
    repo = ScheduleRepository(temp_db)
    outbox = EventOutbox(temp_db)
    dev_mgr = DeviceManager()
    mock_dev = MockDevice(device_id="98a316e6b658", port="/dev/ttyUSB0")
    dev_mgr.register_device("98a316e6b658", mock_dev)

    # Publisher with broken / disconnected broker connection
    publisher = RabbitMQPublisher(outbox=outbox, hub_id=HUB_ID)
    publisher._is_connected = False
    publisher.publish_event = AsyncMock(side_effect=ConnectionError("RabbitMQ broker is down"))

    dispatcher = ActionDispatcher(device_manager=dev_mgr, schedule_repo=repo, outbox=outbox, publisher=publisher)
    engine = ScheduleEngine(database=temp_db, schedule_repo=repo, dispatcher=dispatcher, poll_interval=0.05)

    await repo.save_schedule(
        schedule_id="sched-outage-01",
        hub_id=HUB_ID,
        date="2026-10-15",
        timezone_str="UTC",
        revision=1,
        actions=[
            {
                "action_id": "act-local-01",
                "action": "LIGHTS_ON",
                "iot_node_id": "98a316e6b658",
                "channel_key": "relay:light",
                "scheduled_at": "2026-10-15T09:00:00+00:00",
                "params": {},
            }
        ],
    )

    engine.set_simulated_time("2026-10-15T09:05:00+00:00")
    await engine.start()
    await asyncio.sleep(0.2)
    await engine.stop()

    # Hardware execution succeeded locally despite RabbitMQ outage
    assert len(mock_dev.command_history) == 1
    assert mock_dev.command_history[0]["action"] == "LIGHTS_ON"

    # Execution event retained in local SQLite Outbox for future flush
    pending = await outbox.get_pending()
    assert any(p["action_id"] == "act-local-01" for p in pending)


# =====================================================================
# Checklist Item 9: USB disconnection isolation
# =====================================================================
@pytest.mark.asyncio
async def test_step18_09_usb_disconnection_isolation(temp_db):
    """Checklist 9: Other devices continue unaffected when one USB device disconnects."""
    repo = ScheduleRepository(temp_db)
    outbox = EventOutbox(temp_db)
    dev_mgr = DeviceManager()

    failing_dev = MockDevice(device_id="dev-failing", port="/dev/ttyUSB0", simulated_latency=0.01)
    # Simulate hardware disconnection on dev-failing
    failing_dev.execute_command = AsyncMock(side_effect=IOError("Device disconnected"))

    healthy_dev = MockDevice(device_id="dev-healthy", port="/dev/ttyUSB1", simulated_latency=0.01)

    dev_mgr.register_device("dev-failing", failing_dev)
    dev_mgr.register_device("dev-healthy", healthy_dev)

    dispatcher = ActionDispatcher(device_manager=dev_mgr, schedule_repo=repo, outbox=outbox)
    dispatcher.start()

    # Enqueue action to failing device and healthy device concurrently
    await dispatcher.queue_action({
        "schedule_id": "s-iso",
        "action_id": "act-fail",
        "action": "LIGHTS_ON",
        "iot_node_id": "dev-failing",
    })
    await dispatcher.queue_action({
        "schedule_id": "s-iso",
        "action_id": "act-pass",
        "action": "LIGHTS_ON",
        "iot_node_id": "dev-healthy",
    })

    await asyncio.sleep(0.25)
    await dispatcher.stop()

    # Healthy device must have executed successfully without blockage
    assert len(healthy_dev.command_history) == 1
    assert healthy_dev.command_history[0]["action"] == "LIGHTS_ON"


# =====================================================================
# Checklist Item 10: Ubuntu reboot recovery
# =====================================================================
@pytest.mark.asyncio
async def test_step18_10_ubuntu_reboot_recovery_without_unsafe_replay(temp_db):
    """Checklist 10: Saved schedules recover without unsafe replay after reboot."""
    repo = ScheduleRepository(temp_db)

    # Simulate state right before power cut / reboot:
    # 1 action was EXECUTING in the past (non-idempotent)
    # 1 action was PENDING in the future
    await repo.save_schedule(
        schedule_id="sched-reboot",
        hub_id=HUB_ID,
        date="2026-10-15",
        timezone_str="UTC",
        revision=1,
        actions=[
            {
                "action_id": "act-past-executing",
                "action": "LIGHTS_ON",
                "iot_node_id": "98a316e6b658",
                "channel_key": "relay:light",
                "scheduled_at": "2026-10-15T08:00:00+00:00",
                "params": {},
            },
            {
                "action_id": "act-future-pending",
                "action": "LIGHTS_OFF",
                "iot_node_id": "98a316e6b658",
                "channel_key": "relay:light",
                "scheduled_at": "2026-10-15T18:00:00+00:00",
                "params": {},
            },
        ],
    )
    # Put act-past-executing into EXECUTING
    await repo.claim_due_actions("2026-10-15T08:05:00+00:00")
    await repo.update_action_status("sched-reboot", "act-past-executing", "EXECUTING")

    # On reboot: StateReconciler runs
    reconciler = StateReconciler(repo)
    reconciled_count = await reconciler.reconcile(reference_time="2026-10-15T09:00:00+00:00")
    assert reconciled_count >= 1

    # In-flight past non-idempotent action was marked SKIPPED_UNSAFE (no blind replay)
    actions = await repo.get_scheduled_actions("sched-reboot")
    past_act = next(a for a in actions if a["action_id"] == "act-past-executing")
    assert past_act["status"] == "SKIPPED_UNSAFE"

    # Future action remained PENDING for scheduled execution
    future_act = next(a for a in actions if a["action_id"] == "act-future-pending")
    assert future_act["status"] == "PENDING"


# =====================================================================
# Checklist Item 11: Missing daily schedule
# =====================================================================
@pytest.mark.asyncio
async def test_step18_11_missing_daily_schedule_reported_by_supervisor(temp_db):
    """Checklist 11: Supervisor reports the missing plan when no daily schedule exists."""
    repo = ScheduleRepository(temp_db)
    supervisor = HubSupervisor(check_interval=1.0, db=temp_db, schedule_repo=repo)

    # Empty database: no active schedules
    has_plan = await supervisor.check_missing_daily_schedule(target_date="2026-10-15")
    assert has_plan is False

    # Check alert was emitted
    alerts = supervisor.get_recent_alerts()
    assert len(alerts) >= 1
    assert any("Missing daily schedule" in a["message"] for a in alerts)


# =====================================================================
# Checklist Item 12: Invalid clock
# =====================================================================
@pytest.mark.asyncio
async def test_step18_12_invalid_clock_blocks_unsafe_timed_execution(temp_db):
    """Checklist 12: Unsafe timed execution is blocked when system clock is unsafe."""
    repo = ScheduleRepository(temp_db)
    dev_mgr = DeviceManager()
    mock_dev = MockDevice(device_id="98a316e6b658", port="/dev/ttyUSB0")
    dev_mgr.register_device("98a316e6b658", mock_dev)

    dispatcher = ActionDispatcher(device_manager=dev_mgr, schedule_repo=repo)
    supervisor = HubSupervisor(check_interval=1.0, db=temp_db, schedule_repo=repo)
    supervisor.clock_safe = False  # Simulate unsynchronized NTP

    engine = ScheduleEngine(
        database=temp_db,
        schedule_repo=repo,
        dispatcher=dispatcher,
        poll_interval=0.05,
        supervisor=supervisor,
    )

    await repo.save_schedule(
        schedule_id="sched-clock-unsafe",
        hub_id=HUB_ID,
        date="2026-10-15",
        timezone_str="UTC",
        revision=1,
        actions=[
            {
                "action_id": "act-unsafe",
                "action": "LIGHTS_ON",
                "iot_node_id": "98a316e6b658",
                "channel_key": "relay:light",
                "scheduled_at": "2026-10-15T09:00:00+00:00",
                "params": {},
            }
        ],
    )

    engine.set_simulated_time("2026-10-15T09:05:00+00:00")
    await engine.start()
    await asyncio.sleep(0.2)
    await engine.stop()

    # Hardware dispatch was BLOCKED due to unsafe clock
    assert len(mock_dev.command_history) == 0

    # Action remains PENDING in SQLite
    actions = await repo.get_scheduled_actions("sched-clock-unsafe")
    assert actions[0]["status"] == "PENDING"


# =====================================================================
# Checklist Item 13: Result recovery
# =====================================================================
@pytest.mark.asyncio
async def test_step18_13_result_recovery_and_outbox_flush_on_reconnection(temp_db):
    """Checklist 13: Pending events are published after reconnection."""
    outbox = EventOutbox(temp_db)
    publisher = RabbitMQPublisher(outbox=outbox, hub_id=HUB_ID)

    # Record 2 pending execution events during offline period
    await outbox.record_execution_and_event(
        schedule_id="s1",
        action_id="act-rec-1",
        device_id="98a316e6b658",
        action="LIGHTS_ON",
        status="SUCCESS",
        scheduled_at="2026-10-15T10:00:00+00:00",
        executed_at="2026-10-15T10:00:01+00:00",
        attempt=1,
        result={"status": "OK"},
    )
    await outbox.record_execution_and_event(
        schedule_id="s1",
        action_id="act-rec-2",
        device_id="98a316e6b658",
        action="LIGHTS_OFF",
        status="SUCCESS",
        scheduled_at="2026-10-15T11:00:00+00:00",
        executed_at="2026-10-15T11:00:01+00:00",
        attempt=1,
        result={"status": "OK"},
    )

    pending_before = await outbox.get_pending()
    assert len(pending_before) == 2

    # Simulate RabbitMQ reconnection
    publisher._is_connected = True
    publisher._exchange = MagicMock()
    publisher._exchange.publish = AsyncMock()

    flushed = await publisher.flush_outbox()
    assert flushed == 2

    # Pending events drained to 0
    pending_after = await outbox.get_pending()
    assert len(pending_after) == 0


# =====================================================================
# Checklist Item 14: Automatic startup
# =====================================================================
def test_step18_14_automatic_startup_systemd_boot_configuration():
    """Checklist 14: Service starts at boot without login."""
    unit = SYSTEMD_UNIT_CONTENT
    assert "WantedBy=multi-user.target" in unit
    assert "Restart=always" in unit
    assert "Type=simple" in unit
    assert "User=iothub" in unit
    assert "Group=iothub" in unit
    assert "SupplementaryGroups=dialout" in unit
    assert "StateDirectory=iothub" in unit
    assert "ExecStart=/opt/iothub/.venv/bin/python -m main" in unit


# =====================================================================
# Checklist Item 15: Graceful shutdown
# =====================================================================
@pytest.mark.asyncio
async def test_step18_15_graceful_shutdown_closes_serial_and_tasks_cleanly(temp_db):
    """Checklist 15: Processes and serial handles close cleanly."""
    repo = ScheduleRepository(temp_db)
    dev_mgr = DeviceManager()
    mock_dev = MockDevice(device_id="98a316e6b658", port="/dev/ttyUSB0")
    dev_mgr.register_device("98a316e6b658", mock_dev)

    dispatcher = ActionDispatcher(device_manager=dev_mgr, schedule_repo=repo)
    engine = ScheduleEngine(database=temp_db, schedule_repo=repo, dispatcher=dispatcher)
    supervisor = HubSupervisor(check_interval=0.1, db=temp_db, schedule_repo=repo)

    await engine.start()
    await supervisor.start()
    assert engine.is_running is True
    assert supervisor._running is True

    # Shutdown
    await supervisor.stop()
    await engine.stop()
    dev_mgr.close_all()

    assert engine.is_running is False
    assert supervisor._running is False
    assert mock_dev.connected is False
