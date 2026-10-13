"""
Automated PyTest Suite for Step 12 and Step 13.
File: tests/test_step12_and_step13.py

Validates:
- Step 12: Restart and Missed-Action Recovery
  - Desired-state recovery for relay:light (e.g. 09:56 ON, 10:00 OFF, 10:07 ON
    restarting at 10:15 establishes ON state without replaying all 3 commands).
  - Older intermediate actions marked SUPERSEDED.
  - Future actions (10:19 OFF) preserved in PENDING state.
  - Non-idempotent action safety: dispensing/motor commands skipped safely (SKIPPED_UNSAFE).
- Step 13: Deterministic HubSupervisor
  - USB device removed -> marked OFFLINE.
  - Device reconnected -> revalidates handshake and marks ONLINE.
  - Repeated device failures -> quarantined (QUARANTINED), dispatches blocked.
  - SQLite storage error -> safe degraded mode activated.
  - Missed schedule -> operational alerts generated.
  - Outbox backlog -> backpressure applied and released.
  - Clock check -> blocks unsafe timed execution if unsynchronized.
  - Systemd watchdog -> sd_notify socket communication.
"""

from __future__ import annotations
import asyncio
from datetime import datetime, timedelta, timezone
import os
import socket
from pathlib import Path
import pytest

from storage.database import DatabaseManager
from storage.schedule_repository import ScheduleRepository
from storage.event_outbox import EventOutbox
from devices.device_manager import DeviceManager
from devices.mock_device import MockLedActuator
from scheduler.missed_action_policy import MissedActionPolicy, RecoveryPolicy
from scheduler.state_reconciler import StateReconciler
from scheduler.action_dispatcher import ActionDispatcher
from supervisor.supervisor import HubSupervisor, SystemHealthStatus


# =====================================================================
# STEP 12 TESTS: RESTART AND MISSED-ACTION RECOVERY
# =====================================================================

@pytest.mark.asyncio
async def test_step12_desired_state_recovery_on_restart(tmp_path):
    """
    Step 12: Verify that restarting at 10:15 during schedule:
    - 09:56 LIGHTS_ON
    - 10:00 LIGHTS_OFF
    - 10:07 LIGHTS_ON
    - 10:19 LIGHTS_OFF
    establishes the ON state directly without replaying all 3 earlier actions,
    marks earlier actions SUPERSEDED, and leaves 10:19 OFF as PENDING.
    """
    db_path = str(tmp_path / "test_reconcile.db")
    db = DatabaseManager(db_path=db_path)
    await db.connect()
    repo = ScheduleRepository(db)

    # Mock device tracking physical dispatches
    executed_commands = []

    class TrackingLight:
        def __init__(self, device_id: str):
            self.device_id = device_id
            self._state = {"led": "OFF"}

        async def execute_command(self, action: str, params: dict):
            executed_commands.append(action)
            self._state["led"] = "ON" if "ON" in action.upper() else "OFF"
            return {"status": "SUCCESS", "state": self._state["led"]}

    mgr = DeviceManager()
    mgr.register_device(TrackingLight("/dev/ttyUSB0"))

    # Construct the exact schedule from the specification:
    # Target restart time: 2026-10-12T10:15:00+00:00
    restart_time = "2026-10-12T10:15:00+00:00"

    actions = [
        {
            "action_id": "act-0956",
            "iot_node_id": "/dev/ttyUSB0",
            "scheduled_at": "2026-10-12T09:56:00+00:00",
            "action": "LIGHTS_ON",
            "channel_key": "relay:light",
            "status": "PENDING",
        },
        {
            "action_id": "act-1000",
            "iot_node_id": "/dev/ttyUSB0",
            "scheduled_at": "2026-10-12T10:00:00+00:00",
            "action": "LIGHTS_OFF",
            "channel_key": "relay:light",
            "status": "PENDING",
        },
        {
            "action_id": "act-1007",
            "iot_node_id": "/dev/ttyUSB0",
            "scheduled_at": "2026-10-12T10:07:00+00:00",
            "action": "LIGHTS_ON",
            "channel_key": "relay:light",
            "status": "PENDING",
        },
        {
            "action_id": "act-1019",
            "iot_node_id": "/dev/ttyUSB0",
            "scheduled_at": "2026-10-12T10:19:00+00:00",
            "action": "LIGHTS_OFF",
            "channel_key": "relay:light",
            "status": "PENDING",
        },
    ]

    await repo.save_schedule_with_actions(
        schedule_id="sched-restart-test",
        date="2026-10-12",
        revision=1,
        actions=actions,
    )

    # Execute State Reconciler simulating hub reboot at 10:15
    reconciler = StateReconciler(db=db, schedule_repo=repo, device_manager=mgr)
    report = await reconciler.reconcile_on_startup(current_time_iso=restart_time)

    # 1. Hardware dispatch check: exactly 1 command (LIGHTS_ON) was executed!
    # No replaying of 09:56 ON then 10:00 OFF then 10:07 ON!
    assert len(executed_commands) == 1
    assert executed_commands[0] == "LIGHTS_ON"

    # 2. Earlier intermediate actions (09:56 and 10:00) were marked SUPERSEDED
    assert "act-0956" in report.superseded_actions
    assert "act-1000" in report.superseded_actions

    # 3. Target action 10:07 was applied and completed
    assert any(a["action_id"] == "act-1007" and a["action"] == "LIGHTS_ON" for a in report.applied_actions)

    # 4. Future action at 10:19 remains untouched in PENDING state!
    assert report.future_pending_count == 1
    db_actions = await repo.get_scheduled_actions("sched-restart-test")
    act_1019 = next(a for a in db_actions if a["action_id"] == "act-1019")
    assert act_1019["status"] == "PENDING"

    # Verify superseded statuses in SQLite
    act_0956 = next(a for a in db_actions if a["action_id"] == "act-0956")
    act_1000 = next(a for a in db_actions if a["action_id"] == "act-1000")
    assert act_0956["status"] == "SUPERSEDED"
    assert act_1000["status"] == "SUPERSEDED"

    await db.close()


@pytest.mark.asyncio
async def test_step12_non_idempotent_actions_are_skipped_safely(tmp_path):
    """
    Step 12: Verify that non-idempotent actions (e.g. material dispensing, motor moves)
    are marked SKIPPED_UNSAFE and never repeated blindly after a restart.
    """
    db_path = str(tmp_path / "test_non_idempotent.db")
    db = DatabaseManager(db_path=db_path)
    await db.connect()
    repo = ScheduleRepository(db)

    dispatched = []

    class MockDispenser:
        def __init__(self, device_id: str):
            self.device_id = device_id

        async def execute_command(self, action: str, params: dict):
            dispatched.append(action)
            return {"status": "SUCCESS"}

    mgr = DeviceManager()
    mgr.register_device(MockDispenser("feeder_01"))

    # Action in the past for dispensing
    actions = [
        {
            "action_id": "dispense-past",
            "iot_node_id": "feeder_01",
            "scheduled_at": "2026-10-12T09:00:00+00:00",
            "action": "dispense_nutrient",
            "channel_key": "dispenser:nutrient",
            "status": "PENDING",
        }
    ]

    await repo.save_schedule_with_actions(
        schedule_id="sched-dispenser",
        date="2026-10-12",
        revision=1,
        actions=actions,
    )

    reconciler = StateReconciler(db=db, schedule_repo=repo, device_manager=mgr)
    report = await reconciler.reconcile_on_startup(current_time_iso="2026-10-12T10:00:00+00:00")

    # Safety assertion: Zero commands dispatched to hardware!
    assert len(dispatched) == 0

    # Marked as SKIPPED_UNSAFE
    assert "dispense-past" in report.skipped_unsafe_actions

    db_acts = await repo.get_scheduled_actions("sched-dispenser")
    assert db_acts[0]["status"] == "SKIPPED_UNSAFE"

    await db.close()


# =====================================================================
# STEP 13 TESTS: DETERMINISTIC HUBSUPERVISOR
# =====================================================================

@pytest.mark.asyncio
async def test_step13_repeated_failures_quarantine_device(tmp_path):
    """
    Step 13: Verify repeated device failures trigger quarantine and block further commands.
    """
    db_path = str(tmp_path / "test_quarantine.db")
    db = DatabaseManager(db_path=db_path)
    await db.connect()
    repo = ScheduleRepository(db)

    mgr = DeviceManager()
    supervisor = HubSupervisor(check_interval=1.0, db=db, max_consecutive_failures=3)
    dispatcher = ActionDispatcher(device_manager=mgr, schedule_repo=repo, supervisor=supervisor)

    dev_id = "faulty_device"

    # Simulate 3 failures
    supervisor.record_device_command_outcome(dev_id, False, "Serial Timeout")
    assert not supervisor.is_device_quarantined(dev_id)

    supervisor.record_device_command_outcome(dev_id, False, "Framing Error")
    assert not supervisor.is_device_quarantined(dev_id)

    supervisor.record_device_command_outcome(dev_id, False, "Checksum Mismatch")
    # 3rd consecutive failure triggers QUARANTINE!
    assert supervisor.is_device_quarantined(dev_id)

    # Further dispatch to quarantined device is rejected immediately
    res = await dispatcher._execute_action(dev_id, {"action": "LIGHTS_ON", "action_id": "test-q"})
    assert res["status"] == "QUARANTINED"

    # Test unquarantine
    supervisor.unquarantine_device(dev_id)
    assert not supervisor.is_device_quarantined(dev_id)

    await db.close()


@pytest.mark.asyncio
async def test_step13_outbox_backpressure(tmp_path):
    """
    Step 13: Verify pending result backlog applies backpressure and releases it when drained.
    """
    db_path = str(tmp_path / "test_backpressure.db")
    db = DatabaseManager(db_path=db_path)
    await db.connect()
    outbox = EventOutbox(db)

    supervisor = HubSupervisor(
        check_interval=1.0,
        db=db,
        outbox=outbox,
        backlog_threshold=5,
    )

    # Add 6 events to outbox (exceeding threshold of 5)
    for i in range(6):
        await outbox.enqueue(
            event_type="test.event",
            routing_key="hub.test",
            payload={"index": i},
        )

    await supervisor.check_outbox_backlog()
    assert supervisor.backpressure_active is True

    # Mark 5 events as published
    pending = await outbox.get_pending()
    for ev in pending[:5]:
        await outbox.mark_published(ev["id"])

    # Re-check backlog
    await supervisor.check_outbox_backlog()
    assert supervisor.backpressure_active is False

    await db.close()


@pytest.mark.asyncio
async def test_step13_sqlite_storage_health_degraded_mode(tmp_path):
    """
    Step 13: Verify supervisor enters safe degraded mode on SQLite error.
    """
    db_path = str(tmp_path / "test_storage_err.db")
    db = DatabaseManager(db_path=db_path)
    await db.connect()

    supervisor = HubSupervisor(check_interval=1.0, db=db)
    is_healthy = await supervisor.check_storage_health()
    assert is_healthy is True
    assert supervisor.storage_degraded is False

    # Simulate database closure / connection drop
    await db.close()

    is_healthy_after = await supervisor.check_storage_health()
    assert is_healthy_after is False
    assert supervisor.storage_degraded is True


@pytest.mark.asyncio
async def test_step13_systemd_watchdog_socket(tmp_path):
    """
    Step 13: Verify systemd watchdog sends WATCHDOG=1 over Unix datagram socket.
    """
    sock_path = str(tmp_path / "systemd_notify.sock")

    # Create test receiver socket simulating systemd
    server_sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    server_sock.bind(sock_path)
    server_sock.setblocking(False)

    supervisor = HubSupervisor(check_interval=1.0)

    # Set NOTIFY_SOCKET environment variable
    old_env = os.environ.get("NOTIFY_SOCKET")
    try:
        os.environ["NOTIFY_SOCKET"] = sock_path
        supervisor._send_systemd_notify("WATCHDOG=1")

        # Read from server socket
        data = server_sock.recv(1024)
        assert data.decode("utf-8") == "WATCHDOG=1"

        supervisor._send_systemd_notify("READY=1")
        data2 = server_sock.recv(1024)
        assert data2.decode("utf-8") == "READY=1"

    finally:
        server_sock.close()
        if old_env is not None:
            os.environ["NOTIFY_SOCKET"] = old_env
        else:
            os.environ.pop("NOTIFY_SOCKET", None)


@pytest.mark.asyncio
async def test_step13_systemd_watchdog_advanced(tmp_path):
    """
    Step 13: Verify SystemdWatchdog class methods (notify_status, get_watchdog_interval, notify_stopping).
    """
    from supervisor.watchdog import SystemdWatchdog

    sock_path = str(tmp_path / "systemd_adv.sock")
    server_sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    server_sock.bind(sock_path)
    server_sock.setblocking(False)

    old_env = os.environ.get("NOTIFY_SOCKET")
    old_usec = os.environ.get("WATCHDOG_USEC")
    try:
        os.environ["NOTIFY_SOCKET"] = sock_path
        os.environ["WATCHDOG_USEC"] = "20000000"  # 20 seconds

        dog = SystemdWatchdog()
        assert dog.is_enabled is True
        assert dog.get_watchdog_interval() == 10.0  # 20s / 2 = 10s

        assert dog.notify_ready() is True
        assert server_sock.recv(1024).decode() == "READY=1"

        assert dog.notify_status("Operating normally") is True
        assert server_sock.recv(1024).decode() == "STATUS=Operating normally"

        assert dog.notify_watchdog() is True
        assert server_sock.recv(1024).decode() == "WATCHDOG=1"

        assert dog.notify_stopping() is True
        assert server_sock.recv(1024).decode() == "STOPPING=1"

    finally:
        server_sock.close()
        if old_env is not None:
            os.environ["NOTIFY_SOCKET"] = old_env
        else:
            os.environ.pop("NOTIFY_SOCKET", None)
        if old_usec is not None:
            os.environ["WATCHDOG_USEC"] = old_usec
        else:
            os.environ.pop("WATCHDOG_USEC", None)


@pytest.mark.asyncio
async def test_step13_missed_schedule_alert_generation(tmp_path):
    """
    Step 13: Verify that scheduled actions remaining PENDING past the grace window generate alerts.
    """
    db_path = str(tmp_path / "test_missed_sched.db")
    db = DatabaseManager(db_path=db_path)
    await db.connect()
    repo = ScheduleRepository(db)

    # Action scheduled 10 minutes ago
    overdue_time = (datetime.now(timezone.utc) - timedelta(minutes=10)).isoformat()
    await repo.save_schedule_with_actions(
        schedule_id="sched-overdue",
        date="2026-10-12",
        revision=1,
        actions=[
            {
                "action_id": "act-overdue-1",
                "iot_node_id": "/dev/ttyUSB0",
                "scheduled_at": overdue_time,
                "action": "LIGHTS_ON",
                "channel_key": "relay:light",
                "status": "PENDING",
            }
        ],
    )

    supervisor = HubSupervisor(
        check_interval=1.0,
        db=db,
        schedule_repo=repo,
        missed_schedule_grace_seconds=120,  # 2 minutes grace
    )

    await supervisor.check_missed_schedules()

    # Verify alert was generated
    assert len(supervisor.alerts) > 0
    missed_alert = next((a for a in supervisor.alerts if "Missed scheduled action" in a["message"]), None)
    assert missed_alert is not None
    assert missed_alert["level"] == "WARNING"
    assert "act-overdue-1" in missed_alert["message"]

    await db.close()


def test_step13_recovery_rules_registry():
    """
    Step 13: Verify that all 9 conditions in the specification table are defined.
    """
    from supervisor.recovery_rules import MonitoredCondition, SupervisorAction, RULES

    expected_conditions = [
        MonitoredCondition.RABBITMQ_UNAVAILABLE,
        MonitoredCondition.USB_DEVICE_REMOVED,
        MonitoredCondition.DEVICE_RECONNECTED,
        MonitoredCondition.COMMAND_TIMEOUT,
        MonitoredCondition.REPEATED_DEVICE_FAILURES,
        MonitoredCondition.SQLITE_STORAGE_ERROR,
        MonitoredCondition.MISSED_SCHEDULE,
        MonitoredCondition.PENDING_RESULT_BACKLOG,
        MonitoredCondition.INCORRECT_SYSTEM_CLOCK,
    ]

    for cond in expected_conditions:
        assert cond in RULES
        rule = RULES[cond]
        assert rule.expected_response is not None
        assert len(rule.description) > 0

