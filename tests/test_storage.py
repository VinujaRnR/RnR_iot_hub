"""
Tests for SQLite Persistent Storage System.
Verifies Step 6 requirements:
- WAL mode is enabled and functional
- A schedule and its actions are saved together atomically
- Simulating Python restart retrieves every scheduled action with its correct state
- Failed/crashed schedule import rolls back completely with zero partial state
- Event outbox, execution audit logs, devices, and alerts persist accurately
"""

import os
import tempfile
import pytest
import pytest_asyncio
import aiosqlite

from storage.database import DatabaseManager
from storage.schedule_repository import ScheduleRepository
from storage.event_outbox import EventOutbox
from ingestion.schedule_normalizer import NormalizedActionRecord


@pytest_asyncio.fixture
async def temp_db_file():
    fd, path = tempfile.mkstemp(suffix="_test_iothub.db")
    os.close(fd)
    yield path
    if os.path.exists(path):
        os.remove(path)
    # Remove SQLite WAL files if created
    for ext in ("-wal", "-shm"):
        wal_file = path + ext
        if os.path.exists(wal_file):
            try:
                os.remove(wal_file)
            except OSError:
                pass


@pytest.mark.asyncio
async def test_wal_mode_enabled(temp_db_file):
    """Verifies SQLite WAL mode and foreign key pragmas are active."""
    db = DatabaseManager(db_path=temp_db_file)
    await db.connect()

    async with db.connection.execute("PRAGMA journal_mode;") as cur:
        row = await cur.fetchone()
        assert row[0].lower() == "wal"

    async with db.connection.execute("PRAGMA foreign_keys;") as cur:
        row = await cur.fetchone()
        assert row[0] == 1

    await db.close()


@pytest.mark.asyncio
async def test_save_schedule_restart_python_and_retrieve_actions(temp_db_file):
    """
    Step 6 core requirement:
    Save a schedule, restart Python (close connection and instantiate new process DB),
    and retrieve every scheduled action with its correct state.
    """
    # --- Process 1: Save schedule and actions ---
    db1 = DatabaseManager(db_path=temp_db_file)
    await db1.connect()
    repo1 = ScheduleRepository(db1)

    schedule_id = "hub-01-2026-10-09"
    date_str = "2026-10-09"
    revision_num = 1

    sample_actions = [
        NormalizedActionRecord(
            schedule_id=schedule_id,
            action_id="action-001",
            production_order_id=174513,
            resource_id=20020,
            sub_resource_id=20023,
            iot_node_id="98a316e6b658",
            stage_id=30,
            scheduled_at="2026-10-09T08:00:00+05:30",
            action="LIGHTS_ON",
            channel_key="relay:light",
            device_id="light_glow_1",
            device_type="light_glow",
            device_number=1,
            device_name="light_glow #1",
            status="PENDING",
        ),
        NormalizedActionRecord(
            schedule_id=schedule_id,
            action_id="action-002",
            production_order_id=174513,
            resource_id=20020,
            sub_resource_id=20023,
            iot_node_id="98a316e6b658",
            stage_id=30,
            scheduled_at="2026-10-09T09:56:00+05:30",
            action="LIGHTS_ON",
            channel_key="relay:light",
            params={},
            status="PENDING",
        ),
        NormalizedActionRecord(
            schedule_id=schedule_id,
            action_id="action-003",
            production_order_id=174513,
            resource_id=20020,
            sub_resource_id=20023,
            iot_node_id="98a316e6b658",
            stage_id=30,
            scheduled_at="2026-10-09T10:15:00+05:30",
            action="LIGHTS_OFF",
            channel_key="relay:light",
            params={},
            status="PENDING",
        ),
    ]

    await repo1.save_schedule_with_actions(
        schedule_id=schedule_id,
        date=date_str,
        revision=revision_num,
        actions=sample_actions,
        raw_json='{"mock": "json"}',
    )
    # Simulate stopping Python / shutdown
    await db1.close()

    # --- Process 2: Fresh restart, new connection to same file ---
    db2 = DatabaseManager(db_path=temp_db_file)
    await db2.connect()
    repo2 = ScheduleRepository(db2)

    # 1. Retrieve schedule header
    sched_header = await repo2.get_schedule(schedule_id)
    assert sched_header is not None
    assert sched_header["schedule_id"] == schedule_id
    assert sched_header["date"] == date_str
    assert sched_header["revision"] == revision_num
    assert sched_header["status"] == "ACTIVE"

    # 2. Retrieve every scheduled action
    retrieved_actions = await repo2.get_scheduled_actions(schedule_id)
    assert len(retrieved_actions) == 3

    # 3. Verify every field and state on each retrieved action
    act1 = retrieved_actions[0]
    assert act1["action_id"] == "action-001"
    assert act1["production_order_id"] == "174513"
    assert act1["resource_id"] == "20020"
    assert act1["sub_resource_id"] == "20023"
    assert act1["iot_node_id"] == "98a316e6b658"
    assert act1["stage_id"] == "30"
    assert act1["scheduled_at"] == "2026-10-09T08:00:00+05:30"
    assert act1["action"] == "LIGHTS_ON"
    assert act1["channel_key"] == "relay:light"
    assert act1["device_id"] == "light_glow_1"
    assert act1["device_type"] == "light_glow"
    assert act1["device_number"] == 1
    assert act1["device_name"] == "light_glow #1"
    # params are not stored; they are derived for the command JSON only
    assert act1["params"] == {"device_type": "light_glow", "device_number": 1}
    assert act1["status"] == "PENDING"

    act2 = retrieved_actions[1]
    assert act2["action_id"] == "action-002"
    assert act2["scheduled_at"] == "2026-10-09T09:56:00+05:30"
    assert act2["action"] == "LIGHTS_ON"
    assert act2["status"] == "PENDING"

    act3 = retrieved_actions[2]
    assert act3["action_id"] == "action-003"
    assert act3["action"] == "LIGHTS_OFF"
    assert act3["status"] == "PENDING"

    await db2.close()


@pytest.mark.asyncio
async def test_transaction_rollback_prevents_partial_schedule(temp_db_file):
    """
    Step 6 atomicity requirement:
    If a failure occurs midway through saving actions, the entire transaction
    rolls back and no partial schedule is saved.
    """
    db = DatabaseManager(db_path=temp_db_file)
    await db.connect()
    repo = ScheduleRepository(db)

    schedule_id = "sched-crash-test"

    # Actions list where second action causes an intentional error (duplicate action_id)
    faulty_actions = [
        NormalizedActionRecord(
            schedule_id=schedule_id,
            action_id="act-same-id",
            iot_node_id="node-1",
            scheduled_at="2026-10-09T10:00:00+05:30",
            action="LIGHTS_ON",
            channel_key="ch1",
        ),
        NormalizedActionRecord(
            schedule_id=schedule_id,
            action_id="act-same-id",  # Violates UNIQUE(schedule_id, action_id)
            iot_node_id="node-1",
            scheduled_at="2026-10-09T10:05:00+05:30",
            action="LIGHTS_OFF",
            channel_key="ch1",
        ),
    ]

    with pytest.raises(Exception):
        await repo.save_schedule_with_actions(
            schedule_id=schedule_id,
            date="2026-10-09",
            revision=1,
            actions=faulty_actions,
        )

    # Verify rollback: schedule header must NOT exist
    header = await repo.get_schedule(schedule_id)
    assert header is None

    # Verify rollback: no actions exist
    actions = await repo.get_scheduled_actions(schedule_id)
    assert len(actions) == 0

    await db.close()


@pytest.mark.asyncio
async def test_execution_recording_and_action_state_transition(temp_db_file):
    """Verifies that command execution attempts and acknowledgments update action state."""
    db = DatabaseManager(db_path=temp_db_file)
    await db.connect()
    repo = ScheduleRepository(db)

    schedule_id = "sched-exec-test"
    action = NormalizedActionRecord(
        schedule_id=schedule_id,
        action_id="act-exec-01",
        iot_node_id="iot_001",
        scheduled_at="2026-10-09T11:00:00+05:30",
        action="set_led",
        channel_key="led",
        params={"state": "on"},
    )

    await repo.save_schedule_with_actions(
        schedule_id=schedule_id,
        date="2026-10-09",
        revision=1,
        actions=[action],
    )

    # Initial state is PENDING
    acts = await repo.get_scheduled_actions(schedule_id)
    assert acts[0]["status"] == "PENDING"

    # Record execution success
    exec_id = await repo.record_execution(
        schedule_id=schedule_id,
        action_id="act-exec-01",
        device_id="iot_001",
        command_action="set_led",
        attempt=1,
        status="SUCCESS",
        result={"led": "on"},
    )
    assert exec_id > 0

    # Action status must have transitioned to COMPLETED
    acts_updated = await repo.get_scheduled_actions(schedule_id)
    assert acts_updated[0]["status"] == "COMPLETED"
    assert acts_updated[0]["executed_at"] is not None

    # Executions audit history is retrievable
    history = await repo.get_executions(schedule_id=schedule_id)
    assert len(history) == 1
    assert history[0]["status"] == "SUCCESS"
    assert history[0]["result"] == {"led": "on"}

    await db.close()


@pytest.mark.asyncio
async def test_event_outbox_persistence(temp_db_file):
    """Verifies that event outbox persists events, tracks retries, and records delivery."""
    db = DatabaseManager(db_path=temp_db_file)
    await db.connect()
    outbox = EventOutbox(db)

    # 1. Enqueue event
    event_id = await outbox.enqueue(
        event_type="telemetry.temperature",
        payload={"temp": 24.5, "sensor": "iot_002"},
        routing_key="telemetry.sensor",
    )
    assert event_id > 0

    # 2. Retrieve pending
    pending = await outbox.get_pending()
    assert len(pending) == 1
    assert pending[0]["event_type"] == "telemetry.temperature"
    assert pending[0]["payload"]["temp"] == 24.5
    assert pending[0]["status"] == "PENDING"

    # 3. Mark sent
    await outbox.mark_sent(event_id)
    stats = await outbox.get_stats()
    assert stats.get("SENT") == 1
    assert stats.get("PENDING", 0) == 0

    # 4. Prune sent events
    pruned = await outbox.prune_sent()
    assert pruned == 1

    await db.close()


@pytest.mark.asyncio
async def test_devices_and_alerts_persistence(temp_db_file):
    """Verifies physical device identities and supervisor alerts persistence."""
    db = DatabaseManager(db_path=temp_db_file)
    await db.connect()

    # Save physical device
    await db.save_device(
        device_id="98a316e6b658",
        name="Main Relay Controller",
        capabilities={"relays": 8, "baud": 115200},
        connection_state="ONLINE",
    )
    dev = await db.get_device("98a316e6b658")
    assert dev is not None
    assert dev["name"] == "Main Relay Controller"
    assert dev["capabilities"]["relays"] == 8

    # Record alert
    alert_id = await db.record_alert(
        level="WARNING",
        source="supervisor",
        message="System clock drift detected",
        details={"offset_ms": 120},
    )
    assert alert_id > 0

    alerts = await db.get_alerts()
    assert len(alerts) >= 1
    assert alerts[0]["message"] == "System clock drift detected"

    await db.close()
