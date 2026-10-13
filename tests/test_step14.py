"""
Unit and Integration Tests for Step 14: Execution Result Processing & RabbitMQ Reporting.
File: tests/test_step14.py

Validates:
1. Canonical execution result payload format:
   - event_type: "action_result"
   - hub_id: "hub-01"
   - action_id, device_id, action, status, scheduled_at, executed_at, event_id
2. Transactional consistency:
   - Result and outgoing event stored in ONE database transaction.
   - Atomic rollback prevents orphaned executions or missing events.
3. Local outbox retention during RabbitMQ outage:
   - Events remain PENDING locally when broker is disconnected.
4. Confirmed delivery upon reconnection:
   - Flushes pending events with publisher confirmations.
   - Only marks delivered after broker confirmation.
5. Stable Event ID:
   - Guaranteed stable event_id preserved across retries for backend deduplication.
6. ActionDispatcher integration:
   - End-to-end atomic execution recording and reporting.
"""

from __future__ import annotations
import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, patch
import pytest

from config import HUB_ID, ACTION_RESULT_KEY
from storage.database import DatabaseManager
from storage.schedule_repository import ScheduleRepository
from storage.event_outbox import EventOutbox
from ingestion.rabbitmq_publisher import RabbitMQPublisher
from devices.device_manager import DeviceManager
from scheduler.action_dispatcher import ActionDispatcher


@pytest.mark.asyncio
async def test_step14_canonical_result_payload_format(tmp_path):
    """
    Step 14: Verify that action results match the canonical structure with stable event_id:
    {
      "event_id": "...",
      "event_type": "action_result",
      "hub_id": "hub-01",
      "action_id": "action-001",
      "device_id": "98a316e6b658",
      "action": "LIGHTS_ON",
      "status": "SUCCESS",
      "scheduled_at": "2026-10-09T09:56:00+05:30",
      "executed_at": "2026-10-09T09:56:01+05:30"
    }
    """
    db = DatabaseManager(db_path=str(tmp_path / "payload.db"))
    await db.connect()
    outbox = EventOutbox(db)
    publisher = RabbitMQPublisher(outbox=outbox, hub_id="hub-01")
    publisher.publish_event = AsyncMock(return_value=True)

    sched_time = "2026-10-09T09:56:00+05:30"
    exec_time = "2026-10-09T09:56:01+05:30"

    ok = await publisher.publish_action_result(
        action_id="action-001",
        schedule_id="hub-01-2026-10-09",
        device_id="98a316e6b658",
        action="LIGHTS_ON",
        status="SUCCESS",
        scheduled_at=sched_time,
        executed_at=exec_time,
        result={"relay": "ON"},
    )
    assert ok is True

    publisher.publish_event.assert_called_once()
    call_args = publisher.publish_event.call_args
    payload = call_args.kwargs["payload"]

    assert payload["event_type"] == "action_result"
    assert payload["hub_id"] == "hub-01"
    assert payload["action_id"] == "action-001"
    assert payload["device_id"] == "98a316e6b658"
    assert payload["action"] == "LIGHTS_ON"
    assert payload["status"] == "SUCCESS"
    assert payload["scheduled_at"] == sched_time
    assert payload["executed_at"] == exec_time
    assert "event_id" in payload and len(payload["event_id"]) > 0

    await db.close()


@pytest.mark.asyncio
async def test_step14_atomic_transaction_consistency(tmp_path):
    """
    Step 14: Store execution result and outgoing event in ONE database transaction.
    If transaction rolls back, neither is saved.
    """
    db = DatabaseManager(db_path=str(tmp_path / "tx.db"))
    await db.connect()
    outbox = EventOutbox(db)
    repo = ScheduleRepository(db)

    # 1. Create a schedule with one action
    await repo.save_schedule_with_actions(
        schedule_id="sched-tx-test",
        date="2026-10-13",
        revision=1,
        actions=[
            {
                "action_id": "act-tx-01",
                "iot_node_id": "98a316e6b658",
                "scheduled_at": "2026-10-13T10:00:00+00:00",
                "action": "LIGHTS_ON",
                "channel_key": "relay:light",
                "status": "PENDING",
            }
        ],
    )

    # 2. Record successfully in single transaction
    exec_id, outbox_id, payload = await outbox.record_execution_and_event(
        schedule_id="sched-tx-test",
        action_id="act-tx-01",
        device_id="98a316e6b658",
        action="LIGHTS_ON",
        status="SUCCESS",
        scheduled_at="2026-10-13T10:00:00+00:00",
        result={"pin": 13, "state": "HIGH"},
    )
    assert exec_id > 0
    assert outbox_id > 0
    assert payload["event_id"] is not None

    # Verify both exist in DB
    executions = await repo.get_executions(schedule_id="sched-tx-test")
    assert len(executions) == 1
    assert executions[0]["action_id"] == "act-tx-01"

    pending = await outbox.get_pending()
    assert len(pending) == 1
    assert pending[0]["event_id"] == payload["event_id"]

    # 3. Simulate failure inside transaction: verify atomic rollback
    with pytest.raises(RuntimeError, match="Simulated disk error"):
        async with db.transaction() as conn:
            # Insert execution into table
            await conn.execute(
                "INSERT INTO executions (action_id, schedule_id, device_id, command_action, status, executed_at) "
                "VALUES ('act-fail', 'sched-tx-test', '98a316e6b658', 'LIGHTS_OFF', 'SUCCESS', '2026-10-13T10:05:00')"
            )
            # Simulated error before committing outbox
            raise RuntimeError("Simulated disk error while writing outbox event")

    # Verify atomic rollback: act-fail does NOT exist in executions table
    executions_after = await repo.get_executions(schedule_id="sched-tx-test")
    assert len(executions_after) == 1  # Still only the first one

    await db.close()


@pytest.mark.asyncio
async def test_step14_rabbitmq_outage_local_retention_and_reconnection_flush(tmp_path):
    """
    Step 14:
    - If RabbitMQ is disconnected, retain events locally in SQLite.
    - When RabbitMQ reconnects, publish events and wait for confirmation before marking delivered.
    """
    db = DatabaseManager(db_path=str(tmp_path / "outage.db"))
    await db.connect()
    outbox = EventOutbox(db)
    publisher = RabbitMQPublisher(outbox=outbox, hub_id="hub-01")

    # Phase 1: RabbitMQ is disconnected -> publish fails
    publisher.publish_event = AsyncMock(return_value=False)

    for i in range(3):
        exec_id, outbox_id, _ = await outbox.record_execution_and_event(
            schedule_id="sched-outage",
            action_id=f"act-outage-{i}",
            device_id="98a316e6b658",
            action="LIGHTS_ON" if i % 2 == 0 else "LIGHTS_OFF",
            status="SUCCESS",
        )
        # Attempt immediate publish (fails because broker is down)
        published = await publisher.publish_event(
            routing_key=ACTION_RESULT_KEY,
            payload={"action_id": f"act-outage-{i}"},
        )
        assert published is False

    # Verify all 3 events are retained locally as PENDING
    stats = await outbox.get_stats()
    assert stats["PENDING"] == 3
    assert stats["SENT"] == 0

    # Phase 2: RabbitMQ reconnects -> publish_event succeeds with confirmation
    publisher.publish_event = AsyncMock(return_value=True)

    # Flush outbox
    flushed_count = await publisher.flush_outbox()
    assert flushed_count == 3

    # Verify all events are now marked delivered (SENT)
    stats_after = await outbox.get_stats()
    assert stats_after["PENDING"] == 0
    assert stats_after["SENT"] == 3

    await db.close()


@pytest.mark.asyncio
async def test_step14_stable_event_id_deduplication(tmp_path):
    """
    Step 14: Every event maintains its stable event_id across retry attempts to prevent
    duplicate processing downstream if publisher confirmation was lost.
    """
    db = DatabaseManager(db_path=str(tmp_path / "dedup.db"))
    await db.connect()
    outbox = EventOutbox(db)
    publisher = RabbitMQPublisher(outbox=outbox, hub_id="hub-01")

    custom_event_id = "evt-stable-uuid-4242"
    _, outbox_id, payload = await outbox.record_execution_and_event(
        schedule_id="sched-stable-id",
        action_id="act-dedup",
        device_id="98a316e6b658",
        action="LIGHTS_ON",
        status="SUCCESS",
        event_id=custom_event_id,
    )
    assert payload["event_id"] == custom_event_id

    # Simulated retry attempt 1 (fails)
    publisher.publish_event = AsyncMock(return_value=False)
    await publisher.flush_outbox()

    event_record = await outbox.get_event(custom_event_id)
    assert event_record is not None
    assert event_record["event_id"] == custom_event_id
    assert event_record["status"] == "PENDING"
    assert event_record["retry_count"] == 1

    # Simulated retry attempt 2 (succeeds)
    publisher.publish_event = AsyncMock(return_value=True)
    sent = await publisher.flush_outbox()
    assert sent == 1

    # Verify the published event used the EXACT same stable event_id
    call_args = publisher.publish_event.call_args
    assert call_args.kwargs["message_id"] == custom_event_id
    assert call_args.kwargs["payload"]["event_id"] == custom_event_id

    event_record_sent = await outbox.get_event(custom_event_id)
    assert event_record_sent["status"] == "SENT"

    await db.close()


@pytest.mark.asyncio
async def test_step14_action_dispatcher_transactional_reporting(tmp_path):
    """
    Step 14: End-to-end integration: ActionDispatcher dispatches action,
    records execution result and outbox event in one database transaction,
    and publishes with confirmation.
    """
    db = DatabaseManager(db_path=str(tmp_path / "dispatcher.db"))
    await db.connect()
    repo = ScheduleRepository(db)
    outbox = EventOutbox(db)
    publisher = RabbitMQPublisher(outbox=outbox, hub_id="hub-01")
    publisher.publish_event = AsyncMock(return_value=True)

    class MockLightDevice:
        def __init__(self, device_id: str):
            self.device_id = device_id

        async def execute_command(self, action: str, params: dict):
            return {"status": "SUCCESS", "action": action, "pin": 13}

    device_mgr = DeviceManager()
    device_mgr.register_device(MockLightDevice("98a316e6b658"))

    dispatcher = ActionDispatcher(
        device_manager=device_mgr,
        schedule_repo=repo,
        outbox=outbox,
        publisher=publisher,
    )

    action = {
        "schedule_id": "sched-e2e",
        "action_id": "act-e2e-01",
        "iot_node_id": "98a316e6b658",
        "action": "LIGHTS_ON",
        "scheduled_at": "2026-10-13T10:10:00+00:00",
        "params": {},
    }

    result = await dispatcher._execute_action("98a316e6b658", action)
    assert result["status"] == "SUCCESS"

    # Verify execution was recorded in database
    executions = await repo.get_executions(schedule_id="sched-e2e")
    assert len(executions) == 1
    assert executions[0]["action_id"] == "act-e2e-01"

    # Verify event was atomically enqueued in outbox
    pending = await outbox.get_pending()
    assert len(pending) == 1
    outbox_event = pending[0]
    payload = outbox_event["payload"]

    assert payload["event_type"] == "action_result"
    assert payload["hub_id"] == HUB_ID
    assert payload["action_id"] == "act-e2e-01"
    assert payload["device_id"] == "98a316e6b658"
    assert payload["action"] == "LIGHTS_ON"
    assert payload["status"] == "SUCCESS"
    assert payload["scheduled_at"] == "2026-10-13T10:10:00+00:00"
    assert "event_id" in payload

    # Flush outbox
    flushed = await publisher.flush_outbox()
    assert flushed == 1
    stats = await outbox.get_stats()
    assert stats["SENT"] == 1

    await db.close()
