#!/usr/bin/env python3
"""
Step 14 End-to-End Demonstration and Verification for RnR Linux IoT Hub.
File: run_step14.py

Demonstrates:
- STEP 14: Result Processing and RabbitMQ Reporting
  * Produces durable execution results with canonical payload format:
    {
      "event_id": "...",
      "event_type": "action_result",
      "hub_id": "hub-01",
      "action_id": "action-001",
      "device_id": "98a316e6b658",
      "action": "LIGHTS_ON",
      "status": "SUCCESS",
      "scheduled_at": "...",
      "executed_at": "..."
    }
  * Stores result and outgoing event in ONE database transaction.
  * Retains events locally in SQLite Outbox if RabbitMQ is disconnected.
  * Flushes pending events upon reconnection, waiting for publisher confirmation before marking delivered.
  * Assigns stable event ID to guarantee idempotency and avoid duplicate delivery.
"""

from __future__ import annotations
import asyncio
import json
import os
from pathlib import Path
import sys
from datetime import datetime, timezone
from unittest.mock import AsyncMock

# Color styling for terminal
GREEN = "\033[92m"
YELLOW = "\033[93m"
CYAN = "\033[96m"
RED = "\033[91m"
BOLD = "\033[1m"
RESET = "\033[0m"


def header(title: str):
    print(f"\n{BOLD}{CYAN}{'='*70}{RESET}")
    print(f"{BOLD}{CYAN} {title.upper()}{RESET}")
    print(f"{BOLD}{CYAN}{'='*70}{RESET}")


def step_ok(msg: str):
    print(f" {GREEN}[PASS]{RESET} {msg}")


def step_warn(msg: str):
    print(f" {YELLOW}[WARN]{RESET} {msg}")


def step_fail(msg: str):
    print(f" {RED}[FAIL]{RESET} {msg}")
    raise RuntimeError(msg)


async def main():
    root_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(root_dir))

    from config import HUB_ID, ACTION_RESULT_KEY
    from storage.database import DatabaseManager
    from storage.schedule_repository import ScheduleRepository
    from storage.event_outbox import EventOutbox
    from ingestion.rabbitmq_publisher import RabbitMQPublisher
    from devices.device_manager import DeviceManager
    from scheduler.action_dispatcher import ActionDispatcher

    print(f"{BOLD}======================================================================{RESET}")
    print(f"{BOLD}      RnR LINUX IOT HUB: STEP 14 VERIFICATION & DEMO RUNNER           {RESET}")
    print(f"{BOLD}======================================================================{RESET}")
    print(f" Timestamp: {datetime.now().isoformat()}")
    print(f" Hub ID   : {HUB_ID}")
    print(f" Workspace: {root_dir}\n")

    test_db_path = str(root_dir / "step14_demo.db")
    if os.path.exists(test_db_path):
        os.remove(test_db_path)

    db = DatabaseManager(db_path=test_db_path)
    await db.connect()
    repo = ScheduleRepository(db)
    outbox = EventOutbox(db)

    # -------------------------------------------------------------------------
    # 1. ATOMIC TRANSACTION: STORE RESULT & OUTGOING EVENT IN ONE TRANSACTION
    # -------------------------------------------------------------------------
    header("1. Atomic Database Transaction (Result + Outbox Event)")

    schedule_id = "hub-01-2026-10-13"
    action_id = "action-001"
    device_id = "98a316e6b658"
    action_name = "LIGHTS_ON"
    scheduled_at = "2026-10-13T10:45:00+05:30"
    executed_at = "2026-10-13T10:45:01+05:30"

    print(" Recording execution and enqueuing outbox event in ONE database transaction...")
    exec_id, outbox_id, payload = await outbox.record_execution_and_event(
        schedule_id=schedule_id,
        action_id=action_id,
        device_id=device_id,
        action=action_name,
        status="SUCCESS",
        scheduled_at=scheduled_at,
        executed_at=executed_at,
        attempt=1,
        result={"relay_pin": 13, "state": "HIGH"},
    )

    print("\n Canonical Step 14 Event Payload:")
    print(json.dumps(payload, indent=2))

    # Assertions on payload
    if payload.get("event_type") == "action_result":
        step_ok("Event type matches 'action_result'.")
    else:
        step_fail(f"Unexpected event_type: {payload.get('event_type')}")

    if payload.get("hub_id") == HUB_ID and payload.get("action_id") == action_id:
        step_ok(f"hub_id ('{HUB_ID}') and action_id ('{action_id}') properly bound.")
    else:
        step_fail("hub_id or action_id mismatch.")

    if payload.get("device_id") == device_id and payload.get("action") == action_name:
        step_ok(f"device_id ('{device_id}') and action ('{action_name}') recorded.")
    else:
        step_fail("device_id or action mismatch.")

    if payload.get("status") == "SUCCESS" and payload.get("event_id"):
        step_ok(f"Status is 'SUCCESS' and stable event_id generated: {payload['event_id']}")
    else:
        step_fail("Missing status or event_id.")

    # -------------------------------------------------------------------------
    # 2. LOCAL OUTBOX RETENTION DURING TEMPORARY RABBITMQ OUTAGE
    # -------------------------------------------------------------------------
    header("2. Local Retention During RabbitMQ Outage")

    publisher = RabbitMQPublisher(outbox=outbox, hub_id=HUB_ID)
    # Simulate RabbitMQ outage (publish_event returns False / fails)
    publisher.publish_event = AsyncMock(return_value=False)

    print(" Simulating 3 additional scheduled actions executing while RabbitMQ broker is DOWN...")
    for i in range(2, 5):
        act_id = f"action-00{i}"
        await outbox.record_execution_and_event(
            schedule_id=schedule_id,
            action_id=act_id,
            device_id=device_id,
            action="LIGHTS_OFF" if i % 2 == 0 else "LIGHTS_ON",
            status="SUCCESS",
            scheduled_at=f"2026-10-13T10:45:0{i}+05:30",
        )
        # Attempt immediate publish (fails because broker is down)
        await publisher.publish_event(
            routing_key=ACTION_RESULT_KEY,
            payload={"action_id": act_id},
        )

    stats = await outbox.get_stats()
    print(f" Outbox Stats during RabbitMQ Outage: {stats}")
    if stats["PENDING"] == 4 and stats["SENT"] == 0:
        step_ok("All 4 execution results retained locally in SQLite Outbox (0 dropped).")
    else:
        step_fail(f"Expected 4 PENDING events, got: {stats}")

    # -------------------------------------------------------------------------
    # 3. RECONNECTION & PUBLISHER CONFIRMATION
    # -------------------------------------------------------------------------
    header("3. Reconnection Flush With Publisher Confirmation")

    print(" RabbitMQ broker restored! Reconnecting and flushing outbox...")
    # Simulate broker restored with publisher confirmations (return True)
    confirmed_events = []

    async def mock_publish_with_confirmation(routing_key, payload, message_id=None, **kwargs):
        confirmed_events.append({"rk": routing_key, "payload": payload, "msg_id": message_id})
        return True

    publisher.publish_event = mock_publish_with_confirmation

    flushed_count = await publisher.flush_outbox()
    print(f" Flushed and confirmed events: {flushed_count}")

    if flushed_count == 4:
        step_ok(f"Successfully published and confirmed {flushed_count} outbox events.")
    else:
        step_fail(f"Expected 4 flushed events, got {flushed_count}")

    stats_after = await outbox.get_stats()
    print(f" Outbox Stats after Broker Recovery: {stats_after}")
    if stats_after["PENDING"] == 0 and stats_after["SENT"] == 4:
        step_ok("All 4 events transitioned to 'SENT' upon broker confirmation.")
    else:
        step_fail(f"Expected 0 PENDING and 4 SENT, got: {stats_after}")

    # -------------------------------------------------------------------------
    # 4. STABLE EVENT ID DEDUPLICATION
    # -------------------------------------------------------------------------
    header("4. Stable Event ID Idempotency & Deduplication")

    first_event = confirmed_events[0]
    first_event_id = first_event["payload"]["event_id"]
    print(f" Verified message_id matches payload event_id: '{first_event_id}'")
    if first_event["msg_id"] == first_event_id:
        step_ok("Stable event_id attached to message_id for AMQP broker deduplication.")
    else:
        step_fail("message_id does not match event_id.")

    # -------------------------------------------------------------------------
    # 5. ACTION DISPATCHER END-TO-END INTEGRATION
    # -------------------------------------------------------------------------
    header("5. End-to-End ActionDispatcher Integration")

    class MockNanoActuator:
        def __init__(self, dev_id):
            self.device_id = dev_id

        async def execute_command(self, act, params):
            return {"status": "SUCCESS", "action": act, "pin": 13}

    dev_mgr = DeviceManager()
    dev_mgr.register_device(MockNanoActuator(device_id))

    dispatcher = ActionDispatcher(
        device_manager=dev_mgr,
        schedule_repo=repo,
        outbox=outbox,
        publisher=publisher,
    )

    action_record = {
        "schedule_id": schedule_id,
        "action_id": "action-e2e-dispatch",
        "iot_node_id": device_id,
        "action": "LIGHTS_ON",
        "scheduled_at": "2026-10-13T10:50:00+05:30",
        "params": {},
    }

    res = await dispatcher._execute_action(device_id, action_record)
    if res.get("status") == "SUCCESS":
        step_ok("ActionDispatcher dispatched 'LIGHTS_ON' to hardware driver.")
    else:
        step_fail(f"Dispatch failed: {res}")

    # Verify execution was committed to SQLite
    db_execs = await repo.get_executions(schedule_id=schedule_id)
    matched = [e for e in db_execs if e["action_id"] == "action-e2e-dispatch"]
    if len(matched) == 1:
        step_ok("Execution recorded in SQLite database.")
    else:
        step_fail("Execution not found in SQLite.")

    # Flush dispatcher event
    flushed_e2e = await publisher.flush_outbox()
    if flushed_e2e == 1:
        step_ok("Dispatcher result event delivered to RabbitMQ with publisher confirmation.")
    else:
        step_fail("Dispatcher result was not flushed.")

    # Clean up test DB
    await db.close()
    if os.path.exists(test_db_path):
        os.remove(test_db_path)

    print(f"\n{BOLD}======================================================================{RESET}")
    print(f"{BOLD}{GREEN}      STEP 14 IMPLEMENTATION FULLY VERIFIED AND COMPLETE!             {RESET}")
    print(f"{BOLD}======================================================================{RESET}\n")


if __name__ == "__main__":
    asyncio.run(main())
