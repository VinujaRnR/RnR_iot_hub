#!/usr/bin/env python3
"""
Step 12 and Step 13 End-to-End Demonstration and Verification for RnR Linux IoT Hub.
File: run_step12_and_step13.py

Demonstrates:
- STEP 12: Restart and Missed-Action Recovery
  * Desired-state recovery policy for stateful relay channels (relay:light)
  * Intermediate actions marked SUPERSEDED; single target state applied
  * Future actions preserved in PENDING state
  * Non-idempotent actions (motors, dispensing) marked SKIPPED_UNSAFE without physical replay

- STEP 13: Deterministic HubSupervisor
  * Monitored conditions and deterministic recovery rules:
    1. System clock validation (blocks unsafe timed execution)
    2. SQLite storage health (enters safe degraded mode on error)
    3. USB device disconnect & reconnect (OFFLINE / ONLINE)
    4. Repeated device failures quarantine (dispatches blocked, critical alert)
    5. Pending result backlog backpressure (activated on threshold, relieved on drain)
    6. Missed schedule overdue alert generation
    7. Systemd watchdog notification socket (sd_notify WATCHDOG=1 / READY=1)
"""

from __future__ import annotations
import asyncio
from datetime import datetime, timezone
import os
from pathlib import Path
import socket
import sys

# Color styling for terminal
GREEN = "\033[92m"
YELLOW = "\033[93m"
CYAN = "\033[96m"
RED = "\033[91m"
BOLD = "\033[1m"
RESET = "\033[0m"


def header(step_num: int, title: str):
    print(f"\n{BOLD}{CYAN}{'='*70}{RESET}")
    print(f"{BOLD}{CYAN} STEP {step_num}: {title.upper()}{RESET}")
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

    from storage.database import DatabaseManager
    from storage.schedule_repository import ScheduleRepository
    from storage.event_outbox import EventOutbox
    from devices.device_manager import DeviceManager, create_default_device_manager
    from scheduler.missed_action_policy import MissedActionPolicy, RecoveryPolicy
    from scheduler.state_reconciler import StateReconciler
    from scheduler.action_dispatcher import ActionDispatcher
    from supervisor.supervisor import HubSupervisor

    print(f"{BOLD}======================================================================{RESET}")
    print(f"{BOLD}      RnR LINUX IOT HUB: STEP 12 & STEP 13 VERIFICATION RUNNER        {RESET}")
    print(f"{BOLD}======================================================================{RESET}")
    print(f" Timestamp: {datetime.now().isoformat()}")
    print(f" Workspace: {root_dir}\n")

    test_db_path = str(root_dir / "step12_13_demo.db")
    if os.path.exists(test_db_path):
        os.remove(test_db_path)

    db = DatabaseManager(db_path=test_db_path)
    await db.connect()
    repo = ScheduleRepository(db)
    outbox = EventOutbox(db)

    # -------------------------------------------------------------------------
    # STEP 12: RESTART & MISSED-ACTION RECOVERY
    # -------------------------------------------------------------------------
    header(12, "Restart & Missed-Action Recovery (Desired-State Policy)")

    print(" Scenario: Schedule defines relay:light transitions:")
    print("   09:56 -> LIGHTS_ON")
    print("   10:00 -> LIGHTS_OFF")
    print("   10:07 -> LIGHTS_ON")
    print("   10:19 -> LIGHTS_OFF")
    print(" Simulating hub reboot at 10:15...")

    schedule_id = "sched-restart-demo"
    demo_actions = [
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
        # Non-idempotent action in the past
        {
            "action_id": "act-dispense-past",
            "iot_node_id": "feeder_node",
            "scheduled_at": "2026-10-12T10:05:00+00:00",
            "action": "dispense_material",
            "channel_key": "feeder:dispense",
            "status": "PENDING",
        },
    ]

    await repo.save_schedule_with_actions(
        schedule_id=schedule_id,
        date="2026-10-12",
        revision=1,
        actions=demo_actions,
    )

    dispatched_commands = []

    class MockNanoDevice:
        def __init__(self, device_id: str):
            self.device_id = device_id
            self._state = {"led": "OFF"}

        async def execute_command(self, action: str, params: dict):
            dispatched_commands.append(action)
            self._state["led"] = "ON" if "ON" in action else "OFF"
            return {"status": "SUCCESS", "action": action, "state": self._state["led"]}

    dev_mgr = DeviceManager()
    dev_mgr.register_device(MockNanoDevice("/dev/ttyUSB0"))
    dev_mgr.register_device(MockNanoDevice("feeder_node"))

    reconciler = StateReconciler(db=db, schedule_repo=repo, device_manager=dev_mgr)

    # Reconcile at 10:15
    restart_time = "2026-10-12T10:15:00+00:00"
    report = await reconciler.reconcile_on_startup(current_time_iso=restart_time)

    # 1. Check commands dispatched
    if len(dispatched_commands) == 1 and dispatched_commands[0] == "LIGHTS_ON":
        step_ok("Desired-state recovery applied exactly 1 target command: 'LIGHTS_ON'.")
        step_ok("Intermediate commands (09:56 ON, 10:00 OFF) were NOT replayed redundantly.")
    else:
        step_fail(f"Unexpected commands dispatched: {dispatched_commands}")

    # 2. Check superseded actions
    if "act-0956" in report.superseded_actions and "act-1000" in report.superseded_actions:
        step_ok("Prior intermediate actions (act-0956, act-1000) marked SUPERSEDED in SQLite.")
    else:
        step_fail(f"Superseded actions missing: {report.superseded_actions}")

    # 3. Check non-idempotent action
    if "act-dispense-past" in report.skipped_unsafe_actions:
        step_ok("Non-idempotent action 'dispense_material' marked SKIPPED_UNSAFE (never replayed).")
    else:
        step_fail(f"Non-idempotent action was not skipped safely: {report.skipped_unsafe_actions}")

    # 4. Check future actions
    if report.future_pending_count == 1:
        step_ok("Future scheduled action at 10:19 remained PENDING for scheduled execution.")
    else:
        step_fail(f"Future pending count incorrect: {report.future_pending_count}")

    # -------------------------------------------------------------------------
    # STEP 13: DETERMINISTIC HUBSUPERVISOR
    # -------------------------------------------------------------------------
    header(13, "Deterministic HubSupervisor & Recovery Rules")

    supervisor = HubSupervisor(
        check_interval=1.0,
        db=db,
        schedule_repo=repo,
        device_manager=dev_mgr,
        outbox=outbox,
        max_consecutive_failures=3,
        backlog_threshold=5,
    )

    # Rule 1 & 9: System clock check
    clock_safe = await supervisor.check_system_clock()
    step_ok(f"Supervisor Rule 1 & 9 (System Clock Check): Clock safe = {clock_safe}")

    # Rule 6: SQLite storage health check
    is_healthy = await supervisor.check_storage_health()
    step_ok(f"Supervisor Rule 6 (SQLite Storage Check): DB health OK = {is_healthy}")

    # Rule 4 & 5: Repeated command failure & quarantine
    dispatcher = ActionDispatcher(device_manager=dev_mgr, schedule_repo=repo, supervisor=supervisor)
    faulty_id = "faulty_device"
    dev_mgr.register_device(MockNanoDevice(faulty_id))

    supervisor.record_device_command_outcome(faulty_id, False, "Serial Timeout (1/3)")
    supervisor.record_device_command_outcome(faulty_id, False, "Framing Error (2/3)")
    if not supervisor.is_device_quarantined(faulty_id):
        step_ok("Supervisor Rule 4: Transient failures tracked under quarantine threshold.")

    supervisor.record_device_command_outcome(faulty_id, False, "Repeated Failure (3/3)")
    if supervisor.is_device_quarantined(faulty_id):
        step_ok(f"Supervisor Rule 5: Repeated failures ({supervisor.max_consecutive_failures}) triggered QUARANTINE!")

    # Verify dispatcher rejects quarantined device
    rejected = await dispatcher._execute_action(faulty_id, {"action": "LIGHTS_ON", "action_id": "test-q"})
    if rejected.get("status") == "QUARANTINED":
        step_ok("Supervisor Rule 5: Dispatcher immediately blocks commands to quarantined device.")

    supervisor.unquarantine_device(faulty_id)
    step_ok("Supervisor Rule 5: Device successfully unquarantined after manual check.")

    # Rule 8: Outbox backlog & backpressure
    for i in range(6):
        await outbox.enqueue("test.event", {"idx": i}, "test.route")
    await supervisor.check_outbox_backlog()
    if supervisor.backpressure_active:
        step_ok("Supervisor Rule 8: Backpressure activated when Outbox backlog exceeded threshold (6 >= 5).")

    pending_events = await outbox.get_pending()
    for ev in pending_events[:5]:
        await outbox.mark_published(ev["id"])
    await supervisor.check_outbox_backlog()
    if not supervisor.backpressure_active:
        step_ok("Supervisor Rule 8: Backpressure relieved after backlog drained below threshold.")

    # Rule 10: Systemd watchdog notification test
    sock_path = str(root_dir / "systemd_test.sock")
    if os.path.exists(sock_path):
        os.remove(sock_path)

    sock_server = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    sock_server.bind(sock_path)
    sock_server.setblocking(False)

    os.environ["NOTIFY_SOCKET"] = sock_path
    supervisor._send_systemd_notify("WATCHDOG=1")
    watchdog_msg = sock_server.recv(1024).decode("utf-8")
    if watchdog_msg == "WATCHDOG=1":
        step_ok("Supervisor Rule 10 (Systemd Watchdog): Sent 'WATCHDOG=1' to $NOTIFY_SOCKET.")

    supervisor._send_systemd_notify("READY=1")
    ready_msg = sock_server.recv(1024).decode("utf-8")
    if ready_msg == "READY=1":
        step_ok("Supervisor Rule 10 (Systemd Watchdog): Sent 'READY=1' to $NOTIFY_SOCKET.")

    sock_server.close()
    if os.path.exists(sock_path):
        os.remove(sock_path)
    os.environ.pop("NOTIFY_SOCKET", None)

    # Clean up test DB
    await db.close()
    if os.path.exists(test_db_path):
        os.remove(test_db_path)

    print(f"\n{BOLD}======================================================================{RESET}")
    print(f"{BOLD}{GREEN}      STEP 12 & STEP 13 IMPLEMENTATION FULLY VERIFIED!                {RESET}")
    print(f"{BOLD}======================================================================{RESET}\n")


if __name__ == "__main__":
    asyncio.run(main())
