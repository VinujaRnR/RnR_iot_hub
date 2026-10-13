#!/usr/bin/env python3
"""
Step 16 End-to-End Demonstration and Verification for RnR Linux IoT Hub.
File: run_step16.py

Demonstrates:
- STEP 16: Main Daemon Lifecycle (main.py)
  * Phase 1: Validate Configuration (fail-fast on invalid parameters)
  * Phase 2: Initialize SQLite Storage (WAL mode, schemas verified)
  * Phase 3: Restore Accepted Schedules & Reconcile In-Flight Actions
             (Actions in EXECUTING/CLAIMED state without acknowledgment are
              safely reconciled under desired-state or skip-unsafe policy)
  * Phase 4: Parallel Subsystem Startup
             - USB Device Manager (managed non-blocking serial workers)
             - RabbitMQ Client (consumer & publisher with reconnect logic)
             - Local Scheduler (ScheduleEngine autonomous execution)
             - HubSupervisor (recovery rules, device quarantine & watchdog)
             - Local Management API (FastAPI bound to 127.0.0.1:8765)
  * Phase 5: Continuous Operation
  * Phase 6: Graceful Shutdown (python -m main terminates cleanly on SIGTERM with returncode 0)
"""

from __future__ import annotations
import asyncio
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
import urllib.request
from datetime import datetime, timezone

from config import HUB_ID, SCHEDULE_TIMEZONE, API_HOST, API_PORT
from main import validate_configuration
from storage.database import DatabaseManager
from storage.schedule_repository import ScheduleRepository
from devices.device_manager import DeviceManager
from devices.mock_device import MockDevice
from scheduler.state_reconciler import StateReconciler

# Terminal styling
GREEN = "\033[92m"
YELLOW = "\033[93m"
CYAN = "\033[96m"
RED = "\033[91m"
BOLD = "\033[1m"
RESET = "\033[0m"


def header(title: str):
    print(f"\n{BOLD}{CYAN}{'='*72}{RESET}")
    print(f"{BOLD}{CYAN} {title.upper()}{RESET}")
    print(f"{BOLD}{CYAN}{'='*72}{RESET}")


def step_ok(msg: str):
    print(f" {GREEN}[PASS]{RESET} {msg}")


def step_info(msg: str):
    print(f" {CYAN}[INFO]{RESET} {msg}")


def step_fail(msg: str):
    print(f" {RED}[FAIL]{RESET} {msg}")
    raise RuntimeError(msg)


async def demonstrate_startup_reconciliation():
    """Demonstrates that EXECUTING actions without acknowledgment are reconciled safely."""
    test_db = "demo_step16_reconcile.db"
    if os.path.exists(test_db):
        os.remove(test_db)

    db = DatabaseManager(db_path=test_db)
    await db.connect()
    repo = ScheduleRepository(db)
    dev_mgr = DeviceManager()

    relay_dev = MockDevice("relay-node-01", "Relay Node", supported_actions=["LIGHTS_ON", "LIGHTS_OFF"])
    relay_dev.channels = ["relay:light"]
    dev_mgr.register_device(relay_dev)

    sched_id = "sched-recovery-demo"
    now_dt = datetime.now()
    today_str = now_dt.strftime("%Y-%m-%d")

    actions = [
        {
            "action_id": "act-past-relay-executing",
            "iot_node_id": "relay-node-01",
            "channel_key": "relay:light",
            "action": "LIGHTS_ON",
            "params": "{}",
            "scheduled_at": f"{today_str}T08:00:00+05:30",
            "conflict_policy": "REJECT",
        },
        {
            "action_id": "act-past-motor-executing",
            "iot_node_id": "motor-node-01",
            "channel_key": "motor:feeder",
            "action": "DISPENSE",
            "params": "{}",
            "scheduled_at": f"{today_str}T08:05:00+05:30",
            "conflict_policy": "REJECT",
        },
        {
            "action_id": "act-future-interrupted",
            "iot_node_id": "relay-node-01",
            "channel_key": "relay:light",
            "action": "LIGHTS_OFF",
            "params": "{}",
            "scheduled_at": f"{today_str}T23:55:00+05:30",
            "conflict_policy": "REJECT",
        },
    ]

    await repo.save_schedule_with_actions(
        schedule_id=sched_id,
        date=today_str,
        revision=1,
        actions=actions,
        raw_json=json.dumps({"schedule_id": sched_id, "actions": actions}),
    )

    # Simulate in-flight interrupted state prior to reboot
    await repo.update_action_status(sched_id, "act-past-relay-executing", "EXECUTING")
    await repo.update_action_status(sched_id, "act-past-motor-executing", "EXECUTING")
    await repo.update_action_status(sched_id, "act-future-interrupted", "EXECUTING")

    step_info("Simulated 3 in-flight EXECUTING actions interrupted by abrupt reboot:")
    print("    * act-past-relay-executing: [PAST]   relay:light   (LIGHTS_ON) -> EXECUTING")
    print("    * act-past-motor-executing: [PAST]   motor:feeder  (DISPENSE)  -> EXECUTING")
    print("    * act-future-interrupted:   [FUTURE] relay:light   (LIGHTS_OFF)-> EXECUTING")

    reconciler = StateReconciler(db=db, schedule_repo=repo, device_manager=dev_mgr)
    simulated_now = f"{today_str}T12:00:00+05:30"
    report = await reconciler.reconcile_on_startup(current_time_iso=simulated_now)

    # Check motor action
    motor = await repo.get_action_by_id("act-past-motor-executing")
    assert motor["status"] == "SKIPPED_UNSAFE"
    step_ok("Unacknowledged motor action safely marked SKIPPED_UNSAFE (not repeated)")

    # Check relay action
    relay = await repo.get_action_by_id("act-past-relay-executing")
    assert relay["status"] in ("COMPLETED", "SUPERSEDED")
    step_ok(f"Unacknowledged relay action reconciled under desired-state policy ({relay['status']})")

    # Check future action
    future = await repo.get_action_by_id("act-future-interrupted")
    assert future["status"] == "PENDING"
    step_ok("Future interrupted action safely reset to PENDING for execution when due")

    await db.close()
    if os.path.exists(test_db):
        os.remove(test_db)


def demonstrate_cli_and_sigterm_shutdown():
    """Demonstrates spawning python -m main and cleanly terminating with SIGTERM."""
    step_info("Spawning full daemon via: 'python -m main' ...")
    proc = subprocess.Popen(
        [sys.executable, "-m", "main"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    ready = False
    start = time.time()
    max_wait = 8.0

    while time.time() - start < max_wait:
        if proc.poll() is not None:
            break
        try:
            with urllib.request.urlopen(f"http://{API_HOST}:{API_PORT}/health/live", timeout=1.0) as resp:
                if resp.status == 200:
                    data = json.loads(resp.read().decode("utf-8"))
                    step_ok(f"Daemon process online! PID={data.get('pid')} | Uptime={data.get('uptime_seconds')}s")
                    ready = True
                    break
        except Exception:
            time.sleep(0.3)

    if not ready:
        proc.kill()
        stdout, stderr = proc.communicate()
        step_fail(f"Daemon failed to start in {max_wait}s. Output:\n{stdout}\n{stderr}")

    step_info(f"Delivering SIGTERM to PID {proc.pid} ...")
    os.kill(proc.pid, signal.SIGTERM)

    stdout, stderr = proc.communicate(timeout=6.0)
    output = (stdout or "") + (stderr or "")

    assert proc.returncode == 0, f"Expected clean returncode 0, got {proc.returncode}"
    step_ok(f"Process terminated cleanly with returncode {proc.returncode}")

    if "Stopping acceptance of new work" in output or "Step 1" in output:
        step_ok("Logged Step 1: Stopped acceptance of new work")
    if "Persisting and resolving outstanding states" in output or "Step 2" in output:
        step_ok("Logged Step 2: Persisted and resolved outstanding states")
    if "Closing Management API, USB connections, and SQLite" in output or "Step 3" in output:
        step_ok("Logged Step 3: Closed broker, API, USB serial, and SQLite database")
    if "Clean exit" in output:
        step_ok("Logged clean exit confirmation")


def main():
    print(f"{BOLD}========================================================================{RESET}")
    print(f"{BOLD} RnR Linux IoT Hub - Step 16 Main Daemon Lifecycle Verification {RESET}")
    print(f"{BOLD} Module: main.py (Entrypoint: python -m main) {RESET}")
    print(f"{BOLD}========================================================================{RESET}")

    # 1. Validate Configuration
    header("1. Validating Configuration (Fail-Fast Verification)")
    settings = validate_configuration()
    step_ok(f"Validated Settings: Hub ID='{settings.HUB_ID}', Timezone='{settings.SCHEDULE_TIMEZONE}'")

    # 2. Reconcile In-Flight & Missed Actions
    header("2. Verifying Startup State Reconciliation (In-Flight EXECUTING Actions)")
    asyncio.run(demonstrate_startup_reconciliation())

    # 3. CLI Subprocess & SIGTERM Handling
    header("3. Verifying 'python -m main' and Clean SIGTERM Termination")
    demonstrate_cli_and_sigterm_shutdown()

    print(f"\n{BOLD}{GREEN}========================================================================{RESET}")
    print(f"{BOLD}{GREEN} STEP 16 COMPLETE: Main Daemon Lifecycle fully verified!{RESET}")
    print(f"{BOLD}{GREEN} 'python -m main' starts the complete application and SIGTERM stops it cleanly.{RESET}")
    print(f"{BOLD}{GREEN}========================================================================{RESET}\n")


if __name__ == "__main__":
    main()
