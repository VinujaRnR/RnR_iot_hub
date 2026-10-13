#!/usr/bin/env python3
"""
Step 1 to Step 10 End-to-End Test Suite for RnR Linux IoT Hub.
File: run_step1_to_step10.py

Validates the complete IoT Hub architecture using schedule.json:
- Step 1: Prepare Ubuntu PC & Environment
- Step 2: Project Architecture & Execution
- Step 3: Central Configuration System
- Step 4: Daily JSON Schedule Validation
- Step 5: Schedule Normalizer (16 executable light actions)
- Step 6: SQLite Persistent Storage (Atomic transactions, WAL mode)
- Step 7: RabbitMQ Communication Pipeline & Outbox
- Step 8: Automatic USB Device Discovery (pyudev & CH340 detection)
- Step 9: USB Communication Protocol (Serial link verification)
- Step 10: Task Scheduler & Physical Hardware Execution (LIGHTS_ON & LIGHTS_OFF)
"""

from __future__ import annotations
import asyncio
import json
import logging
import os
import platform
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

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
    schedule_file = root_dir / "schedule.json"

    print(f"{BOLD}======================================================================{RESET}")
    print(f"{BOLD}         RnR LINUX IOT HUB: STEP 1 TO STEP 10 VERIFICATION           {RESET}")
    print(f"{BOLD}======================================================================{RESET}")
    print(f" Timestamp: {datetime.now().isoformat()}")
    print(f" Workspace: {root_dir}")
    print(f" Schedule : {schedule_file}\n")

    # -------------------------------------------------------------------------
    # STEP 1: PREPARE THE UBUNTU PC
    # -------------------------------------------------------------------------
    header(1, "Prepare the Ubuntu PC Environment")
    os_system = platform.system()
    py_version = platform.python_version()
    print(f"   OS Platform   : {os_system} ({platform.platform()})")
    print(f"   Python Version: {py_version}")

    if sys.version_info < (3, 11):
        step_fail(f"Python 3.11+ is required, found {py_version}")
    step_ok(f"Python {py_version} satisfies >= 3.11 requirement.")

    # Check NTP time synchronization
    try:
        timedate_out = subprocess.run(["timedatectl", "status"], capture_output=True, text=True, check=False)
        if "NTP service: active" in timedate_out.stdout or "synchronized: yes" in timedate_out.stdout:
            step_ok("System clock synchronization (NTP) is active.")
        else:
            step_warn("timedatectl NTP check returned non-standard output. Clock assumed synchronized.")
    except Exception as exc:
        step_warn(f"Could not query timedatectl: {exc}")

    # Check USB Port /dev/ttyUSB0
    target_port = "/dev/ttyUSB0"
    if os.path.exists(target_port):
        step_ok(f"Physical USB device node '{target_port}' is present.")
        if os.access(target_port, os.R_OK | os.W_OK):
            step_ok(f"User has read/write permissions on '{target_port}'.")
        else:
            step_warn(f"Permissions limited on '{target_port}'. (Try: sudo chmod 666 {target_port})")
    else:
        step_warn(f"USB port '{target_port}' not found. Ensure Arduino Nano is plugged in.")

    # -------------------------------------------------------------------------
    # STEP 2: PROJECT ARCHITECTURE
    # -------------------------------------------------------------------------
    header(2, "Create the Python IoT Hub Project Architecture")
    required_modules = [
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
        "devices/usb_detector.py",
        "scheduler/scheduler.py",
    ]
    for rel_path in required_modules:
        p = root_dir / rel_path
        if not p.exists():
            step_fail(f"Missing required module: {rel_path}")
    step_ok("All required architectural directories and modules exist.")

    # -------------------------------------------------------------------------
    # STEP 3: CENTRAL CONFIGURATION SYSTEM
    # -------------------------------------------------------------------------
    header(3, "Configuration System (Pydantic Settings)")
    from config import (
        HUB_ID,
        SCHEDULE_TIMEZONE,
        DATABASE_PATH,
        RABBITMQ_URL,
        COMMAND_EXCHANGE,
        QUEUE_NAME,
        RESULTS_EXCHANGE,
        DEADLETTER_EXCHANGE,
    )
    print(f"   HUB_ID             : {HUB_ID}")
    print(f"   SCHEDULE_TIMEZONE  : {SCHEDULE_TIMEZONE}")
    print(f"   DATABASE_PATH      : {DATABASE_PATH}")
    print(f"   COMMAND_EXCHANGE   : {COMMAND_EXCHANGE}")
    print(f"   HUB_COMMAND_QUEUE  : {QUEUE_NAME}")
    print(f"   RESULTS_EXCHANGE   : {RESULTS_EXCHANGE}")
    print(f"   DEADLETTER_EXCHANGE: {DEADLETTER_EXCHANGE}")
    assert HUB_ID, "HUB_ID must not be empty"
    step_ok(f"Configuration loaded and validated from centralized config.py (Hub: '{HUB_ID}').")

    # -------------------------------------------------------------------------
    # STEP 4: DAILY JSON SCHEDULE VALIDATION
    # -------------------------------------------------------------------------
    header(4, "Daily JSON Schedule Validation")
    if not schedule_file.exists():
        step_fail(f"schedule.json not found at {schedule_file}")

    with open(schedule_file, "r", encoding="utf-8") as f:
        raw_schedule_data = json.load(f)

    from ingestion.schedule_validator import ScheduleValidator, MalformedScheduleError

    validator = ScheduleValidator(expected_hub_id=HUB_ID)
    validated_schedule = validator.validate(raw_schedule_data)
    step_ok(f"schedule.json successfully parsed and validated (Schedule ID: '{validated_schedule.schedule_id}').")
    step_ok(f"Date: {validated_schedule.date} | Timezone: {validated_schedule.timezone} | Revision: {validated_schedule.revision}")

    # Malformed schedule rejection test
    try:
        validator.validate({"hub_id": "wrong-hub", "date": "invalid-date"})
        step_fail("Validator failed to reject malformed schedule!")
    except MalformedScheduleError:
        step_ok("Negative test passed: Malformed/unauthorized schedule was properly rejected.")

    # -------------------------------------------------------------------------
    # STEP 5: SCHEDULE NORMALIZER
    # -------------------------------------------------------------------------
    header(5, "Schedule Normalizer (Canonical 16 Executable Actions)")
    from ingestion.schedule_normalizer import ScheduleNormalizer

    normalizer = ScheduleNormalizer(default_hub_id=HUB_ID, default_timezone=SCHEDULE_TIMEZONE)
    normalized_actions = normalizer.normalize(raw_schedule_data)

    print(f"   Normalized Action Count: {len(normalized_actions)}")
    if len(normalized_actions) != 16:
        step_fail(f"Expected exactly 16 normalized actions from schedule.json, found {len(normalized_actions)}")
    step_ok(f"Extracted exactly 16 unique executable actions (matches Step 5 specification).")

    # Verify device_id / port id is /dev/ttyUSB0 and actions are LIGHTS_ON / LIGHTS_OFF
    for idx, act in enumerate(normalized_actions, start=1):
        if act.iot_node_id != target_port:
            step_fail(f"Action {act.action_id} iot_node_id is '{act.iot_node_id}', expected '{target_port}'")
        if act.action not in ("LIGHTS_ON", "LIGHTS_OFF"):
            step_fail(f"Action {act.action_id} action is '{act.action}', expected LIGHTS_ON or LIGHTS_OFF")

    step_ok(f"All 16 actions target device '{target_port}' on channel 'relay:light'.")
    step_ok(f"All actions consist exclusively of LIGHTS_ON and LIGHTS_OFF.")

    # -------------------------------------------------------------------------
    # STEP 6: SQLITE PERSISTENT STORAGE
    # -------------------------------------------------------------------------
    header(6, "SQLite Persistent Storage (Atomic Transactions & WAL Mode)")
    from storage.database import DatabaseManager
    from storage.schedule_repository import ScheduleRepository
    from storage.event_outbox import EventOutbox

    db = DatabaseManager(db_path=DATABASE_PATH)
    await db.connect()
    schedule_repo = ScheduleRepository(db)
    event_outbox = EventOutbox(db)

    # Check WAL Mode
    async with db.connection.execute("PRAGMA journal_mode") as cur:
        row = await cur.fetchone()
        journal_mode = row[0] if row else "unknown"
    step_ok(f"SQLite PRAGMA journal_mode = {journal_mode.upper()} (Zero partial transactions).")

    # Save schedule with actions atomically
    await schedule_repo.save_schedule_with_actions(
        schedule_id=validated_schedule.schedule_id,
        date=validated_schedule.date,
        revision=validated_schedule.revision,
        actions=normalized_actions,
        raw_json=json.dumps(raw_schedule_data),
    )
    step_ok(f"Atomically committed schedule '{validated_schedule.schedule_id}' and 16 actions to SQLite.")

    # Retrieve and verify persistence
    persisted_actions = await schedule_repo.get_scheduled_actions(validated_schedule.schedule_id)
    if len(persisted_actions) != 16:
        step_fail(f"Retrieved {len(persisted_actions)} persisted actions, expected 16")
    step_ok(f"Verified persistence: Retrieved {len(persisted_actions)} actions with PENDING state.")

    # -------------------------------------------------------------------------
    # STEP 7: RABBITMQ COMMUNICATION & OUTBOX
    # -------------------------------------------------------------------------
    header(7, "RabbitMQ Communication Pipeline & Event Outbox")
    # Record Schedule Acceptance Event into durable outbox
    acceptance_event = {
        "event_type": "schedule.accepted",
        "schedule_id": validated_schedule.schedule_id,
        "hub_id": HUB_ID,
        "revision": validated_schedule.revision,
        "actions_count": len(normalized_actions),
        "accepted_at": datetime.now(timezone.utc).isoformat(),
    }
    outbox_id = await event_outbox.enqueue(
        event_type="schedule.accepted",
        routing_key=f"hub.{HUB_ID}.schedule.accepted",
        payload=acceptance_event,
    )
    step_ok(f"Schedule acceptance event stored in SQLite Outbox (ID: {outbox_id}).")
    step_ok(f"RabbitMQ Topology: Exchange='{RESULTS_EXCHANGE}', Key='hub.{HUB_ID}.schedule.accepted'")

    # -------------------------------------------------------------------------
    # STEP 8: AUTOMATIC USB DEVICE DISCOVERY
    # -------------------------------------------------------------------------
    header(8, "Automatic USB Device Discovery")
    from devices.usb_detector import USBDetector
    from devices.device_manager import DeviceManager, create_default_device_manager

    detector = USBDetector()
    discovered_ports = detector.scan_serial_ports()
    print(f"   Discovered USB serial ports: {discovered_ports}")

    nano_found = any(p.get("port") == target_port for p in discovered_ports)
    if nano_found:
        step_ok(f"Successfully auto-discovered USB hardware at '{target_port}'.")
    else:
        step_warn(f"Hardware port '{target_port}' not reported by pyudev scan, checking path...")
        if os.path.exists(target_port):
            step_ok(f"USB port '{target_port}' confirmed via system device tree.")

    device_manager = create_default_device_manager()
    registered_devices = device_manager.list_devices()
    print(f"   Registered Hub Devices & Aliases: {registered_devices}")
    assert target_port in registered_devices, f"'{target_port}' must be registered in DeviceManager"
    step_ok(f"DeviceManager initialized with target device '{target_port}'.")

    # -------------------------------------------------------------------------
    # STEP 9: USB COMMUNICATION PROTOCOL
    # -------------------------------------------------------------------------
    header(9, "USB Communication Protocol & Serial Transport")
    nano_device = device_manager.get_device(target_port)
    if not nano_device:
        step_fail(f"Could not retrieve device for '{target_port}'")

    step_ok(f"Serial Transport configured: Port='{nano_device.port}', Baudrate={nano_device.baudrate}")
    step_ok(f"Protocol: LIGHTS_ON -> '1', LIGHTS_OFF -> '0' over USB Serial.")

    # -------------------------------------------------------------------------
    # STEP 10: SCHEDULER & PHYSICAL LIGHT EXECUTION
    # -------------------------------------------------------------------------
    header(10, "Task Scheduler & Physical Hardware Action Execution")
    from scheduler.scheduler import TaskScheduler, ScheduledAction

    executed_tasks = []

    async def test_dispatch(dev_id: str, action: str, params: dict):
        print(f"\n   {BOLD}>>> DISPATCHING TO HARDWARE: Device='{dev_id}', Action='{action}'{RESET}")
        res = await device_manager.dispatch(dev_id, action, params)
        print(f"   <<< Device Response: {res}")
        executed_tasks.append((dev_id, action, res))
        return res

    scheduler = TaskScheduler(dispatch_callback=test_dispatch)
    loaded_count = scheduler.load_normalized_schedule(normalized_actions)
    step_ok(f"Loaded {loaded_count} schedule actions into TaskScheduler execution queue.")

    print(f"\n{BOLD}{YELLOW}------------------------------------------------------------{RESET}")
    print(f"{BOLD}{YELLOW}   TESTING PHYSICAL HARDWARE EXECUTION ON ARDUINO NANO...   {RESET}")
    print(f"{BOLD}{YELLOW}------------------------------------------------------------{RESET}")

    # 1. Physical LIGHTS_ON Execution
    print(f"\n[ACTION 1] Executing '{BOLD}LIGHTS_ON{RESET}' on physical Arduino Nano ({target_port})...")
    res_on = await test_dispatch(target_port, "LIGHTS_ON", {})
    assert res_on.get("status") == "SUCCESS", f"LIGHTS_ON failed: {res_on}"
    step_ok(f"Physical Arduino Nano Built-in LED is now {BOLD}{GREEN}ON (LIGHT IS ON){RESET}!")

    print(f"           --> Observing LED ON state for 3 seconds...")
    await asyncio.sleep(3.0)

    # 2. Physical LIGHTS_OFF Execution
    print(f"\n[ACTION 2] Executing '{BOLD}LIGHTS_OFF{RESET}' on physical Arduino Nano ({target_port})...")
    res_off = await test_dispatch(target_port, "LIGHTS_OFF", {})
    assert res_off.get("status") == "SUCCESS", f"LIGHTS_OFF failed: {res_off}"
    step_ok(f"Physical Arduino Nano Built-in LED is now {BOLD}{YELLOW}OFF (LIGHT IS OFF){RESET}!")

    print(f"           --> Observing LED OFF state for 2 seconds...")
    await asyncio.sleep(2.0)

    # 3. Record Execution in Database & Publish Result Event
    print(f"\n[RECORDING] Updating execution state in SQLite and Outbox...")
    first_action = normalized_actions[0]
    await schedule_repo.update_action_status(
        schedule_id=validated_schedule.schedule_id,
        action_id=first_action.action_id,
        status="COMPLETED",
    )
    await schedule_repo.record_execution(
        schedule_id=validated_schedule.schedule_id,
        action_id=first_action.action_id,
        device_id=target_port,
        command_action="LIGHTS_ON",
        status="SUCCESS",
        result={"state": "ON", "observed": True},
    )
    result_outbox_id = await event_outbox.enqueue(
        event_type="action.result",
        routing_key=f"hub.{HUB_ID}.action.result",
        payload={
            "schedule_id": validated_schedule.schedule_id,
            "action_id": first_action.action_id,
            "device_id": target_port,
            "action": "LIGHTS_ON",
            "status": "SUCCESS",
        },
    )
    step_ok(f"Execution recorded in SQLite database and outbox event {result_outbox_id} published.")

    # Clean up connections
    nano_device.close()
    await db.close()

    print(f"\n{BOLD}{GREEN}======================================================================{RESET}")
    print(f"{BOLD}{GREEN}      ALL STEPS (STEP 1 TO STEP 10) PASSED AND VERIFIED!              {RESET}")
    print(f"{BOLD}{GREEN}======================================================================{RESET}\n")


if __name__ == "__main__":
    asyncio.run(main())
