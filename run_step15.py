#!/usr/bin/env python3
"""
Step 15 End-to-End Demonstration and Verification for RnR Linux IoT Hub.
File: run_step15.py

Demonstrates:
- STEP 15: Local Management API (FastAPI)
  * Read-only inspection interface bound to 127.0.0.1:8765
  * Process Health (GET /health/live)
  * Operational Readiness (GET /health/ready)
  * Full Hub Status (GET /hub/status)
  * Connected & Registered Devices (GET /devices, GET /devices/{id})
  * Accepted Daily Schedules (GET /schedules, GET /schedules/{id})
  * Pending & Completed Actions (GET /actions, GET /actions/{id})
  * Operational Counters & Metrics (GET /metrics)
  * Read-Only Protection (Mutating HTTP methods return 405)
"""

from __future__ import annotations
import asyncio
import json
import os
from pathlib import Path
import sys
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone
from unittest.mock import MagicMock

from config import HUB_ID, SCHEDULE_TIMEZONE, API_HOST, API_PORT
from storage.database import DatabaseManager
from storage.schedule_repository import ScheduleRepository
from storage.event_outbox import EventOutbox
from devices.device_manager import DeviceManager
from devices.mock_device import MockDevice
from api.management_api import create_app, start_background_api

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


class DemoRelayDevice(MockDevice):
    def __init__(self, device_id: str, name: str):
        super().__init__(
            device_id=device_id,
            name=name,
            supported_actions=["LIGHTS_ON", "LIGHTS_OFF"],
        )
        self.channels = ["relay:light"]
        self.connection_state = "ONLINE"
        self.port = "/dev/ttyUSB0"
        self._state = {"relay:light": "ON"}


class DemoSupervisor:
    def __init__(self):
        self.storage_degraded = False
        self.clock_safe = True
        self.quarantined = set()

    def is_device_quarantined(self, device_id: str) -> bool:
        return device_id in self.quarantined

    def get_system_metrics(self) -> dict:
        return {
            "status": "HEALTHY",
            "quarantined_devices": list(self.quarantined),
            "active_alerts_count": 0,
            "backpressure_active": False,
            "clock_safe": self.clock_safe,
        }


import httpx


async def http_get(client: httpx.AsyncClient, path: str) -> tuple[int, dict]:
    """Performs non-blocking async HTTP GET using httpx."""
    resp = await client.get(path, timeout=5.0)
    data = resp.json() if resp.headers.get("content-type", "").startswith("application/json") else {}
    return resp.status_code, data


async def http_post(client: httpx.AsyncClient, path: str, body: dict) -> tuple[int, dict]:
    """Performs non-blocking async HTTP POST using httpx."""
    resp = await client.post(path, json=body, timeout=5.0)
    data = resp.json() if resp.headers.get("content-type", "").startswith("application/json") else {}
    return resp.status_code, data


async def main():
    print(f"{BOLD}========================================================================{RESET}")
    print(f"{BOLD} RnR Linux IoT Hub - Step 15 Local Management API Verification {RESET}")
    print(f"{BOLD} Binding Address: http://{API_HOST}:{API_PORT} (Read-Only) {RESET}")
    print(f"{BOLD}========================================================================{RESET}")

    test_db_path = "test_step15_management.db"
    if os.path.exists(test_db_path):
        os.remove(test_db_path)

    # 1. Setup Database and Seed Records
    header("1. Initializing Storage & Populating Seed Data")
    db = DatabaseManager(db_path=test_db_path)
    await db.connect()
    repo = ScheduleRepository(db)
    outbox = EventOutbox(db)

    # Seed physical Arduino Nano relay device
    dev_mgr = DeviceManager()
    relay_device = DemoRelayDevice(device_id="98a316e6b658", name="Hydroponic Bay Lights")
    dev_mgr.register_device(relay_device)
    await db.save_device(
        device_id="98a316e6b658",
        name="Hydroponic Bay Lights",
        capabilities={"channels": ["relay:light"], "actions": ["LIGHTS_ON", "LIGHTS_OFF"]},
        connection_state="ONLINE",
    )
    step_ok("Registered Arduino Nano Relay '98a316e6b658' in device manager and SQLite database")

    # Seed daily schedule with realistic actions
    today_str = datetime.now().strftime("%Y-%m-%d")
    schedule_id = f"sched-{today_str.replace('-', '')}-001"
    actions = [
        {
            "action_id": "act-morning-on",
            "iot_node_id": "98a316e6b658",
            "channel_key": "relay:light",
            "action": "LIGHTS_ON",
            "params": "{}",
            "scheduled_at": f"{today_str}T09:00:00+05:30",
            "conflict_policy": "REJECT",
        },
        {
            "action_id": "act-afternoon-off",
            "iot_node_id": "98a316e6b658",
            "channel_key": "relay:light",
            "action": "LIGHTS_OFF",
            "params": "{}",
            "scheduled_at": f"{today_str}T14:00:00+05:30",
            "conflict_policy": "REJECT",
        },
        {
            "action_id": "act-evening-on",
            "iot_node_id": "98a316e6b658",
            "channel_key": "relay:light",
            "action": "LIGHTS_ON",
            "params": "{}",
            "scheduled_at": f"{today_str}T18:00:00+05:30",
            "conflict_policy": "REJECT",
        },
    ]

    await repo.save_schedule_with_actions(
        schedule_id=schedule_id,
        date=today_str,
        revision=1,
        actions=actions,
        raw_json=json.dumps({"schedule_id": schedule_id, "actions": actions}),
    )
    step_ok(f"Saved daily schedule '{schedule_id}' with {len(actions)} scheduled actions")

    # Mark first action completed and record execution
    exec_time = f"{today_str}T09:00:01+05:30"
    await repo.update_action_status(schedule_id, "act-morning-on", "COMPLETED", executed_at=exec_time)
    await repo.record_execution(
        schedule_id=schedule_id,
        action_id="act-morning-on",
        device_id="98a316e6b658",
        command_action="LIGHTS_ON",
        attempt=1,
        status="SUCCESS",
        result={"status": "SUCCESS", "message": "Relay contact closed"},
        error_message=None,
    )
    step_ok("Recorded successful execution history for 'act-morning-on'")

    # Seed event outbox
    await outbox.enqueue(
        event_type="action_result",
        payload={
            "action_id": "act-morning-on",
            "device_id": "98a316e6b658",
            "action": "LIGHTS_ON",
            "status": "SUCCESS",
            "scheduled_at": f"{today_str}T09:00:00+05:30",
            "executed_at": exec_time,
        },
        routing_key="events.action_result",
        event_id="evt-demo-1001",
    )
    step_ok("Queued durable event 'evt-demo-1001' in SQLite outbox")

    supervisor = DemoSupervisor()
    mock_engine = MagicMock()
    mock_engine.is_running = True
    mock_engine.active_schedule_id = schedule_id
    mock_publisher = MagicMock()
    mock_publisher.is_connected = True

    # 2. Launch FastAPI Management API Server
    header(f"2. Launching Management API Server on {API_HOST}:{API_PORT}")
    app = create_app(
        db=db,
        schedule_repo=repo,
        device_manager=dev_mgr,
        supervisor=supervisor,
        schedule_engine=mock_engine,
        outbox=outbox,
        publisher=mock_publisher,
    )

    server, server_task = start_background_api(
        host=API_HOST,
        port=API_PORT,
        application=app,
    )
    # Allow server to bind and listen
    await asyncio.sleep(0.8)
    base_url = f"http://{API_HOST}:{API_PORT}"
    step_ok(f"FastAPI Management API listening at {base_url}")

    try:
        async with httpx.AsyncClient(base_url=base_url) as client:
            # 3. Test GET /health/live
            header("3. Inspecting Process Health (GET /health/live)")
            code, body = await http_get(client, "/health/live")
            assert code == 200, f"Expected 200, got {code}"
            assert body["status"] == "alive"
            step_ok(f"Health Live Status: {body['status']} | PID: {body['pid']} | Uptime: {body['uptime_seconds']}s")

            # 4. Test GET /health/ready
            header("4. Inspecting Operational Readiness (GET /health/ready)")
            code, body = await http_get(client, "/health/ready")
            assert code == 200, f"Expected 200, got {code}"
            assert body["ready"] is True
            step_ok(f"Readiness: {body['status']} | DB: {body['database']} | Supervisor: {body['supervisor_status']}")

            # 5. Test GET /hub/status
            header("5. Inspecting Full Hub Diagnostic Status (GET /hub/status)")
            code, body = await http_get(client, "/hub/status")
            assert code == 200, f"Expected 200, got {code}"
            assert body["hub_id"] == HUB_ID
            step_ok(f"Hub ID: {body['hub_id']} | Timezone: {body['timezone']} | State: {body['status']}")
            step_ok(f"Devices Connected: {body['devices_count']} | Scheduler Active Schedule: {body['scheduler']['active_schedule_id']}")
            step_ok(f"RabbitMQ Publisher: {body['rabbitmq']['publisher_connected']} | Outbox Pending: {body['rabbitmq']['outbox_backlog']}")

            # 6. Test GET /devices
            header("6. Inspecting Connected Devices (GET /devices & GET /devices/{id})")
            code, body = await http_get(client, "/devices")
            assert code == 200, f"Expected 200, got {code}"
            assert body["total"] == 1
            dev_info = body["devices"][0]
            step_ok(f"Discovered Device: {dev_info['device_id']} ({dev_info['name']})")
            step_ok(f"Channels: {dev_info['channels']} | State: {dev_info['connection_state']} | Quarantined: {dev_info['is_quarantined']}")

            # Specific device detail
            code_d, body_d = await http_get(client, "/devices/98a316e6b658")
            assert code_d == 200, f"Expected 200, got {code_d}"
            step_ok(f"Device Specific Detail fetched successfully for '{body_d['device_id']}'")

            # 404 test for unknown device
            code_miss, _ = await http_get(client, "/devices/non_existent_node")
            assert code_miss == 404, f"Expected 404 for unknown device, got {code_miss}"
            step_ok("Verified 404 NOT FOUND for unknown device ID")

            # 7. Test GET /schedules & GET /schedules/{id}
            header("7. Inspecting Daily Schedules (GET /schedules & GET /schedules/{id})")
            code, body = await http_get(client, "/schedules")
            assert code == 200, f"Expected 200, got {code}"
            assert body["total"] == 1
            sched_entry = body["schedules"][0]
            step_ok(f"Retrieved Schedule: {sched_entry['schedule_id']} | Date: {sched_entry['date']} | Status: {sched_entry['status']}")

            # Detail with breakdown
            code_s, body_s = await http_get(client, f"/schedules/{schedule_id}")
            assert code_s == 200, f"Expected 200, got {code_s}"
            summary = body_s["summary"]
            step_ok(f"Schedule Action Summary: Total={summary['total_actions']} | Breakdown={summary['by_status']}")

            # 8. Test GET /actions & GET /actions/{id}
            header("8. Inspecting Scheduled Actions & Execution History (GET /actions)")
            code, body = await http_get(client, "/actions")
            assert code == 200, f"Expected 200, got {code}"
            assert body["total"] == 3
            for act in body["actions"]:
                step_info(f"Action: {act['action_id']:<18} | Type: {act['action']:<12} | Status: {act['status']:<10} | Time: {act['scheduled_at']}")
            step_ok(f"All {body['total']} scheduled actions inspected via HTTP")

            # Filter by status: COMPLETED
            code_filt, body_filt = await http_get(client, "/actions?status=COMPLETED")
            assert code_filt == 200 and body_filt["total"] == 1
            step_ok("Filtered actions by status=COMPLETED -> correctly returned 1 record")

            # Detail for completed action with execution history
            code_a, body_a = await http_get(client, "/actions/act-morning-on")
            assert code_a == 200, f"Expected 200, got {code_a}"
            assert body_a["executions_count"] >= 1
            exec_record = body_a["executions"][0]
            step_ok(f"Action '{body_a['action']['action_id']}' execution audit: Status={exec_record['status']} Attempt={exec_record['attempt']}")

            # 9. Test GET /metrics
            header("9. Inspecting Operational Counters (GET /metrics)")
            code, body = await http_get(client, "/metrics")
            assert code == 200, f"Expected 200, got {code}"
            step_ok(f"Metrics Report:")
            print(f"    - Schedules Total:      {body['schedules_total']}")
            print(f"    - Actions Total:        {body['actions_total']}")
            print(f"    - Actions Pending:      {body['actions_pending']}")
            print(f"    - Actions Completed:    {body['actions_completed']}")
            print(f"    - Executions Total:     {body['executions_total']}")
            print(f"    - Outbox Pending:       {body['outbox_pending']}")
            print(f"    - System Load (1m):     {body['system_load_1m']}")

            # 10. Verify Read-Only Protection (405 Method Not Allowed)
            header("10. Verifying Read-Only Security Policy")
            code_mut, _ = await http_post(client, "/actions", {"action": "FORCE_LIGHTS_ON"})
            assert code_mut == 405, f"Expected 405 Method Not Allowed, got {code_mut}"
            step_ok("Attempted mutating POST /actions correctly rejected with 405 Method Not Allowed")

    finally:
        # Gracefully stop API server
        header("11. Graceful API Server Shutdown")
        server.should_exit = True
        try:
            await asyncio.wait_for(server_task, timeout=2.0)
        except (asyncio.TimeoutError, asyncio.CancelledError):
            server_task.cancel()
        await db.close()
        if os.path.exists(test_db_path):
            os.remove(test_db_path)
        step_ok("API server and SQLite database cleanly stopped and closed")

    print(f"\n{BOLD}{GREEN}========================================================================{RESET}")
    print(f"{BOLD}{GREEN} STEP 15 COMPLETE: Local Management API fully verified!{RESET}")
    print(f"{BOLD}{GREEN} Ubuntu can inspect hub status, schedules & actions without direct process access.{RESET}")
    print(f"{BOLD}{GREEN}========================================================================{RESET}\n")


if __name__ == "__main__":
    asyncio.run(main())
