"""
Unit and Integration Tests for Step 16: Main Daemon Lifecycle.
File: tests/test_step16.py

Validates:
1. Configuration validation on startup (fail-fast behavior).
2. Restart recovery of in-flight/EXECUTING actions without definitive acknowledgment.
3. Complete asynchronous daemon lifecycle and cancellation-aware task coordination.
4. Graceful shutdown sequence (stop work, persist states, close broker/USB/DB, clean exit).
5. 'python -m main' subprocess launch and clean termination on SIGTERM (exit code 0).
"""

from __future__ import annotations
import asyncio
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from unittest.mock import patch
import httpx
import pytest

from config import HUB_ID, API_HOST, API_PORT
from main import validate_configuration, run_hub
from storage.database import DatabaseManager
from storage.schedule_repository import ScheduleRepository
from devices.device_manager import DeviceManager
from scheduler.state_reconciler import StateReconciler


def test_step16_validate_configuration_success():
    """Validates configuration succeeds under normal environment variables."""
    settings = validate_configuration()
    assert settings.HUB_ID == HUB_ID
    assert settings.API_PORT == API_PORT


def test_step16_validate_configuration_fail_fast(monkeypatch):
    """Validates configuration fails fast with SystemExit(1) on invalid settings."""
    monkeypatch.setenv("API_PORT", "999999")  # Invalid port
    with pytest.raises(SystemExit) as exc:
        validate_configuration()
    assert exc.value.code == 1


@pytest.mark.asyncio
async def test_step16_reconcile_in_flight_executing_actions(tmp_path: Path):
    """
    Step 16 requirement:
    'After restart, any action recorded as EXECUTING without a definitive acknowledgment
    must be reconciled rather than automatically marked successful.'
    """
    db_path = str(tmp_path / "reconcile_executing.db")
    db = DatabaseManager(db_path=db_path)
    await db.connect()
    repo = ScheduleRepository(db)
    dev_mgr = DeviceManager()
    from devices.mock_device import MockDevice
    relay_dev = MockDevice("relay-node-01", "Relay Node", supported_actions=["LIGHTS_ON", "LIGHTS_OFF"])
    relay_dev.channels = ["relay:light"]
    dev_mgr.register_device(relay_dev)

    # Create schedule with 1 past EXECUTING relay action, 1 past EXECUTING motor action, 1 future EXECUTING action
    sched_id = "sched-recovery-test"
    actions = [
        {
            "action_id": "act-past-relay-executing",
            "iot_node_id": "relay-node-01",
            "channel_key": "relay:light",
            "action": "LIGHTS_ON",
            "params": "{}",
            "scheduled_at": "2026-10-10T09:00:00+05:30",
            "conflict_policy": "REJECT",
        },
        {
            "action_id": "act-past-motor-executing",
            "iot_node_id": "motor-node-01",
            "channel_key": "motor:feeder",
            "action": "DISPENSE",
            "params": "{}",
            "scheduled_at": "2026-10-10T09:05:00+05:30",
            "conflict_policy": "REJECT",
        },
        {
            "action_id": "act-future-executing",
            "iot_node_id": "relay-node-01",
            "channel_key": "relay:light",
            "action": "LIGHTS_OFF",
            "params": "{}",
            "scheduled_at": "2026-10-25T18:00:00+05:30",
            "conflict_policy": "REJECT",
        },
    ]

    await repo.save_schedule_with_actions(
        schedule_id=sched_id,
        date="2026-10-10",
        revision=1,
        actions=actions,
        raw_json="{}",
    )

    # Simulate abrupt power failure while actions were in EXECUTING state
    await repo.update_action_status(sched_id, "act-past-relay-executing", "EXECUTING")
    await repo.update_action_status(sched_id, "act-past-motor-executing", "EXECUTING")
    await repo.update_action_status(sched_id, "act-future-executing", "EXECUTING")

    # Run StateReconciler with current simulated time 2026-10-13T10:00:00+05:30
    reconciler = StateReconciler(db=db, schedule_repo=repo, device_manager=dev_mgr)
    report = await reconciler.reconcile_on_startup(current_time_iso="2026-10-13T10:00:00+05:30")

    # 1. Past motor action must NOT be marked successful; it must be SKIPPED_UNSAFE!
    motor_act = await repo.get_action_by_id("act-past-motor-executing")
    assert motor_act["status"] == "SKIPPED_UNSAFE"
    assert "act-past-motor-executing" in report.skipped_unsafe_actions

    # 2. Past relay action was reconciled under desired-state
    relay_act = await repo.get_action_by_id("act-past-relay-executing")
    assert relay_act["status"] in ("COMPLETED", "SUPERSEDED")

    # 3. Future action was reset from EXECUTING back to PENDING
    future_act = await repo.get_action_by_id("act-future-executing")
    assert future_act["status"] == "PENDING"
    assert report.future_pending_count == 1

    await db.close()


@pytest.mark.asyncio
async def test_step16_daemon_lifecycle_in_process():
    """
    Tests full daemon start and clean programmatic shutdown sequence in-process.
    """
    stop_event = asyncio.Event()

    # Launch daemon in background task
    hub_task = asyncio.create_task(run_hub(stop_override_event=stop_event))

    # Wait for API server to come online
    client = httpx.AsyncClient(base_url=f"http://{API_HOST}:{API_PORT}")
    max_wait = 6.0
    start = time.time()
    online = False

    while time.time() - start < max_wait:
        try:
            resp = await client.get("/health/live", timeout=1.0)
            if resp.status_code == 200:
                online = True
                break
        except Exception:
            await asyncio.sleep(0.3)

    assert online, "Hub daemon did not become online within 6 seconds"

    # Query status
    status_resp = await client.get("/hub/status")
    assert status_resp.status_code == 200
    assert status_resp.json()["status"] == "ONLINE"
    await client.aclose()

    # Request graceful shutdown
    stop_event.set()

    # Await daemon completion; must exit cleanly
    await asyncio.wait_for(hub_task, timeout=5.0)
    assert hub_task.done() and not hub_task.cancelled()


def test_step16_python_m_main_sigterm_clean_exit():
    """
    Step 16 acceptance test:
    'python -m main starts the complete application and SIGTERM stops it cleanly.'
    """
    # Spawn python -m main as a real subprocess
    proc = subprocess.Popen(
        [sys.executable, "-m", "main"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    try:
        # Wait for daemon to be ready by checking live endpoint over HTTP
        start = time.time()
        ready = False
        while time.time() - start < 8.0:
            if proc.poll() is not None:
                # Exited prematurely
                break
            try:
                import urllib.request
                with urllib.request.urlopen(f"http://{API_HOST}:{API_PORT}/health/live", timeout=1.0) as resp:
                    if resp.status == 200:
                        ready = True
                        break
            except Exception:
                time.sleep(0.4)

        assert ready, f"Daemon failed to start. Returncode: {proc.poll()}"

        # Send SIGTERM to process
        os.kill(proc.pid, signal.SIGTERM)

        # Wait for process to exit cleanly
        stdout, stderr = proc.communicate(timeout=6.0)

        output = (stdout or "") + (stderr or "")
        assert proc.returncode == 0, f"Expected returncode 0 on SIGTERM, got {proc.returncode}. Output: {output}"
        assert "Stopping acceptance of new work" in output or "Shutting down" in output or "clean exit" in output.lower()

    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait()
