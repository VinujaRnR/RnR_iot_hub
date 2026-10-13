"""
Unit and Integration Tests for Step 15: Local Management API.
File: tests/test_step15.py

Validates the read-only FastAPI management interface:
1. GET /health/live       -> Process health (liveness probe)
2. GET /health/ready      -> Operational readiness (readiness probe; 200 vs 503)
3. GET /hub/status        -> Comprehensive hub status & subsystem health
4. GET /devices           -> Connected & registered devices list
5. GET /devices/{id}      -> Specific device details & 404 for unknown
6. GET /schedules         -> Accepted daily schedules list with status filtering
7. GET /schedules/{id}    -> Specific schedule details, action breakdown & 404
8. GET /actions           -> Pending/completed actions with multi-field filtering
9. GET /actions/{id}      -> Execution status & attempt history
10. GET /metrics          -> Operational counters & system metrics
11. Read-Only Protection  -> Mutating HTTP methods return 405 Method Not Allowed
"""

from __future__ import annotations
import asyncio
from contextlib import asynccontextmanager
import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock
import httpx
import pytest

from config import HUB_ID, SCHEDULE_TIMEZONE, API_HOST, API_PORT
from storage.database import DatabaseManager
from storage.schedule_repository import ScheduleRepository
from storage.event_outbox import EventOutbox
from devices.device_manager import DeviceManager
from devices.mock_device import MockDevice
from api.management_api import create_app


class MockRelayDevice(MockDevice):
    def __init__(self, device_id: str = "nano_relay_01", name: str = "Light Controller"):
        super().__init__(
            device_id=device_id,
            name=name,
            supported_actions=["LIGHTS_ON", "LIGHTS_OFF"],
        )
        self.channels = ["relay:light"]
        self.connection_state = "ONLINE"
        self.port = "/dev/ttyUSB0"
        self._state = {"relay:light": "OFF"}

    async def execute_action(self, action: str, parameters: dict | None = None) -> dict:
        return {"status": "SUCCESS", "action": action}


class MockSupervisor:
    def __init__(self, healthy: bool = True):
        self.storage_degraded = not healthy
        self.clock_safe = healthy
        self.quarantined = set()

    def is_device_quarantined(self, device_id: str) -> bool:
        return device_id in self.quarantined

    def get_system_metrics(self) -> dict:
        return {
            "status": "HEALTHY" if self.clock_safe and not self.storage_degraded else "DEGRADED",
            "quarantined_devices": list(self.quarantined),
            "active_alerts_count": 0,
            "backpressure_active": False,
            "clock_safe": self.clock_safe,
        }


@asynccontextmanager
async def create_api_test_env(tmp_path: Path):
    """Context manager initializing SQLite DB, repo, outbox, devices, supervisor and FastAPI test client."""
    db_path = str(tmp_path / "mgmt_test.db")
    db = DatabaseManager(db_path=db_path)
    await db.connect()
    repo = ScheduleRepository(db)
    outbox = EventOutbox(db)

    dev_mgr = DeviceManager()
    device = MockRelayDevice(device_id="98a316e6b658", name="Grow Light Relay")
    dev_mgr.register_device(device)
    await db.register_device(
        device_id="98a316e6b658",
        name="Grow Light Relay",
        capabilities={"channels": ["relay:light"], "actions": ["LIGHTS_ON", "LIGHTS_OFF"]},
        connection_state="ONLINE",
    )

    # Create supervisor
    supervisor = MockSupervisor(healthy=True)

    # Schedule mock engine
    mock_engine = MagicMock()
    mock_engine.is_running = True
    mock_engine.active_schedule_id = "sched-20261013-001"

    # Publisher mock
    mock_publisher = MagicMock()
    mock_publisher.is_connected = True

    # Seed sample schedule and actions
    schedule_id = "sched-20261013-001"
    await repo.save_schedule_with_actions(
        schedule_id=schedule_id,
        date="2026-10-13",
        revision=1,
        actions=[
            {
                "action_id": "act-001",
                "iot_node_id": "98a316e6b658",
                "channel_key": "relay:light",
                "action": "LIGHTS_ON",
                "params": "{}",
                "scheduled_at": "2026-10-13T09:00:00+05:30",
                "conflict_policy": "REJECT",
            },
            {
                "action_id": "act-002",
                "iot_node_id": "98a316e6b658",
                "channel_key": "relay:light",
                "action": "LIGHTS_OFF",
                "params": "{}",
                "scheduled_at": "2026-10-13T10:00:00+05:30",
                "conflict_policy": "REJECT",
            },
        ],
        raw_json="{}",
    )

    # Record execution for act-001
    await repo.update_action_status(schedule_id, "act-001", "COMPLETED", executed_at="2026-10-13T09:00:01+05:30")
    await repo.record_execution(
        schedule_id=schedule_id,
        action_id="act-001",
        device_id="98a316e6b658",
        command_action="LIGHTS_ON",
        attempt=1,
        status="SUCCESS",
        result={"status": "SUCCESS"},
        error_message=None,
    )

    # Outbox seed
    await outbox.enqueue(
        event_type="action_result",
        payload={"action_id": "act-001", "status": "SUCCESS"},
        routing_key="events.action_result",
        event_id="evt-001",
    )

    app = create_app(
        db=db,
        schedule_repo=repo,
        device_manager=dev_mgr,
        supervisor=supervisor,
        schedule_engine=mock_engine,
        outbox=outbox,
        publisher=mock_publisher,
    )

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://127.0.0.1:8765") as client:
        try:
            yield {
                "client": client,
                "db": db,
                "repo": repo,
                "dev_mgr": dev_mgr,
                "supervisor": supervisor,
                "outbox": outbox,
            }
        finally:
            await db.close()


@pytest.mark.asyncio
async def test_health_live_endpoint(tmp_path):
    """GET /health/live returns 200, pid, uptime and status alive."""
    async with create_api_test_env(tmp_path) as env:
        client = env["client"]
        resp = await client.get("/health/live")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "alive"
        assert data["hub_id"] == HUB_ID
        assert isinstance(data["pid"], int) and data["pid"] > 0
        assert data["uptime_seconds"] >= 0
        assert "timestamp" in data


@pytest.mark.asyncio
async def test_health_ready_healthy(tmp_path):
    """GET /health/ready returns 200 when DB and supervisor are operational."""
    async with create_api_test_env(tmp_path) as env:
        client = env["client"]
        resp = await client.get("/health/ready")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ready"
        assert data["ready"] is True
        assert data["database"] == "connected"
        assert data["supervisor_status"] == "HEALTHY"


@pytest.mark.asyncio
async def test_health_ready_unready_when_clock_unsafe(tmp_path):
    """GET /health/ready returns 503 when clock is unsafe or storage degraded."""
    async with create_api_test_env(tmp_path) as env:
        client = env["client"]
        supervisor = env["supervisor"]
        supervisor.clock_safe = False

        resp = await client.get("/health/ready")
        assert resp.status_code == 503
        data = resp.json()
        assert data["status"] == "not_ready"
        assert data["ready"] is False
        assert any("clock" in reason.lower() for reason in data["reasons"])


@pytest.mark.asyncio
async def test_hub_status_endpoint(tmp_path):
    """GET /hub/status returns full hub status with all subsystems."""
    async with create_api_test_env(tmp_path) as env:
        client = env["client"]
        resp = await client.get("/hub/status")
        assert resp.status_code == 200
        data = resp.json()
        assert data["hub_id"] == HUB_ID
        assert data["timezone"] == SCHEDULE_TIMEZONE
        assert data["status"] == "ONLINE"
        assert data["database"]["connected"] is True
        assert data["devices_count"] == 1
        assert data["scheduler"]["running"] is True
        assert data["rabbitmq"]["publisher_connected"] is True
        assert data["rabbitmq"]["outbox_backlog"] >= 1
        assert "load_1m" in data["system"]


@pytest.mark.asyncio
async def test_devices_list_endpoint(tmp_path):
    """GET /devices returns registered devices and their state."""
    async with create_api_test_env(tmp_path) as env:
        client = env["client"]
        resp = await client.get("/devices")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 1
        dev = data["devices"][0]
        assert dev["device_id"] == "98a316e6b658"
        assert dev["name"] == "Grow Light Relay"
        assert "relay:light" in dev["channels"]
        assert "LIGHTS_ON" in dev["supported_actions"]
        assert dev["connection_state"] == "ONLINE"
        assert dev["is_quarantined"] is False


@pytest.mark.asyncio
async def test_devices_get_by_id(tmp_path):
    """GET /devices/{id} returns details for existing device and 404 for missing."""
    async with create_api_test_env(tmp_path) as env:
        client = env["client"]

        # Existing device
        resp = await client.get("/devices/98a316e6b658")
        assert resp.status_code == 200
        data = resp.json()
        assert data["device_id"] == "98a316e6b658"
        assert data["channels"] == ["relay:light"]
        assert data["is_quarantined"] is False

        # Non-existent device
        resp_missing = await client.get("/devices/unknown_dev_999")
        assert resp_missing.status_code == 404
        assert "not found" in resp_missing.json()["detail"].lower()


@pytest.mark.asyncio
async def test_devices_quarantine_status(tmp_path):
    """GET /devices displays QUARANTINED state if device is isolated by supervisor."""
    async with create_api_test_env(tmp_path) as env:
        client = env["client"]
        supervisor = env["supervisor"]
        supervisor.quarantined.add("98a316e6b658")

        resp = await client.get("/devices/98a316e6b658")
        assert resp.status_code == 200
        data = resp.json()
        assert data["is_quarantined"] is True
        assert data["connection_state"] == "QUARANTINED"


@pytest.mark.asyncio
async def test_schedules_list_and_filter(tmp_path):
    """GET /schedules lists accepted schedules and respects status filter."""
    async with create_api_test_env(tmp_path) as env:
        client = env["client"]
        resp = await client.get("/schedules")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 1
        assert data["schedules"][0]["schedule_id"] == "sched-20261013-001"
        assert data["schedules"][0]["status"] == "ACTIVE"

        # Filter with non-matching status
        resp_filtered = await client.get("/schedules?status=SUPERSEDED")
        assert resp_filtered.status_code == 200
        assert resp_filtered.json()["total"] == 0


@pytest.mark.asyncio
async def test_schedule_get_by_id(tmp_path):
    """GET /schedules/{id} returns schedule header and actions breakdown."""
    async with create_api_test_env(tmp_path) as env:
        client = env["client"]
        resp = await client.get("/schedules/sched-20261013-001")
        assert resp.status_code == 200
        data = resp.json()
        assert data["schedule"]["schedule_id"] == "sched-20261013-001"
        assert data["summary"]["total_actions"] == 2
        assert data["summary"]["by_status"]["COMPLETED"] == 1
        assert data["summary"]["by_status"]["PENDING"] == 1
        assert len(data["actions"]) == 2

        # Non-existent schedule
        resp_missing = await client.get("/schedules/sched-does-not-exist")
        assert resp_missing.status_code == 404


@pytest.mark.asyncio
async def test_actions_list_and_filters(tmp_path):
    """GET /actions queries actions with optional status and device filters."""
    async with create_api_test_env(tmp_path) as env:
        client = env["client"]

        # All actions
        resp = await client.get("/actions")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 2

        # Filter by status
        resp_completed = await client.get("/actions?status=COMPLETED")
        assert resp_completed.status_code == 200
        assert resp_completed.json()["total"] == 1
        assert resp_completed.json()["actions"][0]["action_id"] == "act-001"

        # Filter by device_id
        resp_dev = await client.get("/actions?device_id=98a316e6b658")
        assert resp_dev.status_code == 200
        assert resp_dev.json()["total"] == 2


@pytest.mark.asyncio
async def test_action_get_by_id_and_executions(tmp_path):
    """GET /actions/{id} returns action details and associated execution attempts."""
    async with create_api_test_env(tmp_path) as env:
        client = env["client"]

        # Existing action with 1 execution
        resp = await client.get("/actions/act-001")
        assert resp.status_code == 200
        data = resp.json()
        assert data["action"]["action_id"] == "act-001"
        assert data["action"]["status"] == "COMPLETED"
        assert data["action"]["action"] == "LIGHTS_ON"
        assert data["executions_count"] == 1
        assert data["executions"][0]["status"] == "SUCCESS"

        # Non-existent action
        resp_missing = await client.get("/actions/act-unknown")
        assert resp_missing.status_code == 404


@pytest.mark.asyncio
async def test_metrics_endpoint(tmp_path):
    """GET /metrics returns operational counters and aggregate status."""
    async with create_api_test_env(tmp_path) as env:
        client = env["client"]
        resp = await client.get("/metrics")
        assert resp.status_code == 200
        data = resp.json()
        assert data["hub_id"] == HUB_ID
        assert data["uptime_seconds"] >= 0
        assert data["schedules_total"] == 1
        assert data["actions_total"] == 2
        assert data["actions_completed"] == 1
        assert data["actions_pending"] == 1
        assert data["executions_total"] == 1
        assert data["executions_successful"] == 1
        assert data["outbox_pending"] == 1
        assert "system_load_1m" in data


@pytest.mark.asyncio
async def test_read_only_protection(tmp_path):
    """Verifies that mutating HTTP methods (POST, PUT, DELETE) are rejected."""
    async with create_api_test_env(tmp_path) as env:
        client = env["client"]
        # Attempt POST to /actions
        resp_post = await client.post("/actions", json={"action": "OVERRIDE"})
        assert resp_post.status_code == 405

        # Attempt DELETE to /devices
        resp_del = await client.delete("/devices/98a316e6b658")
        assert resp_del.status_code == 405

        # Attempt PUT to /schedules
        resp_put = await client.put("/schedules/sched-20261013-001", json={"status": "CANCELLED"})
        assert resp_put.status_code == 405


def test_default_api_binding_configuration():
    """Verifies the API default host and port match the specification 127.0.0.1:8765."""
    assert API_HOST == "127.0.0.1"
    assert API_PORT == 8765
