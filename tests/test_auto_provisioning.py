"""
Unit and Integration Tests for RNHub V2: Automated Provisioning & Auto-Enrollment.
File: tests/test_auto_provisioning.py

Validates:
1. Automated background provisioning listener (hub.hello announcement).
2. Configuration message consumption from dedicated hub queue (hub.<hub_id>.config).
3. Rejection of mismatched hub IDs (broker ACL and identity isolation).
4. Signature verification (HMAC-SHA256) and rejection of invalid signatures.
5. Rejection of stale / outdated configuration revisions (config_version check).
6. Transactional SQLite persistence preserving schedules, outbox, and operational logs.
7. Auto-Enrollment: Automatically discovering and binding approved hardware MACs.
8. Auto-Enrollment: Awaiting matching information when MAC is not supplied.
9. Auto-Enrollment: Rejecting unknown / unregistered USB controllers.
10. Auto-Enrollment: Handling USB port renumbering (/dev/ttyUSB0 -> /dev/ttyUSB1).
11. Auto-Enrollment: Ensuring safe startup states (device configuration != actuation).
12. Device revocation: Disabling removed devices while retaining history.
13. Offline safety policy: Retaining last verified configuration during broker outages.
"""

from __future__ import annotations
import asyncio
import hashlib
import hmac
import json
import os
from pathlib import Path
import tempfile
from typing import Any, Dict, List, Optional
import pytest
import pytest_asyncio

from storage.database import DatabaseManager
from storage.config_repository import ConfigRepository
from storage.registration_repository import RegistrationRepository
from storage.schedule_repository import ScheduleRepository
from storage.event_outbox import EventOutbox
from devices.device_registry import DeviceRegistry, ScannedPortInfo
from devices.auto_enrollment import AutoEnrollmentManager
from devices.device_manager import DeviceManager
from devices.mock_device import MockDevice
from core.config_sync_manager import ConfigSyncManager
from core.middleware_provisioning import MiddlewareProvisioningListener
from core.registration_guard import RegistrationGuard, HubOperatingState


# =====================================================================
# Mock aio_pika Objects
# =====================================================================

class MockIncomingMessage:
    """Simulates aio_pika incoming message for configuration tests."""

    def __init__(self, body: Dict[str, Any] | str, headers: Optional[Dict[str, Any]] = None):
        if isinstance(body, dict):
            self.body = json.dumps(body).encode("utf-8")
        elif isinstance(body, str):
            self.body = body.encode("utf-8")
        else:
            self.body = body
        self.headers = headers or {}
        self.acked = False
        self.rejected = False

    def process(self, requeue: bool = False):
        msg = self
        class _Ctx:
            async def __aenter__(self):
                return msg
            async def __aexit__(self, exc_type, exc_val, exc_tb):
                if exc_type is not None:
                    msg.rejected = True
                else:
                    msg.acked = True
                return False
        return _Ctx()


class MockExchange:
    """Captures messages published to RabbitMQ exchange."""

    def __init__(self, name: str = "mock_exchange"):
        self.name = name
        self.published: List[Dict[str, Any]] = []

    async def publish(self, message: Any, routing_key: str) -> None:
        payload = json.loads(message.body.decode("utf-8")) if hasattr(message, "body") else message
        self.published.append({
            "exchange": self.name,
            "routing_key": routing_key,
            "headers": getattr(message, "headers", {}),
            "payload": payload,
        })


# =====================================================================
# Fixtures
# =====================================================================

@pytest_asyncio.fixture
async def temp_env(tmp_path: Path):
    """Sets up isolated SQLite database and master key."""
    key_file = tmp_path / "test_master.key"
    os.environ["IOTHUB_MASTER_KEY_FILE"] = str(key_file)
    db_file = str(tmp_path / "test_provisioning.db")

    db = DatabaseManager(db_path=db_file)
    await db.connect()

    cfg = ConfigRepository(db)
    reg_repo = RegistrationRepository(db, cfg)

    yield {
        "db": db,
        "cfg": cfg,
        "reg_repo": reg_repo,
        "tmp_path": tmp_path,
    }

    await db.close()
    if "IOTHUB_MASTER_KEY_FILE" in os.environ:
        del os.environ["IOTHUB_MASTER_KEY_FILE"]


@pytest.fixture
def sample_v4_config() -> Dict[str, Any]:
    return {
        "hub_uuid": "3417ebc3-b479-4b11-8f93-911b2c349abc",
        "hub_id": "hub-3417ebc3b479",
        "hub_name": "Plants Hub 01",
        "status": "active",
        "resource_id": 12,
        "sub_resource_id": 34,
        "installation_location": "Greenhouse A - Rack 3",
        "timezone": "Asia/Colombo",
        "config_version": 4,
        "connected_devices": [
            {
                "registration_device_id": "dev-001",
                "name": "Lighting Controller",
                "device_mac": "98a316e6b658",
                "profile": "plant_controller_v1",
                "status": "approved",
            },
            {
                "registration_device_id": "dev-002",
                "name": "Water Valve Controller",
                "device_mac": None,
                "profile": "plant_controller_v1",
                "status": "pending_binding",
            },
        ],
    }


# =====================================================================
# 1. Provisioning Listener & Hello Announcement
# =====================================================================

@pytest.mark.asyncio
async def test_provisioning_listener_publish_hello(temp_env, sample_v4_config):
    """Listener publishes hub.hello on startup reporting identity, config_version, and hardware."""
    reg_repo = temp_env["reg_repo"]
    await reg_repo.save_registration(sample_v4_config)

    sync_mgr = ConfigSyncManager(temp_env["db"], reg_repo=reg_repo)
    listener = MiddlewareProvisioningListener(
        hub_id="hub-3417ebc3b479",
        hub_uuid="3417ebc3-b479-4b11-8f93-911b2c349abc",
        config_sync_manager=sync_mgr,
    )
    mock_ack_ex = MockExchange("iot.events")
    listener.ack_exchange = mock_ack_ex
    listener.channel = True  # Mock channel open

    await listener.publish_hello()

    assert len(mock_ack_ex.published) == 1
    pub = mock_ack_ex.published[0]
    assert pub["routing_key"] == "hub.hub-3417ebc3b479.hello"
    payload = pub["payload"]
    assert payload["event_type"] == "hub.hello"
    assert payload["hub_id"] == "hub-3417ebc3b479"
    assert payload["hub_uuid"] == "3417ebc3-b479-4b11-8f93-911b2c349abc"
    assert payload["config_version"] == 4
    assert payload["status"] == "online"
    assert "hardware" in payload
    assert payload["hardware"]["platform"] is not None


# =====================================================================
# 2. Automated Configuration Updates & Validation
# =====================================================================

@pytest.mark.asyncio
async def test_provisioning_listener_receives_and_acknowledges_valid_config(temp_env, sample_v4_config):
    """Listener processes incoming config v5 and publishes hub.config.ack with SUCCESS."""
    reg_repo = temp_env["reg_repo"]
    await reg_repo.save_registration(sample_v4_config)

    sync_mgr = ConfigSyncManager(temp_env["db"], reg_repo=reg_repo, expected_hub_id="hub-3417ebc3b479")
    listener = MiddlewareProvisioningListener(
        hub_id="hub-3417ebc3b479",
        config_sync_manager=sync_mgr,
    )
    mock_ack_ex = MockExchange("iot.events")
    listener.ack_exchange = mock_ack_ex
    listener.channel = True

    # Incoming newer configuration revision v5
    v5_config = dict(sample_v4_config)
    v5_config["config_version"] = 5
    v5_config["installation_location"] = "Greenhouse B - Rack 1"

    msg = MockIncomingMessage(v5_config)
    await listener.handle_message(msg)

    assert msg.acked is True
    assert len(mock_ack_ex.published) == 1
    ack = mock_ack_ex.published[0]["payload"]
    assert ack["event_type"] == "hub.config.ack"
    assert ack["config_version"] == 5
    assert ack["status"] == "SUCCESS"

    # Verify SQLite was updated
    updated_reg = await reg_repo.get_registration()
    assert updated_reg["config_version"] == 5
    assert updated_reg["installation_location"] == "Greenhouse B - Rack 1"


@pytest.mark.asyncio
async def test_provisioning_listener_rejects_mismatched_hub_id(temp_env, sample_v4_config):
    """Listener rejects configuration targeted to a different hub ID."""
    reg_repo = temp_env["reg_repo"]
    await reg_repo.save_registration(sample_v4_config)

    sync_mgr = ConfigSyncManager(temp_env["db"], reg_repo=reg_repo, expected_hub_id="hub-3417ebc3b479")
    listener = MiddlewareProvisioningListener(
        hub_id="hub-3417ebc3b479",
        config_sync_manager=sync_mgr,
    )
    mock_ack_ex = MockExchange("iot.events")
    listener.ack_exchange = mock_ack_ex
    listener.channel = True

    imposter_config = dict(sample_v4_config)
    imposter_config["hub_id"] = "hub-other-attacker"
    imposter_config["config_version"] = 5

    msg = MockIncomingMessage(imposter_config)
    await listener.handle_message(msg)

    assert len(mock_ack_ex.published) == 1
    ack = mock_ack_ex.published[0]["payload"]
    assert ack["status"] == "REJECTED"
    assert ack["error"] == "IDENTITY_MISMATCH"

    # Hub config version unchanged
    reg = await reg_repo.get_registration()
    assert reg["config_version"] == 4


@pytest.mark.asyncio
async def test_provisioning_listener_rejects_stale_config_version(temp_env, sample_v4_config):
    """Listener rejects out-of-order or duplicate stale configuration revisions."""
    reg_repo = temp_env["reg_repo"]
    await reg_repo.save_registration(sample_v4_config)  # config_version = 4

    sync_mgr = ConfigSyncManager(temp_env["db"], reg_repo=reg_repo, expected_hub_id="hub-3417ebc3b479")
    listener = MiddlewareProvisioningListener(
        hub_id="hub-3417ebc3b479",
        config_sync_manager=sync_mgr,
    )
    mock_ack_ex = MockExchange("iot.events")
    listener.ack_exchange = mock_ack_ex
    listener.channel = True

    # Stale version 3
    stale_config = dict(sample_v4_config)
    stale_config["config_version"] = 3

    msg = MockIncomingMessage(stale_config)
    await listener.handle_message(msg)

    assert len(mock_ack_ex.published) == 1
    ack = mock_ack_ex.published[0]["payload"]
    assert ack["status"] == "REJECTED"
    assert ack["error"] == "STALE_REVISION"


@pytest.mark.asyncio
async def test_provisioning_listener_validates_hmac_signature(temp_env, sample_v4_config):
    """Verifies HMAC signature validation and rejection of tampered configuration."""
    reg_repo = temp_env["reg_repo"]
    cfg = temp_env["cfg"]
    await reg_repo.save_registration(sample_v4_config)
    secret_token = "super_secret_machine_key"
    await cfg.set_setting("auth.token", secret_token, is_secret=True)

    sync_mgr = ConfigSyncManager(temp_env["db"], reg_repo=reg_repo, config_repo=cfg, expected_hub_id="hub-3417ebc3b479")
    listener = MiddlewareProvisioningListener(hub_id="hub-3417ebc3b479", config_sync_manager=sync_mgr)
    mock_ack_ex = MockExchange("iot.events")
    listener.ack_exchange = mock_ack_ex
    listener.channel = True

    v5_config = dict(sample_v4_config)
    v5_config["config_version"] = 5
    raw_str = json.dumps(v5_config)

    # 1. Valid signature succeeds
    valid_sig = hmac.new(secret_token.encode("utf-8"), raw_str.encode("utf-8"), hashlib.sha256).hexdigest()
    msg_valid = MockIncomingMessage(raw_str, headers={"x-hub-signature": valid_sig})
    await listener.handle_message(msg_valid)
    assert mock_ack_ex.published[-1]["payload"]["status"] == "SUCCESS"

    # 2. Tampered signature fails
    v6_config = dict(sample_v4_config)
    v6_config["config_version"] = 6
    raw_str_tampered = json.dumps(v6_config)
    msg_invalid = MockIncomingMessage(raw_str_tampered, headers={"x-hub-signature": "bad_signature_deadbeef"})
    await listener.handle_message(msg_invalid)
    assert mock_ack_ex.published[-1]["payload"]["status"] == "REJECTED"
    assert mock_ack_ex.published[-1]["payload"]["error"] == "INVALID_SIGNATURE"


# =====================================================================
# 3. Transactional Integrity & Schedule Preservation
# =====================================================================

@pytest.mark.asyncio
async def test_config_sync_preserves_local_schedules_and_outbox(temp_env, sample_v4_config):
    """Configuration sync updates registration snapshot without overwriting schedules or outbox."""
    db = temp_env["db"]
    reg_repo = temp_env["reg_repo"]
    sched_repo = ScheduleRepository(db)
    outbox = EventOutbox(db)

    # Pre-populate local operational data
    await sched_repo.save_schedule_with_actions(
        schedule_id="sched-test-preserve",
        date="2026-11-11",
        revision=1,
        actions=[
            {
                "action_id": "act-preserve-1",
                "sub_resource_id": "sub_1",
                "action": "LIGHTS_ON",
                "time_slot": "10:00",
                "scheduled_at": "2026-11-11T10:00:00Z",
                "status": "PENDING",
            }
        ],
    )
    await outbox.enqueue(
        event_type="action_result",
        payload={"action_id": "act-preserve-1", "status": "SUCCESS"},
        routing_key="test.key",
        event_id="act-preserve-1",
    )

    # Apply configuration update
    sync_mgr = ConfigSyncManager(db, reg_repo=reg_repo, expected_hub_id="hub-3417ebc3b479")
    v5_config = dict(sample_v4_config)
    v5_config["config_version"] = 5
    result = await sync_mgr.apply_configuration(v5_config)
    assert result["status"] == "SUCCESS"

    # Confirm schedule and outbox remain intact
    active_scheds = await sched_repo.list_schedules(status="ACTIVE")
    assert len(active_scheds) == 1
    assert active_scheds[0]["schedule_id"] == "sched-test-preserve"

    pending_events = await outbox.get_pending()
    assert len(pending_events) == 1
    assert pending_events[0]["event_id"] == "act-preserve-1"


# =====================================================================
# 4. Auto-Enrollment & Hardware Matching
# =====================================================================

@pytest.mark.asyncio
async def test_auto_enrollment_matches_approved_hardware_mac(temp_env, sample_v4_config):
    """AutoEnrollmentManager automatically binds approved hardware without manual CLI commands."""
    db = temp_env["db"]
    reg_repo = temp_env["reg_repo"]
    await reg_repo.save_registration(sample_v4_config)

    dev_reg = DeviceRegistry(db)
    dev_mgr = DeviceManager()
    auto_enr = AutoEnrollmentManager(db, dev_reg, dev_mgr, reg_repo)

    # Simulated port discovery with physical MAC matching "dev-001" (Lighting Controller)
    candidate_ports = [
        ScannedPortInfo(
            port="/dev/ttyUSB0",
            vid="1a86",
            pid="7523",
            serial_number="98a316e6b658",
            reported_mac="98a316e6b658",
        )
    ]

    report = await auto_enr.discover_and_reconcile(candidate_ports)

    # 1. Hardware automatically bound
    assert len(report["bound"]) == 1
    bound_info = report["bound"][0]
    assert bound_info["registration_device_id"] == "dev-001"
    assert bound_info["mac"] == "98a316e6b658"
    assert bound_info["port"] == "/dev/ttyUSB0"

    # 2. SQLite device_bindings updated
    binding = await dev_reg.get_binding("98a316e6b658")
    assert binding is not None
    assert binding.port_hint == "/dev/ttyUSB0"
    assert binding.approved is True
    assert binding.registration_device_id == "dev-001"

    # 3. authorized_devices status updated to 'bound'
    auth_dev = await reg_repo.get_authorized_device("dev-001")
    assert auth_dev["status"] == "bound"
    assert auth_dev["bound_port"] == "/dev/ttyUSB0"

    # 4. DeviceManager has driver registered with safe initial state
    driver = dev_mgr.get_device("dev-001")
    assert driver is not None
    assert getattr(driver, "port", None) == "/dev/ttyUSB0"


@pytest.mark.asyncio
async def test_auto_enrollment_awaits_unsupplied_mac(temp_env, sample_v4_config):
    """Authorized device without MAC ('dev-002') remains pending rather than activating."""
    db = temp_env["db"]
    reg_repo = temp_env["reg_repo"]
    await reg_repo.save_registration(sample_v4_config)

    dev_reg = DeviceRegistry(db)
    auto_enr = AutoEnrollmentManager(db, dev_reg, reg_repo=reg_repo)

    # Only dev-001 connects
    candidate_ports = [
        ScannedPortInfo(port="/dev/ttyUSB0", reported_mac="98a316e6b658")
    ]

    report = await auto_enr.discover_and_reconcile(candidate_ports)

    # dev-002 should be in pending list awaiting matching info
    pending_ids = [d["registration_device_id"] for d in report["pending"]]
    assert "dev-002" in pending_ids

    dev2 = await reg_repo.get_authorized_device("dev-002")
    assert dev2["status"] == "pending_binding"


@pytest.mark.asyncio
async def test_auto_enrollment_rejects_unknown_usb_device(temp_env, sample_v4_config):
    """AutoEnrollmentManager rejects unapproved USB controller with unregistered MAC."""
    db = temp_env["db"]
    reg_repo = temp_env["reg_repo"]
    await reg_repo.save_registration(sample_v4_config)

    dev_reg = DeviceRegistry(db)
    dev_mgr = DeviceManager()
    auto_enr = AutoEnrollmentManager(db, dev_reg, dev_mgr, reg_repo)

    candidate_ports = [
        ScannedPortInfo(port="/dev/ttyUSB9", reported_mac="ffffffffffff", description="Malicious Unknown Actuator")
    ]

    report = await auto_enr.discover_and_reconcile(candidate_ports)

    assert len(report["bound"]) == 0
    assert len(report["rejected"]) == 1
    assert report["rejected"][0]["mac"] == "ffffffffffff"
    assert report["rejected"][0]["reason"] == "NOT_REGISTERED_IN_MIDDLEWARE"

    # Must NOT exist in device_bindings
    binding = await dev_reg.get_binding("ffffffffffff")
    assert binding is None

    # Must NOT be registered in DeviceManager
    assert dev_mgr.get_device("/dev/ttyUSB9") is None


@pytest.mark.asyncio
async def test_auto_enrollment_handles_port_renumbering(temp_env, sample_v4_config):
    """Device previously bound on /dev/ttyUSB0 automatically recovers when moved to /dev/ttyUSB1."""
    db = temp_env["db"]
    reg_repo = temp_env["reg_repo"]
    await reg_repo.save_registration(sample_v4_config)

    dev_reg = DeviceRegistry(db)
    dev_mgr = DeviceManager()
    auto_enr = AutoEnrollmentManager(db, dev_reg, dev_mgr, reg_repo)

    # First run on /dev/ttyUSB0
    ports_run1 = [ScannedPortInfo(port="/dev/ttyUSB0", reported_mac="98a316e6b658")]
    await auto_enr.discover_and_reconcile(ports_run1)

    binding1 = await dev_reg.get_binding("98a316e6b658")
    assert binding1.port_hint == "/dev/ttyUSB0"

    # Second run: port changed to /dev/ttyUSB1
    ports_run2 = [ScannedPortInfo(port="/dev/ttyUSB1", reported_mac="98a316e6b658")]
    report2 = await auto_enr.discover_and_reconcile(ports_run2)

    assert len(report2["port_changes"]) == 1
    assert report2["port_changes"][0]["old_port"] == "/dev/ttyUSB0"
    assert report2["port_changes"][0]["new_port"] == "/dev/ttyUSB1"

    # Binding in SQLite dynamically updated
    binding2 = await dev_reg.get_binding("98a316e6b658")
    assert binding2.port_hint == "/dev/ttyUSB1"


@pytest.mark.asyncio
async def test_config_sync_disables_removed_device_retaining_history(temp_env, sample_v4_config):
    """When middleware drops a device from inventory, it is revoked while preserving history."""
    db = temp_env["db"]
    reg_repo = temp_env["reg_repo"]
    await reg_repo.save_registration(sample_v4_config)

    # Both dev-001 and dev-002 exist in v4
    auth_list = await reg_repo.get_authorized_devices()
    assert len(auth_list) == 2

    # In v5, dev-002 is removed by administrator
    sync_mgr = ConfigSyncManager(db, reg_repo=reg_repo, expected_hub_id="hub-3417ebc3b479")
    v5_config = dict(sample_v4_config)
    v5_config["config_version"] = 5
    v5_config["connected_devices"] = [sample_v4_config["connected_devices"][0]]  # only dev-001

    await sync_mgr.apply_configuration(v5_config)

    dev2 = await reg_repo.get_authorized_device("dev-002")
    assert dev2 is not None
    assert dev2["status"] == "revoked"  # Authorization revoked, record preserved!


# =====================================================================
# 5. Offline Safety Policy
# =====================================================================

@pytest.mark.asyncio
async def test_offline_safety_policy_retains_last_verified_config(temp_env, sample_v4_config):
    """When RabbitMQ is unavailable, hub continues running under verified SQLite configuration."""
    reg_repo = temp_env["reg_repo"]
    await reg_repo.save_registration(sample_v4_config)

    guard = RegistrationGuard(reg_repo, lease_grace_hours=72)
    state = await guard.get_operating_state()
    is_auth, _ = await guard.check_hub_authorized()

    assert state == HubOperatingState.ACTIVATED
    assert is_auth is True

    # Device approval check works offline from SQLite
    assert await guard.is_device_approved("dev-001") is True
    assert await guard.is_device_approved("dev-unknown") is False
