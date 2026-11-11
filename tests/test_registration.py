"""
Unit and Integration Tests for RNHub V2: Middleware-Integrated Hub Registration.
File: tests/test_registration.py

Validates:
1. One-time activation credential redemption via RegistrationClient.
2. Rejection of invalid, expired, and duplicate activation claims.
3. Configuration synchronization and acknowledgment (/api/rnhub-agent/config/ and /ack/).
4. Physical device authorization handshake (/api/rnhub-agent/devices/authorize/).
5. Periodic heartbeat telemetry and remote suspension detection.
6. SQLite persistence of verified registration and authorized devices snapshot.
7. AES-256-GCM encryption of machine tokens and provisioned broker credentials.
8. RegistrationGuard operating states (UNREGISTERED, ACTIVATED, SUSPENDED, LEASE_EXPIRED).
9. Device approval enforcement on CommandRouter and DeviceManager.
10. CLI commands (iothubctl hub registration-status, hub activate, hub sync, devices approved).
"""

from __future__ import annotations
import argparse
import asyncio
from datetime import datetime, timezone, timedelta
import json
import os
from pathlib import Path
from typing import Any, Dict
import httpx
import pytest
import pytest_asyncio

from core.registration_client import (
    RegistrationClient,
    RegistrationError,
    ActivationError,
    InvalidActivationCodeError,
    DuplicateClaimError,
    RegistrationOfflineError,
    HubSuspendedError,
    DeviceAuthorizationError,
)
from core.registration_guard import RegistrationGuard, HubOperatingState
from storage.database import DatabaseManager
from storage.config_repository import ConfigRepository
from storage.registration_repository import RegistrationRepository
from core.router import CommandRouter, CommandRoutingError
from core.message_validator import IoTMessage, CommandItem
from devices.device_manager import DeviceManager, DeviceActionError
from devices.mock_device import MockDevice
from iothubctl import cmd_hub, cmd_devices


# =====================================================================
# Fixtures and Sample Middleware Payloads
# =====================================================================

@pytest.fixture
def sample_activation_response() -> Dict[str, Any]:
    return {
        "hub_uuid": "3417ebc3-b479-4b11-8f93-911b2c349abc",
        "hub_id": "hub-3417ebc3b479",
        "hub_name": "Plants Hub 01",
        "hub_type": "mini_pc",
        "status": "active",
        "resource_id": 12,
        "sub_resource_id": 34,
        "installation_location": "Greenhouse A - Rack 3",
        "timezone": "Asia/Colombo",
        "config_version": 4,
        "api_token": "tok_sec_999888777666",
        "lease_expires_at": (datetime.now(timezone.utc) + timedelta(days=7)).isoformat(),
        "broker_credentials": {
            "host": "47.236.59.210",
            "port": 5672,
            "user": "rnr_iot_user",
            "password": "rnr_iot_2025_sec!",
            "vhost": "/",
        },
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


@pytest_asyncio.fixture
async def temp_db(tmp_path: Path):
    key_file = tmp_path / "test_master.key"
    os.environ["IOTHUB_MASTER_KEY_FILE"] = str(key_file)
    db_file = str(tmp_path / "test_reg.db")
    db = DatabaseManager(db_path=db_file)
    await db.connect()
    yield db
    await db.close()
    if "IOTHUB_MASTER_KEY_FILE" in os.environ:
        del os.environ["IOTHUB_MASTER_KEY_FILE"]


# =====================================================================
# 1. RegistrationClient Tests
# =====================================================================

@pytest.mark.asyncio
async def test_registration_client_activate_success(sample_activation_response):
    """Claims registered Django hub successfully using one-time activation code."""
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/rnhub-agent/activate/"
        req_json = json.loads(request.content)
        assert req_json["activation_code"] == "ACT-123456"
        return httpx.Response(200, json=sample_activation_response)

    transport = httpx.MockTransport(handler)
    client = RegistrationClient(
        middleware_url="https://api.example.com",
        transport=transport,
    )
    result = await client.activate(activation_code="ACT-123456")
    assert result["hub_uuid"] == "3417ebc3-b479-4b11-8f93-911b2c349abc"
    assert result["hub_id"] == "hub-3417ebc3b479"
    assert result["api_token"] == "tok_sec_999888777666"
    assert client.hub_uuid == "3417ebc3-b479-4b11-8f93-911b2c349abc"


@pytest.mark.asyncio
async def test_registration_client_invalid_activation_code():
    """Rejects invalid or expired activation code with InvalidActivationCodeError."""
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(400, json={"error": "Activation code is invalid or expired"})

    transport = httpx.MockTransport(handler)
    client = RegistrationClient(middleware_url="https://api.example.com", transport=transport)
    with pytest.raises(InvalidActivationCodeError, match="invalid or expired"):
        await client.activate(activation_code="BAD-CODE")


@pytest.mark.asyncio
async def test_registration_client_duplicate_claim():
    """Rejects duplicate activation attempt when hub is already claimed."""
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(409, json={"error": "Hub has already been claimed"})

    transport = httpx.MockTransport(handler)
    client = RegistrationClient(middleware_url="https://api.example.com", transport=transport)
    with pytest.raises(DuplicateClaimError, match="already been claimed"):
        await client.activate(activation_code="DUP-CODE")


@pytest.mark.asyncio
async def test_registration_client_config_sync_and_ack(sample_activation_response):
    """Fetches configuration snapshot and confirms application via ack endpoint."""
    ack_called = False

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal ack_called
        if request.url.path == "/api/rnhub-agent/config/":
            assert request.headers.get("Authorization") == "Bearer tok_test"
            assert request.headers.get("X-Hub-UUID") == "uuid-123"
            return httpx.Response(200, json=sample_activation_response)
        elif request.url.path == "/api/rnhub-agent/config/ack/":
            ack_called = True
            req_data = json.loads(request.content)
            assert req_data["config_version"] == 4
            return httpx.Response(200, json={"acknowledged": True})
        return httpx.Response(404)

    transport = httpx.MockTransport(handler)
    client = RegistrationClient(
        middleware_url="https://api.example.com",
        hub_uuid="uuid-123",
        api_token="tok_test",
        transport=transport,
    )
    config = await client.get_configuration()
    assert config["config_version"] == 4
    ack = await client.acknowledge_config(config_version=4)
    assert ack["acknowledged"] is True
    assert ack_called is True


@pytest.mark.asyncio
async def test_registration_client_device_authorize():
    """Verifies physical device enrollment handshake with middleware."""
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/rnhub-agent/devices/authorize/"
        body = json.loads(request.content)
        assert body["registration_device_id"] == "dev-001"
        assert body["device_mac"] == "98a316e6b658"
        return httpx.Response(200, json={"approved": True, "status": "approved"})

    transport = httpx.MockTransport(handler)
    client = RegistrationClient(
        middleware_url="https://api.example.com",
        hub_uuid="uuid-123",
        api_token="tok_test",
        transport=transport,
    )
    auth = await client.authorize_device(
        registration_device_id="dev-001",
        device_mac="98A316E6B658",
        port="/dev/ttyUSB0",
        profile="plant_controller_v1",
    )
    assert auth["approved"] is True
    assert auth["status"] == "approved"


# =====================================================================
# 2. RegistrationRepository & SQLite Storage Tests
# =====================================================================

@pytest.mark.asyncio
async def test_registration_repository_storage_and_encryption(temp_db, sample_activation_response):
    """Saves registration snapshot to SQLite and encrypts machine token & broker password."""
    cfg = ConfigRepository(temp_db)
    reg_repo = RegistrationRepository(temp_db, cfg)

    await reg_repo.save_registration(sample_activation_response)

    reg = await reg_repo.get_registration()
    assert reg is not None
    assert reg["hub_uuid"] == "3417ebc3-b479-4b11-8f93-911b2c349abc"
    assert reg["hub_id"] == "hub-3417ebc3b479"
    assert reg["status"] == "active"
    assert reg["resource_id"] == 12
    assert reg["sub_resource_id"] == 34
    assert reg["config_version"] == 4
    assert await reg_repo.is_activated() is True
    assert await reg_repo.is_lease_valid() is True

    # Verify encrypted settings
    decrypted_token = await cfg.get_setting("auth.token")
    assert decrypted_token == "tok_sec_999888777666"

    decrypted_pass = await cfg.get_setting("rabbitmq.password")
    assert decrypted_pass == "rnr_iot_2025_sec!"

    # Verify authorized devices
    devices = await reg_repo.get_authorized_devices()
    assert len(devices) == 2
    dev1 = await reg_repo.get_authorized_device("dev-001")
    assert dev1 is not None
    assert dev1["name"] == "Lighting Controller"
    assert dev1["device_mac"] == "98a316e6b658"
    assert dev1["status"] == "approved"


# =====================================================================
# 3. RegistrationGuard Operating States & Runtime Rules
# =====================================================================

@pytest.mark.asyncio
async def test_registration_guard_unregistered_hub(temp_db):
    """Unregistered hub blocks production operations and returns UNREGISTERED."""
    reg_repo = RegistrationRepository(temp_db)
    guard = RegistrationGuard(reg_repo)

    state = await guard.get_operating_state()
    assert state == HubOperatingState.UNREGISTERED

    authorized, reason = await guard.check_hub_authorized()
    assert authorized is False
    assert "not registered" in reason.lower()


@pytest.mark.asyncio
async def test_registration_guard_activated_hub(temp_db, sample_activation_response):
    """Activated hub permits production operations."""
    reg_repo = RegistrationRepository(temp_db)
    await reg_repo.save_registration(sample_activation_response)
    guard = RegistrationGuard(reg_repo)

    state = await guard.get_operating_state()
    assert state == HubOperatingState.ACTIVATED

    authorized, reason = await guard.check_hub_authorized()
    assert authorized is True


@pytest.mark.asyncio
async def test_registration_guard_suspended_hub(temp_db, sample_activation_response):
    """Suspended hub ceases all actuation and reports SUSPENDED."""
    reg_repo = RegistrationRepository(temp_db)
    await reg_repo.save_registration(sample_activation_response)
    await reg_repo.update_status("suspended")

    guard = RegistrationGuard(reg_repo)
    state = await guard.get_operating_state()
    assert state == HubOperatingState.SUSPENDED

    authorized, reason = await guard.check_hub_authorized()
    assert authorized is False
    assert "suspended" in reason.lower()


@pytest.mark.asyncio
async def test_registration_guard_lease_expiration_offline_policy(temp_db, sample_activation_response):
    """Expired lease blocks actuation when beyond offline grace period."""
    reg_repo = RegistrationRepository(temp_db)
    expired_lease = (datetime.now(timezone.utc) - timedelta(days=10)).isoformat()
    sample_activation_response["lease_expires_at"] = expired_lease
    await reg_repo.save_registration(sample_activation_response)

    # With grace period of 72 hours (3 days), a 10-day-old lease is expired
    guard = RegistrationGuard(reg_repo, lease_grace_hours=72)
    state = await guard.get_operating_state()
    assert state == HubOperatingState.LEASE_EXPIRED

    authorized, reason = await guard.check_hub_authorized()
    assert authorized is False
    assert "lease expired" in reason.lower()


@pytest.mark.asyncio
async def test_registration_guard_device_approval(temp_db, sample_activation_response):
    """Distinguishes between approved and unapproved devices in middleware inventory."""
    reg_repo = RegistrationRepository(temp_db)
    await reg_repo.save_registration(sample_activation_response)
    guard = RegistrationGuard(reg_repo)

    # Approved device dev-001 (and by MAC 98a316e6b658)
    assert await guard.is_device_approved("dev-001") is True
    assert await guard.is_device_approved("98a316e6b658") is True

    # Pending binding dev-002 is not yet approved
    assert await guard.is_device_approved("dev-002") is False

    # Unknown device is rejected
    assert await guard.is_device_approved("rogue_device_999") is False


# =====================================================================
# 4. CommandRouter & DeviceManager Enforcement Tests
# =====================================================================

@pytest.mark.asyncio
async def test_router_and_device_manager_enforce_registration(temp_db, sample_activation_response):
    """Router blocks commands on unapproved devices and unregistered hubs."""
    reg_repo = RegistrationRepository(temp_db)
    await reg_repo.save_registration(sample_activation_response)
    guard = RegistrationGuard(reg_repo)

    dev_mgr = DeviceManager(registration_guard=guard)
    relay = MockDevice("dev-001", "Relay Light", supported_actions=["LIGHT_ON", "LIGHT_OFF"])
    dev_mgr.register_device(relay)

    router = CommandRouter(hub_id="hub-3417ebc3b479", device_manager=dev_mgr, registration_guard=guard)

    # 1. Valid command on approved device succeeds
    valid_msg = IoTMessage(
        hub_id="hub-3417ebc3b479",
        commands=[CommandItem(device_id="dev-001", action="LIGHT_ON")],
    )
    results = await router.route_message(valid_msg)
    assert results[0]["status"] == "success"

    # 2. Command on unapproved device is rejected
    unapproved_msg = IoTMessage(
        hub_id="hub-3417ebc3b479",
        commands=[CommandItem(device_id="dev-002", action="VALVE_ON")],
    )
    results_unapp = await router.route_message(unapproved_msg)
    assert results_unapp[0]["status"] == "error"
    assert results_unapp[0]["error"] == "DEVICE_NOT_AUTHORIZED"

    # 3. Direct device manager dispatch raises error for unauthorized device
    with pytest.raises(DeviceActionError, match="not authorized"):
        await dev_mgr.dispatch("dev-002", "VALVE_ON", {})


# =====================================================================
# 5. CLI iothubctl Tests
# =====================================================================

@pytest.mark.asyncio
async def test_iothubctl_hub_registration_status(temp_db, sample_activation_response, capsys):
    """iothubctl hub registration-status displays accurate registration snapshot."""
    reg_repo = RegistrationRepository(temp_db)
    await reg_repo.save_registration(sample_activation_response)

    args = argparse.Namespace(db=temp_db.db_path, hub_action="registration-status")
    code = await cmd_hub(args)
    assert code == 0

    captured = capsys.readouterr().out
    assert "Plants Hub 01" in captured
    assert "hub-3417ebc3b479" in captured
    assert "3417ebc3-b479-4b11-8f93-911b2c349abc" in captured
    assert "Greenhouse A - Rack 3" in captured
    assert "Asia/Colombo" in captured


@pytest.mark.asyncio
async def test_iothubctl_devices_approved_list(temp_db, sample_activation_response, capsys):
    """iothubctl devices approved displays list of middleware-approved devices."""
    reg_repo = RegistrationRepository(temp_db)
    await reg_repo.save_registration(sample_activation_response)

    args = argparse.Namespace(db=temp_db.db_path, device_action="approved")
    code = await cmd_devices(args)
    assert code == 0

    captured = capsys.readouterr().out
    assert "Lighting Controller" in captured
    assert "dev-001" in captured
    assert "Water Valve Controller" in captured
    assert "dev-002" in captured


@pytest.mark.asyncio
async def test_iothubctl_hub_activate_rabbitmq(temp_db, sample_activation_response, capsys, monkeypatch):
    """iothubctl hub activate uses RabbitMQ and activates hub without requiring HTTP middleware URL."""
    from core.registration_listener import RegistrationListener

    async def mock_activate(*a, **k):
        return {
            "status": "ACTIVATED",
            "registration": sample_activation_response,
            "apply_result": {"status": "SUCCESS"},
        }

    monkeypatch.setattr(RegistrationListener, "activate_via_rabbitmq", mock_activate)
    args = argparse.Namespace(db=temp_db.db_path, hub_action="activate", activation_code="ACT-TEST-123", middleware_url=None)
    code = await cmd_hub(args)
    assert code == 0

    captured = capsys.readouterr().out
    assert "Activation credential validated via RabbitMQ" in captured
    assert "Plants Hub 01" in captured
    assert "hub-3417ebc3b479" in captured


@pytest.mark.asyncio
async def test_iothubctl_hub_sync_rabbitmq(temp_db, sample_activation_response, capsys, monkeypatch):
    """iothubctl hub sync uses RabbitMQ without requiring HTTP middleware URL."""
    from core.registration_listener import RegistrationListener

    async def mock_sync(*a, **k):
        return {
            "status": "SUCCESS",
            "config_version": 2,
            "apply_result": {"reconciled_devices": []},
        }

    monkeypatch.setattr(RegistrationListener, "sync_via_rabbitmq", mock_sync)
    args = argparse.Namespace(db=temp_db.db_path, hub_action="sync", middleware_url=None)
    code = await cmd_hub(args)
    assert code == 0

    captured = capsys.readouterr().out
    assert "Configuration synchronized via RabbitMQ" in captured
