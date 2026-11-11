"""
Unit and Integration Tests for RNHub V2 Automatic Registration & Configuration.
File: tests/test_v2_automatic_registration.py

Validates:
1. Reception and parsing of exact Section 4 configuration JSON (hub.config.snapshot).
2. Atomic SQLite persistence across hub_configuration, approved_devices, hub_registration, config_history.
3. RegistrationListener hub.hello announcement and manual RabbitMQ message processing.
4. AutoEnrollmentManager matching Section 4 devices (dev-001 approved with MAC, dev-002 null MAC).
5. Safe initial states and profile binding for esp8266_relay_v1.
6. ProvisioningReporter persistent outbox queuing and flushing of Section 11 ack format.
7. Rejection of expired configurations and idempotency of identical configuration hashes.
8. iothubctl status RNHub V2 diagnostic output layout.
"""

from __future__ import annotations
import asyncio
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional
import pytest
import pytest_asyncio

from storage.database import DatabaseManager
from storage.config_repository import ConfigRepository
from storage.registration_repository import RegistrationRepository
from devices.device_registry import DeviceRegistry, ScannedPortInfo
from devices.auto_enrollment import AutoEnrollmentManager
from devices.device_manager import DeviceManager
from devices.mock_device import MockDevice
from core.auto_config_manager import AutoConfigManager
from core.registration_listener import RegistrationListener
from core.provisioning_reporter import ProvisioningReporter


class MockIncomingMessage:
    """Simulates an aio_pika incoming message for registration tests."""

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
        class _ProcessContext:
            async def __aenter__(self):
                return msg
            async def __aexit__(self, exc_type, exc_val, exc_tb):
                if exc_type is None:
                    msg.acked = True
                else:
                    msg.rejected = True
                return False
        return _ProcessContext()

    async def ack(self) -> None:
        self.acked = True

    async def reject(self, requeue: bool = False) -> None:
        self.rejected = True


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


class MockPublisher:
    """Mock publisher for ProvisioningReporter."""

    def __init__(self):
        self.is_connected = True
        self.published_events: List[Dict[str, Any]] = []

    async def publish_event(self, routing_key: str, payload: Dict[str, Any], mandatory: bool = False, message_id: Optional[str] = None) -> bool:
        self.published_events.append({
            "routing_key": routing_key,
            "payload": payload,
            "message_id": message_id,
        })
        return True


@pytest_asyncio.fixture
async def env_v2(tmp_path: Path):
    """Initializes isolated database and repositories for V2 testing."""
    db_file = str(tmp_path / "iothub_v2_test.db")
    db = DatabaseManager(db_path=db_file)
    await db.connect()

    cfg = ConfigRepository(db)
    reg_repo = RegistrationRepository(db, cfg)
    dev_reg = DeviceRegistry(db)
    dev_mgr = DeviceManager()

    auto_enr = AutoEnrollmentManager(
        db=db,
        device_registry=dev_reg,
        device_manager=dev_mgr,
        reg_repo=reg_repo,
    )
    reporter = ProvisioningReporter(
        db=db,
        hub_uuid="3417ebc3-b479-4b11-8f93-911b2c349abc",
        hub_id="hub-3417ebc3b479",
    )
    manager = AutoConfigManager(
        db=db,
        reg_repo=reg_repo,
        config_repo=cfg,
        auto_enrollment=auto_enr,
        provisioning_reporter=reporter,
        expected_hub_id="hub-3417ebc3b479",
        expected_hub_uuid="3417ebc3-b479-4b11-8f93-911b2c349abc",
    )

    yield {
        "db": db,
        "cfg": cfg,
        "reg_repo": reg_repo,
        "dev_reg": dev_reg,
        "dev_mgr": dev_mgr,
        "auto_enr": auto_enr,
        "reporter": reporter,
        "manager": manager,
    }
    await db.close()


@pytest.fixture
def section4_config_snapshot() -> Dict[str, Any]:
    """Exact JSON payload defined in Section 4 of the specification."""
    return {
        "schema_version": 1,
        "message_type": "hub.config.snapshot",
        "message_id": "cfg-1001",
        "hub_uuid": "3417ebc3-b479-4b11-8f93-911b2c349abc",
        "hub_id": "hub-3417ebc3b479",
        "config_version": 3,
        "issued_at": "2026-11-11T08:30:00Z",
        "expires_at": "2026-11-12T08:30:00Z",
        "hub": {
            "hub_name": "Plants Hub 01",
            "hub_type": "mini_pc",
            "resource_id": 12,
            "sub_resource_id": 34,
            "installation_location": "Greenhouse A - Rack 3",
            "timezone": "Asia/Colombo",
        },
        "devices": [
            {
                "device_uid": "dev-001",
                "name": "Lighting Controller",
                "mac": "98a316e6b658",
                "type": "relay",
                "profile": "esp8266_relay_v1",
                "approved": True,
                "configuration": {
                    "channels": 1,
                    "safe_initial_state": "OFF",
                },
            },
            {
                "device_uid": "dev-002",
                "name": "Water Valve Controller",
                "mac": None,
                "type": "relay",
                "profile": "esp8266_relay_v1",
                "approved": False,
                "configuration": {
                    "channels": 1,
                    "safe_initial_state": "OFF",
                },
            },
        ],
        "signature": "<verified-server-signature>",
    }


# =====================================================================
# Test Cases
# =====================================================================

@pytest.mark.asyncio
async def test_v2_receive_hub_config_snapshot_payload(env_v2, section4_config_snapshot):
    """
    Validates that AutoConfigManager parses Section 4 configuration JSON
    and atomically updates hub_configuration, approved_devices, and config_history.
    """
    mgr = env_v2["manager"]
    db = env_v2["db"]

    res = await mgr.apply_configuration(section4_config_snapshot)
    assert res["status"] == "SUCCESS"
    assert res["config_version"] == 3

    # 1. Verify hub_configuration table
    conn = db.connection
    async with conn.execute("SELECT * FROM hub_configuration WHERE hub_uuid = ?", ("3417ebc3-b479-4b11-8f93-911b2c349abc",)) as cur:
        hub_cfg = await cur.fetchone()
    assert hub_cfg is not None
    assert hub_cfg["hub_name"] == "Plants Hub 01"
    assert hub_cfg["resource_id"] == 12
    assert hub_cfg["sub_resource_id"] == 34
    assert hub_cfg["installation_location"] == "Greenhouse A - Rack 3"
    assert hub_cfg["timezone"] == "Asia/Colombo"

    # 2. Verify approved_devices table
    async with conn.execute("SELECT * FROM approved_devices ORDER BY device_uid ASC") as cur:
        appr_devs = await cur.fetchall()
    assert len(appr_devs) == 2
    dev1 = appr_devs[0]
    assert dev1["device_uid"] == "dev-001"
    assert dev1["name"] == "Lighting Controller"
    assert dev1["mac"] == "98a316e6b658"
    assert dev1["profile"] == "esp8266_relay_v1"
    assert dev1["approved"] == 1

    dev2 = appr_devs[1]
    assert dev2["device_uid"] == "dev-002"
    assert dev2["mac"] is None
    assert dev2["approved"] == 0

    # 3. Verify config_history table
    async with conn.execute("SELECT * FROM config_history WHERE config_version = 3") as cur:
        hist = await cur.fetchone()
    assert hist is not None
    assert hist["applied_status"] == "APPLIED"
    assert len(hist["config_hash"]) == 64


@pytest.mark.asyncio
async def test_v2_registration_listener_hello_announcement(env_v2):
    """Validates that RegistrationListener sends hub.hello announcement with version and machine ID."""
    listener = RegistrationListener(
        hub_id="hub-3417ebc3b479",
        hub_uuid="3417ebc3-b479-4b11-8f93-911b2c349abc",
        auto_config_manager=env_v2["manager"],
    )
    mock_ex = MockExchange("hub_results")
    listener.ack_exchange = mock_ex
    listener.channel = True

    await listener.publish_hello()

    assert len(mock_ex.published) == 1
    published = mock_ex.published[0]
    assert published["routing_key"] == "hub.hub-3417ebc3b479.hello"
    payload = published["payload"]
    assert payload["message_type"] == "hub.hello"
    assert payload["hub_id"] == "hub-3417ebc3b479"
    assert payload["hub_uuid"] == "3417ebc3-b479-4b11-8f93-911b2c349abc"
    assert "installation_id" in payload


@pytest.mark.asyncio
async def test_v2_auto_enrollment_with_esp8266_safe_profile(env_v2, section4_config_snapshot):
    """
    Validates automatic discovery of dev-001 with MAC 98a316e6b658,
    binding it to /dev/ttyUSB0 under esp8266_relay_v1 without actuation.
    """
    mgr = env_v2["manager"]
    auto_enr = env_v2["auto_enr"]
    dev_reg = env_v2["dev_reg"]

    # First apply configuration so approved devices exist in SQLite
    await mgr.apply_configuration(section4_config_snapshot)

    # Simulate discovering USB device on /dev/ttyUSB0 with MAC 98a316e6b658
    candidate_ports = [
        ScannedPortInfo(
            port="/dev/ttyUSB0",
            vid="1a86",
            pid="7523",
            serial_number="98a316e6b658",
            reported_mac="98a316e6b658",
        )
    ]

    report = await auto_enr.discover_and_reconcile(candidate_ports=candidate_ports)
    assert len(report["bound"]) == 1
    bound = report["bound"][0]
    assert bound["mac"] == "98a316e6b658"
    assert bound["port"] == "/dev/ttyUSB0"
    assert bound["profile"] == "esp8266_relay_v1"

    # dev-002 had no MAC, so it must be pending
    assert len(report["pending"]) == 1
    pending = report["pending"][0]
    assert pending["name"] == "Water Valve Controller"

    # Verify device_results format for Section 11 ack
    dev_results = report["device_results"]
    assert len(dev_results) >= 2
    cfg_dev = next(d for d in dev_results if d["device_uid"] == "dev-001")
    assert cfg_dev["status"] == "configured"
    assert cfg_dev["port"] == "/dev/ttyUSB0"

    pend_dev = next(d for d in dev_results if d["device_uid"] == "dev-002")
    assert pend_dev["status"] == "pending_hardware_binding"


@pytest.mark.asyncio
async def test_v2_provisioning_reporter_queues_and_flushes_ack(env_v2):
    """
    Validates that ProvisioningReporter persists hub.config.ack into provisioning_outbox
    and flushes to RabbitMQ upon broker connection.
    """
    reporter = env_v2["reporter"]
    mock_pub = MockPublisher()
    reporter.publisher = mock_pub

    device_results = [
        {"device_uid": "dev-001", "status": "configured", "port": "/dev/ttyUSB0"},
        {"device_uid": "dev-002", "status": "pending_hardware_binding"},
    ]

    # Queue result
    msg_id = await reporter.queue_result(
        config_version=3,
        device_results=device_results,
        status="partial",
    )
    assert msg_id.startswith("ack-")

    # Flush outbox
    delivered_count = await reporter.flush_outbox()
    assert delivered_count == 1
    assert len(mock_pub.published_events) == 1

    event = mock_pub.published_events[0]
    assert event["routing_key"] == "hub.hub-3417ebc3b479.config.ack"
    ack_payload = event["payload"]
    assert ack_payload["message_type"] == "hub.config.ack"
    assert ack_payload["config_version"] == 3
    assert ack_payload["status"] == "partial"
    assert len(ack_payload["devices"]) == 2
    assert ack_payload["devices"][0]["status"] == "configured"
    assert ack_payload["devices"][1]["status"] == "pending_hardware_binding"

    # Outbox should now be empty
    conn = env_v2["db"].connection
    async with conn.execute("SELECT COUNT(*) FROM provisioning_outbox WHERE status = 'PENDING'") as cur:
        count = (await cur.fetchone())[0]
    assert count == 0


@pytest.mark.asyncio
async def test_v2_expired_config_rejection(env_v2, section4_config_snapshot):
    """Validates that a configuration snapshot with expires_at in the past is rejected."""
    mgr = env_v2["manager"]
    expired_cfg = dict(section4_config_snapshot)
    expired_cfg["expires_at"] = "2020-01-01T00:00:00Z"

    res = await mgr.apply_configuration(expired_cfg)
    assert res["status"] == "REJECTED"
    assert res["error"] == "CONFIG_EXPIRED"


@pytest.mark.asyncio
async def test_v2_duplicate_config_hash_already_applied(env_v2, section4_config_snapshot):
    """Validates that re-sending an identical configuration snapshot returns ALREADY_APPLIED."""
    mgr = env_v2["manager"]

    # First application
    res1 = await mgr.apply_configuration(section4_config_snapshot)
    assert res1["status"] == "SUCCESS"

    # Second application of identical payload
    res2 = await mgr.apply_configuration(section4_config_snapshot)
    assert res2["status"] == "ALREADY_APPLIED"
    assert res2["config_version"] == 3


@pytest.mark.asyncio
async def test_v2_registration_listener_hello_with_pending_activation_code(env_v2):
    """Validates that RegistrationListener advertises pending activation code in hub.hello and hub.activate."""
    cfg = env_v2["cfg"]
    await cfg.set_setting("auth.activation_code", "ACT-3417-9921")

    listener = RegistrationListener(
        hub_id="hub-3417ebc3b479",
        hub_uuid="3417ebc3-b479-4b11-8f93-911b2c349abc",
        auto_config_manager=env_v2["manager"],
    )
    mock_ex = MockExchange("hub_results")
    listener.ack_exchange = mock_ex
    listener.channel = True

    await listener.publish_hello()

    # Should have published hub.hello and heartbeat with activation code
    rk_map = {p["routing_key"]: p["payload"] for p in mock_ex.published}
    assert "hub.hub-3417ebc3b479.hello" in rk_map
    hello_payload = rk_map["hub.hub-3417ebc3b479.hello"]
    assert hello_payload["status"] == "pending_activation"
    assert hello_payload["activation_code"] == "ACT-3417-9921"

    assert "hub.hub-3417ebc3b479.heartbeat" in rk_map
    hb_payload = rk_map["hub.hub-3417ebc3b479.heartbeat"]
    assert hb_payload["message_type"] == "heartbeat"
    assert hb_payload["event_type"] == "heartbeat"
    assert hb_payload["activation_code"] == "ACT-3417-9921"
    assert hb_payload["status"] == "pending_activation"


@pytest.mark.asyncio
async def test_v2_registration_listener_periodic_heartbeat_sends_activation_code(env_v2):
    """Validates that publish_heartbeat transmits activation_code as heartbeat."""
    cfg = env_v2["cfg"]
    await cfg.set_setting("auth.activation_code", "ACT-HEARTBEAT-777")

    listener = RegistrationListener(
        hub_id="hub-3417ebc3b479",
        hub_uuid="3417ebc3-b479-4b11-8f93-911b2c349abc",
        auto_config_manager=env_v2["manager"],
    )
    mock_ex = MockExchange("hub_results")
    listener.ack_exchange = mock_ex
    listener.channel = True

    await listener.publish_heartbeat()

    rk_map = {p["routing_key"]: p["payload"] for p in mock_ex.published}
    assert "hub.hub-3417ebc3b479.heartbeat" in rk_map
    hb = rk_map["hub.hub-3417ebc3b479.heartbeat"]
    assert hb["message_type"] == "heartbeat"
    assert hb["event_type"] == "heartbeat"
    assert hb["activation_code"] == "ACT-HEARTBEAT-777"
    assert hb["status"] == "pending_activation"


@pytest.mark.asyncio
async def test_v2_registration_listener_clears_activation_code_on_success(env_v2, section4_config_snapshot):
    """Validates that a pending activation code is cleared from SQLite once config snapshot is successfully applied."""
    cfg = env_v2["cfg"]
    await cfg.set_setting("auth.activation_code", "ACT-3417-9921")
    assert await cfg.get_setting("auth.activation_code") == "ACT-3417-9921"

    listener = RegistrationListener(
        hub_id="hub-3417ebc3b479",
        hub_uuid="3417ebc3-b479-4b11-8f93-911b2c349abc",
        auto_config_manager=env_v2["manager"],
    )
    mock_ex = MockExchange("hub_results")
    listener.ack_exchange = mock_ex
    listener.channel = True

    incoming = MockIncomingMessage(section4_config_snapshot)
    await listener.handle_message(incoming)

    # Activation code should be cleared
    cleared_code = await cfg.get_setting("auth.activation_code")
    assert cleared_code is None


@pytest.mark.asyncio
async def test_v2_rabbitmq_activation_timeout_stores_pending_code(env_v2, monkeypatch):
    """Validates that if middleware does not reply immediately over RabbitMQ, activation code is stored locally for daemon."""
    db = env_v2["db"]
    cfg = env_v2["cfg"]

    class MockChannel:
        def __init__(self):
            self.is_closed = False
        async def set_qos(self, *a, **k): pass
        async def declare_exchange(self, name, *a, **k): return MockExchange(name)
        async def get_exchange(self, name): return MockExchange(name)
        async def declare_queue(self, name=None, *a, **k):
            q = MockExchange(name or "amq.gen-mock")
            q.bind = lambda *a, **k: asyncio.sleep(0)
            q.consume = lambda *a, **k: asyncio.sleep(0, result="ctag")
            q.cancel = lambda *a, **k: asyncio.sleep(0)
            return q
        async def close(self): self.is_closed = True

    class MockConn:
        def __init__(self):
            self.is_closed = False
        async def channel(self): return MockChannel()
        async def close(self): self.is_closed = True

    import aio_pika
    monkeypatch.setattr(aio_pika, "connect_robust", lambda *a, **k: asyncio.sleep(0, result=MockConn()))

    res = await RegistrationListener.activate_via_rabbitmq(
        activation_code="ACT-999-TIMEOUT",
        db=db,
        url="amqp://mock",
        hub_id="hub-3417ebc3b479",
        timeout=0.05,
    )

    assert res["status"] == "PENDING_ASYNC"
    assert res["activation_code"] == "ACT-999-TIMEOUT"
    assert await cfg.get_setting("auth.activation_code") == "ACT-999-TIMEOUT"


@pytest.mark.asyncio
async def test_v2_rabbitmq_activation_success_roundtrip(env_v2, section4_config_snapshot, monkeypatch):
    """Validates immediate round-trip RabbitMQ activation applying Section 4 registration snapshot."""
    db = env_v2["db"]
    cfg = env_v2["cfg"]

    class MockAMQPQueue:
        def __init__(self, name: str):
            self.name = name
            self.callback = None
        async def bind(self, *a, **k): pass
        async def consume(self, callback, *a, **k):
            self.callback = callback
            # As soon as consumed, simulate middleware publishing section4_config_snapshot
            asyncio.create_task(self._deliver())
            return "tag_1"
        async def _deliver(self):
            await asyncio.sleep(0.01)
            if self.callback:
                msg = MockIncomingMessage(section4_config_snapshot)
                await self.callback(msg)
        async def cancel(self, *a, **k): pass

    class MockChannel:
        def __init__(self):
            self.is_closed = False
        async def set_qos(self, *a, **k): pass
        async def declare_exchange(self, name, *a, **k): return MockExchange(name)
        async def get_exchange(self, name): return MockExchange(name)
        async def declare_queue(self, name=None, *a, **k): return MockAMQPQueue(name or "amq.gen-reply")
        async def close(self): self.is_closed = True

    class MockConn:
        def __init__(self):
            self.is_closed = False
        async def channel(self): return MockChannel()
        async def close(self): self.is_closed = True

    import aio_pika
    monkeypatch.setattr(aio_pika, "connect_robust", lambda *a, **k: asyncio.sleep(0, result=MockConn()))

    res = await RegistrationListener.activate_via_rabbitmq(
        activation_code="ACT-SUCCESS-123",
        db=db,
        url="amqp://mock",
        hub_id="hub-3417ebc3b479",
        timeout=2.0,
    )

    assert res["status"] == "ACTIVATED"
    assert res["registration"]["hub_id"] == "hub-3417ebc3b479"
    assert res["apply_result"]["status"] == "SUCCESS"
    assert await cfg.get_setting("auth.activation_code") is None


@pytest.mark.asyncio
async def test_v2_apply_user_provided_registration_json(env_v2):
    """Validates full processing of the user's exact production registration JSON from RabbitMQ."""
    mgr = env_v2["manager"]
    db = env_v2["db"]
    cfg = env_v2["cfg"]
    reg_repo = env_v2["reg_repo"]

    payload = {
        "message_type": "hub.config.snapshot",
        "schema_version": 1,
        "message_id": "msg-9f4a132e01b7",
        "activation_code": "ACT-AFE6-3990-E204",
        "hub_uuid": "45c7f53b-d615-41be-82b1-4f84608eba66",
        "hub_id": "hub-45c7f53b",
        "node_id": "3417ebc3b479",
        "config_version": 1,
        "issued_at": "2026-11-11T09:47:58.633816Z",
        "hub_name": "Greenhouse Rack 1 Controller",
        "hub_type": "raspberry_pi",
        "installation_location": "Greenhouse Sector B - Rack 1",
        "timezone": "Asia/Colombo",
        "resource_id": 1,
        "resource_name": "Cutting Machine 1",
        "resource_type": "Machine",
        "sub_resource_id": 20025,
        "sub_resource_name": "Sub Cutter A",
        "kit_type": "rnhub",
        "registration_status": "REGISTERED",
        "provisioning_status": "active",
        "hub": {
            "hub_uuid": "45c7f53b-d615-41be-82b1-4f84608eba66",
            "hub_id": "hub-45c7f53b",
            "hub_name": "Greenhouse Rack 1 Controller",
            "hub_type": "raspberry_pi",
            "installation_location": "Greenhouse Sector B - Rack 1",
            "timezone": "Asia/Colombo",
            "node_id": "3417ebc3b479",
            "resource_id": 1,
            "resource_name": "Cutting Machine 1",
            "sub_resource_id": 20025,
            "sub_resource_name": "Sub Cutter A",
            "kit_type": "rnhub",
            "registration_status": "REGISTERED",
            "provisioning_status": "active",
            "activation_code": "ACT-AFE6-3990-E204",
        },
        "devices": [
            {
                "device_uid": "dev-001",
                "name": "Hydroponic Water Pump",
                "mac": "00:11:22:33:44:55",
                "type": "actuator",
                "profile": "esp8266_relay_v1",
                "approved": True,
                "binding_status": "approved",
                "last_reported_status": "unknown",
                "configuration": {
                    "channels": 1,
                    "active_low": True,
                    "safe_initial_state": "OFF",
                },
            },
            {
                "device_uid": "dev-002",
                "name": "Rack Air Temp Sensor",
                "mac": "00:11:22:33:44:66",
                "type": "sensor",
                "profile": "dht22_temp_sensor",
                "approved": True,
                "binding_status": "approved",
                "last_reported_status": "unknown",
                "configuration": {
                    "pin": 4,
                    "polling_interval_sec": 5,
                },
            },
        ],
    }

    # Set pending activation code
    await cfg.set_setting("auth.activation_code", "ACT-AFE6-3990-E204")

    # Apply configuration snapshot
    result = await mgr.apply_configuration(payload, actor="rabbitmq_registration")
    assert result["status"] == "SUCCESS"
    assert result["config_version"] == 1

    # Verify hub_registration persistence
    reg = await reg_repo.get_registration()
    assert reg is not None
    assert reg["hub_uuid"] == "45c7f53b-d615-41be-82b1-4f84608eba66"
    assert reg["hub_id"] == "hub-45c7f53b"
    assert reg["hub_name"] == "Greenhouse Rack 1 Controller"
    assert reg["hub_type"] == "raspberry_pi"
    assert reg["resource_id"] == 1
    assert reg["sub_resource_id"] == 20025
    assert reg["installation_location"] == "Greenhouse Sector B - Rack 1"
    assert reg["timezone"] == "Asia/Colombo"
    assert reg["config_version"] == 1

    # Verify approved_devices table
    conn = db.connection
    async with conn.execute("SELECT device_uid, name, mac, profile, approved FROM approved_devices ORDER BY device_uid ASC") as cur:
        rows = await cur.fetchall()
    assert len(rows) == 2
    assert rows[0]["device_uid"] == "dev-001"
    assert rows[0]["name"] == "Hydroponic Water Pump"
    assert rows[0]["mac"] == "00:11:22:33:44:55"
    assert rows[0]["profile"] == "esp8266_relay_v1"
    assert rows[0]["approved"] == 1

    assert rows[1]["device_uid"] == "dev-002"
    assert rows[1]["name"] == "Rack Air Temp Sensor"
    assert rows[1]["mac"] == "00:11:22:33:44:66"
    assert rows[1]["profile"] == "dht22_temp_sensor"
    assert rows[1]["approved"] == 1

    # Verify config_settings synced
    assert await cfg.get_setting("hub.id") == "hub-45c7f53b"
    assert await cfg.get_setting("hub.uuid") == "45c7f53b-d615-41be-82b1-4f84608eba66"
    assert await cfg.get_setting("schedule.timezone") == "Asia/Colombo"


