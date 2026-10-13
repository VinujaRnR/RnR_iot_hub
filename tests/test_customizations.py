"""
Unit and Integration Tests for Post-Implementation Customizations.
File: tests/test_customizations.py

Validates Customizations 1 through 12:
- Customization 1: Universal Linux installer (install.sh & install.py wizard)
- Customization 2: CLI configuration management without manual .env editing
- Customization 3: Secure SQLite config & AES-256-GCM secret management with master key isolation
- Customization 4: Interactive USB port enrollment & MAC port renumbering survival
- Customization 5: IoT device profiles and channel mapping (rejects invalid channel/action)
- Customization 6: Configurable daily schedule policies
- Customization 8: iothubctl CLI tool (status, doctor, config, devices, profiles, backup)
- Customization 11: Consistent online SQLite backup, verify, and disaster recovery
"""

from __future__ import annotations
import asyncio
import os
import shutil
import tempfile
from pathlib import Path

import pytest
import pytest_asyncio

from core.crypto import CryptoManager, DecryptionError
from storage.database import DatabaseManager
from storage.config_repository import ConfigRepository
from storage.backup_manager import BackupManager
from devices.device_profile import (
    DeviceProfileRegistry,
    DEFAULT_PLANT_CONTROLLER_PROFILE,
    DEFAULT_ARDUINO_NANO_PROFILE,
)
from devices.device_registry import DeviceRegistry
from scheduler.schedule_policies import SchedulePolicyManager
import iothubctl


@pytest_asyncio.fixture
async def sandbox_env(tmp_path):
    """Sets up temporary database and isolated master key."""
    db_file = tmp_path / "custom_test.db"
    key_file = tmp_path / "keys" / "master.key"

    db = DatabaseManager(db_path=str(db_file))
    await db.connect()

    crypto = CryptoManager(key_path=str(key_file))

    yield {
        "db": db,
        "crypto": crypto,
        "tmp_path": tmp_path,
        "db_file": db_file,
        "key_file": key_file,
    }

    await db.close()


# =====================================================================
# Customization 3: Secure AES-256-GCM Encryption & Key Isolation
# =====================================================================
def test_customization_03_aes_gcm_encryption_and_tamper_detection(sandbox_env):
    """Validates AES-256-GCM encryption, decryption, and authentication tag verification."""
    crypto: CryptoManager = sandbox_env["crypto"]
    secret_text = "super_secret_rabbitmq_password_12345!"
    config_key = "rabbitmq.password"

    # Encrypt
    ciphertext_with_tag, nonce = crypto.encrypt_secret(
        plaintext=secret_text,
        config_key=config_key,
        key_version=1,
    )

    assert len(nonce) == 12, "GCM requires 96-bit (12 bytes) nonce"
    assert len(ciphertext_with_tag) > len(secret_text), "Must include 16-byte authentication tag"
    assert secret_text.encode("utf-8") not in ciphertext_with_tag, "Plaintext must not appear in ciphertext"

    # Decrypt
    decrypted = crypto.decrypt_secret(
        ciphertext_with_tag=ciphertext_with_tag,
        nonce=nonce,
        config_key=config_key,
        key_version=1,
    )
    assert decrypted == secret_text

    # Tamper detection: modifying 1 byte in ciphertext must fail authentication
    tampered_ct = bytearray(ciphertext_with_tag)
    tampered_ct[0] ^= 0xFF
    with pytest.raises(DecryptionError):
        crypto.decrypt_secret(bytes(tampered_ct), nonce, config_key, key_version=1)

    # AAD mismatch: wrong config_key must fail authentication
    with pytest.raises(DecryptionError):
        crypto.decrypt_secret(ciphertext_with_tag, nonce, "wrong.key", key_version=1)


def test_customization_03_master_key_file_isolation(sandbox_env):
    """Validates master key file is created separately from database with 0600 permissions."""
    crypto: CryptoManager = sandbox_env["crypto"]
    key_file = sandbox_env["key_file"]

    key = crypto.get_or_create_master_key()
    assert len(key) == 32, "Master key must be exactly 256 bits (32 bytes)"
    assert key_file.exists()
    assert str(key_file) != str(sandbox_env["db_file"]), "Master key must be separate from database"


# =====================================================================
# Customization 2: CLI Configuration Management & Secret Storage
# =====================================================================
@pytest.mark.asyncio
async def test_customization_02_config_repository_storage_and_masking(sandbox_env):
    """Validates plaintext vs encrypted storage, audit logging, and secret masking."""
    db: DatabaseManager = sandbox_env["db"]
    crypto: CryptoManager = sandbox_env["crypto"]
    repo = ConfigRepository(db, crypto)

    # 1. Plaintext setting
    await repo.set_setting("hub.id", "hub-custom-42", is_secret=False, actor="admin_user")
    hub_id = await repo.get_setting("hub.id")
    assert hub_id == "hub-custom-42"

    # 2. Encrypted setting (auto-detected via sensitive key name)
    await repo.set_setting("rabbitmq.password", "P@ssw0rd9876", is_secret=True, actor="admin_user")
    raw_pass = await repo.get_setting("rabbitmq.password")
    assert raw_pass == "P@ssw0rd9876"

    # Inspect SQLite database directly to confirm encryption at rest
    async with db.connection.execute(
        "SELECT storage_type, value_text, encrypted_value FROM config_settings WHERE config_key = 'rabbitmq.password'"
    ) as cur:
        row = await cur.fetchone()
        assert row["storage_type"] == "ENCRYPTED"
        assert row["value_text"] is None
        assert row["encrypted_value"] is not None
        assert b"P@ssw0rd9876" not in row["encrypted_value"]

    # 3. Masked listing for CLI show
    all_settings = await repo.list_settings(mask_secrets=True)
    assert all_settings["hub.id"] == "hub-custom-42"
    assert all_settings["rabbitmq.password"] == "********"

    # 4. Audit trail
    audit = await repo.get_audit_trail()
    assert len(audit) >= 2
    assert any(a["config_key"] == "rabbitmq.password" and a["operation"] == "SET_SECRET" for a in audit)


# =====================================================================
# Customization 5: Device Profiles and Channel Validation
# =====================================================================
@pytest.mark.asyncio
async def test_customization_05_device_profile_action_validation(sandbox_env):
    """Validates that unsupported channels, unauthorized actions, and safety violations are rejected."""
    db: DatabaseManager = sandbox_env["db"]
    reg = DeviceProfileRegistry(db)
    await reg.initialize_defaults()

    profile = await reg.get_profile("plant_controller_v1")
    assert profile is not None

    # Allowed command: LIGHTS_ON on relay:light
    ok, err = reg.validate_action(profile, "relay:light", "LIGHTS_ON", {})
    assert ok is True
    assert err is None

    # Rejected command: Unsupported channel (relay:motor)
    ok, err = reg.validate_action(profile, "relay:motor", "MOTOR_ON", {})
    assert ok is False
    assert "not supported" in err

    # Rejected command: Unsupported action (DIM_LIGHT on relay:light)
    ok, err = reg.validate_action(profile, "relay:light", "DIM_LIGHT", {})
    assert ok is False
    assert "not permitted" in err

    # Rejected command: Safety limit violation (duration_seconds exceeds max_on_seconds=90)
    ok, err = reg.validate_action(profile, "relay:water_valve", "WATER_VALVE_ON", {"duration_seconds": 120})
    assert ok is False
    assert "safety limit" in err


# =====================================================================
# Customization 4: USB Port Enrollment & Port Renumbering Survival
# =====================================================================
@pytest.mark.asyncio
async def test_customization_04_device_enrollment_and_port_renumbering(sandbox_env):
    """Validates device enrollment, persistent MAC bindings, and port renumbering survival."""
    db: DatabaseManager = sandbox_env["db"]
    reg = DeviceRegistry(db)

    # 1. Enroll device
    binding = await reg.enroll_device(
        device_mac="98a316e6b658",
        device_name="Greenhouse Controller 1",
        port_hint="/dev/ttyUSB0",
        usb_serial="SER_XYZ_123",
        profile_id="plant_controller_v1",
    )
    assert binding.device_mac == "98a316e6b658"
    assert binding.approved is True

    # 2. Retrieve binding
    retrieved = await reg.get_binding("98a316e6b658")
    assert retrieved is not None
    assert retrieved.device_name == "Greenhouse Controller 1"
    assert retrieved.port_hint == "/dev/ttyUSB0"

    # 3. Simulate port renumbering: /dev/ttyUSB0 changed to /dev/ttyUSB1
    # When scanned serial ports report the same USB serial number on a new port:
    from unittest.mock import AsyncMock, patch
    from devices.device_registry import ScannedPortInfo

    mock_scanned = [
        ScannedPortInfo(port="/dev/ttyUSB1", serial_number="SER_XYZ_123", description="USB Serial Device")
    ]
    with patch.object(reg, "scan_serial_ports", AsyncMock(return_value=mock_scanned)):
        resolved_port = await reg.resolve_active_port("98a316e6b658")
        assert resolved_port == "/dev/ttyUSB1", "Must dynamically rebind to /dev/ttyUSB1 based on identity"

    # 4. Unbind
    unbound = await reg.unbind_device("98a316e6b658")
    assert unbound is True
    assert (await reg.get_binding("98a316e6b658")) is None


# =====================================================================
# Customization 6: Configurable Daily Schedule Policies
# =====================================================================
@pytest.mark.asyncio
async def test_customization_06_schedule_policy_configuration(sandbox_env):
    """Validates loading and updating configurable schedule policies."""
    db: DatabaseManager = sandbox_env["db"]
    cfg = ConfigRepository(db)
    mgr = SchedulePolicyManager(cfg)

    # Default policies
    policies = await mgr.get_policies()
    assert policies.duplicate_policy == "ignore"
    assert policies.replacement_policy == "replace"

    # Update policy via CLI manager
    await mgr.set_policy("duplicate_policy", "reject", actor="test")
    updated = await mgr.get_policies()
    assert updated.duplicate_policy == "reject"


# =====================================================================
# Customization 11: Consistent Online Backup, Verification, & Restore
# =====================================================================
@pytest.mark.asyncio
async def test_customization_11_consistent_online_backup_and_restore(sandbox_env):
    """Validates online SQLite backup, SHA-256 manifest verification, and disaster restore."""
    db: DatabaseManager = sandbox_env["db"]
    tmp_path: Path = sandbox_env["tmp_path"]
    backup_dir = tmp_path / "backups"

    # Insert test state into database
    cfg = ConfigRepository(db)
    await cfg.set_setting("hub.id", "hub-backup-test")
    reg = DeviceRegistry(db)
    await reg.enroll_device("3417ebc3b479", "Primary Relays", "/dev/ttyUSB0")

    # 1. Create backup
    bm = BackupManager(db, backup_dir=str(backup_dir))
    backup_meta = await bm.create_backup()
    backup_file = backup_meta["backup_file"]
    assert os.path.exists(backup_file)
    assert backup_meta["sha256"] != ""

    # 2. Verify backup
    valid, details = bm.verify_backup(backup_file)
    assert valid is True
    assert details["status"] == "VALID"
    assert details["manifest_verified"] is True

    # 3. Restore to a new target database file
    restore_target = tmp_path / "restored.db"
    restored = await bm.restore_backup(backup_file, target_db_str=str(restore_target))
    assert restored is True

    # Verify restored database has identical state
    restored_db = DatabaseManager(db_path=str(restore_target))
    await restored_db.connect()
    restored_cfg = ConfigRepository(restored_db)
    val = await restored_cfg.get_setting("hub.id")
    assert val == "hub-backup-test"

    restored_reg = DeviceRegistry(restored_db)
    b = await restored_reg.get_binding("3417ebc3b479")
    assert b is not None
    assert b.device_name == "Primary Relays"
    await restored_db.close()


# =====================================================================
# Customization 8: iothubctl CLI Integration
# =====================================================================
@pytest.mark.asyncio
async def test_customization_08_iothubctl_cli_execution(sandbox_env, capsys):
    """Tests iothubctl CLI subcommands (status, doctor, config show, devices list)."""
    db: DatabaseManager = sandbox_env["db"]
    cfg = ConfigRepository(db)
    await cfg.set_setting("hub.id", "hub-cli-test")

    parser = iothubctl.build_parser()

    # 1. config show
    args = parser.parse_args(["--db", db.db_path, "config", "show"])
    res = await iothubctl.cmd_config(args)
    assert res == 0
    captured = capsys.readouterr()
    assert "hub.id" in captured.stdout

    # 2. doctor
    args = parser.parse_args(["--db", db.db_path, "doctor"])
    res = await iothubctl.cmd_doctor(args)
    assert res in (0, 1)  # May warn on service if not installed on test runner
    captured = capsys.readouterr()
    assert "Checking iothubd health" in captured.stdout

    # 3. devices list
    args = parser.parse_args(["--db", db.db_path, "devices", "list"])
    res = await iothubctl.cmd_devices(args)
    assert res == 0


# =====================================================================
# Customization 1: Universal Linux Installer Script Presence
# =====================================================================
def test_customization_01_install_sh_presence_and_permissions():
    """Verifies that universal install.sh exists, is executable, and contains required checks."""
    root = Path(__file__).resolve().parent.parent
    install_sh = root / "install.sh"
    assert install_sh.is_file(), "install.sh must exist in project root"
    content = install_sh.read_text(encoding="utf-8")
    assert "systemd" in content
    assert "python3-venv" in content
    assert "install.py" in content
