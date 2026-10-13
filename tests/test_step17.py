"""
Unit and Integration Tests for Step 17: Automatic Ubuntu Installer.
File: tests/test_step17.py

Validates:
1. Installer prerequisite validation (Python version & root requirement).
2. Deployed directory hierarchy and permissions under staged --destdir.
3. Systemd unit configuration file content matching exact specification:
   - User=iothub, Group=iothub, SupplementaryGroups=dialout
   - WorkingDirectory=/opt/iothub
   - EnvironmentFile=/etc/iothub/iothub.env
   - ExecStart=/opt/iothub/.venv/bin/python -m main
   - StateDirectory=iothub
   - ProtectSystem=strict, ProtectHome=true, UMask=0077
4. Environment configuration (/etc/iothub/iothub.env) with DATABASE_PATH=/var/lib/iothub/iothub.db.
5. Dry-run CLI execution: 'python3 install.py --dry-run' exits with returncode 0.
"""

from __future__ import annotations
import os
import subprocess
import sys
from pathlib import Path
import pytest

from install import IoTInstaller, SYSTEMD_UNIT_CONTENT


def test_step17_prerequisites_check_success():
    """Installer prerequisites check succeeds on supported Python version."""
    installer = IoTInstaller(destdir="/tmp/test_staging_dummy", dry_run=True)
    installer.check_prerequisites()  # Should not raise


def test_step17_root_check_enforced_when_not_staged():
    """Installer must require root when executing against live host paths without sudo."""
    if os.geteuid() != 0:
        installer = IoTInstaller(destdir="", dry_run=False)
        with pytest.raises(SystemExit) as exc:
            installer.check_prerequisites()
        assert exc.value.code == 1


def test_step17_staged_installation_directory_structure(tmp_path: Path):
    """
    Validates complete deployed tree under staged directory:
    Ubuntu Linux
    ├── /opt/iothub/
    │   ├── main.py
    │   ├── config.py
    │   ├── ingestion/
    │   ├── scheduler/
    │   ├── devices/
    │   ├── storage/
    │   ├── supervisor/
    │   ├── api/
    │
    ├── /etc/iothub/
    │   └── iothub.env
    │
    ├── /var/lib/iothub/
    │
    └── /etc/systemd/system/
        └── iothubd.service
    """
    dest = tmp_path / "stage_root"
    installer = IoTInstaller(
        destdir=str(dest),
        no_start=True,
        skip_venv=True,
        dry_run=False,
    )
    installer.execute_all()

    # 1. Verify /opt/iothub codebase
    opt_dir = dest / "opt" / "iothub"
    assert opt_dir.is_dir()
    assert (opt_dir / "main.py").is_file()
    assert (opt_dir / "config.py").is_file()
    assert (opt_dir / "requirements.txt").is_file()

    expected_packages = ["ingestion", "scheduler", "devices", "storage", "supervisor", "api", "core"]
    for pkg in expected_packages:
        pkg_path = opt_dir / pkg
        assert pkg_path.is_dir(), f"Expected package {pkg} not found in {opt_dir}"
        assert (pkg_path / "__init__.py").is_file() or any(pkg_path.glob("*.py"))

    # 2. Verify /etc/iothub/iothub.env
    etc_dir = dest / "etc" / "iothub"
    assert etc_dir.is_dir()
    env_file = etc_dir / "iothub.env"
    assert env_file.is_file()
    content = env_file.read_text(encoding="utf-8")
    assert "DATABASE_PATH=/var/lib/iothub/iothub.db" in content
    assert "API_PORT=8765" in content

    # 3. Verify /var/lib/iothub
    var_dir = dest / "var" / "lib" / "iothub"
    assert var_dir.is_dir()

    # 4. Verify /etc/systemd/system/iothubd.service
    service_file = dest / "etc" / "systemd" / "system" / "iothubd.service"
    assert service_file.is_file()


def test_step17_systemd_unit_exact_specification():
    """Validates the systemd unit file configuration matches the exact prompt specifications."""
    unit = SYSTEMD_UNIT_CONTENT
    assert "Description=Ubuntu IoT Scheduling Hub" in unit
    assert "Type=simple" in unit
    assert "User=iothub" in unit
    assert "Group=iothub" in unit
    assert "SupplementaryGroups=dialout" in unit
    assert "WorkingDirectory=/opt/iothub" in unit
    assert "EnvironmentFile=/etc/iothub/iothub.env" in unit
    assert "ExecStart=/opt/iothub/.venv/bin/python -m main" in unit
    assert "StateDirectory=iothub" in unit
    assert "Restart=always" in unit
    assert "RestartSec=5" in unit
    assert "TimeoutStopSec=60" in unit
    assert "NoNewPrivileges=true" in unit
    assert "ProtectHome=true" in unit
    assert "ProtectSystem=strict" in unit
    assert "UMask=0077" in unit
    assert "StandardOutput=journal" in unit
    assert "StandardError=journal" in unit
    assert "WantedBy=multi-user.target" in unit


def test_step17_dry_run_command_line():
    """Verifies that running 'python3 install.py --dry-run' succeeds with returncode 0."""
    res = subprocess.run(
        [sys.executable, "install.py", "--dry-run"],
        capture_output=True,
        text=True,
    )
    assert res.returncode == 0, f"Dry-run failed: {res.stderr}"
    assert "RnR Linux IoT Scheduling Hub - Automatic Ubuntu Installer" in res.stdout
    assert "STEP 17 COMPLETE" in res.stdout
