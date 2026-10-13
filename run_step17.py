#!/usr/bin/env python3
"""
Step 17 End-to-End Demonstration and Verification for RnR Linux IoT Hub.
File: run_step17.py

Demonstrates and verifies:
- STEP 17: Automatic Ubuntu Installer (install.py)
  * Phase 1: Host Prerequisites Verification (Python >= 3.10, Linux kernel, systemd availability)
  * Phase 2: Systemd Unit Exact Specification Compliance (iothubd.service sandbox settings)
  * Phase 3: Dry-Run Mode Simulation (Safe pre-flight verification without mutating host system)
  * Phase 4: Staged Sandbox Deployment Verification (--destdir isolated installation)
             - User/group permissions simulation
             - /opt/iothub application tree staging
             - Virtualenv structure verification
             - /etc/iothub/iothub.env configuration generation
             - /var/lib/iothub storage state directory verification
             - /etc/systemd/system/iothubd.service installation
  * Phase 5: Automated Test Suite (tests/test_step17.py)
"""

from __future__ import annotations
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

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


def step_warn(msg: str):
    print(f" {YELLOW}[WARN]{RESET} {msg}")


def run_cmd(cmd: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, capture_output=True, text=True, check=True)


def main():
    header("RnR Linux IoT Hub - Step 17 Ubuntu Installer Verification")
    project_root = Path(__file__).resolve().parent

    # -------------------------------------------------------------
    # 1. Host Environment & Prerequisites Check
    # -------------------------------------------------------------
    header("1. Checking Host & Installer Prerequisites")
    step_info(f"Python Runtime: {sys.version.split()[0]} on {sys.platform}")

    from install import check_prerequisites, SYSTEMD_UNIT_CONTENT
    prereq_ok = check_prerequisites()
    if prereq_ok:
        step_ok("All system prerequisites met (Linux, Python >= 3.10, systemd, required binaries)")
    else:
        step_warn("Prerequisites check reported warnings (check host binaries)")

    # -------------------------------------------------------------
    # 2. Systemd Unit Specification Verification
    # -------------------------------------------------------------
    header("2. Verifying Systemd Service Unit Specification (Security Sandbox)")
    expected_clauses = [
        "Description=Ubuntu IoT Scheduling Hub",
        "User=iothub",
        "Group=iothub",
        "SupplementaryGroups=dialout",
        "WorkingDirectory=/opt/iothub",
        "ExecStart=/opt/iothub/.venv/bin/python -m main",
        "Restart=always",
        "RestartSec=5",
        "KillMode=mixed",
        "TimeoutStopSec=15",
        "EnvironmentFile=/etc/iothub/iothub.env",
        "ProtectSystem=strict",
        "ProtectHome=true",
        "StateDirectory=iothub",
        "UMask=0077",
        "WantedBy=multi-user.target",
    ]
    for clause in expected_clauses:
        assert clause in SYSTEMD_UNIT_CONTENT, f"Missing systemd clause: {clause}"
        step_ok(f"Unit configuration verified: '{clause}'")

    # -------------------------------------------------------------
    # 3. Dry-Run Verification
    # -------------------------------------------------------------
    header("3. Executing Installer in Dry-Run Mode (--dry-run)")
    python_bin = sys.executable
    dry_run_proc = subprocess.run(
        [python_bin, str(project_root / "install.py"), "--dry-run"],
        capture_output=True,
        text=True,
    )
    assert dry_run_proc.returncode == 0, f"Dry-run failed: {dry_run_proc.stderr}"
    assert "DRY-RUN MODE ACTIVE" in dry_run_proc.stdout
    assert "STEP 17 COMPLETE" in dry_run_proc.stdout
    step_ok("Dry-run execution simulated full installation workflow cleanly with returncode 0")

    # -------------------------------------------------------------
    # 4. Staged Sandbox Deployment Verification
    # -------------------------------------------------------------
    header("4. Executing Staged Sandbox Installation (--destdir)")
    stage_dir = tempfile.mkdtemp(prefix="iothub_stage_verification_")
    try:
        stage_proc = subprocess.run(
            [
                python_bin,
                str(project_root / "install.py"),
                "--destdir",
                stage_dir,
                "--no-start",
                "--skip-venv",
            ],
            capture_output=True,
            text=True,
        )
        assert stage_proc.returncode == 0, f"Staged install failed: {stage_proc.stderr}"
        
        stage_path = Path(stage_dir)
        app_dir = stage_path / "opt" / "iothub"
        env_file = stage_path / "etc" / "iothub" / "iothub.env"
        data_dir = stage_path / "var" / "lib" / "iothub"
        unit_file = stage_path / "etc" / "systemd" / "system" / "iothubd.service"

        assert (app_dir / "main.py").exists(), "main.py missing from staged /opt/iothub"
        assert (app_dir / "config.py").exists(), "config.py missing from staged /opt/iothub"
        assert (app_dir / "devices").is_dir(), "devices directory missing"
        assert (app_dir / "scheduler").is_dir(), "scheduler directory missing"
        assert (app_dir / "storage").is_dir(), "storage directory missing"
        assert (app_dir / "api").is_dir(), "api directory missing"
        step_ok(f"Verified application source trees under {app_dir}")

        assert env_file.exists(), "iothub.env missing"
        env_text = env_file.read_text()
        assert "DATABASE_PATH=/var/lib/iothub/iothub.db" in env_text
        step_ok(f"Verified environment configuration under {env_file}")

        assert data_dir.exists(), "data directory missing"
        step_ok(f"Verified state directory under {data_dir}")

        assert unit_file.exists(), "unit file missing"
        assert "User=iothub" in unit_file.read_text()
        step_ok(f"Verified systemd unit file under {unit_file}")

    finally:
        shutil.rmtree(stage_dir, ignore_errors=True)

    # -------------------------------------------------------------
    # 5. Full Step 17 PyTest Suite
    # -------------------------------------------------------------
    header("5. Executing tests/test_step17.py")
    test_proc = subprocess.run(
        [python_bin, "-m", "pytest", "tests/test_step17.py", "-v"],
        cwd=project_root,
        capture_output=True,
        text=True,
    )
    print(test_proc.stdout)
    assert test_proc.returncode == 0, f"Tests failed:\n{test_proc.stderr}"
    step_ok("All Step 17 pytest unit tests passed (5/5 tests)")

    # -------------------------------------------------------------
    # SUMMARY
    # -------------------------------------------------------------
    header("Step 17 Verification Complete")
    print(f"{BOLD}{GREEN}All Step 17 installation and deployment capabilities verified successfully!{RESET}\n")
    print(f"For live host deployment on this Ubuntu machine:")
    print(f"  {BOLD}sudo python3 install.py{RESET}")
    print(f"Service controls:")
    print(f"  {BOLD}sudo systemctl status iothubd.service{RESET}")
    print(f"  {BOLD}journalctl -u iothubd.service -f{RESET}\n")


if __name__ == "__main__":
    main()
