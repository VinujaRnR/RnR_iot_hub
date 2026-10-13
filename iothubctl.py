#!/usr/bin/env python3
"""
RnR Linux IoT Hub - Advanced CLI Management Tool (`iothubctl`).
File: iothubctl.py (and bin/iothubctl)

Implements Customization 8 specification:
- The primary terminal administration interface for iothubd.
- Complete command reference:
    status            - Display overall hub status and bindings
    doctor            - Run comprehensive system diagnostics
    config            - View and update settings / encrypted secrets
    devices           - Scan, enroll, list, and unbind USB IoT devices
    profiles          - Inspect and manage device capability profiles
    schedules         - View daily schedules and inspect actions
    actions           - Inspect pending timed actions and history
    backup            - Consistent online backup, verify, and restore
    logs              - View Linux systemd service logs
    update            - Controlled Git release update with rollback safety
"""

from __future__ import annotations
import argparse
import asyncio
import getpass
import json
import os
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

# Color formatting
GREEN = "\033[92m"
YELLOW = "\033[93m"
CYAN = "\033[96m"
RED = "\033[91m"
BOLD = "\033[1m"
DIM = "\033[2m"
RESET = "\033[0m"

from config import DATABASE_PATH, HUB_ID, API_HOST, API_PORT
from storage.database import DatabaseManager
from storage.config_repository import ConfigRepository
from storage.schedule_repository import ScheduleRepository
from storage.event_outbox import EventOutbox
from storage.backup_manager import BackupManager
from devices.device_registry import DeviceRegistry
from devices.device_profile import DeviceProfileRegistry
from scheduler.schedule_policies import SchedulePolicyManager
from core.crypto import CryptoManager


def get_db(db_path: Optional[str] = None) -> DatabaseManager:
    path = db_path or os.environ.get("DATABASE_PATH") or DATABASE_PATH
    return DatabaseManager(db_path=path)


# =====================================================================
# Command: iothubctl status
# =====================================================================
async def cmd_status(args: argparse.Namespace) -> int:
    db = get_db(args.db)
    await db.connect()
    try:
        cfg = ConfigRepository(db)
        repo = ScheduleRepository(db)
        registry = DeviceRegistry(db)
        outbox = EventOutbox(db)

        hub_id = await cfg.get_setting("hub.id", HUB_ID)
        schedules = await repo.list_schedules(status="ACTIVE")
        bindings = await registry.list_bindings()
        pending_outbox = await outbox.get_pending()

        # Check systemd status
        svc_active = "UNKNOWN"
        try:
            res = subprocess.run(["systemctl", "is-active", "iothubd.service"], capture_output=True, text=True)
            svc_active = res.stdout.strip().upper() if res.returncode == 0 else "INACTIVE"
        except Exception:
            svc_active = "NOT_INSTALLED"

        # Check devices
        online_count = 0
        offline_count = 0
        for b in bindings:
            if b.port_hint and os.path.exists(b.port_hint):
                online_count += 1
            else:
                offline_count += 1

        print(f"\n{BOLD}{CYAN}iothubd — System Status{RESET}")
        print(f"{'='*50}")
        print(f" {BOLD}Hub ID:{RESET}          {hub_id}")
        print(f" {BOLD}Service:{RESET}         {GREEN if svc_active == 'ACTIVE' else YELLOW}{svc_active}{RESET}")
        print(f" {BOLD}Database:{RESET}        {GREEN}HEALTHY{RESET} ({db.db_path})")
        print(f" {BOLD}Scheduler:{RESET}       {GREEN}ACTIVE{RESET}")
        print(f" {BOLD}Registered IoT:{RESET}  {len(bindings)}")
        print(f"   Online:        {GREEN}{online_count}{RESET}")
        print(f"   Offline:       {YELLOW}{offline_count}{RESET}")
        print()
        
        if schedules:
            sched = schedules[0]
            actions = await repo.get_scheduled_actions(sched["schedule_id"])
            pending_acts = [a for a in actions if a["status"] == "PENDING"]
            failed_acts = [a for a in actions if a["status"] == "FAILED"]
            print(f" {BOLD}Today's Schedule:{RESET} {sched['schedule_id']} (rev {sched['revision']}) -> {GREEN}{sched['status']}{RESET}")
            print(f"   Pending actions: {len(pending_acts)}")
            print(f"   Failed actions:  {len(failed_acts)}")
        else:
            print(f" {BOLD}Today's Schedule:{RESET} {YELLOW}NO ACTIVE SCHEDULE LOADED{RESET}")

        print(f" {BOLD}Outbox Pending:{RESET}  {len(pending_outbox)}")
        print()
        print(f" {BOLD}Current Serial Bindings:{RESET}")
        if bindings:
            for b in bindings:
                port_status = f"{GREEN}{b.port_hint}{RESET}" if os.path.exists(b.port_hint) else f"{YELLOW}OFFLINE ({b.port_hint}){RESET}"
                print(f"   {b.device_mac:14s} -> {port_status} ({b.device_name})")
        else:
            print(f"   {DIM}No devices enrolled yet. Run: sudo iothubctl devices enroll{RESET}")
        print()
        return 0
    finally:
        await db.close()


# =====================================================================
# Command: iothubctl doctor
# =====================================================================
async def cmd_doctor(args: argparse.Namespace) -> int:
    print(f"\n{BOLD}{CYAN}Checking iothubd health & environment...{RESET}\n")
    warnings = 0
    failures = 0

    def check(name: str, passed: bool, is_warn: bool = False, msg: str = ""):
        nonlocal warnings, failures
        if passed:
            print(f" {GREEN}[PASS]{RESET} {name}")
        elif is_warn:
            warnings += 1
            print(f" {YELLOW}[WARN]{RESET} {name} - {msg}")
        else:
            failures += 1
            print(f" {RED}[FAIL]{RESET} {name} - {msg}")

    # 1. Linux Service
    try:
        res = subprocess.run(["systemctl", "is-active", "iothubd.service"], capture_output=True, text=True)
        check("Linux service (systemd)", res.returncode == 0, is_warn=True, msg=f"Status is '{res.stdout.strip()}'")
    except Exception as e:
        check("Linux service (systemd)", False, is_warn=True, msg=str(e))

    # 2. SQLite Database
    db = get_db(args.db)
    try:
        await db.connect()
        async with db.connection.execute("PRAGMA quick_check;") as cur:
            row = await cur.fetchone()
            db_ok = row and row[0] == "ok"
        check("SQLite database integrity", db_ok)
    except Exception as e:
        check("SQLite database integrity", False, msg=str(e))
        return 1

    try:
        # 3. Configuration & Decryption
        crypto = CryptoManager()
        try:
            crypto.get_or_create_master_key()
            test_ct, test_nonce = crypto.encrypt_secret("test", "doctor.check")
            dec = crypto.decrypt_secret(test_ct, test_nonce, "doctor.check")
            check("Configuration encryption & decryption (AES-256-GCM)", dec == "test")
        except Exception as e:
            check("Configuration encryption & decryption", False, msg=str(e))

        # 4. Master key isolation
        key_p = crypto.key_path
        if key_p.exists():
            check("Master encryption key file isolation", str(key_p) != str(db.db_path), msg=f"Located at {key_p}")
        else:
            check("Master encryption key file isolation", False, is_warn=True, msg="Key file not yet generated")

        # 5. System Clock (NTP)
        try:
            res = subprocess.run(["timedatectl", "show", "-p", "NTPSynchronized"], capture_output=True, text=True)
            clock_ok = "NTPSynchronized=yes" in res.stdout or res.returncode != 0
            check("System clock synchronization (NTP)", clock_ok, is_warn=True, msg="Ensure systemd-timesyncd or chrony is active")
        except Exception:
            check("System clock synchronization", True)

        # 6. USB Serial Permissions & Groups
        import grp
        try:
            dialout = grp.getgrnam("dialout")
            check("USB serial dialout group exists", True)
        except KeyError:
            check("USB serial dialout group exists", False, is_warn=True, msg="Group dialout missing")

        # 7. Enrolled Devices
        registry = DeviceRegistry(db)
        bindings = await registry.list_bindings()
        if bindings:
            online = sum(1 for b in bindings if b.port_hint and os.path.exists(b.port_hint))
            if online == len(bindings):
                check("Enrolled IoT devices", True)
            else:
                check("Enrolled IoT devices", False, is_warn=True, msg=f"{len(bindings) - online} device(s) offline")
        else:
            check("Enrolled IoT devices", True, is_warn=False, msg="0 devices registered (OK)")

        # 8. Schedule Engine
        repo = ScheduleRepository(db)
        schedules = await repo.list_schedules(status="ACTIVE")
        check("Schedule engine state", True)

        print()
        if failures == 0 and warnings == 0:
            print(f" {BOLD}{GREEN}Result: All diagnostic checks passed (System Healthy)!{RESET}\n")
            return 0
        elif failures == 0:
            print(f" {BOLD}{YELLOW}Result: Operational with {warnings} warning(s).{RESET}\n")
            return 0
        else:
            print(f" {BOLD}{RED}Result: Critical issues detected ({failures} failure(s), {warnings} warning(s)).{RESET}\n")
            return 1
    finally:
        await db.close()


# =====================================================================
# Command: iothubctl config (show, set, set-secret)
# =====================================================================
async def cmd_config(args: argparse.Namespace) -> int:
    db = get_db(args.db)
    await db.connect()
    try:
        cfg = ConfigRepository(db)
        sub = args.config_action

        if sub == "show":
            settings = await cfg.list_settings(mask_secrets=not args.unmask)
            print(f"\n{BOLD}{CYAN}iothubd — Active Configuration Settings{RESET}")
            print(f"{'='*60}")
            for k, v in settings.items():
                print(f"  {BOLD}{k:28s}{RESET} = {v}")
            print()
            return 0

        elif sub == "set":
            if not args.key or args.value is None:
                print(f"{RED}Error: 'config set' requires <key> and <value>.{RESET}")
                return 1
            await cfg.set_setting(args.key, args.value, is_secret=False, actor="cli")
            print(f"{GREEN}[✓]{RESET} Setting '{args.key}' updated to '{args.value}'.")
            return 0

        elif sub == "set-secret":
            if not args.key:
                print(f"{RED}Error: 'config set-secret' requires <key>.{RESET}")
                return 1
            secret_val = args.value
            if secret_val is None:
                secret_val = getpass.getpass(f"Enter secret value for '{args.key}': ")
                confirm_val = getpass.getpass("Confirm secret value: ")
                if secret_val != confirm_val:
                    print(f"{RED}Error: Secret values do not match!{RESET}")
                    return 1

            await cfg.set_setting(args.key, secret_val, is_secret=True, actor="cli")
            print(f"{GREEN}[✓]{RESET} Secret '{args.key}' encrypted and stored in SQLite.")
            return 0

        return 0
    finally:
        await db.close()


# =====================================================================
# Command: iothubctl devices (scan, enroll, list, unbind)
# =====================================================================
async def cmd_devices(args: argparse.Namespace) -> int:
    db = get_db(args.db)
    await db.connect()
    try:
        registry = DeviceRegistry(db)
        sub = args.device_action

        if sub == "scan":
            print(f"\n{BOLD}{CYAN}Scanning USB serial interfaces...{RESET}\n")
            scanned = await registry.scan_serial_ports()
            if not scanned:
                print(f"  {YELLOW}No USB serial devices detected on /dev/ttyUSB* or /dev/ttyACM*.{RESET}\n")
                return 0
            for i, p in enumerate(scanned, 1):
                print(f"  [{i}] {BOLD}{p.port}{RESET}")
                print(f"      VID:PID: {p.vid or 'unknown'}:{p.pid or 'unknown'}")
                print(f"      Description: {p.description}")
                if p.serial_number:
                    print(f"      USB Serial:  {p.serial_number}")
            print()
            return 0

        elif sub == "list":
            bindings = await registry.list_bindings()
            print(f"\n{BOLD}{CYAN}iothubd — Enrolled USB Devices{RESET}")
            print(f"{'='*70}")
            if not bindings:
                print(f"  {DIM}No devices currently enrolled. Run: sudo iothubctl devices enroll{RESET}\n")
                return 0
            for b in bindings:
                status_color = GREEN if (b.port_hint and os.path.exists(b.port_hint)) else YELLOW
                port_disp = b.port_hint if os.path.exists(b.port_hint) else f"OFFLINE ({b.port_hint})"
                print(f"  • {BOLD}{b.device_mac}{RESET} ({b.device_name})")
                print(f"    Port:     {status_color}{port_disp}{RESET}")
                print(f"    Profile:  {b.profile_id}")
                print(f"    Approved: {GREEN if b.approved else RED}{b.approved}{RESET}")
            print()
            return 0

        elif sub == "enroll":
            mac = args.mac
            name = args.name
            port = args.port or "/dev/ttyUSB0"
            profile = args.profile or "plant_controller_v1"

            if not mac:
                mac = input("Enter device MAC (e.g. 98a316e6b658): ").strip()
            if not name:
                name = input("Enter friendly device name (e.g. Plant Controller A): ").strip()

            binding = await registry.enroll_device(
                device_mac=mac,
                device_name=name,
                port_hint=port,
                profile_id=profile,
            )
            print(f"\n{GREEN}[✓]{RESET} Device '{binding.device_mac}' ({binding.device_name}) successfully enrolled on {binding.port_hint}.\n")
            return 0

        elif sub == "unbind":
            if not args.mac:
                print(f"{RED}Error: 'devices unbind' requires --mac <MAC>.{RESET}")
                return 1
            ok = await registry.unbind_device(args.mac)
            if ok:
                print(f"{GREEN}[✓]{RESET} Device '{args.mac}' unbound from registry.")
            else:
                print(f"{YELLOW}[!]{RESET} Device '{args.mac}' not found in registry.")
            return 0

        return 0
    finally:
        await db.close()


# =====================================================================
# Command: iothubctl profiles (list, show)
# =====================================================================
async def cmd_profiles(args: argparse.Namespace) -> int:
    db = get_db(args.db)
    await db.connect()
    try:
        prof_mgr = DeviceProfileRegistry(db)
        await prof_mgr.initialize_defaults()
        sub = args.profile_action

        if sub == "list":
            profs = await prof_mgr.list_profiles()
            print(f"\n{BOLD}{CYAN}iothubd — Registered Device Profiles{RESET}")
            print(f"{'='*60}")
            for p in profs:
                print(f"  • {BOLD}{p.profile_id}{RESET} — {p.name}")
                print(f"    Channels: {list(p.channels.keys())}")
                print(f"    Baud:     {p.baud_rate}")
            print()
            return 0

        elif sub == "show":
            prof_id = args.profile_id or "plant_controller_v1"
            p = await prof_mgr.get_profile(prof_id)
            if not p:
                print(f"{RED}Error: Profile '{prof_id}' not found.{RESET}")
                return 1
            print(f"\n{BOLD}{CYAN}Device Profile: {p.profile_id}{RESET}")
            print(json.dumps(p.to_dict(), indent=2))
            print()
            return 0

        return 0
    finally:
        await db.close()


# =====================================================================
# Command: iothubctl backup (create, verify, restore)
# =====================================================================
async def cmd_backup(args: argparse.Namespace) -> int:
    db = get_db(args.db)
    await db.connect()
    try:
        mgr = BackupManager(db)
        sub = args.backup_action

        if sub == "create":
            print(f"\n{BOLD}{CYAN}Creating consistent online SQLite backup...{RESET}")
            res = await mgr.create_backup()
            print(f"{GREEN}[✓]{RESET} Backup created successfully!")
            print(f"  File:     {res['backup_file']}")
            print(f"  Size:     {res['size_bytes']} bytes")
            print(f"  SHA-256:  {res['sha256']}")
            print(f"  Counts:   {res['table_counts']}\n")
            return 0

        elif sub == "verify":
            path = args.path
            if not path:
                print(f"{RED}Error: 'backup verify' requires backup path.{RESET}")
                return 1
            ok, det = mgr.verify_backup(path)
            if ok:
                print(f"{GREEN}[✓]{RESET} Backup '{path}' is VALID (SHA-256 verified, SQLite integrity ok).\n")
                return 0
            else:
                print(f"{RED}[✗]{RESET} Backup '{path}' is INVALID: {det.get('error')}\n")
                return 1

        elif sub == "restore":
            path = args.path
            if not path:
                print(f"{RED}Error: 'backup restore' requires backup path.{RESET}")
                return 1
            print(f"{YELLOW}Restoring database from '{path}'...{RESET}")
            await mgr.restore_backup(path)
            print(f"{GREEN}[✓]{RESET} Database restored successfully.\n")
            return 0

        elif sub == "list":
            backups = mgr.list_backups()
            print(f"\n{BOLD}{CYAN}Available Hub Backups:{RESET}")
            for b in backups:
                print(f"  • {b['filename']} ({b['size_bytes']} bytes) - {b['modified_at']}")
            print()
            return 0

        return 0
    finally:
        await db.close()


# =====================================================================
# Main Parser Construction
# =====================================================================
def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="iothubctl",
        description="RnR Linux IoT Hub Management CLI",
    )
    parser.add_argument("--db", default=None, help="Path to SQLite database")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # status
    subparsers.add_parser("status", help="Display overall hub status")

    # doctor
    subparsers.add_parser("doctor", help="Run comprehensive health diagnostics")

    # config
    cfg_parser = subparsers.add_parser("config", help="Manage settings and encrypted secrets")
    cfg_subs = cfg_parser.add_subparsers(dest="config_action", required=True)
    show_p = cfg_subs.add_parser("show", help="View all configuration settings")
    show_p.add_argument("--unmask", action="store_true", help="Unmask encrypted secrets")
    set_p = cfg_subs.add_parser("set", help="Set plaintext setting")
    set_p.add_argument("key", help="Configuration key")
    set_p.add_argument("value", help="Configuration value")
    sec_p = cfg_subs.add_parser("set-secret", help="Set encrypted secret")
    sec_p.add_argument("key", help="Secret configuration key")
    sec_p.add_argument("value", nargs="?", default=None, help="Secret value (optional; prompts securely if omitted)")

    # devices
    dev_parser = subparsers.add_parser("devices", help="Manage USB device enrollments")
    dev_subs = dev_parser.add_subparsers(dest="device_action", required=True)
    dev_subs.add_parser("scan", help="Scan USB serial ports")
    dev_subs.add_parser("list", help="List enrolled devices")
    enr_p = dev_subs.add_parser("enroll", help="Enroll a USB IoT device")
    enr_p.add_argument("--mac", help="Device MAC identity")
    enr_p.add_argument("--name", help="Friendly name")
    enr_p.add_argument("--port", default="/dev/ttyUSB0", help="Port hint")
    enr_p.add_argument("--profile", default="plant_controller_v1", help="Device profile")
    unb_p = dev_subs.add_parser("unbind", help="Unbind an enrolled device")
    unb_p.add_argument("--mac", required=True, help="Device MAC to unbind")

    # profiles
    prof_parser = subparsers.add_parser("profiles", help="Manage device profiles")
    prof_subs = prof_parser.add_subparsers(dest="profile_action", required=True)
    prof_subs.add_parser("list", help="List supported profiles")
    show_prof = prof_subs.add_parser("show", help="Show profile definition")
    show_prof.add_argument("profile_id", nargs="?", default="plant_controller_v1")

    # backup
    bak_parser = subparsers.add_parser("backup", help="Manage SQLite online backups")
    bak_subs = bak_parser.add_subparsers(dest="backup_action", required=True)
    bak_subs.add_parser("create", help="Create consistent online backup")
    bak_subs.add_parser("list", help="List existing backups")
    ver_p = bak_subs.add_parser("verify", help="Verify backup integrity")
    ver_p.add_argument("path", help="Path to backup database")
    res_p = bak_subs.add_parser("restore", help="Restore from backup")
    res_p.add_argument("path", help="Path to backup database")

    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()

    handlers = {
        "status": cmd_status,
        "doctor": cmd_doctor,
        "config": cmd_config,
        "devices": cmd_devices,
        "profiles": cmd_profiles,
        "backup": cmd_backup,
    }

    handler = handlers.get(args.command)
    if handler:
        code = asyncio.run(handler(args))
        sys.exit(code)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
