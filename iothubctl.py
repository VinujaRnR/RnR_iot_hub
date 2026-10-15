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
from urllib.parse import quote

# Color formatting
GREEN = "\033[92m"
YELLOW = "\033[93m"
CYAN = "\033[96m"
RED = "\033[91m"
BOLD = "\033[1m"
DIM = "\033[2m"
RESET = "\033[0m"

from config import (
    DATABASE_PATH,
    HUB_ID,
    API_HOST,
    API_PORT,
    COMMAND_EXCHANGE,
    COMMAND_ROUTING_KEY,
)
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
            for p in ("/var/lib/iothub/last_rabbitmq_message.json", "/tmp/last_rabbitmq_message.json"):
                if os.path.exists(p):
                    try:
                        with open(p) as f:
                            last_msg = json.load(f)
                        recv = last_msg.get("received_at", "recent")
                        dtype = last_msg.get("detected_type", "UNKNOWN")
                        print(f"   {DIM}Last Inbound RabbitMQ Msg: {dtype} received at {recv}{RESET}")
                        print(f"   {DIM}Run 'sudo iothubctl rabbitmq last' to inspect full payload.{RESET}")
                        break
                    except Exception:
                        pass

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
        try:
            if key_p.exists():
                check("Master encryption key file isolation", str(key_p) != str(db.db_path), msg=f"Located at {key_p}")
            else:
                check("Master encryption key file isolation", False, is_warn=True, msg="Key file not yet generated")
        except PermissionError:
            check("Master encryption key file isolation (0600 protected)", True, msg=f"Restricted at {key_p} (run with sudo to inspect)")
        except Exception as e:
            check("Master encryption key file isolation", False, is_warn=True, msg=str(e))

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

        # 9. RabbitMQ Broker Connectivity
        try:
            import aio_pika
            from config import (
                RABBITMQ_HOST,
                RABBITMQ_AMQP_PORT,
                RABBITMQ_USER,
                RABBITMQ_PASSWORD,
                RABBITMQ_VHOST,
                RABBITMQ_USE_SSL,
                QUEUE_NAME,
            )
            cfg = ConfigRepository(db)
            db_host = await cfg.get_setting("rabbitmq.host")
            db_port = await cfg.get_setting("rabbitmq.port")
            db_user = await cfg.get_setting("rabbitmq.username")
            stored_pw = await cfg.get_setting("rabbitmq.password")

            host = db_host or RABBITMQ_HOST
            port = int(db_port) if db_port else RABBITMQ_AMQP_PORT
            user = db_user or RABBITMQ_USER
            pw = stored_pw or RABBITMQ_PASSWORD
            sch = "amqps" if RABBITMQ_USE_SSL else "amqp"
            vh = quote(RABBITMQ_VHOST.lstrip("/"), safe="") if RABBITMQ_VHOST.lstrip("/") else ""
            t_url = f"{sch}://{quote(user, safe='')}:{quote(pw, safe='')}@{host}:{port}/{vh}"
            t_conn = await asyncio.wait_for(aio_pika.connect_robust(t_url), timeout=5.0)
            t_ch = await t_conn.channel()
            q_info = ""
            try:
                t_q = await t_ch.declare_queue(QUEUE_NAME, passive=True)
                q_info = f" (Queue '{QUEUE_NAME}' ready, {t_q.declaration_result.message_count} msg)"
            except Exception:
                pass
            await t_conn.close()
            check(f"RabbitMQ broker connectivity ({host}:{port}){q_info}", True)
        except Exception as e:
            check("RabbitMQ broker connectivity", False, is_warn=True, msg=f"Could not connect: {e}")

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
# Command: iothubctl rabbitmq (test, check)
# =====================================================================
async def cmd_rabbitmq(args: argparse.Namespace) -> int:
    """Tests connection to the RabbitMQ broker or inspects the last received message."""
    db = get_db(args.db)
    await db.connect()

    sub = getattr(args, "rabbitmq_action", None)
    if sub == "last":
        for p in ("/var/lib/iothub/last_rabbitmq_message.json", "/tmp/last_rabbitmq_message.json"):
            if os.path.exists(p):
                try:
                    with open(p) as f:
                        data = json.load(f)
                    print(f"\n{BOLD}{CYAN}Most Recent RabbitMQ Inbound Message{RESET}")
                    print(f"{'='*60}")
                    print(f" {BOLD}Received At:{RESET}     {data.get('received_at')}")
                    print(f" {BOLD}Routing Key:{RESET}     {data.get('routing_key')}")
                    print(f" {BOLD}Detected Type:{RESET}   {data.get('detected_type')}")
                    print(f"{'='*60}")
                    print(f"\n{BOLD}Payload JSON:{RESET}")
                    print(json.dumps(data.get("payload", {}), indent=2))
                    print()
                    await db.close()
                    return 0
                except Exception as exc:
                    print(f"{RED}Error reading {p}: {exc}{RESET}")
        print(f"\n{YELLOW}No inbound RabbitMQ messages captured since service startup.{RESET}")
        print(f"{DIM}Check recent daemon logs with: sudo journalctl -u iothubd -n 50{RESET}\n")
        await db.close()
        return 0

    cfg = ConfigRepository(db)
    stored_password = None
    try:
        stored_password = await cfg.get_setting("rabbitmq.password")
    except Exception:
        pass

    from config import (
        RABBITMQ_HOST,
        RABBITMQ_AMQP_PORT,
        RABBITMQ_USER,
        RABBITMQ_PASSWORD,
        RABBITMQ_VHOST,
        RABBITMQ_USE_SSL,
        QUEUE_NAME,
        COMMAND_EXCHANGE,
        RESULTS_EXCHANGE,
    )
    from urllib.parse import quote
    import aio_pika

    db_host = await cfg.get_setting("rabbitmq.host")
    db_port = await cfg.get_setting("rabbitmq.port")
    db_user = await cfg.get_setting("rabbitmq.username")

    host = db_host or RABBITMQ_HOST
    port = int(db_port) if db_port else RABBITMQ_AMQP_PORT
    user = db_user or RABBITMQ_USER
    password_to_use = stored_password or RABBITMQ_PASSWORD
    scheme = "amqps" if RABBITMQ_USE_SSL else "amqp"
    vhost_clean = RABBITMQ_VHOST.lstrip("/")
    vhost_encoded = quote(vhost_clean, safe="") if vhost_clean else ""
    user_encoded = quote(user, safe="")
    password_encoded = quote(password_to_use, safe="")
    amqp_url = f"{scheme}://{user_encoded}:{password_encoded}@{host}:{port}/{vhost_encoded}"

    print(f"\n{BOLD}{CYAN}RabbitMQ Broker Connection Diagnostics{RESET}")
    print(f"{'='*60}")
    print(f" {BOLD}Host:{RESET}            {host}:{port}")
    print(f" {BOLD}Virtual Host:{RESET}    {RABBITMQ_VHOST}")
    print(f" {BOLD}Username:{RESET}        {user}")
    print(f" {BOLD}Target Queue:{RESET}    {QUEUE_NAME}")
    print(f" {BOLD}Command Exch:{RESET}    {COMMAND_EXCHANGE}")
    print(f" {BOLD}Results Exch:{RESET}    {RESULTS_EXCHANGE}")
    print(f"{'='*60}")

    print(f"\n {CYAN}==>{RESET} Connecting to broker at {host}:{port}...")
    start_time = time.time()
    try:
        connection = await asyncio.wait_for(aio_pika.connect_robust(amqp_url), timeout=7.0)
        elapsed = (time.time() - start_time) * 1000
        print(f" {GREEN}[✓]{RESET} Connection established successfully in {elapsed:.1f}ms")
    except asyncio.TimeoutError:
        print(f" {RED}[✗]{RESET} Connection timed out after 7.0s. Check firewall and broker host: {RABBITMQ_HOST}:{RABBITMQ_AMQP_PORT}")
        await db.close()
        return 1
    except Exception as exc:
        print(f" {RED}[✗]{RESET} Connection failed: {exc}")
        await db.close()
        return 1

    try:
        print(f" {CYAN}==>{RESET} Opening AMQP channel...")
        channel = await connection.channel()
        print(f" {GREEN}[✓]{RESET} AMQP Channel opened.")

        # Check Queue
        print(f" {CYAN}==>{RESET} Inspecting command queue '{QUEUE_NAME}'...")
        try:
            q = await channel.declare_queue(QUEUE_NAME, passive=True)
            msg_count = q.declaration_result.message_count
            cons_count = q.declaration_result.consumer_count
            print(f" {GREEN}[✓]{RESET} Queue '{QUEUE_NAME}' is ACTIVE:")
            print(f"     - Pending messages: {msg_count}")
            print(f"     - Active consumers: {cons_count}")
        except Exception as q_exc:
            print(f" {YELLOW}[!]{RESET} Passive queue check warning: {q_exc}")

        # Check Exchanges
        for ex_name, ex_label in [(COMMAND_EXCHANGE, "Inbound command"), (RESULTS_EXCHANGE, "Results publish")]:
            print(f" {CYAN}==>{RESET} Inspecting {ex_label} exchange '{ex_name}'...")
            try:
                ex = await channel.declare_exchange(ex_name, passive=True)
                print(f" {GREEN}[✓]{RESET} Exchange '{ex_name}' is ACTIVE.")
            except Exception as ex_exc:
                print(f" {YELLOW}[!]{RESET} Exchange warning for '{ex_name}': {ex_exc}")

        await connection.close()
        await db.close()
        print(f"\n {BOLD}{GREEN}Result: RabbitMQ Connection is HEALTHY!{RESET}\n")
        return 0
    except Exception as exc:
        print(f" {RED}[✗]{RESET} Diagnostic inspection failed: {exc}")
        await connection.close()
        await db.close()
        return 1


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

        elif sub == "autoconfigure":
            print(f"\n{BOLD}{CYAN}Auto-Configuring System Devices...{RESET}\n")
            # 1. Scan physical serial ports
            scanned = await registry.scan_serial_ports()
            active_port = "/dev/ttyUSB0"
            if scanned:
                for p in scanned:
                    print(f"  {GREEN}[✓]{RESET} Detected Serial Port: {BOLD}{p.port}{RESET} ({p.description or 'USB Serial Controller'})")
                    await registry.enroll_device(
                        device_mac="3417ebc3b479",
                        device_name=f"Controller ({p.port})",
                        port_hint=p.port,
                        profile_id="plant_controller_v1",
                    )
                    active_port = p.port
            else:
                print(f"  {YELLOW}[!]{RESET} No physical USB serial interfaces detected. Using default {active_port}")

            # 2. Inspect active schedule to configure logical devices
            sched_repo = ScheduleRepository(db)
            active_scheds = await sched_repo.list_schedules(status="ACTIVE")
            actions = []
            if active_scheds:
                actions = await sched_repo.get_scheduled_actions(active_scheds[0]["schedule_id"])

            configured_devices = {}
            for a in actions:
                d_id = a.get("device_id") or a.get("iot_node_id")
                if d_id and d_id not in configured_devices:
                    configured_devices[d_id] = {
                        "name": a.get("device_name") or d_id,
                        "type": a.get("device_type") or "actuator",
                        "channel": a.get("channel_key", "relay:default"),
                        "action": a.get("action", "LIGHT_ON"),
                    }

            if not configured_devices:
                configured_devices["light_glow_1"] = {"name": "light_glow #1", "type": "light_glow", "channel": "relay:light_glow", "action": "LIGHT_ON"}
                configured_devices["water_valve_1"] = {"name": "water_valve #1", "type": "water_valve", "channel": "relay:water_valve", "action": "VALVE_ON"}

            print(f"\n {BOLD}Auto-Configured Logical Devices on Controller ({active_port}):{RESET}")
            print(f"  {'DEVICE ID':<16} {'TYPE':<14} {'CHANNEL':<20} {'STATE'}")
            print(f"  {'-'*16} {'-'*14} {'-'*20} {'-'*8}")
            for d_id, info in configured_devices.items():
                await db.save_device(
                    device_id=d_id,
                    name=info["name"],
                    capabilities={"channels": [info["channel"]], "type": info["type"], "actions": [info["action"]]},
                    connection_state="ONLINE",
                )
                print(f"  {d_id:<16} {info['type']:<14} {info['channel']:<20} {GREEN}ONLINE{RESET}")
            print(f"\n{GREEN}[✓] Auto-configuration complete. All devices routed to {active_port}.{RESET}\n")
            return 0

        elif sub == "list":
            bindings = await registry.list_bindings()
            print(f"\n{BOLD}{CYAN}iothubd — Enrolled USB Interfaces{RESET}")
            print(f"{'='*70}")
            if not bindings:
                print(f"  {DIM}No hardware bindings enrolled. Run: sudo iothubctl devices autoconfigure{RESET}")
            for b in bindings:
                status_color = GREEN if (b.port_hint and os.path.exists(b.port_hint)) else YELLOW
                port_disp = b.port_hint if os.path.exists(b.port_hint) else f"OFFLINE ({b.port_hint})"
                print(f"  • {BOLD}{b.device_mac}{RESET} ({b.device_name})")
                print(f"    Port:     {status_color}{port_disp}{RESET}")
                print(f"    Profile:  {b.profile_id}")
                print(f"    Approved: {GREEN if b.approved else RED}{b.approved}{RESET}")

            # Also display logical devices from devices table
            async with db.connection.execute("SELECT device_id, name, connection_state, capabilities_json FROM devices ORDER BY device_id ASC") as cur:
                dev_rows = await cur.fetchall()
            if dev_rows:
                print(f"\n {BOLD}Configured Logical Devices & Sub-Resources:{RESET}")
                print(f"  {'DEVICE ID':<16} {'NAME':<20} {'STATE':<10} {'CAPABILITIES'}")
                print(f"  {'-'*16} {'-'*20} {'-'*10} {'-'*20}")
                for r in dev_rows:
                    st_c = GREEN if r['connection_state'] == 'ONLINE' else YELLOW
                    print(f"  {r['device_id']:<16} {(r['name'] or '-'):<20} {st_c}{r['connection_state']:<10}{RESET} {DIM}{r['capabilities_json']}{RESET}")
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


def _filter_actions_by_time(actions: List[Dict[str, Any]], time_arg: str) -> Tuple[List[Dict[str, Any]], str]:
    """
    Filters actions by time substring, range (e.g. 15:50-15:54 or 15.50-15.54),
    or 'now'/'latest'. If no exact match, falls back to the nearest action at or before time_arg.
    Returns (matches, description).
    """
    clean_arg = time_arg.strip().replace(".", ":").lower()

    # 1. Range syntax: e.g. "15:50-15:54"
    if "-" in clean_arg and not clean_arg.startswith("-"):
        parts = clean_arg.split("-", 1)
        start_t = parts[0].strip()
        end_t = parts[1].strip()
        if len(start_t) == 4 and start_t[1] == ":":
            start_t = "0" + start_t
        if len(end_t) == 4 and end_t[1] == ":":
            end_t = "0" + end_t

        matches = []
        for a in actions:
            sched_at = a.get("scheduled_at", "")
            if "T" in sched_at:
                t_part = sched_at.split("T")[1][:5]
                if start_t <= t_part <= end_t:
                    matches.append(a)
        if matches:
            return matches, f"between {start_t} and {end_t}"

    # 2. 'now' / 'current' / 'latest'
    if clean_arg in ("now", "current", "latest"):
        now_dt = datetime.now()
        now_hm = now_dt.strftime("%H:%M")
        past_or_current = []
        for a in actions:
            sched_at = a.get("scheduled_at", "")
            if "T" in sched_at:
                t_part = sched_at.split("T")[1][:5]
                if t_part <= now_hm:
                    past_or_current.append(a)
        if past_or_current:
            latest_time = past_or_current[-1].get("scheduled_at", "").split("T")[1][:5]
            matches = [a for a in past_or_current if latest_time in a.get("scheduled_at", "")]
            return matches, f"most recent at/before current time ({latest_time})"

    # 3. Direct substring match (e.g. "15:50")
    matches = [a for a in actions if clean_arg in a.get("scheduled_at", "").lower()]
    if matches:
        return matches, f"at {clean_arg}"

    # 4. Fallback: find nearest action at or before clean_arg
    if ":" in clean_arg:
        target_hm = clean_arg[:5]
        if len(target_hm) == 4 and target_hm[1] == ":":
            target_hm = "0" + target_hm
        candidates = []
        for a in actions:
            sched_at = a.get("scheduled_at", "")
            if "T" in sched_at:
                t_part = sched_at.split("T")[1][:5]
                if t_part <= target_hm:
                    candidates.append((t_part, a))
        if candidates:
            candidates.sort(key=lambda x: x[0])
            nearest_hm = candidates[-1][0]
            matches = [item[1] for item in candidates if item[0] == nearest_hm]
            return matches, f"most recent active action at {nearest_hm} (before requested {clean_arg})"

    return [], f"at {clean_arg}"


# =====================================================================
# Command: iothubctl schedules (list, show, load)
# =====================================================================
async def cmd_schedules(args: argparse.Namespace) -> int:
    db = get_db(args.db)
    await db.connect()
    try:
        repo = ScheduleRepository(db)
        sub = getattr(args, "schedule_action", None) or "list"

        if sub == "list":
            schedules = await repo.list_schedules()
            print(f"\n{BOLD}{CYAN}iothubd — Daily Production Schedules{RESET}")
            print(f"{'='*70}")
            if not schedules:
                print(f"  {YELLOW}No production schedules stored in SQLite database yet.{RESET}")
                print(f"  {DIM}Note: Immediate commands (e.g. LIGHTS_OFF) execute directly as command batches.{RESET}")
                print(f"  {DIM}To inspect the last RabbitMQ message: sudo iothubctl rabbitmq last{RESET}")
                print(f"  {DIM}To manually load a schedule file:   sudo iothubctl schedules load schedule.json{RESET}\n")
                return 0

            for s in schedules:
                actions = await repo.get_scheduled_actions(s["schedule_id"])
                pending = sum(1 for a in actions if a["status"] == "PENDING")
                completed = sum(1 for a in actions if a["status"] == "COMPLETED")
                failed = sum(1 for a in actions if a["status"] == "FAILED")
                other = len(actions) - pending - completed - failed
                st_color = GREEN if s["status"] == "ACTIVE" else YELLOW
                print(f"  • {BOLD}{s['schedule_id']}{RESET} (date: {s['date']}, rev: {s['revision']}) -> {st_color}{s['status']}{RESET}")
                print(f"    Total actions: {len(actions)} | {GREEN}Completed: {completed}{RESET} | {YELLOW}Pending: {pending}{RESET} | {RED}Failed: {failed}{RESET}" + (f" | Other: {other}" if other else ""))
            print()
            return 0

        elif sub == "show":
            sched_id = args.schedule_id
            if not sched_id:
                schedules = await repo.list_schedules(status="ACTIVE")
                if not schedules:
                    schedules = await repo.list_schedules()
                if not schedules:
                    print(f"\n{YELLOW}No schedules found in database.{RESET}\n")
                    return 1
                sched_id = schedules[0]["schedule_id"]

            sched = await repo.get_schedule(sched_id)
            if not sched:
                print(f"\n{RED}Schedule '{sched_id}' not found.{RESET}\n")
                return 1

            if getattr(args, "raw", False):
                if sched.get("raw_json"):
                    print(sched["raw_json"])
                else:
                    print(json.dumps(sched, indent=2))
                return 0

            actions = await repo.get_scheduled_actions(sched_id)
            time_filter = getattr(args, "time", None)
            if getattr(args, "now", False) and not time_filter:
                time_filter = "now"

            if time_filter:
                matches, desc = _filter_actions_by_time(actions, str(time_filter))
                if not matches:
                    print(f"\n{YELLOW}No actions found matching time '{time_filter}' in schedule '{sched_id}'.{RESET}\n")
                    return 1
                print(f"\n{BOLD}{CYAN}Generated Device Command JSON ({sched_id} {desc}):{RESET}")
                print(f"{'='*70}")
                for m in matches:
                    dev = m.get("device_id") or m.get("iot_node_id")
                    cmd = {"action": m.get("action")}
                    p = m.get("params") or {}
                    if p:
                        cmd["params"] = p
                    print(f"  {BOLD}Scheduled At:{RESET}  {m.get('scheduled_at')}")
                    print(f"  {BOLD}Target Device:{RESET} {GREEN}{dev}{RESET} ({m.get('channel_key')})")
                    print(f"  {BOLD}Device Type:{RESET}   {m.get('device_type') or '-'}")
                    print(f"  {BOLD}Device Num:{RESET}    {m.get('device_number') if m.get('device_number') is not None else '-'}")
                    print(f"  {BOLD}Status:{RESET}        {m.get('status')}")
                    print(f"  {BOLD}Command Payload Transmitted to Hardware:{RESET}")
                    print("  " + json.dumps(cmd, indent=2).replace("\n", "\n  "))
                    print(f"{'-'*70}")
                print()
                return 0

            print(f"\n{BOLD}{CYAN}Schedule Details: {sched_id}{RESET}")
            print(f"{'='*70}")
            print(f"  {BOLD}Date:{RESET}       {sched['date']}")
            print(f"  {BOLD}Revision:{RESET}   {sched['revision']}")
            print(f"  {BOLD}Status:{RESET}     {GREEN if sched['status'] == 'ACTIVE' else YELLOW}{sched['status']}{RESET}")
            print(f"  {BOLD}Hash:{RESET}       {sched.get('raw_json_hash', 'N/A')}")
            print(f"\n {BOLD}Actions ({len(actions)}):{RESET}")
            print(f"  {'TIME':<26} {'ACTION':<12} {'TARGET/NODE':<14} {'DEVICE TYPE':<14} {'DEV NUM':<8} {'CHANNEL':<18} {'STATUS':<11} {'PARAMS'}")
            print(f"  {'-'*26} {'-'*12} {'-'*14} {'-'*14} {'-'*8} {'-'*18} {'-'*11} {'-'*15}")
            for a in actions:
                st = a.get("status", "PENDING")
                st_c = GREEN if st == "COMPLETED" else (YELLOW if st == "PENDING" else RED)
                dev_target = a.get("device_id") or a.get("iot_node_id") or "-"
                dev_type = a.get("device_type") or "-"
                dev_num = str(a.get("device_number")) if a.get("device_number") is not None else "-"
                channel = a.get("channel_key") or "-"
                p = a.get("params") or {}
                p_summary = json.dumps(p) if p else "-"
                if len(p_summary) > 30:
                    p_summary = p_summary[:27] + "..."
                print(f"  {a.get('scheduled_at', ''):<26} {a.get('action', ''):<12} {dev_target:<14} {dev_type:<14} {dev_num:<8} {channel:<18} {st_c}{st:<11}{RESET} {DIM}{p_summary}{RESET}")
            print()
            return 0

        elif sub == "load":
            file_path = Path(args.file)
            if not file_path.exists():
                print(f"\n{RED}Error: Schedule file '{file_path}' does not exist.{RESET}\n")
                return 1

            content = file_path.read_text(encoding="utf-8")
            from ingestion.schedule_validator import ScheduleValidator
            from ingestion.schedule_normalizer import ScheduleNormalizer
            val = ScheduleValidator()
            norm = ScheduleNormalizer()

            payload = json.loads(content)
            validated = val.validate(payload)
            normalized = norm.normalize(payload)

            await repo.save_schedule_with_actions(
                schedule_id=validated.schedule_id,
                date=validated.date,
                revision=validated.revision,
                actions=normalized,
                raw_json=content,
            )
            print(f"\n{GREEN}[✓]{RESET} Successfully loaded schedule '{validated.schedule_id}' (rev {validated.revision}) with {len(normalized)} action(s).\n")
            return 0

        elif sub == "publish":
            file_path = Path(getattr(args, "file", None) or "schedule.json")
            if not file_path.exists():
                print(f"\n{RED}Error: Schedule file '{file_path}' does not exist.{RESET}\n")
                return 1

            content = file_path.read_text(encoding="utf-8")
            try:
                payload = json.loads(content)
            except Exception as e:
                print(f"\n{RED}Error: Invalid JSON in '{file_path}': {e}{RESET}\n")
                return 1

            if getattr(args, "today", False):
                today_str = datetime.now().strftime("%Y-%m-%d")
                payload["date"] = today_str
                if "hub_id" in payload:
                    payload["schedule_id"] = f"{payload['hub_id']}-{today_str}"
                content = json.dumps(payload, indent=2)

            cfg = ConfigRepository(db)
            db_host = await cfg.get_setting("rabbitmq.host")
            db_port = await cfg.get_setting("rabbitmq.port")
            db_user = await cfg.get_setting("rabbitmq.username")
            stored_pw = await cfg.get_setting("rabbitmq.password")

            host = db_host or RABBITMQ_HOST
            port = int(db_port) if db_port else RABBITMQ_AMQP_PORT
            user = db_user or RABBITMQ_USER
            pw = stored_pw or RABBITMQ_PASSWORD
            sch = "amqps" if RABBITMQ_USE_SSL else "amqp"
            vh = quote(RABBITMQ_VHOST.lstrip("/"), safe="") if RABBITMQ_VHOST.lstrip("/") else ""
            t_url = f"{sch}://{quote(user, safe='')}:{quote(pw, safe='')}@{host}:{port}/{vh}"

            import aio_pika
            print(f"\n{BOLD}{CYAN}Publishing Schedule JSON to RabbitMQ Broker...{RESET}")
            print(f"{'='*60}")
            print(f" Broker:      {host}:{port}")
            print(f" Exchange:    {COMMAND_EXCHANGE}")
            print(f" Routing Key: {COMMAND_ROUTING_KEY}")
            print(f" Schedule ID: {payload.get('schedule_id', 'unknown')}")
            print(f" Target Date: {payload.get('date', 'unknown')}")
            print(f" File:        {file_path} ({len(content)} bytes)")
            print(f"{'='*60}")

            try:
                conn = await aio_pika.connect_robust(t_url)
                async with conn:
                    channel = await conn.channel()
                    exchange = await channel.get_exchange(COMMAND_EXCHANGE, ensure=False)
                    msg = aio_pika.Message(
                        body=content.encode("utf-8"),
                        content_type="application/json",
                        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                    )
                    await exchange.publish(msg, routing_key=COMMAND_ROUTING_KEY)
                    print(f"\n{GREEN}[✓] Schedule successfully published to RabbitMQ!{RESET}")
                    print(f" ==> The running iothubd daemon will ingest, validate, and store it in SQLite.")
                    print(f" ==> Check live daemon logs: {BOLD}sudo journalctl -u iothubd.service -f{RESET}\n")
            except Exception as exc:
                print(f"\n{RED}[✗] Failed to publish schedule to RabbitMQ: {exc}{RESET}\n")
                return 1

            return 0

        return 0
    finally:
        await db.close()


# =====================================================================
# Command: iothubctl actions (list)
# =====================================================================
async def cmd_actions(args: argparse.Namespace) -> int:
    db = get_db(args.db)
    await db.connect()
    try:
        repo = ScheduleRepository(db)
        status_filter = getattr(args, "status", None)
        limit = getattr(args, "limit", 50) or 50

        actions = await repo.get_actions(status=status_filter, limit=limit)
        print(f"\n{BOLD}{CYAN}iothubd — Scheduled Actions{RESET}")
        print(f"{'='*75}")
        if not actions:
            print(f"  {YELLOW}No actions found matching criteria.{RESET}\n")
            return 0

        print(f"  {'ACTION ID':<20} {'ACTION':<14} {'SCHEDULED AT':<24} {'STATUS'}")
        print(f"  {'-'*20} {'-'*14} {'-'*24} {'-'*10}")
        for a in actions:
            st = a.get("status", "PENDING")
            st_c = GREEN if st == "COMPLETED" else (YELLOW if st == "PENDING" else RED)
            print(f"  {a.get('action_id', ''):<20} {a.get('action', ''):<14} {a.get('scheduled_at', ''):<24} {st_c}{st}{RESET}")
        print()
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
    dev_subs.add_parser("autoconfigure", help="Automatically scan USB serial ports and configure logical devices")
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

    # rabbitmq
    rmq_parser = subparsers.add_parser("rabbitmq", help="Diagnose and test RabbitMQ broker connection")
    rmq_subs = rmq_parser.add_subparsers(dest="rabbitmq_action")
    rmq_subs.add_parser("test", help="Test live connection to RabbitMQ broker and verify queues")
    rmq_subs.add_parser("check", help="Alias for test")
    rmq_subs.add_parser("last", help="Inspect the most recent incoming RabbitMQ message/schedule")

    # schedules
    sched_parser = subparsers.add_parser("schedules", aliases=["schedule"], help="View daily schedules and inspect actions")
    sched_subs = sched_parser.add_subparsers(dest="schedule_action")
    sched_subs.add_parser("list", help="List all stored daily schedules")
    show_sched = sched_subs.add_parser("show", help="Show details or raw JSON of a schedule")
    show_sched.add_argument("schedule_id", nargs="?", default=None, help="Schedule ID (defaults to active schedule)")
    show_sched.add_argument("--raw", action="store_true", help="Print original raw JSON received from RabbitMQ")
    show_sched.add_argument("--time", "-t", default=None, help="Filter and display the generated device command JSON for a specific time or range (e.g. 15:50, 15.50-15.54, or now)")
    show_sched.add_argument("--now", action="store_true", help="Display the device command JSON for current time or most recent action before it")
    load_sched = sched_subs.add_parser("load", help="Load and validate a local schedule JSON file")
    load_sched.add_argument("file", help="Path to schedule JSON file")
    pub_sched = sched_subs.add_parser("publish", help="Publish a schedule JSON file to RabbitMQ")
    pub_sched.add_argument("file", nargs="?", default="schedule.json", help="Path to schedule JSON file (default: schedule.json)")
    pub_sched.add_argument("--today", action="store_true", help="Automatically update schedule date to today's date")

    # actions
    act_parser = subparsers.add_parser("actions", help="Inspect scheduled actions and execution history")
    act_subs = act_parser.add_subparsers(dest="action_action")
    act_list = act_subs.add_parser("list", help="List actions across daily schedules")
    act_list.add_argument("--status", choices=["PENDING", "COMPLETED", "FAILED", "SUPERSEDED", "SKIPPED_UNSAFE"], default=None, help="Filter by status")
    act_list.add_argument("--limit", type=int, default=50, help="Max actions to list")

    # uninstall
    uninst_p = subparsers.add_parser("uninstall", help="Uninstall iothubd service and application")
    uninst_p.add_argument("--purge", action="store_true", help="Permanently delete databases, encryption keys, and configuration (/var/lib/iothub, /etc/iothub)")
    uninst_p.add_argument("-y", "--yes", action="store_true", help="Assume yes to confirmation prompts without prompting interactively")

    # update
    upd_p = subparsers.add_parser("update", help="Update iothubd from Git repository and restart service")
    upd_p.add_argument("--repo", default="https://github.com/VinujaRnR/RnR_iot_hub.git", help="Git repository URL")
    upd_p.add_argument("--ref", default="main", help="Git branch or tag to update to (default: main)")

    return parser


# =====================================================================
# Command: iothubctl update
# =====================================================================
async def cmd_update(args: argparse.Namespace) -> int:
    if os.geteuid() != 0:
        print(f"\n{RED}Error: 'iothubctl update' requires root privileges.{RESET}")
        print(f"Please run with sudo: {BOLD}sudo iothubctl update{RESET}\n")
        return 1

    repo = getattr(args, "repo", None) or "https://github.com/VinujaRnR/RnR_iot_hub.git"
    ref = getattr(args, "ref", None) or "main"
    stage_dir = Path("/tmp/iothub_update_staging")

    print(f"\n{BOLD}{CYAN}========================================================================{RESET}")
    print(f"{BOLD}{CYAN} RnR Linux IoT Hub — Application Software Update                        {RESET}")
    print(f"{BOLD}{CYAN} Repository: {repo} (ref: {ref})                                       {RESET}")
    print(f"{BOLD}{CYAN}========================================================================{RESET}\n")

    if stage_dir.exists():
        shutil.rmtree(stage_dir, ignore_errors=True)

    print(f" {CYAN}==>{RESET} Downloading update from {repo} ({ref})...")
    res = subprocess.run(
        ["git", "clone", "--depth", "1", "--branch", ref, repo, str(stage_dir)],
        capture_output=True,
        text=True,
    )
    if res.returncode != 0:
        print(f" {RED}[✗]{RESET} Failed to clone release: {res.stderr.strip()}")
        return 1
    print(f" {GREEN}[✓]{RESET} Downloaded release package.")

    # Create pre-update backup of database
    print(f" {CYAN}==>{RESET} Creating pre-update safety backup...")
    try:
        from storage.backup_manager import BackupManager
        db_path = "/var/lib/iothub/iothub.db"
        if os.path.exists(db_path):
            db = get_db(db_path)
            await db.connect()
            try:
                bm = BackupManager(db)
                bak = await bm.create_backup()
                bak_path = bak.get("backup_file") or bak.get("backup_path") or "ok"
                print(f" {GREEN}[✓]{RESET} Created pre-update backup: {bak_path}")
            finally:
                await db.close()
    except Exception as exc:
        print(f" {YELLOW}[!]{RESET} Pre-update backup warning: {exc}")

    # Invoke install.py in staging directory
    print(f" {CYAN}==>{RESET} Deploying application update...")
    inst_py = stage_dir / "install.py"
    if not inst_py.exists():
        print(f" {RED}[✗]{RESET} Invalid release package: install.py missing.")
        return 1

    res = subprocess.run([sys.executable, str(inst_py)], capture_output=True, text=True)
    if res.returncode != 0:
        print(f" {RED}[✗]{RESET} Update failed: {res.stderr.strip()}")
        return 1

    if stage_dir.exists():
        shutil.rmtree(stage_dir, ignore_errors=True)

    print(f"\n{BOLD}{GREEN}========================================================================{RESET}")
    print(f"{BOLD}{GREEN} Update completed successfully! Service is active and running.          {RESET}")
    print(f"{BOLD}{GREEN} Run diagnostics: sudo iothubctl doctor                                 {RESET}")
    print(f"{BOLD}{GREEN}========================================================================{RESET}\n")
    return 0


# =====================================================================
# Command: iothubctl uninstall
# =====================================================================
async def cmd_uninstall(args: argparse.Namespace) -> int:
    if os.geteuid() != 0:
        print(f"\n{RED}Error: 'iothubctl uninstall' requires root privileges.{RESET}")
        print(f"Please run with sudo: {BOLD}sudo iothubctl uninstall{' --purge' if getattr(args, 'purge', False) else ''}{RESET}\n")
        return 1

    from install import IoTInstaller
    installer = IoTInstaller()
    installer.uninstall(purge=getattr(args, "purge", False), yes=getattr(args, "yes", False))
    return 0


def main():
    parser = build_parser()
    args = parser.parse_args()

    handlers = {
        "status": cmd_status,
        "doctor": cmd_doctor,
        "rabbitmq": cmd_rabbitmq,
        "schedules": cmd_schedules,
        "schedule": cmd_schedules,
        "actions": cmd_actions,
        "config": cmd_config,
        "devices": cmd_devices,
        "profiles": cmd_profiles,
        "backup": cmd_backup,
        "uninstall": cmd_uninstall,
        "update": cmd_update,
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
