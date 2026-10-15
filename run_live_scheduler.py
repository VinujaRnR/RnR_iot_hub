#!/usr/bin/env python3
"""
Live Schedule Real-Time Watcher for RnR Linux IoT Hub.
File: run_live_scheduler.py

Checks the schedule every 60 seconds:
- Evaluates the scheduled action for the current time slot.
- Inspects the command payload JSON (e.g. {"action": "LIGHT_ON"}).
- Compares desired state with the previous state.
- Executes and transmits the command JSON to the hardware device IF AND ONLY IF the state has changed.
- Displays the upcoming next command JSON and remaining countdown.
"""

from __future__ import annotations
import asyncio
from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path
import sys
from typing import Any, Dict, Optional
import urllib.error
import urllib.request
import zoneinfo

from config import HUB_ID, SCHEDULE_TIMEZONE, DATABASE_PATH
from storage.database import DatabaseManager
from storage.schedule_repository import ScheduleRepository

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("iothub.live_watcher")

# Styling
GREEN = "\033[92m"
YELLOW = "\033[93m"
CYAN = "\033[96m"
RED = "\033[91m"
BOLD = "\033[1m"
DIM = "\033[2m"
RESET = "\033[0m"


def action_to_state(act: str) -> str:
    act_u = act.strip().upper()
    if act_u in ("LIGHTS_ON", "LIGHT_ON", "ON"):
        return "ON"
    if act_u in ("LIGHTS_OFF", "LIGHT_OFF", "OFF"):
        return "OFF"
    if act_u in ("VALVE_ON", "VALVE_OPEN", "OPEN"):
        return "OPEN"
    if act_u in ("VALVE_OFF", "VALVE_CLOSE", "CLOSE"):
        return "CLOSED"
    return act_u


async def transmit_command(target_device: str, action_name: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Transmits command JSON payload to target device via local API or DeviceManager."""
    cmd_payload: Dict[str, Any] = {"action": action_name}
    if params:
        cmd_payload["params"] = params

    # 1. Try local Management API first (safe sharing with active iothubd daemon)
    api_url = f"http://127.0.0.1:8765/devices/{target_device}/command"
    try:
        req_data = json.dumps(cmd_payload).encode("utf-8")
        req = urllib.request.Request(
            api_url,
            data=req_data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=3.0) as resp:
            resp_json = json.loads(resp.read().decode("utf-8"))
            return resp_json.get("result", {})
    except Exception:
        pass

    # 2. Fallback to direct DeviceManager
    from devices.device_manager import create_default_device_manager
    mgr = create_default_device_manager()
    dev = mgr.get_device(target_device)
    if not dev:
        raise ValueError(f"Device '{target_device}' not found in registry.")
    return await mgr.dispatch(target_device, action_name, {"params": params} if params else {})


async def main():
    print(f"\n{BOLD}{CYAN}======================================================================{RESET}")
    print(f"{BOLD}{CYAN}  RnR IoT Hub: Automated 60-Second Schedule Watcher & State Tracker   {RESET}")
    print(f"{BOLD}{CYAN}======================================================================{RESET}")
    print(f"  {BOLD}Checking Interval:{RESET} Every 60s")
    print(f"  {BOLD}Timezone:{RESET}          {SCHEDULE_TIMEZONE}")
    print(f"  {DIM}Press Ctrl+C to stop watching.{RESET}\n")

    # Connect to SQLite database if available
    db = DatabaseManager(db_path=DATABASE_PATH if os.path.exists(DATABASE_PATH) else "iothub.db")
    repo = None
    try:
        await db.connect()
        repo = ScheduleRepository(db)
    except Exception as exc:
        logger.debug("Database connection warning: %s (will use schedule.json directly)", exc)

    tz = zoneinfo.ZoneInfo(SCHEDULE_TIMEZONE)
    last_known_states: Dict[str, str] = {}

    try:
        while True:
            now_dt = datetime.now(tz)
            now_hm = now_dt.strftime("%H:%M")
            now_sec = now_dt.strftime("%H:%M:%S")

            # 1. Retrieve actions from active schedule or fallback to schedule.json
            actions = []
            active_sched_id = None

            if repo:
                try:
                    schedules = await repo.list_schedules(status="ACTIVE")
                    if not schedules:
                        schedules = await repo.list_schedules()
                    if schedules:
                        active_sched_id = schedules[0]["schedule_id"]
                        actions = await repo.get_scheduled_actions(active_sched_id)
                except Exception as e:
                    logger.debug("Error querying SQLite: %s", e)

            if not actions and Path("schedule.json").exists():
                try:
                    active_sched_id = "schedule.json"
                    raw = json.loads(Path("schedule.json").read_text(encoding="utf-8"))
                    from ingestion.schedule_normalizer import ScheduleNormalizer
                    norm = ScheduleNormalizer()
                    actions = [a.to_dict() for a in norm.normalize(raw)]
                except Exception as e:
                    logger.debug("Error parsing schedule.json: %s", e)

            if not actions:
                print(f"[{now_sec}] {YELLOW}No schedule available. Retrying in 60s...{RESET}")
                await asyncio.sleep(60)
                continue

            # 2. Segregate actions by time (past/current vs future)
            past_or_current = []
            future_actions = []

            for a in actions:
                sched_at = a.get("scheduled_at", "")
                if "T" in sched_at:
                    t_part = sched_at.split("T")[1][:5]
                    if t_part <= now_hm:
                        past_or_current.append(a)
                    else:
                        future_actions.append(a)

            current_action = past_or_current[-1] if past_or_current else actions[0]
            next_action = future_actions[0] if future_actions else None

            dev_key = current_action.get("device_id") or current_action.get("iot_node_id") or "light_glow_1"
            action_name = current_action.get("action", "LIGHT_ON")
            desired_state = action_to_state(action_name)
            prev_state = last_known_states.get(dev_key)

            next_info = "None (End of schedule)"
            if next_action:
                next_time = next_action.get("scheduled_at", "").split("T")[1][:5]
                next_act = next_action.get("action")
                next_info = f"{next_act} at {next_time}"

            print(f"[{BOLD}{now_sec}{RESET}] Schedule: {BOLD}{active_sched_id}{RESET}")
            print(f"  Target Device: {GREEN}{dev_key}{RESET} | Current Time: {BOLD}{now_hm}{RESET} | Desired Action: {BOLD}{action_name}{RESET} (State: {desired_state})")

            # 3. Check if Action State CHANGED!
            if prev_state != desired_state:
                print(f"  {YELLOW}==> [STATE CHANGE DETECTED]{RESET} {prev_state or 'INITIAL/UNKNOWN'} -> {BOLD}{GREEN}{desired_state}{RESET}")
                p = current_action.get("params") or {}
                cmd_payload = {"action": action_name}
                if p:
                    cmd_payload["params"] = p

                print(f"  {BOLD}Transmitting Command Payload JSON to Hardware:{RESET}")
                print("  " + json.dumps(cmd_payload, indent=2).replace("\n", "\n  "))

                try:
                    res = await transmit_command(dev_key, action_name, p)
                    last_known_states[dev_key] = desired_state
                    print(f"  {GREEN}[✓] Command Executed on Hardware! State is now: {desired_state}{RESET}")
                    if res.get("port"):
                        print(f"      Hardware Port: {res.get('port')} | Response: {res.get('state', 'OK')}")
                except Exception as exc:
                    print(f"  {RED}[✗] Hardware transmission error: {exc}{RESET}")
            else:
                print(f"  {DIM}[STATE UNCHANGED] Device '{dev_key}' is already in state '{desired_state}'. Skipping redundant transmission.{RESET}")

            print(f"  Next Scheduled Action: {CYAN}{next_info}{RESET}")
            print(f"  {DIM}Waiting 60s for next schedule check...{RESET}\n")

            await asyncio.sleep(60)

    except (KeyboardInterrupt, asyncio.CancelledError):
        print(f"\n{YELLOW}Schedule watch stopped by user.{RESET}\n")
    finally:
        if db:
            await db.close()


if __name__ == "__main__":
    asyncio.run(main())
