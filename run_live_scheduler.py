#!/usr/bin/env python3
"""
Live Schedule Observer for RnR Linux IoT Hub.
File: run_live_scheduler.py

Loads schedule.json and monitors execution minute-by-minute in real-time,
toggling the physical Arduino Nano LED.
"""

import asyncio
import json
import logging
from datetime import datetime
import zoneinfo

from config import HUB_ID, SCHEDULE_TIMEZONE
from devices.device_manager import create_default_device_manager
from ingestion.schedule_normalizer import ScheduleNormalizer
from scheduler.scheduler import TaskScheduler

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("iothub.live")


async def main():
    print("\n========================================================")
    print("      RnR IOT HUB: LIVE SCHEDULE REAL-TIME OBSERVER     ")
    print("========================================================")

    # 1. Load and normalize schedule.json
    with open("schedule.json", "r", encoding="utf-8") as f:
        data = json.load(f)

    normalizer = ScheduleNormalizer(default_hub_id=HUB_ID, default_timezone=SCHEDULE_TIMEZONE)
    records = normalizer.normalize(data)

    print(f"Loaded {len(records)} scheduled actions for {data.get('date')} ({SCHEDULE_TIMEZONE}):")
    for r in records:
        print(f"  [{r.scheduled_at.split('T')[1][:5]}] -> Device: {r.iot_node_id} | Action: {r.action}")

    # 2. Setup Device Manager & Hardware
    device_manager = create_default_device_manager()

    async def on_dispatch(dev_id: str, action: str, params: dict):
        print(f"\n>>> [LIVE TRIGGER] Time: {datetime.now().strftime('%H:%M:%S')} | Device: {dev_id} | Action: {action}")
        res = await device_manager.dispatch(dev_id, action, params)
        print(f"<<< [HARDWARE RESULT]: {res}")
        if action == "LIGHTS_ON":
            print("💡 Physical LED is now ON!\n")
        elif action == "LIGHTS_OFF":
            print("🌑 Physical LED is now OFF!\n")
        return res

    # 3. Start Scheduler
    scheduler = TaskScheduler(dispatch_callback=on_dispatch)
    scheduler.load_normalized_schedule(records)
    await scheduler.start()

    print("\n--------------------------------------------------------")
    print("Scheduler is RUNNING! Watching clock for next actions...")
    print("Press Ctrl+C at any time to exit.")
    print("--------------------------------------------------------\n")

    tz = zoneinfo.ZoneInfo(SCHEDULE_TIMEZONE)
    try:
        while True:
            now = datetime.now(tz)
            print(f"\rCurrent Time: {now.strftime('%H:%M:%S')} | Awaiting scheduled actions...", end="", flush=True)
            await asyncio.sleep(1)
    except (KeyboardInterrupt, asyncio.CancelledError):
        print("\nStopping live scheduler...")
    finally:
        await scheduler.stop()
        dev = device_manager.get_device("/dev/ttyUSB0")
        if dev and hasattr(dev, "close"):
            dev.close()
        print("Shutdown complete.")


if __name__ == "__main__":
    asyncio.run(main())
