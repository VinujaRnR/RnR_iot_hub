"""
Local Schedule Execution Engine for RnR Linux IoT Hub.
File: scheduler/schedule_engine.py

Implements Step 11 specification:
- Runs continuously using locally stored execution timestamps in SQLite.
- Converts local calendar date and time to absolute UTC timestamps using Python zoneinfo.
- Implements the canonical scheduler loop:
    async def scheduler_loop():
        while running:
            now = current_utc_time()
            due_actions = await database.get_due_actions(now)
            for action in due_actions:
                await dispatcher.queue_action(action)
            await wait_for_next_due_action_or_change()
- Atomic claiming in SQLite prevents duplicate execution across restarts or redundant tasks.
- Integrates with ActionDispatcher for independent per-device command queues.
- Actions execute autonomously at their scheduled times without requiring a new RabbitMQ message for each action.
"""

from __future__ import annotations
import asyncio
from datetime import datetime, timezone
import logging
from typing import Any, Dict, List, Optional
import zoneinfo

from config import SCHEDULE_TIMEZONE
from storage.database import DatabaseManager
from storage.schedule_repository import ScheduleRepository
from scheduler.action_dispatcher import ActionDispatcher

logger = logging.getLogger("iothub.schedule_engine")


def local_to_utc_iso(date_str: str, time_str: str, tz_name: str = SCHEDULE_TIMEZONE) -> str:
    """
    Converts local date ('YYYY-MM-DD') and local time ('HH:MM' or 'HH:MM:SS')
    into an absolute UTC ISO 8601 string using Python zoneinfo.
    """
    try:
        tz = zoneinfo.ZoneInfo(tz_name)
    except Exception:
        tz = timezone.utc

    clean_time = time_str.strip()
    if "T" in clean_time:
        # Already full ISO
        dt = datetime.fromisoformat(clean_time)
        return dt.astimezone(timezone.utc).isoformat()

    parts = clean_time.split(":")
    h = int(parts[0])
    m = int(parts[1]) if len(parts) > 1 else 0
    s = int(parts[2]) if len(parts) > 2 else 0

    d = datetime.strptime(date_str.strip(), "%Y-%m-%d").date()
    local_dt = datetime(d.year, d.month, d.day, h, m, s, tzinfo=tz)
    return local_dt.astimezone(timezone.utc).isoformat()


def current_utc_time() -> str:
    """Returns current absolute UTC timestamp in ISO 8601 format."""
    return datetime.now(timezone.utc).isoformat()


class ScheduleEngine:
    """
    Continuous local schedule execution engine.
    Monitors SQLite for due actions, claims them atomically, and forwards them
    to the ActionDispatcher.
    """

    def __init__(
        self,
        database: DatabaseManager,
        schedule_repo: ScheduleRepository,
        dispatcher: ActionDispatcher,
        poll_interval: float = 1.0,
        max_idle_wait: float = 5.0,
        time_provider: Optional[Any] = None,
        supervisor: Optional[Any] = None,
    ):
        self.database = database
        self.schedule_repo = schedule_repo
        self.dispatcher = dispatcher
        self.poll_interval = poll_interval
        self.max_idle_wait = max_idle_wait
        self.time_provider = time_provider
        self.supervisor = supervisor

        self._running: bool = False
        self._loop_task: Optional[asyncio.Task] = None
        self._watcher_task: Optional[asyncio.Task] = None
        self._wake_event = asyncio.Event()
        self._simulated_time: Optional[str] = None
        self._last_device_states: Dict[str, str] = {}

    @property
    def is_running(self) -> bool:
        return self._running

    def set_simulated_time(self, iso_time: str) -> None:
        """Sets a controllable simulated UTC time and wakes the scheduler loop."""
        self._simulated_time = iso_time
        self.notify_schedule_updated()

    def get_current_utc_time(self) -> str:
        """Returns current UTC ISO timestamp, respecting simulated clock or custom provider."""
        if self._simulated_time is not None:
            return self._simulated_time
        if self.time_provider is not None:
            val = self.time_provider()
            return val if isinstance(val, str) else val.isoformat()
        return current_utc_time()

    async def start(self) -> None:
        """Starts the schedule engine loop, 60s watcher, and action dispatcher."""
        if self._running:
            return

        self._running = True
        self.dispatcher.start()
        self._wake_event.clear()
        self._loop_task = asyncio.create_task(self.scheduler_loop(), name="schedule_engine_loop")
        self._watcher_task = asyncio.create_task(self._schedule_watcher_loop(), name="schedule_watcher_60s")
        logger.info("ScheduleEngine started (Timezone: %s).", SCHEDULE_TIMEZONE)

    async def stop(self) -> None:
        """Gracefully halts the schedule engine and flushes workers."""
        if not self._running:
            return

        self._running = False
        self._wake_event.set()

        if self._watcher_task:
            self._watcher_task.cancel()
            try:
                await self._watcher_task
            except asyncio.CancelledError:
                pass
            self._watcher_task = None

        if self._loop_task:
            self._loop_task.cancel()
            try:
                await self._loop_task
            except asyncio.CancelledError:
                pass
            self._loop_task = None

        await self.dispatcher.stop()
        logger.info("ScheduleEngine stopped gracefully.")

    async def _schedule_watcher_loop(self) -> None:
        """
        Continuous 60-Second Schedule Watcher running natively inside the system daemon:
        - Evaluates schedule every 60s against the local clock (Asia/Kolkata).
        - Inspects the command payload JSON for current time slot.
        - Compares desired state with the previous state.
        - Transmits the command payload JSON to hardware IF AND ONLY IF the action state changed.
        """
        logger.info("ScheduleEngine 60-second automated schedule watcher active.")
        try:
            tz = zoneinfo.ZoneInfo(SCHEDULE_TIMEZONE)
        except Exception:
            tz = timezone.utc

        while self._running:
            try:
                # 0. System Clock Safety Validation
                if self.supervisor and not getattr(self.supervisor, "clock_safe", True):
                    await asyncio.sleep(self.poll_interval)
                    continue

                now_iso = self.get_current_utc_time()
                try:
                    now_dt = datetime.fromisoformat(now_iso).astimezone(tz)
                except Exception:
                    now_dt = datetime.now(tz)
                now_hm = now_dt.strftime("%H:%M")

                schedules = await self.schedule_repo.list_schedules(status="ACTIVE")
                if not schedules:
                    schedules = await self.schedule_repo.list_schedules()

                active_sched_id = schedules[0]["schedule_id"] if schedules else None
                actions = []
                if active_sched_id:
                    actions = await self.schedule_repo.get_scheduled_actions(active_sched_id)

                if actions:
                    past_or_current = []
                    future_actions = []

                    for a in actions:
                        sched_at = a.get("scheduled_at", "")
                        if "T" in sched_at:
                            try:
                                sched_dt = datetime.fromisoformat(sched_at).astimezone(tz)
                                if sched_dt.date() == now_dt.date():
                                    is_past_or_curr = (sched_dt <= now_dt)
                                else:
                                    t_part = sched_at.split("T")[1][:5]
                                    is_past_or_curr = (t_part <= now_hm)

                                if is_past_or_curr:
                                    past_or_current.append(a)
                                else:
                                    future_actions.append(a)
                            except Exception:
                                pass

                    current_action = past_or_current[-1] if past_or_current else None
                    next_action = future_actions[0] if future_actions else None

                    if not current_action:
                        # Schedule has not started yet today
                        wait_dur = 60.0 if self._simulated_time is None else min(0.1, self.poll_interval)
                        await asyncio.sleep(wait_dur)
                        continue

                    dev_key = str(current_action.get("device_id") or current_action.get("iot_node_id") or "light_glow_1")
                    act_name = current_action.get("action", "LIGHT_ON")
                    desired_st = None
                    act_u = act_name.strip().upper()
                    if act_u in ("LIGHTS_ON", "LIGHT_ON", "ON"):
                        desired_st = "ON"
                    elif act_u in ("LIGHTS_OFF", "LIGHT_OFF", "OFF"):
                        desired_st = "OFF"
                    elif act_u in ("VALVE_ON", "VALVE_OPEN", "OPEN"):
                        desired_st = "OPEN"
                    elif act_u in ("VALVE_OFF", "VALVE_CLOSE", "CLOSE"):
                        desired_st = "CLOSED"

                    prev_st = self._last_device_states.get(dev_key)

                    if desired_st and prev_st != desired_st:
                        logger.info(
                            "[SCHEDULE 60s TICK] [%s] State Change: %s -> %s on '%s'. Queueing command JSON for action '%s'.",
                            now_hm,
                            prev_st or "INITIAL",
                            desired_st,
                            dev_key,
                            act_name,
                        )
                        self._last_device_states[dev_key] = desired_st
                        await self.dispatcher.queue_action(current_action)
                    else:
                        next_desc = f"{next_action.get('action')} at {next_action.get('scheduled_at', '').split('T')[1][:5]}" if next_action else "None (End of schedule)"
                        logger.debug(
                            "[SCHEDULE 60s TICK] [%s] Device '%s' already in state '%s'. Action state unchanged. Next: %s",
                            now_hm,
                            dev_key,
                            desired_st,
                            next_desc,
                        )

            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("Error in 60s schedule watcher loop: %s", exc, exc_info=True)

            await asyncio.sleep(60.0)


    def notify_schedule_updated(self) -> None:
        """
        Signals the scheduler loop that a new schedule has been imported,
        waking it up immediately without waiting for the next polling interval.
        """
        self._wake_event.set()

    async def scheduler_loop(self) -> None:
        """
        Canonical scheduling algorithm:
        Continuously polls for due actions in SQLite, claims them atomically,
        enqueues them to per-device dispatcher queues, and sleeps until the next due time.
        """
        logger.info("ScheduleEngine loop entering active scheduling state.")
        while self._running:
            try:
                # 0. System Clock Safety Validation (Blocks timed execution if clock is unsafe)
                if self.supervisor and not getattr(self.supervisor, "clock_safe", True):
                    logger.warning("System clock is unsafe/unsynchronized. Timed execution is blocked.")
                    await asyncio.sleep(self.poll_interval)
                    continue

                now = self.get_current_utc_time()

                # 1. Atomically claim due actions in SQLite (prevents race conditions)
                due_actions = await self.schedule_repo.claim_due_actions(now)

                # 2. Queue claimed actions to per-device dispatcher on state change
                if due_actions:
                    logger.info("Claimed %d due action(s) at UTC %s", len(due_actions), now)
                    for action in due_actions:
                        dev_key = str(action.get("device_id") or action.get("iot_node_id") or "default")
                        act_name = action.get("action", "").strip().upper()
                        
                        desired_st = None
                        if act_name in ("LIGHTS_ON", "LIGHT_ON", "ON"):
                            desired_st = "ON"
                        elif act_name in ("LIGHTS_OFF", "LIGHT_OFF", "OFF"):
                            desired_st = "OFF"
                        elif act_name in ("VALVE_ON", "VALVE_OPEN", "OPEN"):
                            desired_st = "OPEN"
                        elif act_name in ("VALVE_OFF", "VALVE_CLOSE", "CLOSE"):
                            desired_st = "CLOSED"

                        prev_st = self._last_device_states.get(dev_key)
                        if desired_st and prev_st == desired_st:
                            logger.info(
                                "[DEDUPLICATION] Action '%s' for '%s' matches current state '%s'. Suppressing redundant hardware actuation.",
                                act_name,
                                dev_key,
                                desired_st,
                            )
                            continue

                        if desired_st:
                            self._last_device_states[dev_key] = desired_st

                        await self.dispatcher.queue_action(action)

                # 3. Wait until next action is due or until schedule changes
                await self.wait_for_next_due_action_or_change()

            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("Error in scheduler loop: %s", exc, exc_info=True)
                await asyncio.sleep(self.poll_interval)

    async def wait_for_next_due_action_or_change(self) -> None:
        """
        Calculates the sleep duration until the next pending action is due,
        or wakes up immediately if a new schedule arrives via notify_schedule_updated().
        """
        wait_seconds = self.max_idle_wait

        try:
            next_due_iso = await self.schedule_repo.get_next_due_timestamp()
            if next_due_iso:
                next_dt = datetime.fromisoformat(next_due_iso).astimezone(timezone.utc)
                now_dt = datetime.fromisoformat(self.get_current_utc_time()).astimezone(timezone.utc)
                diff = (next_dt - now_dt).total_seconds()
                if diff > 0:
                    wait_seconds = min(diff, self.max_idle_wait)
                else:
                    # An action is due immediately or overdue
                    wait_seconds = 0.05
        except Exception as exc:
            logger.debug("Could not determine next due timestamp: %s", exc)

        try:
            # Wait on event or timeout
            await asyncio.wait_for(self._wake_event.wait(), timeout=max(0.01, wait_seconds))
            self._wake_event.clear()
        except asyncio.TimeoutError:
            pass
