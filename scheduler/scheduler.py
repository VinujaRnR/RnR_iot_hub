import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Coroutine, Dict, List, Optional

import zoneinfo
from config import SCHEDULE_TIMEZONE

logger = logging.getLogger("iothub.scheduler")


@dataclass
class ScheduledAction:
    task_id: str
    device_id: str
    action: str
    parameters: Dict[str, Any] = field(default_factory=dict)
    interval_seconds: Optional[int] = None
    daily_time: Optional[str] = None  # Format "HH:MM" in 24h
    last_executed: Optional[datetime] = None
    retry_count: int = 0
    max_retries: int = 3


class TaskScheduler:
    """Manages timed execution, periodic polling, action dispatch, and automatic recovery."""

    def __init__(self, dispatch_callback: Optional[Callable[[str, str, Dict[str, Any]], Coroutine]] = None):
        self.dispatch_callback = dispatch_callback
        self.tasks: Dict[str, ScheduledAction] = {}
        self._running = False
        self._task_handle: Optional[asyncio.Task] = None

    def add_task(self, task: ScheduledAction) -> None:
        """Registers a scheduled action."""
        self.tasks[task.task_id] = task
        logger.info("Registered scheduled action '%s' for device '%s' (action=%s)", task.task_id, task.device_id, task.action)

    def remove_task(self, task_id: str) -> None:
        """Removes a scheduled action."""
        self.tasks.pop(task_id, None)

    async def _execute_task(self, task: ScheduledAction) -> None:
        """Dispatches an action with recovery on failure."""
        try:
            tz = zoneinfo.ZoneInfo(SCHEDULE_TIMEZONE)
        except Exception:
            tz = timezone.utc
        now = datetime.now(tz)
        logger.info("Executing scheduled task '%s' (device=%s, action=%s)", task.task_id, task.device_id, task.action)
        try:
            if self.dispatch_callback:
                await self.dispatch_callback(task.device_id, task.action, task.parameters)
            task.last_executed = now
            task.retry_count = 0
        except Exception as exc:
            task.retry_count += 1
            logger.error(
                "Task '%s' failed (attempt %d/%d): %s",
                task.task_id,
                task.retry_count,
                task.max_retries,
                exc,
            )
            if task.retry_count < task.max_retries:
                # Schedule immediate retry in 5s
                asyncio.create_task(self._delayed_retry(task, delay=5))

    async def _delayed_retry(self, task: ScheduledAction, delay: int = 5) -> None:
        await asyncio.sleep(delay)
        await self._execute_task(task)

    async def _scheduler_loop(self) -> None:
        """Checks for due tasks periodically."""
        try:
            tz = zoneinfo.ZoneInfo(SCHEDULE_TIMEZONE)
        except Exception:
            tz = timezone.utc

        while self._running:
            now = datetime.now(tz)
            current_hh_mm = now.strftime("%H:%M")

            for task in list(self.tasks.values()):
                due = False

                # Check interval
                if task.interval_seconds:
                    if task.last_executed is None:
                        due = True
                    else:
                        elapsed = (now - task.last_executed).total_seconds()
                        if elapsed >= task.interval_seconds:
                            due = True

                # Check daily time
                elif task.daily_time:
                    if task.daily_time == current_hh_mm:
                        if task.last_executed is None or task.last_executed.date() < now.date():
                            due = True

                if due:
                    asyncio.create_task(self._execute_task(task))

            await asyncio.sleep(1)

    async def start(self) -> None:
        """Starts background scheduler."""
        if not self._running:
            self._running = True
            self._task_handle = asyncio.create_task(self._scheduler_loop())
            logger.info("TaskScheduler started.")

    async def stop(self) -> None:
        """Stops background scheduler."""
        self._running = False
        if self._task_handle:
            self._task_handle.cancel()
            try:
                await self._task_handle
            except asyncio.CancelledError:
                pass
            self._task_handle = None
        logger.info("TaskScheduler stopped.")

    def load_normalized_schedule(self, records: List[Any]) -> int:
        """
        Ingests flat normalized action records directly into the execution queue.
        Bypasses traversing the original nested production-order JSON.
        """
        loaded = 0
        for r in records:
            task_id = f"{getattr(r, 'schedule_id', 'sched')}_{getattr(r, 'action_id', loaded)}"
            raw_sched = getattr(r, "scheduled_at", "")
            # Extract HH:MM
            daily_hh_mm = None
            if "T" in raw_sched:
                time_part = raw_sched.split("T")[1]
                daily_hh_mm = time_part[:5]

            self.add_task(
                ScheduledAction(
                    task_id=task_id,
                    device_id=getattr(r, "iot_node_id", "unknown"),
                    action=getattr(r, "action", "NOOP"),
                    parameters=getattr(r, "params", {}),
                    daily_time=daily_hh_mm,
                )
            )
            loaded += 1
        logger.info("Loaded %d normalized actions into TaskScheduler queue", loaded)
        return loaded

