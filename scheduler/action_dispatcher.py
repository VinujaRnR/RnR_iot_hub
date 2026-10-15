"""
Action Dispatcher with Independent Per-Device Command Queues.
File: scheduler/action_dispatcher.py

Implements Step 11 specification:
- Dedicated FIFO queue for each physical/logical IoT device.
- Concurrency isolation: A long-running command on device 98a316e6b658
  never blocks independent actions targeted at other IoT devices.
- Automatic worker lifecycle management.
- Execution recording in SQLite and Outbox event publishing.
"""

from __future__ import annotations
import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Set

from devices.device_manager import DeviceManager
from storage.schedule_repository import ScheduleRepository
from storage.event_outbox import EventOutbox
from config import HUB_ID, ACTION_RESULT_KEY

logger = logging.getLogger("iothub.action_dispatcher")


class ActionDispatcher:
    """
    Maintains independent, concurrent per-device execution queues.
    Guarantees that commands for one device (e.g. 98a316e6b658) do not block
    commands for other devices.
    """

    def __init__(
        self,
        device_manager: DeviceManager,
        schedule_repo: ScheduleRepository,
        outbox: Optional[EventOutbox] = None,
        publisher: Optional[Any] = None,
        max_retries: int = 3,
        supervisor: Optional[Any] = None,
    ):
        self.device_manager = device_manager
        self.schedule_repo = schedule_repo
        self.outbox = outbox
        self.publisher = publisher
        self.max_retries = max_retries
        self.supervisor = supervisor

        # Per-device queues and worker tasks
        self._queues: Dict[str, asyncio.Queue[Dict[str, Any]]] = {}
        self._workers: Dict[str, asyncio.Task] = {}
        self._running: bool = False
        self._lock = asyncio.Lock()

    @property
    def is_running(self) -> bool:
        return self._running

    def start(self) -> None:
        """Enables the dispatcher to accept and execute queued actions."""
        self._running = True
        logger.info("ActionDispatcher started with per-device concurrency.")

    async def stop(self) -> None:
        """Gracefully halts all per-device worker loops."""
        self._running = False
        # Cancel active workers
        for dev_id, worker in list(self._workers.items()):
            if not worker.done():
                worker.cancel()
        # Wait for workers to finish
        if self._workers:
            await asyncio.gather(*self._workers.values(), return_exceptions=True)
        self._workers.clear()
        logger.info("ActionDispatcher stopped.")

    def get_queue_size(self, device_id: str) -> int:
        """Returns pending queue length for a given device."""
        q = self._queues.get(device_id)
        return q.qsize() if q else 0

    def active_devices(self) -> Set[str]:
        """Returns set of devices currently having queues or active workers."""
        return set(self._queues.keys())

    async def queue_action(self, action: Dict[str, Any]) -> None:
        """
        Enqueues an action into its target device's independent command queue.
        Starts a dedicated device worker task if one is not already running.
        """
        params = action.get("params") or action.get("parameters") or {}
        device_id = None
        if isinstance(params, dict):
            device_id = params.get("device")
        if not device_id:
            device_id = str(action.get("iot_node_id") or action.get("device_id") or "default")
        device_id = str(device_id)

        async with self._lock:
            if device_id not in self._queues:
                self._queues[device_id] = asyncio.Queue()

            # Ensure worker is running for this device
            worker = self._workers.get(device_id)
            if worker is None or worker.done():
                self._workers[device_id] = asyncio.create_task(
                    self._device_worker(device_id),
                    name=f"worker-{device_id}",
                )

        await self._queues[device_id].put(action)
        logger.debug(
            "Queued action '%s' for device '%s' (Queue size: %d)",
            action.get("action_id"),
            device_id,
            self._queues[device_id].qsize(),
        )

    async def _device_worker(self, device_id: str) -> None:
        """
        Dedicated worker loop processing commands strictly for one device in FIFO order.
        Runs concurrently alongside workers for all other devices.
        """
        queue = self._queues[device_id]
        logger.debug("Device worker loop started for '%s'", device_id)

        try:
            while self._running:
                try:
                    # Wait for next action with a timeout to allow graceful exit
                    action = await asyncio.wait_for(queue.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue

                try:
                    await self._execute_action(device_id, action)
                except Exception as exc:
                    logger.error("Unexpected error in worker for '%s': %s", device_id, exc, exc_info=True)
                finally:
                    queue.task_done()

        except asyncio.CancelledError:
            pass
        finally:
            logger.debug("Device worker loop ended for '%s'", device_id)

    async def _execute_action(self, device_id: str, action: Dict[str, Any]) -> Dict[str, Any]:
        """
        Executes a claimed action against the physical/logical device and records outcomes.
        """
        schedule_id = action.get("schedule_id")
        action_id = action.get("action_id")
        action_name = action.get("action", "")
        params = action.get("params") or action.get("parameters") or {}
        channel_key = action.get("channel_key", "relay:light")

        target_name = None
        if isinstance(params, dict):
            target_name = params.get("device")
        if not target_name:
            target_name = device_id

        params_to_dispatch = dict(params) if isinstance(params, dict) else {}
        if "target_device" not in params_to_dispatch:
            params_to_dispatch["target_device"] = target_name

        logger.info(
            ">>> [DISPATCH] Executing '%s' on device '%s' (target: %s, Action ID: %s)",
            action_name,
            device_id,
            target_name,
            action_id,
        )

        if self.supervisor and hasattr(self.supervisor, "is_device_quarantined") and self.supervisor.is_device_quarantined(device_id):
            logger.warning("[DISPATCH BLOCKED] Device '%s' is quarantined! Suppressing '%s'", device_id, action_name)
            return {"status": "QUARANTINED", "error": f"Device {device_id} is quarantined due to repeated failures."}

        attempt = 1
        last_error: Optional[str] = None
        result: Optional[Dict[str, Any]] = None
        success = False

        while attempt <= self.max_retries and not success:
            try:
                # Dispatch command via DeviceManager
                result = await self.device_manager.dispatch(device_id, action_name, params_to_dispatch)
                success = True
                logger.info(
                    "<<< [SUCCESS] Device '%s' responded to '%s': %s",
                    device_id,
                    action_name,
                    result,
                )
            except Exception as exc:
                last_error = str(exc)
                logger.warning(
                    "[RETRY %d/%d] Execution failed for '%s' on '%s': %s",
                    attempt,
                    self.max_retries,
                    action_name,
                    device_id,
                    exc,
                )
                attempt += 1
                if attempt <= self.max_retries:
                    await asyncio.sleep(0.5 * attempt)

        exec_status = "SUCCESS" if success else "FAILED"
        scheduled_at = action.get("scheduled_at")
        executed_at = datetime.now(timezone.utc).isoformat()

        # Notify supervisor of outcome for quarantine tracking
        if self.supervisor and hasattr(self.supervisor, "record_device_command_outcome"):
            self.supervisor.record_device_command_outcome(device_id, success, last_error)

        # Step 14: Store the result and outgoing event in ONE database transaction
        if self.outbox:
            try:
                exec_id, outbox_id, event_payload = await self.outbox.record_execution_and_event(
                    schedule_id=schedule_id,
                    action_id=action_id or "",
                    device_id=device_id,
                    action=action_name,
                    status=exec_status,
                    scheduled_at=scheduled_at,
                    executed_at=executed_at,
                    attempt=attempt if success else self.max_retries,
                    result=result,
                    error_message=last_error if not success else None,
                    routing_key=f"hub.{HUB_ID}.action.result",
                )
                if self.publisher and hasattr(self.publisher, "trigger_flush"):
                    self.publisher.trigger_flush()
            except Exception as outbox_err:
                logger.error("Transactional outbox recording failed: %s", outbox_err)
                exec_id = await self.schedule_repo.record_execution(
                    schedule_id=schedule_id,
                    action_id=action_id,
                    device_id=device_id,
                    command_action=action_name,
                    attempt=attempt if success else self.max_retries,
                    status=exec_status,
                    result=result,
                    error_message=last_error if not success else None,
                )
        else:
            exec_id = await self.schedule_repo.record_execution(
                schedule_id=schedule_id,
                action_id=action_id,
                device_id=device_id,
                command_action=action_name,
                attempt=attempt if success else self.max_retries,
                status=exec_status,
                result=result,
                error_message=last_error if not success else None,
            )

        return result or {"status": exec_status, "error": last_error}
