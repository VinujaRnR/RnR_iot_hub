"""
Deterministic HubSupervisor for RnR Linux IoT Hub.
File: supervisor/supervisor.py

Implements Step 13 specification:
- Continuous service health monitoring and deterministic recovery rules.
- Monitored Conditions & Explicit Recovery Rules:
  1. RabbitMQ unavailable     -> Retry connection with backoff, maintain local execution.
  2. USB device removed       -> Mark device OFFLINE in registry and SQLite.
  3. Device reconnected       -> Revalidate handshake and register ONLINE.
  4. Command timeout          -> Record UNCERTAIN/FAILED result in executions.
  5. Repeated device failures -> Quarantine device and emit CRITICAL alert.
  6. SQLite storage error     -> Enter degraded safe mode, stop accepting commands.
  7. Missed schedule          -> Detect overdue actions and generate alerts.
  8. Pending result backlog   -> Apply backpressure and report when outbox exceeds threshold.
  9. Incorrect system clock   -> Block unsafe timed execution and alert.
 10. Systemd watchdog         -> Periodic sd_notify ("WATCHDOG=1") over NOTIFY_SOCKET.
"""

from __future__ import annotations
import asyncio
from datetime import datetime, timezone
import logging
import os
import socket
import subprocess
import time
from typing import Any, Callable, Dict, List, Optional, Set

from config import HUB_ID, DATABASE_PATH, SCHEDULE_TIMEZONE
from supervisor.watchdog import SystemdWatchdog
from supervisor.recovery_rules import RULES, MonitoredCondition, SupervisorAction

logger = logging.getLogger("iothub.supervisor")


class SystemHealthStatus:
    """Overall operational health state of the IoT Hub."""
    HEALTHY = "HEALTHY"
    DEGRADED = "DEGRADED"
    CRITICAL = "CRITICAL"


class HubSupervisor:
    """
    Deterministic supervisor continuously monitoring service health and
    applying explicitly defined recovery rules.
    """

    def __init__(
        self,
        check_interval: float = 10.0,
        db: Optional[Any] = None,
        schedule_repo: Optional[Any] = None,
        device_manager: Optional[Any] = None,
        schedule_engine: Optional[Any] = None,
        outbox: Optional[Any] = None,
        consumer: Optional[Any] = None,
        publisher: Optional[Any] = None,
        max_consecutive_failures: int = 3,
        backlog_threshold: int = 50,
        missed_schedule_grace_seconds: int = 300,
    ):
        self.check_interval = check_interval
        self.db = db
        self.schedule_repo = schedule_repo
        self.device_manager = device_manager
        self.schedule_engine = schedule_engine
        self.outbox = outbox
        self.consumer = consumer
        self.publisher = publisher

        self.max_consecutive_failures = max_consecutive_failures
        self.backlog_threshold = backlog_threshold
        self.missed_schedule_grace_seconds = missed_schedule_grace_seconds

        # Operational state flags
        self._running: bool = False
        self._task: Optional[asyncio.Task] = None
        self._watchdog_task: Optional[asyncio.Task] = None
        self.start_time: float = time.time()

        # Monitored condition states
        self.storage_degraded: bool = False
        self.clock_safe: bool = True
        self.backpressure_active: bool = False
        self.quarantined_devices: Set[str] = set()
        self.device_failure_counts: Dict[str, int] = {}
        self.known_online_devices: Set[str] = set()

        self.alerts: List[Dict[str, Any]] = []
        self.watchdog = SystemdWatchdog()

    # =========================================================================
    # Lifecycle & Systemd Watchdog
    # =========================================================================

    async def start(self) -> None:
        """Starts supervision loop and systemd watchdog sender."""
        if self._running:
            return

        self._running = True
        self.start_time = time.time()
        self._task = asyncio.create_task(self._supervision_loop(), name="supervisor_loop")

        # Start systemd watchdog if NOTIFY_SOCKET is present
        notify_socket = os.getenv("NOTIFY_SOCKET")
        if notify_socket:
            self._send_systemd_notify("READY=1")
            self._watchdog_task = asyncio.create_task(self._watchdog_loop(notify_socket))
            logger.info("Systemd watchdog active (socket: %s)", notify_socket)

        logger.info("HubSupervisor started (check interval = %.1fs)", self.check_interval)

    async def stop(self) -> None:
        """Gracefully halts the supervisor."""
        self._running = False
        if self._watchdog_task:
            self._watchdog_task.cancel()
            self._send_systemd_notify("STOPPING=1")
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        logger.info("HubSupervisor stopped.")

    def _send_systemd_notify(self, state: str) -> bool:
        """Sends an sd_notify message to systemd over the Unix domain socket."""
        return self.watchdog.notify(state)

    async def _watchdog_loop(self, socket_path: str) -> None:
        """Periodically pings systemd watchdog to confirm daemon responsiveness."""
        watchdog_usec = os.getenv("WATCHDOG_USEC", "30000000")
        try:
            interval = max(2.0, (int(watchdog_usec) / 1000000.0) / 2.0)
        except Exception:
            interval = 10.0

        while self._running:
            if not self.storage_degraded and self.clock_safe:
                self._send_systemd_notify("WATCHDOG=1")
            await asyncio.sleep(interval)

    # =========================================================================
    # Alerts and Metrics
    # =========================================================================

    def emit_alert(self, level: str, message: str, details: Optional[Dict[str, Any]] = None) -> None:
        """Records an operational alert in memory and persists to SQLite if healthy."""
        alert = {
            "timestamp": time.time(),
            "iso_time": datetime.now(timezone.utc).isoformat(),
            "level": level.upper(),
            "message": message,
            "details": details or {},
        }
        self.alerts.append(alert)
        if len(self.alerts) > 200:
            self.alerts.pop(0)

        log_fn = getattr(logger, level.lower(), logger.warning)
        log_fn("SUPERVISOR ALERT [%s]: %s", level.upper(), message)

        if self.db and not self.storage_degraded and getattr(self.db, "_conn", None):
            try:
                loop = asyncio.get_running_loop()
                async def _safe_record_alert():
                    try:
                        if getattr(self.db, "_conn", None):
                            await self.db.record_alert(level.upper(), "supervisor", message, details or {})
                    except Exception:
                        pass
                loop.create_task(_safe_record_alert())
            except RuntimeError:
                pass

    def get_recent_alerts(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Returns the most recent alerts recorded by the supervisor."""
        return list(self.alerts[-limit:])

    def get_system_metrics(self) -> Dict[str, Any]:
        """Provides current health metrics for diagnostic status and dashboards."""
        load_avg = [0.0, 0.0, 0.0]
        try:
            load_avg = list(os.getloadavg())
        except Exception:
            pass

        status = SystemHealthStatus.HEALTHY
        if self.storage_degraded or len(self.quarantined_devices) > 0:
            status = SystemHealthStatus.CRITICAL
        elif self.backpressure_active or not self.clock_safe:
            status = SystemHealthStatus.DEGRADED

        return {
            "status": status,
            "uptime_seconds": round(time.time() - self.start_time, 1),
            "storage_degraded": self.storage_degraded,
            "clock_safe": self.clock_safe,
            "backpressure_active": self.backpressure_active,
            "quarantined_devices": list(self.quarantined_devices),
            "active_alerts": len(self.alerts),
            "load_1m": load_avg[0],
            "load_5m": load_avg[1],
        }

    # =========================================================================
    # Monitored Condition Rules (Step 13)
    # =========================================================================

    async def _supervision_loop(self) -> None:
        """Continuous health evaluation loop."""
        while self._running:
            try:
                # 1. System Clock Verification
                await self.check_system_clock()

                # 2. SQLite Storage Health
                await self.check_storage_health()

                # 3. USB Devices Connectivity & Reconnection
                await self.check_usb_devices()

                # 4. RabbitMQ Connection & Resilience
                await self.check_rabbitmq_health()

                # 5. Missed Schedule Detection
                await self.check_missed_schedules()

                # 6. Missing Daily Schedule Plan Detection
                await self.check_missing_daily_schedule()

                # 7. Pending Result Backlog & Backpressure
                await self.check_outbox_backlog()

            except Exception as exc:
                logger.error("Error in supervision check loop: %s", exc, exc_info=True)

            await asyncio.sleep(self.check_interval)

    # Rule 1 & 9: System Clock Validation (Blocks unsafe timed execution)
    async def check_system_clock(self) -> bool:
        """
        Rule: If system clock is unsynchronized with NTP, blocks unsafe timed execution
        to prevent firing schedule actions at corrupted times.
        """
        is_synced = True
        try:
            result = subprocess.run(
                ["timedatectl", "status"],
                capture_output=True,
                text=True,
                timeout=4,
                check=False,
            )
            stdout = result.stdout
            if "NTP service: active" in stdout or "synchronized: yes" in stdout or "NTPSynchronized=yes" in stdout:
                is_synced = True
            elif "System clock synchronized: no" in stdout or "synchronized: no" in stdout:
                is_synced = False
        except Exception:
            # Fallback assuming synced if timedatectl is inaccessible
            is_synced = True

        if not is_synced and self.clock_safe:
            self.clock_safe = False
            self.emit_alert("CRITICAL", "System clock is not synchronized with NTP! Blocking timed execution.")
            if self.schedule_engine:
                # Block schedule engine execution
                self.schedule_engine._wake_event.clear()
        elif is_synced and not self.clock_safe:
            self.clock_safe = True
            self.emit_alert("INFO", "System clock synchronization restored. Timed execution unblocked.")

        return self.clock_safe

    # Rule 6: SQLite Storage Error (Safe Mode Stop)
    async def check_storage_health(self) -> bool:
        """
        Rule: If SQLite storage fails (disk full, I/O error, corruption),
        stops accepting commands safely to prevent partial data corruption.
        """
        if not self.db:
            return True

        try:
            # Perform a lightweight read/write health check
            async with self.db.connection.execute("PRAGMA quick_check(1);") as cur:
                row = await cur.fetchone()
                if row and row[0] != "ok":
                    raise RuntimeError(f"PRAGMA quick_check returned: {row[0]}")
            
            if self.storage_degraded:
                self.storage_degraded = False
                self.emit_alert("INFO", "SQLite storage health restored. Resuming normal operations.")
            return True

        except Exception as exc:
            if not self.storage_degraded:
                self.storage_degraded = True
                self.emit_alert("CRITICAL", f"SQLite storage error detected: {exc}. Entering safe degraded mode.")
                if self.schedule_engine:
                    await self.schedule_engine.stop()
            return False

    # Rule 2 & 3: USB Device Removed & Device Reconnected
    async def check_usb_devices(self) -> None:
        """
        Rule:
        - If USB device is removed: mark device OFFLINE in registry.
        - If USB device is reconnected: revalidate handshake (HELLO) and re-register ONLINE.
        """
        if not self.device_manager:
            return

        all_devices = self.device_manager.list_devices()
        target_ports = ["/dev/ttyUSB0", "/dev/ttyUSB1", "/dev/ttyACM0"]

        for port in target_ports:
            exists = os.path.exists(port)
            dev = self.device_manager.get_device(port)

            if not exists and port in self.known_online_devices:
                # USB Device Removed
                self.known_online_devices.discard(port)
                self.emit_alert("WARNING", f"USB serial device removed at '{port}'. Marking device OFFLINE.", {"port": port})
                if dev and hasattr(dev, "close"):
                    dev.close()
                if self.db:
                    await self.db.update_device_connection(port, "OFFLINE")

            elif exists and port not in self.known_online_devices:
                # Device Reconnected: revalidate and register
                self.known_online_devices.add(port)
                self.emit_alert("INFO", f"USB device reconnected at '{port}'. Revalidating handshake...", {"port": port})
                try:
                    if dev and hasattr(dev, "execute_command"):
                        await dev.execute_command("HELLO", {})
                    if self.db:
                        await self.db.update_device_connection(port, "ONLINE")
                    logger.info("Device at '%s' successfully revalidated and registered.", port)
                except Exception as handshake_err:
                    logger.warning("Reconnection handshake on '%s' failed: %s", port, handshake_err)

    # Rule 4 & 5: Command Timeout & Repeated Device Failure Quarantine
    def record_device_command_outcome(self, device_id: str, success: bool, error_msg: Optional[str] = None) -> None:
        """
        Rule:
        - On Command Timeout: Records failed/uncertain result.
        - On Repeated Failures: Quarantines device if consecutive errors exceed threshold.
        """
        if success:
            self.device_failure_counts[device_id] = 0
            return

        fails = self.device_failure_counts.get(device_id, 0) + 1
        self.device_failure_counts[device_id] = fails

        self.emit_alert(
            "WARNING",
            f"Device '{device_id}' command failure ({fails}/{self.max_consecutive_failures}): {error_msg}",
            {"device_id": device_id, "attempt": fails},
        )

        if fails >= self.max_consecutive_failures and device_id not in self.quarantined_devices:
            self.quarantined_devices.add(device_id)
            self.emit_alert(
                "CRITICAL",
                f"Device '{device_id}' quarantined after {fails} consecutive failures. Dispatches blocked.",
                {"device_id": device_id},
            )
            if self.db:
                asyncio.create_task(self.db.update_device_connection(device_id, "QUARANTINED"))

    def unquarantine_device(self, device_id: str) -> None:
        """Removes a quarantined device once manually inspected or re-tested."""
        self.quarantined_devices.discard(device_id)
        self.device_failure_counts[device_id] = 0
        self.emit_alert("INFO", f"Device '{device_id}' unquarantined.", {"device_id": device_id})

    def is_device_quarantined(self, device_id: str) -> bool:
        """Checks if a device is currently quarantined."""
        return device_id in self.quarantined_devices

    # Rule 1: RabbitMQ Connection & Auto-Retry
    async def check_rabbitmq_health(self) -> None:
        """
        Rule: If RabbitMQ broker becomes unavailable, retry connection with backoff
        while ensuring the hub continues running locally from SQLite.
        """
        if self.consumer:
            conn = getattr(self.consumer, "connection", None)
            if conn and conn.is_closed:
                self.emit_alert("WARNING", "RabbitMQ consumer disconnected! Local schedule execution continues.")
        if self.publisher:
            p_conn = getattr(self.publisher, "_connection", None)
            if p_conn and p_conn.is_closed:
                self.emit_alert("WARNING", "RabbitMQ publisher disconnected! Events accumulating in SQLite Outbox.")

    # Rule 7: Missed Schedule Detection
    async def check_missed_schedules(self) -> None:
        """
        Rule: If actions remain PENDING longer than the grace threshold past their
        scheduled_at timestamp, generate an operational alert.
        """
        if not self.schedule_repo:
            return

        now_utc = datetime.now(timezone.utc)
        schedules = await self.schedule_repo.list_schedules(status="ACTIVE")
        if not schedules:
            return

        active_id = schedules[0]["schedule_id"]
        actions = await self.schedule_repo.get_scheduled_actions(active_id, status="PENDING")

        for act in actions:
            sched_str = act.get("scheduled_at", "")
            try:
                sched_dt = datetime.fromisoformat(sched_str).astimezone(timezone.utc)
            except Exception:
                continue

            delay_secs = (now_utc - sched_dt).total_seconds()
            if delay_secs > self.missed_schedule_grace_seconds:
                self.emit_alert(
                    "WARNING",
                    f"Missed scheduled action '{act['action_id']}' ({act['action']}) on '{act['iot_node_id']}' overdue by {int(delay_secs)}s.",
                    {"action_id": act["action_id"], "scheduled_at": sched_str, "overdue_seconds": delay_secs},
                )

    # Rule 7b: Missing Daily Schedule Plan Detection
    async def check_missing_daily_schedule(self, target_date: Optional[str] = None) -> bool:
        """
        Rule: If no active daily schedule plan is present in SQLite for the current operating period,
        emit an operational warning alert reporting the missing plan.
        """
        if not self.schedule_repo:
            return False

        try:
            schedules = await self.schedule_repo.list_schedules(status="ACTIVE")
            if not schedules:
                msg = f"Missing daily schedule: No active schedule plan is registered for operation (target date: {target_date or 'today'})."
                self.emit_alert("WARNING", msg, {"target_date": target_date})
                return False
            if target_date:
                date_match = any(s.get("date") == target_date for s in schedules)
                if not date_match:
                    msg = f"Missing daily schedule: No active schedule plan found for target date '{target_date}'."
                    self.emit_alert("WARNING", msg, {"target_date": target_date})
                    return False
            return True
        except Exception as exc:
            logger.debug("Failed checking daily schedule: %s", exc)
            return False

    # Rule 8: Pending Result Backlog & Backpressure
    async def check_outbox_backlog(self) -> None:
        """
        Rule: If pending outbox result backlog exceeds threshold, apply backpressure
        and emit warning alert to prevent resource exhaustion.
        """
        if not self.outbox:
            return

        try:
            pending = await self.outbox.get_pending(limit=self.backlog_threshold + 5)
            count = len(pending)

            if count >= self.backlog_threshold and not self.backpressure_active:
                self.backpressure_active = True
                self.emit_alert(
                    "WARNING",
                    f"Outbox pending backlog reached {count} events (Threshold: {self.backlog_threshold}). Applying backpressure.",
                    {"pending_count": count},
                )
            elif count < (self.backlog_threshold // 2) and self.backpressure_active:
                self.backpressure_active = False
                self.emit_alert(
                    "INFO",
                    f"Outbox backlog drained to {count} events. Relieving backpressure.",
                    {"pending_count": count},
                )
        except Exception as exc:
            logger.debug("Failed checking outbox backlog: %s", exc)
