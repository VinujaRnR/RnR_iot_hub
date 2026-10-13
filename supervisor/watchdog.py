"""
Systemd Watchdog & sd_notify Integration for RnR Linux IoT Hub.
File: supervisor/watchdog.py

Implements native systemd service notification protocol (sd_notify) over Unix Domain Sockets:
- Sends 'READY=1' once the daemon finishes initialization.
- Periodically sends 'WATCHDOG=1' keepalive heartbeats based on $WATCHDOG_USEC.
- Sends 'STATUS=...' freeform health text visible via `systemctl status iothub`.
- Sends 'STOPPING=1' during graceful daemon shutdown.
- If health checks fail (storage degraded or clock unsafe), withholds WATCHDOG=1,
  allowing systemd to detect deadlock or failure and initiate safe recovery/restart.
"""

from __future__ import annotations
import asyncio
import logging
import os
import socket
from typing import Callable, Optional

logger = logging.getLogger("iothub.supervisor.watchdog")


class SystemdWatchdog:
    """
    Manages active communication with systemd over the $NOTIFY_SOCKET datagram socket.
    """

    def __init__(self, socket_path: Optional[str] = None):
        self.socket_path = socket_path or os.getenv("NOTIFY_SOCKET")
        self._task: Optional[asyncio.Task] = None
        self._running: bool = False

    @property
    def is_enabled(self) -> bool:
        """Returns True if systemd notify socket is configured in the environment."""
        return bool(self.socket_path)

    def notify(self, state: str) -> bool:
        """
        Sends an sd_notify protocol string (e.g. 'READY=1', 'WATCHDOG=1') to systemd.
        Supports both filesystem paths and abstract namespace sockets (starting with '@').
        """
        sock_path = self.socket_path or os.getenv("NOTIFY_SOCKET")
        if not sock_path:
            return False

        try:
            # Systemd abstract socket addresses begin with '@', which map to '\0' in Linux AF_UNIX
            if sock_path.startswith("@"):
                sock_path = "\0" + sock_path[1:]

            with socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as sock:
                sock.connect(sock_path)
                sock.sendall(state.encode("utf-8"))
            logger.debug("Sent systemd sd_notify: %s", state.strip())
            return True
        except Exception as exc:
            logger.debug("Failed sending sd_notify '%s': %s", state.strip(), exc)
            return False

    def notify_ready(self) -> bool:
        """Notifies systemd that service startup is complete and ready to serve."""
        return self.notify("READY=1")

    def notify_watchdog(self) -> bool:
        """Sends keepalive heartbeat to systemd watchdog."""
        return self.notify("WATCHDOG=1")

    def notify_status(self, status: str) -> bool:
        """Updates service status message displayed in systemctl status."""
        return self.notify(f"STATUS={status}")

    def notify_stopping(self) -> bool:
        """Notifies systemd that service is beginning orderly shutdown."""
        return self.notify("STOPPING=1")

    def get_watchdog_interval(self) -> float:
        """
        Determines the watchdog heartbeat interval in seconds.
        Per systemd documentation, the application should ping at interval = WATCHDOG_USEC / 2.
        Defaults to 10 seconds if not specified.
        """
        watchdog_usec = os.getenv("WATCHDOG_USEC")
        if watchdog_usec:
            try:
                usec = int(watchdog_usec)
                # Ping at half the timeout period (in seconds), with minimum 1.0s
                return max(1.0, (usec / 1_000_000.0) / 2.0)
            except ValueError:
                pass
        return 10.0

    async def start(self, health_check_fn: Optional[Callable[[], bool]] = None) -> None:
        """
        Starts the periodic watchdog loop.
        Only sends 'WATCHDOG=1' if health_check_fn returns True.
        """
        if not self.is_enabled or self._running:
            return

        self._running = True
        interval = self.get_watchdog_interval()
        logger.info(
            "Starting systemd watchdog heartbeat loop (interval: %.1fs, socket: %s)",
            interval,
            self.socket_path,
        )

        async def _loop():
            while self._running:
                is_healthy = True
                if health_check_fn:
                    try:
                        is_healthy = health_check_fn()
                    except Exception as err:
                        logger.warning("Watchdog health evaluation error: %s", err)
                        is_healthy = False

                if is_healthy:
                    self.notify_watchdog()
                else:
                    logger.warning("System health degraded: withholding WATCHDOG=1 ping.")

                await asyncio.sleep(interval)

        self._task = asyncio.create_task(_loop(), name="systemd_watchdog_loop")

    async def stop(self) -> None:
        """Stops the watchdog heartbeat and sends STOPPING=1 notification."""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        self.notify_stopping()
        logger.debug("Systemd watchdog stopped.")
