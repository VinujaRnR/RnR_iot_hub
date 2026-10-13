"""
RnR Linux IoT Hub Main Daemon (iothubd).
File: main.py

Implements Step 16 Main Daemon Lifecycle:
- Validate configuration: Ensures all environment settings are valid on startup.
- Initialize SQLite: Connects storage engine, verifies WAL mode & table schemas.
- Restore accepted schedules: Reconciles in-flight and missed schedule actions
  (actions recorded as EXECUTING/CLAIMED without definitive acknowledgment
   are reconciled safely rather than automatically marked successful).
- Parallel Subsystem Lifecycle:
  1. USB Device Manager: Manages microcontrollers via non-blocking worker threads.
  2. RabbitMQ Client: Handles inbound commands/schedules and durable outbox publishing.
  3. Local Scheduler: Autonomous local execution engine with atomic claims.
  4. HubSupervisor: Continuous monitoring, recovery rules, and watchdog notifications.
  5. Local Management API: Read-only inspection interface on 127.0.0.1:8765.
- Continuous Operation: Cancellation-aware asyncio tasks.
- Graceful Shutdown:
  Catches SIGTERM / SIGINT, stops accepting work, flushes states, closes broker/USB/DB,
  and exits cleanly.
"""

from __future__ import annotations
import asyncio
import logging
import os
import signal
import sys
import time
from typing import Optional

from config import (
    Settings,
    HUB_ID,
    LOG_LEVEL,
    DATABASE_PATH,
    SCHEDULE_TIMEZONE,
    API_HOST,
    API_PORT,
    USB_SCAN_INTERVAL,
)
from api.management_api import create_app, start_background_api
from core.router import CommandRouter
from devices.device_manager import DeviceManager, create_default_device_manager
from devices.usb_detector import USBDetector
from ingestion.rabbitmq_consumer import RabbitMQConsumer
from ingestion.rabbitmq_publisher import RabbitMQPublisher
from scheduler.scheduler import TaskScheduler
from scheduler.action_dispatcher import ActionDispatcher
from scheduler.schedule_engine import ScheduleEngine
from scheduler.state_reconciler import StateReconciler
from storage import DatabaseManager, ScheduleRepository, EventOutbox
from supervisor.supervisor import HubSupervisor

logger = logging.getLogger("iothub")


def setup_logging():
    """Configures structured daemon logging format."""
    log_numeric = getattr(logging, LOG_LEVEL.upper(), logging.INFO)
    logging.basicConfig(
        level=log_numeric,
        format="%(asctime)s | %(levelname)-7s | [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,
    )


def validate_configuration() -> Settings:
    """
    Validates central application settings at startup.
    Fails fast with clear diagnostic logging if invalid.
    """
    try:
        settings = Settings()
        logger.info("[STARTUP] Configuration successfully validated.")
        logger.info(
            "[STARTUP] Hub ID: '%s' | Timezone: '%s' | DB: '%s'",
            settings.HUB_ID,
            settings.SCHEDULE_TIMEZONE,
            settings.DATABASE_PATH,
        )
        return settings
    except Exception as exc:
        logger.critical("[STARTUP] Configuration validation FAILED: %s", exc)
        raise SystemExit(1)


async def run_hub(stop_override_event: Optional[asyncio.Event] = None):
    """
    Main asynchronous daemon lifecycle coordinating all hub components.
    Accepts optional stop_override_event for automated testing harnesses.
    """
    logger.info("=================================================================")
    logger.info("       Starting RnR Linux IoT Hub Daemon [iothubd]               ")
    logger.info("=================================================================")
    logger.info(" PID: %d | Timezone: %s", os.getpid(), SCHEDULE_TIMEZONE)

    # 1. Validate Configuration
    settings = validate_configuration()

    # 2. Initialize SQLite Storage
    logger.info("[INIT] Initializing SQLite database at '%s'...", DATABASE_PATH)
    db = DatabaseManager(db_path=DATABASE_PATH)
    await db.connect()
    schedule_repo = ScheduleRepository(db)
    event_outbox = EventOutbox(db)
    logger.info("[INIT] SQLite storage ready (WAL mode verified).")

    # 3. Initialize USB Device Manager (Blocking serial I/O managed by worker threads)
    logger.info("[INIT] Initializing USB Device Manager...")
    device_manager = create_default_device_manager()
    router = CommandRouter(hub_id=HUB_ID, device_manager=device_manager)

    # Persist registered devices in database
    for dev_id in device_manager.list_devices():
        dev_obj = device_manager.get_device(dev_id)
        if dev_obj:
            await db.save_device(
                device_id=getattr(dev_obj, "device_id", dev_id),
                name=getattr(dev_obj, "name", dev_id),
                capabilities={
                    "channels": getattr(dev_obj, "channels", ["relay:light"]),
                    "actions": getattr(dev_obj, "supported_actions", ["LIGHTS_ON", "LIGHTS_OFF"]),
                },
                connection_state="ONLINE",
            )
    logger.info("[INIT] Registered %d device(s) and alias(es).", len(device_manager.list_devices()))

    # 4. Restore Accepted Schedules & Reconcile In-Flight/Missed Actions
    logger.info("[INIT] Restoring accepted schedules and reconciling in-flight actions...")
    reconciler = StateReconciler(
        db=db,
        schedule_repo=schedule_repo,
        device_manager=device_manager,
    )
    report = await reconciler.reconcile_on_startup()
    logger.info(
        "[INIT] State reconciliation complete: applied=%d, superseded=%d, skipped_unsafe=%d, future_pending=%d",
        len(report.applied_actions),
        len(report.superseded_actions),
        len(report.skipped_unsafe_actions),
        report.future_pending_count,
    )

    # 5. Initialize Publisher with Durable Outbox
    logger.info("[INIT] Starting RabbitMQ Publisher and Outbox Worker...")
    publisher = RabbitMQPublisher(outbox=event_outbox)
    try:
        await publisher.connect()
        logger.info("[INIT] RabbitMQ Publisher connected successfully.")
    except Exception as exc:
        logger.warning("[INIT] Initial RabbitMQ Publisher connection deferred: %s", exc)
    await publisher.start_outbox_worker(interval=2.0)

    # 6. Initialize Action Dispatcher & Local Schedule Execution Engine
    logger.info("[INIT] Starting Local Schedule Execution Engine...")
    dispatcher = ActionDispatcher(
        device_manager=device_manager,
        schedule_repo=schedule_repo,
        outbox=event_outbox,
        publisher=publisher,
    )
    schedule_engine = ScheduleEngine(
        database=db,
        schedule_repo=schedule_repo,
        dispatcher=dispatcher,
    )
    await schedule_engine.start()

    # Legacy TaskScheduler for periodic ad-hoc commands
    scheduler = TaskScheduler(
        dispatch_callback=lambda dev, act, params: device_manager.dispatch(dev, act, params)
    )
    await scheduler.start()

    # 7. Initialize HubSupervisor (Recovery rules, device quarantining, clock & watchdog)
    logger.info("[INIT] Starting HubSupervisor monitoring daemon...")
    supervisor = HubSupervisor(
        check_interval=15.0,
        db=db,
        schedule_repo=schedule_repo,
        device_manager=device_manager,
        schedule_engine=schedule_engine,
        outbox=event_outbox,
        publisher=publisher,
    )
    dispatcher.supervisor = supervisor
    schedule_engine.supervisor = supervisor
    await supervisor.start()

    # 8. Initialize RabbitMQ Consumer
    logger.info("[INIT] Starting RabbitMQ Consumer loop...")
    consumer = RabbitMQConsumer(
        router=router,
        db=db,
        schedule_repo=schedule_repo,
        publisher=publisher,
        schedule_engine=schedule_engine,
    )
    supervisor.consumer = consumer

    # 9. Initialize & Start Local Management API (127.0.0.1:8765, Read-Only)
    logger.info("[INIT] Starting Local Management API on http://%s:%d...", API_HOST, API_PORT)
    management_app = create_app(
        db=db,
        schedule_repo=schedule_repo,
        device_manager=device_manager,
        supervisor=supervisor,
        schedule_engine=schedule_engine,
        outbox=event_outbox,
        publisher=publisher,
    )
    api_server, api_task = start_background_api(
        host=API_HOST,
        port=API_PORT,
        application=management_app,
    )

    # 10. Cancellation-Aware Signal Handling & Background Loops
    stop_event = stop_override_event or asyncio.Event()

    def _signal_handler(sig_name: str):
        logger.info("[SIGNAL] Received %s signal. Requesting graceful shutdown...", sig_name)
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, lambda s=sig: _signal_handler(s.name))
        except (NotImplementedError, RuntimeError):
            # Fallback for environments without Unix signal handlers
            pass

    # Background Consumer Worker Loop with Automatic Reconnect
    async def _consumer_loop():
        while not stop_event.is_set():
            try:
                await consumer.start()
                await stop_event.wait()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                if stop_event.is_set():
                    break
                logger.error("[RABBITMQ] Connection lost: %s. Reconnecting in 5.0s...", exc)
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=5.0)
                except asyncio.TimeoutError:
                    pass

    consumer_task = asyncio.create_task(_consumer_loop(), name="consumer_loop")

    # Background USB Hardware Detector Task
    async def _usb_monitor_loop():
        detector = USBDetector()
        scan_sec = float(USB_SCAN_INTERVAL)
        while not stop_event.is_set():
            try:
                # Run device scanning off main thread so pyudev doesn't block event loop
                ports = await loop.run_in_executor(None, detector.scan_serial_ports)
                for p in ports:
                    port_path = p.get("port")
                    if port_path and not device_manager.get_device(port_path):
                        await db.save_device(
                            device_id=port_path,
                            name=f"USB Serial ({port_path})",
                            capabilities={"channels": ["relay:light"], "actions": ["LIGHTS_ON", "LIGHTS_OFF"]},
                            connection_state="ONLINE",
                        )
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.debug("[USB] Monitor scan exception: %s", exc)

            try:
                await asyncio.wait_for(stop_event.wait(), timeout=scan_sec)
            except asyncio.TimeoutError:
                pass

    usb_task = asyncio.create_task(_usb_monitor_loop(), name="usb_monitor")

    # 11. Continuous Operation
    logger.info("=================================================================")
    logger.info(" RnR IoT Hub is OPERATIONAL. Press Ctrl+C or send SIGTERM to exit.")
    logger.info(" Management API available at: http://%s:%d", API_HOST, API_PORT)
    logger.info("=================================================================")

    try:
        await stop_event.wait()
    finally:
        # =====================================================================
        # 12. Graceful Shutdown Sequence (Mermaid Diagram Specification)
        # =====================================================================
        logger.info("[SHUTDOWN] Step 1: Stopping acceptance of new work...")
        consumer_task.cancel()
        usb_task.cancel()
        await consumer.stop()
        await schedule_engine.stop()
        await scheduler.stop()

        logger.info("[SHUTDOWN] Step 2: Persisting and resolving outstanding states...")
        await supervisor.stop()
        await publisher.close()

        logger.info("[SHUTDOWN] Step 3: Closing Management API, USB connections, and SQLite database...")
        # Stop Management API
        api_server.should_exit = True
        try:
            await asyncio.wait_for(api_task, timeout=2.0)
        except (asyncio.TimeoutError, asyncio.CancelledError):
            api_task.cancel()

        # Close all active USB hardware serial ports
        device_manager.close_all()

        # Close SQLite storage connection
        await db.close()

        logger.info("=================================================================")
        logger.info(" RnR IoT Hub Daemon successfully shut down. Clean exit.          ")
        logger.info("=================================================================")


def main():
    """Main daemon CLI entrypoint."""
    setup_logging()
    try:
        asyncio.run(run_hub())
    except (KeyboardInterrupt, SystemExit):
        pass


if __name__ == "__main__":
    main()
