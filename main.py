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
from storage import DatabaseManager, ScheduleRepository, EventOutbox, RegistrationRepository, ConfigRepository
from core.registration_guard import RegistrationGuard, HubOperatingState
from core.config_sync_manager import ConfigSyncManager
from core.auto_config_manager import AutoConfigManager
from core.registration_listener import RegistrationListener
from core.middleware_provisioning import MiddlewareProvisioningListener
from core.provisioning_reporter import ProvisioningReporter
from devices.device_registry import DeviceRegistry
from devices.auto_enrollment import AutoEnrollmentManager
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

    # Early Signal Handling Setup
    stop_event = stop_override_event or asyncio.Event()

    def _signal_handler(sig_name: str):
        logger.info("[SIGNAL] Received %s signal. Requesting graceful shutdown...", sig_name)
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, lambda s=sig: _signal_handler(s.name))
        except (NotImplementedError, RuntimeError):
            pass

    # Declare component handles for reliable cleanup in finally block
    db = None
    device_manager = None
    publisher = None
    provisioning_reporter = None
    schedule_engine = None
    scheduler = None
    supervisor = None
    consumer = None
    consumer_task = None
    provisioning_listener = None
    provisioning_task = None
    usb_task = None
    api_server = None
    api_task = None

    try:
        # 2. Initialize SQLite Storage
        logger.info("[INIT] Initializing SQLite database at '%s'...", settings.DATABASE_PATH)
        db = DatabaseManager(db_path=settings.DATABASE_PATH)
        await db.connect()
        schedule_repo = ScheduleRepository(db)
        event_outbox = EventOutbox(db)
        reg_repo = RegistrationRepository(db)
        guard = RegistrationGuard(reg_repo, lease_grace_hours=settings.OFFLINE_LEASE_GRACE_HOURS)
        logger.info("[INIT] SQLite storage ready (WAL mode verified).")

        # Load encrypted RabbitMQ credentials from SQLite if present
        try:
            db_rmq_pass = await reg_repo.config_repo.get_setting("rabbitmq.password")
            if db_rmq_pass:
                from config import update_rabbitmq_password
                update_rabbitmq_password(db_rmq_pass)
                logger.info("[INIT] Loaded encrypted RabbitMQ broker credentials from SQLite.")
        except Exception as exc:
            logger.debug("[INIT] Could not load rabbitmq.password from SQLite: %s", exc)

        # Registration & Activation check (RNHub V2)
        reg_snapshot = await reg_repo.get_registration()
        op_state = await guard.get_operating_state()
        is_auth, auth_reason = await guard.check_hub_authorized()
        logger.info("[SECURITY] Hub Operating State: %s (%s)", op_state, auth_reason)
        if not is_auth:
            logger.warning("[SECURITY] Production operations restricted until registration activation: %s", auth_reason)

        # Start Local Management API immediately so health checks & monitoring respond without delay
        logger.info("[INIT] Starting Local Management API on http://%s:%d...", settings.API_HOST, settings.API_PORT)
        management_app = create_app(
            db=db,
            schedule_repo=schedule_repo,
            outbox=event_outbox,
        )
        api_server, api_task = start_background_api(
            host=settings.API_HOST,
            port=settings.API_PORT,
            application=management_app,
        )

        if stop_event.is_set():
            return

        # 3. Initialize USB Device Manager (Blocking serial I/O managed by worker threads)
        logger.info("[INIT] Initializing USB Device Manager...")
        device_manager = create_default_device_manager()
        device_manager.registration_guard = guard
        management_app.state.device_manager = device_manager
        router = CommandRouter(hub_id=HUB_ID, device_manager=device_manager, registration_guard=guard)

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

        # 3b. Initialize Auto-Enrollment & Configuration Synchronization (RNHub V2)
        device_registry = DeviceRegistry(db)
        auto_enrollment = AutoEnrollmentManager(
            db=db,
            device_registry=device_registry,
            device_manager=device_manager,
            reg_repo=reg_repo,
        )
        config_sync = ConfigSyncManager(
            db=db,
            reg_repo=reg_repo,
            config_repo=reg_repo.config_repo,
            auto_enrollment=auto_enrollment,
            expected_hub_id=settings.HUB_ID,
            expected_hub_uuid=settings.HUB_UUID or (reg_snapshot.get("hub_uuid") if reg_snapshot else None),
        )
        try:
            startup_enrollment = await auto_enrollment.discover_and_reconcile()
            logger.info(
                "[INIT] Auto-enrollment startup scan: bound=%d, pending=%d, rejected=%d",
                len(startup_enrollment.get("bound", [])),
                len(startup_enrollment.get("pending", [])),
                len(startup_enrollment.get("rejected", [])),
            )
        except Exception as exc:
            logger.debug("[INIT] Auto-enrollment startup scan notice: %s", exc)

        if stop_event.is_set():
            return

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

        # 4b. Auto-load cached last_rabbitmq_message if database currently has no active schedules
        try:
            active_scheds = await schedule_repo.list_schedules(status="ACTIVE")
            if not active_scheds:
                for last_msg_path in ("/var/lib/iothub/last_rabbitmq_message.json", "/tmp/last_rabbitmq_message.json"):
                    if os.path.exists(last_msg_path):
                        import json
                        from ingestion.schedule_validator import ScheduleValidator
                        from ingestion.schedule_normalizer import ScheduleNormalizer
                        with open(last_msg_path, "r", encoding="utf-8") as f:
                            meta = json.load(f)
                        if meta.get("detected_type") != "SCHEDULE":
                            continue
                        payload = meta.get("payload", {})
                        raw_body = meta.get("raw_body", "")
                        val = ScheduleValidator().validate(payload)
                        norm = ScheduleNormalizer().normalize(payload)
                        if norm:
                            await schedule_repo.save_schedule_with_actions(
                                schedule_id=val.schedule_id,
                                date=val.date,
                                revision=val.revision,
                                actions=norm,
                                raw_json=raw_body,
                            )
                            logger.info("[INIT] Automatically imported cached schedule '%s' with %d actions from %s", val.schedule_id, len(norm), last_msg_path)
                            break
        except Exception as auto_err:
            logger.debug("[INIT] Auto-import of cached schedule skipped: %s", auto_err)

        if stop_event.is_set():
            return

        # 5. Initialize Publisher with Durable Outbox
        logger.info("[INIT] Starting RabbitMQ Publisher and Outbox Worker...")
        publisher = RabbitMQPublisher(outbox=event_outbox)
        management_app.state.publisher = publisher
        try:
            await publisher.connect()
            logger.info("[INIT] RabbitMQ Publisher connected successfully.")
        except Exception as exc:
            logger.warning("[INIT] Initial RabbitMQ Publisher connection deferred: %s", exc)
        await publisher.start_outbox_worker(interval=2.0)

        # 5b. Initialize Provisioning Reporter (RNHub V2 Outbox)
        provisioning_reporter = ProvisioningReporter(
            db=db,
            publisher=publisher,
            hub_uuid=settings.HUB_UUID or (reg_snapshot.get("hub_uuid") if reg_snapshot else None),
            hub_id=settings.HUB_ID,
        )
        await provisioning_reporter.start_outbox_worker(interval=5.0)
        config_sync.provisioning_reporter = provisioning_reporter

        if stop_event.is_set():
            return

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
        management_app.state.schedule_engine = schedule_engine
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
            registration_client=None,
            registration_guard=guard,
            reg_repo=reg_repo,
            auto_enrollment=auto_enrollment,
        )
        management_app.state.supervisor = supervisor
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
            registration_guard=guard,
        )
        supervisor.consumer = consumer

        # 8b. Initialize Registration Listener (RNHub V2)
        logger.info("[INIT] Starting Registration Listener...")
        registration_listener = RegistrationListener(
            url=settings.RABBITMQ_URL,
            hub_id=settings.HUB_ID,
            hub_uuid=settings.HUB_UUID or (reg_snapshot.get("hub_uuid") if reg_snapshot else None),
            auto_config_manager=config_sync,
            provisioning_reporter=provisioning_reporter,
        )
        provisioning_listener = registration_listener

        # 9. Verify Local Management API Ready
        logger.info("[INIT] Local Management API operational on http://%s:%d", settings.API_HOST, settings.API_PORT)

        # 10. Background Loops
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

        # Background Provisioning Listener Loop with Automatic Reconnect (RNHub V2)
        async def _provisioning_loop():
            while not stop_event.is_set():
                try:
                    await provisioning_listener.start()
                    await stop_event.wait()
                except asyncio.CancelledError:
                    break
                except Exception as exc:
                    if stop_event.is_set():
                        break
                    logger.warning("[PROVISIONING] Connection lost: %s. Reconnecting in 5.0s...", exc)
                    try:
                        await asyncio.wait_for(stop_event.wait(), timeout=5.0)
                    except asyncio.TimeoutError:
                        pass

        provisioning_task = asyncio.create_task(_provisioning_loop(), name="provisioning_loop")

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

        await stop_event.wait()
    finally:
        # =====================================================================
        # 12. Graceful Shutdown Sequence (Mermaid Diagram Specification)
        # =====================================================================
        logger.info("[SHUTDOWN] Step 1: Stopping acceptance of new work...")
        if consumer_task and not consumer_task.done():
            consumer_task.cancel()
        if provisioning_task and not provisioning_task.done():
            provisioning_task.cancel()
        if usb_task and not usb_task.done():
            usb_task.cancel()
        if consumer:
            try:
                await consumer.stop()
            except Exception:
                pass
        if provisioning_listener:
            try:
                await provisioning_listener.stop()
            except Exception:
                pass
        if schedule_engine:
            try:
                await schedule_engine.stop()
            except Exception:
                pass
        if scheduler:
            try:
                await scheduler.stop()
            except Exception:
                pass

        logger.info("[SHUTDOWN] Step 2: Persisting and resolving outstanding states...")
        if supervisor:
            try:
                await supervisor.stop()
            except Exception:
                pass
        if provisioning_reporter:
            try:
                await provisioning_reporter.stop_outbox_worker()
            except Exception:
                pass
        if publisher:
            try:
                await publisher.close()
            except Exception:
                pass

        logger.info("[SHUTDOWN] Step 3: Closing Management API, USB connections, and SQLite database...")
        # Stop Management API
        if api_server and api_task:
            api_server.should_exit = True
            try:
                await asyncio.wait_for(api_task, timeout=2.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                api_task.cancel()

        # Close all active USB hardware serial ports
        if device_manager:
            device_manager.close_all()

        # Close SQLite storage connection
        if db:
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
