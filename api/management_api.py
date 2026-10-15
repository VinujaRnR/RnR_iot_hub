"""
Local Management API for RnR Linux IoT Hub.
File: api/management_api.py

Implements Step 15 specification:
- Read-only management interface built with FastAPI.
- Bound by default to 127.0.0.1:8765.
- Endpoints:
  1. GET /health/live      -> Process health (liveness probe)
  2. GET /health/ready     -> Operational readiness (readiness probe)
  3. GET /hub/status       -> Full hub status
  4. GET /devices          -> Connected & registered devices
  5. GET /devices/{id}     -> Specific device details and current state
  6. GET /schedules        -> Accepted daily schedules
  7. GET /schedules/{id}   -> Specific schedule info and action breakdown
  8. GET /actions          -> Pending and completed actions with filtering
  9. GET /actions/{id}     -> Action execution status and attempt history
 10. GET /metrics          -> Operational counters and system metrics
"""

from __future__ import annotations
import asyncio
from datetime import datetime, timezone
import logging
import os
import time
from typing import Any, Dict, List, Optional
from urllib.parse import unquote

from fastapi import FastAPI, HTTPException, Query, Request, Response, status
from fastapi.responses import JSONResponse

from config import HUB_ID, SCHEDULE_TIMEZONE, DATABASE_PATH, API_HOST, API_PORT
from devices.device_manager import DeviceManager, create_default_device_manager
from storage.database import DatabaseManager
from storage.schedule_repository import ScheduleRepository
from storage.event_outbox import EventOutbox

logger = logging.getLogger("iothub.api.management")

_START_TIME = time.time()


def create_app(
    db: Optional[DatabaseManager] = None,
    schedule_repo: Optional[ScheduleRepository] = None,
    device_manager: Optional[DeviceManager] = None,
    supervisor: Optional[Any] = None,
    schedule_engine: Optional[Any] = None,
    outbox: Optional[EventOutbox] = None,
    publisher: Optional[Any] = None,
) -> FastAPI:
    """
    Factory creating configured FastAPI management application.
    Supports runtime injection of core hub services.
    """
    app = FastAPI(
        title="RnR IoT Hub Local Management API",
        version="1.0.0",
        description="Deterministic local inspection interface for RnR Linux IoT Hub (Step 15)",
        docs_url="/docs",
        redoc_url=None,
    )

    # Store references on app state
    app.state.db = db
    app.state.schedule_repo = schedule_repo
    app.state.device_manager = device_manager
    app.state.supervisor = supervisor
    app.state.schedule_engine = schedule_engine
    app.state.outbox = outbox
    app.state.publisher = publisher
    app.state.start_time = _START_TIME

    # Helper dependencies with fallback instantiation
    async def get_db_instance() -> DatabaseManager:
        if app.state.db is not None:
            return app.state.db
        # Lazy fallback
        fallback_db = DatabaseManager(db_path=DATABASE_PATH)
        await fallback_db.connect()
        app.state.db = fallback_db
        return fallback_db

    async def get_repo_instance() -> ScheduleRepository:
        if app.state.schedule_repo is not None:
            return app.state.schedule_repo
        db_inst = await get_db_instance()
        repo_inst = ScheduleRepository(db_inst)
        app.state.schedule_repo = repo_inst
        return repo_inst

    def get_dev_mgr() -> DeviceManager:
        if app.state.device_manager is not None:
            return app.state.device_manager
        fallback_mgr = create_default_device_manager()
        app.state.device_manager = fallback_mgr
        return fallback_mgr

    # =========================================================================
    # 1. GET /health/live - Process Health (Liveness Probe)
    # =========================================================================
    @app.get("/health/live", summary="Process health (liveness)")
    async def health_live():
        """Returns 200 OK if the Python process is alive and responding."""
        now_utc = datetime.now(timezone.utc).isoformat()
        uptime = round(time.time() - app.state.start_time, 2)
        return {
            "status": "alive",
            "hub_id": HUB_ID,
            "pid": os.getpid(),
            "uptime_seconds": uptime,
            "timestamp": now_utc,
        }

    # =========================================================================
    # 2. GET /health/ready - Operational Readiness (Readiness Probe)
    # =========================================================================
    @app.get("/health/ready", summary="Operational readiness")
    async def health_ready():
        """
        Evaluates operational readiness:
        - SQLite database connection is responsive.
        - System clock is synchronized and safe.
        - Supervisor is not in a degraded or critical state.
        Returns 200 OK if ready, or 503 Service Unavailable if unready.
        """
        is_ready = True
        reasons: List[str] = []

        # Check DB connectivity
        try:
            db_inst = await get_db_instance()
            async with db_inst.connection.execute("SELECT 1;") as cur:
                row = await cur.fetchone()
                if not row or row[0] != 1:
                    is_ready = False
                    reasons.append("Database query returned unexpected result")
        except Exception as exc:
            is_ready = False
            reasons.append(f"Database error: {exc}")

        # Check Supervisor state if present
        sup = app.state.supervisor
        sup_status = "UNKNOWN"
        if sup is not None:
            if getattr(sup, "storage_degraded", False):
                is_ready = False
                reasons.append("Storage is marked degraded by supervisor")
            if not getattr(sup, "clock_safe", True):
                is_ready = False
                reasons.append("System clock is not synchronized with NTP")
            sup_metrics = sup.get_system_metrics() if hasattr(sup, "get_system_metrics") else {}
            sup_status = sup_metrics.get("status", "HEALTHY")

        if is_ready:
            return {
                "status": "ready",
                "ready": True,
                "hub_id": HUB_ID,
                "database": "connected",
                "supervisor_status": sup_status,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
        else:
            return JSONResponse(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                content={
                    "status": "not_ready",
                    "ready": False,
                    "hub_id": HUB_ID,
                    "reasons": reasons,
                    "supervisor_status": sup_status,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                },
            )

    # =========================================================================
    # 3. GET /hub/status - Full Hub Status
    # =========================================================================
    @app.get("/hub/status", summary="Full hub status")
    async def hub_status():
        """Returns comprehensive diagnostic and operational state of the IoT Hub."""
        uptime = round(time.time() - app.state.start_time, 2)
        dev_mgr = get_dev_mgr()
        registered_devices = dev_mgr.list_devices()

        # Database state
        db_path = DATABASE_PATH
        db_connected = False
        try:
            db_inst = await get_db_instance()
            db_connected = db_inst.connection is not None
        except Exception:
            pass

        # Supervisor metrics
        sup = app.state.supervisor
        sup_info = {
            "status": "STANDALONE",
            "quarantined_devices": [],
            "active_alerts_count": 0,
            "backpressure_active": False,
            "clock_safe": True,
        }
        if sup is not None and hasattr(sup, "get_system_metrics"):
            sup_info = sup.get_system_metrics()

        # Scheduler engine status
        engine = app.state.schedule_engine
        scheduler_info = {
            "running": getattr(engine, "is_running", False) if engine else False,
            "active_schedule_id": getattr(engine, "active_schedule_id", None) if engine else None,
        }

        # RabbitMQ publisher and outbox backlog
        pub = app.state.publisher
        outbox_inst = app.state.outbox
        outbox_stats = {"PENDING": 0, "SENT": 0, "FAILED": 0}
        if outbox_inst:
            try:
                outbox_stats = await outbox_inst.get_stats()
            except Exception:
                pass

        load_avg = [0.0, 0.0, 0.0]
        try:
            load_avg = list(os.getloadavg())
        except Exception:
            pass

        return {
            "hub_id": HUB_ID,
            "timezone": SCHEDULE_TIMEZONE,
            "status": "ONLINE",
            "uptime_seconds": uptime,
            "database": {
                "path": db_path,
                "connected": db_connected,
            },
            "supervisor": sup_info,
            "devices_count": len(registered_devices),
            "scheduler": scheduler_info,
            "rabbitmq": {
                "publisher_connected": getattr(pub, "is_connected", False) if pub else False,
                "outbox_backlog": outbox_stats.get("PENDING", 0),
                "outbox_stats": outbox_stats,
            },
            "system": {
                "pid": os.getpid(),
                "load_1m": load_avg[0],
                "load_5m": load_avg[1],
            },
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    # =========================================================================
    # 4. GET /devices - Connected Devices
    # =========================================================================
    @app.get("/devices", summary="Connected and registered devices")
    async def list_devices():
        """Lists all registered physical/logical IoT devices and their current states."""
        dev_mgr = get_dev_mgr()
        db_inst = await get_db_instance()
        sup = app.state.supervisor

        # Read registered devices from SQLite
        db_devices = await db_inst.list_devices()
        db_devices_by_id = {d["device_id"]: d for d in db_devices}

        results = []
        for dev_key in dev_mgr.list_devices():
            dev_obj = dev_mgr.get_device(dev_key)
            real_id = getattr(dev_obj, "device_id", dev_key) if dev_obj else dev_key

            is_quarantined = False
            if sup and hasattr(sup, "is_device_quarantined"):
                is_quarantined = sup.is_device_quarantined(real_id)

            db_entry = db_devices_by_id.get(real_id, {})
            channels = getattr(dev_obj, "channels", ["relay:light"]) if dev_obj else []
            actions = getattr(dev_obj, "supported_actions", ["LIGHTS_ON", "LIGHTS_OFF"]) if dev_obj else []

            conn_state = "QUARANTINED" if is_quarantined else db_entry.get("connection_state", "ONLINE")

            results.append({
                "device_id": real_id,
                "alias": dev_key if dev_key != real_id else None,
                "name": getattr(dev_obj, "name", real_id),
                "channels": channels,
                "supported_actions": actions,
                "connection_state": conn_state,
                "is_quarantined": is_quarantined,
                "last_seen": db_entry.get("last_seen"),
            })

        # Deduplicate results by device_id
        unique_results = {}
        for r in results:
            uid = r["device_id"]
            if uid not in unique_results or r["alias"] is None:
                unique_results[uid] = r

        return {
            "total": len(unique_results),
            "devices": list(unique_results.values()),
        }

    # =========================================================================
    # 5. GET /devices/{id} - Specific Device
    # =========================================================================
    @app.get("/devices/{device_id:path}", summary="Specific device details")
    async def get_device(device_id: str):
        """Retrieves operational details and current state for a specific device."""
        clean_id = unquote(device_id)
        dev_mgr = get_dev_mgr()
        dev_obj = dev_mgr.get_device(clean_id)

        db_inst = await get_db_instance()
        db_info = await db_inst.get_device(clean_id)

        if not dev_obj and not db_info:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Device '{clean_id}' not found on hub '{HUB_ID}'",
            )

        real_id = getattr(dev_obj, "device_id", clean_id) if dev_obj else clean_id
        sup = app.state.supervisor
        is_quarantined = False
        if sup and hasattr(sup, "is_device_quarantined"):
            is_quarantined = sup.is_device_quarantined(real_id)

        current_state = getattr(dev_obj, "_state", {}) if dev_obj else {}
        conn_state = "QUARANTINED" if is_quarantined else (db_info.get("connection_state", "ONLINE") if db_info else "ONLINE")

        return {
            "device_id": real_id,
            "name": getattr(dev_obj, "name", db_info.get("name") if db_info else real_id),
            "channels": getattr(dev_obj, "channels", db_info.get("capabilities", {}).get("channels", ["relay:light"])),
            "supported_actions": getattr(dev_obj, "supported_actions", db_info.get("capabilities", {}).get("actions", ["LIGHTS_ON", "LIGHTS_OFF"])),
            "connection_state": conn_state,
            "is_quarantined": is_quarantined,
            "current_state": current_state,
            "port": getattr(dev_obj, "port", None),
            "last_seen": db_info.get("last_seen") if db_info else None,
            "updated_at": db_info.get("updated_at") if db_info else None,
        }

    # =========================================================================
    # 5b. POST /devices/{device_id:path}/command - Transmit Command to Device
    # =========================================================================
    @app.post("/devices/{device_id:path}/command", summary="Send command payload to device")
    async def send_device_command(device_id: str, payload: Dict[str, Any]):
        """Executes action directly on physical or logical device."""
        clean_id = unquote(device_id)
        dev_mgr = get_dev_mgr()
        action = payload.get("action")
        if not action:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Missing 'action' in request body")
        params = payload.get("params") or {}
        try:
            res = await dev_mgr.dispatch(clean_id, action, {"params": params} if params else {})
            return {"status": "SUCCESS", "device_id": clean_id, "result": res}
        except Exception as exc:
            logger.error("Failed to execute command '%s' on '%s': %s", action, clean_id, exc)
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc))

    # =========================================================================
    # 6. GET /schedules - Accepted Daily Schedules
    # =========================================================================
    @app.get("/schedules", summary="Accepted daily schedules")
    async def list_schedules(
        status_filter: Optional[str] = Query(None, alias="status", description="Filter by status (ACTIVE, SUPERSEDED)"),
        limit: int = Query(50, ge=1, le=200, description="Max results"),
    ):
        """Retrieves list of accepted daily schedules persisted in SQLite."""
        repo = await get_repo_instance()
        schedules = await repo.list_schedules(status=status_filter)
        return {
            "total": len(schedules),
            "schedules": schedules[:limit],
        }

    # =========================================================================
    # 7. GET /schedules/{id} - Schedule Information
    # =========================================================================
    @app.get("/schedules/{schedule_id}", summary="Schedule information and action summary")
    async def get_schedule(schedule_id: str):
        """Retrieves metadata and action execution breakdown for a given schedule ID."""
        repo = await get_repo_instance()
        sched = await repo.get_schedule(schedule_id)
        if not sched:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Schedule '{schedule_id}' not found",
            )

        actions = await repo.get_scheduled_actions(schedule_id)
        status_counts: Dict[str, int] = {}
        for a in actions:
            st = a.get("status", "PENDING")
            status_counts[st] = status_counts.get(st, 0) + 1

        return {
            "schedule": sched,
            "summary": {
                "total_actions": len(actions),
                "by_status": status_counts,
            },
            "actions": actions,
        }

    # =========================================================================
    # 8. GET /actions - Pending/Completed Actions
    # =========================================================================
    @app.get("/actions", summary="Pending and completed actions")
    async def list_actions(
        schedule_id: Optional[str] = Query(None, description="Filter by schedule_id"),
        status_filter: Optional[str] = Query(None, alias="status", description="Filter by status (PENDING, CLAIMED, COMPLETED, FAILED, SUPERSEDED, SKIPPED_UNSAFE)"),
        device_id: Optional[str] = Query(None, description="Filter by iot_node_id"),
        limit: int = Query(100, ge=1, le=500, description="Max results"),
    ):
        """Queries scheduled actions across daily schedules with filtering."""
        repo = await get_repo_instance()
        actions = await repo.get_actions(
            schedule_id=schedule_id,
            status=status_filter,
            device_id=device_id,
            limit=limit,
        )
        return {
            "total": len(actions),
            "actions": actions,
        }

    # =========================================================================
    # 9. GET /actions/{id} - Execution Status
    # =========================================================================
    @app.get("/actions/{action_id}", summary="Action execution status and history")
    async def get_action(action_id: str):
        """Retrieves details of a specific action and its execution attempt history."""
        repo = await get_repo_instance()
        act = await repo.get_action_by_id(action_id)
        if not act:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Action '{action_id}' not found",
            )

        executions = act.get("executions", [])
        return {
            "action": {
                "action_id": act["action_id"],
                "schedule_id": act["schedule_id"],
                "device_id": act["iot_node_id"],
                "action": act["action"],
                "channel_key": act["channel_key"],
                "params": act["params"],
                "status": act["status"],
                "scheduled_at": act["scheduled_at"],
                "executed_at": act.get("executed_at"),
            },
            "executions_count": len(executions),
            "executions": executions,
        }

    # =========================================================================
    # 10. GET /metrics - Operational Counters
    # =========================================================================
    @app.get("/metrics", summary="Operational counters and metrics")
    async def get_metrics():
        """Returns aggregate operational counters for external monitoring/dashboards."""
        repo = await get_repo_instance()
        repo_metrics = await repo.get_metrics()

        outbox_inst = app.state.outbox
        outbox_stats = {"PENDING": 0, "SENT": 0, "FAILED": 0}
        if outbox_inst:
            try:
                outbox_stats = await outbox_inst.get_stats()
            except Exception:
                pass

        load_avg = [0.0, 0.0, 0.0]
        try:
            load_avg = list(os.getloadavg())
        except Exception:
            pass

        actions_by_status = repo_metrics.get("actions_by_status", {})
        exec_by_status = repo_metrics.get("executions_by_status", {})

        return {
            "hub_id": HUB_ID,
            "uptime_seconds": round(time.time() - app.state.start_time, 2),
            "schedules_total": repo_metrics.get("schedules_total", 0),
            "actions_total": repo_metrics.get("actions_total", 0),
            "actions_pending": actions_by_status.get("PENDING", 0),
            "actions_claimed": actions_by_status.get("CLAIMED", 0),
            "actions_completed": actions_by_status.get("COMPLETED", 0),
            "actions_failed": actions_by_status.get("FAILED", 0),
            "actions_superseded": actions_by_status.get("SUPERSEDED", 0),
            "actions_skipped_unsafe": actions_by_status.get("SKIPPED_UNSAFE", 0),
            "executions_total": repo_metrics.get("executions_total", 0),
            "executions_successful": exec_by_status.get("SUCCESS", 0),
            "executions_failed": exec_by_status.get("FAILED", 0),
            "outbox_pending": outbox_stats.get("PENDING", 0),
            "outbox_sent": outbox_stats.get("SENT", 0),
            "outbox_failed": outbox_stats.get("FAILED", 0),
            "system_load_1m": load_avg[0],
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    return app


# Default module instance bound to standard settings
app = create_app()


async def run_server(host: str = API_HOST, port: int = API_PORT, application: Optional[FastAPI] = None) -> None:
    """Runs the management API using uvicorn."""
    import uvicorn
    target_app = application or app
    config = uvicorn.Config(
        app=target_app,
        host=host,
        port=port,
        log_level="info",
        access_log=False,
    )
    server = uvicorn.Server(config)
    logger.info("Starting Management API server on http://%s:%d", host, port)
    await server.serve()


def start_background_api(
    host: str = API_HOST,
    port: int = API_PORT,
    application: Optional[FastAPI] = None,
):
    """
    Starts the management API server as a background asyncio task.
    Returns (server, task) so the caller can set server.should_exit = True
    and await the task during shutdown.
    """
    import uvicorn
    target_app = application or app
    config = uvicorn.Config(
        app=target_app,
        host=host,
        port=port,
        log_level="warning",
        access_log=False,
    )
    server = uvicorn.Server(config)
    task = asyncio.create_task(server.serve())
    logger.info("Started background Management API on http://%s:%d", host, port)
    return server, task
