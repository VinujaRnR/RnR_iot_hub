import logging
from typing import Any, Dict
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from config import HUB_ID
from devices.device_manager import DeviceManager, create_default_device_manager

logger = logging.getLogger("iothub.api")

app = FastAPI(title="RnR IoT Hub API", version="1.0.0", description="Local IoT Hub Management Interface")

# Shared instances (can be injected at runtime)
device_manager: DeviceManager = create_default_device_manager()


class CommandRequest(BaseModel):
    action: str
    parameters: Dict[str, Any] = {}


@app.get("/")
async def root():
    return {
        "status": "online",
        "hub_id": HUB_ID,
        "service": "RnR IoT Hub Local Management API",
    }


@app.get("/health")
async def health():
    return {
        "status": "healthy",
        "hub_id": HUB_ID,
        "device_count": len(device_manager.list_devices()),
    }


@app.get("/devices")
async def list_devices():
    return {
        "devices": [
            {
                "device_id": dev_id,
                "name": device_manager.get_device(dev_id).name if device_manager.get_device(dev_id) else "unknown",
            }
            for dev_id in device_manager.list_devices()
        ]
    }


@app.post("/devices/{device_id}/dispatch")
async def dispatch_command(device_id: str, request: CommandRequest):
    try:
        result = await device_manager.dispatch(
            device_id=device_id,
            action=request.action,
            parameters=request.parameters,
        )
        return {"status": "success", "device_id": device_id, "result": result}
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@app.post("/schedules/validate")
async def validate_schedule_endpoint(payload: Dict[str, Any]):
    from ingestion.schedule_validator import validate_daily_schedule, ScheduleValidationError
    try:
        schedule = validate_daily_schedule(payload, expected_hub_id=HUB_ID)
        return {
            "status": "valid",
            "schedule_id": schedule.schedule_id,
            "revision": schedule.revision,
            "date": schedule.date,
            "actions_count": len(schedule.extract_all_actions()),
        }
    except ScheduleValidationError as exc:
        raise HTTPException(status_code=422, detail=str(exc))


@app.post("/schedules/normalize")
async def normalize_schedule_endpoint(payload: Dict[str, Any], policy: str = "REJECT"):
    from ingestion.schedule_normalizer import normalize_schedule, ConflictPolicy, ScheduleConflictError
    try:
        conflict_policy = ConflictPolicy(policy.upper())
    except ValueError:
        conflict_policy = ConflictPolicy.REJECT

    try:
        records = normalize_schedule(payload, conflict_policy=conflict_policy)
        return {
            "status": "success",
            "total_actions": len(records),
            "actions": [r.to_dict() for r in records],
        }
    except ScheduleConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@app.get("/schedules")
async def list_schedules_endpoint(status: str | None = None):
    from storage import DatabaseManager, ScheduleRepository
    from config import DATABASE_PATH
    db = DatabaseManager(db_path=DATABASE_PATH)
    await db.connect()
    try:
        repo = ScheduleRepository(db)
        return {"schedules": await repo.list_schedules(status=status)}
    finally:
        await db.close()


@app.get("/schedules/{schedule_id}/actions")
async def get_schedule_actions_endpoint(schedule_id: str, status: str | None = None):
    from storage import DatabaseManager, ScheduleRepository
    from config import DATABASE_PATH
    db = DatabaseManager(db_path=DATABASE_PATH)
    await db.connect()
    try:
        repo = ScheduleRepository(db)
        header = await repo.get_schedule(schedule_id)
        if not header:
            raise HTTPException(status_code=404, detail="Schedule not found")
        actions = await repo.get_scheduled_actions(schedule_id, status=status)
        return {
            "schedule": header,
            "actions_count": len(actions),
            "actions": actions,
        }
    finally:
        await db.close()


@app.post("/schedules/save")
async def save_schedule_endpoint(payload: Dict[str, Any]):
    import json
    from ingestion.schedule_validator import validate_daily_schedule, ScheduleValidationError
    from ingestion.schedule_normalizer import normalize_schedule, ScheduleConflictError
    from storage import DatabaseManager, ScheduleRepository
    from config import DATABASE_PATH, HUB_ID

    try:
        schedule = validate_daily_schedule(payload, expected_hub_id=HUB_ID)
        normalized_actions = normalize_schedule(payload)
    except (ScheduleValidationError, ScheduleConflictError) as exc:
        raise HTTPException(status_code=422, detail=str(exc))

    db = DatabaseManager(db_path=DATABASE_PATH)
    await db.connect()
    try:
        repo = ScheduleRepository(db)
        await repo.save_schedule_with_actions(
            schedule_id=schedule.schedule_id,
            date=schedule.date,
            revision=schedule.revision,
            actions=normalized_actions,
            raw_json=json.dumps(payload),
        )
        return {
            "status": "persisted",
            "schedule_id": schedule.schedule_id,
            "revision": schedule.revision,
            "actions_count": len(normalized_actions),
        }
    finally:
        await db.close()



