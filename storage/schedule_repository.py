"""
Schedule and Scheduled Action Repository for RnR Linux IoT Hub.
File: storage/schedule_repository.py

Handles atomic persistence of daily schedules and normalized actions in a single transaction.
Ensures zero partial schedules on crash or import failure.
"""

from __future__ import annotations
import hashlib
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Union, TYPE_CHECKING

if TYPE_CHECKING:
    from ingestion.schedule_normalizer import NormalizedActionRecord
from storage.database import DatabaseManager

logger = logging.getLogger("iothub.schedule_repository")

_DEVICE_COLS = "device_id, device_type, device_number, device_name"


def _device_fields(r: Any) -> Dict[str, Any]:
    """Device assignment columns of a scheduled_actions row."""
    return {
        "device_id": r["device_id"],
        "device_type": r["device_type"],
        "device_number": r["device_number"],
        "device_name": r["device_name"],
    }


def build_command_params(device_type: Any, device_number: Any) -> Dict[str, Any]:
    """Builds the command JSON 'params' from the device columns (never stored in the DB)."""
    params: Dict[str, Any] = {}
    if device_type is not None:
        params["device_type"] = device_type
    if device_number is not None:
        params["device_number"] = device_number
    return params


def _row_params(r: Any) -> Dict[str, Any]:
    return build_command_params(r["device_type"], r["device_number"])


class ScheduleRepository:
    """Repository managing schedules, scheduled_actions, and executions."""

    def __init__(self, db: DatabaseManager):
        self.db = db

    async def save_schedule(
        self,
        schedule_id: str,
        date: str,
        revision: int,
        actions: List[Union[NormalizedActionRecord, Dict[str, Any]]],
        raw_json: str = "",
        status: str = "ACTIVE",
        **kwargs: Any,
    ) -> None:
        """Alias for save_schedule_with_actions for compatibility."""
        return await self.save_schedule_with_actions(
            schedule_id=schedule_id,
            date=date,
            revision=revision,
            actions=actions,
            raw_json=raw_json,
            status=status,
        )

    async def save_schedule_with_actions(
        self,
        schedule_id: str,
        date: str,
        revision: int,
        actions: List[Union[NormalizedActionRecord, Dict[str, Any]]],
        raw_json: str = "",
        status: str = "ACTIVE",
    ) -> None:
        """
        Atomically saves a schedule and all of its associated actions in a single transaction.
        If an exception occurs or the process is interrupted midway, the entire transaction
        is rolled back, preventing partial schedule states.
        """
        now = datetime.now(timezone.utc).isoformat()
        raw_hash = hashlib.sha256(raw_json.encode("utf-8")).hexdigest() if raw_json else "nohash"

        async with self.db.transaction() as conn:
            # 1. Check existing revision if any
            async with conn.execute("SELECT revision FROM schedules WHERE schedule_id = ?", (schedule_id,)) as cur:
                existing = await cur.fetchone()
                if existing and revision < existing["revision"]:
                    raise ValueError(
                        f"Cannot save schedule '{schedule_id}' with revision {revision} "
                        f"because a newer revision ({existing['revision']}) already exists."
                    )

            # 2. Insert or update top-level schedule
            await conn.execute(
                """
                INSERT INTO schedules (schedule_id, date, revision, status, raw_json_hash, raw_json, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(schedule_id) DO UPDATE SET
                    date = excluded.date,
                    revision = excluded.revision,
                    status = excluded.status,
                    raw_json_hash = excluded.raw_json_hash,
                    raw_json = excluded.raw_json,
                    updated_at = excluded.updated_at
                """,
                (schedule_id, date, revision, status, raw_hash, raw_json, now, now),
            )

            # 3. Preserve status and execution time of existing actions (e.g. COMPLETED or EXECUTING)
            async with conn.execute(
                "SELECT action_id, status, executed_at FROM scheduled_actions WHERE schedule_id = ?",
                (schedule_id,),
            ) as cur:
                existing_actions = {r["action_id"]: dict(r) for r in await cur.fetchall()}

            await conn.execute("DELETE FROM scheduled_actions WHERE schedule_id = ?", (schedule_id,))

            # 4. Insert all actions, carrying forward completed/in-flight states for existing action IDs
            for act in actions:
                if hasattr(act, "to_dict"):
                    act_dict = act.to_dict()
                elif hasattr(act, "model_dump"):
                    act_dict = act.model_dump()
                else:
                    act_dict = dict(act)

                act_id = str(act_dict.get("action_id"))
                existing_state = existing_actions.get(act_id)
                if existing_state and existing_state["status"] in ("COMPLETED", "EXECUTING", "SKIPPED_UNSAFE", "SKIPPED_IDEMPOTENT"):
                    act_status = existing_state["status"]
                    act_executed_at = existing_state.get("executed_at")
                else:
                    act_status = str(act_dict.get("status", "PENDING"))
                    act_executed_at = act_dict.get("executed_at")

                dev_number = act_dict.get("device_number")
                try:
                    dev_number = int(dev_number) if dev_number is not None else None
                except (TypeError, ValueError):
                    dev_number = None
                dev_id = act_dict.get("device_id") or str(act_dict.get("iot_node_id"))

                await conn.execute(
                    """
                    INSERT INTO scheduled_actions (
                        schedule_id, action_id, production_order_id, resource_id, sub_resource_id,
                        iot_node_id, stage_id, scheduled_at, action, channel_key,
                        device_id, device_type, device_number, device_name, status, created_at, executed_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        schedule_id,
                        act_id,
                        str(act_dict.get("production_order_id")) if act_dict.get("production_order_id") is not None else None,
                        str(act_dict.get("resource_id")) if act_dict.get("resource_id") is not None else None,
                        str(act_dict.get("sub_resource_id")) if act_dict.get("sub_resource_id") is not None else None,
                        str(act_dict.get("iot_node_id")),
                        str(act_dict.get("stage_id")) if act_dict.get("stage_id") is not None else None,
                        str(act_dict.get("scheduled_at")),
                        str(act_dict.get("action")),
                        str(act_dict.get("channel_key")),
                        str(dev_id),
                        act_dict.get("device_type"),
                        dev_number,
                        act_dict.get("device_name"),
                        act_status,
                        now,
                        act_executed_at,
                    ),
                )

        logger.info("Persisted schedule '%s' (rev %d) with %d action(s) atomically", schedule_id, revision, len(actions))

    async def get_schedule(self, schedule_id: str) -> Optional[Dict[str, Any]]:
        """Retrieves schedule header record."""
        async with self.db.connection.execute(
            "SELECT schedule_id, date, revision, status, raw_json_hash, raw_json, created_at, updated_at FROM schedules WHERE schedule_id = ?",
            (schedule_id,),
        ) as cur:
            row = await cur.fetchone()
            if row:
                return dict(row)
            return None

    async def get_scheduled_actions(
        self,
        schedule_id: str,
        status: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Retrieves all normalized device actions for a schedule with their current states."""
        query = (
            "SELECT id, schedule_id, action_id, production_order_id, resource_id, sub_resource_id, "
            "iot_node_id, stage_id, scheduled_at, action, channel_key, device_id, device_type, device_number, device_name, status, created_at, executed_at "
            "FROM scheduled_actions WHERE schedule_id = ?"
        )
        params: List[Any] = [schedule_id]
        if status:
            query += " AND status = ?"
            params.append(status)
        query += " ORDER BY scheduled_at ASC, id ASC"

        async with self.db.connection.execute(query, params) as cur:
            rows = await cur.fetchall()
            return [
                {
                    "id": r["id"],
                    "schedule_id": r["schedule_id"],
                    "action_id": r["action_id"],
                    "production_order_id": r["production_order_id"],
                    "resource_id": r["resource_id"],
                    "sub_resource_id": r["sub_resource_id"],
                    "iot_node_id": r["iot_node_id"],
                    "stage_id": r["stage_id"],
                    "scheduled_at": r["scheduled_at"],
                    "action": r["action"],
                    "channel_key": r["channel_key"],
                    **_device_fields(r),
                    "params": _row_params(r),
                    "status": r["status"],
                    "created_at": r["created_at"],
                    "executed_at": r["executed_at"],
                }
                for r in rows
            ]

    async def update_action_status(
        self,
        schedule_id: str,
        action_id: str,
        status: str,
        executed_at: Optional[str] = None,
    ) -> None:
        """Updates the lifecycle state of a scheduled action."""
        now = executed_at or (datetime.now(timezone.utc).isoformat() if status in ("COMPLETED", "FAILED") else None)
        await self.db.connection.execute(
            """
            UPDATE scheduled_actions
            SET status = ?, executed_at = COALESCE(?, executed_at)
            WHERE schedule_id = ? AND action_id = ?
            """,
            (status, now, schedule_id, action_id),
        )
        await self.db.connection.commit()

    async def list_schedules(self, status: Optional[str] = None) -> List[Dict[str, Any]]:
        """Lists all stored schedules."""
        query = "SELECT schedule_id, date, revision, status, raw_json_hash, created_at, updated_at FROM schedules"
        params: List[Any] = []
        if status:
            query += " WHERE status = ?"
            params.append(status)
        query += " ORDER BY date DESC, revision DESC"

        async with self.db.connection.execute(query, params) as cur:
            rows = await cur.fetchall()
            return [dict(r) for r in rows]

    async def get_pending_actions_due(self, due_before_iso: str) -> List[Dict[str, Any]]:
        """Finds all actions awaiting execution whose scheduled_at timestamp has passed."""
        query = (
            "SELECT id, schedule_id, action_id, iot_node_id, action, channel_key, device_id, device_type, device_number, device_name, scheduled_at, status "
            "FROM scheduled_actions "
            "WHERE status = 'PENDING' AND scheduled_at <= ? "
            "ORDER BY scheduled_at ASC"
        )
        async with self.db.connection.execute(query, (due_before_iso,)) as cur:
            rows = await cur.fetchall()
            return [
                {
                    "id": r["id"],
                    "schedule_id": r["schedule_id"],
                    "action_id": r["action_id"],
                    "iot_node_id": r["iot_node_id"],
                    "action": r["action"],
                    "channel_key": r["channel_key"],
                    **_device_fields(r),
                    "params": _row_params(r),
                    "scheduled_at": r["scheduled_at"],
                    "status": r["status"],
                }
                for r in rows
            ]

    async def claim_due_actions(self, now_iso: str, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Atomically claims due actions in SQLite using a BEGIN IMMEDIATE transaction.
        Changes status from 'PENDING' to 'CLAIMED' atomically to guarantee no duplicate execution.
        """
        try:
            now_dt = datetime.fromisoformat(now_iso).astimezone(timezone.utc)
        except Exception:
            now_dt = datetime.now(timezone.utc)

        async with self.db.transaction() as conn:
            cur = await conn.execute(
                "SELECT id, schedule_id, action_id, iot_node_id, action, channel_key, device_id, device_type, device_number, device_name, scheduled_at, status "
                "FROM scheduled_actions WHERE status = 'PENDING' ORDER BY scheduled_at ASC LIMIT ?",
                (limit * 2,),
            )
            rows = await cur.fetchall()

            claimed_rows = []
            for r in rows:
                sched_str = r["scheduled_at"]
                try:
                    sched_dt = datetime.fromisoformat(sched_str).astimezone(timezone.utc)
                except Exception:
                    continue
                if sched_dt <= now_dt:
                    claimed_rows.append(r)
                if len(claimed_rows) >= limit:
                    break

            if not claimed_rows:
                return []

            ids = [r["id"] for r in claimed_rows]
            placeholders = ",".join("?" for _ in ids)
            await conn.execute(
                f"UPDATE scheduled_actions SET status = 'CLAIMED' WHERE id IN ({placeholders})",
                ids,
            )

            return [
                {
                    "id": r["id"],
                    "schedule_id": r["schedule_id"],
                    "action_id": r["action_id"],
                    "iot_node_id": r["iot_node_id"],
                    "action": r["action"],
                    "channel_key": r["channel_key"],
                    **_device_fields(r),
                    "params": _row_params(r),
                    "scheduled_at": r["scheduled_at"],
                    "status": "CLAIMED",
                }
                for r in claimed_rows
            ]

    async def get_due_actions(self, now_iso: str) -> List[Dict[str, Any]]:
        """Alias for claim_due_actions satisfying Step 11 specification."""
        return await self.claim_due_actions(now_iso)

    async def get_next_due_timestamp(self) -> Optional[str]:
        """Returns earliest scheduled_at for PENDING actions."""
        async with self.db.connection.execute(
            "SELECT scheduled_at FROM scheduled_actions WHERE status = 'PENDING' ORDER BY scheduled_at ASC LIMIT 1"
        ) as cur:
            row = await cur.fetchone()
            return row[0] if row else None

    async def record_execution(
        self,
        schedule_id: Optional[str],
        action_id: Optional[str],
        device_id: str,
        command_action: str,
        attempt: int = 1,
        status: str = "SUCCESS",
        result: Optional[Dict[str, Any]] = None,
        error_message: Optional[str] = None,
    ) -> int:
        """Records command attempts, acknowledgments, and execution errors."""
        now = datetime.now(timezone.utc).isoformat()
        result_json = json.dumps(result) if result is not None else None

        async with self.db.transaction() as conn:
            cur = await conn.execute(
                """
                INSERT INTO executions (
                    action_id, schedule_id, device_id, command_action, attempt, status, result_json, error_message, executed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (action_id, schedule_id, device_id, command_action, attempt, status, result_json, error_message, now),
            )
            exec_id = cur.lastrowid or 0

            # Update scheduled_action status if associated
            if schedule_id and action_id:
                action_status = "COMPLETED" if status == "SUCCESS" else "FAILED"
                await conn.execute(
                    "UPDATE scheduled_actions SET status = ?, executed_at = ? WHERE schedule_id = ? AND action_id = ?",
                    (action_status, now, schedule_id, action_id),
                )

        return exec_id

    async def get_executions(
        self,
        schedule_id: Optional[str] = None,
        device_id: Optional[str] = None,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """Retrieves command execution audit log."""
        query = (
            "SELECT id, action_id, schedule_id, device_id, command_action, attempt, status, "
            "result_json, error_message, executed_at FROM executions WHERE 1=1"
        )
        params: List[Any] = []
        if schedule_id:
            query += " AND schedule_id = ?"
            params.append(schedule_id)
        if device_id:
            query += " AND device_id = ?"
            params.append(device_id)
        query += " ORDER BY id DESC LIMIT ?"
        params.append(limit)

        async with self.db.connection.execute(query, params) as cur:
            rows = await cur.fetchall()
            return [
                {
                    "id": r["id"],
                    "action_id": r["action_id"],
                    "schedule_id": r["schedule_id"],
                    "device_id": r["device_id"],
                    "command_action": r["command_action"],
                    "attempt": r["attempt"],
                    "status": r["status"],
                    "result": json.loads(r["result_json"]) if r["result_json"] else None,
                    "error_message": r["error_message"],
                    "executed_at": r["executed_at"],
                }
                for r in rows
            ]

    async def get_actions(
        self,
        schedule_id: Optional[str] = None,
        status: Optional[str] = None,
        device_id: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """Queries scheduled actions across schedules with optional filters."""
        query = (
            "SELECT id, schedule_id, action_id, production_order_id, resource_id, sub_resource_id, "
            "iot_node_id, stage_id, scheduled_at, action, channel_key, device_id, device_type, device_number, device_name, status, created_at, executed_at "
            "FROM scheduled_actions WHERE 1=1"
        )
        params: List[Any] = []
        if schedule_id:
            query += " AND schedule_id = ?"
            params.append(schedule_id)
        if status:
            query += " AND status = ?"
            params.append(status)
        if device_id:
            query += " AND iot_node_id = ?"
            params.append(device_id)
        query += " ORDER BY scheduled_at ASC, id ASC LIMIT ?"
        params.append(limit)

        async with self.db.connection.execute(query, params) as cur:
            rows = await cur.fetchall()
            return [
                {
                    "id": r["id"],
                    "schedule_id": r["schedule_id"],
                    "action_id": r["action_id"],
                    "production_order_id": r["production_order_id"],
                    "resource_id": r["resource_id"],
                    "sub_resource_id": r["sub_resource_id"],
                    "iot_node_id": r["iot_node_id"],
                    "stage_id": r["stage_id"],
                    "scheduled_at": r["scheduled_at"],
                    "action": r["action"],
                    "channel_key": r["channel_key"],
                    **_device_fields(r),
                    "params": _row_params(r),
                    "status": r["status"],
                    "created_at": r["created_at"],
                    "executed_at": r["executed_at"],
                }
                for r in rows
            ]

    async def get_action_by_id(self, action_id: str) -> Optional[Dict[str, Any]]:
        """Retrieves a single scheduled action by action_id or numeric PK, including executions."""
        query = (
            "SELECT id, schedule_id, action_id, production_order_id, resource_id, sub_resource_id, "
            "iot_node_id, stage_id, scheduled_at, action, channel_key, device_id, device_type, device_number, device_name, status, created_at, executed_at "
            "FROM scheduled_actions WHERE action_id = ? OR id = ?"
        )
        async with self.db.connection.execute(query, (action_id, action_id)) as cur:
            r = await cur.fetchone()
            if not r:
                return None
            action_data = {
                "id": r["id"],
                "schedule_id": r["schedule_id"],
                "action_id": r["action_id"],
                "production_order_id": r["production_order_id"],
                "resource_id": r["resource_id"],
                "sub_resource_id": r["sub_resource_id"],
                "iot_node_id": r["iot_node_id"],
                "stage_id": r["stage_id"],
                "scheduled_at": r["scheduled_at"],
                "action": r["action"],
                "channel_key": r["channel_key"],
                **_device_fields(r),
                "params": _row_params(r),
                "status": r["status"],
                "created_at": r["created_at"],
                "executed_at": r["executed_at"],
            }

        # Query executions history for this action
        exec_query = (
            "SELECT id, action_id, schedule_id, device_id, command_action, attempt, status, "
            "result_json, error_message, executed_at FROM executions WHERE action_id = ? ORDER BY id ASC"
        )
        async with self.db.connection.execute(exec_query, (action_data["action_id"],)) as cur:
            exec_rows = await cur.fetchall()
            action_data["executions"] = [
                {
                    "id": er["id"],
                    "attempt": er["attempt"],
                    "status": er["status"],
                    "result": json.loads(er["result_json"]) if er["result_json"] else None,
                    "error_message": er["error_message"],
                    "executed_at": er["executed_at"],
                }
                for er in exec_rows
            ]

        return action_data

    async def get_metrics(self) -> Dict[str, Any]:
        """Calculates system metrics and operational counters."""
        async with self.db.connection.execute("SELECT COUNT(*) FROM schedules") as cur:
            row = await cur.fetchone()
            schedules_total = row[0] if row else 0

        action_counts: Dict[str, int] = {}
        async with self.db.connection.execute("SELECT status, COUNT(*) FROM scheduled_actions GROUP BY status") as cur:
            rows = await cur.fetchall()
            for r in rows:
                action_counts[r[0]] = r[1]

        exec_counts: Dict[str, int] = {}
        async with self.db.connection.execute("SELECT status, COUNT(*) FROM executions GROUP BY status") as cur:
            rows = await cur.fetchall()
            for r in rows:
                exec_counts[r[0]] = r[1]

        return {
            "schedules_total": schedules_total,
            "actions_total": sum(action_counts.values()),
            "actions_by_status": action_counts,
            "executions_total": sum(exec_counts.values()),
            "executions_by_status": exec_counts,
        }
