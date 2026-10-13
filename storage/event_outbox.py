"""
Durable Event Outbox for RnR Linux IoT Hub.
File: storage/event_outbox.py

Implements Step 14 specification:
- At-least-once delivery of execution results and domain events to RabbitMQ.
- Guarantees transactional consistency: stores execution result and outgoing event in ONE database transaction.
- Stable event ID on every event for backend deduplication and idempotency.
- Retains events locally if RabbitMQ is disconnected; delivers upon reconnection.
"""

from __future__ import annotations
import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import aiosqlite

from config import HUB_ID, ACTION_RESULT_KEY
from storage.database import DatabaseManager

logger = logging.getLogger("iothub.event_outbox")


class EventOutbox:
    """Manages transactional event persistence, delivery tracking, and retry state for RabbitMQ outbox."""

    def __init__(self, db: DatabaseManager):
        self.db = db

    async def enqueue(
        self,
        event_type: str,
        payload: Dict[str, Any],
        routing_key: Optional[str] = None,
        event_id: Optional[str] = None,
    ) -> int:
        """Persists a new event awaiting RabbitMQ dispatch with a stable event ID."""
        async with self.db.transaction() as conn:
            return await self.enqueue_tx(
                conn=conn,
                event_type=event_type,
                payload=payload,
                routing_key=routing_key,
                event_id=event_id,
            )

    # Alias for compatibility
    enqueue_event = enqueue

    async def enqueue_tx(
        self,
        conn: aiosqlite.Connection,
        event_type: str,
        payload: Dict[str, Any],
        routing_key: Optional[str] = None,
        event_id: Optional[str] = None,
    ) -> int:
        """
        Persists a new event within an existing database transaction.
        Guarantees atomicity when called alongside execution or schedule updates.
        """
        now = datetime.now(timezone.utc).isoformat()
        stable_event_id = event_id or payload.get("event_id") or str(uuid.uuid4())
        
        # Ensure event_id is embedded in payload for downstream deduplication
        payload["event_id"] = stable_event_id
        payload_json = json.dumps(payload)

        cur = await conn.execute(
            """
            INSERT INTO event_outbox (event_id, event_type, routing_key, payload_json, status, retry_count, created_at)
            VALUES (?, ?, ?, ?, 'PENDING', 0, ?)
            """,
            (stable_event_id, event_type, routing_key, payload_json, now),
        )
        outbox_id = cur.lastrowid or 0
        logger.debug("Enqueued outbox event #%d (event_id=%s, type=%s)", outbox_id, stable_event_id, event_type)
        return outbox_id

    async def record_execution_and_event(
        self,
        schedule_id: Optional[str],
        action_id: str,
        device_id: str,
        action: str,
        status: str,
        scheduled_at: Optional[str] = None,
        executed_at: Optional[str] = None,
        attempt: int = 1,
        result: Optional[Dict[str, Any]] = None,
        error_message: Optional[str] = None,
        routing_key: Optional[str] = None,
        event_id: Optional[str] = None,
        event_type: str = "action_result",
    ) -> Tuple[int, int, Dict[str, Any]]:
        """
        Step 14: Stores the execution result and outgoing event in ONE database transaction.
        
        Atomically:
        1. Inserts execution record into 'executions'.
        2. Updates 'scheduled_actions' status (COMPLETED or FAILED).
        3. Enqueues event into 'event_outbox' with a stable event ID.
        
        Returns:
            (exec_id, outbox_id, event_payload)
        """
        now = executed_at or datetime.now(timezone.utc).isoformat()
        stable_event_id = event_id or str(uuid.uuid4())
        key = routing_key or ACTION_RESULT_KEY
        result_json = json.dumps(result) if result is not None else None

        # Build canonical Step 14 execution result event payload
        payload = {
            "event_id": stable_event_id,
            "event_type": event_type,
            "hub_id": HUB_ID,
            "action_id": action_id,
            "device_id": device_id,
            "action": action,
            "status": status,
            "scheduled_at": scheduled_at or now,
            "executed_at": now,
        }
        if result is not None:
            payload["result"] = result
        if error_message:
            payload["error"] = error_message
        if schedule_id:
            payload["schedule_id"] = schedule_id

        async with self.db.transaction() as conn:
            # 1. Insert into executions table
            cur_exec = await conn.execute(
                """
                INSERT INTO executions (
                    action_id, schedule_id, device_id, command_action, attempt, status, result_json, error_message, executed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (action_id, schedule_id, device_id, action, attempt, status, result_json, error_message, now),
            )
            exec_id = cur_exec.lastrowid or 0

            # 2. Update scheduled_actions table if linked
            if schedule_id and action_id:
                action_status = "COMPLETED" if status.upper() == "SUCCESS" else "FAILED"
                await conn.execute(
                    "UPDATE scheduled_actions SET status = ?, executed_at = ? WHERE schedule_id = ? AND action_id = ?",
                    (action_status, now, schedule_id, action_id),
                )

            # 3. Enqueue into event_outbox in the same transaction
            outbox_id = await self.enqueue_tx(
                conn=conn,
                event_type=event_type,
                payload=payload,
                routing_key=key,
                event_id=stable_event_id,
            )

        logger.info(
            "Atomically recorded execution #%d and outbox event #%d (event_id=%s, action=%s, status=%s)",
            exec_id,
            outbox_id,
            stable_event_id,
            action,
            status,
        )
        return exec_id, outbox_id, payload

    async def get_pending(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Retrieves pending events for dispatch to broker."""
        async with self.db.connection.execute(
            """
            SELECT id, event_id, event_type, routing_key, payload_json, status, retry_count, created_at
            FROM event_outbox
            WHERE status = 'PENDING'
            ORDER BY id ASC
            LIMIT ?
            """,
            (limit,),
        ) as cur:
            rows = await cur.fetchall()
            return [
                {
                    "id": r["id"],
                    "event_id": r["event_id"],
                    "event_type": r["event_type"],
                    "routing_key": r["routing_key"],
                    "payload": json.loads(r["payload_json"]),
                    "status": r["status"],
                    "retry_count": r["retry_count"],
                    "created_at": r["created_at"],
                }
                for r in rows
            ]

    async def get_event(self, event_id: Any) -> Optional[Dict[str, Any]]:
        """Retrieves an outbox event by numeric ID or string event_id."""
        query = (
            "SELECT id, event_id, event_type, routing_key, payload_json, status, retry_count, created_at, processed_at "
            "FROM event_outbox WHERE id = ? OR event_id = ?"
        )
        async with self.db.connection.execute(query, (event_id, str(event_id))) as cur:
            r = await cur.fetchone()
            if not r:
                return None
            return {
                "id": r["id"],
                "event_id": r["event_id"],
                "event_type": r["event_type"],
                "routing_key": r["routing_key"],
                "payload": json.loads(r["payload_json"]),
                "status": r["status"],
                "retry_count": r["retry_count"],
                "created_at": r["created_at"],
                "processed_at": r["processed_at"],
            }

    async def mark_sent(self, event_id: int) -> None:
        """Marks an event as successfully delivered to RabbitMQ."""
        now = datetime.now(timezone.utc).isoformat()
        await self.db.connection.execute(
            "UPDATE event_outbox SET status = 'SENT', processed_at = ? WHERE id = ? OR event_id = ?",
            (now, event_id, str(event_id)),
        )
        await self.db.connection.commit()
        logger.debug("Marked outbox event #%s as SENT", event_id)

    async def mark_published(self, event_id: int) -> None:
        """Alias for mark_sent for published event acknowledgments."""
        await self.mark_sent(event_id)

    async def mark_failed(self, event_id: int, max_retries: int = 5) -> None:
        """Increments retry count; marks as FAILED if max retries exceeded."""
        async with self.db.transaction() as conn:
            async with conn.execute(
                "SELECT id, retry_count FROM event_outbox WHERE id = ? OR event_id = ?",
                (event_id, str(event_id)),
            ) as cur:
                row = await cur.fetchone()
                if not row:
                    return
                numeric_id = row["id"]
                current_retries = row["retry_count"] + 1

            new_status = "FAILED" if current_retries >= max_retries else "PENDING"
            await conn.execute(
                "UPDATE event_outbox SET retry_count = ?, status = ? WHERE id = ?",
                (current_retries, new_status, numeric_id),
            )

        logger.warning("Outbox event #%s failed (attempt %d, status=%s)", event_id, current_retries, new_status)

    async def get_stats(self) -> Dict[str, int]:
        """Returns counts of pending, sent, and failed outbox events."""
        async with self.db.connection.execute(
            "SELECT status, COUNT(*) as count FROM event_outbox GROUP BY status"
        ) as cur:
            rows = await cur.fetchall()
            stats = {"PENDING": 0, "SENT": 0, "FAILED": 0}
            for r in rows:
                stats[r["status"]] = r["count"]
            return stats

    async def prune_sent(self, limit: int = 1000) -> int:
        """Deletes processed events to prevent outbox table bloat."""
        cur = await self.db.connection.execute(
            "DELETE FROM event_outbox WHERE id IN (SELECT id FROM event_outbox WHERE status = 'SENT' LIMIT ?)",
            (limit,),
        )
        await self.db.connection.commit()
        return cur.rowcount
