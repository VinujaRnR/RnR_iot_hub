"""
Provisioning Reporter and Durable Outbox for RnR Linux IoT Hub.
File: core/provisioning_reporter.py

Implements RNHub V2 Provisioning Reporter Specification (Section 5 & 11):
- Queues configuration acknowledgments (hub.config.ack) and hardware reconciliation reports into SQLite.
- Keeps acknowledgments in a persistent local outbox (provisioning_outbox) until successfully delivered to RabbitMQ.
- Emits structured outcomes: 'applied', 'partial', 'failed', 'rejected'.
- Runs a background worker to deliver pending acknowledgments with automatic retry on network recovery.
"""

from __future__ import annotations
import asyncio
import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from storage.database import DatabaseManager

logger = logging.getLogger("iothub.provisioning_reporter")


class ProvisioningReporter:
    """
    Manages persistent queuing and guaranteed delivery of configuration
    acknowledgments and device reconciliation results to Django middleware.
    """

    def __init__(
        self,
        db: DatabaseManager,
        publisher: Optional[Any] = None,
        hub_uuid: Optional[str] = None,
        hub_id: Optional[str] = None,
        results_exchange: str = "hub_results",
    ):
        self.db = db
        self.publisher = publisher
        self.hub_uuid = hub_uuid
        self.hub_id = hub_id
        self.results_exchange = results_exchange
        self._worker_task: Optional[asyncio.Task] = None
        self._running: bool = False

    async def queue_result(
        self,
        config_version: int,
        device_results: List[Dict[str, Any]],
        status: Optional[str] = None,
        hub_uuid: Optional[str] = None,
        hub_id: Optional[str] = None,
        error_detail: Optional[str] = None,
    ) -> str:
        """
        Persists a configuration acknowledgment payload into provisioning_outbox.
        Determines overall status ('applied', 'partial', 'failed', 'rejected') if not explicitly given.
        """
        uuid_val = hub_uuid or self.hub_uuid or ""
        id_val = hub_id or self.hub_id or ""

        # Determine overall outcome if not provided
        if not status:
            if not device_results:
                status = "applied"
            else:
                statuses = [d.get("status") for d in device_results]
                if all(s == "configured" for s in statuses):
                    status = "applied"
                elif any(s == "configured" for s in statuses):
                    status = "partial"
                elif any(s in ("failed", "rejected", "revoked") for s in statuses):
                    status = "failed"
                else:
                    status = "partial"

        # Format standard devices result array
        formatted_devices: List[Dict[str, Any]] = []
        for dev in device_results:
            dev_item: Dict[str, Any] = {
                "device_uid": dev.get("device_uid") or dev.get("registration_device_id") or dev.get("mac") or "unknown",
                "status": dev.get("status", "pending_hardware_binding"),
            }
            if dev.get("port"):
                dev_item["port"] = dev["port"]
            if dev.get("error"):
                dev_item["error"] = dev["error"]
            formatted_devices.append(dev_item)

        ack_payload = {
            "message_type": "hub.config.ack",
            "hub_uuid": uuid_val,
            "hub_id": id_val,
            "config_version": config_version,
            "status": status,
            "devices": formatted_devices,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        if error_detail:
            ack_payload["error"] = error_detail

        msg_id = f"ack-{uuid.uuid4().hex[:12]}"
        routing_key = f"hub.{id_val}.config.ack" if id_val else f"hub.{uuid_val}.config.ack"
        now_iso = datetime.now(timezone.utc).isoformat()
        payload_str = json.dumps(ack_payload)

        conn = self.db.connection
        await conn.execute(
            """
            INSERT INTO provisioning_outbox (
                message_id, message_type, routing_key, payload_json, status, attempts, created_at
            ) VALUES (?, ?, ?, ?, 'PENDING', 0, ?)
            """,
            (msg_id, "hub.config.ack", routing_key, payload_str, now_iso),
        )
        await conn.commit()
        logger.info(
            "[PROVISIONING_REPORTER] Queued %s for config v%d (status='%s', %d devices)",
            msg_id,
            config_version,
            status,
            len(formatted_devices),
        )

        return msg_id

    async def flush_outbox(self) -> int:
        """
        Attempts to deliver all pending outbox messages to RabbitMQ.
        Deletes messages upon confirmed delivery. Returns count of delivered messages.
        """
        if not self.publisher or not getattr(self.publisher, "is_connected", False):
            return 0

        conn = self.db.connection
        async with conn.execute(
            """
            SELECT id, message_id, routing_key, payload_json, attempts
            FROM provisioning_outbox
            WHERE status = 'PENDING'
            ORDER BY id ASC
            LIMIT 20
            """
        ) as cur:
            pending_rows = await cur.fetchall()

        if not pending_rows:
            return 0

        delivered = 0
        for row in pending_rows:
            rec_id = row["id"]
            msg_id = row["message_id"]
            routing_key = row["routing_key"]
            payload_str = row["payload_json"]
            attempts = row["attempts"] + 1
            now_iso = datetime.now(timezone.utc).isoformat()

            try:
                payload = json.loads(payload_str)
                success = await self.publisher.publish_event(
                    routing_key=routing_key,
                    payload=payload,
                    mandatory=True,
                    message_id=msg_id,
                )
                if success:
                    await conn.execute("DELETE FROM provisioning_outbox WHERE id = ?", (rec_id,))
                    await conn.commit()
                    delivered += 1
                    logger.info("[PROVISIONING_REPORTER] Delivered %s to '%s'", msg_id, routing_key)
                else:
                    await conn.execute(
                        "UPDATE provisioning_outbox SET attempts = ?, last_attempt_at = ? WHERE id = ?",
                        (attempts, now_iso, rec_id),
                    )
                    await conn.commit()
            except Exception as exc:
                logger.warning("[PROVISIONING_REPORTER] Failed to deliver %s: %s", msg_id, exc)
                await conn.execute(
                    "UPDATE provisioning_outbox SET attempts = ?, last_attempt_at = ? WHERE id = ?",
                    (attempts, now_iso, rec_id),
                )
                await conn.commit()

        return delivered

    async def start_outbox_worker(self, interval: float = 5.0) -> None:
        """Starts background worker task to flush pending provisioning outbox messages."""
        if self._running:
            return
        self._running = True

        async def _worker_loop():
            while self._running:
                try:
                    await self.flush_outbox()
                except asyncio.CancelledError:
                    break
                except Exception as exc:
                    logger.debug("[PROVISIONING_REPORTER] Worker notice: %s", exc)
                try:
                    await asyncio.sleep(interval)
                except asyncio.CancelledError:
                    break

        self._worker_task = asyncio.create_task(_worker_loop(), name="provisioning_reporter_worker")
        logger.info("[PROVISIONING_REPORTER] Background outbox worker started (interval=%.1fs)", interval)

    async def stop_outbox_worker(self) -> None:
        """Stops the background outbox worker cleanly."""
        self._running = False
        if self._worker_task and not self._worker_task.done():
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass
            self._worker_task = None
