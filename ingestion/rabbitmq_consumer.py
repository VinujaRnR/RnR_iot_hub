"""
RabbitMQ Consumer with Manual Acknowledgments and Responsibility Transfer.
File: ingestion/rabbitmq_consumer.py

Follows the ingestion pipeline:
1. Receive schedule from hub queue
2. Validate JSON structure & schema (reject/dead-letter if invalid)
3. Check schedule ID and revision
4. Normalize device actions from sub_resources[].actions
5. Atomically commit schedule & actions to SQLite
6. Manually acknowledge RabbitMQ message
7. Publish schedule acceptance event with publisher confirmations
"""

from __future__ import annotations
import asyncio
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, Optional, TYPE_CHECKING
import aio_pika

from config import (
    HUB_ID,
    RABBITMQ_URL,
    COMMAND_EXCHANGE,
    QUEUE_NAME,
    COMMAND_ROUTING_KEY,
    DEADLETTER_EXCHANGE,
    PREFETCH,
)
from ingestion.schemas import DailyProductionSchedule
from ingestion.schedule_validator import (
    ScheduleValidator,
    ScheduleValidationError,
    MalformedScheduleError,
    ScheduleRevisionError,
)
from ingestion.schedule_normalizer import (
    ScheduleNormalizer,
    ScheduleConflictError,
    ConflictPolicy,
)
from ingestion.rabbitmq_publisher import RabbitMQPublisher
from storage.schedule_repository import ScheduleRepository
from storage.database import DatabaseManager

if TYPE_CHECKING:
    from core.router import CommandRouter

logger = logging.getLogger("iothub.rabbitmq_consumer")


class RabbitMQConsumer:
    """
    Robust RabbitMQ Consumer implementing manual acknowledgments,
    dead-letter routing, and atomic persistence before ACK.
    """

    def __init__(
        self,
        url: str = RABBITMQ_URL,
        queue_name: str = QUEUE_NAME,
        exchange_name: str = COMMAND_EXCHANGE,
        routing_key: str = COMMAND_ROUTING_KEY,
        deadletter_exchange: str = DEADLETTER_EXCHANGE,
        prefetch_count: int = PREFETCH,
        hub_id: str = HUB_ID,
        router: Optional[CommandRouter] = None,
        db: Optional[DatabaseManager] = None,
        schedule_repo: Optional[ScheduleRepository] = None,
        publisher: Optional[RabbitMQPublisher] = None,
        schedule_engine: Optional[Any] = None,
    ):
        self.url = url
        self.queue_name = queue_name
        self.exchange_name = exchange_name
        self.routing_key = routing_key
        self.deadletter_exchange = deadletter_exchange
        self.prefetch_count = prefetch_count
        self.hub_id = hub_id

        self.router = router
        self.db = db
        self.schedule_repo = schedule_repo or (ScheduleRepository(db) if db else None)
        self.publisher = publisher
        self.schedule_engine = schedule_engine

        # Validation & Normalization engines
        self.validator = ScheduleValidator(expected_hub_id=self.hub_id)
        self.normalizer = ScheduleNormalizer(default_hub_id=self.hub_id, conflict_policy=ConflictPolicy.PRIORITY_SAFETY)

        self.connection: Optional[aio_pika.abc.AbstractRobustConnection] = None
        self.channel: Optional[aio_pika.abc.AbstractRobustChannel] = None
        self._consumer_tag: Optional[str] = None

    async def _safe_ack(self, message: Any) -> None:
        """Safely acknowledges an incoming message."""
        if hasattr(message, "ack") and callable(message.ack):
            res = message.ack()
            if asyncio.iscoroutine(res):
                await res
        if hasattr(message, "acked"):
            message.acked = True

    async def _safe_reject(self, message: Any, requeue: bool = False) -> None:
        """Safely rejects an incoming message without requeue (routing to dead-letter)."""
        if hasattr(message, "reject") and callable(message.reject):
            res = message.reject(requeue=requeue)
            if asyncio.iscoroutine(res):
                await res
        if hasattr(message, "rejected"):
            message.rejected = True

    async def _safe_nack(self, message: Any, requeue: bool = True) -> None:
        """Safely nacks an incoming message with requeue."""
        if hasattr(message, "nack") and callable(message.nack):
            res = message.nack(requeue=requeue)
            if asyncio.iscoroutine(res):
                await res
        if hasattr(message, "rejected"):
            message.rejected = True

    async def connect(self) -> None:
        """Declares topology (Command Exchange, Queue, Dead-Letter Exchange) and starts consuming."""
        logger.info("Connecting to RabbitMQ broker at %s...", self.url)
        self.connection = await aio_pika.connect_robust(self.url)
        self.channel = await self.connection.channel()
        await self.channel.set_qos(prefetch_count=self.prefetch_count)

        # 1. Declare Dead-Letter Exchange and Dead-Letter Queue
        dlx = await self.channel.declare_exchange(
            self.deadletter_exchange,
            aio_pika.ExchangeType.TOPIC,
            durable=True,
        )
        dl_queue = await self.channel.declare_queue(f"{self.queue_name}.deadletter", durable=True)
        await dl_queue.bind(dlx, routing_key="#")

        # 2. Declare Inbound Command Exchange
        exchange = await self.channel.declare_exchange(
            self.exchange_name,
            aio_pika.ExchangeType.TOPIC,
            durable=True,
        )

        # 3. Declare Inbound Queue with Dead-Letter configuration
        try:
            queue = await self.channel.declare_queue(
                self.queue_name,
                durable=True,
                arguments={
                    "x-dead-letter-exchange": self.deadletter_exchange,
                    "x-dead-letter-routing-key": f"deadletter.{self.queue_name}",
                },
            )
        except Exception as exc:
            # If the queue already exists on the broker with different arguments (e.g. without x-dead-letter-exchange),
            # RabbitMQ closes the channel with PRECONDITION_FAILED (406).
            # Catch this, reopen channel, re-declare exchange, and attach passively without argument mismatch.
            if "PRECONDITION_FAILED" in str(exc) or "inequivalent arg" in str(exc):
                logger.warning(
                    "Queue '%s' exists on broker with different arguments (%s). Reopening channel and attaching passively...",
                    self.queue_name,
                    exc,
                )
                self.channel = await self.connection.channel()
                await self.channel.set_qos(prefetch_count=self.prefetch_count)
                exchange = await self.channel.declare_exchange(
                    self.exchange_name,
                    aio_pika.ExchangeType.TOPIC,
                    durable=True,
                )
                queue = await self.channel.declare_queue(self.queue_name, passive=True)
            else:
                raise

        # Bind to command routing keys:
        # e.g. "hub.hub-01", "hub.hub-01.#", "iot.command", "iot.command.#"
        bindings_to_add = [
            self.routing_key,
            f"{self.routing_key}.#" if not self.routing_key.endswith("#") else None,
            "iot.command",
            "iot.command.#",
            f"hub.{self.hub_id}",
            f"hub.{self.hub_id}.#",
            f"hub.{self.hub_id}.commands",
        ]
        for rk in set(filter(None, bindings_to_add)):
            try:
                await queue.bind(exchange, routing_key=rk)
            except Exception as bind_err:
                logger.debug("Binding notice for '%s': %s", rk, bind_err)

        logger.info(
            "Topology declared: Queue '%s' bound to Exchange '%s' (Routing key: '%s'). Dead-letter: '%s'",
            self.queue_name,
            self.exchange_name,
            self.routing_key,
            self.deadletter_exchange,
        )

        self._consumer_tag = await queue.consume(self._on_message, no_ack=False)

    async def start(self) -> None:
        """Alias for connect to maintain interface consistency."""
        await self.connect()

    async def stop(self) -> None:
        """Stops consumer channel and closes connections."""
        if self.channel and not self.channel.is_closed:
            await self.channel.close()
        if self.connection and not self.connection.is_closed:
            await self.connection.close()
        logger.info("RabbitMQ consumer stopped.")

    async def _on_message(self, message: aio_pika.abc.AbstractIncomingMessage) -> None:
        """
        Processes inbound schedule/command messages with manual acknowledgments.
        Follows the strict responsibility-transfer model:
        1. Validate JSON
        2. Check revision
        3. Normalize actions
        4. Commit to SQLite
        5. Acknowledge RabbitMQ (manual ACK)
        6. Publish acceptance event
        """
        raw_body = ""
        try:
            raw_body = message.body.decode("utf-8")
        except Exception as exc:
            logger.error("Failed to decode message bytes: %s. Rejecting to dead-letter...", exc)
            await self._safe_reject(message, requeue=False)
            return

        try:
            payload = json.loads(raw_body)
        except json.JSONDecodeError as exc:
            snippet = raw_body[:100]
            logger.error("Malformed JSON: %s | Payload snippet: %s. Rejecting to dead-letter...", exc, snippet)
            if self.db:
                await self.db.record_alert("ERROR", "consumer", f"Malformed JSON: {exc}", {"raw": snippet})
            # Reject without requeue -> routed to dead-letter exchange
            await self._safe_reject(message, requeue=False)
            return

        # Check if message is a production schedule (contains schedules, production_orders, or schedule_id)
        is_schedule = (
            "production_orders" in payload
            or "schedule_id" in payload
            or ("schedules" in payload and any("time" in s or "sub_resources" in s for s in payload["schedules"]))
        )

        rk = getattr(message, "routing_key", "")
        detected_type = "SCHEDULE" if is_schedule else "COMMAND_BATCH"
        log_snippet = raw_body if len(raw_body) <= 1500 else raw_body[:1500] + "... [truncated]"
        logger.info(
            "[RABBITMQ INBOUND] Received message on routing_key='%s' (type=%s, bytes=%d):\n%s",
            rk,
            detected_type,
            len(raw_body),
            log_snippet,
        )

        # Persist last inbound message for inspection via iothubctl rabbitmq last
        try:
            meta = {
                "received_at": datetime.now(timezone.utc).isoformat(),
                "routing_key": rk,
                "detected_type": detected_type,
                "raw_body": raw_body,
                "payload": payload,
            }
            meta_json = json.dumps(meta, indent=2)
            for path in ("/var/lib/iothub/last_rabbitmq_message.json", "/tmp/last_rabbitmq_message.json"):
                try:
                    os.makedirs(os.path.dirname(path), exist_ok=True)
                    with open(path, "w", encoding="utf-8") as f:
                        f.write(meta_json)
                    try:
                        os.chmod(path, 0o666)
                    except Exception:
                        pass
                    break
                except Exception:
                    continue
        except Exception as exc:
            logger.warning("Could not record last RabbitMQ message: %s", exc)

        if is_schedule:
            await self._process_schedule_document(message, payload, raw_body)
        else:
            await self._process_command_batch(message, payload, raw_body)

    async def _process_schedule_document(
        self,
        message: aio_pika.abc.AbstractIncomingMessage,
        payload: Dict[str, Any],
        raw_body: str,
    ) -> None:
        """Executes the daily production schedule ingestion workflow."""
        schedule_id = payload.get("schedule_id", "unknown")
        revision = payload.get("revision", 1)

        # Step 3: Validate JSON
        try:
            validated_schedule = self.validator.validate(payload, check_revision=True)
        except ScheduleRevisionError as re:
            logger.warning("Duplicate/stale schedule revision ignored: %s", re)
            # Acknowledge to remove redundant message from queue
            await self._safe_ack(message)
            return
        except ScheduleValidationError as ve:
            logger.error("Schedule validation failed for '%s': %s. Rejecting...", schedule_id, ve)
            if self.db:
                await self.db.record_alert("WARNING", "consumer", f"Invalid schedule: {ve}", {"schedule_id": schedule_id})
            await self._safe_reject(message, requeue=False)
            return
        except Exception as exc:
            logger.error("Unexpected error validating schedule: %s. Rejecting...", exc)
            if self.db:
                await self.db.record_alert("ERROR", "consumer", f"Validation exception: {exc}", {"schedule_id": schedule_id})
            await self._safe_reject(message, requeue=False)
            return

        # Check SQLite repository for existing revision (persists across restarts)
        if self.schedule_repo:
            try:
                existing = await self.schedule_repo.get_schedule(validated_schedule.schedule_id)
                if existing and validated_schedule.revision <= existing["revision"]:
                    logger.warning(
                        "Schedule '%s' revision %d already persisted in database (stored revision: %d). Skipping duplicate.",
                        validated_schedule.schedule_id,
                        validated_schedule.revision,
                        existing["revision"],
                    )
                    await self._safe_ack(message)
                    return
            except Exception as exc:
                logger.error("Error checking existing schedule in SQLite: %s", exc)

        # Step 4 & 5: Normalize device actions
        try:
            normalized_actions = self.normalizer.normalize(payload)
        except ScheduleConflictError as ce:
            logger.error("Schedule conflict for '%s': %s. Rejecting...", schedule_id, ce)
            if self.db:
                await self.db.record_alert("ERROR", "consumer", f"Schedule conflict: {ce}", {"schedule_id": schedule_id})
            await self._safe_reject(message, requeue=False)
            return
        except Exception as exc:
            logger.error("Normalization error for '%s': %s. Rejecting...", schedule_id, exc)
            if self.db:
                await self.db.record_alert("ERROR", "consumer", f"Normalization exception: {exc}", {"schedule_id": schedule_id})
            await self._safe_reject(message, requeue=False)
            return

        # Step 6: Commit schedule to SQLite (atomic transaction)
        if self.schedule_repo:
            try:
                await self.schedule_repo.save_schedule_with_actions(
                    schedule_id=validated_schedule.schedule_id,
                    date=validated_schedule.date,
                    revision=validated_schedule.revision,
                    actions=normalized_actions,
                    raw_json=raw_body,
                )
            except Exception as exc:
                logger.error("Failed to commit schedule '%s' to SQLite: %s. Not ACKing message.", schedule_id, exc)
                # Do not ack so broker retains message for retry
                await self._safe_nack(message, requeue=True)
                return

        # Step 7: Acknowledge RabbitMQ (manual ACK after successful commit)
        await self._safe_ack(message)
        logger.info(
            "Acknowledged RabbitMQ message. Schedule '%s' (rev %d) safely persisted with %d actions.",
            validated_schedule.schedule_id,
            validated_schedule.revision,
            len(normalized_actions),
        )

        # Step 8: Publish schedule acceptance event
        if self.publisher:
            try:
                await self.publisher.publish_schedule_accepted(
                    schedule_id=validated_schedule.schedule_id,
                    revision=validated_schedule.revision,
                    date=validated_schedule.date,
                    actions_count=len(normalized_actions),
                )
            except Exception as exc:
                logger.error("Failed to publish schedule acceptance event: %s", exc)

        # Step 9: Notify local ScheduleEngine that a new schedule is committed
        if self.schedule_engine and hasattr(self.schedule_engine, "notify_schedule_updated"):
            self.schedule_engine.notify_schedule_updated()

    async def _process_command_batch(
        self,
        message: aio_pika.abc.AbstractIncomingMessage,
        payload: Dict[str, Any],
        raw_body: str,
    ) -> None:
        """Handles direct batch command messages for backward compatibility."""
        from ingestion.validator import validate_incoming_message, MessageValidationError
        from ingestion.normalizer import normalize_payload

        try:
            normalized = normalize_payload(payload, default_hub_id=self.hub_id)
            validated_msg = validate_incoming_message(normalized, expected_hub_id=self.hub_id)

            if self.router:
                results = await self.router.route_message(validated_msg)
                logger.info("Processed command batch %s (%d actions)", validated_msg.message_id, len(results))

            await self._safe_ack(message)

        except MessageValidationError as ve:
            logger.warning("Rejected non-conforming command batch: %s", ve)
            await self._safe_reject(message, requeue=False)
        except Exception as exc:
            logger.error("Error executing command batch: %s", exc)
            await self._safe_reject(message, requeue=False)


async def start_consumer(
    router: Optional[CommandRouter] = None,
    db: Optional[DatabaseManager] = None,
    publisher: Optional[RabbitMQPublisher] = None,
) -> None:
    """Continuously runs the consumer with automatic reconnects."""
    consumer = RabbitMQConsumer(router=router, db=db, publisher=publisher)
    while True:
        try:
            await consumer.start()
            await asyncio.Future()  # Run forever until cancelled
        except asyncio.CancelledError:
            await consumer.stop()
            break
        except Exception as exc:
            logger.error("Consumer connection failed: %s. Retrying in 5s...", exc)
            await asyncio.sleep(5)
