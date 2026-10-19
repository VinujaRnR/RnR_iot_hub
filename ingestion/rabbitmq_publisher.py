"""
RabbitMQ Event & Result Publisher for RnR Linux IoT Hub.
File: ingestion/rabbitmq_publisher.py

Implements Step 14 specification:
- At-least-once delivery of execution results with publisher confirmations.
- Durable local outbox retention when RabbitMQ broker is unavailable.
- Automatic flushing with broker confirmation upon reconnection.
- Stable event ID on all messages to prevent duplicate delivery downstream.
"""

from __future__ import annotations
import asyncio
from datetime import datetime, timezone
import json
import logging
import uuid
from typing import Any, Dict, List, Optional
import aio_pika

from config import (
    HUB_ID,
    RABBITMQ_URL,
    RESULTS_EXCHANGE,
    SCHEDULE_ACCEPTED_KEY,
    ACTION_RESULT_KEY,
)
from storage.event_outbox import EventOutbox

logger = logging.getLogger("iothub.publisher")


class RabbitMQPublisher:
    """
    Publishes domain events (schedule accepted, execution results) with publisher confirmations
    and durable local SQLite outbox fallback.
    """

    def __init__(
        self,
        url: str = RABBITMQ_URL,
        results_exchange: str = RESULTS_EXCHANGE,
        hub_id: str = HUB_ID,
        outbox: Optional[EventOutbox] = None,
    ):
        self.url = url
        self.results_exchange_name = results_exchange
        self.hub_id = hub_id
        self.outbox = outbox

        self.connection: Optional[aio_pika.abc.AbstractRobustConnection] = None
        self.channel: Optional[aio_pika.abc.AbstractRobustChannel] = None
        self.exchange: Optional[aio_pika.abc.AbstractRobustExchange] = None

        self._worker_running: bool = False
        self._worker_task: Optional[asyncio.Task] = None
        self._outbox_event: Optional[asyncio.Event] = None

    @property
    def is_connected(self) -> bool:
        """Returns True if connected to RabbitMQ broker and channel is open."""
        return (
            self.connection is not None
            and not self.connection.is_closed
            and self.channel is not None
            and not self.channel.is_closed
            and self.exchange is not None
        )

    async def connect(self) -> None:
        """Connects to RabbitMQ and enables publisher confirmations."""
        logger.info("Publisher connecting to RabbitMQ broker at %s...", self.url)
        self.connection = await aio_pika.connect_robust(self.url)
        # Enable publisher confirms for guaranteed delivery
        self.channel = await self.connection.channel(publisher_confirms=True)
        self.exchange = await self.channel.declare_exchange(
            self.results_exchange_name,
            aio_pika.ExchangeType.TOPIC,
            durable=True,
        )
        logger.info(
            "Publisher connected. Results exchange '%s' declared with publisher confirms.",
            self.results_exchange_name,
        )

        # On reconnection, flush any pending events retained during downtime
        if self.outbox:
            try:
                flushed = await self.flush_outbox()
                if flushed > 0:
                    logger.info("Flushed %d retained outbox event(s) upon RabbitMQ reconnection.", flushed)
            except Exception as exc:
                logger.warning("Error flushing outbox upon connect: %s", exc)

    async def close(self) -> None:
        """Closes background worker, publisher channel, and connection."""
        await self.stop_outbox_worker()
        if self.channel and not self.channel.is_closed:
            await self.channel.close()
        if self.connection and not self.connection.is_closed:
            await self.connection.close()
        logger.info("Publisher connection closed.")

    async def publish_event(
        self,
        routing_key: str,
        payload: Dict[str, Any],
        mandatory: bool = False,
        message_id: Optional[str] = None,
    ) -> bool:
        """
        Publishes an event to the results exchange with publisher confirmations.
        Waits for confirmation before returning True.
        """
        if not self.is_connected:
            try:
                await self.connect()
            except Exception as conn_err:
                logger.warning("Could not establish RabbitMQ publisher connection: %s", conn_err)
                return False

        msg_id = message_id or payload.get("event_id") or str(uuid.uuid4())
        if "event_id" not in payload:
            payload["event_id"] = msg_id

        body = json.dumps(payload).encode("utf-8")
        message = aio_pika.Message(
            body=body,
            content_type="application/json",
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            message_id=msg_id,
            headers={"event_id": msg_id, "hub_id": self.hub_id},
        )

        try:
            assert self.exchange is not None
            # publish with publisher confirmation (raises if NACKed or rejected)
            await self.exchange.publish(
                message,
                routing_key=routing_key,
                mandatory=mandatory,
            )
            logger.info(
                "Confirmed publication to '%s' (routing_key='%s', event_id=%s)",
                self.results_exchange_name,
                routing_key,
                msg_id,
            )
            return True
        except Exception as exc:
            logger.error(
                "Failed to publish event to '%s' (routing_key=%s, event_id=%s): %s",
                self.results_exchange_name,
                routing_key,
                msg_id,
                exc,
            )
            return False

    async def publish_schedule_accepted(
        self,
        schedule_id: str,
        revision: int,
        date: str,
        actions_count: int,
        routing_key: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
        event_id: Optional[str] = None,
    ) -> bool:
        """
        Publishes explicit schedule acceptance event (e.g. hub.hub-01.schedule.accepted).
        """
        key = routing_key or SCHEDULE_ACCEPTED_KEY
        stable_event_id = event_id or str(uuid.uuid4())
        payload = {
            "event_id": stable_event_id,
            "event_type": "schedule.accepted",
            "hub_id": self.hub_id,
            "schedule_id": schedule_id,
            "revision": revision,
            "date": date,
            "actions_count": actions_count,
            "status": "ACCEPTED",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "details": details or {},
        }

        # 1. Store in durable SQLite outbox first if configured (at-least-once model)
        outbox_id = None
        if self.outbox:
            outbox_id = await self.outbox.enqueue(
                event_type="schedule.accepted",
                payload=payload,
                routing_key=key,
                event_id=stable_event_id,
            )

        # 2. Publish with publisher confirmations
        success = await self.publish_event(routing_key=key, payload=payload, message_id=stable_event_id)

        # 3. If outbox active and publication confirmed, mark as sent
        if success and self.outbox and outbox_id:
            await self.outbox.mark_sent(outbox_id)
        elif not success and self.outbox and outbox_id:
            await self.outbox.mark_failed(outbox_id)

        return success

    async def publish_action_result(
        self,
        action_id: str,
        schedule_id: Optional[str],
        device_id: str,
        action: str,
        status: str,
        result: Optional[Dict[str, Any]] = None,
        error_message: Optional[str] = None,
        scheduled_at: Optional[str] = None,
        executed_at: Optional[str] = None,
        routing_key: Optional[str] = None,
        event_id: Optional[str] = None,
        event_type: str = "action_result",
        device_name: Optional[str] = None,
    ) -> bool:
        """
        Step 14: Publishes durable execution result event.
        
        Example format:
        {
          "device_name": "light_glow_1",
          "device name": "light_glow_1",
          "status": "successfull",
          "statue": "successfull",
          "action": "LIGHT_ON",
          "event_id": "9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d",
          "event_type": "action_result",
          "hub_id": "hub-01",
          "action_id": "action-001",
          "device_id": "98a316e6b658",
          "scheduled_at": "2026-10-09T09:56:00+05:30",
          "executed_at": "2026-10-09T09:56:01+05:30"
        }
        """
        key = routing_key or ACTION_RESULT_KEY
        stable_event_id = event_id or str(uuid.uuid4())
        now = executed_at or datetime.now(timezone.utc).isoformat()
        sched_time = scheduled_at or now

        user_status = "successfull" if str(status).upper() in ("SUCCESS", "SUCCESSFULL") else "failed"
        resolved_device_name = str(device_name or device_id)

        payload = {
            "device_name": resolved_device_name,
            "device name": resolved_device_name,
            "status": status,
            "statue": user_status,
            "action": action,
            "event_id": stable_event_id,
            "event_type": event_type,
            "hub_id": self.hub_id,
            "action_id": action_id,
            "device_id": device_id,
            "scheduled_at": sched_time,
            "executed_at": now,
        }
        if result is not None:
            payload["result"] = result
        if error_message:
            payload["error"] = error_message
        if schedule_id:
            payload["schedule_id"] = schedule_id

        # 1. Store in durable SQLite outbox first if configured
        outbox_id = None
        if self.outbox:
            outbox_id = await self.outbox.enqueue(
                event_type=event_type,
                payload=payload,
                routing_key=key,
                event_id=stable_event_id,
            )

        # 2. Attempt publish with confirmations
        success = await self.publish_event(routing_key=key, payload=payload, message_id=stable_event_id)

        # 3. Only mark sent when broker confirms delivery
        if success and self.outbox and outbox_id:
            await self.outbox.mark_sent(outbox_id)
        elif not success and self.outbox and outbox_id:
            await self.outbox.mark_failed(outbox_id)

        return success

    async def flush_outbox(self, limit: int = 50) -> int:
        """
        Step 14: Flushes pending events from local SQLite outbox to RabbitMQ.
        Publishes with publisher confirmations and marks delivered ONLY upon broker confirmation.
        If RabbitMQ is unavailable or fails, stops and retains events locally for subsequent retry.
        """
        if not self.outbox:
            return 0

        pending_events = await self.outbox.get_pending(limit=limit)
        sent_count = 0

        for event in pending_events:
            ev_id = event["id"]
            stable_event_id = event.get("event_id") or event.get("payload", {}).get("event_id")
            rk = event.get("routing_key") or ACTION_RESULT_KEY
            payload = event["payload"]

            success = await self.publish_event(routing_key=rk, payload=payload, message_id=stable_event_id)
            if success:
                await self.outbox.mark_sent(ev_id)
                sent_count += 1
            else:
                await self.outbox.mark_failed(ev_id)
                # If connection dropped during flush, stop loop and wait for reconnect
                if not self.is_connected:
                    break

        return sent_count

    async def start_outbox_worker(self, interval: float = 2.0) -> None:
        """Starts background worker to flush outbox periodically and upon reconnection."""
        if self._worker_task and not self._worker_task.done():
            return
        self._worker_running = True
        self._outbox_event = asyncio.Event()

        async def _worker_loop():
            while self._worker_running:
                try:
                    if self.is_connected:
                        await self.flush_outbox()
                except Exception as exc:
                    logger.debug("Outbox worker flush error: %s", exc)

                try:
                    await asyncio.wait_for(self._outbox_event.wait(), timeout=interval)
                    self._outbox_event.clear()
                except asyncio.TimeoutError:
                    pass

        self._worker_task = asyncio.create_task(_worker_loop(), name="outbox_publisher_worker")
        logger.info("Outbox publisher worker started (interval: %.1fs)", interval)

    async def stop_outbox_worker(self) -> None:
        """Stops background outbox worker."""
        self._worker_running = False
        if self._outbox_event:
            self._outbox_event.set()
        if self._worker_task:
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass
            self._worker_task = None

    def trigger_flush(self) -> None:
        """Notifies background worker to flush outbox immediately."""
        if self._outbox_event:
            self._outbox_event.set()
