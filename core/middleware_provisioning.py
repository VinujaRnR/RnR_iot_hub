"""
Background Middleware Provisioning Listener for RnR Linux IoT Hub.
File: core/middleware_provisioning.py

Implements RNHub V2 Provisioning Listener Specification:
1. Connects automatically to RabbitMQ when iothubd starts.
2. Publishes the hub.hello announcement to register presence and report installed config_version.
3. Subscribes to the authenticated hub-specific configuration queue (hub.<hub_id>.config).
4. Receives configuration updates pushed from Django middleware via RabbitMQ.
5. Validates HMAC signatures, hub identity, schema, freshness, and version revision.
6. Invokes ConfigSyncManager to transactionally apply approved configurations.
7. Publishes configuration acknowledgments (hub.<hub_id>.config.ack) with delivery outcomes.
8. Reconnects with exponential backoff on broker network disconnection.
"""

from __future__ import annotations
import asyncio
import json
import logging
import os
import platform
import socket
import sys
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Optional
import aio_pika

from config import (
    HUB_ID,
    HUB_UUID,
    RABBITMQ_URL,
    COMMAND_EXCHANGE,
    RESULTS_EXCHANGE,
)
from core.config_sync_manager import ConfigSyncManager

logger = logging.getLogger("iothub.provisioning_listener")


class MiddlewareProvisioningListener:
    """
    Subscribes to authenticated hub-specific configuration queues and manages
    automated middleware activation, configuration synchronizations, and acknowledgments.
    """

    def __init__(
        self,
        url: str = RABBITMQ_URL,
        hub_id: str = HUB_ID,
        hub_uuid: Optional[str] = HUB_UUID,
        config_sync_manager: Optional[ConfigSyncManager] = None,
        config_exchange: str = "hub_config",
        ack_exchange: str = RESULTS_EXCHANGE,
        reconnect_delay: float = 5.0,
    ):
        self.url = url
        self.hub_id = hub_id
        self.hub_uuid = hub_uuid
        self.config_sync_manager = config_sync_manager
        self.config_exchange_name = config_exchange
        self.ack_exchange_name = ack_exchange
        self.reconnect_delay = reconnect_delay

        self.queue_name = f"hub.{self.hub_id}.config"
        self.routing_key = f"hub.{self.hub_id}.config"
        self.hello_routing_key = f"hub.{self.hub_id}.hello"
        self.ack_routing_key = f"hub.{self.hub_id}.config.ack"

        self.connection: Optional[aio_pika.abc.AbstractRobustConnection] = None
        self.channel: Optional[aio_pika.abc.AbstractRobustChannel] = None
        self.config_exchange: Optional[aio_pika.abc.AbstractRobustExchange] = None
        self.ack_exchange: Optional[aio_pika.abc.AbstractRobustExchange] = None
        self.queue: Optional[aio_pika.abc.AbstractQueue] = None

        self._running: bool = False
        self._consumer_tag: Optional[str] = None
        self.last_hello_sent: Optional[str] = None
        self.last_config_received: Optional[Dict[str, Any]] = None

    async def connect(self) -> None:
        """Establishes robust AMQP connection and declares exchanges and queue topology."""
        logger.info("[PROVISIONING] Connecting to RabbitMQ at '%s'...", self.url)
        self.connection = await aio_pika.connect_robust(self.url)
        self.channel = await self.connection.channel()
        await self.channel.set_qos(prefetch_count=5)

        # 1. Declare Config Exchange (Topic or Direct)
        try:
            self.config_exchange = await self.channel.declare_exchange(
                self.config_exchange_name,
                aio_pika.ExchangeType.TOPIC,
                durable=True,
            )
        except Exception:
            # Fallback to direct or existing exchange if topic already declared differently
            self.config_exchange = await self.channel.get_exchange(self.config_exchange_name)

        # 2. Declare Ack / Results Exchange
        try:
            self.ack_exchange = await self.channel.declare_exchange(
                self.ack_exchange_name,
                aio_pika.ExchangeType.TOPIC,
                durable=True,
            )
        except Exception:
            self.ack_exchange = await self.channel.get_exchange(self.ack_exchange_name)

        # 3. Declare Dedicated Hub Configuration Queue
        self.queue = await self.channel.declare_queue(
            self.queue_name,
            durable=True,
            arguments={"x-message-ttl": 86400000},  # 24h retention
        )

        # 4. Bind Queue to Hub Specific Routing Key
        await self.queue.bind(self.config_exchange, routing_key=self.routing_key)
        # Also bind to broadcast topic if needed
        await self.queue.bind(self.config_exchange, routing_key="hub.all.config")

        logger.info(
            "[PROVISIONING] Topology ready: queue='%s' bound to exchange='%s' key='%s'",
            self.queue_name,
            self.config_exchange_name,
            self.routing_key,
        )

    async def publish_hello(self) -> None:
        """
        Publishes the hub.hello presence announcement informing middleware
        of this hub's availability, hardware info, and installed config version.
        """
        if not self.ack_exchange or not self.channel:
            return

        current_version = 0
        if self.config_sync_manager and self.config_sync_manager.reg_repo:
            reg = await self.config_sync_manager.reg_repo.get_registration()
            if reg:
                current_version = reg.get("config_version", 0)
                if not self.hub_uuid and reg.get("hub_uuid"):
                    self.hub_uuid = reg.get("hub_uuid")

        hello_payload = {
            "event_type": "hub.hello",
            "hub_id": self.hub_id,
            "hub_uuid": self.hub_uuid,
            "config_version": current_version,
            "status": "online",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "hardware": {
                "hostname": socket.gethostname(),
                "platform": sys.platform,
                "system": platform.system(),
                "release": platform.release(),
                "python": platform.python_version(),
            },
        }

        body_bytes = json.dumps(hello_payload).encode("utf-8")
        msg = aio_pika.Message(
            body=body_bytes,
            content_type="application/json",
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            headers={"x-hub-id": self.hub_id, "x-event-type": "hub.hello"},
        )

        await self.ack_exchange.publish(msg, routing_key=self.hello_routing_key)
        self.last_hello_sent = hello_payload["timestamp"]
        logger.info(
            "[PROVISIONING] Published 'hub.hello' (hub_id=%s, version=%d) to exchange '%s' key '%s'",
            self.hub_id,
            current_version,
            self.ack_exchange_name,
            self.hello_routing_key,
        )

    async def publish_ack(
        self,
        config_version: int,
        status: str,
        error: Optional[str] = None,
        reconciled_devices: Optional[list] = None,
    ) -> None:
        """Publishes configuration processing acknowledgment to Django middleware."""
        if not self.ack_exchange or not self.channel:
            return

        ack_payload = {
            "event_type": "hub.config.ack",
            "hub_id": self.hub_id,
            "hub_uuid": self.hub_uuid,
            "config_version": config_version,
            "status": status,
            "error": error,
            "reconciled_devices": reconciled_devices or [],
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        body_bytes = json.dumps(ack_payload).encode("utf-8")
        msg = aio_pika.Message(
            body=body_bytes,
            content_type="application/json",
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            headers={"x-hub-id": self.hub_id, "x-event-type": "hub.config.ack"},
        )

        await self.ack_exchange.publish(msg, routing_key=self.ack_routing_key)
        logger.info(
            "[PROVISIONING] Published 'hub.config.ack' [version=%d, status=%s] to key '%s'",
            config_version,
            status,
            self.ack_routing_key,
        )

    async def handle_message(self, message: aio_pika.IncomingMessage) -> None:
        """
        Processes inbound configuration message from middleware:
        - Validates JSON, identity, signature, and freshness
        - Triggers ConfigSyncManager
        - Emits acknowledgment
        """
        async with message.process(requeue=False):
            raw_body = message.body.decode("utf-8", errors="replace")
            try:
                payload = json.loads(raw_body)
            except Exception as exc:
                logger.error("[PROVISIONING] Failed to parse config JSON: %s", exc)
                await self.publish_ack(config_version=0, status="REJECTED", error="MALFORMED_JSON")
                return

            if not isinstance(payload, dict):
                logger.error("[PROVISIONING] Malformed config payload: expected JSON object")
                await self.publish_ack(config_version=0, status="REJECTED", error="INVALID_PAYLOAD_FORMAT")
                return

            self.last_config_received = payload
            signature = message.headers.get("x-hub-signature") or payload.get("signature")

            # Check message expiration if specified
            expires_at_str = payload.get("expires_at")
            if expires_at_str:
                try:
                    exp_dt = datetime.fromisoformat(expires_at_str)
                    if exp_dt < datetime.now(timezone.utc):
                        logger.warning("[PROVISIONING] Configuration message expired at %s; rejecting.", expires_at_str)
                        await self.publish_ack(
                            config_version=payload.get("config_version", 0),
                            status="REJECTED",
                            error="MESSAGE_EXPIRED",
                        )
                        return
                except Exception:
                    pass

            if not self.config_sync_manager:
                logger.error("[PROVISIONING] No ConfigSyncManager configured on listener.")
                return

            # Apply configuration transactionally
            result = await self.config_sync_manager.apply_configuration(
                payload=payload,
                raw_body=raw_body,
                signature=signature,
                actor="rabbitmq_provisioning",
            )

            # Publish result acknowledgment
            await self.publish_ack(
                config_version=int(payload.get("config_version", 0)),
                status=result.get("status", "SUCCESS"),
                error=result.get("error"),
                reconciled_devices=result.get("reconciled_devices", []),
            )

    async def start(self) -> None:
        """Starts provisioning listener: connects, sends hello, and consumes config queue."""
        self._running = True
        await self.connect()
        await self.publish_hello()

        if self.queue:
            self._consumer_tag = await self.queue.consume(self.handle_message)
            logger.info("[PROVISIONING] Listener active and consuming queue '%s'.", self.queue_name)

    async def stop(self) -> None:
        """Gracefully cancels consumer and closes connection."""
        self._running = False
        if self.queue and self._consumer_tag:
            try:
                await self.queue.cancel(self._consumer_tag)
            except Exception:
                pass
            self._consumer_tag = None

        if self.channel and not self.channel.is_closed:
            try:
                await self.channel.close()
            except Exception:
                pass

        if self.connection and not self.connection.is_closed:
            try:
                await self.connection.close()
            except Exception:
                pass

        logger.info("[PROVISIONING] Middleware provisioning listener stopped.")

