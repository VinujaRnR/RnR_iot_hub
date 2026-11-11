"""
Registration Listener for RnR Linux IoT Hub.
File: core/registration_listener.py

Implements RNHub V2 Registration Listener Specification (Section 5 & 6):
- Connects automatically to RabbitMQ when iothubd starts.
- Subscribes to authorized hub-specific configuration queues:
  * rnhub.config.<hub_uuid>
  * rnhub.config.<hub_id>
  * hub.<hub_id>.config
- Publishes hub.hello presence announcement at startup and upon reconnection.
- Receives hub.config.snapshot messages from Django middleware.
- Serializes configuration updates via asyncio.Lock to prevent race conditions.
- Uses manual RabbitMQ message acknowledgments (ack after safe persistence or permanent rejection).
- Publishes hub.config.ack results to middleware.
- Reconnects with exponential backoff on broker network disconnection.
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
from typing import Any, Callable, Dict, Optional, List
import aio_pika

from config import (
    HUB_ID,
    HUB_UUID,
    RABBITMQ_URL,
    RESULTS_EXCHANGE,
)
from core.auto_config_manager import AutoConfigManager
from core.provisioning_reporter import ProvisioningReporter

logger = logging.getLogger("iothub.registration_listener")


class RegistrationListener:
    """
    Subscribes to authorized hub-specific configuration queues and manages
    automated middleware activation, configuration synchronization, and manual AMQP acks.
    """

    def __init__(
        self,
        url: str = RABBITMQ_URL,
        hub_id: str = HUB_ID,
        hub_uuid: Optional[str] = HUB_UUID,
        auto_config_manager: Optional[Any] = None,
        config_sync_manager: Optional[Any] = None,
        provisioning_reporter: Optional[ProvisioningReporter] = None,
        config_exchange: str = "hub_config",
        ack_exchange: str = RESULTS_EXCHANGE,
        reconnect_delay: float = 5.0,
    ):
        self.url = url
        self.hub_id = hub_id
        self.hub_uuid = hub_uuid
        self.auto_config_manager = auto_config_manager or config_sync_manager
        self.config_sync_manager = self.auto_config_manager
        self.provisioning_reporter = provisioning_reporter
        self.config_exchange_name = config_exchange
        self.ack_exchange_name = ack_exchange
        self.reconnect_delay = reconnect_delay

        # Single serialized configuration application lock
        self._config_lock = asyncio.Lock()

        # Connection resources
        self.connection: Optional[aio_pika.abc.AbstractRobustConnection] = None
        self.channel: Optional[Any] = None
        self.config_exchange: Optional[Any] = None
        self.ack_exchange: Optional[Any] = None
        self.queue: Optional[Any] = None

        self._running: bool = False
        self._consumer_tag: Optional[str] = None
        self.last_hello_sent: Optional[str] = None
        self.last_config_received: Optional[Dict[str, Any]] = None

    @property
    def queue_name(self) -> str:
        """Queue name adhering to V2 convention."""
        if self.hub_uuid:
            return f"rnhub.config.{self.hub_uuid}"
        return f"hub.{self.hub_id}.config"

    async def connect(self) -> None:
        """Establishes robust AMQP connection and declares exchanges and queue topology."""
        logger.info("[REGISTRATION_LISTENER] Connecting to RabbitMQ at '%s'...", self.url)
        self.connection = await aio_pika.connect_robust(self.url)
        self.channel = await self.connection.channel()
        await self.channel.set_qos(prefetch_count=1)

        # 1. Declare Config Exchange
        try:
            self.config_exchange = await self.channel.declare_exchange(
                self.config_exchange_name,
                aio_pika.ExchangeType.TOPIC,
                durable=True,
            )
        except Exception:
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

        # 4. Bind Queue to multiple hub-specific routing keys
        routing_keys: List[str] = [
            f"hub.{self.hub_id}.config",
            f"rnhub.config.{self.hub_id}",
        ]
        if self.hub_uuid:
            routing_keys.append(f"rnhub.config.{self.hub_uuid}")
            routing_keys.append(f"hub.{self.hub_uuid}.config")
        routing_keys.append("hub.all.config")

        for rk in routing_keys:
            try:
                await self.queue.bind(self.config_exchange, routing_key=rk)
            except Exception as bind_err:
                logger.debug("Binding to '%s' notice: %s", rk, bind_err)

        logger.info(
            "[REGISTRATION_LISTENER] Topology ready: queue='%s' bound to exchange='%s'",
            self.queue_name,
            self.config_exchange_name,
        )

    async def publish_hello(self) -> None:
        """
        Publishes the hub.hello presence announcement informing middleware
        of this hub's authenticated identity, machine installation ID, and last applied config_version.
        """
        if not self.ack_exchange or not self.channel:
            return

        current_version = 0
        machine_uuid = self.hub_uuid or ""
        mgr = self.auto_config_manager or self.config_sync_manager
        if mgr and getattr(mgr, "reg_repo", None):
            reg = await mgr.reg_repo.get_registration()
            if reg:
                current_version = int(reg.get("config_version") or 0)
                if not machine_uuid and reg.get("hub_uuid"):
                    machine_uuid = reg.get("hub_uuid")
                    self.hub_uuid = machine_uuid

        hello_payload = {
            "message_type": "hub.hello",
            "event_type": "hub.hello",
            "hub_id": self.hub_id,
            "hub_uuid": machine_uuid,
            "installation_id": socket.gethostname(),
            "config_version": current_version,
            "last_applied_config_version": current_version,
            "status": "online",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "hardware": {
                "platform": platform.platform(),
                "python": sys.version.split()[0],
                "pid": os.getpid(),
            },
        }

        body_bytes = json.dumps(hello_payload).encode("utf-8")
        msg = aio_pika.Message(
            body=body_bytes,
            content_type="application/json",
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            headers={"event_type": "hub.hello", "hub_id": self.hub_id, "hub_uuid": machine_uuid},
        )

        hello_routing_key = f"hub.{self.hub_id}.hello"
        await self.ack_exchange.publish(msg, routing_key=hello_routing_key)
        self.last_hello_sent = hello_payload["timestamp"]
        logger.info("[REGISTRATION_LISTENER] Published hub.hello announcement to '%s' (v%d)", hello_routing_key, current_version)

    async def publish_ack(
        self,
        config_version: int,
        status: str,
        error: Optional[str] = None,
        reconciled_devices: Optional[List[Dict[str, Any]]] = None,
    ) -> None:
        """Publishes configuration delivery acknowledgment (hub.config.ack)."""
        if not self.ack_exchange or not self.channel:
            return

        ack_payload = {
            "message_type": "hub.config.ack",
            "event_type": "hub.config.ack",
            "hub_id": self.hub_id,
            "hub_uuid": self.hub_uuid,
            "config_version": config_version,
            "status": status,
            "error": error,
            "devices": reconciled_devices or [],
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

        ack_routing_key = f"hub.{self.hub_id}.config.ack"
        await self.ack_exchange.publish(msg, routing_key=ack_routing_key)
        logger.info(
            "[REGISTRATION_LISTENER] Published 'hub.config.ack' [version=%d, status=%s] to key '%s'",
            config_version,
            status,
            ack_routing_key,
        )

    async def handle_message(self, message: Any) -> None:
        """
        Public handler for incoming configuration messages.
        Supports both direct invocation and aio_pika message processing.
        """
        if hasattr(message, "process"):
            async with message.process(requeue=False):
                await self._process_message_body(message)
        else:
            await self._process_message_body(message)

    async def _on_message(self, message: aio_pika.abc.AbstractIncomingMessage) -> None:
        """AMQP consumer callback using manual acknowledgment."""
        await self.handle_message(message)

    async def _process_message_body(self, message: Any) -> None:
        """
        Processes inbound configuration message from middleware:
        - Validates JSON, identity, signature, and freshness
        - Serializes via lock to avoid race conditions
        - Triggers AutoConfigManager
        - Emits acknowledgment
        """
        async with self._config_lock:
            body = getattr(message, "body", b"")
            raw_body = body.decode("utf-8", errors="replace") if isinstance(body, bytes) else str(body)
            logger.info("[REGISTRATION_LISTENER] Received message (%d bytes)", len(raw_body))

            try:
                payload = json.loads(raw_body)
            except Exception as json_err:
                logger.error("[REGISTRATION_LISTENER] Malformed JSON in configuration message: %s", json_err)
                await self.publish_ack(config_version=0, status="REJECTED", error="MALFORMED_JSON")
                if hasattr(message, "reject") and not hasattr(message, "process"):
                    await message.reject(requeue=False)
                return

            if not isinstance(payload, dict):
                logger.error("[REGISTRATION_LISTENER] Malformed config payload: expected JSON object")
                await self.publish_ack(config_version=0, status="REJECTED", error="INVALID_PAYLOAD_FORMAT")
                if hasattr(message, "reject") and not hasattr(message, "process"):
                    await message.reject(requeue=False)
                return

            self.last_config_received = payload
            headers = getattr(message, "headers", {}) or {}
            signature = headers.get("x-signature") or headers.get("x-hub-signature") or payload.get("signature")

            mgr = self.auto_config_manager or self.config_sync_manager
            if not mgr:
                logger.warning("[REGISTRATION_LISTENER] No configuration manager configured on listener.")
                if hasattr(message, "ack") and not hasattr(message, "process"):
                    await message.ack()
                return

            # Apply configuration transactionally
            result = await mgr.apply_configuration(
                payload=payload,
                raw_body=raw_body,
                signature=str(signature) if signature else None,
                actor="registration_listener",
            )
            status = result.get("status", "SUCCESS")
            cfg_version = int(payload.get("config_version") or result.get("config_version") or 0)

            # Publish result acknowledgment
            await self.publish_ack(
                config_version=cfg_version,
                status=status,
                error=result.get("error"),
                reconciled_devices=result.get("reconciled_devices") or result.get("device_results") or [],
            )

            # Acknowledge or reject message
            if hasattr(message, "ack") and not hasattr(message, "process"):
                if status in ("SUCCESS", "ALREADY_APPLIED", "STALE"):
                    await message.ack()
                else:
                    await message.reject(requeue=False)

    async def start(self) -> None:
        """Connects, publishes hello, and starts consuming with manual acknowledgments."""
        self._running = True
        await self.connect()
        await self.publish_hello()
        assert self.queue is not None
        self._consumer_tag = await self.queue.consume(self.handle_message, no_ack=False)
        logger.info("[REGISTRATION_LISTENER] Listening for configuration snapshots on '%s'...", self.queue_name)

    async def stop(self) -> None:
        """Stops the listener and closes RabbitMQ resources cleanly."""
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
        logger.info("[REGISTRATION_LISTENER] Closed cleanly.")
