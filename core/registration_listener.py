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
        pending_act_code = None
        if mgr and getattr(mgr, "reg_repo", None):
            reg = await mgr.reg_repo.get_registration()
            if reg:
                current_version = int(reg.get("config_version") or 0)
                if not machine_uuid and reg.get("hub_uuid"):
                    machine_uuid = reg.get("hub_uuid")
                    self.hub_uuid = machine_uuid

        if mgr and getattr(mgr, "config_repo", None):
            try:
                pending_act_code = await mgr.config_repo.get_setting("auth.activation_code")
            except Exception:
                pending_act_code = None

        hello_payload = {
            "message_type": "hub.hello",
            "event_type": "hub.hello",
            "hub_id": self.hub_id,
            "hub_uuid": machine_uuid,
            "installation_id": socket.gethostname(),
            "config_version": current_version,
            "last_applied_config_version": current_version,
            "status": "pending_activation" if pending_act_code else "online",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "hardware": {
                "platform": platform.platform(),
                "python": sys.version.split()[0],
                "pid": os.getpid(),
            },
        }
        if pending_act_code:
            hello_payload["activation_code"] = pending_act_code

        body_bytes = json.dumps(hello_payload).encode("utf-8")
        headers = {"event_type": "hub.hello", "hub_id": self.hub_id, "hub_uuid": machine_uuid}
        if pending_act_code:
            headers["activation_code"] = pending_act_code
        msg = aio_pika.Message(
            body=body_bytes,
            content_type="application/json",
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            headers=headers,
        )

        hello_routing_key = f"hub.{self.hub_id}.hello"
        await self.ack_exchange.publish(msg, routing_key=hello_routing_key)
        self.last_hello_sent = hello_payload["timestamp"]
        logger.info("[REGISTRATION_LISTENER] Published hub.hello announcement to '%s' (v%d)", hello_routing_key, current_version)

        # If activation code is pending, also publish explicit hub.activate
        if pending_act_code:
            act_payload = {
                "message_type": "hub.activate",
                "event_type": "hub.activate",
                "action": "activate",
                "activation_code": pending_act_code,
                "hub_id": self.hub_id,
                "hub_uuid": machine_uuid,
                "installation_id": socket.gethostname(),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
            act_msg = aio_pika.Message(
                body=json.dumps(act_payload).encode("utf-8"),
                content_type="application/json",
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                headers={"event_type": "hub.activate", "action": "activate", "activation_code": pending_act_code, "hub_id": self.hub_id},
            )
            try:
                await self.ack_exchange.publish(act_msg, routing_key=f"hub.{self.hub_id}.activate")
                await self.ack_exchange.publish(act_msg, routing_key="hub.activate")
                logger.info("[REGISTRATION_LISTENER] Published explicit hub.activate for pending code")
            except Exception as act_pub_err:
                logger.debug("Failed publishing hub.activate: %s", act_pub_err)

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

            # Clear pending activation code on successful configuration application
            if status in ("SUCCESS", "ALREADY_APPLIED") and mgr and getattr(mgr, "config_repo", None):
                try:
                    await mgr.config_repo.delete_setting("auth.activation_code")
                    await mgr.config_repo.delete_setting("auth.status")
                    logger.info("[REGISTRATION_LISTENER] Cleared pending activation code after successful configuration.")
                except Exception:
                    pass

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

    @classmethod
    async def activate_via_rabbitmq(
        cls,
        activation_code: str,
        db: Any,
        url: str = RABBITMQ_URL,
        hub_id: Optional[str] = None,
        hub_uuid: Optional[str] = None,
        timeout: float = 10.0,
    ) -> Dict[str, Any]:
        """
        Submits activation credential to Django middleware over RabbitMQ,
        awaits configuration snapshot, applies it to SQLite, and emits acknowledgment.
        If middleware does not respond synchronously, stores code locally for daemon.
        """
        from storage.config_repository import ConfigRepository
        from storage.registration_repository import RegistrationRepository
        from core.auto_config_manager import AutoConfigManager
        from devices.auto_enrollment import AutoEnrollmentManager
        from devices.device_registry import DeviceRegistry

        cfg = ConfigRepository(db)
        reg_repo = RegistrationRepository(db, cfg)
        reg = await reg_repo.get_registration()
        resolved_hub_id = hub_id or await cfg.get_setting("hub.id") or (reg.get("hub_id") if reg else None) or HUB_ID
        resolved_hub_uuid = hub_uuid or await cfg.get_setting("hub.uuid") or (reg.get("hub_uuid") if reg else None) or HUB_UUID

        # Always save activation code first so even if connection fails, it's persisted
        await cfg.set_setting("auth.activation_code", activation_code)
        await cfg.set_setting("auth.status", "pending_activation")

        try:
            connection = await aio_pika.connect_robust(url)
        except Exception as conn_err:
            logger.warning("[ACTIVATION] RabbitMQ connection failed: %s", conn_err)
            return {
                "status": "PENDING_ASYNC",
                "activation_code": activation_code,
                "detail": f"Could not connect to RabbitMQ ({conn_err}). Activation code saved locally; iothubd will complete registration upon startup.",
            }

        channel = None
        ctag_reply = None
        ctag_config = None
        reply_queue = None
        config_queue = None

        try:
            channel = await connection.channel()
            await channel.set_qos(prefetch_count=1)

            # Declare Exchanges
            try:
                ack_exchange = await channel.declare_exchange(
                    RESULTS_EXCHANGE,
                    aio_pika.ExchangeType.TOPIC,
                    durable=True,
                )
            except Exception:
                ack_exchange = await channel.get_exchange(RESULTS_EXCHANGE)

            try:
                config_exchange = await channel.declare_exchange(
                    "hub_config",
                    aio_pika.ExchangeType.TOPIC,
                    durable=True,
                )
            except Exception:
                config_exchange = await channel.get_exchange("hub_config")

            # Declare temporary exclusive reply queue
            reply_queue = await channel.declare_queue(name=None, exclusive=True, auto_delete=True)

            # Declare dedicated hub configuration queue
            queue_name = f"rnhub.config.{resolved_hub_uuid}" if resolved_hub_uuid else f"hub.{resolved_hub_id}.config"
            config_queue = await channel.declare_queue(
                queue_name,
                durable=True,
                arguments={"x-message-ttl": 86400000},
            )

            # Bind queues to receive config snapshot
            config_keys = [
                f"hub.{resolved_hub_id}.config",
                f"rnhub.config.{resolved_hub_id}",
                "hub.all.config",
                f"hub.{resolved_hub_id}.config.snapshot",
                "hub.config.snapshot",
            ]
            if resolved_hub_uuid:
                config_keys.append(f"rnhub.config.{resolved_hub_uuid}")
                config_keys.append(f"hub.{resolved_hub_uuid}.config")

            for rk in config_keys:
                try:
                    await config_queue.bind(config_exchange, routing_key=rk)
                except Exception:
                    pass
                try:
                    await config_queue.bind(ack_exchange, routing_key=rk)
                except Exception:
                    pass

            try:
                await reply_queue.bind(config_exchange, routing_key=reply_queue.name)
            except Exception:
                pass
            try:
                await reply_queue.bind(ack_exchange, routing_key=reply_queue.name)
            except Exception:
                pass

            loop = asyncio.get_running_loop()
            snapshot_future: asyncio.Future = loop.create_future()

            async def _extract_snapshot(message: Any):
                try:
                    raw = message.body.decode("utf-8", errors="replace")
                    parsed = json.loads(raw)
                    m_type = parsed.get("message_type") or parsed.get("event_type") or ""
                    if m_type == "hub.activate":
                        return  # ignore own activate message echo
                    if not snapshot_future.done():
                        snapshot_future.set_result((parsed, raw, getattr(message, "headers", {})))
                except Exception as parse_ex:
                    logger.debug("Failed parsing response message: %s", parse_ex)

            async def on_snapshot(message: aio_pika.abc.AbstractIncomingMessage):
                if hasattr(message, "process"):
                    async with message.process(requeue=False):
                        await _extract_snapshot(message)
                else:
                    await _extract_snapshot(message)

            ctag_reply = await reply_queue.consume(on_snapshot)
            ctag_config = await config_queue.consume(on_snapshot)

            # Publish activation message
            activation_payload = {
                "message_type": "hub.activate",
                "event_type": "hub.activate",
                "action": "activate",
                "activation_code": activation_code,
                "hub_id": resolved_hub_id,
                "hub_uuid": resolved_hub_uuid or "",
                "installation_id": socket.gethostname(),
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "hardware": {
                    "platform": platform.platform(),
                    "python": sys.version.split()[0],
                    "pid": os.getpid(),
                },
            }
            msg = aio_pika.Message(
                body=json.dumps(activation_payload).encode("utf-8"),
                content_type="application/json",
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                reply_to=reply_queue.name,
                headers={
                    "event_type": "hub.activate",
                    "action": "activate",
                    "activation_code": activation_code,
                    "hub_id": resolved_hub_id,
                    "hub_uuid": resolved_hub_uuid or "",
                },
            )
            await ack_exchange.publish(msg, routing_key=f"hub.{resolved_hub_id}.activate")
            await ack_exchange.publish(msg, routing_key="hub.activate")
            logger.info("[ACTIVATION] Published hub.activate via RabbitMQ for hub '%s'", resolved_hub_id)

            try:
                payload, raw_body, headers = await asyncio.wait_for(snapshot_future, timeout=timeout)
                if payload.get("status") in ("REJECTED", "ERROR") or "error" in payload:
                    err_msg = payload.get("error") or payload.get("detail") or "Activation rejected"
                    return {
                        "status": "REJECTED",
                        "error": err_msg,
                    }

                auto_enr = AutoEnrollmentManager(db, DeviceRegistry(db), reg_repo=reg_repo)
                mgr = AutoConfigManager(
                    db=db,
                    reg_repo=reg_repo,
                    config_repo=cfg,
                    auto_enrollment=auto_enr,
                    expected_hub_id=resolved_hub_id,
                    expected_hub_uuid=resolved_hub_uuid,
                )
                sig = headers.get("x-signature") or headers.get("x-hub-signature") or payload.get("signature")
                apply_res = await mgr.apply_configuration(
                    payload=payload,
                    raw_body=raw_body,
                    signature=str(sig) if sig else None,
                    actor="rabbitmq_activate",
                )

                # Clear pending activation code
                await cfg.delete_setting("auth.activation_code")
                await cfg.delete_setting("auth.status")

                cfg_ver = int(payload.get("config_version") or apply_res.get("config_version") or 0)
                ack_payload = {
                    "message_type": "hub.config.ack",
                    "event_type": "hub.config.ack",
                    "hub_id": payload.get("hub_id") or resolved_hub_id,
                    "hub_uuid": payload.get("hub_uuid") or resolved_hub_uuid,
                    "config_version": cfg_ver,
                    "status": apply_res.get("status", "SUCCESS"),
                    "error": apply_res.get("error"),
                    "devices": apply_res.get("reconciled_devices") or apply_res.get("device_results") or [],
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }
                ack_msg = aio_pika.Message(
                    body=json.dumps(ack_payload).encode("utf-8"),
                    content_type="application/json",
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                    headers={"x-hub-id": resolved_hub_id, "x-event-type": "hub.config.ack"},
                )
                await ack_exchange.publish(ack_msg, routing_key=f"hub.{resolved_hub_id}.config.ack")

                return {
                    "status": "ACTIVATED",
                    "registration": payload,
                    "apply_result": apply_res,
                }
            except asyncio.TimeoutError:
                logger.info("[ACTIVATION] Awaiting middleware response timed out (%0.1fs); code saved for daemon.", timeout)
                return {
                    "status": "PENDING_ASYNC",
                    "activation_code": activation_code,
                    "detail": "Activation request sent via RabbitMQ. Saved locally; iothubd will complete configuration upon response.",
                }
            finally:
                if reply_queue and ctag_reply:
                    try:
                        await reply_queue.cancel(ctag_reply)
                    except Exception:
                        pass
                if config_queue and ctag_config:
                    try:
                        await config_queue.cancel(ctag_config)
                    except Exception:
                        pass
        finally:
            if channel and not channel.is_closed:
                try:
                    await channel.close()
                except Exception:
                    pass
            if connection and not connection.is_closed:
                try:
                    await connection.close()
                except Exception:
                    pass

    @classmethod
    async def sync_via_rabbitmq(
        cls,
        db: Any,
        url: str = RABBITMQ_URL,
        hub_id: Optional[str] = None,
        hub_uuid: Optional[str] = None,
        timeout: float = 10.0,
    ) -> Dict[str, Any]:
        """
        Requests configuration snapshot from Django middleware over RabbitMQ,
        awaits snapshot, applies it to SQLite, and emits acknowledgment.
        """
        from storage.config_repository import ConfigRepository
        from storage.registration_repository import RegistrationRepository
        from core.auto_config_manager import AutoConfigManager
        from devices.auto_enrollment import AutoEnrollmentManager
        from devices.device_registry import DeviceRegistry

        cfg = ConfigRepository(db)
        reg_repo = RegistrationRepository(db, cfg)
        reg = await reg_repo.get_registration()
        resolved_hub_id = hub_id or await cfg.get_setting("hub.id") or (reg.get("hub_id") if reg else None) or HUB_ID
        resolved_hub_uuid = hub_uuid or await cfg.get_setting("hub.uuid") or (reg.get("hub_uuid") if reg else None) or HUB_UUID
        current_version = int(reg.get("config_version") or 0) if reg else 0

        try:
            connection = await aio_pika.connect_robust(url)
        except Exception as conn_err:
            logger.warning("[SYNC] RabbitMQ connection failed: %s", conn_err)
            return {
                "status": "ERROR",
                "error": f"Could not connect to RabbitMQ ({conn_err}).",
            }

        channel = None
        ctag_reply = None
        ctag_config = None
        reply_queue = None
        config_queue = None

        try:
            channel = await connection.channel()
            await channel.set_qos(prefetch_count=1)

            try:
                ack_exchange = await channel.declare_exchange(
                    RESULTS_EXCHANGE,
                    aio_pika.ExchangeType.TOPIC,
                    durable=True,
                )
            except Exception:
                ack_exchange = await channel.get_exchange(RESULTS_EXCHANGE)

            try:
                config_exchange = await channel.declare_exchange(
                    "hub_config",
                    aio_pika.ExchangeType.TOPIC,
                    durable=True,
                )
            except Exception:
                config_exchange = await channel.get_exchange("hub_config")

            reply_queue = await channel.declare_queue(name=None, exclusive=True, auto_delete=True)

            queue_name = f"rnhub.config.{resolved_hub_uuid}" if resolved_hub_uuid else f"hub.{resolved_hub_id}.config"
            config_queue = await channel.declare_queue(
                queue_name,
                durable=True,
                arguments={"x-message-ttl": 86400000},
            )

            config_keys = [
                f"hub.{resolved_hub_id}.config",
                f"rnhub.config.{resolved_hub_id}",
                "hub.all.config",
                f"hub.{resolved_hub_id}.config.snapshot",
                "hub.config.snapshot",
            ]
            if resolved_hub_uuid:
                config_keys.append(f"rnhub.config.{resolved_hub_uuid}")
                config_keys.append(f"hub.{resolved_hub_uuid}.config")

            for rk in config_keys:
                try:
                    await config_queue.bind(config_exchange, routing_key=rk)
                except Exception:
                    pass
                try:
                    await config_queue.bind(ack_exchange, routing_key=rk)
                except Exception:
                    pass

            try:
                await reply_queue.bind(config_exchange, routing_key=reply_queue.name)
            except Exception:
                pass
            try:
                await reply_queue.bind(ack_exchange, routing_key=reply_queue.name)
            except Exception:
                pass

            loop = asyncio.get_running_loop()
            snapshot_future: asyncio.Future = loop.create_future()

            async def _extract_snapshot_sync(message: Any):
                try:
                    raw = message.body.decode("utf-8", errors="replace")
                    parsed = json.loads(raw)
                    m_type = parsed.get("message_type") or parsed.get("event_type") or ""
                    if m_type in ("hub.hello", "hub.sync"):
                        return
                    if not snapshot_future.done():
                        snapshot_future.set_result((parsed, raw, getattr(message, "headers", {})))
                except Exception as parse_ex:
                    logger.debug("Failed parsing response message: %s", parse_ex)

            async def on_snapshot(message: aio_pika.abc.AbstractIncomingMessage):
                if hasattr(message, "process"):
                    async with message.process(requeue=False):
                        await _extract_snapshot_sync(message)
                else:
                    await _extract_snapshot_sync(message)

            ctag_reply = await reply_queue.consume(on_snapshot)
            ctag_config = await config_queue.consume(on_snapshot)

            # Publish sync request
            sync_payload = {
                "message_type": "hub.hello",
                "event_type": "hub.hello",
                "action": "sync",
                "hub_id": resolved_hub_id,
                "hub_uuid": resolved_hub_uuid or "",
                "installation_id": socket.gethostname(),
                "config_version": current_version,
                "last_applied_config_version": current_version,
                "status": "online",
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
            msg = aio_pika.Message(
                body=json.dumps(sync_payload).encode("utf-8"),
                content_type="application/json",
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                reply_to=reply_queue.name,
                headers={"event_type": "hub.hello", "action": "sync", "hub_id": resolved_hub_id},
            )
            await ack_exchange.publish(msg, routing_key=f"hub.{resolved_hub_id}.hello")
            await ack_exchange.publish(msg, routing_key=f"hub.{resolved_hub_id}.sync")

            try:
                payload, raw_body, headers = await asyncio.wait_for(snapshot_future, timeout=timeout)
                if payload.get("status") in ("REJECTED", "ERROR") or "error" in payload:
                    err_msg = payload.get("error") or payload.get("detail") or "Sync rejected"
                    return {"status": "REJECTED", "error": err_msg}

                auto_enr = AutoEnrollmentManager(db, DeviceRegistry(db), reg_repo=reg_repo)
                mgr = AutoConfigManager(
                    db=db,
                    reg_repo=reg_repo,
                    config_repo=cfg,
                    auto_enrollment=auto_enr,
                    expected_hub_id=resolved_hub_id,
                    expected_hub_uuid=resolved_hub_uuid,
                )
                sig = headers.get("x-signature") or headers.get("x-hub-signature") or payload.get("signature")
                apply_res = await mgr.apply_configuration(
                    payload=payload,
                    raw_body=raw_body,
                    signature=str(sig) if sig else None,
                    actor="rabbitmq_sync",
                )

                cfg_ver = int(payload.get("config_version") or apply_res.get("config_version") or 0)
                ack_payload = {
                    "message_type": "hub.config.ack",
                    "event_type": "hub.config.ack",
                    "hub_id": payload.get("hub_id") or resolved_hub_id,
                    "hub_uuid": payload.get("hub_uuid") or resolved_hub_uuid,
                    "config_version": cfg_ver,
                    "status": apply_res.get("status", "SUCCESS"),
                    "error": apply_res.get("error"),
                    "devices": apply_res.get("reconciled_devices") or apply_res.get("device_results") or [],
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }
                ack_msg = aio_pika.Message(
                    body=json.dumps(ack_payload).encode("utf-8"),
                    content_type="application/json",
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                    headers={"x-hub-id": resolved_hub_id, "x-event-type": "hub.config.ack"},
                )
                await ack_exchange.publish(ack_msg, routing_key=f"hub.{resolved_hub_id}.config.ack")

                return {
                    "status": "SUCCESS",
                    "config_version": cfg_ver,
                    "apply_result": apply_res,
                }
            except asyncio.TimeoutError:
                return {
                    "status": "PENDING_ASYNC",
                    "detail": "Sync request sent via RabbitMQ. Background listener will apply configuration when received.",
                }
            finally:
                if reply_queue and ctag_reply:
                    try:
                        await reply_queue.cancel(ctag_reply)
                    except Exception:
                        pass
                if config_queue and ctag_config:
                    try:
                        await config_queue.cancel(ctag_config)
                    except Exception:
                        pass
        finally:
            if channel and not channel.is_closed:
                try:
                    await channel.close()
                except Exception:
                    pass
            if connection and not connection.is_closed:
                try:
                    await connection.close()
                except Exception:
                    pass
