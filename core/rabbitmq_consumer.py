import asyncio
import logging
from typing import Optional
import aio_pika

from config import (
    HUB_ID,
    RABBITMQ_URL,
    QUEUE_NAME,
    PREFETCH,
    COMMAND_EXCHANGE,
    COMMAND_ROUTING_KEY,
)
from core.message_validator import validate_incoming_message, MessageValidationError
from core.router import CommandRouter

logger = logging.getLogger("iothub.rabbitmq")


class RabbitMQConsumer:
    """Asynchronous RabbitMQ consumer piping messages through Validator and Router."""

    def __init__(
        self,
        router: Optional[CommandRouter] = None,
        url: str = RABBITMQ_URL,
        queue_name: str = QUEUE_NAME,
        exchange_name: Optional[str] = COMMAND_EXCHANGE,
        routing_key: Optional[str] = COMMAND_ROUTING_KEY,
        prefetch_count: int = PREFETCH,
    ):
        self.router = router or CommandRouter(hub_id=HUB_ID)
        self.url = url
        self.queue_name = queue_name
        self.exchange_name = exchange_name
        self.routing_key = routing_key
        self.prefetch_count = prefetch_count

        self.connection: Optional[aio_pika.abc.AbstractRobustConnection] = None
        self.channel: Optional[aio_pika.abc.AbstractRobustChannel] = None
        self._consumer_tag: Optional[str] = None

    async def _on_message(self, message: aio_pika.abc.AbstractIncomingMessage) -> None:
        """
        Message processing pipeline:
        1. Decode payload
        2. Validate via Pydantic validator
        3. Route commands to devices via CommandRouter
        4. Acknowledge message (or reject without requeueing if invalid)
        """
        # Automatically ACKs on successful block completion
        async with message.process():
            raw_body = ""
            try:
                raw_body = message.body.decode("utf-8")
                # 1. Validation phase (Consumer -> Validator)
                validated_msg = validate_incoming_message(raw_body, expected_hub_id=self.router.hub_id)

                # 2. Routing phase (Validator -> Router)
                results = await self.router.route_message(validated_msg)
                logger.info("Processed message %s: %d command(s) executed", validated_msg.message_id, len(results))

            except MessageValidationError as ve:
                snippet = (raw_body[:100] + "...") if len(raw_body) > 100 else raw_body
                logger.warning("Rejected non-conforming message: %s | Payload snippet: %s", ve, snippet)
            except Exception as e:
                logger.error("Error processing message pipeline: %s", e)

    async def start(self) -> None:
        """Connects to RabbitMQ and starts consuming messages."""
        logger.info("Connecting to RabbitMQ...")
        self.connection = await aio_pika.connect_robust(self.url)
        self.channel = await self.connection.channel()
        await self.channel.set_qos(prefetch_count=self.prefetch_count)

        queue = await self.channel.declare_queue(self.queue_name, durable=True)

        if self.exchange_name and self.routing_key:
            exchange = await self.channel.declare_exchange(
                self.exchange_name,
                aio_pika.ExchangeType.TOPIC,
                durable=True,
            )
            await queue.bind(exchange, routing_key=self.routing_key)

        logger.info("Connected. Listening on %s", self.queue_name)
        self._consumer_tag = await queue.consume(self._on_message)

    async def stop(self) -> None:
        """Stops consumer and closes connections."""
        if self.channel and not self.channel.is_closed:
            await self.channel.close()
        if self.connection and not self.connection.is_closed:
            await self.connection.close()
        logger.info("RabbitMQ consumer stopped.")


async def start_consumer(router: Optional[CommandRouter] = None) -> None:
    """Continuously runs the consumer with automatic reconnects."""
    consumer = RabbitMQConsumer(router=router)
    while True:
        try:
            await consumer.start()
            await asyncio.Future()  # Run forever until cancelled
        except asyncio.CancelledError:
            await consumer.stop()
            break
        except Exception as exc:
            logger.error("Connection failed: %s. Retrying in 5 seconds...", exc)
            await asyncio.sleep(5)
