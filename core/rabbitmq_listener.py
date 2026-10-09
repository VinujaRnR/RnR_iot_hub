import asyncio
import json
import logging
import aio_pika
from config import (
    HUB_ID,
    RABBITMQ_URL,
    QUEUE_NAME,
    PREFETCH,
    COMMAND_EXCHANGE,
    COMMAND_ROUTING_KEY,
)

logger = logging.getLogger("iothub.rabbitmq")


def validate_message(payload):
    if not isinstance(payload, dict):
        raise ValueError("Message must be a JSON object")
    
    # Accept schema_version as integer 1 or string "1"
    if payload.get("schema_version") not in (1, "1", 1.0):
        raise ValueError(f"Unsupported schema version: {payload.get('schema_version')}")
        
    if payload.get("hub_id") != HUB_ID:
        raise ValueError(f"Incorrect hub_id: expected '{HUB_ID}', got '{payload.get('hub_id')}'")
        
    if not isinstance(payload.get("message_id"), str):
        raise ValueError("Missing or invalid message_id")
        
    commands = payload.get("commands")
    if not isinstance(commands, list) or not commands:
        raise ValueError("Missing commands")
        
    if len(commands) > 50:
        raise ValueError("Too many commands (maximum allowed is 50)")


async def on_message(message: aio_pika.abc.AbstractIncomingMessage):
    async with message.process():
        raw_body = ""
        try:
            raw_body = message.body.decode("utf-8")
            payload = json.loads(raw_body)
            validate_message(payload)
            commands = payload["commands"]
            logger.info("Received message %s with %d commands", payload.get("message_id"), len(commands))
            for cmd in commands:
                target = cmd.get("device_id")
                action = cmd.get("action")
                params = json.dumps(cmd.get("parameters", {}))
                logger.info("Target=%s | Action=%s | Params=%s", target, action, params)
        except (ValueError, json.JSONDecodeError) as ve:
            # Informative warning with snippet when foreign or non-conforming message arrives
            snippet = (raw_body[:120] + "...") if len(raw_body) > 120 else raw_body
            logger.warning("Skipped non-conforming message: %s | Payload snippet: %s", ve, snippet)
        except Exception as e:
            logger.error("Failed to process message: %s", e)


async def start_listener():
    while True:
        try:
            logger.info("Connecting to RabbitMQ...")
            connection = await aio_pika.connect_robust(RABBITMQ_URL)
            async with connection:
                channel = await connection.channel()
                await channel.set_qos(prefetch_count=PREFETCH)
                
                # Declare queue
                queue = await channel.declare_queue(QUEUE_NAME, durable=True)
                
                # If an exchange and routing key are configured, declare exchange and bind
                if COMMAND_EXCHANGE:
                    exchange = await channel.declare_exchange(
                        COMMAND_EXCHANGE,
                        aio_pika.ExchangeType.TOPIC,
                        durable=True,
                    )
                    await queue.bind(exchange, routing_key=COMMAND_ROUTING_KEY)

                logger.info("Connected. Listening on %s", QUEUE_NAME)
                await queue.consume(on_message)
                await asyncio.Future()
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error("Connection failed: %s. Retrying in 5 seconds...", e)
            await asyncio.sleep(5)
