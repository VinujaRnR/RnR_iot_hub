import asyncio
import json
import sys
import uuid
from pathlib import Path
import aio_pika

# Ensure project root is in sys.path when running script directly
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config import (
    HUB_ID,
    RABBITMQ_URL,
    QUEUE_NAME,
    COMMAND_EXCHANGE,
    COMMAND_ROUTING_KEY,
)


async def main():
    payload = {
        "schema_version": 1,
        "message_id": str(uuid.uuid4()),
        "hub_id": HUB_ID,
        "commands": [
            {
                "command_id": str(uuid.uuid4()),
                "device_id": "iot_001",
                "action": "set_led",
                "parameters": {
                    "state": "on"
                }
            },
            {
                "command_id": str(uuid.uuid4()),
                "device_id": "iot_002",
                "action": "read_temperature",
                "parameters": {}
            }
        ]
    }
    
    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    async with connection:
        channel = await connection.channel()
        message = aio_pika.Message(
            body=json.dumps(payload).encode("utf-8"),
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        )

        if COMMAND_EXCHANGE:
            exchange = await channel.declare_exchange(
                COMMAND_EXCHANGE,
                aio_pika.ExchangeType.TOPIC,
                durable=True,
            )
            # Ensure queue is declared & bound
            queue = await channel.declare_queue(QUEUE_NAME, durable=True)
            await queue.bind(exchange, routing_key=COMMAND_ROUTING_KEY)
            
            await exchange.publish(message, routing_key=COMMAND_ROUTING_KEY)
            print(f"Message published to exchange '{COMMAND_EXCHANGE}' with routing key '{COMMAND_ROUTING_KEY}'")
        else:
            await channel.declare_queue(QUEUE_NAME, durable=True)
            await channel.default_exchange.publish(message, routing_key=QUEUE_NAME)
            print(f"Message published to queue '{QUEUE_NAME}'")


if __name__ == "__main__":
    asyncio.run(main())
