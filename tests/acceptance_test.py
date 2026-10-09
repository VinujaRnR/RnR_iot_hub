import asyncio
import json
import sys
import uuid
from pathlib import Path
import aio_pika

# Ensure project root is in sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config import (
    HUB_ID,
    RABBITMQ_URL,
    QUEUE_NAME,
    COMMAND_EXCHANGE,
    COMMAND_ROUTING_KEY,
)


async def send_raw_message(channel, body: bytes, content_type: str = "application/json") -> None:
    message = aio_pika.Message(
        body=body,
        content_type=content_type,
        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
    )
    if COMMAND_EXCHANGE:
        exchange = await channel.declare_exchange(
            COMMAND_EXCHANGE,
            aio_pika.ExchangeType.TOPIC,
            durable=True,
        )
        queue = await channel.declare_queue(QUEUE_NAME, durable=True)
        await queue.bind(exchange, routing_key=COMMAND_ROUTING_KEY)
        await exchange.publish(message, routing_key=COMMAND_ROUTING_KEY)
    else:
        await channel.declare_queue(QUEUE_NAME, durable=True)
        await channel.default_exchange.publish(message, routing_key=QUEUE_NAME)


async def run_acceptance_tests() -> None:
    print(f" Connecting to RabbitMQ broker for Acceptance Tests...")
    connection = await aio_pika.connect_robust(RABBITMQ_URL)

    async with connection:
        channel = await connection.channel()

        print("\n--- Test 1: Valid Message with Batch of Multiple Commands ---")
        valid_batch_payload = {
            "schema_version": 1,
            "message_id": str(uuid.uuid4()),
            "hub_id": HUB_ID,
            "commands": [
                {
                    "command_id": str(uuid.uuid4()),
                    "device_id": "iot_001",
                    "action": "set_led",
                    "parameters": {"state": "on"},
                },
                {
                    "command_id": str(uuid.uuid4()),
                    "device_id": "iot_002",
                    "action": "read_temperature",
                    "parameters": {},
                },
                {
                    "command_id": str(uuid.uuid4()),
                    "device_id": "iot_003",
                    "action": "lock_door",
                    "parameters": {"timeout": 30},
                },
            ],
        }
        await send_raw_message(channel, json.dumps(valid_batch_payload).encode())
        print(f" Sent valid batch ({len(valid_batch_payload['commands'])} commands) for hub '{HUB_ID}'")

        await asyncio.sleep(0.5)

        print("\n--- Test 2: Malformed Non-JSON Message ---")
        malformed_bytes = b"MALFORMED_NON_JSON_DATA_<<<>>>"
        await send_raw_message(channel, malformed_bytes, content_type="text/plain")
        print(" Sent malformed non-JSON payload (Expected: Listener skips/rejects without crash)")

        await asyncio.sleep(0.5)

        print("\n--- Test 3: Message Addressed to Another Hub ---")
        wrong_hub_payload = {
            "schema_version": 1,
            "message_id": str(uuid.uuid4()),
            "hub_id": "other-hub-99",
            "commands": [
                {
                    "command_id": str(uuid.uuid4()),
                    "device_id": "iot_999",
                    "action": "remote_wipe",
                    "parameters": {},
                }
            ],
        }
        await send_raw_message(channel, json.dumps(wrong_hub_payload).encode())
        print(f" Sent payload addressed to 'other-hub-99' (Expected: Listener rejects hub_id mismatch)")

        await asyncio.sleep(0.5)

        print("\n--- Test 4: Invalid Schema Version ---")
        wrong_schema_payload = {
            "schema_version": 99,
            "message_id": str(uuid.uuid4()),
            "hub_id": HUB_ID,
            "commands": [
                {
                    "command_id": str(uuid.uuid4()),
                    "device_id": "iot_001",
                    "action": "ping",
                    "parameters": {},
                }
            ],
        }
        await send_raw_message(channel, json.dumps(wrong_schema_payload).encode())
        print(" Sent payload with schema_version=99 (Expected: Listener rejects unsupported schema)")

        await asyncio.sleep(0.5)

        print("\n--- Test 5: Empty Commands List ---")
        empty_commands_payload = {
            "schema_version": 1,
            "message_id": str(uuid.uuid4()),
            "hub_id": HUB_ID,
            "commands": [],
        }
        await send_raw_message(channel, json.dumps(empty_commands_payload).encode())
        print(" Sent payload with empty commands list (Expected: Listener rejects missing commands)")

        print("\n All acceptance test messages published successfully!")
        print("Check your running listener terminal to verify the corresponding logs.")


if __name__ == "__main__":
    asyncio.run(run_acceptance_tests())
