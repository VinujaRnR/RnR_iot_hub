"""
Tests for RabbitMQ Communication Pipeline (Step 7).
Verifies the complete 8-step ingestion pipeline, topology, manual acknowledgments,
dead-letter routing, publisher confirmations, and SQLite outbox retention.
"""

from __future__ import annotations
import json
import os
import tempfile
from unittest.mock import AsyncMock, MagicMock, patch
import pytest
import pytest_asyncio
import aio_pika

from config import (
    HUB_ID,
    COMMAND_EXCHANGE,
    QUEUE_NAME,
    COMMAND_ROUTING_KEY,
    RESULTS_EXCHANGE,
    DEADLETTER_EXCHANGE,
    SCHEDULE_ACCEPTED_KEY,
    ACTION_RESULT_KEY,
)
from ingestion.rabbitmq_consumer import RabbitMQConsumer
from ingestion.rabbitmq_publisher import RabbitMQPublisher
from storage.database import DatabaseManager
from storage.schedule_repository import ScheduleRepository
from storage.event_outbox import EventOutbox


class MockIncomingMessage:
    """Mock aio_pika incoming message tracking ack, reject, and nack status."""

    def __init__(self, body: bytes | str):
        if isinstance(body, str):
            self.body = body.encode("utf-8")
        else:
            self.body = body
        self.acked = False
        self.rejected = False
        self.nacked = False
        self.requeue = None

    async def ack(self, multiple: bool = False) -> None:
        self.acked = True

    async def reject(self, requeue: bool = False) -> None:
        self.rejected = True
        self.requeue = requeue

    async def nack(self, multiple: bool = False, requeue: bool = True) -> None:
        self.nacked = True
        self.requeue = requeue


@pytest_asyncio.fixture
async def temp_db():
    fd, path = tempfile.mkstemp(suffix="_test_pipeline.db")
    os.close(fd)
    db = DatabaseManager(db_path=path)
    await db.connect()
    yield db
    await db.close()
    if os.path.exists(path):
        os.remove(path)
    for ext in ("-wal", "-shm"):
        wal = path + ext
        if os.path.exists(wal):
            try:
                os.remove(wal)
            except OSError:
                pass


@pytest.fixture
def valid_production_schedule_payload():
    """Valid daily production schedule with timed sub_resource actions."""
    return {
        "schema_version": 1,
        "schedule_id": "hub-01-2026-10-09",
        "hub_id": "hub-01",
        "revision": 1,
        "date": "2026-10-09",
        "timezone": "Asia/Kolkata",
        "production_orders": [
            {
                "production_order_id": 174513,
                "stage_id": 30,
                "resources": [
                    {
                        "resource_id": 20020,
                        "sub_resources": [
                            {
                                "sub_resource_id": 20023,
                                "iot_node_id": "98a316e6b658",
                                "channel_key": "relay:light",
                                "actions": [
                                    {
                                        "action_id": "action-001",
                                        "action": "LIGHTS_ON",
                                        "time": "09:56:00",
                                        "channel_key": "relay:light",
                                        "sub_resource": "20023",
                                        "iot_node_id": "98a316e6b658",
                                        "params": {},
                                    },
                                    {
                                        "action_id": "action-002",
                                        "action": "LIGHTS_OFF",
                                        "time": "18:00:00",
                                        "channel_key": "relay:light",
                                        "sub_resource": "20023",
                                        "iot_node_id": "98a316e6b658",
                                        "params": {},
                                    },
                                ],
                            }
                        ],
                    }
                ],
            }
        ],
    }


@pytest.mark.asyncio
async def test_full_schedule_ingestion_and_acceptance(temp_db, valid_production_schedule_payload):
    """
    Step 7 Core Flow:
    1. Receive schedule from hub-specific queue
    2. Validate JSON structure & schema
    3. Check revision
    4. Normalize device actions
    5. Commit schedule & actions to SQLite
    6. Manually acknowledge RabbitMQ message (msg.ack)
    7. Publish schedule acceptance event with publisher confirmations
    """
    repo = ScheduleRepository(temp_db)
    outbox = EventOutbox(temp_db)

    # Mock publisher to verify confirmation and outbox integration
    publisher = RabbitMQPublisher(outbox=outbox, hub_id="hub-01")
    publisher.publish_event = AsyncMock(return_value=True)

    consumer = RabbitMQConsumer(
        hub_id="hub-01",
        db=temp_db,
        schedule_repo=repo,
        publisher=publisher,
    )

    msg = MockIncomingMessage(body=json.dumps(valid_production_schedule_payload))
    await consumer._on_message(msg)

    # 1. Verify message was manually ACKed (and NOT rejected or nacked)
    assert msg.acked is True
    assert msg.rejected is False
    assert msg.nacked is False

    # 2. Verify schedule committed to SQLite
    saved_schedule = await repo.get_schedule("hub-01-2026-10-09")
    assert saved_schedule is not None
    assert saved_schedule["schedule_id"] == "hub-01-2026-10-09"
    assert saved_schedule["revision"] == 1
    assert saved_schedule["status"] == "ACTIVE"

    # 3. Verify actions committed to SQLite
    actions = await repo.get_scheduled_actions("hub-01-2026-10-09")
    assert len(actions) == 2
    assert actions[0]["action_id"] == "action-001"
    assert actions[0]["action"] == "LIGHTS_ON"
    assert actions[1]["action_id"] == "action-002"
    assert actions[1]["action"] == "LIGHTS_OFF"

    # 4. Verify schedule accepted event published to results exchange
    publisher.publish_event.assert_called_once()
    call_args = publisher.publish_event.call_args
    assert call_args.kwargs["routing_key"] == "hub.hub-01.schedule.accepted"
    payload = call_args.kwargs["payload"]
    assert payload["event_type"] == "schedule.accepted"
    assert payload["schedule_id"] == "hub-01-2026-10-09"
    assert payload["actions_count"] == 2
    assert payload["status"] == "ACCEPTED"

    # 5. Verify SQLite event outbox recorded the event and marked it SENT
    stats = await outbox.get_stats()
    assert stats["SENT"] == 1
    assert stats["PENDING"] == 0


@pytest.mark.asyncio
async def test_malformed_json_rejection_and_dead_letter(temp_db):
    """
    Malformed JSON must be rejected with requeue=False (routing to iot.deadletter)
    and recorded in SQLite alerts table.
    """
    repo = ScheduleRepository(temp_db)
    consumer = RabbitMQConsumer(hub_id="hub-01", db=temp_db, schedule_repo=repo)

    msg = MockIncomingMessage(body=b"INVALID_NON_JSON_DATA<<<>>>")
    await consumer._on_message(msg)

    # Must be rejected without requeue (to dead-letter)
    assert msg.rejected is True
    assert msg.requeue is False
    assert msg.acked is False

    # Verify alert logged in DB
    async with temp_db.connection.execute("SELECT * FROM alerts WHERE source = 'consumer'") as cur:
        alerts = await cur.fetchall()
        assert len(alerts) >= 1
        assert "Malformed JSON" in alerts[0]["message"]


@pytest.mark.asyncio
async def test_invalid_schedule_schema_rejection(temp_db):
    """
    Invalid schema (e.g. invalid date or missing required fields) must be rejected
    without requeue to dead-letter, and never committed to SQLite.
    """
    repo = ScheduleRepository(temp_db)
    consumer = RabbitMQConsumer(hub_id="hub-01", db=temp_db, schedule_repo=repo)

    invalid_payload = {
        "schedule_id": "hub-01-invalid",
        "hub_id": "hub-01",
        "date": "NOT-A-DATE",  # Invalid date format
        "timezone": "Invalid/Timezone",
        "production_orders": [],
    }
    msg = MockIncomingMessage(body=json.dumps(invalid_payload))
    await consumer._on_message(msg)

    # Must be rejected to dead-letter
    assert msg.rejected is True
    assert msg.requeue is False
    assert msg.acked is False

    # Must not have committed any schedule
    saved = await repo.get_schedule("hub-01-invalid")
    assert saved is None


@pytest.mark.asyncio
async def test_duplicate_and_stale_revision_handling(temp_db, valid_production_schedule_payload):
    """
    - Ingest revision 1: accepted and persisted.
    - Re-send revision 1: duplicate detected, ACKed to discard without duplicating actions.
    - Send revision 2: newer revision accepted and updates SQLite.
    """
    repo = ScheduleRepository(temp_db)
    outbox = EventOutbox(temp_db)
    publisher = RabbitMQPublisher(outbox=outbox, hub_id="hub-01")
    publisher.publish_event = AsyncMock(return_value=True)

    consumer = RabbitMQConsumer(
        hub_id="hub-01",
        db=temp_db,
        schedule_repo=repo,
        publisher=publisher,
    )

    # 1. Ingest revision 1
    msg1 = MockIncomingMessage(body=json.dumps(valid_production_schedule_payload))
    await consumer._on_message(msg1)
    assert msg1.acked is True
    actions = await repo.get_scheduled_actions("hub-01-2026-10-09")
    assert len(actions) == 2

    # 2. Re-send revision 1 (duplicate)
    msg2 = MockIncomingMessage(body=json.dumps(valid_production_schedule_payload))
    await consumer._on_message(msg2)
    # Duplicate is ACKed to remove from queue
    assert msg2.acked is True
    assert msg2.rejected is False
    # Actions count must remain 2 (no duplicates)
    actions_after = await repo.get_scheduled_actions("hub-01-2026-10-09")
    assert len(actions_after) == 2

    # 3. Send revision 2
    rev2_payload = dict(valid_production_schedule_payload)
    rev2_payload["revision"] = 2
    msg3 = MockIncomingMessage(body=json.dumps(rev2_payload))
    await consumer._on_message(msg3)
    assert msg3.acked is True

    sched = await repo.get_schedule("hub-01-2026-10-09")
    assert sched["revision"] == 2


@pytest.mark.asyncio
async def test_atomic_rollback_and_nack_on_sqlite_failure(temp_db, valid_production_schedule_payload):
    """
    If committing schedule to SQLite fails (e.g. disk or database crash),
    consumer MUST NOT ACK the message, but instead call nack(requeue=True)
    to retain the message on RabbitMQ for retry.
    """
    repo = ScheduleRepository(temp_db)
    consumer = RabbitMQConsumer(hub_id="hub-01", db=temp_db, schedule_repo=repo)

    # Force save_schedule_with_actions to fail
    with patch.object(repo, "save_schedule_with_actions", side_effect=RuntimeError("Disk I/O error")):
        msg = MockIncomingMessage(body=json.dumps(valid_production_schedule_payload))
        await consumer._on_message(msg)

        # Must NOT be ACKed
        assert msg.acked is False
        # Must be NACKed with requeue=True
        assert msg.nacked is True
        assert msg.requeue is True


@pytest.mark.asyncio
async def test_publisher_confirmations_and_outbox_retention(temp_db):
    """
    Verifies that RabbitMQPublisher:
    1. Retains event in SQLite outbox before transmission
    2. Marks as SENT only when broker confirmation succeeds
    3. Retains as PENDING / retries when broker fails
    """
    outbox = EventOutbox(temp_db)
    publisher = RabbitMQPublisher(outbox=outbox, hub_id="hub-01")

    # Case A: Successful confirmed publication
    publisher.publish_event = AsyncMock(return_value=True)
    ok = await publisher.publish_schedule_accepted(
        schedule_id="hub-01-2026-10-09",
        revision=1,
        date="2026-10-09",
        actions_count=5,
    )
    assert ok is True
    stats = await outbox.get_stats()
    assert stats["SENT"] == 1
    assert stats["PENDING"] == 0

    # Case B: Broker publication fails (temporary disconnect)
    publisher.publish_event = AsyncMock(return_value=False)
    ok_fail = await publisher.publish_schedule_accepted(
        schedule_id="hub-01-2026-10-09",
        revision=2,
        date="2026-10-09",
        actions_count=5,
    )
    assert ok_fail is False
    stats2 = await outbox.get_stats()
    # Failed attempt was marked as pending retry
    assert stats2["PENDING"] == 1

    # Case C: Flush outbox after broker recovery
    publisher.publish_event = AsyncMock(return_value=True)
    flushed = await publisher.flush_outbox()
    assert flushed == 1
    stats3 = await outbox.get_stats()
    assert stats3["PENDING"] == 0
    assert stats3["SENT"] == 2


@pytest.mark.asyncio
async def test_publish_action_result(temp_db):
    """Verifies publication of execution results to hub.{HUB_ID}.action.result."""
    outbox = EventOutbox(temp_db)
    publisher = RabbitMQPublisher(outbox=outbox, hub_id="hub-01")
    publisher.publish_event = AsyncMock(return_value=True)

    ok = await publisher.publish_action_result(
        action_id="action-001",
        schedule_id="hub-01-2026-10-09",
        device_id="98a316e6b658",
        action="LIGHTS_ON",
        status="SUCCESS",
        result={"relay_state": 1},
    )
    assert ok is True
    publisher.publish_event.assert_called_once()
    call_args = publisher.publish_event.call_args
    assert call_args.kwargs["routing_key"] == "hub.hub-01.action.result"
    payload = call_args.kwargs["payload"]
    assert payload["event_type"] in ("action_result", "action.result")
    assert payload["status"] == "SUCCESS"
    assert payload["device_id"] == "98a316e6b658"


@pytest.mark.asyncio
async def test_consumer_topology_declaration():
    """
    Verifies that connect() declares:
    - Dead-letter exchange 'iot.deadletter'
    - Dead-letter queue
    - Command exchange 'iot.commands'
    - Durable inbound queue 'iot.hub.hub-01.commands' with DLX arguments
    - Binds routing key 'hub.hub-01'
    """
    consumer = RabbitMQConsumer(
        url="amqp://guest:guest@localhost:5672/",
        queue_name="iot.hub.hub-01.commands",
        exchange_name="iot.commands",
        routing_key="hub.hub-01",
        deadletter_exchange="iot.deadletter",
        prefetch_count=10,
        hub_id="hub-01",
    )

    mock_channel = AsyncMock()
    mock_connection = AsyncMock()
    mock_connection.channel = AsyncMock(return_value=mock_channel)

    mock_dl_queue = AsyncMock()
    mock_inbound_queue = AsyncMock()
    mock_dlx = AsyncMock()
    mock_cmd_exchange = AsyncMock()

    mock_channel.declare_exchange = AsyncMock(side_effect=[mock_dlx, mock_cmd_exchange])
    mock_channel.declare_queue = AsyncMock(side_effect=[mock_dl_queue, mock_inbound_queue])
    mock_inbound_queue.consume = AsyncMock(return_value="consumer_tag_123")

    with patch("aio_pika.connect_robust", AsyncMock(return_value=mock_connection)):
        await consumer.connect()

        # Check QoS
        mock_channel.set_qos.assert_called_once_with(prefetch_count=10)

        # Check exchange declarations
        assert mock_channel.declare_exchange.call_count == 2
        mock_channel.declare_exchange.assert_any_call("iot.deadletter", aio_pika.ExchangeType.TOPIC, durable=True)
        mock_channel.declare_exchange.assert_any_call("iot.commands", aio_pika.ExchangeType.TOPIC, durable=True)

        # Check inbound queue declared with dead-letter exchange arguments
        mock_channel.declare_queue.assert_any_call(
            "iot.hub.hub-01.commands",
            durable=True,
            arguments={
                "x-dead-letter-exchange": "iot.deadletter",
                "x-dead-letter-routing-key": "deadletter.iot.hub.hub-01.commands",
            },
        )

        # Check routing key bindings
        mock_inbound_queue.bind.assert_any_call(mock_cmd_exchange, routing_key="hub.hub-01")
        mock_inbound_queue.bind.assert_any_call(mock_cmd_exchange, routing_key="hub.hub-01.#")

        # Check manual ack consumption (no_ack=False)
        mock_inbound_queue.consume.assert_called_once_with(consumer._on_message, no_ack=False)


@pytest.mark.asyncio
async def test_topology_declaration_precondition_failed_fallback():
    """Verify that consumer gracefully recovers when queue exists with different arguments."""
    consumer = RabbitMQConsumer(
        url="amqp://guest:guest@localhost:5672/",
        queue_name="iot_exchange",
        exchange_name="iot_commands",
        routing_key="hub.hub-01",
        deadletter_exchange="iot.deadletter",
        prefetch_count=10,
    )

    mock_connection = AsyncMock()
    mock_channel1 = AsyncMock()
    mock_channel2 = AsyncMock()
    mock_connection.channel = AsyncMock(side_effect=[mock_channel1, mock_channel2])

    mock_dl_queue = AsyncMock()
    mock_fallback_queue = AsyncMock()
    mock_dlx = AsyncMock()
    mock_cmd_exchange = AsyncMock()

    mock_channel1.declare_exchange = AsyncMock(side_effect=[mock_dlx, mock_cmd_exchange])
    # First queue declare succeeds (dl_queue), second fails with PRECONDITION_FAILED
    mock_channel1.declare_queue = AsyncMock(
        side_effect=[
            mock_dl_queue,
            Exception("PRECONDITION_FAILED - inequivalent arg 'x-dead-letter-exchange' for queue 'iot_exchange'"),
        ]
    )

    mock_channel2.declare_exchange = AsyncMock(return_value=mock_cmd_exchange)
    mock_channel2.declare_queue = AsyncMock(return_value=mock_fallback_queue)
    mock_fallback_queue.consume = AsyncMock(return_value="consumer_tag_fallback")

    with patch("aio_pika.connect_robust", AsyncMock(return_value=mock_connection)):
        await consumer.connect()

        # Channel 2 should have been opened and configured
        mock_channel2.set_qos.assert_called_once_with(prefetch_count=10)
        mock_channel2.declare_queue.assert_called_once_with("iot_exchange", passive=True)
        mock_fallback_queue.bind.assert_any_call(mock_cmd_exchange, routing_key="hub.hub-01")
        mock_fallback_queue.consume.assert_called_once_with(consumer._on_message, no_ack=False)

