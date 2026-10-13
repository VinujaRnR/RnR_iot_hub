"""Ingestion module: RabbitMQ consumer, JSON validation, normalization, schedule validator, and schedule normalizer."""

from ingestion.validator import (
    CommandItem,
    IoTMessage,
    MessageValidationError,
    validate_incoming_message,
)
from ingestion.normalizer import normalize_payload
from ingestion.consumer import RabbitMQConsumer, start_consumer
from ingestion.schemas import (
    DailyProductionSchedule,
    ScheduleEntry,
    ProductionOrder,
    Resource,
    SubResource,
    ScheduledAction,
)
from ingestion.schedule_validator import (
    ScheduleValidator,
    ScheduleValidationError,
    MalformedScheduleError,
    ScheduleRevisionError,
    UnauthorizedActionError,
    UnknownDeviceError,
    InvalidChannelError,
    SubResourceMismatchError,
    ScheduleRevisionTracker,
    validate_daily_schedule,
)
from ingestion.schedule_normalizer import (
    NormalizedActionRecord,
    ConflictPolicy,
    ScheduleConflictError,
    ScheduleNormalizer,
    normalize_schedule,
)

from ingestion.rabbitmq_publisher import RabbitMQPublisher

__all__ = [
    "CommandItem",
    "IoTMessage",
    "MessageValidationError",
    "validate_incoming_message",
    "normalize_payload",
    "RabbitMQConsumer",
    "RabbitMQPublisher",
    "start_consumer",
    "DailyProductionSchedule",
    "ScheduleEntry",
    "ProductionOrder",
    "Resource",
    "SubResource",
    "ScheduledAction",
    "ScheduleValidator",
    "ScheduleValidationError",
    "MalformedScheduleError",
    "ScheduleRevisionError",
    "UnauthorizedActionError",
    "UnknownDeviceError",
    "InvalidChannelError",
    "SubResourceMismatchError",
    "ScheduleRevisionTracker",
    "validate_daily_schedule",
    "NormalizedActionRecord",
    "ConflictPolicy",
    "ScheduleConflictError",
    "ScheduleNormalizer",
    "normalize_schedule",
]

