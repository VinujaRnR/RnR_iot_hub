"""Re-export RabbitMQ consumer from ingestion module for backward compatibility."""

from ingestion.consumer import RabbitMQConsumer, start_consumer

__all__ = [
    "RabbitMQConsumer",
    "start_consumer",
]
