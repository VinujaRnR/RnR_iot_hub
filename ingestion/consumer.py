"""Re-export RabbitMQ consumer for backward compatibility."""

from ingestion.rabbitmq_consumer import RabbitMQConsumer, start_consumer

__all__ = ["RabbitMQConsumer", "start_consumer"]
