"""Re-export message validator from ingestion module for backward compatibility."""

from ingestion.validator import (
    CommandItem,
    IoTMessage,
    MessageValidationError,
    validate_incoming_message,
)

__all__ = [
    "CommandItem",
    "IoTMessage",
    "MessageValidationError",
    "validate_incoming_message",
]
