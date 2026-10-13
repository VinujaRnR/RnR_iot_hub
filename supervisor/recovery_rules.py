"""
Deterministic Recovery Rules for HubSupervisor.
File: supervisor/recovery_rules.py

Formalizes the 9 monitored conditions and deterministic recovery rules defined in Step 13:
1. RabbitMQ unavailable     -> Retry connection with exponential backoff; local SQLite operation continues.
2. USB device removed       -> Mark device OFFLINE in device manager and SQLite registry.
3. Device reconnected       -> Revalidate handshake (HELLO) and register ONLINE.
4. Command timeout          -> Record UNCERTAIN/FAILED result in executions.
5. Repeated device failures -> Quarantine device after threshold consecutive failures; block dispatches & alert.
6. SQLite storage error     -> Enter safe degraded mode; stop accepting commands safely.
7. Missed schedule          -> Detect overdue actions past grace threshold and generate alert.
8. Pending result backlog   -> Apply backpressure when outbox exceeds threshold; release when drained.
9. Incorrect system clock   -> Validate NTP synchronization; block unsafe timed execution and alert.
"""

from __future__ import annotations
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Optional


class MonitoredCondition(str, Enum):
    RABBITMQ_UNAVAILABLE = "RabbitMQ unavailable"
    USB_DEVICE_REMOVED = "USB device removed"
    DEVICE_RECONNECTED = "Device reconnected"
    COMMAND_TIMEOUT = "Command timeout"
    REPEATED_DEVICE_FAILURES = "Repeated device failures"
    SQLITE_STORAGE_ERROR = "SQLite storage error"
    MISSED_SCHEDULE = "Missed schedule"
    PENDING_RESULT_BACKLOG = "Pending result backlog"
    INCORRECT_SYSTEM_CLOCK = "Incorrect system clock"


class SupervisorAction(str, Enum):
    RETRY_CONNECTION = "Retry connection"
    MARK_DEVICE_OFFLINE = "Mark device offline"
    REVALIDATE_AND_REGISTER = "Revalidate and register"
    RECORD_UNCERTAIN_RESULT = "Record uncertain or failed result"
    QUARANTINE_AND_ALERT = "Quarantine and alert"
    STOP_ACCEPTING_COMMANDS = "Stop accepting commands safely"
    GENERATE_ALERT = "Generate alert"
    APPLY_BACKPRESSURE = "Apply backpressure and report"
    BLOCK_TIMED_EXECUTION = "Block unsafe timed execution and alert"


@dataclass(frozen=True)
class RecoveryRule:
    """Explicit mapping from a monitored condition to its deterministic response."""
    condition: MonitoredCondition
    expected_response: SupervisorAction
    description: str


# Canonical registry of Step 13 recovery rules
RULES: Dict[MonitoredCondition, RecoveryRule] = {
    MonitoredCondition.RABBITMQ_UNAVAILABLE: RecoveryRule(
        condition=MonitoredCondition.RABBITMQ_UNAVAILABLE,
        expected_response=SupervisorAction.RETRY_CONNECTION,
        description="Continuously retry broker connection; hub continues autonomous local schedule execution.",
    ),
    MonitoredCondition.USB_DEVICE_REMOVED: RecoveryRule(
        condition=MonitoredCondition.USB_DEVICE_REMOVED,
        expected_response=SupervisorAction.MARK_DEVICE_OFFLINE,
        description="Detect removed USB device and mark state OFFLINE in local registry and database.",
    ),
    MonitoredCondition.DEVICE_RECONNECTED: RecoveryRule(
        condition=MonitoredCondition.DEVICE_RECONNECTED,
        expected_response=SupervisorAction.REVALIDATE_AND_REGISTER,
        description="Issue HELLO handshake, verify capabilities, and register device back to ONLINE.",
    ),
    MonitoredCondition.COMMAND_TIMEOUT: RecoveryRule(
        condition=MonitoredCondition.COMMAND_TIMEOUT,
        expected_response=SupervisorAction.RECORD_UNCERTAIN_RESULT,
        description="Record UNCERTAIN or FAILED status in execution repository and update device failure statistics.",
    ),
    MonitoredCondition.REPEATED_DEVICE_FAILURES: RecoveryRule(
        condition=MonitoredCondition.REPEATED_DEVICE_FAILURES,
        expected_response=SupervisorAction.QUARANTINE_AND_ALERT,
        description="When consecutive failures hit threshold, isolate device into QUARANTINED and emit CRITICAL alert.",
    ),
    MonitoredCondition.SQLITE_STORAGE_ERROR: RecoveryRule(
        condition=MonitoredCondition.SQLITE_STORAGE_ERROR,
        expected_response=SupervisorAction.STOP_ACCEPTING_COMMANDS,
        description="Enter safe degraded mode and stop accepting commands safely to prevent data loss.",
    ),
    MonitoredCondition.MISSED_SCHEDULE: RecoveryRule(
        condition=MonitoredCondition.MISSED_SCHEDULE,
        expected_response=SupervisorAction.GENERATE_ALERT,
        description="Detect actions remaining PENDING past scheduled time plus grace seconds and alert operators.",
    ),
    MonitoredCondition.PENDING_RESULT_BACKLOG: RecoveryRule(
        condition=MonitoredCondition.PENDING_RESULT_BACKLOG,
        expected_response=SupervisorAction.APPLY_BACKPRESSURE,
        description="Apply ingestion backpressure when event outbox exceeds backlog limit; relieve when drained.",
    ),
    MonitoredCondition.INCORRECT_SYSTEM_CLOCK: RecoveryRule(
        condition=MonitoredCondition.INCORRECT_SYSTEM_CLOCK,
        expected_response=SupervisorAction.BLOCK_TIMED_EXECUTION,
        description="Block scheduled action execution when NTP clock is unsynchronized and emit CRITICAL alert.",
    ),
}
