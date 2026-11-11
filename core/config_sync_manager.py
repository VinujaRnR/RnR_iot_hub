"""
Transactional Configuration Synchronization Manager for RnR Linux IoT Hub.
File: core/config_sync_manager.py

Implements RNHub V2 Configuration Sync Specification:
- Subclasses AutoConfigManager for complete RNHub V2 auto-configuration support.
- Validates identity (hub_id, hub_uuid), signature, and revision (config_version).
- Executes transactional updates to SQLite (hub_registration, hub_configuration, approved_devices, config_history).
- Preserves schedules, outbox events, and operational histories without blind overwrites.
- Invokes AutoEnrollmentManager to reconcile physical hardware against updated device inventory.
"""

from __future__ import annotations
from core.auto_config_manager import AutoConfigManager, ConfigurationValidationError

__all__ = ["ConfigSyncManager", "ConfigurationValidationError"]


class ConfigSyncManager(AutoConfigManager):
    """
    Applies versioned configuration snapshots from Django middleware transactionally,
    preserves local historical data, and triggers hardware reconciliation.
    Maintained for full backward compatibility alongside AutoConfigManager.
    """
    pass
