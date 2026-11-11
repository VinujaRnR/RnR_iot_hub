"""
Background Middleware Provisioning Listener for RnR Linux IoT Hub.
File: core/middleware_provisioning.py

Implements RNHub V2 Provisioning Listener Specification:
- Subclasses RegistrationListener for unified RNHub V2 registration listener support.
- Automatically connects to RabbitMQ when iothubd starts.
- Publishes hub.hello presence announcement.
- Subscribes to authenticated hub-specific configuration queues.
- Dispatches received configuration snapshots to AutoConfigManager / ConfigSyncManager.
"""

from __future__ import annotations
from typing import Any, Optional
from config import HUB_ID, HUB_UUID, RABBITMQ_URL, RESULTS_EXCHANGE
from core.registration_listener import RegistrationListener

__all__ = ["MiddlewareProvisioningListener"]


class MiddlewareProvisioningListener(RegistrationListener):
    """
    Subclasses RegistrationListener for backward compatibility.
    Manages automated middleware activation, configuration synchronizations, and acknowledgments.
    """

    def __init__(
        self,
        url: str = RABBITMQ_URL,
        hub_id: str = HUB_ID,
        hub_uuid: Optional[str] = HUB_UUID,
        config_sync_manager: Optional[Any] = None,
        config_exchange: str = "hub_config",
        ack_exchange: str = RESULTS_EXCHANGE,
        reconnect_delay: float = 5.0,
    ):
        super().__init__(
            url=url,
            hub_id=hub_id,
            hub_uuid=hub_uuid,
            auto_config_manager=config_sync_manager,
            config_exchange=config_exchange,
            ack_exchange=ack_exchange,
            reconnect_delay=reconnect_delay,
        )
        self.config_sync_manager = config_sync_manager
