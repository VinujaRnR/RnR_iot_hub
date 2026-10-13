"""
Configuration and Environment Settings for RnR Linux IoT Hub.
Validates all settings at startup using pydantic-settings.
"""

from __future__ import annotations
import logging
import os
import sys
from typing import Optional, Set
from urllib.parse import quote
import zoneinfo

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Validated IoT Hub configuration.
    Loads from environment variables and .env file.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # ==========================================
    # Hub Identification & Prefetch
    # ==========================================
    HUB_ID: str = Field(default="hub-01", description="Unique hub identifier")
    RABBITMQ_PREFETCH: int = Field(default=10, ge=1, le=1000, description="RabbitMQ prefetch count")

    # ==========================================
    # RabbitMQ Broker Connection Settings
    # ==========================================
    RABBITMQ_URL: Optional[str] = Field(default=None, description="Full AMQP/AMQPS broker connection URL")
    RABBITMQ_HOST: str = Field(default="localhost", description="Broker hostname/IP")
    RABBITMQ_AMQP_PORT: int = Field(default=5672, ge=1, le=65535, description="Broker AMQP port")
    RABBITMQ_USER: str = Field(default="guest", description="Broker username")
    RABBITMQ_PASSWORD: str = Field(default="guest", description="Broker password")
    RABBITMQ_VHOST: str = Field(default="/", description="Broker virtual host")
    RABBITMQ_USE_SSL: bool = Field(default=False, description="Enable TLS/SSL connection")

    # ==========================================
    # RabbitMQ Topology Configuration
    # ==========================================
    RABBITMQ_COMMAND_EXCHANGE: str = Field(default="iot.commands", description="Exchange for command/schedule routing")
    RABBITMQ_COMMAND_QUEUE: Optional[str] = Field(default=None, description="Inbound hub command queue")
    RABBITMQ_COMMAND_ROUTING_KEY: Optional[str] = Field(default=None, description="Inbound routing key")
    RABBITMQ_RESULTS_EXCHANGE: str = Field(default="iot.events", description="Exchange for publishing events/results")
    RABBITMQ_DEADLETTER_EXCHANGE: str = Field(default="iot.deadletter", description="Dead-letter exchange for unprocessable messages")

    # ==========================================
    # Operational & Hardware Hub Settings
    # ==========================================
    SCHEDULE_TIMEZONE: str = Field(default="Asia/Kolkata", description="Timezone for scheduled operations")
    DATABASE_PATH: str = Field(default="iothub.db", description="Path to SQLite database")

    USB_BAUD_RATE: int = Field(default=115200, gt=0, description="Serial baud rate for USB microcontrollers")
    USB_SCAN_INTERVAL: int = Field(default=5, gt=0, description="Interval in seconds for scanning USB devices")
    COMMAND_TIMEOUT_SECONDS: int = Field(default=5, gt=0, description="Command execution timeout in seconds")

    # ==========================================
    # Local FastAPI Management API
    # ==========================================
    API_HOST: str = Field(default="127.0.0.1", description="API listen address")
    API_PORT: int = Field(default=8765, ge=1, le=65535, description="API port")

    # ==========================================
    # Logging Configuration
    # ==========================================
    LOG_LEVEL: str = Field(default="INFO", description="Log level (DEBUG, INFO, WARNING, ERROR)")

    @field_validator("SCHEDULE_TIMEZONE")
    @classmethod
    def validate_timezone(cls, v: str) -> str:
        """Validates that SCHEDULE_TIMEZONE is a valid IANA timezone."""
        try:
            zoneinfo.ZoneInfo(v)
        except Exception as exc:
            raise ValueError(f"Invalid timezone '{v}': {exc}")
        return v

    @field_validator("LOG_LEVEL")
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        """Validates that LOG_LEVEL is an accepted level."""
        upper_v = v.strip().upper()
        allowed: Set[str] = {"DEBUG", "INFO", "WARNING", "WARN", "ERROR", "CRITICAL"}
        if upper_v not in allowed:
            raise ValueError(f"Invalid LOG_LEVEL '{v}'. Must be one of {sorted(allowed)}")
        return upper_v

    @field_validator("DATABASE_PATH")
    @classmethod
    def validate_database_path(cls, v: str) -> str:
        """Ensures database path is non-empty."""
        clean = v.strip()
        if not clean:
            raise ValueError("DATABASE_PATH cannot be empty")
        return clean

    @model_validator(mode="after")
    def compute_defaults_and_url(self) -> Settings:
        """Computes RabbitMQ URL and default queue name if not explicitly set."""
        # Set default queue name based on HUB_ID if not given
        if not self.RABBITMQ_COMMAND_QUEUE:
            self.RABBITMQ_COMMAND_QUEUE = f"iot.hub.{self.HUB_ID}.commands"

        # Set default routing key based on HUB_ID if not given
        if not self.RABBITMQ_COMMAND_ROUTING_KEY:
            self.RABBITMQ_COMMAND_ROUTING_KEY = f"hub.{self.HUB_ID}"

        # Compute RABBITMQ_URL if not provided
        if not self.RABBITMQ_URL or not self.RABBITMQ_URL.strip():
            scheme = "amqps" if self.RABBITMQ_USE_SSL else "amqp"
            vhost_clean = self.RABBITMQ_VHOST.lstrip("/")
            vhost_encoded = quote(vhost_clean, safe="") if vhost_clean else ""
            user_encoded = quote(self.RABBITMQ_USER, safe="")
            password_encoded = quote(self.RABBITMQ_PASSWORD, safe="")
            self.RABBITMQ_URL = (
                f"{scheme}://{user_encoded}:{password_encoded}@"
                f"{self.RABBITMQ_HOST}:{self.RABBITMQ_AMQP_PORT}/{vhost_encoded}"
            )
        else:
            self.RABBITMQ_URL = self.RABBITMQ_URL.strip()

        return self


def load_settings() -> Settings:
    """
    Instantiates and validates application settings.
    Exits with a clear error report if validation fails.
    """
    try:
        return Settings()
    except Exception as exc:
        print(f"\n[FATAL CONFIGURATION ERROR] IoT Hub failed to initialize settings:\n{exc}\n", file=sys.stderr)
        raise SystemExit(1) from exc


# Global validated settings singleton
settings: Settings = load_settings()

# Module-level exports for direct import and backward compatibility
HUB_ID = settings.HUB_ID
RABBITMQ_URL = settings.RABBITMQ_URL
SCHEDULE_TIMEZONE = settings.SCHEDULE_TIMEZONE
DATABASE_PATH = settings.DATABASE_PATH
USB_BAUD_RATE = settings.USB_BAUD_RATE
USB_SCAN_INTERVAL = settings.USB_SCAN_INTERVAL
COMMAND_TIMEOUT_SECONDS = settings.COMMAND_TIMEOUT_SECONDS
RABBITMQ_PREFETCH = settings.RABBITMQ_PREFETCH
API_HOST = settings.API_HOST
API_PORT = settings.API_PORT
LOG_LEVEL = settings.LOG_LEVEL

# Topology constants
PREFETCH = settings.RABBITMQ_PREFETCH
COMMAND_EXCHANGE = settings.RABBITMQ_COMMAND_EXCHANGE
QUEUE_NAME = settings.RABBITMQ_COMMAND_QUEUE
COMMAND_ROUTING_KEY = settings.RABBITMQ_COMMAND_ROUTING_KEY
RESULTS_EXCHANGE = settings.RABBITMQ_RESULTS_EXCHANGE
DEADLETTER_EXCHANGE = settings.RABBITMQ_DEADLETTER_EXCHANGE
SCHEDULE_ACCEPTED_KEY = f"hub.{HUB_ID}.schedule.accepted"
ACTION_RESULT_KEY = f"hub.{HUB_ID}.action.result"
