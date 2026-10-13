import pytest
from pydantic import ValidationError
from config import Settings


def test_default_settings():
    settings = Settings()
    assert settings.HUB_ID is not None
    assert settings.SCHEDULE_TIMEZONE == "Asia/Kolkata" or settings.SCHEDULE_TIMEZONE == "Asia/Colombo"
    assert settings.USB_BAUD_RATE == 115200
    assert settings.USB_SCAN_INTERVAL == 5
    assert settings.COMMAND_TIMEOUT_SECONDS == 5
    assert settings.API_PORT > 0


def test_invalid_api_port():
    with pytest.raises(ValidationError) as exc:
        Settings(API_PORT=99999)
    assert "API_PORT" in str(exc.value)


def test_invalid_timezone():
    with pytest.raises(ValidationError) as exc:
        Settings(SCHEDULE_TIMEZONE="Invalid/Timezone_Not_Real")
    assert "SCHEDULE_TIMEZONE" in str(exc.value)


def test_invalid_log_level():
    with pytest.raises(ValidationError) as exc:
        Settings(LOG_LEVEL="SUPER_VERBOSE")
    assert "LOG_LEVEL" in str(exc.value)


def test_url_construction_when_missing():
    settings = Settings(
        RABBITMQ_URL="",
        RABBITMQ_HOST="10.0.0.5",
        RABBITMQ_AMQP_PORT=5672,
        RABBITMQ_USER="testuser",
        RABBITMQ_PASSWORD="secretpassword",
        RABBITMQ_VHOST="myvhost",
        RABBITMQ_USE_SSL=False,
    )
    assert settings.RABBITMQ_URL.startswith("amqp://testuser:secretpassword@10.0.0.5:5672")
    assert "myvhost" in settings.RABBITMQ_URL


def test_ssl_url_construction():
    settings = Settings(
        RABBITMQ_URL="",
        RABBITMQ_HOST="broker.example.com",
        RABBITMQ_AMQP_PORT=5671,
        RABBITMQ_USER="testuser",
        RABBITMQ_PASSWORD="secretpassword",
        RABBITMQ_VHOST="/",
        RABBITMQ_USE_SSL=True,
    )
    assert settings.RABBITMQ_URL.startswith("amqps://testuser:secretpassword@broker.example.com:5671")
