import os
from urllib.parse import quote
from dotenv import load_dotenv

load_dotenv()

# ==========================================
# Hub Identification & Prefetch
# ==========================================
HUB_ID = os.getenv("HUB_ID", "hub-01")
PREFETCH = int(os.getenv("RABBITMQ_PREFETCH", "10"))

# ==========================================
# RabbitMQ Remote Broker Connection
# ==========================================
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_AMQP_PORT = int(os.getenv("RABBITMQ_AMQP_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD", "guest")
RABBITMQ_VHOST = os.getenv("RABBITMQ_VHOST", "/")
RABBITMQ_USE_SSL = os.getenv("RABBITMQ_USE_SSL", "false").strip().lower() in ("true", "1", "yes")

# Construct RabbitMQ URL if not explicitly provided
if "RABBITMQ_URL" in os.environ and os.environ["RABBITMQ_URL"].strip():
    RABBITMQ_URL = os.environ["RABBITMQ_URL"].strip()
else:
    scheme = "amqps" if RABBITMQ_USE_SSL else "amqp"
    vhost_encoded = quote(RABBITMQ_VHOST, safe="") if RABBITMQ_VHOST else ""
    user_encoded = quote(RABBITMQ_USER, safe="")
    password_encoded = quote(RABBITMQ_PASSWORD, safe="")
    RABBITMQ_URL = f"{scheme}://{user_encoded}:{password_encoded}@{RABBITMQ_HOST}:{RABBITMQ_AMQP_PORT}/{vhost_encoded}"

# ==========================================
# RabbitMQ Command Routing Configuration
# ==========================================
COMMAND_EXCHANGE = os.getenv("RABBITMQ_COMMAND_EXCHANGE", "iot_commands")
QUEUE_NAME = os.getenv("RABBITMQ_COMMAND_QUEUE", f"iot.hub.{HUB_ID}.commands")
COMMAND_ROUTING_KEY = os.getenv("RABBITMQ_COMMAND_ROUTING_KEY", "iot.command")

# ==========================================
# FastAPI Web Service Settings
# ==========================================
API_HOST = os.getenv("API_HOST", "0.0.0.0")
API_PORT = int(os.getenv("API_PORT", "8000"))
