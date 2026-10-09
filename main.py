import asyncio
import logging
from config import HUB_ID
from core.rabbitmq_consumer import RabbitMQConsumer
from core.router import CommandRouter
from devices.device_manager import create_default_device_manager


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )


async def run_hub():
    logging.info("Starting Linux IoT Hub [%s]", HUB_ID)
    
    # Initialize devices and router
    device_manager = create_default_device_manager()
    router = CommandRouter(hub_id=HUB_ID, device_manager=device_manager)
    
    # Initialize RabbitMQ consumer with pipeline
    consumer = RabbitMQConsumer(router=router)

    while True:
        try:
            await consumer.start()
            await asyncio.Future()  # Keep running until cancelled
        except asyncio.CancelledError:
            await consumer.stop()
            break
        except Exception as exc:
            logging.error("Connection failed: %s. Retrying in 5 seconds...", exc)
            await asyncio.sleep(5)


def main():
    setup_logging()
    try:
        asyncio.run(run_hub())
    except KeyboardInterrupt:
        print("\nIoT Hub stopped.")


if __name__ == "__main__":
    main()
