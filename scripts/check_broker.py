"""Verify the local RabbitMQ connection: python -m scripts.check_broker"""

import asyncio

from app.broker import Broker


async def main() -> None:
    broker = Broker()
    try:
        await broker.connect()
        print("RabbitMQ connection OK; durable queue:", broker.queue.name)
    finally:
        await broker.close()


if __name__ == "__main__":
    asyncio.run(main())
