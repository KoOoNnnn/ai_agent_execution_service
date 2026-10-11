"""Remove only the exact queues/exchange allocated to an isolated test session."""

import asyncio
import os
import re

from app.broker import Broker


async def main():
    name = os.environ.get("RABBITMQ_QUEUE", "")
    if not re.fullmatch(r"agent\.tests\.[0-9a-f]{32}", name):
        raise SystemExit("Refusing to remove a queue outside an isolated test session")
    broker = Broker()
    try:
        await broker.connect()
        await broker.channel.queue_delete(name, if_unused=True, if_empty=False)
        await broker.channel.queue_delete(name + ".dead", if_unused=True, if_empty=False)
        await broker.channel.exchange_delete(name + ".dlx", if_unused=True)
    finally:
        await broker.close()


if __name__ == "__main__":
    asyncio.run(main())
