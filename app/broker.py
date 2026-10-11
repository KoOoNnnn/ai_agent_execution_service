"""Durable run-message delivery; PostgreSQL remains the business source of truth."""

import asyncio
from collections.abc import Awaitable, Callable
import json
import logging
from typing import Literal
from uuid import UUID

import aio_pika
from aio_pika.abc import AbstractIncomingMessage
from pamqp.commands import Basic

from app.config import get_settings


DispatchStatus = Literal["CONFIRMED", "UNCONFIRMED"]
logger = logging.getLogger(__name__)


class Broker:
    def __init__(self) -> None:
        self.connection = None
        self.channel = None
        self.queue = None
        self._connect_lock = asyncio.Lock()

    async def connect(self) -> None:
        async with self._connect_lock:
            if self.channel is not None and not self.channel.is_closed and self.queue is not None:
                return
            settings = get_settings()
            self.connection = await aio_pika.connect_robust(
                str(settings.rabbitmq_url), timeout=settings.broker_timeout_seconds,
            )
            self.channel = await self.connection.channel(publisher_confirms=True, on_return_raises=True)
            dead_exchange = await self.channel.declare_exchange(
                settings.rabbitmq_queue + ".dlx", aio_pika.ExchangeType.DIRECT, durable=True,
            )
            dead_queue = await self.channel.declare_queue(settings.rabbitmq_queue + ".dead", durable=True)
            await dead_queue.bind(dead_exchange, routing_key="dead")
            self.queue = await self.channel.declare_queue(
                settings.rabbitmq_queue,
                durable=True,
                arguments={"x-dead-letter-exchange": dead_exchange.name, "x-dead-letter-routing-key": "dead"},
            )

    async def _publish(self, run_id: UUID) -> None:
        await self.connect()
        confirmed = await self.channel.default_exchange.publish(
            aio_pika.Message(
                body=json.dumps({"run_id": str(run_id)}).encode(),
                content_type="application/json",
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                message_id=str(run_id),
            ),
            routing_key=self.queue.name,
            mandatory=True,
            timeout=get_settings().broker_timeout_seconds,
        )
        if not isinstance(confirmed, Basic.Ack):
            raise RuntimeError("Publish did not receive a positive confirmation")

    async def publish_run(self, run_id: UUID) -> DispatchStatus:
        try:
            await asyncio.wait_for(self._publish(run_id), timeout=get_settings().broker_timeout_seconds)
            return "CONFIRMED"
        except Exception as error:
            logger.warning("Publish unconfirmed: run=%s error=%s", run_id, type(error).__name__)
            return "UNCONFIRMED"

    async def consume(self, handler: Callable[[AbstractIncomingMessage], Awaitable[None]]) -> None:
        await self.connect()
        await self.channel.set_qos(prefetch_count=get_settings().worker_prefetch)
        await self.queue.consume(handler, no_ack=False)

    async def close(self) -> None:
        if self.connection is not None and not self.connection.is_closed:
            await self.connection.close()
        self.connection = self.channel = self.queue = None


broker = Broker()  # API publisher; Worker uses its own process-local instance.
