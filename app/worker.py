"""Independent consumer with atomic claims, heartbeat and guarded execution."""

import argparse
import asyncio
import json
import logging
import os
from uuid import UUID, uuid4

from aio_pika.abc import AbstractIncomingMessage

from app.broker import Broker
from app.config import get_settings
from app.db.execution_store import RunAlreadyRunning
from app.db.models import Run
from app.db.run_store import Claim, LeaseLost, get_run, renew_lease, try_claim_run
from app.db.session import close_db, init_db
from app.decision_provider import DecisionProvider
from app.engine import execute_run


logger = logging.getLogger(__name__)


async def heartbeat_loop(claim: Claim) -> None:
    interval = get_settings().heartbeat_interval_seconds
    while True:
        await asyncio.sleep(interval)
        try:
            renewed = await asyncio.wait_for(renew_lease(claim), timeout=interval)
        except Exception as error:
            raise LeaseLost("Lease renewal could not be confirmed") from error
        if not renewed:
            raise LeaseLost("Lease no longer belongs to this acquisition")


async def execute_with_lease(
    run_id: UUID,
    decision_provider: DecisionProvider,
    worker_id: str | None = None,
) -> Run:
    worker_id = worker_id or f"manual:{os.getpid()}:{uuid4().hex}"
    claim = await try_claim_run(run_id, worker_id)
    if claim is None:
        run = await get_run(run_id)
        if run is None:
            raise LookupError("Run not found")
        if run.status in {"COMPLETED", "FAILED", "CANCELLED"}:
            return run
        raise RunAlreadyRunning("Another acquisition has a valid lease")

    logger.info("Claim acquired: run=%s worker=%s claim=%s", run_id, worker_id, claim.claim_id)
    execution = asyncio.create_task(execute_run(claim, decision_provider))
    heartbeat = asyncio.create_task(heartbeat_loop(claim))
    try:
        done, _ = await asyncio.wait({execution, heartbeat}, return_when=asyncio.FIRST_COMPLETED)
        if execution in done:
            return execution.result()
        heartbeat.result()  # A heartbeat ending means ownership could not be maintained.
        raise LeaseLost("Heartbeat stopped unexpectedly")
    finally:
        execution.cancel()
        heartbeat.cancel()
        await asyncio.gather(execution, heartbeat, return_exceptions=True)


class Worker:
    def __init__(self, decision_provider: DecisionProvider) -> None:
        self.decision_provider = decision_provider
        self.broker = Broker()
        self.slots = asyncio.Semaphore(get_settings().worker_concurrency)
        self.stop = asyncio.Event()
        self.failed = False
        self.worker_id = f"worker:{os.getpid()}:{uuid4().hex}"
        self.active_tasks = set()

    async def handle_delivery(self, message: AbstractIncomingMessage) -> None:
        task = asyncio.current_task()
        self.active_tasks.add(task)
        try:
            await self._handle_delivery(message)
        finally:
            self.active_tasks.discard(task)

    async def _handle_delivery(self, message: AbstractIncomingMessage) -> None:
        async with self.slots:
            try:
                payload = json.loads(message.body)
                if not isinstance(payload, dict) or set(payload) != {"run_id"} or not isinstance(payload["run_id"], str):
                    raise ValueError("Invalid run message")
                run_id = UUID(payload["run_id"])
            except (ValueError, TypeError, UnicodeDecodeError):
                await message.reject(requeue=False)
                logger.warning("Invalid message sent to dead-letter queue")
                return

            try:
                while True:
                    try:
                        result = await execute_with_lease(run_id, self.decision_provider, self.worker_id)
                        break
                    except RunAlreadyRunning:
                        # Keep delivery unacked while another healthy executor finishes.
                        # On expiry, the next loop can acquire a fresh claim.
                        await asyncio.sleep(1)
                if result.status not in {"COMPLETED", "FAILED", "CANCELLED"}:
                    raise RuntimeError("Execution did not reach a persisted terminal state")
                await message.ack()
                logger.info("ACK run=%s status=%s", run_id, result.status)
            except LookupError:
                await message.reject(requeue=False)
                logger.warning("Unknown run sent to dead-letter queue: %s", run_id)
            except LeaseLost:
                # Heartbeat/execution have stopped; a new receiver waits for any remaining lease.
                await message.nack(requeue=True)
                logger.warning("Lease lost; delivery requeued: %s", run_id)
            except Exception as error:
                logger.error("Worker stops with unacked delivery: run=%s error=%s", run_id, type(error).__name__)
                self.failed = True
                self.stop.set()

    async def run(self) -> None:
        try:
            await init_db()
            await self.broker.consume(self.handle_delivery)
            logger.info("Worker ready; queue=%s", get_settings().rabbitmq_queue)
            await self.stop.wait()
            if self.failed:
                raise RuntimeError("Worker stopped after a delivery failure")
        finally:
            active = list(self.active_tasks)
            for task in active:
                task.cancel()
            await asyncio.gather(*active, return_exceptions=True)
            try:
                await self.broker.close()  # Unacked deliveries become eligible for redelivery.
            finally:
                await close_db()


def main() -> None:
    parser = argparse.ArgumentParser(description="Consume Agent execution tasks")
    parser.add_argument("--decision-provider", choices=["fake-caller"], required=True)
    args = parser.parse_args()
    if args.decision_provider == "fake-caller":
        # Explicit demonstration bootstrap, not a built-in production LLM dependency.
        from app.fake_llm import FakeCallerDecisionProvider
        provider = FakeCallerDecisionProvider()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    try:
        asyncio.run(Worker(provider).run())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
