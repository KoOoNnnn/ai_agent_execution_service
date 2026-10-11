"""Integration checks with a short test lease, e.g. TTL 6s / heartbeat 1s."""

import asyncio

from sqlalchemy import func, select, update

from app.config import get_settings
from app.db.execution_store import finish_failure, save_iteration
from app.db.models import Run, Step
from app.db.run_store import LeaseLost, get_run, renew_lease, try_claim_run, create_run
from app.db.session import close_db, session_scope
from app.engine import execute_run
from app.fake_llm import FakeCallerDecisionProvider
from app.worker import execute_with_lease


async def make_run(case):
    return await create_run({"task": "check_inventory", "input": {"sku": "BOOK-001", "test_case": case}})


async def force_expired(run_id):
    async with session_scope() as session:
        async with session.begin():
            await session.execute(update(Run).where(Run.id == run_id).values(lease_expires_at=func.clock_timestamp()))


async def main():
    try:
        contested = await make_run("lease-competition")
        claims = await asyncio.gather(*(try_claim_run(contested.id, f"claim-test-{i}") for i in range(4)))
        winners = [claim for claim in claims if claim is not None]
        assert len(winners) == 1
        await finish_failure(winners[0], "TEST_FINISHED", "Atomic claim check completed")
        print("Four concurrent claim attempts: exactly one owner")

        stale = await make_run("stale-acquisition")
        old = await try_claim_run(stale.id, "same-worker")
        await force_expired(stale.id)
        assert await renew_lease(old) is False
        new = await try_claim_run(stale.id, "same-worker")
        assert new is not None and new.claim_id != old.claim_id
        for operation in (
            save_iteration(old, 1, [], final_result={"wrong": True}),
            finish_failure(old, "STALE_WRITE", "Must not overwrite the new owner"),
        ):
            try:
                await operation
            except LeaseLost:
                pass
            else:
                raise AssertionError("Stale owner wrote to the task")
        assert await renew_lease(old) is False
        assert (await execute_run(new, FakeCallerDecisionProvider())).status == "COMPLETED"
        print("Expired renewal and stale progress/final writes rejected, even for reused worker ID")

        healthy = await make_run("long-call-heartbeat")
        entered = asyncio.Event()
        class SlowCaller(FakeCallerDecisionProvider):
            async def decide(self, context):
                if context["iteration"] == 1:
                    entered.set()
                    await asyncio.sleep(get_settings().lease_ttl_seconds + 2)
                return await super().decide(context)
        task = asyncio.create_task(execute_with_lease(healthy.id, SlowCaller(), "healthy-long-call"))
        await asyncio.wait_for(entered.wait(), timeout=5)
        await asyncio.sleep(get_settings().lease_ttl_seconds + 0.5)
        assert await try_claim_run(healthy.id, "would-be-takeover") is None
        assert (await asyncio.wait_for(task, timeout=5)).status == "COMPLETED"
        print("Decision call longer than original lease: heartbeat keeps ownership valid")

        lost = await make_run("heartbeat-loss")
        entered = asyncio.Event()
        cancelled = asyncio.Event()
        class WaitingCaller:
            async def decide(self, context):
                entered.set()
                try:
                    await asyncio.sleep(60)
                except asyncio.CancelledError:
                    cancelled.set()
                    raise
        task = asyncio.create_task(execute_with_lease(lost.id, WaitingCaller(), "losing-owner"))
        await asyncio.wait_for(entered.wait(), timeout=5)
        await force_expired(lost.id)
        try:
            await asyncio.wait_for(task, timeout=get_settings().heartbeat_interval_seconds + 3)
        except LeaseLost:
            pass
        else:
            raise AssertionError("Lost ownership did not stop execution")
        assert cancelled.is_set()
        assert (await get_run(lost.id)).iteration_count == 0
        assert (await execute_with_lease(lost.id, FakeCallerDecisionProvider())).status == "COMPLETED"
        print("Heartbeat loss cancels execution; a new claim completes the task")
    finally:
        await close_db()


if __name__ == "__main__":
    asyncio.run(main())
