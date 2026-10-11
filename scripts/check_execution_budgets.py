"""Durable retry/deadline checks; only newly created fixture runs are modified."""

import asyncio
from unittest.mock import patch
from uuid import uuid4

from sqlalchemy import func, select, update

from app.config import get_settings
from app.db import execution_store, inventory_store, run_store
from app.db.models import DecisionBudget, Inventory, Run, ToolEffect
from app.db.session import close_db, session_scope
from app.engine import execute_run
from app.worker import execute_with_lease


async def make_run(case, *, seconds=300, maximum=3, key=None):
    limits = get_settings().model_copy(update={"run_timeout_seconds": seconds, "decision_max_attempts": maximum})
    with patch("app.db.run_store.get_settings", return_value=limits):
        payload = {"task": "check_inventory", "input": {"sku": "BOOK-001", "test_case": case}}
        if key:
            return (await run_store.create_or_get_run(payload, request_key=key))[0]
        return await run_store.create_run(payload)


async def expire(claim):
    async with session_scope() as session:
        async with session.begin():
            await session.execute(update(Run).where(Run.id == claim.run_id).values(lease_expires_at=func.clock_timestamp()))


async def attempts(run_id, iteration=1):
    async with session_scope() as session:
        budget = await session.get(DecisionBudget, (run_id, iteration))
        return budget.attempt_count if budget else 0


class FinalCaller:
    def __init__(self):
        self.calls = 0

    async def decide(self, context):
        self.calls += 1
        return {"kind": "FINAL", "result": {"done": True}}


async def crash_during_attempt(run, stop_at):
    entered = asyncio.Event()
    old = await run_store.try_claim_run(run.id, "budget-old")

    class InterruptedCaller:
        calls = 0

        async def decide(self, context):
            self.calls += 1
            if self.calls == stop_at:
                entered.set()
                await asyncio.sleep(60)
            raise ConnectionError("Temporary test failure")

    caller = InterruptedCaller()
    task = asyncio.create_task(execute_run(old, caller))
    try:
        await asyncio.wait_for(entered.wait(), timeout=5)
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    assert caller.calls == await attempts(run.id) == stop_at
    await expire(old)
    return old


async def main():
    fast = get_settings().model_copy(update={"decision_retry_base_seconds": 0.01, "decision_retry_jitter_seconds": 0})
    try:
        with patch("app.engine.get_settings", return_value=fast):
            partial = await make_run("durable-partial")
            old = await crash_during_attempt(partial, 2)
            new = await run_store.try_claim_run(partial.id, "budget-new")
            try:
                await execution_store.begin_decision_attempt(old, 1)
            except run_store.LeaseLost:
                pass
            else:
                raise AssertionError("Stale owner spent the retry budget")
            caller = FinalCaller()
            result = await execute_run(new, caller)
            assert result.status == "COMPLETED" and caller.calls == 1 and await attempts(partial.id) == 3

            exhausted = await make_run("durable-exhausted")
            await crash_during_attempt(exhausted, 3)
            caller = FinalCaller()
            result = await execute_with_lease(exhausted.id, caller)
            assert result.status == "FAILED" and result.error_payload["code"] == "DECISION_RETRY_EXHAUSTED"
            assert caller.calls == 0 and await attempts(exhausted.id) == 3
            print("Crash after two attempts: one remains; crash on third: no fourth call; stale claim rejected")

            fixed = await make_run("fixed-attempt-limit", maximum=1)
            class AlwaysTransient:
                async def decide(self, context):
                    raise ConnectionError("Transient failure")
            result = await execute_with_lease(fixed.id, AlwaysTransient())
            assert result.error_payload["code"] == "DECISION_RETRY_EXHAUSTED" and await attempts(fixed.id) == 1
            print("Attempt limit is snapshotted in Run; Worker configuration cannot reset it")

        queued = await make_run("expired-in-queue", seconds=0.15)
        await asyncio.sleep(0.2)
        caller = FinalCaller()
        result = await execute_with_lease(queued.id, caller)
        assert result.error_payload["code"] == "RUN_DEADLINE_EXCEEDED" and caller.calls == 0
        assert result.claim_id is None and result.deadline_at == queued.deadline_at
        print("Queue time counts: expired task fails before caller I/O, releases ownership")

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

        slow = await make_run("deadline-during-call", seconds=0.4)
        result = await execute_with_lease(slow.id, WaitingCaller())
        assert entered.is_set() and cancelled.is_set()
        assert result.error_payload["code"] == "RUN_DEADLINE_EXCEEDED" and await attempts(slow.id) == 1

        backing_off = await make_run("deadline-during-backoff", seconds=0.4)
        caller_calls = []
        class TransientCaller:
            async def decide(self, context):
                caller_calls.append(1)
                raise ConnectionError("Transient failure")
        long_backoff = fast.model_copy(update={"decision_retry_base_seconds": 2})
        with patch("app.engine.get_settings", return_value=long_backoff):
            result = await execute_with_lease(backing_off.id, TransientCaller())
        assert result.error_payload["code"] == "RUN_DEADLINE_EXCEEDED" and len(caller_calls) == 1
        print("Deadline interrupts an in-flight call and retry backoff without spending another attempt")

        key = "deadline-check-" + uuid4().hex
        first = await make_run("deadline-idempotency", key=key, seconds=5)
        replay = await make_run("deadline-idempotency", key=key, seconds=50, maximum=10)
        assert replay.id == first.id and replay.deadline_at == first.deadline_at and replay.decision_max_attempts == 3
        print("Idempotent resubmission preserves deadline and retry allowance")

        # An action saved before expiry still cannot produce a new stock effect afterwards.
        sku = "deadline-test-" + uuid4().hex
        async with session_scope() as session:
            async with session.begin():
                session.add(Inventory(sku=sku, available=10))
        expired_tool = await make_run("deadline-before-stock", seconds=0.3)
        owner = await run_store.try_claim_run(expired_tool.id, "deadline-stock")
        saved = await execution_store.save_action(owner, 1, {}, {
            "kind": "TOOL_CALL", "tool_name": "reserve_inventory", "arguments": {"sku": sku, "quantity": 2},
        })
        await asyncio.sleep(0.35)
        try:
            await inventory_store.reserve_once(owner, saved.id, sku, 2)
        except run_store.RunDeadlineExceeded:
            pass
        else:
            raise AssertionError("Expired task changed stock")
        result = await execution_store.finish_failure(owner, "OTHER_ERROR", "Late failure")
        assert result.error_payload["code"] == "RUN_DEADLINE_EXCEEDED"
        async with session_scope() as session:
            assert (await session.get(Inventory, sku)).available == 10
            assert await session.get(ToolEffect, saved.id) is None
        print("Expired action cannot deduct stock; deadline takes priority over a late failure")
    finally:
        await close_db()


if __name__ == "__main__":
    asyncio.run(main())
