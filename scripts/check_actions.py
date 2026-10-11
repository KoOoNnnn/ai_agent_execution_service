"""Fault-injection checks for persisted decisions; only own fixture runs are changed."""

import asyncio
from unittest.mock import patch

from sqlalchemy import func, select, update

from app.db import execution_store, run_store
from app.db.models import Action, Run, Step
from app.db.session import close_db, session_scope
from app.engine import execute_run
from app.fake_llm import FakeCallerDecisionProvider
from app.tools import execute_tool
from app.worker import execute_with_lease


class CountingCaller(FakeCallerDecisionProvider):
    def __init__(self):
        self.iterations = []

    async def decide(self, context):
        self.iterations.append(context["iteration"])
        return await super().decide(context)


async def make_run(case):
    return await run_store.create_run({
        "task": "check_inventory", "input": {"sku": "BOOK-001", "test_case": case},
    })


async def expire(run_id):
    async with session_scope() as session:
        async with session.begin():
            await session.execute(update(Run).where(Run.id == run_id).values(
                lease_expires_at=func.clock_timestamp(),
            ))


async def recover_tool_boundary(after_tool):
    run = await make_run(f"action-recovery-after-tool-{after_tool}")
    old = await run_store.try_claim_run(run.id, "action-check-old")
    caller = CountingCaller()
    entered = asyncio.Event()

    async def interrupted_tool(claim, action, operation_id):
        if after_tool:
            await execute_tool(claim, action, operation_id)
        entered.set()
        await asyncio.sleep(60)

    with patch("app.engine.execute_tool", side_effect=interrupted_tool):
        task = asyncio.create_task(execute_run(old, caller))
        try:
            await asyncio.wait_for(entered.wait(), timeout=5)
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    saved = await execution_store.load_action(old, 1)
    assert saved is not None and saved.status == "DECIDED"
    assert (await run_store.get_run(run.id)).iteration_count == 0
    duplicate = await execution_store.save_action(old, 1, saved.input_payload, saved.decision_payload)
    assert duplicate.id == saved.id
    try:
        await execution_store.save_action(old, 1, saved.input_payload, {"kind": "FINAL", "result": {}})
    except RuntimeError:
        pass
    else:
        raise AssertionError("Saved decision was replaced")

    await expire(run.id)
    new = await run_store.try_claim_run(run.id, "action-check-new")
    for operation in (
        execution_store.load_action(old, 1),
        execution_store.save_action(old, 1, saved.input_payload, saved.decision_payload),
    ):
        try:
            await operation
        except run_store.LeaseLost:
            pass
        else:
            raise AssertionError("Stale claim accessed the action")

    result = await execute_run(new, caller)
    assert result.status == "COMPLETED" and result.iteration_count == 2
    assert caller.iterations == [1, 2], "Recovery called the provider again for iteration 1"
    async with session_scope() as session:
        actions = list(await session.scalars(select(Action).where(
            Action.run_id == run.id,
        ).order_by(Action.iteration_index)))
        assert len(actions) == 2 and all(a.status == "COMPLETED" for a in actions)
        assert actions[0].id == saved.id
        assert await session.scalar(select(func.count()).select_from(Step).where(Step.run_id == run.id)) == 3
    replay = await execute_with_lease(run.id, caller)
    assert replay.status == "COMPLETED" and caller.iterations == [1, 2]


async def main():
    try:
        for after_tool in (False, True):
            await recover_tool_boundary(after_tool)
        print("Before/after read-only tool interruption: saved decision reused; stable ID; no duplicate steps")
        print("Identical save is idempotent; changed decision and stale owner access rejected")

        run = await make_run("action-final-recovery")
        old = await run_store.try_claim_run(run.id, "final-check-old")
        final = {"kind": "FINAL", "result": {"saved": True}}
        await execution_store.save_action(old, 1, {"iteration": 1}, final)
        await expire(run.id)

        class MustNotCall:
            async def decide(self, context):
                raise AssertionError("A saved FINAL does not require a new decision")

        result = await execute_with_lease(run.id, MustNotCall())
        assert result.status == "COMPLETED" and result.result_payload == final["result"]
        print("Saved FINAL resumes without calling DecisionProvider")
    finally:
        await close_db()


if __name__ == "__main__":
    asyncio.run(main())
