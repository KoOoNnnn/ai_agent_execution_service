"""Stock/effect integration checks; each case uses a new, isolated test SKU."""

import asyncio
from unittest.mock import patch
from uuid import uuid4

from sqlalchemy import func, select, update

from app.db import execution_store, inventory_store, run_store
from app.db.models import Inventory, Reservation, Run, ToolEffect, Step
from app.db.session import close_db, session_scope
from app.engine import execute_run
from app.fake_llm import FakeCallerDecisionProvider
from app.tools import execute_tool


async def stock_fixture(available):
    sku = "test-" + uuid4().hex
    async with session_scope() as session:
        async with session.begin():
            session.add(Inventory(sku=sku, available=available))
    return sku


async def operation_fixture(sku, quantity):
    run = await run_store.create_run({
        "task": "reserve_inventory", "input": {"sku": sku, "quantity": quantity},
    })
    claim = await run_store.try_claim_run(run.id, "inventory-check")
    action = await execution_store.save_action(claim, 1, {"iteration": 1}, {
        "kind": "TOOL_CALL", "tool_name": "reserve_inventory",
        "arguments": {"sku": sku, "quantity": quantity},
    })
    return claim, action


async def state(sku):
    async with session_scope() as session:
        stock = await session.get(Inventory, sku)
        reservations = await session.scalar(select(func.count()).select_from(Reservation).where(Reservation.sku == sku))
        effects = await session.scalar(select(func.count()).select_from(ToolEffect).where(
            ToolEffect.arguments_payload["sku"].astext == sku,
        ))
        return stock.available, reservations, effects


async def expire(run_id):
    async with session_scope() as session:
        async with session.begin():
            await session.execute(update(Run).where(Run.id == run_id).values(lease_expires_at=func.clock_timestamp()))


async def main():
    try:
        sku = await stock_fixture(10)
        claim, action = await operation_fixture(sku, 2)
        results = await asyncio.gather(*(
            inventory_store.reserve_once(claim, action.id, sku, 2) for _ in range(4)
        ))
        assert all(result == results[0] for result in results)
        assert await state(sku) == (8, 1, 1)
        try:
            await inventory_store.reserve_once(claim, action.id, sku, 3)
        except ValueError:
            pass
        else:
            raise AssertionError("Changed operation arguments were accepted")
        await expire(claim.run_id)
        try:
            await inventory_store.reserve_once(claim, action.id, sku, 2)
        except run_store.LeaseLost:
            pass
        else:
            raise AssertionError("Stale owner applied a business effect")
        print("Four concurrent replays: one deduction / reservation / effect; mismatch and stale claim rejected")

        scarce = await stock_fixture(3)
        pairs = [await operation_fixture(scarce, 2) for _ in range(2)]
        results = await asyncio.gather(*(
            inventory_store.reserve_once(owner, saved.id, scarce, 2) for owner, saved in pairs
        ))
        assert sum(result["reserved"] for result in results) == 1
        assert await state(scarce) == (1, 1, 2)
        failed_index = next(i for i, result in enumerate(results) if not result["reserved"])
        # Even after a stock replenishment, the rejected operation keeps its original result.
        async with session_scope() as session:
            async with session.begin():
                await session.execute(update(Inventory).where(Inventory.sku == scarce).values(available=10))
        owner, saved = pairs[failed_index]
        assert await inventory_store.reserve_once(owner, saved.id, scarce, 2) == results[failed_index]
        assert await state(scarce) == (10, 1, 2)
        print("Competing runs cannot oversell; insufficient-stock outcome is stable on replay")

        atomic = await stock_fixture(5)
        owner, saved = await operation_fixture(atomic, 2)
        with patch("app.db.inventory_store.Reservation", side_effect=RuntimeError("Injected insert failure")):
            try:
                await inventory_store.reserve_once(owner, saved.id, atomic, 2)
            except RuntimeError:
                pass
            else:
                raise AssertionError("Injected failure did not interrupt the transaction")
        assert await state(atomic) == (5, 0, 0)
        assert (await inventory_store.reserve_once(owner, saved.id, atomic, 2))["reserved"]
        assert await state(atomic) == (3, 1, 1)
        print("Failure after effect flush rolls back stock/effect together; retry succeeds once")

        cancelled = await stock_fixture(5)
        owner, saved = await operation_fixture(cancelled, 2)
        await run_store.request_cancel(owner.run_id)
        try:
            await inventory_store.reserve_once(owner, saved.id, cancelled, 2)
        except inventory_store.ToolCancelled:
            pass
        else:
            raise AssertionError("Cancellation did not prevent reservation")
        assert await state(cancelled) == (5, 0, 0)
        print("Cancellation before business commit prevents deduction")

        recovering = await stock_fixture(10)
        run = await run_store.create_run({"task": "reserve_inventory", "input": {"sku": recovering, "quantity": 2}})
        old = await run_store.try_claim_run(run.id, "effect-crash-old")
        entered = asyncio.Event()
        provider_calls = []

        class CountingCaller(FakeCallerDecisionProvider):
            async def decide(self, context):
                provider_calls.append(context["iteration"])
                return await super().decide(context)

        async def after_effect(claim, action, operation_id):
            result = await execute_tool(claim, action, operation_id)
            if action.tool_name == "reserve_inventory":
                entered.set()
                await asyncio.sleep(60)
            return result

        with patch("app.engine.execute_tool", side_effect=after_effect):
            task = asyncio.create_task(execute_run(old, CountingCaller()))
            try:
                await asyncio.wait_for(entered.wait(), timeout=5)
            finally:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
        assert await state(recovering) == (8, 1, 1)
        assert (await run_store.get_run(run.id)).iteration_count == 1
        saved = await execution_store.load_action(old, 2)
        assert saved.status == "DECIDED"
        await expire(run.id)
        new = await run_store.try_claim_run(run.id, "effect-crash-new")
        result = await execute_run(new, CountingCaller())
        assert result.status == "COMPLETED" and result.iteration_count == 3
        assert result.result_payload["operation_id"] == str(saved.id)
        assert provider_calls == [1, 2, 3]
        assert await state(recovering) == (8, 1, 1)
        async with session_scope() as session:
            assert await session.scalar(select(func.count()).select_from(Step).where(Step.run_id == run.id)) == 5
        print("Effect committed / progress interrupted: recovery reuses result, no second deduction, 3 rounds / 5 steps")
    finally:
        await close_db()


if __name__ == "__main__":
    asyncio.run(main())
