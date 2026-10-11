"""Integration checks for the minimal engine; fixtures persist for inspection."""

import asyncio
from unittest.mock import patch

from sqlalchemy import func, select

from app.db.models import Inventory, Step
from app.db.run_store import create_run, request_cancel
from app.db.session import close_db, session_scope
from app.worker import execute_with_lease as execute_run
from app.fake_llm import FakeCallerDecisionProvider


async def count_steps(run_id):
    async with session_scope() as session:
        return await session.scalar(select(func.count()).select_from(Step).where(Step.run_id == run_id))


async def make_run(task="check_inventory", sku="BOOK-001", case="normal"):
    return await create_run({"task": task, "input": {"sku": sku, "test_case": case}})


async def main():
    provider = FakeCallerDecisionProvider()
    try:
        run = await make_run()
        async with session_scope() as session:
            available = (await session.get(Inventory, "BOOK-001")).available
        result = await execute_run(run.id, provider)
        assert result.status == "COMPLETED" and result.iteration_count == 2
        assert result.result_payload == {"sku": "BOOK-001", "available": available, "source": "postgresql"}
        assert await count_steps(run.id) == 3
        repeated = await execute_run(run.id, provider)
        assert repeated.status == "COMPLETED" and await count_steps(run.id) == 3

        cancelled = await make_run(case="cancel-before-start")
        await request_cancel(cancelled.id)
        result = await execute_run(cancelled.id, provider)
        assert result.status == "CANCELLED" and await count_steps(cancelled.id) == 0

        during = await make_run(case="cancel-before-tool")
        class CancelBeforeToolProvider:
            async def decide(self, context):
                await request_cancel(during.id)
                return await provider.decide(context)
        with patch("app.engine.execute_tool", side_effect=AssertionError("Tool must not run after cancellation")):
            result = await execute_run(during.id, CancelBeforeToolProvider())
        assert result.status == "CANCELLED" and await count_steps(during.id) == 0

        for task, sku, code in (
            ("unsupported-engine-check", "BOOK-001", "UNSUPPORTED_TASK"),
            ("check_inventory", "", "INVALID_ACTION"),
            ("check_inventory", "UNKNOWN-SKU", "TOOL_ERROR"),
        ):
            invalid = await make_run(task=task, sku=sku, case=code)
            result = await execute_run(invalid.id, provider)
            assert result.status == "FAILED" and result.error_payload["code"] == code

        limited = await make_run(case="iteration-budget")
        class RepeatingToolProvider:
            async def decide(self, context):
                return {"kind": "TOOL_CALL", "tool_name": "get_inventory", "arguments": {"sku": "BOOK-001"}}
        result = await execute_run(limited.id, RepeatingToolProvider())
        assert result.status == "FAILED" and result.error_payload["code"] == "MAX_ITERATIONS"
        assert result.iteration_count == result.max_iterations == 5
        assert await count_steps(limited.id) == 10

        alternate = await make_run(case="alternate-decision-provider")
        class ImmediateFinalProvider:
            async def decide(self, context):
                assert context["run_id"] == str(alternate.id)
                assert context["iteration"] == 1
                return {"kind": "FINAL", "result": {"message": "Caller chose to finish immediately"}}
        result = await execute_run(alternate.id, ImmediateFinalProvider())
        assert result.status == "COMPLETED" and result.iteration_count == 1
        assert result.result_payload == {"message": "Caller chose to finish immediately"}
        assert await count_steps(alternate.id) == 1

        print("Normal two-round loop and three persisted steps: verified")
        print("Terminal replay does not add steps; cancellation before start/tool: verified")
        print("Unsupported task, invalid action, tool error and iteration budget: verified")
        print("Same Engine accepts an alternate caller provider without modification: verified")
    finally:
        await close_db()


if __name__ == "__main__":
    asyncio.run(main())
