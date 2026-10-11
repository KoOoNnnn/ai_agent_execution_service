"""Test-only Worker pausing after saved progress or a committed reservation."""

import asyncio
import logging
from pathlib import Path
import sys

from app.fake_llm import FakeCallerDecisionProvider
from app.decision_provider import TransientDecisionProviderError
from app.worker import Worker
from app import engine
from app.tools import execute_tool


class PausingCaller(FakeCallerDecisionProvider):
    async def decide(self, context):
        if context["run_id"] == sys.argv[1] and context["iteration"] == 2:
            Path(sys.argv[2]).write_text("First iteration persisted; second decision paused", encoding="utf-8")
            await asyncio.sleep(60)
        return await super().decide(context)


class PausingRetryCaller(FakeCallerDecisionProvider):
    calls = 0

    async def decide(self, context):
        if context["run_id"] == sys.argv[1] and context["iteration"] == 1:
            self.calls += 1
            if self.calls < 3:
                raise TransientDecisionProviderError("Temporary fixture failure")
            Path(sys.argv[2]).write_text("Third attempt reserved; caller paused", encoding="utf-8")
            await asyncio.sleep(60)
        return await super().decide(context)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    if len(sys.argv) > 3 and sys.argv[3] == "after-reservation":
        async def pause_after_reservation(claim, action, operation_id):
            result = await execute_tool(claim, action, operation_id)
            if str(claim.run_id) == sys.argv[1] and action.tool_name == "reserve_inventory":
                Path(sys.argv[2]).write_text("Reservation committed; iteration not saved", encoding="utf-8")
                await asyncio.sleep(60)
            return result

        engine.execute_tool = pause_after_reservation
        provider = FakeCallerDecisionProvider()
    elif len(sys.argv) > 3 and sys.argv[3] == "during-retry":
        provider = PausingRetryCaller()
    else:
        provider = PausingCaller()
    asyncio.run(Worker(provider).run())
