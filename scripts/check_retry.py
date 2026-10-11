"""Decision retry integration checks; fixtures persist for inspection."""

import asyncio
from unittest.mock import patch

from app.config import get_settings
from app.db.run_store import create_run, request_cancel, try_claim_run
from app.db.session import close_db
from app.decision_provider import DecisionProviderError, TransientDecisionProviderError
from app.fake_llm import FakeCallerDecisionProvider
from app.worker import execute_with_lease


async def make_run(case):
    return await create_run({"task": "check_inventory", "input": {"sku": "BOOK-001", "test_case": case}})


async def main():
    settings = get_settings()
    fast = settings.model_copy(update={
        "decision_timeout_seconds": 0.05,
        "decision_max_attempts": 3,
        "decision_retry_base_seconds": 0.01,
        "decision_retry_jitter_seconds": 0,
    })
    try:
        with patch("app.engine.get_settings", return_value=fast):
            class FlakyCaller(FakeCallerDecisionProvider):
                def __init__(self):
                    self.first_round_calls = 0
                async def decide(self, context):
                    if context["iteration"] == 1:
                        self.first_round_calls += 1
                        if self.first_round_calls < 3:
                            raise TransientDecisionProviderError("Temporary upstream failure")
                    return await super().decide(context)
            caller = FlakyCaller()
            run = await make_run("retry-success")
            result = await execute_with_lease(run.id, caller)
            assert result.status == "COMPLETED" and result.iteration_count == 2
            assert caller.first_round_calls == 3
            print("Two transient failures then success: exactly three attempts, still two iterations")

            class TimeoutCaller:
                def __init__(self):
                    self.calls = 0
                    self.cancelled = 0
                async def decide(self, context):
                    self.calls += 1
                    try:
                        await asyncio.sleep(60)
                    except asyncio.CancelledError:
                        self.cancelled += 1
                        raise
            caller = TimeoutCaller()
            run = await make_run("retry-timeout-exhaustion")
            result = await execute_with_lease(run.id, caller)
            assert result.status == "FAILED" and result.error_payload["code"] == "DECISION_RETRY_EXHAUSTED"
            assert result.iteration_count == 0 and caller.calls == caller.cancelled == 3
            print("Three timed-out calls: FAILED; timeout cancels each local coroutine")

            class PermanentCaller:
                def __init__(self):
                    self.calls = 0
                async def decide(self, context):
                    self.calls += 1
                    raise DecisionProviderError("Caller authentication failed", code="AUTH_FAILED")
            caller = PermanentCaller()
            result = await execute_with_lease((await make_run("retry-permanent-error")).id, caller)
            assert result.status == "FAILED" and result.error_payload["code"] == "AUTH_FAILED"
            assert caller.calls == 1

            class InvalidCaller:
                def __init__(self):
                    self.calls = 0
                async def decide(self, context):
                    self.calls += 1
                    return {"kind": "TOOL_CALL", "tool_name": "unregistered", "arguments": {}}
            caller = InvalidCaller()
            result = await execute_with_lease((await make_run("retry-invalid-action")).id, caller)
            assert result.status == "FAILED" and result.error_payload["code"] == "INVALID_ACTION"
            assert caller.calls == 1
            print("Authentication errors and invalid actions: no retry")

        entered = asyncio.Event()
        cancel_case = await make_run("cancel-during-retry")
        class CancelRetryCaller:
            def __init__(self):
                self.calls = 0
            async def decide(self, context):
                self.calls += 1
                entered.set()
                raise ConnectionError("Temporary connection failure")
        caller = CancelRetryCaller()
        cancel_timing = fast.model_copy(update={"decision_retry_base_seconds": 0.5})
        with patch("app.engine.get_settings", return_value=cancel_timing):
            task = asyncio.create_task(execute_with_lease(cancel_case.id, caller))
            await asyncio.wait_for(entered.wait(), timeout=3)
            await request_cancel(cancel_case.id)
            result = await task
        assert result.status == "CANCELLED" and caller.calls == 1
        print("Cancellation during backoff prevents the next attempt")

        # Use short lease timing in this check process, separate from the normal Worker.
        entered = asyncio.Event()
        long_case = await make_run("retry-heartbeat")
        class SlowFlakyCaller(FakeCallerDecisionProvider):
            def __init__(self):
                self.calls = 0
            async def decide(self, context):
                if context["iteration"] == 1:
                    self.calls += 1
                    entered.set()
                    if self.calls < 3:
                        await asyncio.sleep(0.8)
                        raise TransientDecisionProviderError("Temporary upstream delay")
                    await asyncio.sleep(0.6)
                return await super().decide(context)
        caller = SlowFlakyCaller()
        long_timing = fast.model_copy(update={"decision_timeout_seconds": 1, "decision_retry_base_seconds": 0.1})
        with patch("app.engine.get_settings", return_value=long_timing):
            task = asyncio.create_task(execute_with_lease(long_case.id, caller, "retry-heartbeat-owner"))
            await asyncio.wait_for(entered.wait(), timeout=3)
            await asyncio.sleep(settings.lease_ttl_seconds + 0.1)
            assert not task.done(), "Use test lease TTL 2s / heartbeat 0.5s for this check"
            assert await try_claim_run(long_case.id, "retry-takeover-attempt") is None
            result = await task
        assert result.status == "COMPLETED" and caller.calls == 3
        print("Retry sequence exceeds initial lease, but independent heartbeat retains ownership")
    finally:
        await close_db()


if __name__ == "__main__":
    asyncio.run(main())
