"""Verify the live read-only Steps API with isolated local Run/stock fixtures."""

import asyncio
import base64
import json
import os
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import urlopen
from uuid import uuid4

from app.db.models import Inventory
from app.db.run_store import create_run, request_cancel
from app.db.session import close_db, session_scope
from app.fake_llm import FakeCallerDecisionProvider
from app.worker import execute_with_lease

BASE = os.environ.get("API_BASE_URL", "http://127.0.0.1:8000").rstrip("/")

def request(run_id, **params):
    url = f"{BASE}/runs/{run_id}/steps"
    if params:
        url += "?" + urlencode(params)
    try:
        with urlopen(url, timeout=5) as response:
            return response.status, json.loads(response.read())
    except HTTPError as error:
        return error.code, json.loads(error.read())


async def read(run_id, **params):
    return await asyncio.to_thread(request, run_id, **params)


async def main():
    try:
        pending = await create_run({"task": "steps-pending", "input": {}})
        code, empty = await read(pending.id)
        assert code == 200 and empty["status"] == "PENDING"
        assert empty["items"] == empty["decision_attempts"] == [] and empty["next_cursor"] is None
        assert await read(pending.id) == (code, empty), "Read endpoint mutated the task"

        sku = "steps-test-" + uuid4().hex
        async with session_scope() as session:
            async with session.begin():
                session.add(Inventory(sku=sku, available=10))
        reserved = await create_run({"task": "reserve_inventory", "input": {"sku": sku, "quantity": 2}})
        result = await execute_with_lease(reserved.id, FakeCallerDecisionProvider())
        assert result.status == "COMPLETED"
        code, full = await read(reserved.id)
        assert code == 200 and full["status"] == "COMPLETED" and full["error_payload"] is None
        expected_order = [(1, "LLM_CALL"), (1, "TOOL_CALL"), (2, "LLM_CALL"), (2, "TOOL_CALL"), (3, "LLM_CALL")]
        assert [(step["iteration_index"], step["step_type"]) for step in full["items"]] == expected_order
        assert full["decision_attempts"] == [{"iteration_index": i, "attempt_count": 1} for i in (1, 2, 3)]
        assert full["items"][3]["output_payload"]["available"] == 8
        assert full["items"][4]["output_payload"]["kind"] == "FINAL"
        assert not {"claim_id", "worker_id", "lease_expires_at"}.intersection(full)

        collected = []
        cursor = None
        first_cursor = None
        for _ in range(6):
            params = {"limit": 1}
            if cursor:
                params["cursor"] = cursor
            code, page = await read(reserved.id, **params)
            assert code == 200 and len(page["items"]) == 1
            collected.extend(page["items"])
            cursor = page["next_cursor"]
            first_cursor = first_cursor or cursor
            if cursor is None:
                break
        else:
            raise AssertionError("Pagination did not terminate")
        assert collected == full["items"] and len({item["id"] for item in collected}) == 5
        assert (await read(reserved.id, limit=100))[1]["items"] == full["items"]
        print("Reservation trace: 3 rounds / 5 steps, decisions before tools; page size 1 has no duplicates/skips")

        for params in ({"limit": 0}, {"limit": 101}, {"limit": "x"}, {"cursor": ""}, {"cursor": "%%%"}):
            assert (await read(reserved.id, **params))[0] == 422
        assert (await read(pending.id, cursor=first_cursor))[0] == 422
        invalid_position = base64.urlsafe_b64encode(json.dumps([1, str(reserved.id), True, "LLM_CALL"]).encode()).decode()
        assert (await read(reserved.id, cursor=invalid_position))[0] == 422
        assert (await read(uuid4()))[0] == 404
        assert (await read("not-a-uuid"))[0] == 422
        print("Missing Run 404; invalid UUID/limit/cursor and cross-run cursor 422; pending trace empty")

        failed = await create_run({"task": "unsupported-steps-check", "input": {}})
        await execute_with_lease(failed.id, FakeCallerDecisionProvider())
        code, page = await read(failed.id)
        assert code == 200 and page["status"] == "FAILED" and page["items"] == []
        assert page["error_payload"]["code"] == "UNSUPPORTED_TASK"
        assert page["decision_attempts"] == [{"iteration_index": 1, "attempt_count": 1}]
        cancelled = await create_run({"task": "check_inventory", "input": {"sku": sku}})
        await request_cancel(cancelled.id)
        await execute_with_lease(cancelled.id, FakeCallerDecisionProvider())
        assert (await read(cancelled.id))[1]["status"] == "CANCELLED"
        assert (await read(cancelled.id))[1]["items"] == []
        print("Failure without completed Steps still exposes Run error and attempts; cancelled trace readable")

        entered, resume = asyncio.Event(), asyncio.Event()
        class PausedCaller(FakeCallerDecisionProvider):
            async def decide(self, context):
                if context["iteration"] == 2:
                    entered.set()
                    await resume.wait()
                return await super().decide(context)

        active = await create_run({"task": "check_inventory", "input": {"sku": sku}})
        task = asyncio.create_task(execute_with_lease(active.id, PausedCaller()))
        try:
            await asyncio.wait_for(entered.wait(), timeout=5)
            code, first = await read(active.id, limit=1)
            assert code == 200 and first["status"] == "RUNNING" and first["next_cursor"]
            assert first["decision_attempts"] == [{"iteration_index": i, "attempt_count": 1} for i in (1, 2)]
            resume.set()
            assert (await asyncio.wait_for(task, timeout=5)).status == "COMPLETED"
            _, rest = await read(active.id, cursor=first["next_cursor"])
            assert rest["status"] == "COMPLETED"
            assert [item["step_type"] for item in rest["items"]] == ["TOOL_CALL", "LLM_CALL"]
        finally:
            resume.set()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        print("RUNNING trace includes in-flight decision count; cursor continues correctly after a new round commits")
        print("Example completed Run:", reserved.id)
    finally:
        await close_db()


if __name__ == "__main__":
    asyncio.run(main())
