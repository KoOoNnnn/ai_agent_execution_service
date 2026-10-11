"""Create and execute one mock inventory task: python -m scripts.demo_engine"""

import asyncio

from app.db.run_store import create_run
from app.db.session import close_db, init_db
from app.worker import execute_with_lease
from app.fake_llm import FakeCallerDecisionProvider
from app.schemas import RunResponse


async def main() -> None:
    try:
        await init_db()
        run = await create_run({"task": "check_inventory", "input": {"sku": "BOOK-001"}})
        completed = await execute_with_lease(run.id, FakeCallerDecisionProvider())
        if completed.status != "COMPLETED":
            raise RuntimeError("Demo execution failed")
        print(RunResponse.model_validate(completed).model_dump_json(indent=2))
    finally:
        await close_db()


if __name__ == "__main__":
    asyncio.run(main())
