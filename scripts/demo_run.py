"""Create one persistent demo task each time: python -m scripts.demo_run"""

import asyncio
import json
from uuid import uuid4

from app.db.run_store import create_run, get_run
from app.db.session import close_db, init_db


async def main() -> None:
    try:
        await init_db()
        payload = {
            "task": "reserve_inventory",
            "input": {"sku": "BOOK-001", "quantity": 2},
        }
        created = await create_run(payload)
        loaded = await get_run(created.id)
        if loaded is None or loaded.status != "PENDING" or loaded.input_payload != payload:
            raise RuntimeError("Created run could not be read back correctly")
        if await get_run(uuid4()) is not None:
            raise RuntimeError("Missing run lookup did not return None")

        print(json.dumps({
            "run_id": str(loaded.id),
            "status": loaded.status,
            "input_payload": loaded.input_payload,
            "created_at": loaded.created_at.isoformat(),
            "updated_at": loaded.updated_at.isoformat(),
        }, indent=2))
        print("Run committed and read back; missing ID returns None.")
    finally:
        await close_db()


if __name__ == "__main__":
    asyncio.run(main())
