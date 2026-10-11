"""Check a running API. Persists one keyed demo and two unkeyed demo runs."""

import asyncio
from concurrent.futures import ThreadPoolExecutor
import json
import os
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from uuid import uuid4

from sqlalchemy import func, select

from app.db.models import Run
from app.db.session import close_db, session_scope


BASE = os.environ.get("API_BASE_URL", "http://127.0.0.1:8000").rstrip("/")


def post(payload, key=None):
    headers = {"Content-Type": "application/json"}
    if key is not None:
        headers["Idempotency-Key"] = key
    request = Request(BASE + "/runs", data=json.dumps(payload).encode(), headers=headers)
    try:
        with urlopen(request, timeout=10) as result:
            return result.status, json.loads(result.read())
    except HTTPError as error:
        return error.code, json.loads(error.read())


async def verify_count(key):
    try:
        async with session_scope() as session:
            count = await session.scalar(
                select(func.count()).select_from(Run).where(Run.caller_scope == "local", Run.request_key == key)
            )
            assert count == 1, count
    finally:
        await close_db()


def main():
    for _ in range(30):
        try:
            with urlopen(BASE + "/openapi.json", timeout=2):
                break
        except URLError:
            time.sleep(0.5)
    else:
        raise RuntimeError("API did not become ready")

    key = "idempotency-check-" + uuid4().hex
    payload = {"task": "reserve_inventory", "input": {"sku": "IDEM-DEMO", "quantity": 2}}
    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(lambda _: post(payload, key), range(8)))
    assert sum(code == 201 for code, _ in results) == 1, results
    assert sum(code == 200 for code, _ in results) == 7, results
    assert len({body["id"] for _, body in results}) == 1

    reordered = {"input": {"quantity": 2, "sku": "IDEM-DEMO"}, "task": " reserve_inventory "}
    assert post(reordered, key)[0] == 200
    conflict = {"task": "reserve_inventory", "input": {"sku": "IDEM-DEMO", "quantity": 3}}
    assert post(conflict, key)[0] == 409
    assert post(payload, " ")[0] == 422
    first, second = post(payload), post(payload)
    assert first[0] == second[0] == 201 and first[1]["id"] != second[1]["id"]
    asyncio.run(verify_count(key))
    print("8 concurrent keyed requests -> one 201, seven 200, one database row")
    print("Reordered/normalized request 200; changed payload 409; blank key 422")
    print("Unkeyed submissions remain independent; previous demo records preserved")
    print("Demo key:", key)


if __name__ == "__main__":
    main()
