"""Check cancellation on a running API; persist four clearly named test tasks."""

import asyncio
from concurrent.futures import ThreadPoolExecutor
import json
import os
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from uuid import UUID, uuid4

from sqlalchemy import update

from app.db.models import Run
from app.db.session import close_db, session_scope


BASE = os.environ.get("API_BASE_URL", "http://127.0.0.1:8000").rstrip("/")


def request(method, path, payload=None):
    body = None if payload is None else json.dumps(payload).encode()
    req = Request(BASE + path, data=body, method=method, headers={"Content-Type": "application/json"})
    try:
        with urlopen(req, timeout=10) as response:
            return response.status, json.loads(response.read())
    except HTTPError as error:
        return error.code, json.loads(error.read())


async def make_terminal(fixtures):
    """Only adjust the brand-new fixture rows created by this check."""
    try:
        async with session_scope() as session:
            async with session.begin():
                for run_id, status in fixtures:
                    result = await session.execute(
                        update(Run)
                        .where(Run.id == UUID(run_id), Run.status == "PENDING")
                        .values(status=status)
                    )
                    assert result.rowcount == 1
    finally:
        await close_db()


def main():
    for _ in range(30):
        try:
            code, spec = request("GET", "/openapi.json")
            if code == 200 and "/runs/{run_id}/cancel" in spec["paths"]:
                break
        except URLError:
            pass
        time.sleep(0.5)
    else:
        raise RuntimeError("Updated API did not become ready")

    code, run = request("POST", "/runs", {"task": "cancel-check-pending", "input": {}})
    assert code == 201 and run["cancel_requested"] is False
    path = "/runs/" + run["id"]
    with ThreadPoolExecutor(max_workers=4) as pool:
        responses = list(pool.map(lambda _: request("POST", path + "/cancel"), range(4)))
    for code, response in responses:
        assert code == 200
        assert response["status"] == "PENDING" and response["cancel_requested"] is True
    code, cancelled = request("GET", path)
    assert code == 200 and cancelled["cancel_requested"] is True
    assert request("POST", path + "/cancel")[1] == cancelled
    assert request("POST", "/runs/" + str(uuid4()) + "/cancel")[0] == 404
    assert request("POST", "/runs/not-a-uuid/cancel")[0] == 422

    fixtures = []
    for status in ("COMPLETED", "FAILED", "CANCELLED"):
        code, terminal = request("POST", "/runs", {"task": "cancel-check-terminal-" + status.lower(), "input": {}})
        assert code == 201
        fixtures.append((terminal["id"], status))
    asyncio.run(make_terminal(fixtures))
    for run_id, status in fixtures:
        code, before = request("GET", "/runs/" + run_id)
        assert code == 200 and before["status"] == status
        code, after = request("POST", "/runs/" + run_id + "/cancel")
        assert code == 200 and after == before

    print("Cancellation flag persists; 4 concurrent requests remain safe")
    print("Repeated cancel is unchanged; missing ID 404; invalid ID 422")
    print("COMPLETED, FAILED and CANCELLED records stay unchanged")
    print("Pending cancellation demo:", run["id"])


if __name__ == "__main__":
    main()
