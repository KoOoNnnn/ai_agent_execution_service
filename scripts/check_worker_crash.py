"""Kill/restart check. Stop the normal demo Worker before running this script."""

import asyncio
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import time
from urllib.request import Request, urlopen
from uuid import UUID, uuid4

from sqlalchemy import func, select

from app.db.execution_store import save_iteration
from app.db.models import Action, DecisionBudget, Inventory, Reservation, Step, ToolEffect
from app.db.run_store import Claim, LeaseLost, get_run, try_claim_run
from app.db.session import close_db, session_scope


ROOT = Path(__file__).resolve().parents[1]
BASE = os.environ.get("API_BASE_URL", "http://127.0.0.1:8000").rstrip("/")


async def read_state(run_id):
    return await get_run(UUID(run_id))


async def verify_stock(session, run_id, sku):
    assert (await session.get(Inventory, sku)).available == 8
    assert await session.scalar(select(func.count()).select_from(Reservation).where(Reservation.sku == sku)) == 1
    effects = list(await session.scalars(select(ToolEffect).where(ToolEffect.run_id == UUID(run_id))))
    assert len(effects) == 1
    return effects[0]


async def make_stock_fixture():
    sku = "crash-test-" + uuid4().hex
    try:
        async with session_scope() as session:
            async with session.begin():
                session.add(Inventory(sku=sku, available=10))
        return sku
    finally:
        await close_db()


async def verify_recovery(run_id, old_claim, sku=None, during_retry=False):
    try:
        async with session_scope() as session:
            count = await session.scalar(select(func.count()).select_from(Step).where(Step.run_id == UUID(run_id)))
            assert count == (0 if during_retry else 5 if sku else 3), count
            if during_retry:
                budget = await session.get(DecisionBudget, (UUID(run_id), 1))
                assert budget.attempt_count == 3
                assert await session.scalar(select(func.count()).select_from(Action).where(Action.run_id == UUID(run_id))) == 0
            if sku:
                effect = await verify_stock(session, run_id, sku)
                action = await session.get(Action, effect.operation_id)
                assert action.status == "COMPLETED" and action.iteration_index == 2
        try:
            await save_iteration(old_claim, 2, [], final_result={"stale": True})
        except LeaseLost:
            pass
        else:
            raise AssertionError("Dead acquisition wrote after recovery")
    finally:
        await close_db()


async def prepare_owner_snapshot(run_id, sku=None, during_retry=False):
    try:
        run = await read_state(run_id)
        assert run.iteration_count == (0 if during_retry else 1) and run.status == "RUNNING"
        assert await try_claim_run(run.id, "premature-takeover") is None
        if sku:
            async with session_scope() as session:
                effect = await verify_stock(session, run_id, sku)
                action = await session.get(Action, effect.operation_id)
                assert action.status == "DECIDED" and action.iteration_index == 2
        return Claim(run.id, run.claim_id, run.worker_id)
    finally:
        await close_db()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--after-reservation", action="store_true")
    mode.add_argument("--during-retry", action="store_true")
    args = parser.parse_args()
    sku = asyncio.run(make_stock_fixture()) if args.after_reservation else None
    payload = {
        "task": "reserve_inventory" if sku else "check_inventory",
        "input": {"sku": sku or "BOOK-001", "test_case": "worker-kill-recovery"},
    }
    if sku:
        payload["input"]["quantity"] = 2
    request = Request(BASE + "/runs", data=json.dumps(payload).encode(), headers={"Content-Type": "application/json"})
    with urlopen(request, timeout=10) as response:
        created = json.loads(response.read())
    assert created["dispatch_status"] == "CONFIRMED"
    run_id = created["id"]
    marker = ROOT / ".venv" / ("crash-marker-" + run_id + ".txt")
    env = os.environ.copy()
    env["LEASE_TTL_SECONDS"] = "6"
    env["HEARTBEAT_INTERVAL_SECONDS"] = "1"
    env["DECISION_RETRY_BASE_SECONDS"] = "0.01"
    env["DECISION_RETRY_JITTER_SECONDS"] = "0"
    children = []
    handles = []
    try:
        for phase in ("paused", "replacement"):
            handle = (ROOT / ".venv" / ("crash-" + phase + ".log")).open("w", encoding="utf-8")
            handles.append(handle)
            command = [sys.executable, "-m", "scripts.worker_crash_fixture", run_id, str(marker)] if phase == "paused" else [sys.executable, "-m", "app.worker", "--decision-provider", "fake-caller"]
            if sku and phase == "paused":
                command.append("after-reservation")
            elif args.during_retry and phase == "paused":
                command.append("during-retry")
            process = subprocess.Popen(command, cwd=ROOT, env=env, stdout=handle, stderr=subprocess.STDOUT, creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
            children.append(process)
            if phase == "paused":
                for _ in range(100):
                    if marker.exists():
                        break
                    if process.poll() is not None:
                        raise RuntimeError("Fixture Worker exited before the pause point")
                    time.sleep(0.1)
                else:
                    raise RuntimeError("Fixture Worker did not reach the saved-iteration pause")
                old_claim = asyncio.run(prepare_owner_snapshot(run_id, sku, args.during_retry))
                process.terminate()
                process.wait(timeout=5)
                print("Worker killed during third decision attempt" if args.during_retry else "Worker killed after committed reservation" if sku else "Worker killed after iteration 1")
                print("Active lease denied premature takeover")
            else:
                for _ in range(60):
                    with urlopen(BASE + "/runs/" + run_id, timeout=5) as response:
                        recovered = json.loads(response.read())
                    if recovered["status"] == ("FAILED" if args.during_retry else "COMPLETED"):
                        break
                    time.sleep(0.25)
                else:
                    raise RuntimeError("Replacement Worker did not recover the task")
                assert recovered["iteration_count"] == (0 if args.during_retry else 3 if sku else 2)
                if args.during_retry:
                    assert recovered["error_payload"]["code"] == "DECISION_RETRY_EXHAUSTED"
                elif sku:
                    assert recovered["result_payload"]["reserved"] is True
                    assert recovered["result_payload"]["available"] == 8
                else:
                    assert recovered["result_payload"]["source"] == "postgresql"
                asyncio.run(verify_recovery(run_id, old_claim, sku, args.during_retry))
                print("RabbitMQ redelivery + expired lease takeover + saved-observation resume: verified")
                print("Budget remains 3; no fourth call/action; task FAILED" if args.during_retry else "Exactly 3 iterations / 5 steps; stock reduced once, one reservation / effect" if sku else "Exactly 2 iterations / 3 steps")
                print("Stale writes rejected")
                print("Recovered run:", run_id)
                time.sleep(0.5)  # Allow the replacement consumer to complete its ACK.
    finally:
        for child in children:
            if child.poll() is None:
                child.terminate()
                child.wait(timeout=5)
        for handle in handles:
            handle.close()


if __name__ == "__main__":
    main()
