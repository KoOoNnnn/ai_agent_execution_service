"""Task records, submission idempotency and database-backed execution ownership."""

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert

from app.db.models import Run
from app.db.session import session_scope
from app.config import get_settings


@dataclass(frozen=True)
class Claim:
    run_id: UUID
    claim_id: UUID
    worker_id: str


class LeaseLost(RuntimeError):
    """This acquisition no longer has permission to advance the task."""


class RunDeadlineExceeded(RuntimeError):
    """The original Run deadline has passed; only terminal cleanup is allowed."""


class RunCancelled(RuntimeError):
    """Cancellation prevents starting another decision attempt."""


def run_seconds_remaining(run: Run, db_now: datetime) -> float:
    seconds = (run.deadline_at - db_now).total_seconds()
    if seconds <= 0:
        raise RunDeadlineExceeded("Run deadline exceeded")
    return seconds


def new_run_limits() -> dict[str, Any]:
    settings = get_settings()
    return {
        "decision_max_attempts": settings.decision_max_attempts,
        "deadline_at": func.clock_timestamp() + timedelta(seconds=settings.run_timeout_seconds),
    }


async def try_claim_run(run_id: UUID, worker_id: str) -> Claim | None:
    """Atomically acquire an unowned/expired non-terminal run under a row lock."""
    async with session_scope() as session:
        async with session.begin():
            run = await session.scalar(select(Run).where(Run.id == run_id).with_for_update())
            if run is None:
                raise LookupError("Run not found")
            if run.status not in {"PENDING", "RUNNING"}:
                return None
            db_now = await session.scalar(select(func.clock_timestamp()))
            if run.claim_id is not None and run.lease_expires_at > db_now:
                return None
            run.claim_id = uuid4()
            run.worker_id = worker_id
            run.lease_expires_at = db_now + timedelta(seconds=get_settings().lease_ttl_seconds)
            run.status = "RUNNING"
            await session.flush()
            claim = Claim(run.id, run.claim_id, worker_id)
        return claim


async def require_claim(session: AsyncSession, claim: Claim, *, check_deadline: bool = True) -> Run:
    """Called inside a short transaction; retain the row lock through its writes."""
    run = await session.scalar(select(Run).where(Run.id == claim.run_id).with_for_update())
    db_now = await session.scalar(select(func.clock_timestamp()))
    if (
        run is None or run.status != "RUNNING"
        or run.claim_id != claim.claim_id or run.worker_id != claim.worker_id
        or run.lease_expires_at is None or run.lease_expires_at <= db_now
    ):
        raise LeaseLost("Claim is expired, replaced or terminal")
    if check_deadline:
        run_seconds_remaining(run, db_now)
    return run


async def remaining_run_seconds(claim: Claim) -> float:
    async with session_scope() as session:
        async with session.begin():
            run = await require_claim(session, claim)
            return run_seconds_remaining(run, await session.scalar(select(func.clock_timestamp())))


async def assert_claim(claim: Claim) -> Run:
    async with session_scope() as session:
        async with session.begin():
            return await require_claim(session, claim)


async def renew_lease(claim: Claim) -> bool:
    async with session_scope() as session:
        async with session.begin():
            try:
                run = await require_claim(session, claim, check_deadline=False)
            except LeaseLost:
                return False
            db_now = await session.scalar(select(func.clock_timestamp()))
            run.lease_expires_at = db_now + timedelta(seconds=get_settings().lease_ttl_seconds)
        return True


class IdempotencyConflict(Exception):
    """The same caller/key was used with a different normalized payload."""


async def create_run(input_payload: dict[str, Any]) -> Run:
    """Commit a new PENDING run and return its generated fields."""
    async with session_scope() as session:
        async with session.begin():
            run = Run(input_payload=input_payload, **new_run_limits())
            session.add(run)
            await session.flush()
            await session.refresh(run)
        return run


async def get_run(run_id: UUID) -> Run | None:
    """Look up a run by primary key; return None when it does not exist."""
    async with session_scope() as session:
        return await session.get(Run, run_id)


async def request_cancel(run_id: UUID) -> Run | None:
    """Record cancellation intent; terminal runs and repeat requests are unchanged."""
    async with session_scope() as session:
        async with session.begin():
            run = await session.scalar(
                select(Run).where(Run.id == run_id).with_for_update()
            )
            if run is None:
                return None
            if run.status in {"PENDING", "RUNNING"} and not run.cancel_requested:
                run.cancel_requested = True
                await session.flush()
                await session.refresh(run)
        return run


async def create_or_get_run(
    input_payload: dict[str, Any],
    request_key: str | None = None,
    caller_scope: str = "local",
) -> tuple[Run, bool]:
    """Return (run, created), using the database to serialize duplicate keys."""
    if request_key is None:
        async with session_scope() as session:
            async with session.begin():
                run = Run(input_payload=input_payload, caller_scope=caller_scope, **new_run_limits())
                session.add(run)
                await session.flush()
                await session.refresh(run)
            return run, True
    if not request_key.strip() or len(request_key) > 200:
        raise ValueError("Idempotency key must contain 1-200 characters")

    normalized = json.dumps(
        input_payload, sort_keys=True, separators=(",", ":"),
        ensure_ascii=False, allow_nan=False,
    )
    fingerprint = hashlib.sha256(normalized.encode("utf-8")).hexdigest()
    async with session_scope() as session:
        async with session.begin():
            statement = (
                insert(Run)
                .values(
                    input_payload=input_payload,
                    caller_scope=caller_scope,
                    request_key=request_key,
                    request_hash=fingerprint,
                    **new_run_limits(),
                )
                .on_conflict_do_nothing(constraint="uq_runs_caller_request_key")
                .returning(Run)
            )
            run = await session.scalar(statement)
            created = run is not None
            if run is None:
                run = await session.scalar(
                    select(Run).where(
                        Run.caller_scope == caller_scope,
                        Run.request_key == request_key,
                    )
                )
                if run is None:
                    raise RuntimeError("Conflicting run could not be found")
                if run.request_hash != fingerprint:
                    raise IdempotencyConflict
        return run, created
