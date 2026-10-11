"""Guard progress writes by claim; recover observations from completed steps."""

import base64
from dataclasses import dataclass
import json
from typing import Any
from uuid import UUID

from sqlalchemy import and_, case, func, or_, select, text

from app.db.models import Action, DecisionBudget, Run, Step
from app.db.session import session_scope
from app.db.run_store import Claim, RunCancelled, require_claim


class RunAlreadyRunning(RuntimeError):
    pass


class DecisionBudgetExhausted(RuntimeError):
    pass


class InvalidStepCursor(ValueError):
    pass


@dataclass
class StepPageData:
    run: Run
    steps: list[Step]
    decision_budgets: list[DecisionBudget]
    next_cursor: str | None


def encode_step_cursor(run_id: UUID, step: Step) -> str:
    payload = json.dumps([1, str(run_id), step.iteration_index, step.step_type]).encode()
    return base64.urlsafe_b64encode(payload).decode().rstrip("=")


def decode_step_cursor(run_id: UUID, cursor: str) -> tuple[int, int]:
    try:
        if not 1 <= len(cursor) <= 512:
            raise ValueError
        raw = base64.b64decode(cursor + "=" * (-len(cursor) % 4), altchars=b"-_", validate=True)
        values = json.loads(raw)
        if not isinstance(values, list) or len(values) != 4:
            raise ValueError
        version, cursor_run_id, iteration, step_type = values
        if (
            type(version) is not int or version != 1 or not isinstance(cursor_run_id, str)
            or UUID(cursor_run_id) != run_id or type(iteration) is not int
            or not 1 <= iteration <= 2_147_483_647 or step_type not in ("LLM_CALL", "TOOL_CALL")
        ):
            raise ValueError
        return iteration, 0 if step_type == "LLM_CALL" else 1
    except (ValueError, TypeError, UnicodeError) as error:
        raise InvalidStepCursor("Invalid cursor for this run") from error


async def list_steps(run_id: UUID, limit: int = 50, cursor: str | None = None) -> StepPageData | None:
    """Read a consistent snapshot; keyset pagination follows immutable round/type order."""
    if not 1 <= limit <= 100:
        raise ValueError("Step page limit must be between 1 and 100")
    position = decode_step_cursor(run_id, cursor) if cursor is not None else None
    step_order = case((Step.step_type == "LLM_CALL", 0), else_=1)
    async with session_scope() as session:
        async with session.begin():
            # Run status, attempts and completed steps must describe the same DB snapshot.
            await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"))
            run = await session.get(Run, run_id)
            if run is None:
                return None
            query = select(Step).where(Step.run_id == run_id)
            if position is not None:
                iteration, order = position
                query = query.where(or_(
                    Step.iteration_index > iteration,
                    and_(Step.iteration_index == iteration, step_order > order),
                ))
            rows = list(await session.scalars(query.order_by(Step.iteration_index, step_order).limit(limit + 1)))
            steps = rows[:limit]
            next_cursor = encode_step_cursor(run_id, steps[-1]) if len(rows) > limit else None
            budgets = list(await session.scalars(select(DecisionBudget).where(
                DecisionBudget.run_id == run_id,
            ).order_by(DecisionBudget.iteration_index)))
        return StepPageData(run, steps, budgets, next_cursor)


async def begin_decision_attempt(claim: Claim, iteration: int) -> tuple[int, int]:
    """Reserve an attempt durably before I/O; a crash never refunds it."""
    async with session_scope() as session:
        async with session.begin():
            run = await require_claim(session, claim)
            if run.cancel_requested:
                raise RunCancelled("Cancellation requested")
            require_next_iteration(run, iteration)
            if await session.scalar(select(Action.id).where(
                Action.run_id == claim.run_id, Action.iteration_index == iteration,
            )) is not None:
                raise RuntimeError("A persisted action must be reused without another decision call")
            budget = await session.get(DecisionBudget, (claim.run_id, iteration))
            used = budget.attempt_count if budget is not None else 0
            if used >= run.decision_max_attempts:
                raise DecisionBudgetExhausted("Persisted decision attempts exhausted")
            if budget is None:
                budget = DecisionBudget(run_id=claim.run_id, iteration_index=iteration, attempt_count=1)
                session.add(budget)
            else:
                budget.attempt_count += 1
            await session.flush()
            attempt, maximum = budget.attempt_count, run.decision_max_attempts
        return attempt, maximum


async def load_observations(claim: Claim) -> list[dict[str, Any]]:
    async with session_scope() as session:
        async with session.begin():
            run = await require_claim(session, claim)
            outputs = await session.scalars(
                select(Step.output_payload).where(
                    Step.run_id == claim.run_id,
                    Step.step_type == "TOOL_CALL",
                    Step.status == "COMPLETED",
                    Step.iteration_index <= run.iteration_count,
                ).order_by(Step.iteration_index)
            )
            return list(outputs)


def clear_ownership(run: Run) -> None:
    run.claim_id = run.worker_id = run.lease_expires_at = None


def require_next_iteration(run: Run, iteration: int) -> None:
    if iteration != run.iteration_count + 1 or iteration > run.max_iterations:
        raise RuntimeError("Invalid iteration progression")


async def load_action(claim: Claim, iteration: int) -> Action | None:
    """Read the next decision under the current lease, including on recovery."""
    async with session_scope() as session:
        async with session.begin():
            run = await require_claim(session, claim)
            require_next_iteration(run, iteration)
            return await session.scalar(select(Action).where(
                Action.run_id == claim.run_id, Action.iteration_index == iteration,
            ))


async def save_action(
    claim: Claim, iteration: int, input_payload: dict[str, Any], decision_payload: dict[str, Any],
) -> Action:
    """Commit an immutable decision before any tool I/O; same content is idempotent."""
    async with session_scope() as session:
        async with session.begin():
            run = await require_claim(session, claim)
            require_next_iteration(run, iteration)
            action = await session.scalar(select(Action).where(
                Action.run_id == claim.run_id, Action.iteration_index == iteration,
            ))
            if action is None:
                action = Action(run_id=claim.run_id, iteration_index=iteration,
                                input_payload=input_payload, decision_payload=decision_payload)
                session.add(action)
                await session.flush()
                await session.refresh(action)
            elif action.input_payload != input_payload or action.decision_payload != decision_payload:
                raise RuntimeError("Persisted decision cannot be replaced")
        return action


async def save_iteration(
    claim: Claim,
    iteration: int,
    steps: list[dict[str, Any]],
    final_result: dict[str, Any] | None = None,
) -> Run:
    async with session_scope() as session:
        async with session.begin():
            run = await require_claim(session, claim)
            if run.cancel_requested:
                run.status = "CANCELLED"
            else:
                require_next_iteration(run, iteration)
                action = await session.scalar(select(Action).where(
                    Action.run_id == claim.run_id, Action.iteration_index == iteration,
                ))
                if action is None or action.status != "DECIDED":
                    raise RuntimeError("A persisted decision is required before completing an iteration")
                if not steps or steps[0] != {
                    "step_type": "LLM_CALL", "input_payload": action.input_payload,
                    "output_payload": action.decision_payload,
                }:
                    raise RuntimeError("Completed decision must match the persisted action")
                for step in steps:
                    session.add(Step(run_id=claim.run_id, iteration_index=iteration, **step))
                action.status = "COMPLETED"
                run.iteration_count = iteration
                if final_result is not None:
                    run.status = "COMPLETED"
                    run.result_payload = final_result
            if run.status != "RUNNING":
                clear_ownership(run)
            await session.flush()
            await session.refresh(run)
        return run


async def finish_failure(claim: Claim, code: str, message: str) -> Run:
    async with session_scope() as session:
        async with session.begin():
            run = await require_claim(session, claim, check_deadline=False)
            if run.cancel_requested:
                run.status = "CANCELLED"
            else:
                db_now = await session.scalar(select(func.clock_timestamp()))
                if run.deadline_at <= db_now:
                    code, message = "RUN_DEADLINE_EXCEEDED", "Run deadline exceeded"
                run.status = "FAILED"
                run.error_payload = {"code": code, "message": message}
            clear_ownership(run)
            await session.flush()
            await session.refresh(run)
        return run
