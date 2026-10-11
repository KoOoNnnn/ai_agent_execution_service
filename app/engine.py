"""Advance a leased run using caller decisions and completed-step observations."""

from copy import deepcopy
import asyncio
import logging
import random
from typing import Annotated, Any

from pydantic import Field, TypeAdapter, ValidationError

from app.db import execution_store, run_store
from app.db.models import Run
from app.db.inventory_store import ToolCancelled
from app.config import get_settings
from app.decision_provider import DecisionProvider, DecisionProviderError, TransientDecisionProviderError
from app.schemas import FinalAction, ToolAction
from app.tools import execute_tool


action_parser = TypeAdapter(Annotated[ToolAction | FinalAction, Field(discriminator="kind")])
logger = logging.getLogger(__name__)


def parse_action(response: dict[str, Any]) -> ToolAction | FinalAction:
    return action_parser.validate_python(response)


async def call_decision_with_retry(
    claim: run_store.Claim,
    provider: DecisionProvider,
    context: dict[str, Any],
) -> dict[str, Any]:
    """Reserve attempts in PostgreSQL; only transient failures receive another attempt."""
    settings = get_settings()
    while True:
        attempt, maximum = await execution_store.begin_decision_attempt(claim, context["iteration"])
        try:
            return await asyncio.wait_for(
                provider.decide(deepcopy(context)),
                timeout=settings.decision_timeout_seconds,
            )
        except (TimeoutError, ConnectionError, TransientDecisionProviderError) as error:
            if attempt == maximum:
                raise DecisionProviderError(
                    f"Decision call failed after {attempt} attempts; last error: {type(error).__name__}",
                    code="DECISION_RETRY_EXHAUSTED",
                ) from error
            delay = settings.decision_retry_base_seconds * (2 ** (attempt - 1))
            delay += random.uniform(0, settings.decision_retry_jitter_seconds)
            logger.warning(
                "Decision retry: run=%s iteration=%s attempt=%s/%s error=%s delay=%.2fs",
                claim.run_id, context["iteration"], attempt, maximum,
                type(error).__name__, delay,
            )
            await asyncio.sleep(delay)  # Independent heartbeat continues during backoff.


async def execute_run(claim: run_store.Claim, decision_provider: DecisionProvider) -> Run:
    """Bound execution by the original DB deadline; terminal cleanup still checks ownership."""
    timer = None
    try:
        remaining = await run_store.remaining_run_seconds(claim)
        timer = asyncio.timeout(remaining)
        async with timer:
            return await advance_run(claim, decision_provider)
    except run_store.RunDeadlineExceeded:
        return await execution_store.finish_failure(claim, "RUN_DEADLINE_EXCEEDED", "Run deadline exceeded")
    except TimeoutError:
        if timer is None or not timer.expired():
            raise
        return await execution_store.finish_failure(claim, "RUN_DEADLINE_EXCEEDED", "Run deadline exceeded")


async def advance_run(claim: run_store.Claim, decision_provider: DecisionProvider) -> Run:
    """Orchestrate execution while the injected caller adapter supplies decisions."""
    run_id = claim.run_id
    run = await run_store.assert_claim(claim)
    observations = await execution_store.load_observations(claim)
    context = {**run.input_payload, "run_id": str(run_id), "observations": observations}
    try:
        while True:
            run = await run_store.assert_claim(claim)
            if run.cancel_requested:
                return await execution_store.finish_failure(claim, "CANCEL_REQUESTED", "Cancellation requested")
            if run.iteration_count >= run.max_iterations:
                return await execution_store.finish_failure(claim, "MAX_ITERATIONS", "Decision budget exceeded")

            iteration = run.iteration_count + 1
            context["iteration"] = iteration
            saved_action = await execution_store.load_action(claim, iteration)
            if saved_action is None:
                model_input = deepcopy(context)
                action = parse_action(await call_decision_with_retry(claim, decision_provider, context))
                saved_action = await execution_store.save_action(
                    claim, iteration, model_input, action.model_dump(mode="json"),
                )
            else:
                # A new owner reuses the committed decision, not a fresh caller response.
                action = parse_action(saved_action.decision_payload)
            steps = [{"step_type": "LLM_CALL", "input_payload": saved_action.input_payload,
                      "output_payload": saved_action.decision_payload}]
            if isinstance(action, FinalAction):
                return await execution_store.save_iteration(claim, iteration, steps, final_result=action.result)

            current = await run_store.assert_claim(claim)
            if current.cancel_requested:
                return await execution_store.finish_failure(claim, "CANCEL_REQUESTED", "Cancellation requested")
            output = await execute_tool(claim, action, saved_action.id)
            steps.append({"step_type": "TOOL_CALL", "input_payload": action.arguments.model_dump(), "output_payload": output})
            context["observations"].append(output)
            run = await execution_store.save_iteration(claim, iteration, steps)
            if run.status != "RUNNING":
                return run
    except (ToolCancelled, run_store.RunCancelled):
        return await execution_store.finish_failure(claim, "CANCEL_REQUESTED", "Cancellation requested")
    except execution_store.DecisionBudgetExhausted:
        return await execution_store.finish_failure(claim, "DECISION_RETRY_EXHAUSTED", "Persisted decision attempts exhausted")
    except DecisionProviderError as error:
        return await execution_store.finish_failure(claim, error.code, str(error))
    except ValidationError:
        return await execution_store.finish_failure(claim, "INVALID_ACTION", "Model action failed schema validation")
    except ValueError as error:
        return await execution_store.finish_failure(claim, "TOOL_ERROR", str(error))
