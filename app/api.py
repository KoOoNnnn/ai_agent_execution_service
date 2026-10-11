"""HTTP routes delegate persistence to run_store."""

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Header, HTTPException, Query, Response, status

from app.db import execution_store, run_store
from app.broker import broker
from app.schemas import CreateRunRequest, DecisionAttemptsResponse, DispatchResponse, RunCreatedResponse, RunResponse, StepPage, StepResponse


router = APIRouter(prefix="/runs", tags=["runs"])


@router.post("", response_model=RunCreatedResponse, status_code=status.HTTP_201_CREATED)
async def create_run_endpoint(
    request: CreateRunRequest,
    response: Response,
    idempotency_key: Annotated[
        str | None,
        Header(alias="Idempotency-Key", min_length=1, max_length=200, pattern=r"\S"),
    ] = None,
) -> RunCreatedResponse:
    """Create a task (201), reuse the same keyed request (200), or reject conflict (409)."""
    try:
        run, created = await run_store.create_or_get_run(
            request.model_dump(mode="json"),
            request_key=idempotency_key,
            caller_scope="local",  # Development scope; authentication is not implemented yet.
        )
    except run_store.IdempotencyConflict:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Idempotency-Key already used with a different request",
        ) from None
    response.status_code = status.HTTP_201_CREATED if created else status.HTTP_200_OK
    dispatch_status = "SKIPPED"
    if run.status in {"PENDING", "RUNNING"}:
        dispatch_status = await broker.publish_run(run.id)
    return RunCreatedResponse(
        **RunResponse.model_validate(run).model_dump(), dispatch_status=dispatch_status,
    )


@router.get("/{run_id}", response_model=RunResponse)
async def get_run_endpoint(run_id: UUID) -> RunResponse:
    """Return a task or HTTP 404 when its ID does not exist."""
    run = await run_store.get_run(run_id)
    if run is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Run not found")
    return RunResponse.model_validate(run)


@router.post("/{run_id}/cancel", response_model=RunResponse)
async def cancel_run_endpoint(run_id: UUID) -> RunResponse:
    """Request cooperative cancellation; a Worker must process the intent later."""
    run = await run_store.request_cancel(run_id)
    if run is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Run not found")
    return RunResponse.model_validate(run)


@router.get("/{run_id}/steps", response_model=StepPage)
async def list_steps_endpoint(
    run_id: UUID,
    limit: Annotated[int, Query(ge=1, le=100)] = 50,
    cursor: Annotated[str | None, Query(min_length=1, max_length=512)] = None,
) -> StepPage:
    """Read completed operations in round order, with Run errors and reserved decision attempts."""
    try:
        page = await execution_store.list_steps(run_id, limit, cursor)
    except execution_store.InvalidStepCursor:
        raise HTTPException(status_code=422, detail="Invalid cursor for this run") from None
    if page is None:
        raise HTTPException(status_code=404, detail="Run not found")
    return StepPage(
        run_id=page.run.id, status=page.run.status, error_payload=page.run.error_payload,
        decision_attempts=[DecisionAttemptsResponse.model_validate(budget) for budget in page.decision_budgets],
        items=[StepResponse.model_validate(step) for step in page.steps], next_cursor=page.next_cursor,
    )


@router.post("/{run_id}/dispatch", response_model=DispatchResponse, status_code=status.HTTP_202_ACCEPTED)
async def dispatch_run_endpoint(run_id: UUID) -> DispatchResponse:
    """Republish a non-terminal run without resetting its progress."""
    run = await run_store.get_run(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Run not found")
    if run.status not in {"PENDING", "RUNNING"}:
        raise HTTPException(status_code=409, detail="Terminal run cannot be dispatched")
    dispatch_status = await broker.publish_run(run.id)
    if dispatch_status == "UNCONFIRMED":
        raise HTTPException(status_code=503, detail={"run_id": str(run.id), "dispatch_status": dispatch_status})
    return DispatchResponse(run_id=run.id, dispatch_status=dispatch_status)
