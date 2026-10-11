"""API data contracts; these models do not create database tables."""

from datetime import datetime
from typing import Annotated, Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, StringConstraints


class CreateRunRequest(BaseModel):
    """Required task name and input; clients cannot set status or IDs."""

    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={
            "examples": [
                {
                    "task": "check_inventory",
                    "input": {"sku": "BOOK-001"},
                },
                {
                    "task": "reserve_inventory",
                    "input": {"sku": "BOOK-003", "quantity": 2},
                },
            ]
        },
    )

    task: Annotated[
        str,
        StringConstraints(strip_whitespace=True, min_length=1, max_length=100),
    ]
    input: dict[str, Any]


class RunResponse(BaseModel):
    """Public task fields, readable directly from a Run ORM object."""

    model_config = ConfigDict(from_attributes=True)

    id: UUID
    status: Literal["PENDING", "RUNNING", "COMPLETED", "FAILED", "CANCELLED"]
    cancel_requested: bool
    iteration_count: int
    max_iterations: int
    decision_max_attempts: int
    deadline_at: datetime
    result_payload: dict[str, Any] | None
    error_payload: dict[str, Any] | None
    input_payload: dict[str, Any]
    created_at: datetime
    updated_at: datetime


class InventoryArguments(BaseModel):
    model_config = ConfigDict(extra="forbid")
    sku: Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=100)]


class ReservationArguments(InventoryArguments):
    quantity: Annotated[int, Field(strict=True, ge=1, le=1_000_000)]


class RunCreatedResponse(RunResponse):
    dispatch_status: Literal["CONFIRMED", "UNCONFIRMED", "SKIPPED"]


class DispatchResponse(BaseModel):
    run_id: UUID
    dispatch_status: Literal["CONFIRMED", "UNCONFIRMED"]


class StepResponse(BaseModel):
    """A completed operation; failed/in-flight calls are not Step rows yet."""

    model_config = ConfigDict(from_attributes=True)

    id: UUID
    iteration_index: int
    step_type: Literal["LLM_CALL", "TOOL_CALL"]
    status: Literal["COMPLETED"]
    input_payload: dict[str, Any]
    output_payload: dict[str, Any]
    created_at: datetime


class DecisionAttemptsResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    iteration_index: int
    attempt_count: int


class StepPage(BaseModel):
    run_id: UUID
    status: Literal["PENDING", "RUNNING", "COMPLETED", "FAILED", "CANCELLED"]
    error_payload: dict[str, Any] | None
    decision_attempts: list[DecisionAttemptsResponse]
    items: list[StepResponse]
    next_cursor: str | None


class GetInventoryAction(BaseModel):
    model_config = ConfigDict(extra="forbid")
    kind: Literal["TOOL_CALL"]
    tool_name: Literal["get_inventory"]
    arguments: InventoryArguments


class ReserveInventoryAction(BaseModel):
    model_config = ConfigDict(extra="forbid")
    kind: Literal["TOOL_CALL"]
    tool_name: Literal["reserve_inventory"]
    arguments: ReservationArguments


ToolAction = Annotated[GetInventoryAction | ReserveInventoryAction, Field(discriminator="tool_name")]


class FinalAction(BaseModel):
    model_config = ConfigDict(extra="forbid")
    kind: Literal["FINAL"]
    result: dict[str, Any]
