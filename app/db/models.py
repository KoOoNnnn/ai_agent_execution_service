"""ORM table definitions. Migrations create the actual database tables."""

from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import Boolean, CheckConstraint, DateTime, ForeignKey, Integer, String, UniqueConstraint, false, func, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """Shared table metadata for all models and Alembic migrations."""


class Run(Base):
    """The initial task record; reliability fields will be added later."""

    __tablename__ = "runs"
    __table_args__ = (
        UniqueConstraint("caller_scope", "request_key", name="uq_runs_caller_request_key"),
        CheckConstraint(
            "(claim_id IS NULL AND worker_id IS NULL AND lease_expires_at IS NULL) OR "
            "(claim_id IS NOT NULL AND worker_id IS NOT NULL AND lease_expires_at IS NOT NULL)",
            name="ck_runs_lease_fields",
        ),
        CheckConstraint("iteration_count >= 0 AND max_iterations >= 1 AND iteration_count <= max_iterations", name="ck_runs_iteration_budget"),
        CheckConstraint("decision_max_attempts BETWEEN 1 AND 10", name="ck_runs_decision_max_attempts"),
        CheckConstraint(
            "status IN ('PENDING', 'RUNNING', 'COMPLETED', 'FAILED', 'CANCELLED')",
            name="ck_runs_status",
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    caller_scope: Mapped[str] = mapped_column(
        String(100), nullable=False, default="local", server_default="local",
    )
    request_key: Mapped[str | None] = mapped_column(String(200), nullable=True)
    request_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)
    input_payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    status: Mapped[str] = mapped_column(
        String(16),
        nullable=False,
        default="PENDING",
        server_default="PENDING",
    )
    cancel_requested: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default=false(),
    )
    iteration_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    max_iterations: Mapped[int] = mapped_column(Integer, nullable=False, default=5, server_default="5")
    decision_max_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=3, server_default="3")
    deadline_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False,
        server_default=text("(now() + interval '300 seconds')"),
    )
    result_payload: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    error_payload: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    claim_id: Mapped[UUID | None] = mapped_column(nullable=True)
    worker_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )


class DecisionBudget(Base):
    """Attempts reserved before caller I/O, independent of action persistence."""

    __tablename__ = "decision_budgets"
    __table_args__ = (
        CheckConstraint("iteration_index >= 1", name="ck_decision_budgets_iteration"),
        CheckConstraint("attempt_count BETWEEN 1 AND 10", name="ck_decision_budgets_attempts"),
    )

    run_id: Mapped[UUID] = mapped_column(ForeignKey("runs.id"), primary_key=True)
    iteration_index: Mapped[int] = mapped_column(Integer, primary_key=True)
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False)


class Action(Base):
    """Validated caller decision, committed before tool execution or finalization."""

    __tablename__ = "actions"
    __table_args__ = (
        UniqueConstraint("run_id", "iteration_index", name="uq_actions_run_iteration"),
        CheckConstraint("iteration_index >= 1", name="ck_actions_iteration_index"),
        CheckConstraint("status IN ('DECIDED', 'COMPLETED')", name="ck_actions_status"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    run_id: Mapped[UUID] = mapped_column(ForeignKey("runs.id"), nullable=False, index=True)
    iteration_index: Mapped[int] = mapped_column(Integer, nullable=False)
    input_payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    decision_payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="DECIDED", server_default="DECIDED")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())


class Step(Base):
    """Completed model/tool operations, grouped by decision iteration."""

    __tablename__ = "steps"
    __table_args__ = (
        UniqueConstraint("run_id", "iteration_index", "step_type", name="uq_steps_run_iteration_type"),
        CheckConstraint("iteration_index >= 1", name="ck_steps_iteration_index"),
        CheckConstraint("step_type IN ('LLM_CALL', 'TOOL_CALL')", name="ck_steps_type"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    run_id: Mapped[UUID] = mapped_column(ForeignKey("runs.id"), nullable=False, index=True)
    iteration_index: Mapped[int] = mapped_column(Integer, nullable=False)
    step_type: Mapped[str] = mapped_column(String(16), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="COMPLETED", server_default="COMPLETED")
    input_payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    output_payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())


class Inventory(Base):
    """Example business stock, independent of execution progress."""

    __tablename__ = "inventory"
    __table_args__ = (CheckConstraint("available >= 0", name="ck_inventory_available"),)

    sku: Mapped[str] = mapped_column(String(100), primary_key=True)
    available: Mapped[int] = mapped_column(Integer, nullable=False)


class ToolEffect(Base):
    """Committed business outcome; the saved action UUID is the operation ID."""

    __tablename__ = "tool_effects"

    operation_id: Mapped[UUID] = mapped_column(ForeignKey("actions.id"), primary_key=True)
    run_id: Mapped[UUID] = mapped_column(ForeignKey("runs.id"), nullable=False, index=True)
    tool_name: Mapped[str] = mapped_column(String(100), nullable=False)
    arguments_payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    result_payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())


class Reservation(Base):
    """One successful stock reservation per operation."""

    __tablename__ = "reservations"
    __table_args__ = (CheckConstraint("quantity >= 1", name="ck_reservations_quantity"),)

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    operation_id: Mapped[UUID] = mapped_column(ForeignKey("tool_effects.operation_id"), nullable=False, unique=True)
    sku: Mapped[str] = mapped_column(ForeignKey("inventory.sku"), nullable=False, index=True)
    quantity: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
