"""Add execution budgets/results and completed step records."""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "0004"
down_revision = "0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("runs", sa.Column("iteration_count", sa.Integer(), server_default="0", nullable=False))
    op.add_column("runs", sa.Column("max_iterations", sa.Integer(), server_default="5", nullable=False))
    op.add_column("runs", sa.Column("result_payload", postgresql.JSONB(), nullable=True))
    op.add_column("runs", sa.Column("error_payload", postgresql.JSONB(), nullable=True))
    op.create_check_constraint("ck_runs_iteration_budget", "runs", "iteration_count >= 0 AND max_iterations >= 1 AND iteration_count <= max_iterations")
    op.create_table(
        "steps",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("run_id", sa.Uuid(), nullable=False),
        sa.Column("iteration_index", sa.Integer(), nullable=False),
        sa.Column("step_type", sa.String(16), nullable=False),
        sa.Column("status", sa.String(16), server_default="COMPLETED", nullable=False),
        sa.Column("input_payload", postgresql.JSONB(), nullable=False),
        sa.Column("output_payload", postgresql.JSONB(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(["run_id"], ["runs.id"]),
        sa.UniqueConstraint("run_id", "iteration_index", "step_type", name="uq_steps_run_iteration_type"),
        sa.CheckConstraint("iteration_index >= 1", name="ck_steps_iteration_index"),
        sa.CheckConstraint("step_type IN ('LLM_CALL', 'TOOL_CALL')", name="ck_steps_type"),
    )
    op.create_index("ix_steps_run_id", "steps", ["run_id"])


def downgrade() -> None:
    op.drop_index("ix_steps_run_id", table_name="steps")
    op.drop_table("steps")
    op.drop_constraint("ck_runs_iteration_budget", "runs", type_="check")
    for column in ("error_payload", "result_payload", "max_iterations", "iteration_count"):
        op.drop_column("runs", column)
