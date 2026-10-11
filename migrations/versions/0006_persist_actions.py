"""Persist validated decisions before execution."""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

revision = "0006"
down_revision = "0005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "actions",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("run_id", sa.Uuid(), nullable=False),
        sa.Column("iteration_index", sa.Integer(), nullable=False),
        sa.Column("input_payload", JSONB(), nullable=False),
        sa.Column("decision_payload", JSONB(), nullable=False),
        sa.Column("status", sa.String(16), nullable=False, server_default="DECIDED"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(["run_id"], ["runs.id"]),
        sa.UniqueConstraint("run_id", "iteration_index", name="uq_actions_run_iteration"),
        sa.CheckConstraint("iteration_index >= 1", name="ck_actions_iteration_index"),
        sa.CheckConstraint("status IN ('DECIDED', 'COMPLETED')", name="ck_actions_status"),
    )
    op.create_index("ix_actions_run_id", "actions", ["run_id"])


def downgrade() -> None:
    op.drop_index("ix_actions_run_id", table_name="actions")
    op.drop_table("actions")
