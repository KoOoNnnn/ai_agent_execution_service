"""Persist per-round decision attempts and each Run's original deadline."""

from alembic import op
import sqlalchemy as sa

revision = "0008"
down_revision = "0007"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("runs", sa.Column("decision_max_attempts", sa.Integer(), nullable=False, server_default="3"))
    op.create_check_constraint("ck_runs_decision_max_attempts", "runs", "decision_max_attempts BETWEEN 1 AND 10")
    op.add_column("runs", sa.Column("deadline_at", sa.DateTime(timezone=True), nullable=True))
    op.execute("UPDATE runs SET deadline_at = created_at + interval '300 seconds'")
    op.alter_column("runs", "deadline_at", nullable=False,
                    server_default=sa.text("(clock_timestamp() + interval '300 seconds')"))
    op.create_table(
        "decision_budgets",
        sa.Column("run_id", sa.Uuid(), sa.ForeignKey("runs.id"), primary_key=True),
        sa.Column("iteration_index", sa.Integer(), primary_key=True),
        sa.Column("attempt_count", sa.Integer(), nullable=False),
        sa.CheckConstraint("iteration_index >= 1", name="ck_decision_budgets_iteration"),
        sa.CheckConstraint("attempt_count BETWEEN 1 AND 10", name="ck_decision_budgets_attempts"),
    )


def downgrade() -> None:
    op.drop_table("decision_budgets")
    op.drop_column("runs", "deadline_at")
    op.drop_constraint("ck_runs_decision_max_attempts", "runs", type_="check")
    op.drop_column("runs", "decision_max_attempts")
