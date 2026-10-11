"""Add per-acquisition ownership and database-clock lease expiry."""

from alembic import op
import sqlalchemy as sa

revision = "0005"
down_revision = "0004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("runs", sa.Column("claim_id", sa.Uuid(), nullable=True))
    op.add_column("runs", sa.Column("worker_id", sa.String(128), nullable=True))
    op.add_column("runs", sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True))
    op.create_check_constraint(
        "ck_runs_lease_fields", "runs",
        "(claim_id IS NULL AND worker_id IS NULL AND lease_expires_at IS NULL) OR "
        "(claim_id IS NOT NULL AND worker_id IS NOT NULL AND lease_expires_at IS NOT NULL)",
    )


def downgrade() -> None:
    op.drop_constraint("ck_runs_lease_fields", "runs", type_="check")
    for column in ("lease_expires_at", "worker_id", "claim_id"):
        op.drop_column("runs", column)
