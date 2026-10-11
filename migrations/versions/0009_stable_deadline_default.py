"""Use a transaction-stable fallback default so schema comparisons are reliable."""

from alembic import op
import sqlalchemy as sa

revision = "0009"
down_revision = "0008"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column("runs", "deadline_at", server_default=sa.text("(now() + interval '300 seconds')"))


def downgrade() -> None:
    op.alter_column("runs", "deadline_at", server_default=sa.text("(clock_timestamp() + interval '300 seconds')"))
