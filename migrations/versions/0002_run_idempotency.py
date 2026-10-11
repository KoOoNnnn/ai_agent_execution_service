"""Add caller-scoped submission idempotency.

Revision ID: 0002
Revises: 0001
"""

from alembic import op
import sqlalchemy as sa


revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("runs", sa.Column("caller_scope", sa.String(100), server_default="local", nullable=False))
    op.add_column("runs", sa.Column("request_key", sa.String(200), nullable=True))
    op.add_column("runs", sa.Column("request_hash", sa.String(64), nullable=True))
    op.create_unique_constraint("uq_runs_caller_request_key", "runs", ["caller_scope", "request_key"])


def downgrade() -> None:
    op.drop_constraint("uq_runs_caller_request_key", "runs", type_="unique")
    op.drop_column("runs", "request_hash")
    op.drop_column("runs", "request_key")
    op.drop_column("runs", "caller_scope")
