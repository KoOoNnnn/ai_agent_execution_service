"""Add stock, durable tool results and reservations in the same database."""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

revision = "0007"
down_revision = "0006"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "inventory",
        sa.Column("sku", sa.String(100), primary_key=True),
        sa.Column("available", sa.Integer(), nullable=False),
        sa.CheckConstraint("available >= 0", name="ck_inventory_available"),
    )
    op.create_table(
        "tool_effects",
        sa.Column("operation_id", sa.Uuid(), sa.ForeignKey("actions.id"), primary_key=True),
        sa.Column("run_id", sa.Uuid(), sa.ForeignKey("runs.id"), nullable=False),
        sa.Column("tool_name", sa.String(100), nullable=False),
        sa.Column("arguments_payload", JSONB(), nullable=False),
        sa.Column("result_payload", JSONB(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    )
    op.create_index("ix_tool_effects_run_id", "tool_effects", ["run_id"])
    op.create_table(
        "reservations",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("operation_id", sa.Uuid(), sa.ForeignKey("tool_effects.operation_id"), nullable=False, unique=True),
        sa.Column("sku", sa.String(100), sa.ForeignKey("inventory.sku"), nullable=False),
        sa.Column("quantity", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.CheckConstraint("quantity >= 1", name="ck_reservations_quantity"),
    )
    op.create_index("ix_reservations_sku", "reservations", ["sku"])


def downgrade() -> None:
    op.drop_index("ix_reservations_sku", table_name="reservations")
    op.drop_table("reservations")
    op.drop_index("ix_tool_effects_run_id", table_name="tool_effects")
    op.drop_table("tool_effects")
    op.drop_table("inventory")
