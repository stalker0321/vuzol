"""Durable provider account slot admission across worker/connection crashes."""

import sqlalchemy as sa
from alembic import op

revision = "d7a4f50e9404"  # pragma: allowlist secret - migration identifier
down_revision = "c6f3e49d8303"  # pragma: allowlist secret - migration identifier
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "provider_account_leases",
        sa.Column("account_id", sa.String(100), nullable=False),
        sa.Column("slot", sa.Integer(), nullable=False),
        sa.Column("token", sa.UUID(), nullable=False),
        sa.Column("profile_id", sa.String(100), nullable=False),
        sa.Column("task_id", sa.UUID(), nullable=False),
        sa.Column("run_id", sa.UUID(), nullable=False),
        sa.Column("request_id", sa.UUID(), nullable=False),
        sa.Column("provider_attempt", sa.Integer(), nullable=False),
        sa.Column("invocation_id", sa.UUID(), nullable=False),
        sa.Column("binding_kind", sa.String(20), nullable=True),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.PrimaryKeyConstraint("account_id", "slot"),
        sa.UniqueConstraint("token"),
        sa.CheckConstraint("slot BETWEEN 0 AND 99", name="account_slot_range"),
    )


def downgrade() -> None:
    # Refuse to erase live/uncertain reservations and accidentally permit replay.
    connection = op.get_bind()
    if connection.scalar(sa.text("SELECT EXISTS (SELECT 1 FROM provider_account_leases)")):
        raise RuntimeError("cannot remove unresolved provider account tickets")
    op.drop_table("provider_account_leases")
