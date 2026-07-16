"""add subscription limit snapshots

Revision ID: a8b1c2d3e4f5
Revises: f6c8a1d42b77
Create Date: 2026-07-16 22:40:00
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "a8b1c2d3e4f5"  # pragma: allowlist secret
down_revision: str | Sequence[str] | None = "f6c8a1d42b77"  # pragma: allowlist secret
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "subscription_limit_snapshots",
        sa.Column("profile_id", sa.String(length=100), primary_key=True, nullable=False),
        sa.Column("company", sa.String(length=50), nullable=False),
        sa.Column("plan_label", sa.String(length=50), nullable=False),
        sa.Column("five_hour_remaining_percent", sa.Integer(), nullable=True),
        sa.Column("five_hour_reset_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("weekly_remaining_percent", sa.Integer(), nullable=True),
        sa.Column("weekly_reset_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("ok", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("detail", sa.String(length=200), nullable=True),
        sa.Column(
            "payload",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column(
            "observed_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
    )


def downgrade() -> None:
    op.drop_table("subscription_limit_snapshots")
