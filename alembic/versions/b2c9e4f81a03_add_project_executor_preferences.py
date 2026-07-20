"""add project executor preferences

Revision ID: b2c9e4f81a03
Revises: a8b1c2d3e4f5
Create Date: 2026-07-18 16:00:00
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "b2c9e4f81a03"  # pragma: allowlist secret
down_revision: str | Sequence[str] | None = "a8b1c2d3e4f5"  # pragma: allowlist secret
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "project_executor_preferences",
        sa.Column("project_id", sa.String(length=100), primary_key=True, nullable=False),
        sa.Column("mode", sa.String(length=20), nullable=False, server_default="auto"),
        sa.Column("worker_key", sa.String(length=40), nullable=True),
        sa.Column("reasoning_effort", sa.String(length=20), nullable=True),
        sa.Column("revision", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("updated_by_user_id", sa.BigInteger(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.CheckConstraint(
            "mode IN ('auto', 'pin')",
            name="project_executor_preference_mode",
        ),
        sa.CheckConstraint(
            "(mode = 'auto' AND worker_key IS NULL AND reasoning_effort IS NULL) OR "
            "(mode = 'pin' AND worker_key IS NOT NULL)",
            name="project_executor_preference_shape",
        ),
        sa.CheckConstraint(
            "worker_key IS NULL OR worker_key IN ('sol', 'terra', 'luna', 'grok')",
            name="project_executor_preference_worker",
        ),
        sa.CheckConstraint(
            "reasoning_effort IS NULL OR reasoning_effort IN "
            "('low', 'medium', 'high', 'xhigh', 'max', 'ultra')",
            name="project_executor_preference_effort",
        ),
        sa.CheckConstraint("revision >= 1", name="project_executor_preference_revision"),
    )


def downgrade() -> None:
    op.drop_table("project_executor_preferences")
