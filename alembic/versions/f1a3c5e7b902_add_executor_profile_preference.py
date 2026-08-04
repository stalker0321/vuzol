"""Add exact executor profile preference.

Revision ID: f1a3c5e7b902
Revises: e8f2a1c4d903
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "f1a3c5e7b902"  # pragma: allowlist secret
down_revision: str | Sequence[str] | None = "e8f2a1c4d903"  # pragma: allowlist secret
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "project_executor_preferences",
        sa.Column("profile_id", sa.String(length=100), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("project_executor_preferences", "profile_id")
