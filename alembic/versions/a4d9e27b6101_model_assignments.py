"""Add project role assignments to the revision-fenced preference row.

Revision ID: a4d9e27b6101
Revises: f3a8c2d14e70
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "a4d9e27b6101"  # pragma: allowlist secret
down_revision: str | Sequence[str] | None = "f3a8c2d14e70"  # pragma: allowlist secret
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "project_executor_preferences",
        sa.Column(
            "model_assignments",
            postgresql.JSONB(),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
    )


def downgrade() -> None:
    op.drop_column("project_executor_preferences", "model_assignments")
