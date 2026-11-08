"""The assistant's plan may create a project.

Revision ID: a1d3f5b7c988
Revises: f8c0e2a4b766
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "a1d3f5b7c988"  # pragma: allowlist secret - migration identifier
down_revision = "f8c0e2a4b766"  # pragma: allowlist secret - migration identifier
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("assistant_entries", sa.Column("new_project", postgresql.JSONB()))


def downgrade() -> None:
    op.drop_column("assistant_entries", "new_project")
