"""Add existing repository import metadata.

Revision ID: a7c3e91f4b20
Revises: d4a8f2c71b90
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "a7c3e91f4b20"  # pragma: allowlist secret
down_revision: str | Sequence[str] | None = "d4a8f2c71b90"  # pragma: allowlist secret
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("project_provisioning", sa.Column("source_repository_url", sa.String(1000)))
    op.add_column(
        "project_provisioning",
        sa.Column("default_branch", sa.String(255), nullable=False, server_default="main"),
    )


def downgrade() -> None:
    op.drop_column("project_provisioning", "default_branch")
    op.drop_column("project_provisioning", "source_repository_url")
