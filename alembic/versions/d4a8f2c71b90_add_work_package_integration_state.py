"""Add persisted work-package integration state.

Revision ID: d4a8f2c71b90
Revises: c9f4a7b21d30
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "d4a8f2c71b90"  # pragma: allowlist secret
down_revision: str | Sequence[str] | None = "c9f4a7b21d30"  # pragma: allowlist secret
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("work_packages", sa.Column("integration_branch", sa.String(255)))
    op.add_column("work_packages", sa.Column("integration_target_branch", sa.String(255)))
    op.add_column("work_packages", sa.Column("integration_base_commit", sa.String(64)))
    op.add_column("work_packages", sa.Column("integration_head_commit", sa.String(64)))
    op.add_column("work_packages", sa.Column("preview_url", sa.String(500)))
    op.create_check_constraint(
        "integration_state_complete",
        "work_packages",
        "(integration_branch IS NULL AND integration_base_commit IS NULL "
        "AND integration_head_commit IS NULL AND integration_target_branch IS NULL) OR "
        "(integration_branch IS NOT NULL AND integration_base_commit IS NOT NULL "
        "AND integration_head_commit IS NOT NULL AND integration_target_branch IS NOT NULL)",
    )


def downgrade() -> None:
    op.drop_constraint("ck_work_packages_integration_state_complete", "work_packages")
    op.drop_column("work_packages", "preview_url")
    op.drop_column("work_packages", "integration_head_commit")
    op.drop_column("work_packages", "integration_base_commit")
    op.drop_column("work_packages", "integration_target_branch")
    op.drop_column("work_packages", "integration_branch")
