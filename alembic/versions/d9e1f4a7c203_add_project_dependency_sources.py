"""Add project-scoped user dependency sources.

Revision ID: d9e1f4a7c203
Revises: b8d4f02a6c31
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "d9e1f4a7c203"  # pragma: allowlist secret
down_revision: str | Sequence[str] | None = "b8d4f02a6c31"  # pragma: allowlist secret
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "project_dependency_sources",
        sa.Column("project_id", sa.String(length=100), nullable=False),
        sa.Column("ecosystem", sa.String(length=20), nullable=False),
        sa.Column("package_name", sa.String(length=214), nullable=False),
        sa.Column("source_kind", sa.String(length=20), nullable=False),
        sa.Column("source_url", sa.String(length=500), nullable=False),
        sa.Column("source_pin", sa.String(length=64), nullable=False),
        sa.Column("created_by_user_id", sa.BigInteger(), nullable=False),
        sa.Column("revoked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "ecosystem IN ('python', 'node')",
            name="project_dependency_source_ecosystem",
        ),
        sa.CheckConstraint(
            "source_kind IN ('git', 'https')",
            name="project_dependency_source_kind",
        ),
        sa.CheckConstraint(
            "(source_kind = 'git' AND source_pin ~ '^[0-9a-f]{40}$') OR "
            "(source_kind = 'https' AND source_pin ~ '^[0-9a-f]{64}$')",
            name="project_dependency_source_pin",
        ),
        sa.CheckConstraint(
            "ecosystem <> 'node' OR source_kind = 'git'",
            name="project_dependency_source_node_git_only",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "project_id",
            "ecosystem",
            "package_name",
            "source_kind",
            "source_url",
            "source_pin",
            name="uq_project_dependency_source_exact",
        ),
    )
    op.create_index(
        op.f("ix_project_dependency_sources_project_id"),
        "project_dependency_sources",
        ["project_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        op.f("ix_project_dependency_sources_project_id"),
        table_name="project_dependency_sources",
    )
    op.drop_table("project_dependency_sources")
