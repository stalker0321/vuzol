"""Add immutable project environment revisions.

Revision ID: b8d4f02a6c31
Revises: a7c3e91f4b20
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "b8d4f02a6c31"  # pragma: allowlist secret
down_revision: str | Sequence[str] | None = "a7c3e91f4b20"  # pragma: allowlist secret
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "project_environment_revisions",
        sa.Column("project_id", sa.String(100), nullable=False),
        sa.Column("revision_number", sa.Integer(), nullable=False),
        sa.Column("parent_revision_id", sa.UUID()),
        sa.Column("source", sa.String(30), nullable=False),
        sa.Column("source_plan_revision_id", sa.UUID()),
        sa.Column("contract", postgresql.JSONB(), nullable=False),
        sa.Column("content_hash", sa.String(64), nullable=False),
        sa.Column("approved_by_user_id", sa.BigInteger()),
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint("revision_number >= 1", name="project_environment_revision_positive"),
        sa.CheckConstraint(
            "content_hash ~ '^[0-9a-f]{64}$'",
            name="project_environment_content_hash_lower_hex",
        ),
        sa.CheckConstraint(
            "source IN ('detected', 'plan_approval', 'manual')",
            name="project_environment_source_known",
        ),
        sa.CheckConstraint(
            "jsonb_typeof(contract) = 'object'", name="project_environment_contract_object"
        ),
        sa.ForeignKeyConstraint(
            ["parent_revision_id"], ["project_environment_revisions.id"], ondelete="RESTRICT"
        ),
        sa.ForeignKeyConstraint(
            ["source_plan_revision_id"], ["plan_revisions.id"], ondelete="RESTRICT"
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "project_id", "revision_number", name="uq_project_environment_revision_number"
        ),
        sa.UniqueConstraint(
            "source_plan_revision_id", name="uq_project_environment_source_plan_revision"
        ),
    )
    op.create_index(
        "ix_project_environment_revisions_project_id",
        "project_environment_revisions",
        ["project_id"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_project_environment_revisions_project_id",
        table_name="project_environment_revisions",
    )
    op.drop_table("project_environment_revisions")
