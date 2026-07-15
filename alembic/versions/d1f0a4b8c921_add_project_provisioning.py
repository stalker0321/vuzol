"""add project provisioning

Revision ID: d1f0a4b8c921
Revises: c85d1f4a2b90
Create Date: 2026-07-15 19:30:00
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "d1f0a4b8c921"  # pragma: allowlist secret
down_revision: str | Sequence[str] | None = "c85d1f4a2b90"  # pragma: allowlist secret
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "project_provisioning",
        sa.Column("task_id", sa.UUID(), nullable=False),
        sa.Column("requested_by_user_id", sa.BigInteger(), nullable=False),
        sa.Column("chat_id", sa.BigInteger(), nullable=False),
        sa.Column("source_thread_id", sa.BigInteger(), nullable=False),
        sa.Column("project_id", sa.String(100), nullable=False),
        sa.Column("display_name", sa.String(100), nullable=False),
        sa.Column("description", sa.Text(), nullable=False),
        sa.Column("repository_path", sa.String(500), nullable=False),
        sa.Column("topic_thread_id", sa.BigInteger()),
        sa.Column(
            "status",
            sa.Enum(
                "pending",
                "repository_created",
                "topic_creating",
                "topic_created",
                "configured",
                "completed",
                "blocked",
                "failed",
                name="project_provisioning_status",
                native_enum=False,
                create_constraint=True,
            ),
            nullable=False,
        ),
        sa.Column("configuration_revision", sa.String(64)),
        sa.Column("last_error_category", sa.String(100)),
        sa.Column("id", sa.UUID(), nullable=False),
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
        sa.ForeignKeyConstraint(["task_id"], ["tasks.id"], ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("project_id"),
        sa.UniqueConstraint("task_id"),
        sa.UniqueConstraint("topic_thread_id"),
    )


def downgrade() -> None:
    op.drop_table("project_provisioning")
