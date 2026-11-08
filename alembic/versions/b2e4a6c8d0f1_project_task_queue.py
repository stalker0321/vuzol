"""Owner tasks queued behind a project's current goal.

Revision ID: b2e4a6c8d0f1
Revises: a1d3f5b7c988
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "b2e4a6c8d0f1"  # pragma: allowlist secret - migration identifier
down_revision = "a1d3f5b7c988"  # pragma: allowlist secret - migration identifier
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "project_task_queue",
        sa.Column("id", sa.UUID(), primary_key=True),
        sa.Column("project_id", sa.String(100), nullable=False),
        sa.Column("owner_user_id", sa.BigInteger(), nullable=False),
        sa.Column("discussion_session_id", sa.UUID(), nullable=False),
        sa.Column("source_event_id", sa.UUID(), nullable=False),
        sa.Column("body", postgresql.JSONB(), nullable=False),
        sa.Column("summary", sa.String(200), nullable=False),
        sa.Column("state", sa.String(10), nullable=False),
        sa.Column("queued_by_goal_id", sa.UUID(), nullable=False),
        sa.Column("started_goal_id", sa.UUID()),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.UniqueConstraint("source_event_id", name="uq_project_task_queue_source"),
    )
    op.create_index(
        "ix_project_task_queue_waiting", "project_task_queue", ["project_id", "state", "created_at"]
    )


def downgrade() -> None:
    op.drop_table("project_task_queue")
