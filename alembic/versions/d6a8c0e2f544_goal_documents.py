"""Large documents attached to a goal, kept out of the lead's prompt.

Revision ID: d6a8c0e2f544
Revises: c5f7a9b1e433
"""

import sqlalchemy as sa
from alembic import op

revision = "d6a8c0e2f544"
down_revision = "c5f7a9b1e433"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "goal_documents",
        sa.Column("id", sa.UUID(), primary_key=True),
        sa.Column(
            "goal_id",
            sa.UUID(),
            sa.ForeignKey("managed_goals.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("filename", sa.String(255), nullable=False),
        sa.Column("media_type", sa.String(200), nullable=False),
        sa.Column("sha256", sa.String(64), nullable=False),
        sa.Column("body", sa.Text(), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.UniqueConstraint("goal_id", "sha256", name="uq_goal_document"),
    )
    op.create_index("ix_goal_documents_goal_id", "goal_documents", ["goal_id"])


def downgrade() -> None:
    op.drop_table("goal_documents")
