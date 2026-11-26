"""The owner's news interests, rebuilt from their verdicts every few days.

Revision ID: f1a3b5c7d9e2
Revises: e5f9a2b3c4d6
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "f1a3b5c7d9e2"  # pragma: allowlist secret
down_revision = "e5f9a2b3c4d6"  # pragma: allowlist secret
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "monitor_profiles",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("interests", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("changes", sa.Text(), nullable=False),
        sa.PrimaryKeyConstraint("id", name="pk_monitor_profiles"),
    )
    op.create_index("ix_monitor_profiles_created", "monitor_profiles", ["created_at"])


def downgrade() -> None:
    op.drop_index("ix_monitor_profiles_created", table_name="monitor_profiles")
    op.drop_table("monitor_profiles")
