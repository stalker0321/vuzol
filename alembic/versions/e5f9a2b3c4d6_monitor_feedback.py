"""Each news item explains itself and takes the owner's verdict.

Revision ID: e5f9a2b3c4d6
Revises: d4e8f1a2b3c5
"""

import sqlalchemy as sa
from alembic import op

revision = "e5f9a2b3c4d6"  # pragma: allowlist secret
down_revision = "d4e8f1a2b3c5"  # pragma: allowlist secret
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("monitor_events", sa.Column("details", sa.Text(), nullable=True))
    op.add_column("monitor_events", sa.Column("feedback", sa.String(length=8), nullable=True))
    op.add_column(
        "monitor_events",
        sa.Column("saved", sa.Boolean(), nullable=False, server_default=sa.text("false")),
    )
    op.add_column("monitor_events", sa.Column("message_id", sa.BigInteger(), nullable=True))


def downgrade() -> None:
    for column in ("message_id", "saved", "feedback", "details"):
        op.drop_column("monitor_events", column)
