"""Persist the topic's conversational addressee, independently of execution."""

import sqlalchemy as sa
from alembic import op

revision = "e8b5a61fa505"
down_revision = "d7a4f50e9404"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "topic_mappings",
        sa.Column("conversation_mode", sa.String(16), nullable=False, server_default="think"),
    )
    op.add_column("topic_mappings", sa.Column("conversation_mode_message_id", sa.BigInteger()))


def downgrade() -> None:
    op.drop_column("topic_mappings", "conversation_mode_message_id")
    op.drop_column("topic_mappings", "conversation_mode")
