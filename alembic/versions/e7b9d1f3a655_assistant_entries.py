"""The assistant topic: owner turns, replies and dispatch plans.

Revision ID: e7b9d1f3a655
Revises: d6a8c0e2f544
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "e7b9d1f3a655"
down_revision = "d6a8c0e2f544"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "assistant_entries",
        sa.Column("id", sa.UUID(), primary_key=True),
        sa.Column("chat_id", sa.BigInteger(), nullable=False),
        sa.Column("message_thread_id", sa.BigInteger(), nullable=False),
        sa.Column("role", sa.String(10), nullable=False),
        sa.Column("body", sa.Text(), nullable=False),
        sa.Column("telegram_message_id", sa.BigInteger()),
        sa.Column("reply_to_message_id", sa.BigInteger()),
        sa.Column(
            "dispatches",
            postgresql.JSONB(),
            nullable=False,
            server_default=sa.text("'[]'::jsonb"),
        ),
        sa.Column("plan_status", sa.String(12)),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.UniqueConstraint("chat_id", "telegram_message_id", name="uq_assistant_entry_message"),
    )
    op.create_index(
        "ix_assistant_entries_topic",
        "assistant_entries",
        ["chat_id", "message_thread_id", "created_at"],
    )


def downgrade() -> None:
    op.drop_table("assistant_entries")
