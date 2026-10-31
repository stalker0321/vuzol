"""Private-chat inbox: raw messages, knowledge-base records, reply links.

Revision ID: c5f7a9b1e433
Revises: b4e6f8a0d322
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB, TSVECTOR

revision = "c5f7a9b1e433"
down_revision = "b4e6f8a0d322"
branch_labels = None
depends_on = None


def _now() -> sa.TextClause:
    return sa.text("now()")


def upgrade() -> None:
    op.create_table(
        "inbox_messages",
        sa.Column("id", sa.UUID(), primary_key=True),
        sa.Column("chat_id", sa.BigInteger(), nullable=False),
        sa.Column("message_id", sa.BigInteger(), nullable=False),
        sa.Column("user_id", sa.BigInteger(), nullable=False),
        sa.Column("body", sa.Text()),
        sa.Column("forward_label", sa.String(300)),
        sa.Column("forward_url", sa.String(500)),
        sa.Column("urls", JSONB(), nullable=False),
        sa.Column("attachments", JSONB(), nullable=False),
        sa.Column("media_group_id", sa.String(100)),
        sa.Column("reply_to_message_id", sa.BigInteger()),
        sa.Column("transcript", sa.Text()),
        sa.Column("batch_id", sa.UUID()),
        sa.Column("received_at", sa.DateTime(timezone=True), nullable=False, server_default=_now()),
        sa.UniqueConstraint("chat_id", "message_id", name="uq_inbox_message"),
    )
    op.create_index("ix_inbox_messages_batch_id", "inbox_messages", ["batch_id"])
    op.create_table(
        "inbox_items",
        sa.Column("id", sa.UUID(), primary_key=True),
        sa.Column("owner_user_id", sa.BigInteger(), nullable=False),
        sa.Column("chat_id", sa.BigInteger(), nullable=False),
        sa.Column("batch_id", sa.UUID()),
        sa.Column("ordinal", sa.Integer(), nullable=False),
        sa.Column("kind", sa.String(30), nullable=False),
        sa.Column("title", sa.String(300), nullable=False),
        sa.Column("summary", sa.Text(), nullable=False),
        sa.Column("tags", JSONB(), nullable=False),
        sa.Column("fields", JSONB(), nullable=False),
        sa.Column("event_start", sa.DateTime(timezone=True)),
        sa.Column("event_end", sa.DateTime(timezone=True)),
        sa.Column("evidence", sa.Text(), nullable=False),
        sa.Column("answers", JSONB(), nullable=False),
        sa.Column("removed_at", sa.DateTime(timezone=True)),
        sa.Column(
            "search",
            TSVECTOR(),
            sa.Computed(
                "to_tsvector('simple', kind || ' ' || title || ' ' || summary || ' ' "
                "|| tags::text || ' ' || fields::text)",
                persisted=True,
            ),
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=_now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=_now()),
        sa.UniqueConstraint("batch_id", "ordinal", name="uq_inbox_item_batch"),
    )
    op.create_index("ix_inbox_items_owner_kind", "inbox_items", ["owner_user_id", "kind"])
    op.create_index("ix_inbox_items_event_start", "inbox_items", ["event_start"])
    op.create_index("ix_inbox_items_search", "inbox_items", ["search"], postgresql_using="gin")
    op.create_table(
        "inbox_links",
        sa.Column("id", sa.UUID(), primary_key=True),
        sa.Column("chat_id", sa.BigInteger(), nullable=False),
        sa.Column("message_id", sa.BigInteger(), nullable=False),
        sa.Column(
            "item_id",
            sa.UUID(),
            sa.ForeignKey("inbox_items.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.UniqueConstraint("chat_id", "message_id", name="uq_inbox_link"),
    )
    op.create_index("ix_inbox_links_item_id", "inbox_links", ["item_id"])


def downgrade() -> None:
    op.drop_table("inbox_links")
    op.drop_table("inbox_items")
    op.drop_table("inbox_messages")
