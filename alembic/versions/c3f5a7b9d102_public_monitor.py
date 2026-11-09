"""Persist public monitor evidence, source intervals and digest reservations.

Revision ID: c3f5a7b9d102
Revises: b2e4a6c8d0f1
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "c3f5a7b9d102"  # pragma: allowlist secret - migration identifier
down_revision = "b2e4a6c8d0f1"  # pragma: allowlist secret - migration identifier
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "monitor_events",
        sa.Column("id", sa.UUID(), primary_key=True),
        sa.Column("source_id", sa.String(100), nullable=False),
        sa.Column("external_id", sa.Text(), nullable=False),
        sa.Column("url", sa.Text(), nullable=False),
        sa.Column("title", sa.Text(), nullable=False),
        sa.Column("excerpt", sa.Text(), nullable=False),
        sa.Column("published_at", sa.DateTime(timezone=True)),
        sa.Column("fetched_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("state", sa.String(12), nullable=False),
        sa.Column("relevance", sa.Float()),
        sa.Column("summary", sa.Text()),
        sa.Column("reason", sa.Text()),
        sa.Column("digested_at", sa.DateTime(timezone=True)),
        sa.UniqueConstraint("source_id", "external_id", name="uq_monitor_event_source_external"),
        sa.CheckConstraint("char_length(excerpt) <= 4000", name="excerpt_length"),
        sa.CheckConstraint("state IN ('new', 'dropped', 'classified', 'digested')", name="state"),
        sa.CheckConstraint("relevance >= 0 AND relevance <= 1", name="relevance"),
    )
    op.create_index("ix_monitor_events_state", "monitor_events", ["state", "fetched_at"])
    op.create_index("ix_monitor_events_url", "monitor_events", ["url"])
    op.create_table(
        "monitor_source_fetches",
        sa.Column("source_id", sa.String(100), primary_key=True),
        sa.Column("fetched_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_table(
        "monitor_digests",
        sa.Column("key", sa.String(160), primary_key=True),
        sa.Column("state", sa.String(12), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("event_ids", postgresql.JSONB(), nullable=False),
    )


def downgrade() -> None:
    op.drop_table("monitor_digests")
    op.drop_table("monitor_source_fetches")
    op.drop_table("monitor_events")
