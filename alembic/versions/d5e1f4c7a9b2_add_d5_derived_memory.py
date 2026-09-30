"""Add D5 derived-memory substrate (additive only).

- memory_units: semantic units with origin refs/versions, scope, status,
  supersession chain, unique extraction identity, FTS index.
- No renames, no backfill (table is new in D5; legacy = no rows).

Revision ID: d5e1f4c7a9b2
Revises: d3c1a8f2e4b7
Create Date: 2026-09-30
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import TSVECTOR, UUID

revision: str = "d5e1f4c7a9b2"  # pragma: allowlist secret
down_revision: str | Sequence[str] | None = "d3c1a8f2e4b7"  # pragma: allowlist secret
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_MEMORY_UNIT_TYPES = ("decision_template", "outcome_template", "observation", "lesson")
_MEMORY_UNIT_STATUSES = (
    "hypothesis",
    "observation",
    "verified",
    "superseded",
    "retracted",
    "tombstoned",
)


def upgrade() -> None:
    op.create_table(
        "memory_units",
        sa.Column("id", UUID(as_uuid=True), nullable=False),
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
        sa.Column("project_id", sa.String(length=100), nullable=True),
        sa.Column("session_id", UUID(as_uuid=True), nullable=True),
        sa.Column("unit_type", sa.String(length=30), nullable=False),
        sa.Column("status", sa.String(length=30), nullable=False),
        sa.Column("text", sa.Text(), nullable=False),
        sa.Column(
            "text_search",
            TSVECTOR(),
            sa.Computed("to_tsvector('simple', text)", persisted=True),
            nullable=True,
        ),
        sa.Column("trigger_event_id", UUID(as_uuid=True), nullable=True),
        sa.Column("extractor_version", sa.String(length=30), nullable=False),
        sa.Column("source_event_id", UUID(as_uuid=True), nullable=True),
        sa.Column("source_decision_id", UUID(as_uuid=True), nullable=True),
        sa.Column("source_artifact_id", UUID(as_uuid=True), nullable=True),
        sa.Column("source_acceptance_evidence_id", UUID(as_uuid=True), nullable=True),
        sa.Column("source_turn_id", UUID(as_uuid=True), nullable=True),
        sa.Column("source_summary_id", UUID(as_uuid=True), nullable=True),
        sa.Column("source_key", sa.String(length=64), nullable=True),
        sa.Column("effective_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "observed_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("superseded_by", UUID(as_uuid=True), nullable=True),
        sa.Column("superseded_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("extraction_identity", sa.String(length=255), nullable=False),
        sa.Column("tombstone_event_id", UUID(as_uuid=True), nullable=True),
        sa.Column("redaction_revision", sa.String(length=64), nullable=True),
        sa.ForeignKeyConstraint(["session_id"], ["project_discussion_sessions.id"]),
        sa.ForeignKeyConstraint(["source_event_id"], ["events.id"]),
        sa.ForeignKeyConstraint(["source_decision_id"], ["accepted_decisions.id"]),
        sa.ForeignKeyConstraint(["source_artifact_id"], ["artifacts.id"]),
        sa.ForeignKeyConstraint(["source_acceptance_evidence_id"], ["acceptance_evidence.id"]),
        sa.ForeignKeyConstraint(["source_turn_id"], ["conversation_turns.id"]),
        sa.ForeignKeyConstraint(["superseded_by"], ["memory_units.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(
            "unit_type IN ('decision_template', 'outcome_template', 'observation', 'lesson')",
            name="memory_unit_type_bounded",
        ),
        sa.CheckConstraint(
            "status IN "
            "('hypothesis', 'observation', 'verified', 'superseded', 'retracted', 'tombstoned')",
            name="memory_unit_status_bounded",
        ),
        sa.CheckConstraint(
            "char_length(text) BETWEEN 1 AND 4000", name="memory_unit_text_bounded"
        ),
        sa.CheckConstraint(
            "char_length(extraction_identity) BETWEEN 1 AND 255",
            name="memory_unit_extraction_identity_bounded",
        ),
        sa.UniqueConstraint(
            "extraction_identity", name="uq_memory_unit_extraction_identity"
        ),
    )
    op.create_index(
        "ix_memory_units_project_id", "memory_units", ["project_id"], unique=False
    )
    op.create_index(
        "ix_memory_units_session_id", "memory_units", ["session_id"], unique=False
    )
    op.create_index(
        "ix_memory_units_project_type_status",
        "memory_units",
        ["project_id", "unit_type", "status"],
        unique=False,
    )
    op.create_index(
        "ix_memory_units_chain",
        "memory_units",
        ["project_id", "session_id", "unit_type", "source_key"],
        unique=False,
    )
    op.create_index(
        "ix_memory_units_source_decision_id",
        "memory_units",
        ["source_decision_id"],
        unique=False,
    )
    op.create_index(
        "ix_memory_units_source_artifact",
        "memory_units",
        ["source_artifact_id"],
        unique=False,
    )
    op.execute(
        "CREATE INDEX ix_memory_units_text_search "
        "ON memory_units USING gin (text_search)"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_memory_units_text_search")
    op.drop_table("memory_units")
