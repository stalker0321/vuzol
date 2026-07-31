"""add project discussion persistence

Revision ID: f3a7c91d2e04
Revises: b2c9e4f81a03
Create Date: 2026-07-31 12:00:00
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "f3a7c91d2e04"  # pragma: allowlist secret
down_revision: str | Sequence[str] | None = "b2c9e4f81a03"  # pragma: allowlist secret
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "project_discussion_sessions",
        sa.Column("project_id", sa.String(length=100), nullable=False),
        sa.Column("chat_id", sa.BigInteger(), nullable=False),
        sa.Column("message_thread_id", sa.BigInteger(), nullable=False),
        sa.Column(
            "status",
            sa.Enum(
                "active",
                "archived",
                "superseded",
                name="discussion_session_status",
                native_enum=False,
                create_constraint=True,
            ),
            nullable=False,
        ),
        sa.Column("summary_revision", sa.Integer(), server_default="0", nullable=False),
        sa.Column("version", sa.Integer(), server_default="1", nullable=False),
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
        sa.CheckConstraint("summary_revision >= 0", name="discussion_summary_revision_nonnegative"),
        sa.CheckConstraint("version >= 1", name="discussion_session_version_positive"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_project_discussion_project_status",
        "project_discussion_sessions",
        ["project_id", "status"],
    )
    op.create_index(
        "uq_active_project_discussion_topic",
        "project_discussion_sessions",
        ["chat_id", "message_thread_id"],
        unique=True,
        postgresql_where=sa.text("status = 'active'"),
    )

    op.create_table(
        "conversation_turns",
        sa.Column("session_id", sa.UUID(), nullable=False),
        sa.Column("ordinal", sa.Integer(), nullable=False),
        sa.Column(
            "role",
            sa.Enum(
                "user",
                "assistant",
                "system",
                name="conversation_turn_role",
                native_enum=False,
                create_constraint=True,
            ),
            nullable=False,
        ),
        sa.Column(
            "source",
            sa.Enum(
                "telegram_user",
                "model",
                "control",
                name="conversation_turn_source",
                native_enum=False,
                create_constraint=True,
            ),
            nullable=False,
        ),
        sa.Column("intake_message_id", sa.UUID()),
        sa.Column("content", sa.Text(), nullable=False),
        sa.Column("content_hash", sa.String(length=64), nullable=False),
        sa.Column(
            "classifier_mode",
            sa.Enum(
                "discussion",
                "plan_request",
                "task_request",
                "plan_control",
                "item_edit",
                "query_only",
                "query_refuse",
                name="interaction_mode",
                native_enum=False,
                create_constraint=True,
            ),
            nullable=False,
        ),
        sa.Column("classifier_confidence", sa.Numeric(precision=5, scale=4)),
        sa.Column("classifier_prompt_version", sa.String(length=100)),
        sa.Column("should_create_task", sa.Boolean(), server_default="false", nullable=False),
        sa.Column("override_kind", sa.String(length=50)),
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint("ordinal >= 1", name="conversation_turn_ordinal_positive"),
        sa.CheckConstraint(
            "classifier_confidence IS NULL OR "
            "(classifier_confidence >= 0 AND classifier_confidence <= 1)",
            name="conversation_turn_confidence_range",
        ),
        sa.CheckConstraint(
            "NOT should_create_task OR classifier_mode = 'task_request'",
            name="conversation_turn_task_mode_consistent",
        ),
        sa.CheckConstraint(
            "char_length(content) BETWEEN 1 AND 100000", name="conversation_turn_content_bounded"
        ),
        sa.CheckConstraint("char_length(content_hash) = 64", name="conversation_turn_hash_length"),
        sa.ForeignKeyConstraint(
            ["intake_message_id"], ["telegram_intake_messages.id"], ondelete="RESTRICT"
        ),
        sa.ForeignKeyConstraint(
            ["session_id"], ["project_discussion_sessions.id"], ondelete="RESTRICT"
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("intake_message_id"),
        sa.UniqueConstraint("session_id", "ordinal", name="uq_conversation_turn_ordinal"),
    )
    op.create_index("ix_conversation_turns_session_id", "conversation_turns", ["session_id"])

    op.create_table(
        "conversation_summaries",
        sa.Column("session_id", sa.UUID(), nullable=False),
        sa.Column("revision", sa.Integer(), nullable=False),
        sa.Column("body", sa.Text(), nullable=False),
        sa.Column("covered_through_turn_ordinal", sa.Integer(), nullable=False),
        sa.Column(
            "generator",
            sa.Enum(
                "model",
                "heuristic",
                "user_edit",
                name="conversation_summary_generator",
                native_enum=False,
                create_constraint=True,
            ),
            nullable=False,
        ),
        sa.Column("content_hash", sa.String(length=64), nullable=False),
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint("revision >= 1", name="conversation_summary_revision_positive"),
        sa.CheckConstraint(
            "covered_through_turn_ordinal >= 1", name="conversation_summary_covered_turn_positive"
        ),
        sa.CheckConstraint(
            "char_length(body) BETWEEN 1 AND 100000", name="conversation_summary_body_bounded"
        ),
        sa.CheckConstraint(
            "char_length(content_hash) = 64", name="conversation_summary_hash_length"
        ),
        sa.ForeignKeyConstraint(
            ["session_id"], ["project_discussion_sessions.id"], ondelete="RESTRICT"
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("session_id", "revision", name="uq_conversation_summary_revision"),
    )
    op.create_index(
        "ix_conversation_summaries_session_id", "conversation_summaries", ["session_id"]
    )

    op.create_table(
        "accepted_decisions",
        sa.Column("session_id", sa.UUID(), nullable=False),
        sa.Column("key", sa.String(length=64), nullable=False),
        sa.Column("statement", sa.Text(), nullable=False),
        sa.Column("source_turn_id", sa.UUID()),
        sa.Column("accepted_by_user_id", sa.BigInteger(), nullable=False),
        sa.Column(
            "status",
            sa.Enum(
                "active",
                "superseded",
                "retracted",
                name="accepted_decision_status",
                native_enum=False,
                create_constraint=True,
            ),
            nullable=False,
        ),
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
        sa.CheckConstraint(
            "char_length(key) BETWEEN 1 AND 64", name="accepted_decision_key_bounded"
        ),
        sa.CheckConstraint("key ~ '^[a-z0-9]+(?:-[a-z0-9]+)*$'", name="accepted_decision_key_slug"),
        sa.CheckConstraint(
            "char_length(statement) BETWEEN 1 AND 500",
            name="accepted_decision_statement_bounded",
        ),
        sa.ForeignKeyConstraint(
            ["session_id"], ["project_discussion_sessions.id"], ondelete="RESTRICT"
        ),
        sa.ForeignKeyConstraint(["source_turn_id"], ["conversation_turns.id"], ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_accepted_decisions_session_id", "accepted_decisions", ["session_id"])
    op.create_index(
        "uq_active_accepted_decision_key",
        "accepted_decisions",
        ["session_id", "key"],
        unique=True,
        postgresql_where=sa.text("status = 'active'"),
    )


def downgrade() -> None:
    op.drop_index("uq_active_accepted_decision_key", table_name="accepted_decisions")
    op.drop_index("ix_accepted_decisions_session_id", table_name="accepted_decisions")
    op.drop_table("accepted_decisions")
    op.drop_index("ix_conversation_summaries_session_id", table_name="conversation_summaries")
    op.drop_table("conversation_summaries")
    op.drop_index("ix_conversation_turns_session_id", table_name="conversation_turns")
    op.drop_table("conversation_turns")
    op.drop_index("uq_active_project_discussion_topic", table_name="project_discussion_sessions")
    op.drop_index("ix_project_discussion_project_status", table_name="project_discussion_sessions")
    op.drop_table("project_discussion_sessions")
