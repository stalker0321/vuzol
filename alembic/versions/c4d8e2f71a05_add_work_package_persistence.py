"""add work package persistence

Revision ID: c4d8e2f71a05
Revises: f3a7c91d2e04
Create Date: 2026-07-31 17:01:11.154831
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "c4d8e2f71a05"  # pragma: allowlist secret
down_revision: str | Sequence[str] | None = "f3a7c91d2e04"  # pragma: allowlist secret
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "work_packages",
        sa.Column("session_id", sa.UUID(), nullable=False),
        sa.Column("project_id", sa.String(length=100), nullable=False),
        sa.Column(
            "status",
            sa.Enum(
                "draft",
                "approved",
                "running",
                "paused",
                "completed",
                "stopped",
                "discarded",
                name="work_package_status",
                native_enum=False,
                create_constraint=True,
            ),
            nullable=False,
        ),
        sa.Column("title", sa.String(length=240), nullable=False),
        sa.Column("head_revision_id", sa.UUID(), nullable=True),
        sa.Column("approved_revision_id", sa.UUID(), nullable=True),
        sa.Column("running_revision_id", sa.UUID(), nullable=True),
        sa.Column("version", sa.Integer(), server_default="1", nullable=False),
        sa.Column("start_generation", sa.Integer(), nullable=True),
        sa.Column("cursor_ordinal", sa.Integer(), nullable=True),
        sa.Column(
            "pause_reason",
            sa.Enum(
                "user",
                "item_failed",
                "replan_required",
                "policy",
                name="work_package_pause_reason",
                native_enum=False,
                create_constraint=True,
            ),
            nullable=True,
        ),
        sa.Column("last_failure_task_id", sa.UUID(), nullable=True),
        sa.Column(
            "queue_mode",
            sa.Enum(
                "sequential",
                name="work_package_queue_mode",
                native_enum=False,
                create_constraint=True,
            ),
            server_default="sequential",
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
            "char_length(title) BETWEEN 1 AND 240",
            name=op.f("ck_work_packages_work_package_title_bounded"),
        ),
        sa.CheckConstraint(
            "cursor_ordinal IS NULL OR cursor_ordinal >= 1",
            name=op.f("ck_work_packages_work_package_cursor_ordinal_positive"),
        ),
        sa.CheckConstraint(
            "start_generation IS NULL OR start_generation >= 1",
            name=op.f("ck_work_packages_work_package_start_generation_positive"),
        ),
        sa.CheckConstraint(
            "version >= 1", name=op.f("ck_work_packages_work_package_version_positive")
        ),
        sa.ForeignKeyConstraint(
            ["last_failure_task_id"],
            ["tasks.id"],
            name=op.f("fk_work_packages_last_failure_task_id_tasks"),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["session_id"],
            ["project_discussion_sessions.id"],
            name=op.f("fk_work_packages_session_id_project_discussion_sessions"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_work_packages")),
        sa.UniqueConstraint("id", "session_id", name="uq_work_package_session_identity"),
    )
    op.create_index(
        op.f("ix_work_packages_project_id"), "work_packages", ["project_id"], unique=False
    )
    op.create_index(
        op.f("ix_work_packages_session_id"), "work_packages", ["session_id"], unique=False
    )
    op.create_index(
        "uq_live_work_package_session",
        "work_packages",
        ["session_id"],
        unique=True,
        postgresql_where=sa.text("status IN ('draft', 'approved', 'running', 'paused')"),
    )
    op.create_table(
        "plan_revisions",
        sa.Column("work_package_id", sa.UUID(), nullable=False),
        sa.Column("revision_number", sa.Integer(), nullable=False),
        sa.Column("parent_revision_id", sa.UUID(), nullable=True),
        sa.Column(
            "state",
            sa.Enum(
                "draft",
                "approved",
                "superseded",
                "discarded",
                name="plan_revision_state",
                native_enum=False,
                create_constraint=True,
            ),
            nullable=False,
        ),
        sa.Column("content_hash", sa.String(length=64), nullable=False),
        sa.Column(
            "created_by",
            sa.Enum(
                "user",
                "planner_model",
                "system",
                name="plan_revision_created_by",
                native_enum=False,
                create_constraint=True,
            ),
            nullable=False,
        ),
        sa.Column("planner_profile", sa.String(length=100), nullable=True),
        sa.Column("prompt_version", sa.String(length=100), nullable=True),
        sa.Column("approved_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("approved_by_user_id", sa.BigInteger(), nullable=True),
        sa.Column("approval_token_hash", sa.String(length=64), nullable=True),
        sa.Column("immutable_body", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("id", sa.UUID(), nullable=False),
        sa.CheckConstraint(
            "content_hash ~ '^[0-9a-f]{64}$'",
            name=op.f("ck_plan_revisions_plan_revision_content_hash_lower_hex"),
        ),
        sa.CheckConstraint(
            "jsonb_typeof(immutable_body) = 'object'",
            name=op.f("ck_plan_revisions_plan_revision_body_object"),
        ),
        sa.CheckConstraint(
            "state != 'approved' OR (approved_at IS NOT NULL AND approved_by_user_id IS NOT NULL)",
            name=op.f("ck_plan_revisions_plan_revision_approval_provenance"),
        ),
        sa.CheckConstraint(
            "approval_token_hash IS NULL OR char_length(approval_token_hash) = 64",
            name=op.f("ck_plan_revisions_plan_revision_approval_token_hash_length"),
        ),
        sa.CheckConstraint(
            "revision_number >= 1", name=op.f("ck_plan_revisions_plan_revision_number_positive")
        ),
        sa.ForeignKeyConstraint(
            ["parent_revision_id", "work_package_id"],
            ["plan_revisions.id", "plan_revisions.work_package_id"],
            name=op.f("fk_plan_revisions_parent_revision_id_plan_revisions"),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["work_package_id"],
            ["work_packages.id"],
            name=op.f("fk_plan_revisions_work_package_id_work_packages"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_plan_revisions")),
        sa.UniqueConstraint("id", "work_package_id", name="uq_plan_revision_package_identity"),
        sa.UniqueConstraint("work_package_id", "revision_number", name="uq_plan_revision_number"),
    )
    op.create_index(
        op.f("ix_plan_revisions_work_package_id"),
        "plan_revisions",
        ["work_package_id"],
        unique=False,
    )
    op.create_foreign_key(
        "fk_work_package_head_revision",
        "work_packages",
        "plan_revisions",
        ["head_revision_id", "id"],
        ["id", "work_package_id"],
        ondelete="RESTRICT",
    )
    op.create_foreign_key(
        "fk_work_package_approved_revision",
        "work_packages",
        "plan_revisions",
        ["approved_revision_id", "id"],
        ["id", "work_package_id"],
        ondelete="RESTRICT",
    )
    op.create_foreign_key(
        "fk_work_package_running_revision",
        "work_packages",
        "plan_revisions",
        ["running_revision_id", "id"],
        ["id", "work_package_id"],
        ondelete="RESTRICT",
    )
    op.create_table(
        "work_item_drafts",
        sa.Column("work_package_id", sa.UUID(), nullable=False),
        sa.Column("local_id", sa.String(length=64), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("id", sa.UUID(), nullable=False),
        sa.CheckConstraint(
            "local_id IS NULL OR (char_length(local_id) BETWEEN 1 AND 64 "
            "AND local_id ~ '^[a-z0-9]+(?:-[a-z0-9]+)*$')",
            name=op.f("ck_work_item_drafts_work_item_local_id_slug"),
        ),
        sa.ForeignKeyConstraint(
            ["work_package_id"],
            ["work_packages.id"],
            name=op.f("fk_work_item_drafts_work_package_id_work_packages"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_work_item_drafts")),
        sa.UniqueConstraint("id", "work_package_id", name="uq_work_item_package_identity"),
    )
    op.create_index(
        op.f("ix_work_item_drafts_work_package_id"),
        "work_item_drafts",
        ["work_package_id"],
        unique=False,
    )
    op.create_index(
        "uq_work_item_local_id",
        "work_item_drafts",
        ["work_package_id", "local_id"],
        unique=True,
        postgresql_where=sa.text("local_id IS NOT NULL"),
    )
    op.create_table(
        "plan_revision_items",
        sa.Column("work_package_id", sa.UUID(), nullable=False),
        sa.Column("plan_revision_id", sa.UUID(), nullable=False),
        sa.Column("item_id", sa.UUID(), nullable=False),
        sa.Column("ordinal", sa.Integer(), nullable=False),
        sa.Column("summary", sa.String(length=240), nullable=False),
        sa.Column("goal", sa.Text(), nullable=False),
        sa.Column("expected_outcome", sa.Text(), nullable=False),
        sa.Column("completion_criteria", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("allowed_scope", sa.Text(), nullable=False),
        sa.Column(
            "out_of_scope",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'[]'::jsonb"),
            nullable=False,
        ),
        sa.Column(
            "dependencies",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'[]'::jsonb"),
            nullable=False,
        ),
        sa.Column(
            "trusted_checks",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'[]'::jsonb"),
            nullable=False,
        ),
        sa.Column(
            "suggested_risk",
            sa.Enum(
                "low",
                "medium",
                "high",
                "privileged",
                name="plan_item_risk_level",
                native_enum=False,
                create_constraint=True,
            ),
            nullable=False,
        ),
        sa.Column("needs_approval", sa.Boolean(), server_default="false", nullable=False),
        sa.Column(
            "estimated_complexity",
            sa.Enum(
                "small",
                "medium",
                "large",
                name="estimated_complexity",
                native_enum=False,
                create_constraint=True,
            ),
            server_default="medium",
            nullable=False,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("id", sa.UUID(), nullable=False),
        sa.CheckConstraint(
            "char_length(allowed_scope) >= 1",
            name=op.f("ck_plan_revision_items_plan_revision_item_allowed_scope_required"),
        ),
        sa.CheckConstraint(
            "char_length(expected_outcome) >= 1",
            name=op.f("ck_plan_revision_items_plan_revision_item_expected_outcome_required"),
        ),
        sa.CheckConstraint(
            "char_length(goal) >= 1",
            name=op.f("ck_plan_revision_items_plan_revision_item_goal_required"),
        ),
        sa.CheckConstraint(
            "char_length(summary) BETWEEN 1 AND 240",
            name=op.f("ck_plan_revision_items_plan_revision_item_summary_bounded"),
        ),
        sa.CheckConstraint(
            "jsonb_array_length(completion_criteria) >= 1",
            name=op.f("ck_plan_revision_items_plan_revision_item_completion_criteria_required"),
        ),
        sa.CheckConstraint(
            "ordinal >= 1", name=op.f("ck_plan_revision_items_plan_revision_item_ordinal_positive")
        ),
        sa.ForeignKeyConstraint(
            ["item_id", "work_package_id"],
            ["work_item_drafts.id", "work_item_drafts.work_package_id"],
            name=op.f("fk_plan_revision_items_item_id_work_item_drafts"),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["plan_revision_id", "work_package_id"],
            ["plan_revisions.id", "plan_revisions.work_package_id"],
            name=op.f("fk_plan_revision_items_plan_revision_id_plan_revisions"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_plan_revision_items")),
        sa.UniqueConstraint(
            "id",
            "plan_revision_id",
            "item_id",
            "work_package_id",
            "ordinal",
            name="uq_plan_revision_item_fenced_identity",
        ),
        sa.UniqueConstraint(
            "plan_revision_id",
            "item_id",
            "work_package_id",
            "ordinal",
            name="uq_plan_revision_item_fenced_membership",
        ),
        sa.UniqueConstraint(
            "plan_revision_id",
            "item_id",
            "work_package_id",
            name="uq_plan_revision_item_package_identity",
        ),
        sa.UniqueConstraint("plan_revision_id", "item_id", name="uq_plan_revision_item_identity"),
        sa.UniqueConstraint("plan_revision_id", "ordinal", name="uq_plan_revision_item_ordinal"),
    )
    op.create_index(
        op.f("ix_plan_revision_items_item_id"), "plan_revision_items", ["item_id"], unique=False
    )
    op.create_index(
        op.f("ix_plan_revision_items_plan_revision_id"),
        "plan_revision_items",
        ["plan_revision_id"],
        unique=False,
    )
    op.create_table(
        "edit_sessions",
        sa.Column("package_id", sa.UUID(), nullable=False),
        sa.Column("plan_revision_id", sa.UUID(), nullable=False),
        sa.Column("plan_revision_number", sa.Integer(), nullable=False),
        sa.Column("content_hash", sa.String(length=64), nullable=False),
        sa.Column("item_id", sa.UUID(), nullable=False),
        sa.Column("ordinal", sa.Integer(), nullable=False),
        sa.Column("opened_by_user_id", sa.BigInteger(), nullable=False),
        sa.Column(
            "status",
            sa.Enum(
                "open",
                "accepted",
                "closed",
                "expired",
                name="edit_session_status",
                native_enum=False,
                create_constraint=True,
            ),
            nullable=False,
        ),
        sa.Column("session_generation", sa.Integer(), server_default="1", nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
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
            "content_hash ~ '^[0-9a-f]{64}$'",
            name=op.f("ck_edit_sessions_edit_session_hash_lower_hex"),
        ),
        sa.CheckConstraint(
            "ordinal >= 1", name=op.f("ck_edit_sessions_edit_session_ordinal_positive")
        ),
        sa.CheckConstraint(
            "plan_revision_number >= 1",
            name=op.f("ck_edit_sessions_edit_session_revision_positive"),
        ),
        sa.CheckConstraint(
            "session_generation >= 1",
            name=op.f("ck_edit_sessions_edit_session_generation_positive"),
        ),
        sa.ForeignKeyConstraint(
            ["package_id"],
            ["work_packages.id"],
            name=op.f("fk_edit_sessions_package_id_work_packages"),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["plan_revision_id", "item_id", "package_id", "ordinal"],
            [
                "plan_revision_items.plan_revision_id",
                "plan_revision_items.item_id",
                "plan_revision_items.work_package_id",
                "plan_revision_items.ordinal",
            ],
            name=op.f("fk_edit_sessions_plan_revision_id_plan_revision_items"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_edit_sessions")),
    )
    op.create_index(
        op.f("ix_edit_sessions_package_id"), "edit_sessions", ["package_id"], unique=False
    )
    op.create_index(
        "uq_open_edit_session_author_package",
        "edit_sessions",
        ["package_id", "opened_by_user_id"],
        unique=True,
        postgresql_where=sa.text("status = 'open'"),
    )
    op.create_table(
        "materialization_links",
        sa.Column("work_package_id", sa.UUID(), nullable=False),
        sa.Column("plan_revision_id", sa.UUID(), nullable=False),
        sa.Column("work_item_draft_id", sa.UUID(), nullable=False),
        sa.Column("plan_revision_item_id", sa.UUID(), nullable=False),
        sa.Column("task_id", sa.UUID(), nullable=False),
        sa.Column("ordinal", sa.Integer(), nullable=False),
        sa.Column(
            "materialized_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("id", sa.UUID(), nullable=False),
        sa.CheckConstraint(
            "ordinal >= 1", name=op.f("ck_materialization_links_materialization_ordinal_positive")
        ),
        sa.ForeignKeyConstraint(
            [
                "plan_revision_item_id",
                "plan_revision_id",
                "work_item_draft_id",
                "work_package_id",
                "ordinal",
            ],
            [
                "plan_revision_items.id",
                "plan_revision_items.plan_revision_id",
                "plan_revision_items.item_id",
                "plan_revision_items.work_package_id",
                "plan_revision_items.ordinal",
            ],
            name=op.f("fk_materialization_links_plan_revision_item_id_plan_revision_items"),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["task_id"],
            ["tasks.id"],
            name=op.f("fk_materialization_links_task_id_tasks"),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["work_package_id"],
            ["work_packages.id"],
            name=op.f("fk_materialization_links_work_package_id_work_packages"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_materialization_links")),
        sa.UniqueConstraint(
            "plan_revision_item_id", name=op.f("uq_materialization_links_plan_revision_item_id")
        ),
        sa.UniqueConstraint("task_id", name="uq_materialization_task"),
        sa.UniqueConstraint(
            "work_package_id", "plan_revision_id", "ordinal", name="uq_materialization_ordinal"
        ),
    )
    op.create_index(
        op.f("ix_materialization_links_work_package_id"),
        "materialization_links",
        ["work_package_id"],
        unique=False,
    )
    op.create_table(
        "work_package_open_details",
        sa.Column("package_id", sa.UUID(), nullable=False),
        sa.Column("plan_revision_id", sa.UUID(), nullable=False),
        sa.Column("item_id", sa.UUID(), nullable=False),
        sa.Column("plan_revision_number", sa.Integer(), nullable=False),
        sa.Column("h8", sa.String(length=8), nullable=False),
        sa.Column("ordinal", sa.Integer(), nullable=True),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "h8 ~ '^[0-9a-f]{8}$'",
            name=op.f("ck_work_package_open_details_open_detail_h8_lower_hex"),
        ),
        sa.CheckConstraint(
            "ordinal IS NULL OR ordinal >= 1",
            name=op.f("ck_work_package_open_details_open_detail_ordinal_positive"),
        ),
        sa.CheckConstraint(
            "plan_revision_number >= 1",
            name=op.f("ck_work_package_open_details_open_detail_revision_positive"),
        ),
        sa.ForeignKeyConstraint(
            ["package_id"],
            ["work_packages.id"],
            name=op.f("fk_work_package_open_details_package_id_work_packages"),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["plan_revision_id", "item_id", "package_id"],
            [
                "plan_revision_items.plan_revision_id",
                "plan_revision_items.item_id",
                "plan_revision_items.work_package_id",
            ],
            name=op.f("fk_work_package_open_details_plan_revision_id_plan_revision_items"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("package_id", name=op.f("pk_work_package_open_details")),
    )
    op.add_column(
        "project_discussion_sessions", sa.Column("active_work_package_id", sa.UUID(), nullable=True)
    )
    op.create_foreign_key(
        "fk_discussion_active_work_package",
        "project_discussion_sessions",
        "work_packages",
        ["active_work_package_id", "id"],
        ["id", "session_id"],
        ondelete="RESTRICT",
        use_alter=True,
    )


def downgrade() -> None:
    op.drop_constraint(
        "fk_discussion_active_work_package", "project_discussion_sessions", type_="foreignkey"
    )
    op.drop_column("project_discussion_sessions", "active_work_package_id")
    op.drop_table("work_package_open_details")
    op.drop_index(
        op.f("ix_materialization_links_work_package_id"), table_name="materialization_links"
    )
    op.drop_table("materialization_links")
    op.drop_index(
        "uq_open_edit_session_author_package",
        table_name="edit_sessions",
        postgresql_where=sa.text("status = 'open'"),
    )
    op.drop_index(op.f("ix_edit_sessions_package_id"), table_name="edit_sessions")
    op.drop_table("edit_sessions")
    op.drop_index(op.f("ix_plan_revision_items_plan_revision_id"), table_name="plan_revision_items")
    op.drop_index(op.f("ix_plan_revision_items_item_id"), table_name="plan_revision_items")
    op.drop_table("plan_revision_items")
    op.drop_index(
        "uq_work_item_local_id",
        table_name="work_item_drafts",
        postgresql_where=sa.text("local_id IS NOT NULL"),
    )
    op.drop_index(op.f("ix_work_item_drafts_work_package_id"), table_name="work_item_drafts")
    op.drop_table("work_item_drafts")
    op.drop_constraint("fk_work_package_running_revision", "work_packages", type_="foreignkey")
    op.drop_constraint("fk_work_package_approved_revision", "work_packages", type_="foreignkey")
    op.drop_constraint("fk_work_package_head_revision", "work_packages", type_="foreignkey")
    op.drop_index(op.f("ix_plan_revisions_work_package_id"), table_name="plan_revisions")
    op.drop_table("plan_revisions")
    op.drop_index(
        "uq_live_work_package_session",
        table_name="work_packages",
        postgresql_where=sa.text("status IN ('draft', 'approved', 'running', 'paused')"),
    )
    op.drop_index(op.f("ix_work_packages_session_id"), table_name="work_packages")
    op.drop_index(op.f("ix_work_packages_project_id"), table_name="work_packages")
    op.drop_table("work_packages")
