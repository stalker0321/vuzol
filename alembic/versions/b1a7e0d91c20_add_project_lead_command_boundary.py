"""Add the project-lead command boundary.

Additive tables only. Legacy tasks, packages, and approvals do not read them.
Feature-off is a process flag, not a schema rollback.

Revision ID: b1a7e0d91c20
Revises: d5e1f4c7a9b2
Create Date: 2026-10-19 12:00:00
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "b1a7e0d91c20"  # pragma: allowlist secret
down_revision: str | Sequence[str] | None = "d5e1f4c7a9b2"  # pragma: allowlist secret
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

JSON_OBJECT = sa.text("'{}'::jsonb")
JSON_ARRAY = sa.text("'[]'::jsonb")
LIVE = sa.text("lifecycle IN ('draft','active','waiting','paused')")


def upgrade() -> None:
    op.create_table(
        "goal_delegations",
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
        sa.Column("project_id", sa.String(length=100), nullable=False),
        sa.Column("owner_user_id", sa.BigInteger(), nullable=False),
        sa.Column("agent_principal_id", sa.String(length=200), nullable=False),
        sa.Column("policy_revision", sa.String(length=64), nullable=False),
        sa.Column("allowed_actions", postgresql.JSONB(), server_default=JSON_ARRAY, nullable=False),
        sa.Column("integration_branch", sa.String(length=255), nullable=False),
        sa.Column("environments", postgresql.JSONB(), server_default=JSON_ARRAY, nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("revoked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("version", sa.Integer(), server_default="1", nullable=False),
        sa.CheckConstraint(
            "agent_principal_id LIKE 'agent:%' AND agent_principal_id !~ '^agent:[0-9]+$'",
            name="goal_delegation_agent_principal",
        ),
        sa.CheckConstraint("version >= 1", name="goal_delegation_version_positive"),
        sa.CheckConstraint(
            "jsonb_typeof(allowed_actions) = 'array'",
            name="goal_delegation_actions_array",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_goal_delegations_project_id", "goal_delegations", ["project_id"])
    op.create_table(
        "managed_goals",
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
        sa.Column("project_id", sa.String(length=100), nullable=False),
        sa.Column("owner_user_id", sa.BigInteger(), nullable=False),
        sa.Column("discussion_session_id", sa.UUID(), nullable=False),
        sa.Column("delegation_id", sa.UUID(), nullable=False),
        sa.Column("source_chat_id", sa.BigInteger(), nullable=True),
        sa.Column("source_thread_id", sa.BigInteger(), nullable=True),
        sa.Column("original_request", sa.Text(), nullable=False),
        sa.Column("scope_text", sa.Text(), nullable=False),
        sa.Column("scope_revision", sa.Integer(), server_default="1", nullable=False),
        sa.Column("criteria", postgresql.JSONB(), server_default=JSON_ARRAY, nullable=False),
        sa.Column("constraints", postgresql.JSONB(), server_default=JSON_OBJECT, nullable=False),
        sa.Column("lifecycle", sa.String(length=20), nullable=False),
        sa.Column("waiting_reason", sa.String(length=80), nullable=True),
        sa.Column("wakeup_condition", postgresql.JSONB(), nullable=True),
        sa.Column("integration_branch", sa.String(length=255), nullable=False),
        sa.Column("current_package_id", sa.UUID(), nullable=True),
        sa.Column("proposed_plan", postgresql.JSONB(), nullable=True),
        sa.Column("version", sa.Integer(), server_default="1", nullable=False),
        sa.Column("fence_epoch", sa.Integer(), server_default="1", nullable=False),
        sa.Column("lease_owner", sa.String(length=200), nullable=True),
        sa.Column("lease_epoch", sa.Integer(), server_default="0", nullable=False),
        sa.Column("lease_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column("repeat_fingerprint", sa.String(length=64), nullable=True),
        sa.Column("repeat_count", sa.Integer(), server_default="0", nullable=False),
        sa.CheckConstraint(
            "lifecycle IN ('draft','active','waiting','paused','completed','cancelled')",
            name="managed_goal_lifecycle",
        ),
        sa.CheckConstraint("version >= 1", name="managed_goal_version_positive"),
        sa.CheckConstraint("scope_revision >= 1", name="managed_goal_scope_revision_positive"),
        sa.CheckConstraint("fence_epoch >= 1", name="managed_goal_fence_epoch_positive"),
        sa.CheckConstraint("lease_epoch >= 0", name="managed_goal_lease_epoch_nonnegative"),
        sa.CheckConstraint("repeat_count >= 0", name="managed_goal_repeat_count_nonnegative"),
        sa.CheckConstraint("jsonb_typeof(criteria) = 'array'", name="managed_goal_criteria_array"),
        sa.ForeignKeyConstraint(
            ["discussion_session_id"],
            ["project_discussion_sessions.id"],
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(["delegation_id"], ["goal_delegations.id"], ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "uq_managed_goal_live_project",
        "managed_goals",
        ["project_id"],
        unique=True,
        postgresql_where=LIVE,
    )
    op.create_index(
        "uq_managed_goal_live_session",
        "managed_goals",
        ["discussion_session_id"],
        unique=True,
        postgresql_where=LIVE,
    )
    op.create_table(
        "leadership_command_receipts",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("actor_kind", sa.String(length=16), nullable=False),
        sa.Column("actor_subject", sa.String(length=200), nullable=False),
        sa.Column("ingress_source", sa.String(length=20), nullable=False),
        sa.Column("command_name", sa.String(length=64), nullable=False),
        sa.Column("project_id", sa.String(length=100), nullable=False),
        sa.Column("goal_id", sa.UUID(), nullable=True),
        sa.Column("expected_version", sa.Integer(), nullable=True),
        sa.Column("idempotency_key", sa.String(length=255), nullable=False),
        sa.Column("payload_hash", sa.String(length=64), nullable=False),
        sa.Column("payload", postgresql.JSONB(), server_default=JSON_OBJECT, nullable=False),
        sa.Column("contract_version", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column("result", postgresql.JSONB(), server_default=JSON_OBJECT, nullable=False),
        sa.Column("reason", sa.String(length=120), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint("actor_kind IN ('human','agent')", name="leadership_receipt_actor_kind"),
        sa.CheckConstraint(
            "status IN ('applied','duplicate','stale','denied','waiting','unsupported')",
            name="leadership_receipt_status",
        ),
        sa.CheckConstraint(
            "char_length(payload_hash) = 64", name="leadership_receipt_payload_hash"
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "uq_leadership_receipt_idempotency",
        "leadership_command_receipts",
        ["actor_kind", "actor_subject", "idempotency_key"],
        unique=True,
    )


def downgrade() -> None:
    op.drop_index("uq_leadership_receipt_idempotency", table_name="leadership_command_receipts")
    op.drop_table("leadership_command_receipts")
    op.drop_index("uq_managed_goal_live_session", table_name="managed_goals")
    op.drop_index("uq_managed_goal_live_project", table_name="managed_goals")
    op.drop_table("managed_goals")
    op.drop_index("ix_goal_delegations_project_id", table_name="goal_delegations")
    op.drop_table("goal_delegations")
