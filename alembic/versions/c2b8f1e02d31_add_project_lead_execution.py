"""Add managed-goal execution links.

Additive only. Legacy packages keep a null managed_goal_id. An approved plan
revision still needs a timestamp plus either a human user or a delegation.

Revision ID: c2b8f1e02d31
Revises: b1a7e0d91c20
Create Date: 2026-10-19 18:00:00
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "c2b8f1e02d31"  # pragma: allowlist secret
down_revision: str | Sequence[str] | None = "b1a7e0d91c20"  # pragma: allowlist secret
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

JSON_OBJECT = sa.text("'{}'::jsonb")


def upgrade() -> None:
    op.add_column(
        "plan_revisions",
        sa.Column("approved_by_delegation_id", sa.UUID(), nullable=True),
    )
    op.create_foreign_key(
        "fk_plan_revisions_approved_by_delegation",
        "plan_revisions",
        "goal_delegations",
        ["approved_by_delegation_id"],
        ["id"],
        ondelete="RESTRICT",
    )
    op.drop_constraint("plan_revision_approval_provenance", "plan_revisions", type_="check")
    op.create_check_constraint(
        "plan_revision_approval_provenance",
        "plan_revisions",
        "state != 'approved' OR (approved_at IS NOT NULL AND "
        "(approved_by_user_id IS NOT NULL OR approved_by_delegation_id IS NOT NULL))",
    )
    op.add_column("work_packages", sa.Column("managed_goal_id", sa.UUID(), nullable=True))
    op.create_foreign_key(
        "fk_work_packages_managed_goal",
        "work_packages",
        "managed_goals",
        ["managed_goal_id"],
        ["id"],
        ondelete="RESTRICT",
    )
    op.create_index("ix_work_packages_managed_goal_id", "work_packages", ["managed_goal_id"])
    op.create_table(
        "leadership_turns",
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
        sa.Column("goal_id", sa.UUID(), nullable=False),
        sa.Column("wakeup_id", sa.UUID(), nullable=False),
        sa.Column("lease_epoch", sa.Integer(), nullable=False),
        sa.Column("goal_version", sa.Integer(), nullable=False),
        sa.Column("scope_revision", sa.Integer(), nullable=False),
        sa.Column("fence_epoch", sa.Integer(), nullable=False),
        sa.Column("profile_id", sa.String(length=100), nullable=False),
        sa.Column("usage", postgresql.JSONB(), server_default=JSON_OBJECT, nullable=False),
        sa.Column("decision", postgresql.JSONB(), nullable=True),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column("owner", sa.String(length=200), nullable=False),
        sa.CheckConstraint(
            "status IN ('claimed','responded','applied','superseded','lost')",
            name="leadership_turn_status",
        ),
        sa.CheckConstraint("lease_epoch >= 1", name="leadership_turn_lease_epoch_positive"),
        sa.CheckConstraint("jsonb_typeof(usage) = 'object'", name="leadership_turn_usage_object"),
        sa.ForeignKeyConstraint(["goal_id"], ["managed_goals.id"], ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "uq_leadership_turn_wakeup",
        "leadership_turns",
        ["goal_id", "wakeup_id"],
        unique=True,
    )


def downgrade() -> None:
    op.drop_index("uq_leadership_turn_wakeup", table_name="leadership_turns")
    op.drop_table("leadership_turns")
    op.drop_index("ix_work_packages_managed_goal_id", table_name="work_packages")
    op.drop_constraint("fk_work_packages_managed_goal", "work_packages", type_="foreignkey")
    op.drop_column("work_packages", "managed_goal_id")
    op.drop_constraint("plan_revision_approval_provenance", "plan_revisions", type_="check")
    op.create_check_constraint(
        "plan_revision_approval_provenance",
        "plan_revisions",
        "state != 'approved' OR (approved_at IS NOT NULL AND approved_by_user_id IS NOT NULL)",
    )
    op.drop_constraint(
        "fk_plan_revisions_approved_by_delegation", "plan_revisions", type_="foreignkey"
    )
    op.drop_column("plan_revisions", "approved_by_delegation_id")
