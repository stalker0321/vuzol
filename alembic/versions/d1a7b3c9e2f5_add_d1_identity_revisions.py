"""Add D1 identity/revision substrate (additive only).

New tables: `work_attempts` (append-only lineage), `task_spec_revisions`
(versioned spec snapshots), `review_outcome_history` (verdict retention
separate from mutable Step.result). New nullable columns: `tasks.source_turn_id`,
`tasks.spec_revision`, `work_packages.intent_revision`. No renames, no
backfill (NULL/unknown = legacy provenance, never fabricated).

Revision ID: d1a7b3c9e2f5
Revises: d0c0n7r4c7v1
Create Date: 2026-09-30
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB, UUID

revision: str = "d1a7b3c9e2f5"  # pragma: allowlist secret
down_revision: str | Sequence[str] | None = "d0c0n7r4c7v1"  # pragma: allowlist secret
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("tasks", sa.Column("source_turn_id", UUID(as_uuid=True), nullable=True))
    op.create_foreign_key(
        "fk_tasks_source_turn",
        "tasks",
        "conversation_turns",
        ["source_turn_id"],
        ["id"],
        ondelete="RESTRICT",
    )
    op.create_index("ix_tasks_source_turn_id", "tasks", ["source_turn_id"])
    op.add_column("tasks", sa.Column("spec_revision", sa.String(length=64), nullable=True))
    op.add_column(
        "work_packages", sa.Column("intent_revision", sa.String(length=64), nullable=True)
    )
    op.create_table(
        "work_attempts",
        sa.Column("id", UUID(as_uuid=True), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False
        ),
        sa.Column("task_id", UUID(as_uuid=True), nullable=False),
        sa.Column("run_id", UUID(as_uuid=True), nullable=False),
        sa.Column("step_id", UUID(as_uuid=True), nullable=False),
        sa.Column("plan_revision_id", UUID(as_uuid=True), nullable=True),
        sa.Column("item_id", UUID(as_uuid=True), nullable=True),
        sa.Column("stable_item_id", UUID(as_uuid=True), nullable=True),
        sa.Column("horizon_id", UUID(as_uuid=True), nullable=True),
        sa.Column("attempt_no", sa.Integer(), nullable=False),
        sa.Column("parent_attempt_id", UUID(as_uuid=True), nullable=True),
        sa.Column("attempt_kind", sa.String(length=20), nullable=False),
        sa.Column("purpose", sa.String(length=30), nullable=False),
        sa.Column("executor_profile_id", sa.String(length=100), nullable=True),
        sa.Column("executor_model", sa.String(length=200), nullable=True),
        sa.Column("node_id", sa.String(length=100), nullable=True),
        sa.Column("lease_owner", sa.String(length=200), nullable=True),
        sa.Column("lease_generation", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("input_bindings", JSONB, nullable=False, server_default=sa.text("'[]'::jsonb")),
        sa.Column("input_hash", sa.String(length=64), nullable=True),
        sa.Column("output_hash", sa.String(length=64), nullable=True),
        sa.Column("usage_ref", sa.String(length=100), nullable=True),
        sa.Column("cost_known", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("outcome", sa.String(length=20), nullable=False, server_default="running"),
        sa.Column("failure_category", sa.String(length=100), nullable=True),
        sa.Column("failure_fingerprint", sa.String(length=64), nullable=True),
        sa.Column("prior_candidate_hash", sa.String(length=64), nullable=True),
        sa.Column("prior_review_summary", sa.Text(), nullable=True),
        sa.Column("intent_revision", sa.String(length=64), nullable=True),
        sa.Column("closed_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["task_id"], ["tasks.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["run_id"], ["runs.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["step_id"], ["steps.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(
            ["plan_revision_id"], ["plan_revisions.id"], ondelete="RESTRICT"
        ),
        sa.ForeignKeyConstraint(["parent_attempt_id"], ["work_attempts.id"], ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("step_id", "attempt_no", name="uq_work_attempt_step_number"),
        sa.CheckConstraint("attempt_no >= 1", name="work_attempt_number_positive"),
    )
    op.create_index("ix_work_attempts_task_id", "work_attempts", ["task_id"])
    op.create_index("ix_work_attempts_run_id", "work_attempts", ["run_id"])
    op.create_index("ix_work_attempts_step_id", "work_attempts", ["step_id"])
    op.create_index("ix_work_attempts_stable_item_id", "work_attempts", ["stable_item_id"])
    op.create_table(
        "task_spec_revisions",
        sa.Column("id", UUID(as_uuid=True), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False
        ),
        sa.Column("task_id", UUID(as_uuid=True), nullable=False),
        sa.Column("spec_revision", sa.String(length=64), nullable=False),
        sa.Column("spec", JSONB, nullable=False),
        sa.Column("source_turn_id", UUID(as_uuid=True), nullable=True),
        sa.ForeignKeyConstraint(["task_id"], ["tasks.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(
            ["source_turn_id"], ["conversation_turns.id"], ondelete="RESTRICT"
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("task_id", "spec_revision", name="uq_task_spec_task_revision"),
    )
    op.create_index("ix_task_spec_revisions_task_id", "task_spec_revisions", ["task_id"])
    op.create_table(
        "review_outcome_history",
        sa.Column("id", UUID(as_uuid=True), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False
        ),
        sa.Column("task_id", UUID(as_uuid=True), nullable=False),
        sa.Column("run_id", UUID(as_uuid=True), nullable=False),
        sa.Column("step_id", UUID(as_uuid=True), nullable=False),
        sa.Column("acceptance_key", sa.String(length=64), nullable=False),
        sa.Column("verdict", sa.String(length=30), nullable=False),
        sa.Column("review_kind", sa.String(length=30), nullable=True),
        sa.Column("risk", sa.String(length=20), nullable=True),
        sa.Column("base_commit", sa.String(length=64), nullable=True),
        sa.Column("result_commit", sa.String(length=64), nullable=True),
        sa.Column("diff_hash", sa.String(length=64), nullable=True),
        sa.Column("findings", JSONB, nullable=False, server_default=sa.text("'[]'::jsonb")),
        sa.Column("summary", sa.Text(), nullable=True),
        sa.Column("policy_revision", sa.String(length=64), nullable=True),
        sa.Column("partition_count", sa.Integer(), nullable=True),
        sa.Column("unknown_usage", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.ForeignKeyConstraint(["task_id"], ["tasks.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["run_id"], ["runs.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["step_id"], ["steps.id"], ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "run_id",
            "step_id",
            "acceptance_key",
            name="uq_review_outcome_run_step_key",
        ),
    )
    op.create_index("ix_review_outcome_history_task_id", "review_outcome_history", ["task_id"])
    op.create_index("ix_review_outcome_history_run_id", "review_outcome_history", ["run_id"])
    op.create_index("ix_review_outcome_history_step_id", "review_outcome_history", ["step_id"])


def downgrade() -> None:
    op.drop_table("review_outcome_history")
    op.drop_table("task_spec_revisions")
    op.drop_table("work_attempts")
    op.drop_column("work_packages", "intent_revision")
    op.drop_column("tasks", "spec_revision")
    op.drop_index("ix_tasks_source_turn_id", table_name="tasks")
    op.drop_constraint("fk_tasks_source_turn", "tasks", type_="foreignkey")
    op.drop_column("tasks", "source_turn_id")
