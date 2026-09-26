"""Add the durable effect intent/receipt ledger.

Additive only: one new table. Existing runs have no effect rows and are read
through their existing Worktree/Approval state; no historical effect is
fabricated (legacy provenance). `Step.unknown_effects` is intentionally kept.

Revision ID: e7d2c4a91b06
Revises: b3f7a2c91e04
Create Date: 2026-09-26 00:00:00
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision: str = "e7d2c4a91b06"  # pragma: allowlist secret
down_revision: str | Sequence[str] | None = "b3f7a2c91e04"  # pragma: allowlist secret
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "effects",
        sa.Column(
            "schema_version", sa.String(length=50), server_default="effect.v1", nullable=False
        ),
        sa.Column("operation_key", sa.String(length=255), nullable=False),
        sa.Column("step_id", sa.UUID(), nullable=False),
        sa.Column("task_id", sa.UUID(), nullable=True),
        sa.Column("run_id", sa.UUID(), nullable=True),
        sa.Column("horizon_id", sa.UUID(), nullable=True),
        sa.Column("attempt_id", sa.UUID(), nullable=True),
        sa.Column("effect_class", sa.String(length=30), nullable=False),
        sa.Column("target_kind", sa.String(length=30), nullable=False),
        sa.Column("target_reference", sa.String(length=500), nullable=False),
        sa.Column("idempotency", sa.String(length=30), nullable=False),
        sa.Column("permission_envelope_hash", sa.String(length=64), nullable=True),
        sa.Column("approval_id", sa.UUID(), nullable=True),
        sa.Column("approval_envelope_hash", sa.String(length=64), nullable=True),
        sa.Column("payload_hash", sa.String(length=64), nullable=False),
        sa.Column("lease_generation", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(length=20), server_default="intent_recorded", nullable=False),
        sa.Column("launch_started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("launch_dispatch_token", sa.String(length=255), nullable=True),
        sa.Column("launch_generation", sa.Integer(), nullable=True),
        sa.Column("receipt_status", sa.String(length=20), nullable=True),
        sa.Column("receipt_observed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("receipt_external_ref", sa.String(length=500), nullable=True),
        sa.Column("receipt_output_hash", sa.String(length=64), nullable=True),
        sa.Column(
            "reconcile_status", sa.String(length=20), server_default="not_started", nullable=False
        ),
        sa.Column("reconcile_reconciled_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("reconcile_method", sa.String(length=100), nullable=True),
        sa.Column("context", JSONB(), server_default=sa.text("'{}'::jsonb"), nullable=False),
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
        sa.Column("id", sa.UUID(), nullable=False),
        sa.ForeignKeyConstraint(
            ["step_id"], ["steps.id"], name="fk_effects_step_id_steps", ondelete="RESTRICT"
        ),
        sa.ForeignKeyConstraint(
            ["task_id"], ["tasks.id"], name="fk_effects_task_id_tasks", ondelete="RESTRICT"
        ),
        sa.ForeignKeyConstraint(
            ["run_id"], ["runs.id"], name="fk_effects_run_id_runs", ondelete="RESTRICT"
        ),
        sa.ForeignKeyConstraint(
            ["approval_id"],
            ["approvals.id"],
            name="fk_effects_approval_id_approvals",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_effects"),
        sa.UniqueConstraint("operation_key", name="uq_effects_operation_key"),
    )
    op.create_index("ix_effects_step_id", "effects", ["step_id"])
    op.create_index("ix_effects_status", "effects", ["status"])
    # No row backfill: the table is new. Existing runs carry no effect intent and
    # are reconciled through their Worktree/Approval state (legacy), never by
    # fabricating a historical effect.


def downgrade() -> None:
    op.drop_index("ix_effects_status", table_name="effects")
    op.drop_index("ix_effects_step_id", table_name="effects")
    op.drop_table("effects")
