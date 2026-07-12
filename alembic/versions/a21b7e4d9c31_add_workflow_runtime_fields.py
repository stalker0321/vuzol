"""add workflow runtime fields

Revision ID: a21b7e4d9c31
Revises: cf3ae0c222db
Create Date: 2026-07-11 22:15:00
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "a21b7e4d9c31"  # pragma: allowlist secret
down_revision: str | Sequence[str] | None = "cf3ae0c222db"  # pragma: allowlist secret
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.drop_constraint("run_status", "runs", type_="check")
    op.alter_column(
        "runs",
        "status",
        existing_type=sa.String(length=9),
        type_=sa.String(length=20),
        existing_nullable=False,
    )
    op.create_check_constraint(
        "run_status",
        "runs",
        "status IN ('created', 'running', 'awaiting_user', 'paused', 'blocked', "
        "'failed', 'cancelled', 'completed')",
    )
    op.drop_constraint("step_status", "steps", type_="check")
    op.create_check_constraint(
        "step_status",
        "steps",
        "status IN ('pending', 'queued', 'leased', 'running', 'waiting_approval', "
        "'awaiting_user', 'blocked', 'failed', 'cancelled', 'completed')",
    )
    op.add_column("runs", sa.Column("source_interpretation_id", sa.UUID(), nullable=True))
    op.create_foreign_key(
        "fk_runs_source_interpretation_id_interpretations",
        "runs",
        "interpretations",
        ["source_interpretation_id"],
        ["id"],
        ondelete="RESTRICT",
    )
    op.create_unique_constraint(
        "uq_runs_source_interpretation_id",
        "runs",
        ["source_interpretation_id"],
    )
    op.add_column(
        "steps",
        sa.Column("queue_class", sa.String(length=20), server_default="light", nullable=False),
    )
    op.create_check_constraint(
        "ck_steps_queue_class",
        "steps",
        "queue_class IN ('control', 'light', 'heavy', 'privileged')",
    )
    op.alter_column("steps", "queue_class", server_default=None)
    op.create_check_constraint(
        "ck_steps_positive_limits",
        "steps",
        "max_attempts > 0 AND timeout_seconds > 0 AND attempt_count >= 0",
    )
    op.drop_index("ix_steps_queue", table_name="steps")
    op.create_index(
        "ix_steps_queue",
        "steps",
        ["queue_class", "priority", "available_at", "created_at"],
        postgresql_where=sa.text("status = 'queued'"),
    )


def downgrade() -> None:
    op.drop_index("ix_steps_queue", table_name="steps")
    op.create_index(
        "ix_steps_queue",
        "steps",
        ["priority", "available_at", "created_at"],
        postgresql_where=sa.text("status = 'queued'"),
    )
    op.drop_constraint("ck_steps_positive_limits", "steps", type_="check")
    op.drop_constraint("ck_steps_queue_class", "steps", type_="check")
    op.drop_column("steps", "queue_class")
    op.drop_constraint("uq_runs_source_interpretation_id", "runs", type_="unique")
    op.drop_constraint(
        "fk_runs_source_interpretation_id_interpretations", "runs", type_="foreignkey"
    )
    op.drop_column("runs", "source_interpretation_id")
    op.drop_constraint("step_status", "steps", type_="check")
    op.execute("UPDATE steps SET status = 'blocked' WHERE status = 'awaiting_user'")
    op.create_check_constraint(
        "step_status",
        "steps",
        "status IN ('pending', 'queued', 'leased', 'running', 'waiting_approval', "
        "'blocked', 'failed', 'cancelled', 'completed')",
    )
    op.drop_constraint("run_status", "runs", type_="check")
    op.execute("UPDATE runs SET status = 'blocked' WHERE status = 'awaiting_user'")
    op.alter_column(
        "runs",
        "status",
        existing_type=sa.String(length=20),
        type_=sa.String(length=9),
        existing_nullable=False,
    )
    op.create_check_constraint(
        "run_status",
        "runs",
        "status IN ('created', 'running', 'paused', 'blocked', 'failed', 'cancelled', 'completed')",
    )
