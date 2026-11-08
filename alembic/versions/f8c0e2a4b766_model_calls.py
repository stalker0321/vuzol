"""Model calls a service asks the executor to make on a CLI subscription account.

Revision ID: f8c0e2a4b766
Revises: e7b9d1f3a655
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "f8c0e2a4b766"  # pragma: allowlist secret - migration identifier
down_revision = "e7b9d1f3a655"  # pragma: allowlist secret - migration identifier
branch_labels = None
depends_on = None

_STEP = (
    "(leadership_turn_id IS NULL AND invocation_id IS NULL "
    "AND task_id IS NOT NULL AND run_id IS NOT NULL "
    "AND step_id IS NOT NULL AND worktree_id IS NOT NULL"
)
_TURN = (
    "(leadership_turn_id IS NOT NULL AND invocation_id IS NOT NULL "
    "AND task_id IS NULL AND run_id IS NULL AND step_id IS NULL AND worktree_id IS NULL"
)


def upgrade() -> None:
    op.create_table(
        "model_calls",
        sa.Column("id", sa.UUID(), primary_key=True),
        sa.Column("purpose", sa.String(60), nullable=False),
        sa.Column("profile_id", sa.String(100), nullable=False),
        sa.Column("request", postgresql.JSONB(), nullable=False),
        sa.Column("timeout_seconds", sa.Integer(), nullable=False),
        sa.Column("state", sa.String(12), nullable=False),
        sa.Column("lease_owner", sa.String(200)),
        sa.Column("lease_generation", sa.Integer(), nullable=False),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True)),
        sa.Column("deadline_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("output", postgresql.JSONB()),
        sa.Column("error", sa.String(200)),
        sa.Column(
            "usage", postgresql.JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")
        ),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
    )
    op.create_index(
        "ix_model_calls_claim", "model_calls", ["state", "lease_expires_at", "created_at"]
    )
    op.add_column("supervised_processes", sa.Column("model_call_id", sa.UUID(), nullable=True))
    op.create_foreign_key(
        "fk_supervised_process_model_call",
        "supervised_processes",
        "model_calls",
        ["model_call_id"],
        ["id"],
        ondelete="RESTRICT",
    )
    op.create_index(
        "ix_supervised_processes_model_call_id", "supervised_processes", ["model_call_id"]
    )
    op.drop_constraint("supervised_process_binding_xor", "supervised_processes", type_="check")
    op.create_check_constraint(
        "supervised_process_binding_xor",
        "supervised_processes",
        f"{_STEP} AND model_call_id IS NULL) OR {_TURN} AND model_call_id IS NULL) OR "
        "(model_call_id IS NOT NULL AND invocation_id IS NOT NULL "
        "AND leadership_turn_id IS NULL AND task_id IS NULL AND run_id IS NULL "
        "AND step_id IS NULL AND worktree_id IS NULL)",
    )


def downgrade() -> None:
    # Refuses while model-call processes exist: they fit neither old shape.
    op.drop_constraint("supervised_process_binding_xor", "supervised_processes", type_="check")
    op.create_check_constraint(
        "supervised_process_binding_xor", "supervised_processes", f"{_STEP}) OR {_TURN})"
    )
    op.drop_index("ix_supervised_processes_model_call_id", table_name="supervised_processes")
    op.drop_constraint(
        "fk_supervised_process_model_call", "supervised_processes", type_="foreignkey"
    )
    op.drop_column("supervised_processes", "model_call_id")
    op.drop_table("model_calls")
