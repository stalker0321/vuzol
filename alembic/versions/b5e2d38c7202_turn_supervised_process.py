"""Bind supervised inference processes to leadership turns without fake tasks."""

import sqlalchemy as sa
from alembic import op

revision = "b5e2d38c7202"  # pragma: allowlist secret - migration identifier
down_revision = "a4d9e27b6101"  # pragma: allowlist secret - migration identifier
branch_labels = None
depends_on = None


def upgrade() -> None:
    for column in ("task_id", "run_id", "step_id", "worktree_id"):
        op.alter_column("supervised_processes", column, existing_type=sa.UUID(), nullable=True)
    op.add_column("supervised_processes", sa.Column("leadership_turn_id", sa.UUID(), nullable=True))
    op.add_column("supervised_processes", sa.Column("invocation_id", sa.UUID(), nullable=True))
    op.create_foreign_key(
        "fk_supervised_process_turn",
        "supervised_processes",
        "leadership_turns",
        ["leadership_turn_id"],
        ["id"],
        ondelete="RESTRICT",
    )
    op.create_index(
        "ix_supervised_processes_leadership_turn_id", "supervised_processes", ["leadership_turn_id"]
    )
    op.create_index(
        "uq_supervised_process_invocation",
        "supervised_processes",
        ["invocation_id"],
        unique=True,
        postgresql_where=sa.text("invocation_id IS NOT NULL"),
    )
    op.create_check_constraint(
        "supervised_process_binding_xor",
        "supervised_processes",
        "(leadership_turn_id IS NULL AND invocation_id IS NULL "
        "AND task_id IS NOT NULL AND run_id IS NOT NULL "
        "AND step_id IS NOT NULL AND worktree_id IS NOT NULL) OR "
        "(leadership_turn_id IS NOT NULL AND invocation_id IS NOT NULL "
        "AND task_id IS NULL AND run_id IS NULL AND step_id IS NULL AND worktree_id IS NULL)",
    )


def downgrade() -> None:
    # Restoring NOT NULL deliberately refuses a downgrade with turn history.
    for column in ("task_id", "run_id", "step_id", "worktree_id"):
        op.alter_column("supervised_processes", column, existing_type=sa.UUID(), nullable=False)
    op.drop_constraint("supervised_process_binding_xor", "supervised_processes", type_="check")
    op.drop_index("uq_supervised_process_invocation", table_name="supervised_processes")
    op.drop_index("ix_supervised_processes_leadership_turn_id", table_name="supervised_processes")
    op.drop_constraint("fk_supervised_process_turn", "supervised_processes", type_="foreignkey")
    op.drop_column("supervised_processes", "invocation_id")
    op.drop_column("supervised_processes", "leadership_turn_id")
