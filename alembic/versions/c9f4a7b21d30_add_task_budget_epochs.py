"""add task budget epochs

Revision ID: c9f4a7b21d30
Revises: b8e2c4d61f90
Create Date: 2026-08-15 19:10:00
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "c9f4a7b21d30"  # pragma: allowlist secret
down_revision: str | Sequence[str] | None = "b8e2c4d61f90"  # pragma: allowlist secret
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "tasks",
        sa.Column("budget_epoch", sa.Integer(), server_default="0", nullable=False),
    )
    op.add_column(
        "provider_budget_reservations",
        sa.Column("budget_epoch", sa.Integer(), server_default="0", nullable=False),
    )
    op.create_check_constraint("tasks_budget_epoch_nonnegative", "tasks", "budget_epoch >= 0")
    op.create_check_constraint(
        "provider_budget_reservations_epoch_nonnegative",
        "provider_budget_reservations",
        "budget_epoch >= 0",
    )
    op.create_index(
        "ix_provider_budget_reservations_task_epoch",
        "provider_budget_reservations",
        ["task_id", "budget_epoch"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_provider_budget_reservations_task_epoch",
        table_name="provider_budget_reservations",
    )
    op.drop_constraint(
        "provider_budget_reservations_epoch_nonnegative",
        "provider_budget_reservations",
        type_="check",
    )
    op.drop_constraint("tasks_budget_epoch_nonnegative", "tasks", type_="check")
    op.drop_column("provider_budget_reservations", "budget_epoch")
    op.drop_column("tasks", "budget_epoch")
