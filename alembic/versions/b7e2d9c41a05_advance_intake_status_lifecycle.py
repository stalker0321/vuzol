"""Advance intake status lifecycle with a completed state.

Revision ID: b7e2d9c41a05
Revises: d9e1f4a7c203
"""

from collections.abc import Sequence

from alembic import op

revision: str = "b7e2d9c41a05"  # pragma: allowlist secret
down_revision: str | Sequence[str] | None = "d9e1f4a7c203"  # pragma: allowlist secret
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.drop_constraint(
        "intake_status",
        "telegram_intake_messages",
        type_="check",
    )
    op.create_check_constraint(
        "intake_status",
        "telegram_intake_messages",
        "status IN ('received', 'awaiting_interpretation', 'needs_clarification', "
        "'completed', 'rejected')",
    )


def downgrade() -> None:
    op.execute(
        "UPDATE telegram_intake_messages SET status='awaiting_interpretation' "
        "WHERE status='completed'"
    )
    op.drop_constraint(
        "intake_status",
        "telegram_intake_messages",
        type_="check",
    )
    op.create_check_constraint(
        "intake_status",
        "telegram_intake_messages",
        "status IN ('received', 'awaiting_interpretation', 'needs_clarification', 'rejected')",
    )
