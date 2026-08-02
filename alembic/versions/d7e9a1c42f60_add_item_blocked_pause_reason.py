"""add item blocked work package pause reason

Revision ID: d7e9a1c42f60
Revises: e5b9f3a82c16
Create Date: 2026-08-02 23:00:00.000000
"""

from collections.abc import Sequence

from alembic import op

revision: str = "d7e9a1c42f60"  # pragma: allowlist secret
down_revision: str | Sequence[str] | None = "e5b9f3a82c16"  # pragma: allowlist secret
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.drop_constraint("work_package_pause_reason", "work_packages", type_="check")
    op.create_check_constraint(
        "work_package_pause_reason",
        "work_packages",
        "pause_reason IN ('user','item_failed','item_blocked','replan_required','policy')",
    )


def downgrade() -> None:
    op.execute(
        "UPDATE work_packages SET pause_reason = 'item_failed' WHERE pause_reason = 'item_blocked'"
    )
    op.drop_constraint("work_package_pause_reason", "work_packages", type_="check")
    op.create_check_constraint(
        "work_package_pause_reason",
        "work_packages",
        "pause_reason IN ('user','item_failed','replan_required','policy')",
    )
