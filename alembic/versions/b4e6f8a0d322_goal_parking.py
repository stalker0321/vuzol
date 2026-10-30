"""A parked goal is set aside, not live, so another goal can run.

Revision ID: b4e6f8a0d322
Revises: a3d5e7f9c211
"""

from alembic import op

revision = "b4e6f8a0d322"
down_revision = "a3d5e7f9c211"
branch_labels = None
depends_on = None

_NAME = "managed_goal_lifecycle"


def upgrade() -> None:
    op.drop_constraint(_NAME, "managed_goals", type_="check")
    op.create_check_constraint(
        _NAME,
        "managed_goals",
        "lifecycle IN ('draft','active','waiting','paused','parked','completed','cancelled')",
    )


def downgrade() -> None:
    op.execute("UPDATE managed_goals SET lifecycle = 'paused' WHERE lifecycle = 'parked'")
    op.drop_constraint(_NAME, "managed_goals", type_="check")
    op.create_check_constraint(
        _NAME,
        "managed_goals",
        "lifecycle IN ('draft','active','waiting','paused','completed','cancelled')",
    )
