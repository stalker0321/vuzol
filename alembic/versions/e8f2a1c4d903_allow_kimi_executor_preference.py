"""Allow Kimi as a project executor preference.

Revision ID: e8f2a1c4d903
Revises: d7e9a1c42f60
"""

from alembic import op

revision = "e8f2a1c4d903"
down_revision = "d7e9a1c42f60"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_constraint(
        "project_executor_preference_worker",
        "project_executor_preferences",
        type_="check",
    )
    op.create_check_constraint(
        "project_executor_preference_worker",
        "project_executor_preferences",
        "worker_key IS NULL OR worker_key IN ('sol', 'terra', 'luna', 'grok', 'kimi')",
    )


def downgrade() -> None:
    op.execute(
        "UPDATE project_executor_preferences SET mode='auto', worker_key=NULL, "
        "reasoning_effort=NULL WHERE worker_key='kimi'"
    )
    op.drop_constraint(
        "project_executor_preference_worker",
        "project_executor_preferences",
        type_="check",
    )
    op.create_check_constraint(
        "project_executor_preference_worker",
        "project_executor_preferences",
        "worker_key IS NULL OR worker_key IN ('sol', 'terra', 'luna', 'grok')",
    )
