"""Add explicit memory scopes and project-lead notebook notes."""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "d9a2f6b4c8e1"  # pragma: allowlist secret
down_revision: str | Sequence[str] | None = "c8e4f2a7d1b9"  # pragma: allowlist secret
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("memory_units", sa.Column("scope", sa.String(30), nullable=True))
    op.execute(
        "UPDATE memory_units SET scope = CASE WHEN project_id IS NOT NULL "
        "THEN 'project' ELSE 'session' END"
    )
    op.alter_column("memory_units", "scope", nullable=False)
    op.add_column("memory_units", sa.Column("goal_id", sa.UUID(), nullable=True))
    op.create_foreign_key(
        "fk_memory_units_goal_id_managed_goals",
        "memory_units",
        "managed_goals",
        ["goal_id"],
        ["id"],
        ondelete="RESTRICT",
    )
    op.drop_constraint("memory_unit_type_bounded", "memory_units", type_="check")
    op.create_check_constraint(
        "memory_unit_type_bounded",
        "memory_units",
        "unit_type IN ('decision_template', 'outcome_template', 'observation', 'lesson', "
        "'notebook_note')",
    )
    op.create_check_constraint(
        "memory_scope",
        "memory_units",
        "scope IN ('session', 'project', 'global')",
    )
    op.create_check_constraint(
        "memory_unit_scope_project",
        "memory_units",
        "(scope = 'project' AND project_id IS NOT NULL) OR "
        "(scope IN ('session', 'global') AND project_id IS NULL)",
    )
    op.create_check_constraint(
        "memory_unit_notebook_bounded",
        "memory_units",
        "unit_type <> 'notebook_note' OR "
        "(scope IN ('project', 'global') AND char_length(text) <= 600)",
    )


def downgrade() -> None:
    # Preserve notebook history rather than silently delete notes on downgrade.
    if (
        op.get_bind()
        .execute(sa.text("SELECT count(*) FROM memory_units WHERE unit_type = 'notebook_note'"))
        .scalar_one()
    ):
        raise RuntimeError("notebook notes must be archived before downgrade")
    for name in ("memory_unit_notebook_bounded", "memory_unit_scope_project", "memory_scope"):
        op.drop_constraint(name, "memory_units", type_="check")
    op.drop_constraint("memory_unit_type_bounded", "memory_units", type_="check")
    op.create_check_constraint(
        "memory_unit_type_bounded",
        "memory_units",
        "unit_type IN ('decision_template', 'outcome_template', 'observation', 'lesson')",
    )
    op.drop_column("memory_units", "goal_id")
    op.drop_column("memory_units", "scope")
