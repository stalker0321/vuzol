"""Add structured project-lead decision entries."""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision: str = "e1b7c9d4a6f2"  # pragma: allowlist secret
down_revision: str | Sequence[str] | None = "d9a2f6b4c8e1"  # pragma: allowlist secret
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("memory_units", sa.Column("details", JSONB, nullable=True))
    op.drop_constraint("memory_unit_type_bounded", "memory_units", type_="check")
    op.create_check_constraint(
        "memory_unit_type_bounded",
        "memory_units",
        "unit_type IN ('decision_template', 'outcome_template', 'observation', 'lesson', "
        "'notebook_note', 'decision_entry')",
    )
    op.create_check_constraint(
        "memory_unit_decision_scope",
        "memory_units",
        "unit_type <> 'decision_entry' OR scope = 'project'",
    )


def downgrade() -> None:
    if (
        op.get_bind()
        .execute(sa.text("SELECT count(*) FROM memory_units WHERE unit_type = 'decision_entry'"))
        .scalar_one()
    ):
        raise RuntimeError("decision entries must be archived before downgrade")
    op.drop_constraint("memory_unit_decision_scope", "memory_units", type_="check")
    op.drop_constraint("memory_unit_type_bounded", "memory_units", type_="check")
    op.create_check_constraint(
        "memory_unit_type_bounded",
        "memory_units",
        "unit_type IN ('decision_template', 'outcome_template', 'observation', 'lesson', "
        "'notebook_note')",
    )
    op.drop_column("memory_units", "details")
