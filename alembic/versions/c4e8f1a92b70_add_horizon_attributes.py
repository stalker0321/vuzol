"""Add optional horizon contract attributes to work packages.

Additive/nullable only: the SQL table is not renamed and no existing column is
changed (ADR-A01: Horizon evolves WorkPackage). Existing packages are marked
with owner='legacy' and keep their current lifecycle; horizon behaviour is
opt-in behind a flag.

Revision ID: c4e8f1a92b70
Revises: a1c5e7b93d20
Create Date: 2026-09-26 00:00:00
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision: str = "c4e8f1a92b70"  # pragma: allowlist secret
down_revision: str | Sequence[str] | None = "a1c5e7b93d20"  # pragma: allowlist secret
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("work_packages", sa.Column("goal", sa.Text(), nullable=True))
    op.add_column("work_packages", sa.Column("goal_revision", sa.Integer(), nullable=True))
    op.add_column("work_packages", sa.Column("exit_criteria", JSONB(), nullable=True))
    op.add_column("work_packages", sa.Column("lifetime_budget", JSONB(), nullable=True))
    op.add_column(
        "work_packages", sa.Column("deadline", sa.DateTime(timezone=True), nullable=True)
    )
    op.add_column(
        "work_packages", sa.Column("permission_envelope_hash", sa.String(length=64), nullable=True)
    )
    op.add_column("work_packages", sa.Column("owner", sa.String(length=100), nullable=True))
    op.add_column("work_packages", sa.Column("horizon_phase", sa.String(length=30), nullable=True))
    op.add_column(
        "work_packages", sa.Column("acceptance_artifact_id", sa.UUID(), nullable=True)
    )
    op.add_column(
        "work_packages", sa.Column("accepted_at", sa.DateTime(timezone=True), nullable=True)
    )
    op.create_foreign_key(
        "fk_work_packages_acceptance_artifact_id_artifacts",
        "work_packages",
        "artifacts",
        ["acceptance_artifact_id"],
        ["id"],
        ondelete="RESTRICT",
    )
    # Legacy provenance for rows that predate the horizon contract.
    op.execute("UPDATE work_packages SET owner = 'legacy' WHERE owner IS NULL")


def downgrade() -> None:
    op.drop_constraint(
        "fk_work_packages_acceptance_artifact_id_artifacts",
        "work_packages",
        type_="foreignkey",
    )
    op.drop_column("work_packages", "accepted_at")
    op.drop_column("work_packages", "acceptance_artifact_id")
    op.drop_column("work_packages", "horizon_phase")
    op.drop_column("work_packages", "owner")
    op.drop_column("work_packages", "permission_envelope_hash")
    op.drop_column("work_packages", "deadline")
    op.drop_column("work_packages", "lifetime_budget")
    op.drop_column("work_packages", "exit_criteria")
    op.drop_column("work_packages", "goal_revision")
    op.drop_column("work_packages", "goal")
