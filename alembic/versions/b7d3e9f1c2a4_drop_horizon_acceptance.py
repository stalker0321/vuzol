"""Drop the retired horizon acceptance substrate.

Nothing wrote package goals, exit criteria, acceptance evidence or waivers since
the lead took over goals; managed goals carry their own criteria. The upgrade
refuses to run if any of the dropped columns or tables still holds data.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB, UUID

revision: str = "b7d3e9f1c2a4"  # pragma: allowlist secret
down_revision: str | Sequence[str] | None = "a6c9e2107b32"  # pragma: allowlist secret
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_PACKAGE_COLUMNS = (
    "goal",
    "goal_revision",
    "exit_criteria",
    "lifetime_budget",
    "deadline",
    "permission_envelope_hash",
    "owner",
    "horizon_phase",
    "acceptance_artifact_id",
    "accepted_at",
)


def _created_at() -> sa.Column:
    return sa.Column(
        "created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False
    )


def upgrade() -> None:
    bind = op.get_bind()
    filled = " OR ".join(f"{column} IS NOT NULL" for column in _PACKAGE_COLUMNS)
    checks = (
        f"SELECT count(*) FROM work_packages WHERE {filled}",  # noqa: S608
        "SELECT count(*) FROM acceptance_evidence",
        "SELECT count(*) FROM acceptance_waivers",
        "SELECT count(*) FROM memory_units WHERE source_acceptance_evidence_id IS NOT NULL",
    )
    for check in checks:
        if bind.execute(sa.text(check)).scalar_one():
            raise RuntimeError(f"horizon acceptance data still present: {check}")
    op.drop_column("memory_units", "source_acceptance_evidence_id")
    op.drop_table("acceptance_waivers")
    op.drop_table("acceptance_evidence")
    op.drop_constraint(
        "fk_work_packages_acceptance_artifact_id_artifacts",
        "work_packages",
        type_="foreignkey",
    )
    for column in _PACKAGE_COLUMNS:
        op.drop_column("work_packages", column)


def downgrade() -> None:
    op.add_column("work_packages", sa.Column("goal", sa.Text(), nullable=True))
    op.add_column("work_packages", sa.Column("goal_revision", sa.Integer(), nullable=True))
    op.add_column("work_packages", sa.Column("exit_criteria", JSONB(), nullable=True))
    op.add_column("work_packages", sa.Column("lifetime_budget", JSONB(), nullable=True))
    op.add_column("work_packages", sa.Column("deadline", sa.DateTime(timezone=True), nullable=True))
    op.add_column(
        "work_packages", sa.Column("permission_envelope_hash", sa.String(length=64), nullable=True)
    )
    op.add_column("work_packages", sa.Column("owner", sa.String(length=100), nullable=True))
    op.add_column("work_packages", sa.Column("horizon_phase", sa.String(length=30), nullable=True))
    op.add_column("work_packages", sa.Column("acceptance_artifact_id", sa.UUID(), nullable=True))
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
    op.create_table(
        "acceptance_evidence",
        sa.Column("id", UUID(as_uuid=True), nullable=False),
        _created_at(),
        sa.Column("package_id", UUID(as_uuid=True), nullable=False),
        sa.Column("plan_revision_id", UUID(as_uuid=True), nullable=True),
        sa.Column("evidence_hash", sa.String(length=64), nullable=False),
        sa.Column("integration_base_head", sa.String(length=64), nullable=True),
        sa.Column("result_commit", sa.String(length=64), nullable=True),
        sa.Column("artifact_id", UUID(as_uuid=True), nullable=True),
        sa.Column("evidence", JSONB, nullable=False),
        sa.ForeignKeyConstraint(["package_id"], ["work_packages.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["plan_revision_id"], ["plan_revisions.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["artifact_id"], ["artifacts.id"], ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "package_id", "evidence_hash", name="uq_acceptance_evidence_package_hash"
        ),
    )
    op.create_index("ix_acceptance_evidence_package_id", "acceptance_evidence", ["package_id"])
    op.create_table(
        "acceptance_waivers",
        sa.Column("id", UUID(as_uuid=True), nullable=False),
        _created_at(),
        sa.Column("package_id", UUID(as_uuid=True), nullable=False),
        sa.Column("integration_head", sa.String(length=64), nullable=False),
        sa.Column("principal_user_id", sa.BigInteger(), nullable=False),
        sa.Column("reason", sa.Text(), nullable=False),
        sa.ForeignKeyConstraint(["package_id"], ["work_packages.id"], ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "package_id", "integration_head", name="uq_acceptance_waiver_package_head"
        ),
    )
    op.create_index("ix_acceptance_waivers_package_id", "acceptance_waivers", ["package_id"])
    op.add_column(
        "memory_units",
        sa.Column("source_acceptance_evidence_id", UUID(as_uuid=True), nullable=True),
    )
    op.create_foreign_key(
        None,
        "memory_units",
        "acceptance_evidence",
        ["source_acceptance_evidence_id"],
        ["id"],
    )
