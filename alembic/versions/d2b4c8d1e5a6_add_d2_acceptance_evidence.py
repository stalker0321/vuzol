"""Add D2 acceptance evidence/waiver substrate (additive only).

New tables only: `acceptance_evidence` (unique within
(package_id, evidence_hash)) and `acceptance_waivers` (unique within
(package_id, integration_head)). No renames, no backfill, no changes to
existing rows (Q4 compat: old approvals read as before).

Revision ID: d2b4c8d1e5a6
Revises: d1a7b3c9e2f5
Create Date: 2026-09-30
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB, UUID

revision: str = "d2b4c8d1e5a6"  # pragma: allowlist secret
down_revision: str | Sequence[str] | None = "d1a7b3c9e2f5"  # pragma: allowlist secret
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "acceptance_evidence",
        sa.Column("id", UUID(as_uuid=True), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False
        ),
        sa.Column("package_id", UUID(as_uuid=True), nullable=False),
        sa.Column("plan_revision_id", UUID(as_uuid=True), nullable=True),
        sa.Column("evidence_hash", sa.String(length=64), nullable=False),
        sa.Column("integration_base_head", sa.String(length=64), nullable=True),
        sa.Column("result_commit", sa.String(length=64), nullable=True),
        sa.Column("artifact_id", UUID(as_uuid=True), nullable=True),
        sa.Column("evidence", JSONB, nullable=False),
        sa.ForeignKeyConstraint(["package_id"], ["work_packages.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(
            ["plan_revision_id"], ["plan_revisions.id"], ondelete="RESTRICT"
        ),
        sa.ForeignKeyConstraint(["artifact_id"], ["artifacts.id"], ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "package_id", "evidence_hash", name="uq_acceptance_evidence_package_hash"
        ),
    )
    op.create_index(
        "ix_acceptance_evidence_package_id", "acceptance_evidence", ["package_id"]
    )
    op.create_table(
        "acceptance_waivers",
        sa.Column("id", UUID(as_uuid=True), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False
        ),
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
    op.create_index(
        "ix_acceptance_waivers_package_id", "acceptance_waivers", ["package_id"]
    )


def downgrade() -> None:
    op.drop_table("acceptance_waivers")
    op.drop_table("acceptance_evidence")
