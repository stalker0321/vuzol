"""Store goal-criterion verdicts separately from plan item ids.

Additive only. A criterion id remains an address; the row is the verdict for
one description at one scope revision and one integration head.

Revision ID: f3a8c2d14e70
Revises: c2b8f1e02d31
Create Date: 2026-10-19 22:00:00
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "f3a8c2d14e70"  # pragma: allowlist secret
down_revision: str | Sequence[str] | None = "c2b8f1e02d31"  # pragma: allowlist secret
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

JSON_OBJECT = sa.text("'{}'::jsonb")


def upgrade() -> None:
    op.create_table(
        "managed_goal_evidence",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("goal_id", sa.UUID(), nullable=False),
        sa.Column("criterion_id", sa.String(length=80), nullable=False),
        sa.Column("description", sa.Text(), nullable=False),
        sa.Column("scope_revision", sa.Integer(), nullable=False),
        sa.Column("result_commit", sa.String(length=64), nullable=False),
        sa.Column("artifact_digest", sa.String(length=64), nullable=False),
        sa.Column(
            "observation",
            postgresql.JSONB(),
            server_default=JSON_OBJECT,
            nullable=False,
        ),
        sa.Column("satisfied", sa.Boolean(), nullable=False),
        sa.Column("checker", sa.String(length=40), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(["goal_id"], ["managed_goals.id"], ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "uq_managed_goal_evidence_binding",
        "managed_goal_evidence",
        ["goal_id", "criterion_id", "scope_revision", "result_commit"],
        unique=True,
    )


def downgrade() -> None:
    op.drop_index("uq_managed_goal_evidence_binding", table_name="managed_goal_evidence")
    op.drop_table("managed_goal_evidence")
