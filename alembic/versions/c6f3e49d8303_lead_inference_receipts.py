"""Pin lead role assignments and retain per-phase provider receipts.

Revision ID: c6f3e49d8303
Revises: b5e2d38c7202
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "c6f3e49d8303"  # pragma: allowlist secret
down_revision = "b5e2d38c7202"  # pragma: allowlist secret
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("leadership_turns", sa.Column("model_assignments", postgresql.JSONB()))
    op.create_table(
        "leadership_inferences",
        sa.Column("id", sa.UUID(), primary_key=True),
        sa.Column("turn_id", sa.UUID(), nullable=False),
        sa.Column("phase", sa.String(20), nullable=False),
        sa.Column("profile_id", sa.String(100), nullable=False),
        sa.Column("profile_revision", sa.String(64), nullable=False),
        sa.Column("input_hash", sa.String(64), nullable=False),
        sa.Column("request", postgresql.JSONB(), nullable=False),
        sa.Column("state", sa.String(20), nullable=False),
        sa.Column("result", postgresql.JSONB()),
        sa.Column("reservation_id", sa.UUID(), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.ForeignKeyConstraint(["turn_id"], ["leadership_turns.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(
            ["reservation_id"], ["provider_budget_reservations.id"], ondelete="RESTRICT"
        ),
        sa.CheckConstraint("phase IN ('lead', 'goal_review')", name="leadership_inference_phase"),
        sa.CheckConstraint(
            "state IN ('sending', 'received', 'unknown', 'not_sent')",
            name="leadership_inference_state",
        ),
    )
    op.create_index(
        "uq_leadership_inference_phase", "leadership_inferences", ["turn_id", "phase"], unique=True
    )


def downgrade() -> None:
    op.drop_table("leadership_inferences")
    op.drop_column("leadership_turns", "model_assignments")
