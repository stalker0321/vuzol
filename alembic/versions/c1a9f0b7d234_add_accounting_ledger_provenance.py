"""Add accounting provenance to the usage and reservation ledger.

Additive only: purpose/attempt_kind (orthogonal), nullable horizon scope
reference, pricing_revision/currency provenance, explicit cost_known and
late_receipt markers. Legacy rows are backfilled with pricing_revision='legacy'
and cost_known=false; purpose/attempt_kind/currency stay NULL rather than being
fabricated. No table is renamed and no existing column is altered.

Revision ID: c1a9f0b7d234
Revises: b7e2d9c41a05
Create Date: 2026-09-26 00:00:00
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "c1a9f0b7d234"  # pragma: allowlist secret
down_revision: str | Sequence[str] | None = "b7e2d9c41a05"  # pragma: allowlist secret
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("usage_records", sa.Column("purpose", sa.String(length=30), nullable=True))
    op.add_column(
        "usage_records", sa.Column("attempt_kind", sa.String(length=20), nullable=True)
    )
    op.add_column("usage_records", sa.Column("horizon_id", sa.UUID(), nullable=True))
    op.add_column(
        "usage_records", sa.Column("pricing_revision", sa.String(length=100), nullable=True)
    )
    op.add_column("usage_records", sa.Column("currency", sa.String(length=16), nullable=True))
    op.add_column(
        "usage_records",
        sa.Column("cost_known", sa.Boolean(), server_default=sa.text("false"), nullable=False),
    )
    op.add_column(
        "usage_records",
        sa.Column("late_receipt", sa.Boolean(), server_default=sa.text("false"), nullable=False),
    )
    op.create_index("ix_usage_records_horizon_id", "usage_records", ["horizon_id"])

    op.add_column(
        "provider_budget_reservations",
        sa.Column("purpose", sa.String(length=30), nullable=True),
    )
    op.add_column(
        "provider_budget_reservations",
        sa.Column("attempt_kind", sa.String(length=20), nullable=True),
    )
    op.add_column(
        "provider_budget_reservations", sa.Column("horizon_id", sa.UUID(), nullable=True)
    )
    op.add_column(
        "provider_budget_reservations",
        sa.Column("pricing_revision", sa.String(length=100), nullable=True),
    )
    op.add_column(
        "provider_budget_reservations",
        sa.Column("currency", sa.String(length=16), nullable=True),
    )
    op.create_index(
        "ix_provider_budget_reservations_status",
        "provider_budget_reservations",
        ["status"],
    )
    op.create_index(
        "ix_provider_budget_reservations_horizon_id",
        "provider_budget_reservations",
        ["horizon_id"],
    )

    # Honest provenance for pre-existing rows: they were written before pricing
    # revisions existed, so they are marked legacy and explicitly unknown.
    op.execute(
        "UPDATE usage_records SET pricing_revision = 'legacy' WHERE pricing_revision IS NULL"
    )
    op.execute(
        "UPDATE provider_budget_reservations SET pricing_revision = 'legacy' "
        "WHERE pricing_revision IS NULL"
    )


def downgrade() -> None:
    op.drop_index(
        "ix_provider_budget_reservations_horizon_id",
        table_name="provider_budget_reservations",
    )
    op.drop_index(
        "ix_provider_budget_reservations_status",
        table_name="provider_budget_reservations",
    )
    op.drop_column("provider_budget_reservations", "currency")
    op.drop_column("provider_budget_reservations", "pricing_revision")
    op.drop_column("provider_budget_reservations", "horizon_id")
    op.drop_column("provider_budget_reservations", "attempt_kind")
    op.drop_column("provider_budget_reservations", "purpose")

    op.drop_index("ix_usage_records_horizon_id", table_name="usage_records")
    op.drop_column("usage_records", "late_receipt")
    op.drop_column("usage_records", "cost_known")
    op.drop_column("usage_records", "currency")
    op.drop_column("usage_records", "pricing_revision")
    op.drop_column("usage_records", "horizon_id")
    op.drop_column("usage_records", "attempt_kind")
    op.drop_column("usage_records", "purpose")
