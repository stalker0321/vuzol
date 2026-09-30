"""Add D3 context/budget substrate (additive + nullable only).

- PlanRevisionItem: work_kind/capability/effect_intent/item_contract_version.
- provider_budget_reservations: step-less nullable refs (task/run/step),
  invocation_id (+ partial unique), review allowance columns.
- usage_records: invocation_id.
- input_bindings: source_retrieved_at freshness anchor.
No renames, no backfill (NULL = legacy provenance, never fabricated).

Revision ID: d3c1a8f2e4b7
Revises: d2b4c8d1e5a6
Create Date: 2026-09-30
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import UUID

revision: str = "d3c1a8f2e4b7"  # pragma: allowlist secret
down_revision: str | Sequence[str] | None = "d2b4c8d1e5a6"  # pragma: allowlist secret
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("plan_revision_items", sa.Column("work_kind", sa.String(length=30), nullable=True))
    op.add_column("plan_revision_items", sa.Column("capability", sa.String(length=100), nullable=True))
    op.add_column(
        "plan_revision_items", sa.Column("effect_intent", sa.String(length=30), nullable=True)
    )
    op.add_column(
        "plan_revision_items",
        sa.Column("item_contract_version", sa.String(length=64), nullable=True),
    )
    op.alter_column("provider_budget_reservations", "task_id", existing_type=UUID(as_uuid=True), nullable=True)
    op.alter_column("provider_budget_reservations", "run_id", existing_type=UUID(as_uuid=True), nullable=True)
    op.alter_column("provider_budget_reservations", "step_id", existing_type=UUID(as_uuid=True), nullable=True)
    op.add_column(
        "provider_budget_reservations",
        sa.Column("invocation_id", UUID(as_uuid=True), nullable=True),
    )
    op.create_index(
        "ix_provider_budget_reservations_invocation_id",
        "provider_budget_reservations",
        ["invocation_id"],
    )
    op.execute(
        "CREATE UNIQUE INDEX uq_budget_invocation "
        "ON provider_budget_reservations (invocation_id) "
        "WHERE invocation_id IS NOT NULL"
    )
    op.add_column(
        "provider_budget_reservations",
        sa.Column(
            "allowance_input_tokens",
            sa.BigInteger(),
            nullable=False,
            server_default="0",
        ),
    )
    op.add_column(
        "provider_budget_reservations",
        sa.Column(
            "allowance_output_tokens",
            sa.BigInteger(),
            nullable=False,
            server_default="0",
        ),
    )
    op.add_column(
        "provider_budget_reservations",
        sa.Column(
            "allowance_cost_units",
            sa.Numeric(20, 6),
            nullable=False,
            server_default="0",
        ),
    )
    op.add_column(
        "usage_records", sa.Column("invocation_id", UUID(as_uuid=True), nullable=True)
    )
    op.create_index("ix_usage_records_invocation_id", "usage_records", ["invocation_id"])
    op.add_column(
        "input_bindings",
        sa.Column("source_retrieved_at", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("input_bindings", "source_retrieved_at")
    op.drop_index("ix_usage_records_invocation_id", table_name="usage_records")
    op.drop_column("usage_records", "invocation_id")
    op.drop_column("provider_budget_reservations", "allowance_cost_units")
    op.drop_column("provider_budget_reservations", "allowance_output_tokens")
    op.drop_column("provider_budget_reservations", "allowance_input_tokens")
    op.execute("DROP INDEX IF EXISTS uq_budget_invocation")
    op.drop_index(
        "ix_provider_budget_reservations_invocation_id",
        table_name="provider_budget_reservations",
    )
    op.drop_column("provider_budget_reservations", "invocation_id")
    op.alter_column("provider_budget_reservations", "step_id", existing_type=UUID(as_uuid=True), nullable=False)
    op.alter_column("provider_budget_reservations", "run_id", existing_type=UUID(as_uuid=True), nullable=False)
    op.alter_column("provider_budget_reservations", "task_id", existing_type=UUID(as_uuid=True), nullable=False)
    op.drop_column("plan_revision_items", "item_contract_version")
    op.drop_column("plan_revision_items", "effect_intent")
    op.drop_column("plan_revision_items", "capability")
    op.drop_column("plan_revision_items", "work_kind")
