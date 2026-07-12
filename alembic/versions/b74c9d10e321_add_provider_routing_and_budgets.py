"""add provider routing health and budget reservations

Revision ID: b74c9d10e321
Revises: a21b7e4d9c31
Create Date: 2026-07-12 05:30:00
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "b74c9d10e321"  # pragma: allowlist secret
down_revision: str | Sequence[str] | None = "a21b7e4d9c31"  # pragma: allowlist secret
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("routing_decisions", sa.Column("step_id", sa.UUID(), nullable=True))
    op.add_column(
        "routing_decisions",
        sa.Column("provider_attempt", sa.Integer(), server_default="1", nullable=False),
    )
    op.add_column(
        "routing_decisions",
        sa.Column("decision_kind", sa.String(length=30), server_default="initial", nullable=False),
    )
    op.create_foreign_key(
        "fk_routing_decisions_step_id_steps",
        "routing_decisions",
        "steps",
        ["step_id"],
        ["id"],
        ondelete="RESTRICT",
    )
    op.create_index("ix_routing_decisions_step_id", "routing_decisions", ["step_id"])
    op.create_index(
        "uq_routing_decisions_step_attempt",
        "routing_decisions",
        ["step_id", "provider_attempt"],
        unique=True,
        postgresql_where=sa.text("step_id IS NOT NULL"),
    )
    op.alter_column("routing_decisions", "provider_attempt", server_default=None)
    op.alter_column("routing_decisions", "decision_kind", server_default=None)

    op.add_column(
        "profile_health_observations",
        sa.Column(
            "configuration_revision", sa.String(length=64), server_default="legacy", nullable=False
        ),
    )
    op.add_column(
        "profile_health_observations",
        sa.Column("quota_state", sa.String(length=20), server_default="unknown", nullable=False),
    )
    op.add_column("profile_health_observations", sa.Column("quota_remaining", sa.Numeric(20, 6)))
    op.add_column(
        "profile_health_observations", sa.Column("last_success_at", sa.DateTime(timezone=True))
    )
    op.add_column(
        "profile_health_observations", sa.Column("last_failure_at", sa.DateTime(timezone=True))
    )
    op.create_check_constraint(
        "ck_profile_health_quota_state",
        "profile_health_observations",
        "quota_state IN ('available', 'limited', 'exhausted', 'unknown')",
    )
    op.alter_column("profile_health_observations", "configuration_revision", server_default=None)
    op.alter_column("profile_health_observations", "quota_state", server_default=None)

    op.alter_column(
        "usage_records",
        "cost_units",
        existing_type=sa.Float(),
        type_=sa.Numeric(20, 6),
        existing_nullable=True,
        postgresql_using="cost_units::numeric(20,6)",
    )
    op.alter_column(
        "usage_records",
        "quota_units",
        existing_type=sa.Float(),
        type_=sa.Numeric(20, 6),
        existing_nullable=True,
        postgresql_using="quota_units::numeric(20,6)",
    )
    op.create_table(
        "provider_budget_reservations",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("task_id", sa.UUID(), nullable=False),
        sa.Column("run_id", sa.UUID(), nullable=False),
        sa.Column("step_id", sa.UUID(), nullable=False),
        sa.Column("profile_id", sa.String(length=100), nullable=False),
        sa.Column("provider_attempt", sa.Integer(), nullable=False),
        sa.Column("reserved_input_tokens", sa.BigInteger(), nullable=False),
        sa.Column("reserved_output_tokens", sa.BigInteger(), nullable=False),
        sa.Column("reserved_cost_units", sa.Numeric(20, 6), nullable=False),
        sa.Column("reserved_quota_units", sa.Numeric(20, 6), nullable=False),
        sa.Column("reconciled_input_tokens", sa.BigInteger()),
        sa.Column("reconciled_output_tokens", sa.BigInteger()),
        sa.Column("reconciled_cost_units", sa.Numeric(20, 6)),
        sa.Column("reconciled_quota_units", sa.Numeric(20, 6)),
        sa.Column(
            "status",
            sa.Enum(
                "reserved",
                "reconciled",
                "conservative",
                "released",
                name="budget_reservation_status",
                native_enum=False,
                create_constraint=True,
            ),
            nullable=False,
        ),
        sa.Column("provider_request_id", sa.String(length=255)),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("reconciled_at", sa.DateTime(timezone=True)),
        sa.ForeignKeyConstraint(["run_id"], ["runs.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["step_id"], ["steps.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["task_id"], ["tasks.id"], ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("step_id", "provider_attempt", name="uq_budget_step_attempt"),
    )
    op.create_index(
        "ix_provider_budget_reservations_task_id",
        "provider_budget_reservations",
        ["task_id"],
    )
    op.create_index(
        "ix_provider_budget_reservations_run_id",
        "provider_budget_reservations",
        ["run_id"],
    )
    op.create_index(
        "ix_provider_budget_reservations_step_id",
        "provider_budget_reservations",
        ["step_id"],
    )
    op.create_index(
        "ix_provider_budget_reservations_profile_id",
        "provider_budget_reservations",
        ["profile_id"],
    )
    op.add_column("usage_records", sa.Column("reservation_id", sa.UUID(), nullable=True))
    op.create_foreign_key(
        "fk_usage_records_reservation_id_provider_budget_reservations",
        "usage_records",
        "provider_budget_reservations",
        ["reservation_id"],
        ["id"],
        ondelete="RESTRICT",
    )
    op.create_unique_constraint(
        "uq_usage_records_reservation_id", "usage_records", ["reservation_id"]
    )


def downgrade() -> None:
    op.drop_constraint("uq_usage_records_reservation_id", "usage_records", type_="unique")
    op.drop_constraint(
        "fk_usage_records_reservation_id_provider_budget_reservations",
        "usage_records",
        type_="foreignkey",
    )
    op.drop_column("usage_records", "reservation_id")
    op.drop_index(
        "ix_provider_budget_reservations_profile_id", table_name="provider_budget_reservations"
    )
    op.drop_index(
        "ix_provider_budget_reservations_step_id", table_name="provider_budget_reservations"
    )
    op.drop_index(
        "ix_provider_budget_reservations_run_id", table_name="provider_budget_reservations"
    )
    op.drop_index(
        "ix_provider_budget_reservations_task_id", table_name="provider_budget_reservations"
    )
    op.drop_table("provider_budget_reservations")
    op.alter_column(
        "usage_records",
        "quota_units",
        existing_type=sa.Numeric(20, 6),
        type_=sa.Float(),
        existing_nullable=True,
        postgresql_using="quota_units::double precision",
    )
    op.alter_column(
        "usage_records",
        "cost_units",
        existing_type=sa.Numeric(20, 6),
        type_=sa.Float(),
        existing_nullable=True,
        postgresql_using="cost_units::double precision",
    )
    op.drop_constraint(
        "ck_profile_health_quota_state", "profile_health_observations", type_="check"
    )
    op.drop_column("profile_health_observations", "last_failure_at")
    op.drop_column("profile_health_observations", "last_success_at")
    op.drop_column("profile_health_observations", "quota_remaining")
    op.drop_column("profile_health_observations", "quota_state")
    op.drop_column("profile_health_observations", "configuration_revision")
    op.drop_index("uq_routing_decisions_step_attempt", table_name="routing_decisions")
    op.drop_index("ix_routing_decisions_step_id", table_name="routing_decisions")
    op.drop_constraint(
        "fk_routing_decisions_step_id_steps", "routing_decisions", type_="foreignkey"
    )
    op.drop_column("routing_decisions", "decision_kind")
    op.drop_column("routing_decisions", "provider_attempt")
    op.drop_column("routing_decisions", "step_id")
