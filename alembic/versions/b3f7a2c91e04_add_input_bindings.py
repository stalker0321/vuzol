"""Add persisted input bindings for versioned context manifests.

Additive only: one new table. Existing runs have no rows and keep their legacy
context behavior. New producer steps may resolve predecessor artifacts into a
consumer request through an explicit, hash-pinned binding.

Revision ID: b3f7a2c91e04
Revises: c1a9f0b7d234
Create Date: 2026-09-26 00:00:00
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "b3f7a2c91e04"  # pragma: allowlist secret
down_revision: str | Sequence[str] | None = "c1a9f0b7d234"  # pragma: allowlist secret
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "input_bindings",
        sa.Column("consumer_step_id", sa.UUID(), nullable=False),
        sa.Column("producer_step_id", sa.UUID(), nullable=True),
        sa.Column("artifact_id", sa.UUID(), nullable=True),
        sa.Column(
            "slot", sa.String(length=100), server_default="predecessor_result", nullable=False
        ),
        sa.Column("schema_name", sa.String(length=100), nullable=False),
        sa.Column("schema_version", sa.String(length=100), nullable=False),
        sa.Column("content_hash", sa.String(length=64), nullable=True),
        sa.Column("scope_project_id", sa.String(length=100), nullable=True),
        sa.Column("access_scope", sa.String(length=100), server_default="private", nullable=False),
        sa.Column("required", sa.Boolean(), server_default=sa.text("true"), nullable=False),
        sa.Column("status", sa.String(length=20), server_default="pending", nullable=False),
        sa.Column("freshness_max_age_seconds", sa.Integer(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("resolved_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("id", sa.UUID(), nullable=False),
        sa.ForeignKeyConstraint(
            ["consumer_step_id"],
            ["steps.id"],
            name="fk_input_bindings_consumer_step_id_steps",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["producer_step_id"],
            ["steps.id"],
            name="fk_input_bindings_producer_step_id_steps",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["artifact_id"],
            ["artifacts.id"],
            name="fk_input_bindings_artifact_id_artifacts",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_input_bindings"),
        sa.UniqueConstraint("consumer_step_id", "slot", name="uq_input_binding_consumer_slot"),
    )
    op.create_index(
        "ix_input_bindings_consumer_step_id", "input_bindings", ["consumer_step_id"]
    )
    op.create_index(
        "ix_input_bindings_producer_step_id", "input_bindings", ["producer_step_id"]
    )


def downgrade() -> None:
    op.drop_index("ix_input_bindings_producer_step_id", table_name="input_bindings")
    op.drop_index("ix_input_bindings_consumer_step_id", table_name="input_bindings")
    op.drop_table("input_bindings")
