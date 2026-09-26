"""Add verified capability installations and run version pins.

Additive only: two new tables. Existing runs have no installation rows and no
pins; they read the on-disk toolchain receipt exactly as before (legacy
behavior, no fabricated history). Permissions and approvals are untouched.

Revision ID: a1c5e7b93d20
Revises: e7d2c4a91b06
Create Date: 2026-09-26 00:00:00
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "a1c5e7b93d20"  # pragma: allowlist secret
down_revision: str | Sequence[str] | None = "e7d2c4a91b06"  # pragma: allowlist secret
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "capability_installations",
        sa.Column("capability_key", sa.String(length=64), nullable=False),
        sa.Column("version", sa.String(length=100), nullable=False),
        sa.Column("status", sa.String(length=20), server_default="unknown", nullable=False),
        sa.Column("receipt_hash", sa.String(length=64), nullable=True),
        sa.Column("environment_hash", sa.String(length=64), nullable=True),
        sa.Column("installation_root", sa.String(length=1000), nullable=False),
        sa.Column("node_id", sa.String(length=100), server_default="local", nullable=False),
        sa.Column("probe_status", sa.String(length=20), server_default="unknown", nullable=False),
        sa.Column("probed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("health_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column("detail", sa.String(length=500), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("id", sa.UUID(), nullable=False),
        sa.PrimaryKeyConstraint("id", name="pk_capability_installations"),
        sa.UniqueConstraint(
            "capability_key", "node_id", name="uq_capability_installation_node"
        ),
    )
    op.create_index(
        "ix_capability_installations_capability_key",
        "capability_installations",
        ["capability_key"],
    )
    op.create_index("ix_capability_installations_status", "capability_installations", ["status"])

    op.create_table(
        "capability_run_pins",
        sa.Column("run_id", sa.UUID(), nullable=False),
        sa.Column("capability_key", sa.String(length=64), nullable=False),
        sa.Column("version", sa.String(length=100), nullable=False),
        sa.Column("receipt_hash", sa.String(length=64), nullable=False),
        sa.Column(
            "pinned_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("id", sa.UUID(), nullable=False),
        sa.ForeignKeyConstraint(
            ["run_id"], ["runs.id"], name="fk_capability_run_pins_run_id_runs", ondelete="RESTRICT"
        ),
        sa.PrimaryKeyConstraint("id", name="pk_capability_run_pins"),
        sa.UniqueConstraint("run_id", "capability_key", name="uq_capability_run_pin"),
    )
    op.create_index("ix_capability_run_pins_run_id", "capability_run_pins", ["run_id"])
    # No row backfill: the tables are new and historical installs/pins must not
    # be fabricated. Legacy runs keep reading the on-disk receipt.


def downgrade() -> None:
    op.drop_index("ix_capability_run_pins_run_id", table_name="capability_run_pins")
    op.drop_table("capability_run_pins")
    op.drop_index("ix_capability_installations_status", table_name="capability_installations")
    op.drop_index(
        "ix_capability_installations_capability_key", table_name="capability_installations"
    )
    op.drop_table("capability_installations")
