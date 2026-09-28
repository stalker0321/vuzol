"""Add second-node registry and device slots (WP12).

New tables only (additive): `nodes` (one control plane, local + one remote)
and `node_slots` (exclusive device leases with generation fencing).
Pre-registry deployments are backfilled with the `local` node row
(provenance `legacy`), so single-node behavior is unchanged.

Revision ID: 2e137b138124
Revises: 9feb4e9d4de3
Create Date: 2026-09-28 02:43:30.623448
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "2e137b138124"  # pragma: allowlist secret
down_revision: str | Sequence[str] | None = "9feb4e9d4de3"  # pragma: allowlist secret
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "nodes",
        sa.Column("id", sa.UUID(), nullable=False),
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
        sa.Column("node_id", sa.String(length=100), nullable=False),
        sa.Column("trust_class", sa.String(length=20), nullable=False),
        sa.Column("status", sa.String(length=20), server_default="offline", nullable=False),
        sa.Column("protocol_version", sa.String(length=50), nullable=False),
        sa.Column("credential_ref", sa.String(length=100), nullable=True),
        sa.Column("last_heartbeat_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("detail", sa.String(length=500), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("node_id", name="uq_node_node_id"),
    )
    op.create_index("ix_nodes_node_id", "nodes", ["node_id"])
    op.create_table(
        "node_slots",
        sa.Column("id", sa.UUID(), nullable=False),
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
        sa.Column("node_id", sa.String(length=100), nullable=False),
        sa.Column("slot_name", sa.String(length=100), nullable=False),
        sa.Column("claimed_by", sa.String(length=200), nullable=True),
        sa.Column("claim_generation", sa.Integer(), server_default="0", nullable=False),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("claimed_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["node_id"], ["nodes.node_id"], ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("node_id", "slot_name", name="uq_node_slot"),
    )
    op.create_index("ix_node_slots_node_id", "node_slots", ["node_id"])
    # Backfill: deployments that predate the registry ran on the local node.
    op.execute(
        "INSERT INTO nodes (id, node_id, trust_class, status, protocol_version, detail) "
        "VALUES (gen_random_uuid(), 'local', 'local', 'online', 'node-protocol.v1', 'legacy')"
    )


def downgrade() -> None:
    op.drop_index("ix_node_slots_node_id", table_name="node_slots")
    op.drop_table("node_slots")
    op.drop_index("ix_nodes_node_id", table_name="nodes")
    op.drop_table("nodes")
