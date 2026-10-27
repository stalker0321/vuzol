"""Drop node slots and configuration revisions; nothing writes either table.

The upgrade refuses to run if either table holds rows.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "c8e4f2a7d1b9"  # pragma: allowlist secret
down_revision: str | Sequence[str] | None = "b7d3e9f1c2a4"  # pragma: allowlist secret
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _now(name: str) -> sa.Column:
    return sa.Column(
        name, sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False
    )


def upgrade() -> None:
    bind = op.get_bind()
    for table in ("node_slots", "configuration_revisions"):
        if bind.execute(sa.text(f"SELECT count(*) FROM {table}")).scalar_one():  # noqa: S608
            raise RuntimeError(f"{table} still holds rows")
    op.drop_index("ix_node_slots_node_id", table_name="node_slots")
    op.drop_table("node_slots")
    op.drop_table("configuration_revisions")


def downgrade() -> None:
    op.create_table(
        "configuration_revisions",
        sa.Column("revision", sa.String(length=64), nullable=False),
        sa.Column("kind", sa.String(length=50), nullable=False),
        sa.Column("normalized_content", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("artifact_reference", sa.String(length=500), nullable=True),
        _now("created_at"),
        sa.Column("id", sa.UUID(), nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_configuration_revisions")),
        sa.UniqueConstraint("revision", name=op.f("uq_configuration_revisions_revision")),
    )
    op.create_table(
        "node_slots",
        sa.Column("id", sa.UUID(), nullable=False),
        _now("created_at"),
        _now("updated_at"),
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
