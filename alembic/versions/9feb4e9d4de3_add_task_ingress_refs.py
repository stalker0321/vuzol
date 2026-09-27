"""Add explicit task ingress refs without silent sentinels (WP09).

source_chat_id becomes nullable (CLI/API tasks carry no chat) and a new
explicit ingress_source column records the origin. Existing rows predate the
contract and are backfilled with ingress_source='legacy'. No sentinel value
(such as chat 0) means "no chat": NULL means no chat, and projections skip
Telegram explicitly on NULL.

Revision ID: 9feb4e9d4de3
Revises: c4e8f1a92b70
Create Date: 2026-09-27 00:00:00
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "9feb4e9d4de3"  # pragma: allowlist secret
down_revision: str | Sequence[str] | None = "c4e8f1a92b70"  # pragma: allowlist secret
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("tasks", sa.Column("ingress_source", sa.String(length=20), nullable=True))
    op.alter_column("tasks", "source_chat_id", existing_type=sa.BigInteger(), nullable=True)
    # Legacy provenance for rows that predate the ingress contract.
    op.execute("UPDATE tasks SET ingress_source = 'legacy' WHERE ingress_source IS NULL")


def downgrade() -> None:
    op.alter_column("tasks", "source_chat_id", existing_type=sa.BigInteger(), nullable=False)
    op.drop_column("tasks", "ingress_source")
