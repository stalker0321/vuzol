"""add projection revisions

Revision ID: cf3ae0c222db
Revises: 7cdc769ba80a
Create Date: 2026-07-11 00:37:41.122159
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "cf3ae0c222db"  # pragma: allowlist secret
down_revision: str | Sequence[str] | None = "7cdc769ba80a"  # pragma: allowlist secret
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "telegram_message_links",
        sa.Column("projection_revision", sa.Integer(), server_default="0", nullable=False),
    )


def downgrade() -> None:
    op.drop_column("telegram_message_links", "projection_revision")
