"""Offers the assistant makes from the owner's mail, acted on with a button.

Revision ID: d4e8f1a2b3c5
Revises: c7a9e1b3d544
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "d4e8f1a2b3c5"  # pragma: allowlist secret
down_revision = "c7a9e1b3d544"  # pragma: allowlist secret
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("assistant_entries", sa.Column("offers", postgresql.JSONB(), nullable=True))


def downgrade() -> None:
    op.drop_column("assistant_entries", "offers")
