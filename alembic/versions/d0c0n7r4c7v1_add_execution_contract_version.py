"""Add pinned execution contract version (D0, additive only).

Nullable/additive columns only; no table renames, no backfill of historical
semantics (NULL = pre-D0 row, read through legacy behaviour).

Revision ID: d0c0n7r4c7v1
Revises: 2e137b138124
Create Date: 2026-09-30
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "d0c0n7r4c7v1"  # pragma: allowlist secret
down_revision: str | Sequence[str] | None = "2e137b138124"  # pragma: allowlist secret
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("runs", sa.Column("execution_contract_version", sa.String(length=64), nullable=True))
    op.add_column(
        "work_packages", sa.Column("execution_contract_version", sa.String(length=64), nullable=True)
    )


def downgrade() -> None:
    op.drop_column("work_packages", "execution_contract_version")
    op.drop_column("runs", "execution_contract_version")
