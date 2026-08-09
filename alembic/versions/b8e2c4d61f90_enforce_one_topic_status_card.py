"""enforce one work-package status card per Telegram topic

Revision ID: b8e2c4d61f90
Revises: a7c4e92d1f30
Create Date: 2026-08-09 23:58:00
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "b8e2c4d61f90"  # pragma: allowlist secret
down_revision: str | Sequence[str] | None = "a7c4e92d1f30"  # pragma: allowlist secret
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_index(
        "uq_telegram_topic_work_package_status",
        "telegram_message_links",
        ["chat_id", "message_thread_id"],
        unique=True,
        postgresql_where=sa.text("message_role = 'work_package_status'"),
    )


def downgrade() -> None:
    op.drop_index(
        "uq_telegram_topic_work_package_status",
        table_name="telegram_message_links",
    )
