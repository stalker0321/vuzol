"""add work package message link fences

Revision ID: e5b9f3a82c16
Revises: c4d8e2f71a05
Create Date: 2026-07-31 17:12:04.590154
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "e5b9f3a82c16"  # pragma: allowlist secret
down_revision: str | Sequence[str] | None = "c4d8e2f71a05"  # pragma: allowlist secret
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("telegram_message_links", sa.Column("work_package_id", sa.UUID(), nullable=True))
    op.add_column("telegram_message_links", sa.Column("plan_revision_id", sa.UUID(), nullable=True))
    op.add_column(
        "telegram_message_links",
        sa.Column("control_status_generation", sa.Integer(), nullable=True),
    )
    op.create_check_constraint(
        "ck_telegram_message_links_telegram_message_link_package_revision_shape",
        "telegram_message_links",
        "(work_package_id IS NULL AND plan_revision_id IS NULL) OR "
        "(work_package_id IS NOT NULL AND plan_revision_id IS NOT NULL)",
    )
    op.create_check_constraint(
        "ck_telegram_message_links_telegram_message_link_control_generation_positive",
        "telegram_message_links",
        "control_status_generation IS NULL OR control_status_generation >= 1",
    )
    op.create_check_constraint(
        "ck_telegram_message_links_telegram_message_link_control_generation_target",
        "telegram_message_links",
        "control_status_generation IS NULL OR work_package_id IS NOT NULL",
    )
    op.create_index(
        op.f("ix_telegram_message_links_work_package_id"),
        "telegram_message_links",
        ["work_package_id"],
        unique=False,
    )
    op.create_foreign_key(
        op.f("fk_telegram_message_links_plan_revision_id_plan_revisions"),
        "telegram_message_links",
        "plan_revisions",
        ["plan_revision_id", "work_package_id"],
        ["id", "work_package_id"],
        ondelete="RESTRICT",
    )


def downgrade() -> None:
    op.drop_constraint(
        op.f("fk_telegram_message_links_plan_revision_id_plan_revisions"),
        "telegram_message_links",
        type_="foreignkey",
    )
    op.drop_index(
        op.f("ix_telegram_message_links_work_package_id"), table_name="telegram_message_links"
    )
    op.drop_constraint(
        "ck_telegram_message_links_telegram_message_link_control_generation_target",
        "telegram_message_links",
        type_="check",
    )
    op.drop_constraint(
        "ck_telegram_message_links_telegram_message_link_control_generation_positive",
        "telegram_message_links",
        type_="check",
    )
    op.drop_constraint(
        "ck_telegram_message_links_telegram_message_link_package_revision_shape",
        "telegram_message_links",
        type_="check",
    )
    op.drop_column("telegram_message_links", "control_status_generation")
    op.drop_column("telegram_message_links", "plan_revision_id")
    op.drop_column("telegram_message_links", "work_package_id")
