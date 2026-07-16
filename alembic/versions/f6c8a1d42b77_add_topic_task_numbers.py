"""add topic task numbers

Revision ID: f6c8a1d42b77
Revises: e4b7c2d91a60
Create Date: 2026-07-16 10:15:00
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "f6c8a1d42b77"  # pragma: allowlist secret
down_revision: str | Sequence[str] | None = "e4b7c2d91a60"  # pragma: allowlist secret
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("tasks", sa.Column("topic_task_number", sa.Integer(), nullable=True))
    op.add_column("tasks", sa.Column("public_task_number", sa.BigInteger(), nullable=True))
    op.execute(
        """
        WITH numbered AS (
            SELECT id,
                   row_number() OVER (
                       PARTITION BY source_chat_id, source_thread_id
                       ORDER BY created_at, id
                   ) AS topic_number
            FROM tasks
            WHERE source_thread_id IS NOT NULL
        )
        UPDATE tasks AS task
        SET topic_task_number = numbered.topic_number,
            public_task_number = task.source_thread_id * 10000 + numbered.topic_number
        FROM numbered
        WHERE task.id = numbered.id
        """
    )
    op.create_check_constraint(
        "ck_tasks_topic_task_number_range",
        "tasks",
        "topic_task_number IS NULL OR topic_task_number BETWEEN 1 AND 9999",
    )
    op.create_unique_constraint(
        "uq_task_topic_number",
        "tasks",
        ["source_chat_id", "source_thread_id", "topic_task_number"],
    )
    op.create_unique_constraint(
        "uq_task_public_number",
        "tasks",
        ["source_chat_id", "public_task_number"],
    )
    op.create_table(
        "topic_task_counters",
        sa.Column("chat_id", sa.BigInteger(), nullable=False),
        sa.Column("message_thread_id", sa.BigInteger(), nullable=False),
        sa.Column("last_number", sa.Integer(), nullable=False),
        sa.Column("id", sa.UUID(), nullable=False),
        sa.CheckConstraint(
            "last_number BETWEEN 1 AND 9999",
            name="ck_topic_task_counters_last_number_range",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("chat_id", "message_thread_id", name="uq_topic_task_counter"),
    )
    op.execute(
        """
        INSERT INTO topic_task_counters (
            id, chat_id, message_thread_id, last_number
        )
        SELECT gen_random_uuid(), source_chat_id, source_thread_id, max(topic_task_number)
        FROM tasks
        WHERE source_thread_id IS NOT NULL
        GROUP BY source_chat_id, source_thread_id
        """
    )


def downgrade() -> None:
    op.drop_table("topic_task_counters")
    op.drop_constraint("uq_task_public_number", "tasks", type_="unique")
    op.drop_constraint("uq_task_topic_number", "tasks", type_="unique")
    op.drop_constraint("ck_tasks_topic_task_number_range", "tasks", type_="check")
    op.drop_column("tasks", "public_task_number")
    op.drop_column("tasks", "topic_task_number")
