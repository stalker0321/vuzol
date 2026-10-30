"""Owner work slang for speech-to-text, curated by the lead.

Revision ID: a3d5e7f9c211
Revises: f7a2d9c4b106
"""

import sqlalchemy as sa
from alembic import op

revision = "a3d5e7f9c211"
down_revision = "f7a2d9c4b106"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "transcription_terms",
        sa.Column("id", sa.UUID(), primary_key=True),
        sa.Column("term", sa.String(100), nullable=False),
        sa.Column("added_by_user_id", sa.BigInteger()),
        sa.Column("goal_id", sa.UUID()),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column("removed_at", sa.DateTime(timezone=True)),
        sa.CheckConstraint(
            "char_length(btrim(term)) BETWEEN 1 AND 100 AND term !~ '[<>\\n\\r]'",
            name="transcription_term_text",
        ),
    )
    op.create_index(
        "uq_transcription_terms_active",
        "transcription_terms",
        [sa.text("lower(term)")],
        unique=True,
        postgresql_where=sa.text("removed_at IS NULL"),
    )


def downgrade() -> None:
    op.drop_index("uq_transcription_terms_active", table_name="transcription_terms")
    op.drop_table("transcription_terms")
