"""Retain each lead read round and index existing decision details."""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB, TSVECTOR

revision = "f2c8a4e6b901"  # pragma: allowlist secret
down_revision = "e1b7c9d4a6f2"  # pragma: allowlist secret
branch_labels = None
depends_on = None


def _search(expression: str) -> None:
    op.drop_index("ix_memory_units_text_search", table_name="memory_units")
    op.drop_column("memory_units", "text_search")
    # PostgreSQL regenerates every existing row, including old decision entries.
    op.add_column(
        "memory_units",
        sa.Column("text_search", TSVECTOR, sa.Computed(expression, persisted=True), nullable=False),
    )
    op.create_index(
        "ix_memory_units_text_search", "memory_units", ["text_search"], postgresql_using="gin"
    )


def upgrade() -> None:
    op.add_column(
        "leadership_inferences",
        sa.Column("tool_audit", JSONB, nullable=False, server_default=sa.text("'[]'::jsonb")),
    )
    op.add_column(
        "leadership_inferences",
        sa.Column("round", sa.Integer(), nullable=False, server_default="0"),
    )
    op.drop_index("uq_leadership_inference_phase", table_name="leadership_inferences")
    op.create_index(
        "uq_leadership_inference_phase",
        "leadership_inferences",
        ["turn_id", "phase", "round"],
        unique=True,
    )
    op.create_check_constraint(
        "leadership_inference_round_nonnegative", "leadership_inferences", "round >= 0"
    )
    _search(
        "to_tsvector('simple', text || CASE WHEN unit_type = 'decision_entry' "
        "THEN ' ' || coalesce(details::text, '') ELSE '' END)"
    )


def downgrade() -> None:
    if (
        op.get_bind()
        .execute(
            sa.text(
                'SELECT count(*) FROM leadership_inferences WHERE "round" > 0 '
                "OR jsonb_array_length(tool_audit) > 0"
            )
        )
        .scalar_one()
    ):
        raise RuntimeError("archive tool audits and additional inference rounds before downgrade")
    _search("to_tsvector('simple', text)")
    op.drop_constraint(
        "leadership_inference_round_nonnegative", "leadership_inferences", type_="check"
    )
    op.drop_index("uq_leadership_inference_phase", table_name="leadership_inferences")
    op.create_index(
        "uq_leadership_inference_phase", "leadership_inferences", ["turn_id", "phase"], unique=True
    )
    op.drop_column("leadership_inferences", "round")
    op.drop_column("leadership_inferences", "tool_audit")
