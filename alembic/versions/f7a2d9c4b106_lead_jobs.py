"""Persist read-only research jobs independently of work packages.

Revision ID: f7a2d9c4b106
Revises: f2c8a4e6b901
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "f7a2d9c4b106"
down_revision = "f2c8a4e6b901"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "lead_jobs",
        sa.Column("id", sa.UUID(), primary_key=True),
        sa.Column("goal_id", sa.UUID(), nullable=False),
        sa.Column("kind", sa.String(20), nullable=False),
        sa.Column("input", postgresql.JSONB(), nullable=False),
        sa.Column("pinned_commit", sa.String(64), nullable=False),
        sa.Column("scope_revision", sa.Integer(), nullable=False),
        sa.Column("state", sa.String(20), nullable=False, server_default="queued"),
        sa.Column("lease_owner", sa.String(200)),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True)),
        sa.Column("lease_generation", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("result_text", sa.Text()),
        sa.Column("failure_reason", sa.String(512)),
        sa.Column(
            "usage", postgresql.JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")
        ),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column("started_at", sa.DateTime(timezone=True)),
        sa.Column("finished_at", sa.DateTime(timezone=True)),
        sa.ForeignKeyConstraint(["goal_id"], ["managed_goals.id"], ondelete="RESTRICT"),
        sa.CheckConstraint("kind = 'research'", name="lead_job_kind"),
        sa.CheckConstraint(
            "state IN ('queued', 'running', 'succeeded', 'failed', 'cancelled')",
            name="lead_job_state",
        ),
        sa.CheckConstraint(
            "jsonb_typeof(input) = 'object' AND input ? 'question' "
            "AND jsonb_typeof(input->'question') = 'string' "
            "AND char_length(btrim(input->>'question')) BETWEEN 1 AND 2000",
            name="lead_job_input",
        ),
        sa.CheckConstraint("scope_revision >= 1", name="lead_job_scope_revision"),
        sa.CheckConstraint("lease_generation >= 0", name="lead_job_lease_generation"),
        sa.CheckConstraint("octet_length(result_text) <= 16384", name="lead_job_result_bytes"),
        sa.CheckConstraint("jsonb_typeof(usage) = 'object'", name="lead_job_usage"),
        sa.CheckConstraint(
            "(state = 'running' AND lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL) "
            "OR (state <> 'running' AND lease_owner IS NULL AND lease_expires_at IS NULL)",
            name="lead_job_lease",
        ),
        sa.CheckConstraint(
            "(state IN ('succeeded', 'failed', 'cancelled')) = (finished_at IS NOT NULL)",
            name="lead_job_finished",
        ),
    )
    op.create_index("ix_lead_jobs_goal_id", "lead_jobs", ["goal_id"])
    op.create_index(
        "ix_lead_jobs_queued",
        "lead_jobs",
        ["created_at", "id"],
        postgresql_where=sa.text("state = 'queued'"),
    )
    op.create_index(
        "ix_lead_jobs_running_expiry",
        "lead_jobs",
        ["lease_expires_at"],
        postgresql_where=sa.text("state = 'running'"),
    )


def downgrade() -> None:
    op.drop_table("lead_jobs")
