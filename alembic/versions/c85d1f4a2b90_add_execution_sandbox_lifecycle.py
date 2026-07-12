"""add execution sandbox lifecycle

Revision ID: c85d1f4a2b90
Revises: b74c9d10e321
Create Date: 2026-07-12 15:30:00
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "c85d1f4a2b90"  # pragma: allowlist secret
down_revision: str | Sequence[str] | None = "b74c9d10e321"  # pragma: allowlist secret
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _enum(name: str, *values: str) -> sa.Enum:
    return sa.Enum(*values, name=name, native_enum=False, create_constraint=True)


def upgrade() -> None:
    op.add_column(
        "artifacts",
        sa.Column(
            "storage_state",
            _enum(
                "artifact_storage_state",
                "staging",
                "available",
                "quarantined",
                "missing",
                "deleted",
            ),
            server_default="available",
            nullable=False,
        ),
    )
    op.add_column("artifacts", sa.Column("storage_key", sa.String(500)))
    op.add_column("artifacts", sa.Column("producer_process_id", sa.UUID()))
    op.add_column("artifacts", sa.Column("redaction_revision", sa.String(64)))
    op.add_column("artifacts", sa.Column("verified_at", sa.DateTime(timezone=True)))
    op.create_unique_constraint("uq_artifacts_storage_key", "artifacts", ["storage_key"])

    op.add_column("worktrees", sa.Column("source_remote_hash", sa.String(64)))
    op.add_column(
        "worktrees",
        sa.Column(
            "repository_identity_hash", sa.String(64), server_default="legacy", nullable=False
        ),
    )
    op.add_column(
        "worktrees",
        sa.Column("default_branch", sa.String(255), server_default="main", nullable=False),
    )
    op.add_column(
        "worktrees",
        sa.Column("expected_target_head", sa.String(64), server_default="legacy", nullable=False),
    )
    op.add_column(
        "worktrees",
        sa.Column("lifecycle_generation", sa.Integer(), server_default="1", nullable=False),
    )
    op.add_column("worktrees", sa.Column("diff_hash", sa.String(64)))
    op.add_column("worktrees", sa.Column("changed_files_artifact_id", sa.UUID()))
    op.add_column("worktrees", sa.Column("patch_artifact_id", sa.UUID()))
    op.add_column(
        "worktrees",
        sa.Column(
            "retention_until",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
    )
    op.add_column("worktrees", sa.Column("last_inspected_at", sa.DateTime(timezone=True)))
    op.add_column("worktrees", sa.Column("cleanup_reason", sa.String(100)))
    op.add_column("worktrees", sa.Column("delivery_operation_hash", sa.String(64)))
    op.add_column("worktrees", sa.Column("delivered_remote", sa.String(1000)))
    op.add_column("worktrees", sa.Column("delivered_ref", sa.String(500)))
    op.create_unique_constraint("uq_worktrees_run_id", "worktrees", ["run_id"])
    op.create_unique_constraint(
        "uq_worktree_project_branch",
        "worktrees",
        ["project_id", "repository_identity_hash", "branch"],
    )
    op.create_foreign_key(
        "fk_worktrees_changed_files_artifact_id",
        "worktrees",
        "artifacts",
        ["changed_files_artifact_id"],
        ["id"],
        ondelete="RESTRICT",
    )
    op.create_foreign_key(
        "fk_worktrees_patch_artifact_id",
        "worktrees",
        "artifacts",
        ["patch_artifact_id"],
        ["id"],
        ondelete="RESTRICT",
    )
    for column in (
        "repository_identity_hash",
        "default_branch",
        "expected_target_head",
        "lifecycle_generation",
        "retention_until",
    ):
        op.alter_column("worktrees", column, server_default=None)

    additions = (
        sa.Column("task_id", sa.UUID(), nullable=False),
        sa.Column("run_id", sa.UUID(), nullable=False),
        sa.Column("worktree_id", sa.UUID(), nullable=False),
        sa.Column("profile_id", sa.String(100), nullable=False),
        sa.Column("lease_generation", sa.Integer(), nullable=False),
        sa.Column("provider_attempt", sa.Integer(), nullable=False),
        sa.Column("idempotency_key", sa.String(64), nullable=False),
        sa.Column(
            "command_envelope",
            postgresql.JSONB(),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column("sandbox_spec_hash", sa.String(64), nullable=False),
        sa.Column("container_runtime", sa.String(50), nullable=False),
        sa.Column("image_digest", sa.String(255), nullable=False),
        sa.Column(
            "outcome",
            _enum(
                "process_outcome",
                "succeeded",
                "failed",
                "timed_out",
                "cancelled",
                "resource_exhausted",
                "unknown",
            ),
        ),
        sa.Column(
            "termination_stage",
            _enum("termination_stage", "none", "interrupt", "terminate", "kill"),
            server_default="none",
            nullable=False,
        ),
        sa.Column("timed_out", sa.Boolean(), server_default=sa.false(), nullable=False),
        sa.Column("resource_limited", sa.Boolean(), server_default=sa.false(), nullable=False),
        sa.Column("provider_events_artifact_id", sa.UUID()),
        sa.Column("provider_result_artifact_id", sa.UUID()),
        sa.Column("heartbeat_at", sa.DateTime(timezone=True)),
        sa.Column("cancellation_requested_at", sa.DateTime(timezone=True)),
        sa.Column("termination_started_at", sa.DateTime(timezone=True)),
        sa.Column("reaped_at", sa.DateTime(timezone=True)),
        sa.Column(
            "runtime_metadata",
            postgresql.JSONB(),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
    )
    for column in additions:
        op.add_column("supervised_processes", column)
    op.create_unique_constraint(
        "uq_supervised_processes_idempotency_key", "supervised_processes", ["idempotency_key"]
    )
    for column, table in (("task_id", "tasks"), ("run_id", "runs"), ("worktree_id", "worktrees")):
        op.create_foreign_key(
            f"fk_supervised_processes_{column}",
            "supervised_processes",
            table,
            [column],
            ["id"],
            ondelete="RESTRICT",
        )
        op.create_index(f"ix_supervised_processes_{column}", "supervised_processes", [column])
    for column in ("provider_events_artifact_id", "provider_result_artifact_id"):
        op.create_foreign_key(
            f"fk_supervised_processes_{column}",
            "supervised_processes",
            "artifacts",
            [column],
            ["id"],
            ondelete="RESTRICT",
        )
    op.create_foreign_key(
        "fk_artifacts_producer_process_id",
        "artifacts",
        "supervised_processes",
        ["producer_process_id"],
        ["id"],
        ondelete="RESTRICT",
        use_alter=True,
    )


def downgrade() -> None:
    op.drop_constraint("fk_artifacts_producer_process_id", "artifacts", type_="foreignkey")
    for column in ("provider_result_artifact_id", "provider_events_artifact_id"):
        op.drop_constraint(
            f"fk_supervised_processes_{column}", "supervised_processes", type_="foreignkey"
        )
    for column in ("worktree_id", "run_id", "task_id"):
        op.drop_index(f"ix_supervised_processes_{column}", table_name="supervised_processes")
        op.drop_constraint(
            f"fk_supervised_processes_{column}", "supervised_processes", type_="foreignkey"
        )
    op.drop_constraint(
        "uq_supervised_processes_idempotency_key", "supervised_processes", type_="unique"
    )
    for column in (
        "runtime_metadata",
        "reaped_at",
        "termination_started_at",
        "cancellation_requested_at",
        "heartbeat_at",
        "provider_result_artifact_id",
        "provider_events_artifact_id",
        "resource_limited",
        "timed_out",
        "termination_stage",
        "outcome",
        "image_digest",
        "container_runtime",
        "sandbox_spec_hash",
        "command_envelope",
        "idempotency_key",
        "provider_attempt",
        "lease_generation",
        "profile_id",
        "worktree_id",
        "run_id",
        "task_id",
    ):
        op.drop_column("supervised_processes", column)
    op.drop_constraint("fk_worktrees_patch_artifact_id", "worktrees", type_="foreignkey")
    op.drop_constraint("fk_worktrees_changed_files_artifact_id", "worktrees", type_="foreignkey")
    op.drop_constraint("uq_worktree_project_branch", "worktrees", type_="unique")
    op.drop_constraint("uq_worktrees_run_id", "worktrees", type_="unique")
    for column in (
        "delivered_ref",
        "delivered_remote",
        "delivery_operation_hash",
        "cleanup_reason",
        "last_inspected_at",
        "retention_until",
        "patch_artifact_id",
        "changed_files_artifact_id",
        "diff_hash",
        "lifecycle_generation",
        "expected_target_head",
        "default_branch",
        "repository_identity_hash",
        "source_remote_hash",
    ):
        op.drop_column("worktrees", column)
    op.drop_constraint("uq_artifacts_storage_key", "artifacts", type_="unique")
    for column in (
        "verified_at",
        "redaction_revision",
        "producer_process_id",
        "storage_key",
        "storage_state",
    ):
        op.drop_column("artifacts", column)
