"""Equivalent names for existing project-lead grants; no schema changes."""

from alembic import op

revision = "f4b8c2d91a06"
down_revision = "e8b5a61fa505"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Keep old names for in-flight receipts/clients. Every alias is derived from
    # a present grant; neither missing privileges nor revoked grants are added.
    op.execute("""
        WITH aliases(old_name, new_name) AS (VALUES
            ('start_batch', 'start_task'), ('propose_batch', 'propose_plan'),
            ('request_revision', 'amend_goal'), ('request_permission', 'ask_owner'),
            ('get_project_state', 'read_status'), ('cancel_goal', 'cancel_task'),
            ('apply_integration', 'accept_result')
        ), changes AS (
            SELECT d.id, jsonb_agg(a.new_name ORDER BY a.new_name) AS added
            FROM goal_delegations d CROSS JOIN aliases a
            WHERE d.revoked_at IS NULL AND d.allowed_actions ? a.old_name
                AND NOT d.allowed_actions ? a.new_name
            GROUP BY d.id
        )
        UPDATE goal_delegations d
        SET allowed_actions = d.allowed_actions || c.added, version = d.version + 1
        FROM changes c WHERE c.id = d.id
    """)


def downgrade() -> None:
    # No lossy removal: an alias may also have been explicitly granted later.
    # Older binaries ignore unknown names and still have the original grants.
    pass
