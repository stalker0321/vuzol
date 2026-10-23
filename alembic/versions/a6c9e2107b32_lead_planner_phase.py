"""Strong planner uses the existing leadership inference receipt boundary."""

from alembic import op

revision = "a6c9e2107b32"
down_revision = "f4b8c2d91a06"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_constraint("leadership_inference_phase", "leadership_inferences", type_="check")
    op.create_check_constraint(
        "leadership_inference_phase",
        "leadership_inferences",
        "phase IN ('lead', 'goal_review', 'planner')",
    )


def downgrade() -> None:
    # Retain paid receipts; downgrading requires there be no planner history.
    op.drop_constraint("leadership_inference_phase", "leadership_inferences", type_="check")
    op.create_check_constraint(
        "leadership_inference_phase",
        "leadership_inferences",
        "phase IN ('lead', 'goal_review')",
    )
