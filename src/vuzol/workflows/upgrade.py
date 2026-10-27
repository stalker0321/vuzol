"""Read-only release guard for handlers needed by persisted, resumable runs."""

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from vuzol.storage.models import Run, Step, WorkPackage
from vuzol.storage.types import RunStatus, StepStatus, WorkPackageStatus

# The union of consumers shipped in this release, independent of local process
# roles/configuration. Keep in sync with service composition (covered by tests).
SUPPORTED_STEP_TYPES = frozenset(
    {
        "finalize",
        "prepare_context",
        "prepare_worktree",
        "ensure_dependencies",
        "validate",
        "produce_artifacts",
        "build_static",
        "publish_preview",
        "publish_static",
        "execute_code",
        "execute_agent",
        "research_execute",
        "synthesize",
        "deliver_discussion_reply",
        "review",
        "approval",
        "ensure_capabilities",
        "acceptance",
    }
)


async def check_stored_handlers(
    session: AsyncSession,
    *,
    supported: frozenset[str] = SUPPORTED_STEP_TYPES,
) -> None:
    """Failed/blocked steps remain resumable; only completed/cancelled are settled."""
    missing = (
        await session.execute(
            select(Run.id, Step.id, Step.step_type)
            .join(Step, Step.run_id == Run.id)
            .where(
                Run.status.not_in((RunStatus.COMPLETED, RunStatus.CANCELLED, RunStatus.FAILED)),
                Step.status.not_in((StepStatus.COMPLETED, StepStatus.CANCELLED)),
                Step.step_type.not_in(supported),
            )
            .order_by(Run.id, Step.ordinal)
            .limit(20)
        )
    ).all()
    if missing:
        details = "; ".join(f"run={run} step={step} handler={kind}" for run, step, kind in missing)
        raise RuntimeError(
            f"workflow upgrade refused: pending steps need removed handlers: {details}"
        )
    # No such packages exist in the supplied production inventory. Fail closed
    # on a different installation rather than silently bypassing old semantics.
    package = await session.scalar(
        select(WorkPackage.id)
        .where(
            WorkPackage.status.not_in((WorkPackageStatus.COMPLETED, WorkPackageStatus.DISCARDED)),
            WorkPackage.execution_contract_version == "horizon-v1:enabled",
        )
        .limit(1)
    )
    if package is not None:
        raise RuntimeError(f"workflow upgrade refused: horizon package {package} needs conversion")


async def require_workflow_upgrade(engine: AsyncEngine) -> None:
    async with AsyncSession(engine) as session:
        await check_stored_handlers(session)
