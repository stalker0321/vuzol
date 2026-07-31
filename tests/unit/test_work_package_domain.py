import uuid

import pytest

from vuzol.discussion.domain import (
    DomainError,
    PackageControlAction,
    PlanDraft,
    PlanItemDraft,
    canonical_plan_body,
    canonical_plan_hash,
    control_transition_target,
    require_generation,
    require_mutable,
)
from vuzol.storage.types import WorkPackageStatus


def item(**overrides: object) -> PlanItemDraft:
    values: dict[str, object] = {
        "summary": "Implement domain",
        "goal": "Keep lifecycle deterministic",
        "expected_outcome": "A tested domain service",
        "completion_criteria": ("Unit tests pass",),
        "allowed_scope": "src/vuzol/discussion/**",
        "local_id": "domain",
    }
    values.update(overrides)
    return PlanItemDraft(**values)  # type: ignore[arg-type]


def test_canonical_plan_hash_is_stable_and_binds_item_identity() -> None:
    first_id = uuid.uuid4()
    plan = PlanDraft(title=" P2 lifecycle ", items=(item(),))
    body = canonical_plan_body(plan, (first_id,))

    assert body["title"] == "P2 lifecycle"
    assert body["items"][0]["ordinal"] == 1
    assert canonical_plan_hash(body) == canonical_plan_hash(dict(reversed(body.items())))
    assert canonical_plan_hash(body) != canonical_plan_hash(
        canonical_plan_body(plan, (uuid.uuid4(),))
    )


def test_plan_validation_rejects_bad_shapes() -> None:
    with pytest.raises(DomainError, match="at least one item"):
        PlanDraft(title="empty", items=())
    with pytest.raises(DomainError, match="unique"):
        PlanDraft(title="duplicate", items=(item(), item(summary="second")))
    with pytest.raises(DomainError, match="lowercase slug"):
        item(local_id="Not Valid")
    with pytest.raises(DomainError, match="completion criteria"):
        item(completion_criteria=())


def test_generation_and_terminal_mutations_fail_closed() -> None:
    require_generation(4, 4)
    with pytest.raises(DomainError) as stale:
        require_generation(4, 3)
    assert stale.value.code == "stale_generation"

    for status in (
        WorkPackageStatus.COMPLETED,
        WorkPackageStatus.STOPPED,
        WorkPackageStatus.DISCARDED,
    ):
        with pytest.raises(DomainError) as terminal:
            require_mutable(status)
        assert terminal.value.code == "terminal_package"


@pytest.mark.parametrize(
    ("status", "action", "target"),
    [
        (WorkPackageStatus.DRAFT, PackageControlAction.APPROVE, WorkPackageStatus.APPROVED),
        (WorkPackageStatus.APPROVED, PackageControlAction.START, WorkPackageStatus.RUNNING),
        (WorkPackageStatus.PAUSED, PackageControlAction.RETRY_ITEM, WorkPackageStatus.RUNNING),
        (WorkPackageStatus.PAUSED, PackageControlAction.SKIP_ITEM, WorkPackageStatus.RUNNING),
        (WorkPackageStatus.RUNNING, PackageControlAction.STOP_PACKAGE, WorkPackageStatus.STOPPED),
        (WorkPackageStatus.RUNNING, PackageControlAction.REQUEST_REPLAN, WorkPackageStatus.DRAFT),
    ],
)
def test_authoritative_control_state_machine(
    status: WorkPackageStatus,
    action: PackageControlAction,
    target: WorkPackageStatus,
) -> None:
    assert control_transition_target(status, action) is target

    with pytest.raises(DomainError) as rejected:
        control_transition_target(WorkPackageStatus.COMPLETED, action)
    assert rejected.value.code == "invalid_transition"
