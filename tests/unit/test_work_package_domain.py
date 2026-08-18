import uuid
from collections.abc import Callable

import pytest

from vuzol.discussion.domain import (
    CapabilityProvisioning,
    CapabilityRequirementDraft,
    ComponentKind,
    DomainError,
    EnvironmentComponentDraft,
    EnvironmentDeltaDraft,
    PackageControlAction,
    PlanDraft,
    PlanItemDraft,
    canonical_plan_body,
    canonical_plan_hash,
    control_transition_target,
    plan_outline_hash,
    require_generation,
    require_mutable,
    revision_outline_hash,
    semantic_plan_hash,
    semantic_revision_hash,
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
    assert semantic_plan_hash(plan) == semantic_revision_hash(body)


def test_plan_validation_rejects_bad_shapes() -> None:
    with pytest.raises(DomainError, match="at least one item"):
        PlanDraft(title="empty", items=())
    with pytest.raises(DomainError, match="unique"):
        PlanDraft(title="duplicate", items=(item(), item(summary="second")))
    with pytest.raises(DomainError, match="lowercase slug"):
        item(local_id="Not Valid")
    with pytest.raises(DomainError, match="completion criteria"):
        item(completion_criteria=())


@pytest.mark.parametrize(
    "overrides",
    [
        {"summary": ""},
        {"summary": "x" * 241},
        {"goal": " "},
        {"expected_outcome": ""},
        {"allowed_scope": ""},
        {"completion_criteria": ("",)},
        {"local_id": "x" * 65},
    ],
)
def test_plan_item_rejects_each_invalid_field(overrides: dict[str, object]) -> None:
    with pytest.raises(DomainError) as error:
        item(**overrides)
    assert error.value.code == "invalid_plan"


def test_plan_rejects_invalid_title_duplicate_item_ids_and_identity_count() -> None:
    identifier = uuid.uuid4()
    with pytest.raises(DomainError, match="package title"):
        PlanDraft(title="", items=(item(),))
    with pytest.raises(DomainError, match="package title"):
        PlanDraft(title="x" * 241, items=(item(),))
    with pytest.raises(DomainError, match="item_id values must be unique"):
        PlanDraft(
            title="duplicate ids",
            items=(item(item_id=identifier), item(item_id=identifier, local_id="second")),
        )
    with pytest.raises(DomainError, match="identities do not match"):
        canonical_plan_body(PlanDraft(title="plan", items=(item(),)), ())


def test_environment_delta_is_canonical_and_part_of_every_plan_fingerprint() -> None:
    component = EnvironmentComponentDraft(
        key="api",
        label=" API ",
        kind=ComponentKind.WEB_SERVICE,
        technology=" Node.js ",
        version="22",
        run_command=("node", "server.js"),
        port=8080,
        healthcheck_path="/ready",
        artifact_patterns=("dist/**",),
    )
    capability = CapabilityRequirementDraft(
        key="node-runtime",
        label=" Node runtime ",
        provisioning=CapabilityProvisioning.AUTOMATIC,
        reason=" required ",
    )
    plan = PlanDraft(
        title="Runtime",
        items=(item(),),
        environment_delta=EnvironmentDeltaDraft(
            upsert_components=(component,),
            remove_components=("old-web",),
            required_capabilities=(capability,),
        ),
    )
    body = canonical_plan_body(plan, (uuid.uuid4(),))

    assert body["environment_delta"]["upsert_components"][0]["label"] == "API"
    assert body["environment_delta"]["required_capabilities"][0]["reason"] == "required"
    assert semantic_plan_hash(plan) == semantic_revision_hash(body)
    assert plan_outline_hash(plan) == revision_outline_hash(body)


@pytest.mark.parametrize(
    "factory",
    [
        lambda: EnvironmentComponentDraft("Bad", "Web", ComponentKind.STATIC_SITE, "HTML"),
        lambda: EnvironmentComponentDraft("web", "", ComponentKind.STATIC_SITE, "HTML"),
        lambda: EnvironmentComponentDraft("web", "Web", ComponentKind.STATIC_SITE, ""),
        lambda: EnvironmentComponentDraft("web", "Web", ComponentKind.STATIC_SITE, "HTML", port=0),
        lambda: EnvironmentComponentDraft(
            "web", "Web", ComponentKind.STATIC_SITE, "HTML", healthcheck_path="ready"
        ),
        lambda: EnvironmentComponentDraft("web", "Web", ComponentKind.WEB_SERVICE, "Node", port=1),
        lambda: EnvironmentComponentDraft(
            "web", "Web", ComponentKind.WEB_SERVICE, "Node", run_command=("node",)
        ),
        lambda: EnvironmentComponentDraft(
            "cli", "CLI", ComponentKind.CLI, "Python", run_command=("",)
        ),
        lambda: EnvironmentComponentDraft(
            "cli", "CLI", ComponentKind.CLI, "Python", run_command=("python",) * 33
        ),
        lambda: EnvironmentComponentDraft(
            "library", "Library", ComponentKind.LIBRARY, "Python", artifact_patterns=("../x",)
        ),
        lambda: EnvironmentComponentDraft(
            "library", "Library", ComponentKind.LIBRARY, "Python", artifact_patterns=("/outside/x",)
        ),
        lambda: CapabilityRequirementDraft("Bad", "Node"),
        lambda: CapabilityRequirementDraft("node", ""),
        lambda: CapabilityRequirementDraft("node", "Node", reason="x" * 501),
    ],
)
def test_environment_values_reject_invalid_shapes(factory: Callable[[], object]) -> None:
    with pytest.raises(DomainError):
        factory()


def test_environment_delta_rejects_conflicts_and_duplicates() -> None:
    component = EnvironmentComponentDraft("web", "Web", ComponentKind.STATIC_SITE, "HTML")
    capability = CapabilityRequirementDraft("runtime", "Runtime")
    for kwargs in (
        {"upsert_components": (component, component)},
        {"remove_components": ("web", "web")},
        {"remove_components": ("Bad",)},
        {"upsert_components": (component,), "remove_components": ("web",)},
        {"required_capabilities": (capability, capability)},
    ):
        with pytest.raises(DomainError):
            EnvironmentDeltaDraft(**kwargs)


def test_stored_fingerprints_reject_missing_items() -> None:
    with pytest.raises(DomainError):
        semantic_revision_hash({})
    with pytest.raises(DomainError):
        revision_outline_hash({})


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
        (
            WorkPackageStatus.STOPPED,
            PackageControlAction.RESTART_PACKAGE,
            WorkPackageStatus.RUNNING,
        ),
        (WorkPackageStatus.RUNNING, PackageControlAction.REQUEST_REPLAN, WorkPackageStatus.PAUSED),
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
