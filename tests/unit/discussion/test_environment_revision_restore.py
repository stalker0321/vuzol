import uuid
from collections.abc import Callable
from typing import Any, cast

import pytest

from vuzol.discussion.domain import (
    CapabilityProvisioning,
    CapabilityRequirementDraft,
    ComponentKind,
    DomainError,
    EnvironmentComponentDraft,
    EnvironmentDeltaDraft,
    PlanDraft,
    PlanItemDraft,
    canonical_plan_body,
)
from vuzol.discussion.service import _plan_from_revision_body


def _body() -> dict[str, object]:
    plan = PlanDraft(
        title="Runtime plan",
        items=(
            PlanItemDraft(
                summary="Run service",
                goal="Publish a preview",
                expected_outcome="Healthy endpoint",
                completion_criteria=("HTTP 200",),
                allowed_scope="server.js",
                local_id="runtime",
            ),
        ),
        environment_delta=EnvironmentDeltaDraft(
            upsert_components=(
                EnvironmentComponentDraft(
                    key="web",
                    label="Web API",
                    kind=ComponentKind.WEB_SERVICE,
                    technology="Node.js",
                    version="22",
                    run_command=("node", "server.js"),
                    port=8080,
                    healthcheck_path="/ready",
                    artifact_patterns=("public/**",),
                ),
            ),
            remove_components=("legacy",),
            required_capabilities=(
                CapabilityRequirementDraft(
                    key="node-runtime",
                    label="Node runtime",
                    provisioning=CapabilityProvisioning.AUTOMATIC,
                    reason="Run preview",
                ),
            ),
        ),
    )
    return canonical_plan_body(plan, (uuid.uuid4(),))


def test_plan_revision_restores_complete_environment_delta() -> None:
    restored = _plan_from_revision_body(_body())

    assert restored.title == "Runtime plan"
    assert restored.items[0].local_id == "runtime"
    component = restored.environment_delta.upsert_components[0]
    assert component.kind is ComponentKind.WEB_SERVICE
    assert component.run_command == ("node", "server.js")
    assert component.healthcheck_path == "/ready"
    assert restored.environment_delta.remove_components == ("legacy",)
    assert (
        restored.environment_delta.required_capabilities[0].provisioning
        is CapabilityProvisioning.AUTOMATIC
    )


def test_legacy_revision_restores_empty_environment_delta() -> None:
    body = _body()
    body.pop("environment_delta")

    assert _plan_from_revision_body(body).environment_delta.is_empty


@pytest.mark.parametrize(
    "mutate",
    [
        lambda body: body.update(environment_delta="bad"),
        lambda body: body["environment_delta"].update(upsert_components="bad"),
        lambda body: body["environment_delta"]["upsert_components"][0].pop("key"),
        lambda body: body["environment_delta"]["upsert_components"][0].update(kind="bad"),
        lambda body: body["environment_delta"]["required_capabilities"][0].update(
            provisioning="bad"
        ),
        lambda body: body["environment_delta"].update(remove_components=["Bad key"]),
    ],
)
def test_corrupt_environment_revision_fails_closed(
    mutate: Callable[[dict[str, Any]], None],
) -> None:
    body = _body()
    mutate(cast(dict[str, Any], body))

    with pytest.raises(DomainError):
        _plan_from_revision_body(body)


@pytest.mark.parametrize(
    "mutate",
    [
        lambda body: body.update(title=None),
        lambda body: body.update(items=None),
        lambda body: body.update(items=["bad"]),
        lambda body: body["items"][0].update(local_id=7),
        lambda body: body["items"][0].update(item_id="not-a-uuid"),
        lambda body: body["items"][0].pop("summary"),
        lambda body: body["items"][0].update(suggested_risk="unknown"),
        lambda body: body["items"][0].update(completion_criteria=None),
    ],
)
def test_corrupt_plan_revision_fails_closed(
    mutate: Callable[[dict[str, Any]], None],
) -> None:
    body = _body()
    mutate(cast(dict[str, Any], body))

    with pytest.raises(DomainError, match="invalid_plan"):
        _plan_from_revision_body(body)
