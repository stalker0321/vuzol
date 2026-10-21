"""Stage 1 policy, identity, and canonical hash. No database."""

import uuid

import pytest

from vuzol.config.settings import ProjectLeadSettings, Settings
from vuzol.leadership.actor import Actor, ActorRejected, server_agent_principal
from vuzol.leadership.commands import LeadCommand, payload_hash
from vuzol.leadership.policy import integration_branch_rejection
from vuzol.workflows.application import Principal


def test_project_lead_is_off_unless_configured() -> None:
    assert ProjectLeadSettings().enabled is False
    assert Settings(environment="test").project_lead.enabled is False
    assert Settings(environment="test").project_lead.repeat_limit == 2
    assert Settings(environment="test").project_lead.integration_branch_prefix == "vuzol-managed/"
    assert Settings(environment="test").project_lead.profile_id is None
    assert ProjectLeadSettings().inference_timeout_seconds == 60


@pytest.mark.parametrize("seconds", [0, -1, 601])
def test_lead_inference_timeout_is_bounded(seconds: int) -> None:
    with pytest.raises(ValueError):
        ProjectLeadSettings(inference_timeout_seconds=seconds)


def test_lead_inference_timeout_is_environment_configurable(monkeypatch) -> None:
    monkeypatch.setenv("VUZOL_PROJECT_LEAD__INFERENCE_TIMEOUT_SECONDS", "180")
    assert Settings(environment="test").project_lead.inference_timeout_seconds == 180


def test_agent_principal_cannot_be_an_owner_user_id() -> None:
    with pytest.raises(ActorRejected, match="agent_principal_invalid"):
        Actor.agent("42", "cli")
    with pytest.raises(ActorRejected, match="agent_principal_invalid"):
        Actor.agent("agent:42", "cli")
    with pytest.raises(ActorRejected, match="agent_ingress_invalid"):
        Actor.agent("agent:project-lead:demo", "model")
    actor = Actor.agent(server_agent_principal("demo"), "worker")
    assert actor.user_id is None
    assert actor.subject == "agent:project-lead:demo"


def test_human_adapter_rejects_user_zero() -> None:
    with pytest.raises(ValueError, match="principal_invalid"):
        Actor.from_principal(Principal(user_id=0, ingress_source="telegram"))


def test_protected_and_default_branches_are_rejected() -> None:
    prefix = "vuzol-managed/"
    assert integration_branch_rejection("main", prefix=prefix, default_branch="main") == (
        "integration_branch_not_delegated"
    )
    assert (
        integration_branch_rejection("vuzol-managed/main", prefix=prefix, default_branch="main")
        == "protected_branch"
    )
    assert (
        integration_branch_rejection("vuzol-managed/prod/topic", prefix=prefix, default_branch=None)
        == "protected_branch"
    )
    assert (
        integration_branch_rejection("vuzol-managed/../secret", prefix=prefix, default_branch=None)
        == "integration_branch_not_delegated"
    )
    assert (
        integration_branch_rejection(
            "vuzol-managed/demo/goal", prefix=prefix, default_branch="main"
        )
        is None
    )


def test_payload_hash_is_stable_and_binds_reason() -> None:
    goal_id = uuid.UUID("11111111-1111-1111-1111-111111111111")
    left = LeadCommand(
        name="pause_goal",
        project_id="demo",
        idempotency_key="k",
        payload={"b": 1, "a": {"z": 2, "y": 1}},
        goal_id=goal_id,
        expected_version=3,
        reason="hold",
    )
    right = LeadCommand(
        name="pause_goal",
        project_id="demo",
        idempotency_key="other-key-is-not-hashed",
        payload={"a": {"y": 1, "z": 2}, "b": 1},
        goal_id=goal_id,
        expected_version=3,
        reason="hold",
    )
    changed = LeadCommand(
        name="pause_goal",
        project_id="demo",
        idempotency_key="k",
        payload={"b": 1, "a": {"z": 2, "y": 1}},
        goal_id=goal_id,
        expected_version=3,
        reason="different",
    )
    assert left.canonical_hash() == payload_hash(right)
    assert left.canonical_hash() != changed.canonical_hash()
    assert len(left.canonical_hash()) == 64
