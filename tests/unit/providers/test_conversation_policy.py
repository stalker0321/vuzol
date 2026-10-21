"""Conversation eligibility is shared and cannot authorize ordinary work."""

from dataclasses import replace

import pytest

from vuzol.config.models import Capability, LaunchMode, ProviderRole
from vuzol.providers.catalog import CONVERSATION_TASK_TYPE, conversation_compatible
from vuzol.providers.policy import ExclusionReason, select_profile

from ._test_providers_helpers import profile, routing_request


def responder(provider="pi", role=ProviderRole.LEAD):
    return profile(
        "selected",
        provider=provider,
        launch_mode="cli",
        api_base_url=None,
        runtime_identity="isolated",
        state_directory="/test-state/isolated",
        sandbox_required=True,
        roles=frozenset({role}),
        supported_task_types=frozenset({"coding", "project_lead", "general"}),
    )


def conversation_request(role=ProviderRole.LEAD):
    return routing_request(
        role=role,
        task_type=CONVERSATION_TASK_TYPE,
        requires_sandbox=True,
        required_launch_mode=LaunchMode.CLI,
        trusted_profile_id="selected",
        restrict_to_profile_ids=frozenset({"selected"}),
    )


@pytest.mark.parametrize("provider", ["pi", "codex", "grok", "kimi"])
@pytest.mark.parametrize("role", [ProviderRole.LEAD, ProviderRole.EXECUTOR])
def test_conversation_uses_role_and_read_only_transport_not_architecture(provider, role):
    selected = responder(provider, role)
    assert "architecture" not in selected.supported_task_types
    assert conversation_compatible(selected, role)
    assert (
        select_profile(conversation_request(role), (selected,), {}).selected_profile_id
        == selected.id
    )
    # The same profile still cannot execute an unconfigured task type.
    ordinary = replace(conversation_request(role), task_type="architecture")
    result = select_profile(ordinary, (selected,), {})
    assert result.selected_profile_id is None
    assert ExclusionReason.TASK_TYPE in result.evaluations[0].reasons


@pytest.mark.parametrize(
    "changes",
    [
        {"required_capabilities": frozenset({Capability.REPOSITORY_READ, Capability.CODE_EDIT})},
        {"required_capabilities": frozenset()},
        {"requires_sandbox": False},
        {"required_launch_mode": None},
        {"required_launch_mode": LaunchMode.API},
        {"trusted_profile_id": None},
        {"trusted_profile_id": "someone-else"},
        {"restrict_to_profile_ids": None},
        {"restrict_to_profile_ids": frozenset({"selected", "fallback"})},
        {"role": ProviderRole.PLANNER},
    ],
)
def test_internal_task_name_is_not_a_bypass(changes):
    selected = responder().model_copy(
        update={
            "supported_task_types": frozenset({CONVERSATION_TASK_TYPE}),
            "roles": frozenset(ProviderRole),
            "capabilities": frozenset({Capability.REPOSITORY_READ, Capability.CODE_EDIT}),
        }
    )
    result = select_profile(replace(conversation_request(), **changes), (selected,), {})
    assert result.selected_profile_id is None
    assert ExclusionReason.TASK_TYPE in result.evaluations[0].reasons


@pytest.mark.parametrize(
    "changes",
    [
        {"enabled": False},
        {"roles": frozenset({ProviderRole.REVIEWER})},
        {"capabilities": frozenset({Capability.CODE_EDIT})},
        {"sandbox_required": False},
        {"launch_mode": LaunchMode.API, "provider": "openai-compatible"},
        {"provider": "uninstalled"},
    ],
)
def test_conversation_rejects_incompatible_profiles_at_policy_too(changes):
    selected = responder().model_copy(update=changes)
    assert not conversation_compatible(selected, ProviderRole.LEAD)
    result = select_profile(conversation_request(), (selected,), {})
    assert result.selected_profile_id is None


def test_conversation_does_not_bypass_project_permissions():
    result = select_profile(
        replace(conversation_request(), project_allowed_capabilities=frozenset()),
        (responder(),),
        {},
    )
    assert result.selected_profile_id is None
    assert ExclusionReason.PROJECT_POLICY in result.evaluations[0].reasons
