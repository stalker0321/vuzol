"""Profile-based role authorization, revision pins, and preference CAS."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from vuzol.config.models import LaunchMode, ProviderRole
from vuzol.config.registries import RegistryError
from vuzol.config.revision import content_revision
from vuzol.projects.executor_preference import ExecutorPreferenceError
from vuzol.projects.model_matrix import (
    MATRIX_KEY,
    SLOT_ROLES,
    ModelAssignment,
    assignment_profile,
    assignment_snapshot,
    eligible_profiles,
    pinned_assignment,
    set_assignment,
)
from vuzol.storage.models import ProjectExecutorPreference

from .test_executor_preference import _codex_profile


def configured(provider="codex", *, profile_id="selected", roles=frozenset(ProviderRole)):
    return _codex_profile(
        id=profile_id,
        provider=provider,
        model="vendor/model-name",
        launch_mode=LaunchMode.API if provider == "openai-compatible" else LaunchMode.CLI,
        api_base_url="https://provider.example/v1" if provider == "openai-compatible" else None,
        runtime_identity=None if provider == "openai-compatible" else profile_id,
        state_directory=None if provider == "openai-compatible" else f"/test-state/{profile_id}",
        roles=roles,
        fallback_profile_ids=(),
    )


def bundle(*profiles):
    registry = MagicMock()
    registry.profiles.items.return_value = profiles

    def get(key):
        found = next((p for p in profiles if p.id == key), None)
        if found is None:
            raise RegistryError("profile is missing")
        return found

    registry.profiles.get.side_effect = get
    return registry


def assignment(profile):
    return ModelAssignment(
        profile_id=profile.id,
        model=profile.model,
        reasoning_effort=profile.model_reasoning_effort,
        content_revision=content_revision(profile),
    )


@pytest.mark.parametrize("provider", ["codex", "grok", "kimi", "pi", "openai-compatible"])
@pytest.mark.parametrize("slot", SLOT_ROLES)
def test_supported_profiles_are_eligible_by_configured_role(provider, slot):
    profile = configured(provider)
    expected = () if slot == "executor" and profile.launch_mode is LaunchMode.API else (profile,)
    assert eligible_profiles(bundle(profile), slot) == expected


@pytest.mark.parametrize("slot", SLOT_ROLES)
def test_eligibility_never_infers_an_unauthorized_role(slot):
    profile = configured(roles=frozenset({ProviderRole.SUMMARIZER}))
    assert eligible_profiles(bundle(profile), slot) == ()


@pytest.mark.parametrize(
    "changes", [{"enabled": False}, {"provider": "unknown"}, {"launch_mode": LaunchMode.TOOL}]
)
def test_unsupported_or_disabled_profile_is_ineligible(changes):
    profile = configured().model_copy(update=changes)
    assert eligible_profiles(bundle(profile), "planner_light") == ()


def test_assignment_accepts_provider_native_model_and_pins_full_revision():
    profile = configured()
    chosen = assignment(profile)
    snapshot = {MATRIX_KEY: {"executor": chosen.model_dump(mode="json")}}
    assert pinned_assignment(snapshot, "executor") == chosen
    assert assignment_profile(chosen, bundle(profile), "executor") is profile
    assert pinned_assignment(snapshot, "planner_light") is None


@pytest.mark.parametrize(
    "changes",
    [
        {"model": "changed"},
        {"model_reasoning_effort": "high"},
        {"routing_priority": 999},
        {"account_id": "new-account"},
    ],
)
def test_assignment_rejects_changed_configuration_without_fallback(changes):
    profile = configured()
    with pytest.raises(ExecutorPreferenceError, match="configuration changed"):
        assignment_profile(
            assignment(profile), bundle(profile.model_copy(update=changes)), "executor"
        )


@pytest.mark.parametrize(
    "raw", [None, [], {"unknown": {}}, {"lead": None}, {"lead": {"profile_id": "selected"}}]
)
def test_malformed_matrix_fails_closed(raw):
    with pytest.raises(ValueError):
        pinned_assignment({MATRIX_KEY: raw}, "executor")


@pytest.mark.anyio
async def test_assignment_cas_changes_only_one_slot_and_clears_legacy_executor(monkeypatch):
    profile = configured()
    reviewer = assignment(profile).model_dump(mode="json")
    row = ProjectExecutorPreference(
        project_id="project",
        mode="pin",
        worker_key="sol",
        profile_id="old",
        reasoning_effort="low",
        revision=4,
        model_assignments={"reviewer_light": reviewer},
    )
    before = assignment_snapshot(row.model_assignments)
    monkeypatch.setattr(
        "vuzol.projects.model_matrix.ensure_preference_row", AsyncMock(return_value=row)
    )
    session = AsyncMock()
    await set_assignment(
        session,
        project_id="project",
        slot="executor",
        profile_id=profile.id,
        expected_revision=4,
        user_id=7,
        registries=bundle(profile),
    )
    assert row.model_assignments == {
        **before,
        "executor": assignment(profile).model_dump(mode="json"),
    }
    assert row.revision == 5 and row.updated_by_user_id == 7
    assert row.mode == "auto" and row.worker_key is None and row.profile_id is None
    assert before == {"reviewer_light": reviewer}
    with pytest.raises(ExecutorPreferenceError, match="stale"):
        await set_assignment(
            session,
            project_id="project",
            slot="executor",
            profile_id=None,
            expected_revision=4,
            user_id=8,
            registries=bundle(profile),
        )
    assert row.revision == 5
    await set_assignment(
        session,
        project_id="project",
        slot="executor",
        profile_id=None,
        expected_revision=5,
        user_id=8,
        registries=bundle(profile),
    )
    assert row.model_assignments == before and row.revision == 6


@pytest.mark.anyio
async def test_unavailable_selection_cannot_mutate_preferences(monkeypatch):
    ensure = AsyncMock()
    monkeypatch.setattr("vuzol.projects.model_matrix.ensure_preference_row", ensure)
    with pytest.raises(ExecutorPreferenceError, match="unavailable"):
        await set_assignment(
            AsyncMock(),
            project_id="project",
            slot="lead",
            profile_id="missing",
            expected_revision=1,
            user_id=7,
            registries=bundle(configured()),
        )
    ensure.assert_not_awaited()


def test_legacy_status_view_reports_matrix_executor_instead_of_auto():
    from vuzol.projects.executor_preference import format_preference_label, preference_from_row

    profile = configured("pi")
    row = ProjectExecutorPreference(
        project_id="project",
        mode="auto",
        revision=3,
        worker_key=None,
        model_assignments={"executor": assignment(profile).model_dump(mode="json")},
    )
    view = preference_from_row(row)
    assert not view.is_auto
    assert view.profile_id == profile.id and view.model == profile.model
    label = format_preference_label(view)
    assert profile.id in label and profile.model in label and "automatic" not in label
