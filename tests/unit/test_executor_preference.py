"""Project-level executor preference mapping and payload overrides."""

from pathlib import Path

import pytest

from vuzol.config import (
    Capability,
    ConfigurationBundle,
    CostClass,
    LaunchMode,
    ProfileRegistry,
    ProjectConfig,
    ProjectRegistry,
    ProviderProfileConfig,
    ProviderRole,
    SandboxProfileConfig,
    SandboxRegistry,
    TopicRegistry,
)
from vuzol.projects.executor_preference import (
    DEFAULT_REASONING_EFFORT,
    ExecutorPreferenceMode,
    ExecutorPreferenceView,
    ExecutorWorkerKey,
    apply_profile_overrides,
    available_workers,
    codex_model_for_worker,
    format_preference_label,
    preference_payload,
    resolve_route_pin,
    same_family_fallback_ids,
)


def _codex_profile(**changes: object) -> ProviderProfileConfig:
    values: dict[str, object] = {
        "id": "codex-subscription-prod",
        "provider": "codex",
        "model": "gpt-5.6-sol",
        "model_reasoning_effort": "medium",
        "launch_mode": LaunchMode.CLI,
        "credential_required": False,
        "capabilities": frozenset(
            {
                Capability.REPOSITORY_READ,
                Capability.CODE_EDIT,
                Capability.GIT,
                Capability.PROJECT_SHELL,
            }
        ),
        "concurrency_limit": 1,
        "cost_class": CostClass.STRONG,
        "roles": frozenset({ProviderRole.EXECUTOR, ProviderRole.PLANNER}),
        "routing_priority": 200,
        "supported_task_types": frozenset({"coding", "architecture"}),
        "fallback_profile_ids": ("grok-subscription-a",),
        "sandbox_required": True,
        "runtime_identity": "vuzol-executor",
        "state_directory": Path("/var/lib/vuzol-provider-state/codex-subscription-prod"),
        "enabled": True,
    }
    values.update(changes)
    return ProviderProfileConfig.model_validate(values)


def _grok_profile(
    profile_id: str, *, priority: int, fallbacks: tuple[str, ...] = ()
) -> ProviderProfileConfig:
    return ProviderProfileConfig.model_validate(
        {
            "id": profile_id,
            "provider": "grok",
            "model": "grok-build",
            "launch_mode": LaunchMode.CLI,
            "credential_required": False,
            "capabilities": frozenset(
                {
                    Capability.REPOSITORY_READ,
                    Capability.CODE_EDIT,
                    Capability.GIT,
                    Capability.PROJECT_SHELL,
                }
            ),
            "concurrency_limit": 1,
            "cost_class": CostClass.STRONG,
            "roles": frozenset({ProviderRole.EXECUTOR, ProviderRole.PLANNER}),
            "routing_priority": priority,
            "supported_task_types": frozenset({"coding", "architecture"}),
            "fallback_profile_ids": fallbacks,
            "sandbox_required": True,
            "runtime_identity": f"vuzol-{profile_id}",
            "state_directory": Path(f"/var/lib/vuzol-provider-state/{profile_id}"),
            "enabled": True,
        }
    )


def _bundle(*profiles: ProviderProfileConfig) -> ConfigurationBundle:
    sandbox = SandboxProfileConfig.model_validate(
        {
            "id": "project-default",
            "image": "registry.example/vuzol-sandbox@sha256:" + ("0" * 64),
            "enabled": False,
        }
    )
    project = ProjectConfig.model_validate(
        {
            "id": "bill-buddy",
            "display_name": "Bill Buddy",
            "repository_path": "bill-buddy",
            "default_branch": "main",
            "allowed_capabilities": frozenset(
                {
                    Capability.REPOSITORY_READ,
                    Capability.CODE_EDIT,
                    Capability.GIT,
                    Capability.PROJECT_SHELL,
                }
            ),
            "sandbox_profile": "project-default",
            "enabled": False,
        }
    )
    projects = ProjectRegistry((project,), repository_root=Path("/tmp"))  # noqa: S108
    return ConfigurationBundle(
        projects=projects,
        profiles=ProfileRegistry(profiles),
        topics=TopicRegistry((), projects=projects),
        sandboxes=SandboxRegistry((sandbox,)),
        revision="test-revision",
    )


def test_available_workers_lists_codex_variants_and_grok() -> None:
    bundle = _bundle(
        _codex_profile(),
        _grok_profile("grok-subscription-a", priority=210, fallbacks=("grok-subscription-b",)),
        _grok_profile("grok-subscription-b", priority=220),
    )
    keys = [option.key for option in available_workers(bundle)]
    assert keys == [
        ExecutorWorkerKey.SOL,
        ExecutorWorkerKey.TERRA,
        ExecutorWorkerKey.LUNA,
        ExecutorWorkerKey.GROK,
    ]


def test_codex_model_for_worker_swaps_variant() -> None:
    assert codex_model_for_worker("gpt-5.6-sol", ExecutorWorkerKey.TERRA) == "gpt-5.6-terra"
    assert codex_model_for_worker("gpt-5.6-sol", ExecutorWorkerKey.LUNA) == "gpt-5.6-luna"
    assert codex_model_for_worker("codex", ExecutorWorkerKey.SOL) == "gpt-5.6-sol"


def test_resolve_route_pin_for_sol_and_grok() -> None:
    bundle = _bundle(
        _codex_profile(),
        _grok_profile("grok-subscription-a", priority=210, fallbacks=("grok-subscription-b",)),
        _grok_profile("grok-subscription-b", priority=220),
    )
    sol = resolve_route_pin(
        ExecutorPreferenceView(
            project_id="bill-buddy",
            mode=ExecutorPreferenceMode.PIN,
            worker_key=ExecutorWorkerKey.SOL,
            reasoning_effort="high",
            revision=2,
        ),
        bundle,
    )
    assert sol is not None
    assert sol.trusted_profile_id == "codex-subscription-prod"
    assert sol.model_override == "gpt-5.6-sol"
    assert sol.reasoning_effort == "high"

    terra = resolve_route_pin(
        ExecutorPreferenceView(
            project_id="bill-buddy",
            mode=ExecutorPreferenceMode.PIN,
            worker_key=ExecutorWorkerKey.TERRA,
            reasoning_effort="low",
            revision=3,
        ),
        bundle,
    )
    assert terra is not None
    assert terra.model_override == "gpt-5.6-terra"

    grok = resolve_route_pin(
        ExecutorPreferenceView(
            project_id="bill-buddy",
            mode=ExecutorPreferenceMode.PIN,
            worker_key=ExecutorWorkerKey.GROK,
            reasoning_effort=None,
            revision=4,
        ),
        bundle,
    )
    assert grok is not None
    assert grok.trusted_profile_id == "grok-subscription-a"
    assert grok.model_override is None

    auto = resolve_route_pin(
        ExecutorPreferenceView(
            project_id="bill-buddy",
            mode=ExecutorPreferenceMode.AUTO,
            worker_key=None,
            reasoning_effort=None,
            revision=1,
        ),
        bundle,
    )
    assert auto is None


def test_same_family_fallbacks_drop_cross_provider_edges() -> None:
    bundle = _bundle(
        _codex_profile(),
        _grok_profile("grok-subscription-a", priority=210, fallbacks=("grok-subscription-b",)),
        _grok_profile("grok-subscription-b", priority=220),
    )
    assert (
        same_family_fallback_ids(
            bundle,
            worker_key=ExecutorWorkerKey.SOL,
            primary_profile_id="codex-subscription-prod",
        )
        == ()
    )
    assert same_family_fallback_ids(
        bundle,
        worker_key=ExecutorWorkerKey.GROK,
        primary_profile_id="grok-subscription-a",
    ) == ("grok-subscription-b",)


def test_apply_profile_overrides_and_payload() -> None:
    profile = _codex_profile(fallback_profile_ids=())
    pin = resolve_route_pin(
        ExecutorPreferenceView(
            project_id="bill-buddy",
            mode=ExecutorPreferenceMode.PIN,
            worker_key=ExecutorWorkerKey.LUNA,
            reasoning_effort="xhigh",
            revision=5,
        ),
        _bundle(profile),
    )
    assert pin is not None
    payload = preference_payload(pin)
    overridden = apply_profile_overrides(profile, payload)
    assert overridden.model == "gpt-5.6-luna"
    assert overridden.model_reasoning_effort == "xhigh"
    assert profile.model == "gpt-5.6-sol"
    assert (
        format_preference_label(
            ExecutorPreferenceView(
                project_id="bill-buddy",
                mode=ExecutorPreferenceMode.PIN,
                worker_key=ExecutorWorkerKey.LUNA,
                reasoning_effort="xhigh",
                revision=5,
            )
        )
        == "Luna · xhigh"
    )
    assert (
        format_preference_label(
            ExecutorPreferenceView(
                project_id="bill-buddy",
                mode=ExecutorPreferenceMode.AUTO,
                worker_key=None,
                reasoning_effort=None,
                revision=1,
            )
        )
        == "Routing (automatic)"
    )
    assert DEFAULT_REASONING_EFFORT == "medium"


def test_apply_profile_overrides_rejects_invalid_values() -> None:
    profile = _codex_profile(fallback_profile_ids=())
    with pytest.raises(ValueError, match="model override"):
        apply_profile_overrides(profile, {"executor_model_override": "../etc/passwd"})
    with pytest.raises(ValueError, match="reasoning effort"):
        apply_profile_overrides(profile, {"executor_reasoning_effort": "ludicrous"})
    with pytest.raises(ValueError, match="reasoning effort"):
        apply_profile_overrides(profile, {"executor_reasoning_effort": 3})
    cleared = apply_profile_overrides(profile, {"executor_reasoning_effort": None})
    assert cleared.model_reasoning_effort is None


class _PreferenceRow:
    def __init__(self, *, revision: int = 1) -> None:
        self.project_id = "bill-buddy"
        self.mode = "auto"
        self.worker_key: str | None = None
        self.reasoning_effort: str | None = None
        self.revision = revision
        self.updated_by_user_id: int | None = None


@pytest.mark.anyio
async def test_set_worker_preference_rejects_unavailable_and_effort_rules() -> None:
    from unittest.mock import AsyncMock, MagicMock

    from vuzol.projects.executor_preference import (
        ExecutorPreferenceError,
        set_worker_preference,
    )

    row = _PreferenceRow(revision=1)
    session = MagicMock()
    session.get = AsyncMock(return_value=row)
    session.flush = AsyncMock()
    codex_only = _bundle(_codex_profile(fallback_profile_ids=()))
    with pytest.raises(ExecutorPreferenceError, match="not available"):
        await set_worker_preference(
            session,
            project_id="bill-buddy",
            user_id=1,
            expected_revision=1,
            worker_key=ExecutorWorkerKey.GROK,
            reasoning_effort=None,
            registries=codex_only,
        )
    full = _bundle(
        _codex_profile(),
        _grok_profile("grok-subscription-a", priority=210, fallbacks=("grok-subscription-b",)),
        _grok_profile("grok-subscription-b", priority=220),
    )
    with pytest.raises(ExecutorPreferenceError, match="effort is required"):
        await set_worker_preference(
            session,
            project_id="bill-buddy",
            user_id=1,
            expected_revision=1,
            worker_key=ExecutorWorkerKey.SOL,
            reasoning_effort=None,
            registries=full,
        )
    with pytest.raises(ExecutorPreferenceError, match="does not support"):
        await set_worker_preference(
            session,
            project_id="bill-buddy",
            user_id=1,
            expected_revision=1,
            worker_key=ExecutorWorkerKey.GROK,
            reasoning_effort="high",
            registries=full,
        )


@pytest.mark.anyio
async def test_set_worker_preference_rejects_stale_revision() -> None:
    from unittest.mock import AsyncMock, MagicMock

    from vuzol.projects.executor_preference import (
        ExecutorPreferenceError,
        set_auto_preference,
        set_worker_preference,
    )

    row = _PreferenceRow(revision=4)
    session = MagicMock()
    session.get = AsyncMock(return_value=row)
    session.flush = AsyncMock()
    bundle = _bundle(
        _codex_profile(),
        _grok_profile("grok-subscription-a", priority=210, fallbacks=("grok-subscription-b",)),
        _grok_profile("grok-subscription-b", priority=220),
    )
    with pytest.raises(ExecutorPreferenceError, match="stale"):
        await set_worker_preference(
            session,
            project_id="bill-buddy",
            user_id=1,
            expected_revision=1,
            worker_key=ExecutorWorkerKey.SOL,
            reasoning_effort="medium",
            registries=bundle,
        )
    with pytest.raises(ExecutorPreferenceError, match="stale"):
        await set_auto_preference(
            session,
            project_id="bill-buddy",
            user_id=1,
            expected_revision=2,
        )


@pytest.mark.anyio
async def test_ensure_preference_row_inserts_when_missing() -> None:
    from unittest.mock import AsyncMock, MagicMock

    from vuzol.projects.executor_preference import ensure_preference_row

    created = _PreferenceRow(revision=1)
    session = MagicMock()
    session.get = AsyncMock(side_effect=[None, created])
    session.execute = AsyncMock()
    row = await ensure_preference_row(session, "bill-buddy")
    assert row.project_id == "bill-buddy"
    assert row.revision == 1
    session.execute.assert_awaited_once()
    assert session.get.await_count == 2


@pytest.mark.anyio
async def test_load_preference_defaults_when_missing() -> None:
    from unittest.mock import AsyncMock, MagicMock

    from vuzol.projects.executor_preference import load_preference

    session = MagicMock()
    session.get = AsyncMock(return_value=None)
    view = await load_preference(session, "notes")
    assert view.project_id == "notes"
    assert view.is_auto
    assert view.revision == 1


@pytest.mark.anyio
async def test_set_auto_preference_clears_pin() -> None:
    from unittest.mock import AsyncMock, MagicMock

    from vuzol.projects.executor_preference import set_auto_preference

    row = _PreferenceRow(revision=3)
    row.mode = "pin"
    row.worker_key = "grok"
    session = MagicMock()
    session.get = AsyncMock(return_value=row)
    session.flush = AsyncMock()
    view = await set_auto_preference(
        session, project_id="bill-buddy", user_id=5, expected_revision=3
    )
    assert view.is_auto
    assert row.worker_key is None
    assert row.revision == 4


@pytest.mark.anyio
async def test_set_worker_preference_persists_pin() -> None:
    from unittest.mock import AsyncMock, MagicMock

    from vuzol.projects.executor_preference import set_worker_preference

    row = _PreferenceRow(revision=1)
    session = MagicMock()
    session.get = AsyncMock(return_value=row)
    session.flush = AsyncMock()
    bundle = _bundle(
        _codex_profile(),
        _grok_profile("grok-subscription-a", priority=210, fallbacks=("grok-subscription-b",)),
        _grok_profile("grok-subscription-b", priority=220),
    )
    view = await set_worker_preference(
        session,
        project_id="bill-buddy",
        user_id=99,
        expected_revision=1,
        worker_key=ExecutorWorkerKey.TERRA,
        reasoning_effort="high",
        registries=bundle,
    )
    assert view.mode is ExecutorPreferenceMode.PIN
    assert view.worker_key is ExecutorWorkerKey.TERRA
    assert view.reasoning_effort == "high"
    assert view.revision == 2
    assert row.mode == "pin"
    assert row.worker_key == "terra"
    assert row.reasoning_effort == "high"
    assert row.updated_by_user_id == 99
