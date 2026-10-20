"""Explicit account sharing never weakens default CLI isolation."""

from pathlib import Path

import pytest
from pydantic import ValidationError

from vuzol.config import ProfileRegistry, ProviderProfileConfig, RegistryError


def profile(profile_id: str = "first", **changes: object) -> ProviderProfileConfig:
    return ProviderProfileConfig.model_validate(
        {
            "id": profile_id,
            "provider": "openai-compatible",
            "model": "model",
            "launch_mode": "api",
            "api_base_url": "https://provider.example/v1",
            "credential_reference": "env:SHARED_KEY",
            "capabilities": frozenset(),
            "concurrency_limit": 1,
            "cost_class": "balanced",
            "roles": frozenset({"executor"}),
            "supported_task_types": frozenset({"general"}),
            **changes,
        }
    )


def cli(tmp_path: Path, profile_id: str = "first", **changes: object) -> ProviderProfileConfig:
    return profile(
        profile_id,
        **{
            "provider": "pi",
            "launch_mode": "cli",
            "api_base_url": None,
            "credential_reference": None,
            "credential_required": False,
            "runtime_identity": "account-user",
            "state_directory": tmp_path / "account-state",
            **changes,
        },
    )


def test_account_id_is_optional_bounded_and_non_secret() -> None:
    assert profile().account_id is None
    assert profile(account_id="team_account-1").account_id == "team_account-1"
    for invalid in ("", "secret:token", "has spaces", "a" * 101, "x\n"):
        with pytest.raises(ValidationError):
            profile(account_id=invalid)


def test_explicit_account_shares_exact_cli_state_across_models_and_roles(tmp_path: Path) -> None:
    first = cli(tmp_path, account_id="shared")
    second = cli(
        tmp_path,
        "second",
        account_id="shared",
        model="another-model",
        model_reasoning_effort="high",
        roles=frozenset({"reviewer"}),
    )
    registry = ProfileRegistry((first, second))
    assert registry.items() == (first, second)
    assert first.provider == second.provider == "pi"
    assert first.account_binding == second.account_binding


@pytest.mark.parametrize("account_id", [None, "different"])
def test_sharing_requires_explicit_same_account_on_both_profiles(
    tmp_path: Path, account_id: str | None
) -> None:
    with pytest.raises(RegistryError, match="share runtime identity"):
        ProfileRegistry(
            (cli(tmp_path, account_id="shared"), cli(tmp_path, "second", account_id=account_id))
        )


@pytest.mark.parametrize(
    "changes",
    [
        {"provider": "codex"},
        {"launch_mode": "tool", "runtime_identity": None, "state_directory": None},
        {"credential_reference": "env:OTHER_KEY"},
        {"credential_required": True, "credential_reference": "env:OTHER_KEY"},
        {"runtime_identity": "other-account"},
        {"state_directory": Path("/another/account")},
        {"concurrency_limit": 2},
        {
            "runtime_network": {
                "enabled": True,
                "destinations": [{"url": "https://example.com", "purpose": "provider"}],
            }
        },
        {"api_base_url": "https://another.example/v1"},
    ],
)
def test_same_account_cannot_bind_different_actual_accounts(
    tmp_path: Path, changes: dict[str, object]
) -> None:
    with pytest.raises(RegistryError, match="incompatible account bindings"):
        ProfileRegistry(
            (
                cli(tmp_path, account_id="shared"),
                cli(tmp_path, "second", account_id="shared", **changes),
            )
        )


@pytest.mark.parametrize("enabled", [True, False])
def test_subdirectory_is_never_an_account_sharing_exception(tmp_path: Path, enabled: bool) -> None:
    with pytest.raises(RegistryError, match="incompatible account bindings"):
        ProfileRegistry(
            (
                cli(tmp_path, account_id="shared"),
                cli(
                    tmp_path,
                    "second",
                    account_id="shared",
                    state_directory=tmp_path / "account-state" / "nested",
                    enabled=enabled,
                ),
            )
        )


def test_resolved_state_alias_is_exact_but_cannot_bypass_default_isolation(tmp_path: Path) -> None:
    state = tmp_path / "account-state"
    state.mkdir()
    alias = tmp_path / "alias"
    alias.symlink_to(state, target_is_directory=True)
    first = cli(tmp_path, account_id="shared")
    second = cli(tmp_path, "second", account_id="shared", state_directory=alias)
    ProfileRegistry((first, second))
    with pytest.raises(RegistryError, match="overlapping state directories"):
        ProfileRegistry(
            (
                first,
                cli(tmp_path, "third", runtime_identity="other", state_directory=alias),
            )
        )


def test_api_accounts_match_credentials_endpoint_and_limit() -> None:
    first = profile("first", account_id="shared", credential_reference="env:SHARED_KEY")
    second = profile("second", account_id="shared", credential_reference="env:SHARED_KEY")
    ProfileRegistry((first, second))
    for changes in (
        {"credential_reference": "env:OTHER_KEY"},
        {"api_base_url": "https://other.example/v1"},
        {"concurrency_limit": 2},
        {"credential_required": False},
    ):
        with pytest.raises(RegistryError, match="incompatible account bindings"):
            ProfileRegistry((first, second.model_copy(update=changes)))
