"""One descriptor -> many profiles, with conflict and legacy-pin protection."""

import json
from pathlib import Path

import pytest
from tests.unit.providers._test_subscription_limits_helpers import _cli_profile

from vuzol.config.loader import ConfigurationLoadError, load_document
from vuzol.config.models import AccountConfig, RegistryDocument
from vuzol.config.registries import ProfileRegistry
from vuzol.config.revision import content_revision


def descriptor(tmp_path: Path, **changes: object) -> dict[str, object]:
    return {
        "id": "test-go",
        "service": "opencode-go",
        "provider": "pi",
        "launch_mode": "cli",
        "state_directory": str(tmp_path / "state"),
        "runtime_identity": "test-go",
        "auth_method": "static_api_key",
        "quota_collector": "opencode_go",
        "quota_scope_id": "go:account-meter:test",
        "concurrency_limit": 2,
        "control_reserved_slots": 1,
        **changes,
    }


def document(tmp_path: Path, **changes: object) -> Path:
    path = tmp_path / "registry.json"
    path.write_text(
        json.dumps(
            {
                "accounts": [descriptor(tmp_path)],
                "profiles": [
                    {
                        "id": "worker",
                        "account_ref": "test-go",
                        "model": "one",
                        "capabilities": [],
                        "roles": ["executor"],
                        "supported_task_types": ["coding"],
                        "cost_class": "balanced",
                        **changes,
                    },
                    {"id": "lead", "base_profile_id": "worker", "roles": ["lead"], "model": "two"},
                ],
            }
        )
    )
    return path


def test_descriptor_compiles_through_real_loader_and_registry(tmp_path: Path) -> None:
    loaded = load_document(document(tmp_path))
    profiles = ProfileRegistry(loaded.profiles).items()
    assert profiles[0].account_binding == profiles[1].account_binding
    assert profiles[0].account_id == "test-go"
    assert profiles[0].concurrency_limit == 2
    assert profiles[1].control_reserved_slots == 1
    assert profiles[0].model != profiles[1].model


@pytest.mark.parametrize(
    "changes",
    [
        {"concurrency_limit": 5},
        {"provider": "codex"},
        {"account_id": "other"},
        {"account_ref": "missing"},
        {"account": {}},
    ],
)
def test_conflicts_are_not_last_overlay_wins(tmp_path: Path, changes: dict[str, object]) -> None:
    with pytest.raises(ConfigurationLoadError):
        load_document(document(tmp_path, **changes))


def test_legacy_profile_revision_is_unchanged(tmp_path: Path) -> None:
    profile = _cli_profile("legacy", "pi", tmp_path)
    raw = profile.model_dump(mode="python")
    for key in ("account", "control_reserved_slots", "subscription_limits_visible"):
        raw.pop(key)
    assert content_revision(profile) == content_revision(raw)
    registry = RegistryDocument(profiles=(profile,))
    legacy = registry.model_dump(mode="python")
    legacy.pop("accounts")
    legacy["profiles"][0].pop("account")
    assert content_revision(registry) == content_revision(legacy)


def test_visibility_and_collector_metadata_do_not_invalidate_execution(tmp_path: Path) -> None:
    profile = load_document(document(tmp_path)).profiles[0]
    assert profile.account is not None
    changed = profile.model_copy(
        update={
            "account": profile.account.model_copy(
                update={"quota_scope_id": "other", "subscription_limits_visible": False}
            )
        }
    )
    assert content_revision(profile) == content_revision(changed)


@pytest.mark.parametrize(
    "changes",
    [
        {"state_directory": "/home/owner/.pi"},
        {
            "auth_method": "cli_oauth",
            "auth_owner": "serialized_cli",
            "concurrency_limit": 1,
            "control_reserved_slots": 0,
        },
        {"service": "opencode-payg"},
        {"quota_scope_id": None},
    ],
)
def test_unsupported_auth_and_wrong_product_fail_closed(
    tmp_path: Path, changes: dict[str, object]
) -> None:
    with pytest.raises(ValueError):
        AccountConfig.model_validate(descriptor(tmp_path, **changes))


def test_explicit_collector_must_match_runtime(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="does not match"):
        AccountConfig.model_validate(descriptor(tmp_path, quota_collector="codex"))


def test_shared_scope_between_distinct_descriptors_is_not_assumed(tmp_path: Path) -> None:
    path = document(tmp_path)
    raw = json.loads(path.read_text())
    raw["accounts"].append(descriptor(tmp_path, id="other-account"))
    path.write_text(json.dumps(raw))
    with pytest.raises(ConfigurationLoadError, match="shared quota pools"):
        load_document(path)
