from pathlib import Path

import pytest

from vuzol.config import (
    Capability,
    ConfigurationBundle,
    ProfileRegistry,
    ProjectConfig,
    ProjectRegistry,
    ProviderProfileConfig,
    RegistryError,
    TopicConfig,
    TopicKind,
    TopicRegistry,
)


def project(path: Path, **changes: object) -> ProjectConfig:
    values: dict[str, object] = {
        "id": "project-a",
        "display_name": "Project A",
        "repository_path": path,
        "default_branch": "main",
        "allowed_capabilities": frozenset({Capability.REPOSITORY_READ, Capability.CODE_EDIT}),
        "sandbox_profile": "project-default",
    }
    values.update(changes)
    return ProjectConfig.model_validate(values)


def profile(profile_id: str = "profile-a", **changes: object) -> ProviderProfileConfig:
    values: dict[str, object] = {
        "id": profile_id,
        "provider": "provider",
        "model": "model",
        "api_base_url": "https://provider.example/v1",
        "launch_mode": "api",
        "credential_reference": f"env:{profile_id.upper().replace('-', '_')}_KEY",
        "capabilities": frozenset({Capability.REPOSITORY_READ}),
        "concurrency_limit": 1,
        "cost_class": "balanced",
        "roles": frozenset({"executor"}),
        "supported_task_types": frozenset({"general"}),
    }
    values.update(changes)
    return ProviderProfileConfig.model_validate(values)


def test_project_paths_are_normalized_and_constrained(tmp_path: Path) -> None:
    repository = tmp_path / "repositories" / "project-a"
    repository.mkdir(parents=True)
    registry = ProjectRegistry(
        [project(Path("project-a"), summary_path=Path("docs/summary.md"))],
        repository_root=tmp_path / "repositories",
    )

    configured = registry.get("project-a")
    assert configured.repository_path == repository.resolve()
    assert configured.summary_path == (repository / "docs/summary.md").resolve()

    with pytest.raises(RegistryError, match="escapes repository root"):
        ProjectRegistry(
            [project(Path("../outside"), enabled=False)],
            repository_root=tmp_path / "repositories",
        )


def test_enabled_project_path_must_exist(tmp_path: Path) -> None:
    with pytest.raises(RegistryError, match="path does not exist"):
        ProjectRegistry([project(Path("missing"))], repository_root=tmp_path)


def test_duplicate_ids_are_rejected(tmp_path: Path) -> None:
    duplicate = project(Path("unused"), enabled=False)
    with pytest.raises(RegistryError, match="duplicate project ID"):
        ProjectRegistry([duplicate, duplicate], repository_root=tmp_path)

    duplicate_profile = profile()
    with pytest.raises(RegistryError, match="duplicate profile ID"):
        ProfileRegistry([duplicate_profile, duplicate_profile])


def test_fallbacks_reject_unknown_ids_and_cycles() -> None:
    with pytest.raises(RegistryError, match="unknown fallback"):
        ProfileRegistry([profile(fallback_profile_ids=("missing",))])

    with pytest.raises(RegistryError, match="fallback cycle"):
        ProfileRegistry(
            [
                profile("profile-a", fallback_profile_ids=("profile-b",)),
                profile("profile-b", fallback_profile_ids=("profile-a",)),
            ]
        )


def test_cli_profiles_require_distinct_identity_and_state_paths(tmp_path: Path) -> None:
    def cli(profile_id: str, identity: str, directory: Path) -> ProviderProfileConfig:
        return profile(
            profile_id,
            provider="codex",
            api_base_url=None,
            launch_mode="cli",
            credential_reference=None,
            credential_required=False,
            runtime_identity=identity,
            state_directory=directory,
        )

    with pytest.raises(RegistryError, match="share runtime identity"):
        ProfileRegistry(
            [
                cli("codex-a", "codex", tmp_path / "a"),
                cli("codex-b", "codex", tmp_path / "b"),
            ]
        )
    with pytest.raises(RegistryError, match="overlapping state directories"):
        ProfileRegistry(
            [
                cli("codex-a", "codex-a", tmp_path / "codex"),
                cli("codex-b", "codex-b", tmp_path / "codex" / "nested"),
            ]
        )


def test_profile_capability_matching_ignores_disabled_profiles() -> None:
    registry = ProfileRegistry(
        [
            profile("reader"),
            profile(
                "coder",
                capabilities=frozenset({Capability.REPOSITORY_READ, Capability.CODE_EDIT}),
            ),
            profile("disabled", enabled=False, credential_required=False),
        ]
    )

    assert [candidate.id for candidate in registry.find_candidates(frozenset())] == [
        "reader",
        "coder",
    ]
    assert [
        candidate.id for candidate in registry.find_candidates(frozenset({Capability.CODE_EDIT}))
    ] == ["coder"]
    with pytest.raises(RegistryError, match="unknown profile ID"):
        registry.get("missing")


def test_topic_registry_validates_project_and_duplicate_mapping(tmp_path: Path) -> None:
    projects = ProjectRegistry(
        [project(Path("project-a"), enabled=False)], repository_root=tmp_path
    )
    topic = TopicConfig(
        chat_id=-100,
        message_thread_id=5,
        kind=TopicKind.PROJECT,
        project_id="project-a",
        default_workflow="coding_task",
    )
    registry = TopicRegistry([topic], projects=projects)
    assert registry.resolve(-100, 5) == topic

    with pytest.raises(RegistryError, match="duplicate topic mapping"):
        TopicRegistry([topic, topic], projects=projects)
    with pytest.raises(RegistryError, match="unknown project ID"):
        TopicRegistry([topic.model_copy(update={"project_id": "missing"})], projects=projects)
    with pytest.raises(RegistryError, match="unknown topic mapping"):
        registry.resolve(-100, 9)


def test_snapshot_preserves_non_security_configuration_and_detects_revocation(
    tmp_path: Path,
) -> None:
    (tmp_path / "project-a").mkdir()
    original_project = project(Path("project-a"))
    original_profile = profile()

    def bundle(
        configured_project: ProjectConfig, configured_profile: ProviderProfileConfig
    ) -> ConfigurationBundle:
        projects = ProjectRegistry([configured_project], repository_root=tmp_path)
        profiles = ProfileRegistry([configured_profile])
        topics = TopicRegistry([], projects=projects)
        return ConfigurationBundle(
            projects=projects, profiles=profiles, topics=topics, revision="revision"
        )

    original = bundle(original_project, original_profile)
    snapshot = original.snapshot(project_id="project-a", profile_id="profile-a")

    renamed = bundle(
        original_project.model_copy(update={"display_name": "Renamed"}), original_profile
    )
    assert renamed.evaluate(snapshot).allowed
    assert snapshot.project is not None and snapshot.project.display_name == "Project A"

    revoked = bundle(
        original_project.model_copy(
            update={"allowed_capabilities": frozenset(), "sandbox_profile": "restricted"}
        ),
        original_profile.model_copy(
            update={"enabled": False, "sandbox_required": False, "roles": frozenset()}
        ),
    ).evaluate(snapshot)
    assert not revoked.allowed
    assert any("capabilities revoked" in reason for reason in revoked.reasons)
    assert any("sandbox policy changed" in reason for reason in revoked.reasons)
    assert any("profile disabled" in reason for reason in revoked.reasons)
    assert any("profile roles revoked" in reason for reason in revoked.reasons)
