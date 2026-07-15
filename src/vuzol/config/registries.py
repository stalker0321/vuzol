"""Immutable validated registries used by policy and business modules."""

from collections.abc import Iterable
from pathlib import Path

from pydantic import BaseModel, ConfigDict

from vuzol.config.models import (
    Capability,
    ProjectConfig,
    ProviderProfileConfig,
    SandboxProfileConfig,
    TopicConfig,
    TopicKind,
)
from vuzol.config.revision import (
    RunConfigurationSnapshot,
    SnapshotCompatibility,
    content_revision,
)


class RegistryError(ValueError):
    """Precise configuration validation failure."""


def _unique_by_id[T: ProjectConfig | ProviderProfileConfig | SandboxProfileConfig](
    entries: Iterable[T], *, kind: str
) -> dict[str, T]:
    result: dict[str, T] = {}
    for entry in entries:
        if entry.id in result:
            raise RegistryError(f"duplicate {kind} ID: {entry.id}")
        result[entry.id] = entry
    return result


class ProjectRegistry:
    def __init__(self, projects: Iterable[ProjectConfig], *, repository_root: Path) -> None:
        self._repository_root = repository_root.resolve()
        configured = _unique_by_id(projects, kind="project")
        self._projects = {
            project_id: self._normalize_project(project)
            for project_id, project in configured.items()
        }

    def _normalize_project(self, project: ProjectConfig) -> ProjectConfig:
        path = project.repository_path
        normalized = path if path.is_absolute() else self._repository_root / path
        normalized = normalized.resolve()
        try:
            normalized.relative_to(self._repository_root)
        except ValueError as error:
            raise RegistryError(
                f"project {project.id} path escapes repository root: {path}"
            ) from error
        if project.enabled and not normalized.is_dir():
            raise RegistryError(f"enabled project {project.id} path does not exist: {normalized}")
        summary = project.summary_path
        if summary is not None:
            summary = summary if summary.is_absolute() else normalized / summary
            summary = summary.resolve()
            try:
                summary.relative_to(normalized)
            except ValueError as error:
                raise RegistryError(
                    f"project {project.id} summary path escapes repository"
                ) from error
        return project.model_copy(update={"repository_path": normalized, "summary_path": summary})

    def get(self, project_id: str) -> ProjectConfig:
        try:
            return self._projects[project_id]
        except KeyError as error:
            raise RegistryError(f"unknown project ID: {project_id}") from error

    def items(self) -> tuple[ProjectConfig, ...]:
        return tuple(self._projects.values())


class ProfileRegistry:
    def __init__(self, profiles: Iterable[ProviderProfileConfig]) -> None:
        self._profiles = _unique_by_id(profiles, kind="profile")
        self._validate_fallbacks()
        self._validate_cli_isolation()

    def _validate_fallbacks(self) -> None:
        for profile in self._profiles.values():
            for fallback_id in profile.fallback_profile_ids:
                if fallback_id not in self._profiles:
                    raise RegistryError(f"profile {profile.id} has unknown fallback: {fallback_id}")
                fallback = self._profiles[fallback_id]
                if not profile.roles.intersection(fallback.roles):
                    raise RegistryError(
                        f"profile {profile.id} fallback {fallback_id} has no overlapping role"
                    )
                if not profile.supported_task_types.intersection(fallback.supported_task_types):
                    raise RegistryError(
                        f"profile {profile.id} fallback {fallback_id} has no overlapping task type"
                    )

        visiting: set[str] = set()
        visited: set[str] = set()

        def visit(profile_id: str) -> None:
            if profile_id in visiting:
                raise RegistryError(f"fallback cycle includes profile: {profile_id}")
            if profile_id in visited:
                return
            visiting.add(profile_id)
            for fallback_id in self._profiles[profile_id].fallback_profile_ids:
                visit(fallback_id)
            visiting.remove(profile_id)
            visited.add(profile_id)

        for profile_id in self._profiles:
            visit(profile_id)

    def _validate_cli_isolation(self) -> None:
        cli_profiles = [
            profile
            for profile in self._profiles.values()
            if profile.enabled and profile.launch_mode.value == "cli"
        ]
        identities: dict[str, str] = {}
        directories: dict[Path, str] = {}
        for profile in cli_profiles:
            assert profile.runtime_identity is not None
            assert profile.state_directory is not None
            if owner := identities.get(profile.runtime_identity):
                raise RegistryError(f"CLI profiles {owner} and {profile.id} share runtime identity")
            identities[profile.runtime_identity] = profile.id
            normalized = profile.state_directory.resolve()
            for directory, owner in directories.items():
                if (
                    normalized == directory
                    or normalized in directory.parents
                    or directory in normalized.parents
                ):
                    raise RegistryError(
                        f"CLI profiles {owner} and {profile.id} have overlapping state directories"
                    )
            directories[normalized] = profile.id

    def get(self, profile_id: str) -> ProviderProfileConfig:
        try:
            return self._profiles[profile_id]
        except KeyError as error:
            raise RegistryError(f"unknown profile ID: {profile_id}") from error

    def find_candidates(
        self, required_capabilities: frozenset[Capability]
    ) -> tuple[ProviderProfileConfig, ...]:
        return tuple(
            profile
            for profile in self._profiles.values()
            if profile.enabled and required_capabilities.issubset(profile.capabilities)
        )

    def items(self) -> tuple[ProviderProfileConfig, ...]:
        return tuple(self._profiles.values())


class SandboxRegistry:
    def __init__(self, sandboxes: Iterable[SandboxProfileConfig]) -> None:
        configured = _unique_by_id(sandboxes, kind="sandbox")
        self._sandboxes = configured

    def get(self, sandbox_id: str) -> SandboxProfileConfig:
        try:
            return self._sandboxes[sandbox_id]
        except KeyError as error:
            raise RegistryError(f"unknown sandbox ID: {sandbox_id}") from error

    def items(self) -> tuple[SandboxProfileConfig, ...]:
        return tuple(self._sandboxes.values())


class TopicRegistry:
    def __init__(self, topics: Iterable[TopicConfig], *, projects: ProjectRegistry) -> None:
        self._topics: dict[tuple[int, int], TopicConfig] = {}
        self._system_topics: dict[tuple[int, TopicKind], TopicConfig] = {}
        for topic in topics:
            key = (topic.chat_id, topic.message_thread_id)
            if key in self._topics:
                raise RegistryError(f"duplicate topic mapping: {key[0]}/{key[1]}")
            if topic.project_id is not None:
                projects.get(topic.project_id)
            self._topics[key] = topic
            if topic.kind is not TopicKind.PROJECT:
                system_key = (topic.chat_id, topic.kind)
                if system_key in self._system_topics:
                    raise RegistryError(
                        f"duplicate {topic.kind.value} topic for chat: {topic.chat_id}"
                    )
                self._system_topics[system_key] = topic

    def resolve(self, chat_id: int, message_thread_id: int) -> TopicConfig:
        try:
            return self._topics[(chat_id, message_thread_id)]
        except KeyError as error:
            raise RegistryError(f"unknown topic mapping: {chat_id}/{message_thread_id}") from error

    def items(self) -> tuple[TopicConfig, ...]:
        return tuple(self._topics.values())

    def system_topic(self, chat_id: int, kind: TopicKind) -> TopicConfig | None:
        """Return the one configured global topic of a kind for a chat, if present."""

        if kind is TopicKind.PROJECT:
            raise RegistryError("project topics must be resolved by stable thread ID")
        return self._system_topics.get((chat_id, kind))


class ConfigurationBundle(BaseModel):
    """Validated registries with a stable non-secret revision."""

    model_config = ConfigDict(arbitrary_types_allowed=True, frozen=True)

    projects: ProjectRegistry
    profiles: ProfileRegistry
    topics: TopicRegistry
    sandboxes: SandboxRegistry
    revision: str

    def snapshot(
        self, *, project_id: str | None = None, profile_id: str | None = None
    ) -> RunConfigurationSnapshot:
        project = self.projects.get(project_id) if project_id is not None else None
        profile = self.profiles.get(profile_id) if profile_id is not None else None
        sandbox = self.sandboxes.get(project.sandbox_profile) if project is not None else None
        return RunConfigurationSnapshot(
            bundle_revision=self.revision,
            project=project,
            profile=profile,
            sandbox=sandbox,
            project_revision=content_revision(project) if project is not None else None,
            profile_revision=content_revision(profile) if profile is not None else None,
            sandbox_revision=content_revision(sandbox) if sandbox is not None else None,
        )

    def evaluate(self, snapshot: RunConfigurationSnapshot) -> SnapshotCompatibility:
        reasons: list[str] = []
        if snapshot.project is not None:
            try:
                current_project = self.projects.get(snapshot.project.id)
                if not current_project.enabled:
                    reasons.append(f"project disabled: {current_project.id}")
                if current_project.repository_path != snapshot.project.repository_path:
                    reasons.append(f"project repository changed: {current_project.id}")
                if current_project.sandbox_profile != snapshot.project.sandbox_profile:
                    reasons.append(f"project sandbox policy changed: {current_project.id}")
                if (
                    current_project.validation_sandbox_profile
                    != snapshot.project.validation_sandbox_profile
                ):
                    reasons.append(f"project validation sandbox changed: {current_project.id}")
                if current_project.network != snapshot.project.network:
                    reasons.append(f"project network policy changed: {current_project.id}")
                if current_project.git_delivery != snapshot.project.git_delivery:
                    reasons.append(f"project delivery policy changed: {current_project.id}")
                removed = (
                    snapshot.project.allowed_capabilities - current_project.allowed_capabilities
                )
                if removed:
                    reasons.append(f"project capabilities revoked: {sorted(removed)}")
            except RegistryError:
                reasons.append(f"project removed: {snapshot.project.id}")
        if snapshot.profile is not None:
            try:
                current_profile = self.profiles.get(snapshot.profile.id)
                if not current_profile.enabled:
                    reasons.append(f"profile disabled: {current_profile.id}")
                removed = snapshot.profile.capabilities - current_profile.capabilities
                if removed:
                    reasons.append(f"profile capabilities revoked: {sorted(removed)}")
                removed_roles = snapshot.profile.roles - current_profile.roles
                if removed_roles:
                    reasons.append(f"profile roles revoked: {sorted(removed_roles)}")
                if current_profile.credential_reference != snapshot.profile.credential_reference:
                    reasons.append(f"profile credential reference changed: {current_profile.id}")
                if current_profile.sandbox_required != snapshot.profile.sandbox_required:
                    reasons.append(f"profile sandbox policy changed: {current_profile.id}")
                if current_profile.runtime_identity != snapshot.profile.runtime_identity:
                    reasons.append(f"profile runtime identity changed: {current_profile.id}")
                if current_profile.state_directory != snapshot.profile.state_directory:
                    reasons.append(f"profile state directory changed: {current_profile.id}")
                accounting_fields = (
                    "input_cost_units_per_million",
                    "output_cost_units_per_million",
                    "quota_units_per_call",
                    "minimum_unknown_usage_cost",
                )
                if any(
                    getattr(current_profile, field) != getattr(snapshot.profile, field)
                    for field in accounting_fields
                ):
                    reasons.append(f"profile accounting policy changed: {current_profile.id}")
            except RegistryError:
                reasons.append(f"profile removed: {snapshot.profile.id}")
        if snapshot.sandbox is not None:
            try:
                current_sandbox = self.sandboxes.get(snapshot.sandbox.id)
                if not current_sandbox.enabled:
                    reasons.append(f"sandbox disabled: {current_sandbox.id}")
                if current_sandbox != snapshot.sandbox:
                    reasons.append(f"sandbox policy changed: {current_sandbox.id}")
            except RegistryError:
                reasons.append(f"sandbox removed: {snapshot.sandbox.id}")
        return SnapshotCompatibility(allowed=not reasons, reasons=tuple(reasons))
