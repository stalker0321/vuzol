"""Immutable validated registries used by policy and business modules."""

from collections.abc import Iterable
from pathlib import Path

from pydantic import BaseModel, ConfigDict

from vuzol.config.models import Capability, ProjectConfig, ProviderProfileConfig, TopicConfig
from vuzol.config.revision import (
    RunConfigurationSnapshot,
    SnapshotCompatibility,
    content_revision,
)


class RegistryError(ValueError):
    """Precise configuration validation failure."""


def _unique_by_id[T: ProjectConfig | ProviderProfileConfig](
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

    def _validate_fallbacks(self) -> None:
        for profile in self._profiles.values():
            for fallback_id in profile.fallback_profile_ids:
                if fallback_id not in self._profiles:
                    raise RegistryError(f"profile {profile.id} has unknown fallback: {fallback_id}")

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


class TopicRegistry:
    def __init__(self, topics: Iterable[TopicConfig], *, projects: ProjectRegistry) -> None:
        self._topics: dict[tuple[int, int], TopicConfig] = {}
        for topic in topics:
            key = (topic.chat_id, topic.message_thread_id)
            if key in self._topics:
                raise RegistryError(f"duplicate topic mapping: {key[0]}/{key[1]}")
            if topic.project_id is not None:
                projects.get(topic.project_id)
            self._topics[key] = topic

    def resolve(self, chat_id: int, message_thread_id: int) -> TopicConfig:
        try:
            return self._topics[(chat_id, message_thread_id)]
        except KeyError as error:
            raise RegistryError(f"unknown topic mapping: {chat_id}/{message_thread_id}") from error

    def items(self) -> tuple[TopicConfig, ...]:
        return tuple(self._topics.values())


class ConfigurationBundle(BaseModel):
    """Validated registries with a stable non-secret revision."""

    model_config = ConfigDict(arbitrary_types_allowed=True, frozen=True)

    projects: ProjectRegistry
    profiles: ProfileRegistry
    topics: TopicRegistry
    revision: str

    def snapshot(
        self, *, project_id: str | None = None, profile_id: str | None = None
    ) -> RunConfigurationSnapshot:
        project = self.projects.get(project_id) if project_id is not None else None
        profile = self.profiles.get(profile_id) if profile_id is not None else None
        return RunConfigurationSnapshot(
            bundle_revision=self.revision,
            project=project,
            profile=profile,
            project_revision=content_revision(project) if project is not None else None,
            profile_revision=content_revision(profile) if profile is not None else None,
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
                if current_profile.credential_reference != snapshot.profile.credential_reference:
                    reasons.append(f"profile credential reference changed: {current_profile.id}")
                if current_profile.sandbox_required != snapshot.profile.sandbox_required:
                    reasons.append(f"profile sandbox policy changed: {current_profile.id}")
            except RegistryError:
                reasons.append(f"profile removed: {snapshot.profile.id}")
        return SnapshotCompatibility(allowed=not reasons, reasons=tuple(reasons))
