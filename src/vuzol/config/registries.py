"""Immutable validated registries used by policy and business modules."""

from collections.abc import Iterable
from pathlib import Path

from pydantic import BaseModel, ConfigDict

from vuzol.config.models import (
    ProjectConfig,
    ProviderProfileConfig,
    SandboxProfileConfig,
    TopicConfig,
    TopicKind,
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
        self._validate_accounts()
        self._validate_cli_isolation()

    def _validate_accounts(self) -> None:
        accounts: dict[str, ProviderProfileConfig] = {}
        for profile in self._profiles.values():
            if profile.account_id is None:
                continue
            owner = accounts.setdefault(profile.account_id, profile)
            if owner.account_binding != profile.account_binding:
                raise RegistryError(
                    f"profiles {owner.id} and {profile.id} have incompatible account bindings"
                )

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
        identities: dict[str, ProviderProfileConfig] = {}
        directories: dict[Path, ProviderProfileConfig] = {}
        for profile in cli_profiles:
            assert profile.runtime_identity is not None
            assert profile.state_directory is not None
            if (owner := identities.get(profile.runtime_identity)) and (
                profile.account_id is None or profile.account_id != owner.account_id
            ):
                raise RegistryError(
                    f"CLI profiles {owner.id} and {profile.id} share runtime identity"
                )
            identities[profile.runtime_identity] = profile
            normalized = profile.state_directory.resolve()
            for directory, owner in directories.items():
                if (
                    normalized == directory
                    and profile.account_id is not None
                    and profile.account_id == owner.account_id
                ):
                    continue
                if (
                    normalized == directory
                    or normalized in directory.parents
                    or directory in normalized.parents
                ):
                    raise RegistryError(
                        f"CLI profiles {owner.id} and {profile.id} "
                        "have overlapping state directories"
                    )
            directories[normalized] = profile

    def get(self, profile_id: str) -> ProviderProfileConfig:
        try:
            return self._profiles[profile_id]
        except KeyError as error:
            raise RegistryError(f"unknown profile ID: {profile_id}") from error

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
