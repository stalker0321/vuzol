"""TOML parsing and validated registry composition boundary."""

import tomllib
from collections.abc import Mapping
from pathlib import Path

from pydantic import ValidationError

from vuzol.config.models import RegistryDocument, SandboxNetworkMode
from vuzol.config.registries import (
    ConfigurationBundle,
    ProfileRegistry,
    ProjectRegistry,
    RegistryError,
    SandboxRegistry,
    TopicRegistry,
)
from vuzol.config.revision import content_revision
from vuzol.config.secrets import ScopedSecretResolver, SecretResolutionError
from vuzol.config.settings import Settings


class ConfigurationLoadError(ValueError):
    """Configuration file or cross-registry validation failed."""


def load_document(path: Path) -> RegistryDocument:
    """Parse TOML into strict provider-neutral models."""

    try:
        with path.open("rb") as config_file:
            raw = tomllib.load(config_file)
        return RegistryDocument.model_validate(raw)
    except (OSError, tomllib.TOMLDecodeError, ValidationError) as error:
        raise ConfigurationLoadError(f"invalid registry file {path}: {error}") from error


def _secret_access_policy(
    document: RegistryDocument, settings: Settings
) -> dict[str, frozenset[str]]:
    policy: dict[str, set[str]] = {}

    def allow(reference: str, consumer: str) -> None:
        policy.setdefault(reference, set()).add(consumer)

    for profile in document.profiles:
        if profile.credential_reference is not None:
            allow(profile.credential_reference, f"profile:{profile.id}")
    if settings.database_dsn_reference is not None:
        allow(settings.database_dsn_reference, "system:database")
    if settings.telegram_bot_token_reference is not None:
        allow(settings.telegram_bot_token_reference, "system:telegram")
    return {reference: frozenset(consumers) for reference, consumers in policy.items()}


def build_bundle(
    document: RegistryDocument,
    settings: Settings,
    *,
    environment: Mapping[str, str] | None = None,
    validate_profile_credentials: bool = True,
) -> ConfigurationBundle:
    """Validate cross-references, paths, fallbacks, and required secrets."""

    try:
        projects = ProjectRegistry(document.projects, repository_root=settings.repository_root)
        profiles = ProfileRegistry(document.profiles)
        sandboxes = SandboxRegistry(document.sandboxes)
        for project in projects.items():
            if project.enabled:
                sandbox = sandboxes.get(project.sandbox_profile)
                if not sandbox.enabled:
                    raise RegistryError(f"project {project.id} references disabled sandbox")
                networked = sandbox.network_mode is SandboxNetworkMode.HTTPS_PROXY
                if project.network.enabled != networked:
                    raise RegistryError(
                        f"project {project.id} network policy does not match its sandbox"
                    )
                if project.validation_sandbox_profile is not None:
                    validation = sandboxes.get(project.validation_sandbox_profile)
                    if not validation.enabled:
                        raise RegistryError(
                            f"project {project.id} references disabled validation sandbox"
                        )
                    if validation.network_mode is not SandboxNetworkMode.NONE:
                        raise RegistryError(
                            f"project {project.id} validation sandbox must disable networking"
                        )
                    if (validation.uid, validation.gid) != (sandbox.uid, sandbox.gid):
                        raise RegistryError(
                            f"project {project.id} validation sandbox identity "
                            "must match its sandbox"
                        )
        topics = TopicRegistry(document.topics, projects=projects)
        resolver = ScopedSecretResolver(
            access_policy=_secret_access_policy(document, settings),
            secret_file_root=settings.secret_file_root,
            environment=environment,
        )
        for profile in profiles.items():
            if validate_profile_credentials and profile.enabled and profile.credential_required:
                assert profile.credential_reference is not None
                resolver.get(profile.credential_reference, f"profile:{profile.id}")
        if settings.database_dsn_reference is not None:
            resolver.get(settings.database_dsn_reference, "system:database")
        if settings.telegram_bot_token_reference is not None:
            resolver.get(settings.telegram_bot_token_reference, "system:telegram")
        return ConfigurationBundle(
            projects=projects,
            profiles=profiles,
            topics=topics,
            sandboxes=sandboxes,
            revision=content_revision(document),
        )
    except (RegistryError, SecretResolutionError) as error:
        raise ConfigurationLoadError(str(error)) from error
