"""TOML parsing and validated registry composition boundary."""

import tomllib
from collections.abc import Mapping
from pathlib import Path

from pydantic import ValidationError

from vuzol.config.models import RegistryDocument
from vuzol.config.registries import (
    ConfigurationBundle,
    ProfileRegistry,
    ProjectRegistry,
    RegistryError,
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
) -> ConfigurationBundle:
    """Validate cross-references, paths, fallbacks, and required secrets."""

    try:
        projects = ProjectRegistry(document.projects, repository_root=settings.repository_root)
        profiles = ProfileRegistry(document.profiles)
        topics = TopicRegistry(document.topics, projects=projects)
        resolver = ScopedSecretResolver(
            access_policy=_secret_access_policy(document, settings),
            secret_file_root=settings.secret_file_root,
            environment=environment,
        )
        for profile in profiles.items():
            if profile.enabled and profile.credential_required:
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
            revision=content_revision(document),
        )
    except (RegistryError, SecretResolutionError) as error:
        raise ConfigurationLoadError(str(error)) from error
