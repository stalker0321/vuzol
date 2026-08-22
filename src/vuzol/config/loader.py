"""TOML parsing and validated registry composition boundary."""

import json
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
        if path.suffix == ".json":
            raw = json.loads(path.read_text())
        else:
            with path.open("rb") as config_file:
                raw = tomllib.load(config_file)
        return RegistryDocument.model_validate(_resolve_profile_inheritance(raw))
    except ConfigurationLoadError:
        raise
    except (OSError, json.JSONDecodeError, tomllib.TOMLDecodeError, ValidationError) as error:
        raise ConfigurationLoadError(f"invalid registry file {path}: {error}") from error


def _resolve_profile_inheritance(raw: object) -> object:
    """Flatten ``base_profile_id`` chains before validation.

    Merging happens on raw tables so an unset child key genuinely inherits the
    base value instead of silently falling back to a model default.
    """

    if not isinstance(raw, dict):
        return raw
    profiles = raw.get("profiles")
    if not isinstance(profiles, list):
        return raw
    by_id: dict[str, dict[str, object]] = {}
    for entry in profiles:
        if not isinstance(entry, dict):
            raise ConfigurationLoadError("profile entries must be tables")
        profile_id = entry.get("id")
        if isinstance(profile_id, str):
            if profile_id in by_id:
                raise ConfigurationLoadError(f"duplicate profile id {profile_id!r}")
            by_id[profile_id] = entry

    def resolve(entry: dict[str, object], chain: tuple[str, ...]) -> dict[str, object]:
        base_id = entry.get("base_profile_id")
        if base_id is None:
            return entry
        entry_id = entry.get("id")
        if not isinstance(base_id, str):
            raise ConfigurationLoadError(
                f"profile {entry_id!r} base_profile_id must be a string"
            )
        if base_id in chain:
            raise ConfigurationLoadError(
                f"profile inheritance cycle: {' -> '.join((*chain, base_id))}"
            )
        base_entry = by_id.get(base_id)
        if base_entry is None:
            raise ConfigurationLoadError(
                f"profile {entry_id!r} inherits unknown profile {base_id!r}"
            )
        merged = {**resolve(base_entry, (*chain, base_id)), **entry}
        merged.pop("base_profile_id", None)
        return merged

    return {**raw, "profiles": [resolve(entry, ()) for entry in profiles]}


def merge_documents(base: RegistryDocument, overlay: RegistryDocument) -> RegistryDocument:
    """Merge an append-only dynamic project/topic overlay into static configuration."""

    return RegistryDocument(
        projects=(*base.projects, *overlay.projects),
        profiles=(*base.profiles, *overlay.profiles),
        topics=(*base.topics, *overlay.topics),
        sandboxes=(*base.sandboxes, *overlay.sandboxes),
    )


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
        allow(settings.database_dsn_reference, "system:backup")
    # A distinct restore DSN is backup-only. A shared database/restore reference
    # naturally keeps the union of both consumers.
    if settings.backup.restore_dsn_reference is not None:
        allow(settings.backup.restore_dsn_reference, "system:backup")
    if settings.backup.kek_reference is not None:
        allow(settings.backup.kek_reference, "system:backup")
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
