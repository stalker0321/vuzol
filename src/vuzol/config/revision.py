"""Stable non-secret configuration revisions and run snapshots."""

import hashlib
import json
from collections.abc import Mapping
from enum import Enum
from pathlib import Path
from typing import Any

from pydantic import AnyUrl, BaseModel, ConfigDict

from vuzol.config.models import (
    ProjectConfig,
    ProviderProfileConfig,
    RegistryDocument,
    SandboxProfileConfig,
)


def content_revision(value: BaseModel | Mapping[str, Any]) -> str:
    """Hash normalized non-secret configuration content.

    Sets/frozensets are sorted so the digest is stable across process starts even
    when PYTHONHASHSEED randomizes set iteration order.
    """

    if isinstance(value, BaseModel):
        raw: object = value.model_dump(mode="python")
        if isinstance(value, RegistryDocument) and not value.accounts:
            assert isinstance(raw, dict)
            raw.pop("accounts", None)
            for profile in raw.get("profiles", ()):
                if profile.get("account") is None:
                    profile.pop("account", None)
        if isinstance(value, ProviderProfileConfig):
            assert isinstance(raw, dict)
            raw.pop("subscription_limits_visible", None)
            if value.account is None:
                raw.pop("account", None)  # Preserve all existing frozen profile revisions.
            else:
                account = raw["account"]
                assert isinstance(account, dict)
                for field in (
                    "subscription_limits_visible",
                    "quota_collector",
                    "quota_scope_id",
                    "quota_oauth",
                ):
                    account.pop(field, None)
        # An absent reservation retains the pre-control-lane profile identity.
        # Nonzero reservations are policy changes and do invalidate old pins.
        if isinstance(value, ProviderProfileConfig) and value.control_reserved_slots == 0:
            assert isinstance(raw, dict)
            raw.pop("control_reserved_slots", None)
    else:
        raw = dict(value)
    payload = json.dumps(
        _canonicalize(raw),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    )
    return hashlib.sha256(payload.encode()).hexdigest()


def _canonicalize(value: object) -> object:
    if isinstance(value, BaseModel):
        return _canonicalize(value.model_dump(mode="python"))
    if isinstance(value, Mapping):
        return {str(key): _canonicalize(item) for key, item in value.items()}
    if isinstance(value, (set, frozenset)):
        return sorted((_canonicalize(item) for item in value), key=_sort_key)
    if isinstance(value, tuple):
        return [_canonicalize(item) for item in value]
    if isinstance(value, list):
        # Preserve list/tuple order: these are intentional sequences.
        return [_canonicalize(item) for item in value]
    if isinstance(value, Enum):
        return _canonicalize(value.value)
    if isinstance(value, Path | AnyUrl):
        return str(value)
    if isinstance(value, bytes):
        return value.decode("utf-8", "replace")
    return value


def _sort_key(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


class RunConfigurationSnapshot(BaseModel):
    """Immutable policy inputs retained by a future run record."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    bundle_revision: str
    project: ProjectConfig | None
    profile: ProviderProfileConfig | None
    sandbox: SandboxProfileConfig | None
    project_revision: str | None
    profile_revision: str | None
    sandbox_revision: str | None


class SnapshotCompatibility(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    allowed: bool
    reasons: tuple[str, ...] = ()


# What a stored model choice means: which model, how it reasons, through which account
# and endpoint. Limits, prices, routing order, fallbacks, network policy, roles and
# capabilities are left out: they are checked when a profile is chosen, and editing them
# must not invalidate the owner's choices (2026-10-27: dropping a dead fallback did).
_ASSIGNMENT_FIELDS = frozenset(
    {
        "id",
        "account_id",
        "account",
        "provider",
        "model",
        "model_reasoning_effort",
        "max_reasoning_tokens",
        "reasoning_enabled",
        "api_base_url",
        "provider_routing",
        "launch_mode",
        "credential_reference",
        "runtime_identity",
        "state_directory",
    }
)
_ACCOUNT_QUOTA_FIELDS = (
    "subscription_limits_visible",
    "quota_collector",
    "quota_scope_id",
    "quota_oauth",
)


def assignment_revision(profile: ProviderProfileConfig) -> str:
    """Revision pinned by durable model assignments; see _ASSIGNMENT_FIELDS."""

    raw = profile.model_dump(mode="python", include=set(_ASSIGNMENT_FIELDS))
    if isinstance(raw.get("account"), dict):
        for field in _ACCOUNT_QUOTA_FIELDS:
            raw["account"].pop(field, None)
    return content_revision(raw)


def assignment_matches(profile: ProviderProfileConfig, revision: str | None) -> bool:
    """A pin made before assignment revisions existed carries the full revision."""

    return revision in {assignment_revision(profile), content_revision(profile)}
