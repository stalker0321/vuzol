"""Stable non-secret configuration revisions and run snapshots."""

import hashlib
import json
from collections.abc import Mapping
from enum import Enum
from pathlib import Path
from typing import Any

from pydantic import AnyUrl, BaseModel, ConfigDict

from vuzol.config.models import ProjectConfig, ProviderProfileConfig, SandboxProfileConfig


def content_revision(value: BaseModel | Mapping[str, Any]) -> str:
    """Hash normalized non-secret configuration content.

    Sets/frozensets are sorted so the digest is stable across process starts even
    when PYTHONHASHSEED randomizes set iteration order.
    """

    if isinstance(value, BaseModel):
        raw: object = value.model_dump(mode="python")
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
