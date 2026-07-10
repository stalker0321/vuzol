"""Stable non-secret configuration revisions and run snapshots."""

import hashlib
import json
from collections.abc import Mapping
from typing import Any

from pydantic import BaseModel, ConfigDict

from vuzol.config.models import ProjectConfig, ProviderProfileConfig


def content_revision(value: BaseModel | Mapping[str, Any]) -> str:
    """Hash normalized non-secret configuration content."""

    normalized = value.model_dump(mode="json") if isinstance(value, BaseModel) else dict(value)
    payload = json.dumps(normalized, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(payload.encode()).hexdigest()


class RunConfigurationSnapshot(BaseModel):
    """Immutable policy inputs retained by a future run record."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    bundle_revision: str
    project: ProjectConfig | None
    profile: ProviderProfileConfig | None
    project_revision: str | None
    profile_revision: str | None


class SnapshotCompatibility(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    allowed: bool
    reasons: tuple[str, ...] = ()
