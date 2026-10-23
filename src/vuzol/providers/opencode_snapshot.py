"""Secret-free OpenCode usage transport from the auth owner to the executor."""

from __future__ import annotations

import hashlib
import json
import os
import stat
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from vuzol.config.models import ProviderProfileConfig
from vuzol.config.settings import OpenCodeSnapshotBinding
from vuzol.providers.opencode_usage import GoUsage
from vuzol.providers.quota import QuotaWindow

MAX_BYTES = 32_768
MAX_AGE = timedelta(minutes=15)


def execution_binding(profile: ProviderProfileConfig) -> str:
    """Bind a meter to the actual runtime account, independent of model and role."""
    raw = {
        "account_id": profile.account_id or profile.id,
        "provider": profile.provider,
        "launch_mode": profile.launch_mode,
        "runtime_identity": profile.runtime_identity,
        "state_directory": str(profile.state_directory),
        "api_base_url": str(profile.api_base_url) if profile.api_base_url else None,
    }
    return hashlib.sha256(json.dumps(raw, sort_keys=True).encode()).hexdigest()


class UsageSnapshot(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["vuzol-opencode-usage.v1"] = "vuzol-opencode-usage.v1"
    account_id: str = Field(min_length=1, max_length=100)
    binding_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    scope_id: str = Field(min_length=1, max_length=160)
    generated_at: datetime
    attempted_at: datetime
    observed_at: datetime | None = None
    detail: str = Field(default="", pattern=r"^[a-z_]*$", max_length=80)
    windows: tuple[QuotaWindow, ...] = Field(default=(), max_length=3)
    entitlement_observed_at: datetime | None = None

    @field_validator("generated_at", "attempted_at", "observed_at", "entitlement_observed_at")
    @classmethod
    def aware(cls, value: datetime | None) -> datetime | None:
        if value is not None and value.tzinfo is None:
            raise ValueError("snapshot timestamps must carry timezone")
        return value

    @model_validator(mode="after")
    def consistent(self) -> UsageSnapshot:
        if self.detail:
            if self.windows or self.observed_at is not None:
                raise ValueError("failed measurements cannot carry quota evidence")
        elif (
            self.observed_at is None
            or len(self.windows) != 3
            or {w.id for w in self.windows} != {"rolling", "weekly", "monthly"}
            or any(w.scope_id != self.scope_id for w in self.windows)
        ):
            raise ValueError("successful measurement requires all three bound windows")
        if self.entitlement_observed_at and self.detail != "go_entitlement_missing":
            raise ValueError("unexpected entitlement evidence")
        return self


def publish_usage_snapshot(path: Path, snapshot: UsageSnapshot) -> None:
    """Atomic replace in an owner-writable directory, readable by executor group."""
    parent = path.parent
    metadata = parent.stat()
    if parent.resolve() != parent or metadata.st_uid != os.geteuid() or metadata.st_mode & 0o022:
        raise ValueError("snapshot output directory permissions")
    payload = snapshot.model_dump_json().encode()
    if len(payload) > MAX_BYTES:
        raise ValueError("snapshot too large")
    fd, temporary = tempfile.mkstemp(prefix=".usage-", dir=parent)
    try:
        with os.fdopen(fd, "wb") as stream:
            os.fchmod(stream.fileno(), 0o640)
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        directory_fd = os.open(parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        Path(temporary).unlink(missing_ok=True)


def load_usage_snapshot(
    binding: OpenCodeSnapshotBinding, *, profile: ProviderProfileConfig, now: datetime
) -> GoUsage:
    """No credentials or network access, including on invalid or expired input."""
    try:
        if binding.path.resolve() != binding.path or binding.path.parent.stat().st_mode & 0o022:
            return GoUsage(now, detail="limits_snapshot_invalid")
        fd = os.open(binding.path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(fd, "rb") as stream:
            metadata = os.fstat(stream.fileno())
            if not stat.S_ISREG(metadata.st_mode) or metadata.st_mode & 0o022:
                return GoUsage(now, detail="limits_snapshot_invalid")
            raw = stream.read(MAX_BYTES + 1)
        if len(raw) > MAX_BYTES:
            return GoUsage(now, detail="limits_snapshot_invalid")
        snap = UsageSnapshot.model_validate_json(raw)
        if (
            snap.account_id != binding.account_id
            or snap.account_id != (profile.account_id or profile.id)
            or snap.scope_id != binding.scope_id
            or snap.binding_digest != execution_binding(profile)
            or profile.provider != "pi"
        ):
            return GoUsage(now, detail="limits_binding_mismatch")
        stamps = (
            snap.generated_at,
            snap.attempted_at,
            snap.observed_at,
            snap.entitlement_observed_at,
        )
        if any(
            stamp is not None and not -timedelta(seconds=120) <= now - stamp <= MAX_AGE
            for stamp in stamps
        ):
            return GoUsage(now, detail="limits_snapshot_stale")
        if snap.observed_at:
            bounds = {"rolling": 5 * 3600, "weekly": 7 * 86400, "monthly": 32 * 86400}
            if any(
                not -120 <= (w.reset_at - snap.observed_at).total_seconds() <= bounds[w.id] + 120
                for w in snap.windows
            ):
                return GoUsage(now, detail="limits_snapshot_invalid")
        return GoUsage(
            attempted_at=snap.attempted_at,
            observed_at=snap.observed_at,
            windows=snap.windows,
            detail=snap.detail,
            entitlement_observed_at=snap.entitlement_observed_at,
        )
    except OSError:
        return GoUsage(now, detail="limits_snapshot_unreadable")
    except (ValueError, TypeError, RecursionError):
        return GoUsage(now, detail="limits_snapshot_invalid")
