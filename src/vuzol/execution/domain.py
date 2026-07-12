"""Immutable contracts for isolated coding execution."""

import hashlib
import json
import uuid
from enum import StrEnum
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field, model_validator


class FrozenModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class CommandClass(StrEnum):
    SYSTEM_GIT = "system_git"
    PROVIDER_AGENT = "provider_agent"
    PROJECT_READ = "project_read"
    PROJECT_WRITE = "project_write"
    NETWORK = "network"
    DESTRUCTIVE_PROJECT = "destructive_project"
    HOST_PRIVILEGED = "host_privileged"
    PROHIBITED = "prohibited"


class MountMode(StrEnum):
    READ_ONLY = "ro"
    READ_WRITE = "rw"


class SandboxMount(FrozenModel):
    source: Path
    target: Path
    mode: MountMode
    purpose: str = Field(min_length=1, max_length=100)

    @model_validator(mode="after")
    def validate_target(self) -> "SandboxMount":
        if not self.source.is_absolute() or not self.target.is_absolute():
            raise ValueError("sandbox mount paths must be absolute")
        if self.target in {Path("/"), Path("/home"), Path("/run"), Path("/proc"), Path("/sys")}:
            raise ValueError("sandbox mount target is prohibited")
        return self


class SandboxSpec(FrozenModel):
    image: str = Field(pattern=r"^[^\s@]+@sha256:[0-9a-f]{64}$")
    uid: int = Field(ge=1)
    gid: int = Field(ge=1)
    working_directory: Path
    mounts: tuple[SandboxMount, ...]
    cpu_count: float = Field(gt=0)
    memory_bytes: int = Field(ge=1)
    pids_limit: int = Field(ge=1)
    tmpfs_bytes: int = Field(ge=1)
    open_files_limit: int = Field(ge=1)
    output_bytes: int = Field(ge=1)
    timeout_seconds: int = Field(ge=1)
    stop_grace_seconds: int = Field(ge=1)
    network_disabled: bool = True
    proxy_network: str | None = Field(default=None, pattern=r"^[a-zA-Z0-9][a-zA-Z0-9_.-]*$")
    https_proxy_url: str | None = None
    environment: dict[str, str] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_isolation(self) -> "SandboxSpec":
        if not self.working_directory.is_absolute():
            raise ValueError("sandbox working directory must be absolute")
        targets = [mount.target for mount in self.mounts]
        if len(set(targets)) != len(targets):
            raise ValueError("sandbox mount targets must be unique")
        forbidden_names = {"DOCKER_HOST", "DATABASE_URL", "VUZOL_DATABASE_DSN"}
        if forbidden_names.intersection(self.environment):
            raise ValueError("sandbox environment contains a prohibited variable")
        proxy_configured = self.proxy_network is not None or self.https_proxy_url is not None
        if self.network_disabled and proxy_configured:
            raise ValueError("network-disabled sandbox cannot configure proxy egress")
        if not self.network_disabled and not (
            self.proxy_network is not None and self.https_proxy_url is not None
        ):
            raise ValueError("networked sandbox requires a controlled proxy transport")
        return self

    @property
    def stable_hash(self) -> str:
        payload = json.dumps(
            self.model_dump(mode="json"), sort_keys=True, separators=(",", ":")
        ).encode()
        return hashlib.sha256(payload).hexdigest()


class ProcessEnvelope(FrozenModel):
    task_id: uuid.UUID
    run_id: uuid.UUID
    step_id: uuid.UUID
    worktree_id: uuid.UUID
    profile_id: str = Field(pattern=r"^[a-z][a-z0-9_-]*$")
    provider_attempt: int = Field(ge=1)
    lease_generation: int = Field(ge=1)
    argv: tuple[str, ...] = Field(min_length=1, max_length=64)
    stdin: str = Field(max_length=1_000_000)
    sandbox: SandboxSpec

    @property
    def stable_hash(self) -> str:
        payload = json.dumps(
            self.model_dump(mode="json"), sort_keys=True, separators=(",", ":")
        ).encode()
        return hashlib.sha256(payload).hexdigest()

    @property
    def redacted(self) -> dict[str, object]:
        return {
            "argv": list(self.argv),
            "stdin_sha256": hashlib.sha256(self.stdin.encode()).hexdigest(),
            "sandbox_spec_hash": self.sandbox.stable_hash,
        }


class WorktreeReference(FrozenModel):
    id: uuid.UUID
    task_id: uuid.UUID
    run_id: uuid.UUID
    project_id: str
    path: Path
    branch: str
    base_commit: str = Field(pattern=r"^[0-9a-f]{40,64}$")
    repository_identity_hash: str = Field(pattern=r"^[0-9a-f]{64}$")


class GitInspection(FrozenModel):
    head: str = Field(pattern=r"^[0-9a-f]{40,64}$")
    branch: str
    changed_files: tuple[str, ...]
    diff: bytes

    @property
    def diff_hash(self) -> str:
        return hashlib.sha256(self.diff).hexdigest()
