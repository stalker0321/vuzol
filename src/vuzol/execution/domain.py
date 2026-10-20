"""Immutable contracts for isolated coding execution."""

import hashlib
import json
import uuid
from enum import StrEnum
from pathlib import Path
from typing import TypedDict

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
    seccomp_profile: Path
    seccomp_profile_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
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
    empty_workspace: bool = False

    @model_validator(mode="after")
    def validate_isolation(self) -> "SandboxSpec":
        if self.empty_workspace and (
            self.working_directory != Path("/workspace")
            or any(
                mount.target == Path("/workspace") or Path("/workspace") in mount.target.parents
                for mount in self.mounts
            )
        ):
            raise ValueError("empty workspace cannot expose workspace mounts")
        if not self.working_directory.is_absolute():
            raise ValueError("sandbox working directory must be absolute")
        if not self.seccomp_profile.is_absolute():
            raise ValueError("sandbox seccomp profile must be absolute")
        targets = [mount.target for mount in self.mounts]
        if len(set(targets)) != len(targets):
            raise ValueError("sandbox mount targets must be unique")
        forbidden_names = {
            "DOCKER_HOST",
            "DATABASE_URL",
            "VUZOL_DATABASE_DSN",
            "HTTP_PROXY",
            "HTTPS_PROXY",
            "ALL_PROXY",
            "NO_PROXY",
            "http_proxy",
            "https_proxy",
            "all_proxy",
            "no_proxy",
        }
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


class TurnProcessBinding(FrozenModel):
    """Durable inference identity, distinct from workflow execution identity."""

    turn_id: uuid.UUID
    invocation_id: uuid.UUID

    def resource_name(self, generation: int, resource: str) -> str:
        material = f"lead-turn:{self.turn_id}:{self.invocation_id}:{generation}:{resource}"
        return f"vuzol-turn-{hashlib.sha256(material.encode()).hexdigest()[:24]}-{resource}"

    def labels(self, generation: int, resource: str) -> dict[str, str]:
        return {
            "vuzol.managed": "true",
            "vuzol.resource": resource,
            "vuzol.binding": "leadership-turn",
            "vuzol.turn_id": str(self.turn_id),
            "vuzol.invocation_id": str(self.invocation_id),
            "vuzol.lease_generation": str(generation),
        }


class TurnBindingKeywords(TypedDict, total=False):
    turn_binding: TurnProcessBinding


class ProcessEnvelope(FrozenModel):
    task_id: uuid.UUID | None = None
    run_id: uuid.UUID | None = None
    step_id: uuid.UUID | None = None
    worktree_id: uuid.UUID | None = None
    turn_binding: TurnProcessBinding | None = None
    profile_id: str = Field(pattern=r"^[a-z][a-z0-9_-]*$")
    provider_attempt: int = Field(ge=1)
    lease_generation: int = Field(ge=1)
    argv: tuple[str, ...] = Field(min_length=1, max_length=64)
    stdin: str = Field(max_length=1_000_000)
    sandbox: SandboxSpec

    @model_validator(mode="after")
    def validate_binding(self) -> "ProcessEnvelope":
        workflow = (self.task_id, self.run_id, self.step_id, self.worktree_id)
        if self.turn_binding is None:
            if any(value is None for value in workflow):
                raise ValueError(
                    "workflow process requires complete task/run/step/worktree binding"
                )
        elif any(value is not None for value in workflow):
            raise ValueError("turn process cannot carry workflow bindings")
        return self

    @property
    def container_name(self) -> str:
        if self.turn_binding is not None:
            return self.turn_binding.resource_name(self.lease_generation, "sandbox")
        return f"vuzol-{str(self.step_id)[:12]}-{self.lease_generation}"

    @property
    def ownership_labels(self) -> dict[str, str]:
        if self.turn_binding is not None:
            return self.turn_binding.labels(self.lease_generation, "sandbox-container")
        return {
            "vuzol.managed": "true",
            "vuzol.resource": "sandbox-container",
            "vuzol.task_id": str(self.task_id),
            "vuzol.run_id": str(self.run_id),
            "vuzol.step_id": str(self.step_id),
            "vuzol.lease_generation": str(self.lease_generation),
        }

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
    added_files: tuple[str, ...] = ()

    @property
    def diff_hash(self) -> str:
        return hashlib.sha256(self.diff).hexdigest()
