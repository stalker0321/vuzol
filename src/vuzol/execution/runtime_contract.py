"""Certified, content-free operational contracts for provider agent runtimes."""

import hashlib
import json
import os
from datetime import UTC, datetime
from pathlib import Path

from pydantic import Field, model_validator

from vuzol.config.models import FrozenModel, ProviderProfileConfig, SandboxProfileConfig
from vuzol.config.revision import content_revision

CERTIFICATE_SCHEMA_VERSION = "agent-runtime-certificate.v1"
CERTIFICATE_MAX_BYTES = 16_384


class AgentCertificationKey(FrozenModel):
    cli_version: str = Field(min_length=1, max_length=100)
    provider_image_digest: str = Field(pattern=r"^[^\s@]+@sha256:[0-9a-f]{64}$")
    profile_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    sandbox_policy_hash: str = Field(pattern=r"^[0-9a-f]{64}$")

    @property
    def digest(self) -> str:
        body = json.dumps(
            self.model_dump(mode="json"), sort_keys=True, separators=(",", ":")
        ).encode()
        return hashlib.sha256(body).hexdigest()


class AgentRuntimeCertificate(FrozenModel):
    schema_version: str = CERTIFICATE_SCHEMA_VERSION
    key: AgentCertificationKey
    profile_id: str = Field(pattern=r"^[a-z][a-z0-9_-]*$")
    certified_at: datetime
    ordinary_file_read: bool
    ordinary_file_edited: bool
    git_protected: bool
    structured_output_valid: bool
    cleanup_succeeded: bool
    task_uuid: str = Field(min_length=1, max_length=100)
    run_uuid: str = Field(min_length=1, max_length=100)

    @model_validator(mode="after")
    def require_all_invariants(self) -> "AgentRuntimeCertificate":
        facts = (
            self.ordinary_file_read,
            self.ordinary_file_edited,
            self.git_protected,
            self.structured_output_valid,
            self.cleanup_succeeded,
        )
        if not all(facts):
            raise ValueError("agent certification requires every runtime invariant")
        return self


def certification_key(
    profile: ProviderProfileConfig, sandbox: SandboxProfileConfig
) -> AgentCertificationKey:
    contract = profile.agent_runtime_contract
    if contract is None:
        raise ValueError(f"profile {profile.id} has no agent runtime contract")
    return AgentCertificationKey(
        cli_version=contract.cli_version,
        provider_image_digest=sandbox.image,
        profile_hash=content_revision(profile),
        sandbox_policy_hash=content_revision(sandbox),
    )


class AgentCertificateStore:
    """Executor-owned certificate files; provider sandboxes never mount this root."""

    def __init__(self, root: Path) -> None:
        self._root = root.absolute()

    def require(
        self, profile: ProviderProfileConfig, sandbox: SandboxProfileConfig
    ) -> AgentRuntimeCertificate:
        key = certification_key(profile, sandbox)
        path = self._path(key)
        try:
            stat = path.lstat()
        except FileNotFoundError as error:
            raise ValueError(f"agent runtime is uncertified for profile {profile.id}") from error
        if path.is_symlink() or not path.is_file() or stat.st_size > CERTIFICATE_MAX_BYTES:
            raise ValueError("agent runtime certificate is unsafe")
        try:
            certificate = AgentRuntimeCertificate.model_validate_json(path.read_bytes())
        except (OSError, ValueError) as error:
            raise ValueError("agent runtime certificate is invalid") from error
        if certificate.key != key or certificate.profile_id != profile.id:
            raise ValueError("agent runtime certificate is stale")
        return certificate

    def issue(self, certificate: AgentRuntimeCertificate) -> Path:
        self._root.mkdir(mode=0o700, parents=True, exist_ok=True)
        if self._root.is_symlink():
            raise ValueError("agent certificate root cannot be a symlink")
        payload = certificate.model_dump_json(indent=2).encode()
        if len(payload) > CERTIFICATE_MAX_BYTES:
            raise ValueError("agent runtime certificate exceeds its size limit")
        path = self._path(certificate.key)
        temporary = self._root / f".{certificate.key.digest}.{os.getpid()}.tmp"
        try:
            descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(descriptor, "wb") as stream:
                stream.write(payload)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, path)
        finally:
            temporary.unlink(missing_ok=True)
        return path

    def _path(self, key: AgentCertificationKey) -> Path:
        return self._root / f"{key.digest}.json"


def new_certificate(
    *,
    key: AgentCertificationKey,
    profile_id: str,
    task_uuid: str,
    run_uuid: str,
) -> AgentRuntimeCertificate:
    return AgentRuntimeCertificate(
        key=key,
        profile_id=profile_id,
        certified_at=datetime.now(UTC),
        ordinary_file_read=True,
        ordinary_file_edited=True,
        git_protected=True,
        structured_output_valid=True,
        cleanup_succeeded=True,
        task_uuid=task_uuid,
        run_uuid=run_uuid,
    )
