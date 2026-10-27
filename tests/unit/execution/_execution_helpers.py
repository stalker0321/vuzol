import asyncio
import contextlib
import errno
import hashlib
import io
import json
import os
import signal
import stat
import subprocess
import tarfile
import uuid
from pathlib import Path
from typing import Any
from unittest.mock import ANY, AsyncMock, MagicMock, patch

import pytest

from vuzol.config.models import (
    EgressDestination,
    NetworkPolicy,
    SandboxNetworkMode,
    SandboxProfileConfig,
)
from vuzol.execution.access import (
    RootlessIdentity,
    RootlessIdentityResolver,
    WorktreeAccessError,
    WorktreeAccessManager,
    _acl_has_named_user,
    _base_acl_mode,
    _collect_entries,
    _get_xattr,
    _map_id,
    _read_id_map,
    _require_trusted_command,
    _select_chown_command,
    _set_xattr,
)
from vuzol.execution.artifacts import ArtifactSecretError, ArtifactStore
from vuzol.execution.codex import (
    ExecutionEnvelopeFactory,
    SandboxCodexTransport,
    _summarize_grok_process,
)
from vuzol.execution.domain import (
    MountMode,
    ProcessEnvelope,
    SandboxMount,
    SandboxSpec,
)
from vuzol.execution.gates import (
    CapturedOutput,
    GateEvidence,
    GateExecutionContext,
    GateRun,
    RequiredGate,
    TrustedGateRunner,
)
from vuzol.execution.git import SYSTEM_GIT_CONFIG, GitError, LocalGit
from vuzol.execution.handlers import PrepareWorktreeHandler
from vuzol.execution.paths import (
    PathViolation,
    contained,
    trusted_root,
    worktree_branch,
    worktree_path,
)
from vuzol.execution.proxy_networks import ProxyNetworkLease
from vuzol.execution.proxy_service import ProxyServiceError, ProxyServiceLease
from vuzol.execution.sandbox import (
    RootlessDockerRuntime,
    SandboxError,
    _artifact_staging,
    _atomic_write_diagnostic,
    _bounded_read,
    _prepare_diagnostic_destinations,
    _single_regular_tar_file,
    docker_run_argv,
    validate_seccomp_profile,
)
from vuzol.execution.worktrees import WorktreeService
from vuzol.providers.codex import canonical_codex_argv
from vuzol.providers.domain import NormalizedUsage
from vuzol.providers.grok import (
    GROK_DIAGNOSTIC_FILE_MAX_BYTES,
    canonical_grok_argv,
    staged_grok_diagnostic_paths,
)
from vuzol.providers.ports import CodexInvocation, CodexProcessResult
from vuzol.storage.models import Step, Worktree
from vuzol.storage.types import IdempotencyClass, StepStatus
from vuzol.workflows.ports import CancellationContext

__all__ = [
    "ANY",
    "GROK_DIAGNOSTIC_FILE_MAX_BYTES",
    "SYSTEM_GIT_CONFIG",
    "Any",
    "ArtifactSecretError",
    "ArtifactStore",
    "AsyncMock",
    "CancellationContext",
    "CapturedOutput",
    "CodexInvocation",
    "CodexProcessResult",
    "EgressDestination",
    "ExecutionEnvelopeFactory",
    "GateEvidence",
    "GateExecutionContext",
    "GateRun",
    "GitError",
    "IdempotencyClass",
    "LocalGit",
    "MagicMock",
    "MountMode",
    "NetworkPolicy",
    "NormalizedUsage",
    "Path",
    "PathViolation",
    "PrepareWorktreeHandler",
    "ProcessEnvelope",
    "ProxyNetworkLease",
    "ProxyServiceError",
    "ProxyServiceLease",
    "RequiredGate",
    "RootlessDockerRuntime",
    "RootlessIdentity",
    "RootlessIdentityResolver",
    "SandboxCodexTransport",
    "SandboxError",
    "SandboxMount",
    "SandboxNetworkMode",
    "SandboxProfileConfig",
    "SandboxSpec",
    "Step",
    "StepStatus",
    "TrustedGateRunner",
    "Worktree",
    "WorktreeAccessError",
    "WorktreeAccessManager",
    "WorktreeService",
    "_FixedIdentityResolver",
    "_TestAccessManager",
    "_acl_has_named_user",
    "_artifact_staging",
    "_atomic_write_diagnostic",
    "_base_acl_mode",
    "_bounded_read",
    "_collect_entries",
    "_get_xattr",
    "_git",
    "_map_id",
    "_numeric_acl",
    "_prepare_diagnostic_destinations",
    "_read_id_map",
    "_reader",
    "_require_trusted_command",
    "_sandbox_gate_runner",
    "_seccomp_profile",
    "_select_chown_command",
    "_set_numeric_acl",
    "_set_xattr",
    "_single_regular_tar_file",
    "_summarize_grok_process",
    "_tar_file",
    "asyncio",
    "canonical_codex_argv",
    "canonical_grok_argv",
    "contained",
    "contextlib",
    "docker_run_argv",
    "envelope",
    "errno",
    "hashlib",
    "io",
    "json",
    "os",
    "patch",
    "pytest",
    "sandbox_spec",
    "signal",
    "staged_grok_diagnostic_paths",
    "stat",
    "subprocess",
    "tarfile",
    "trusted_root",
    "uuid",
    "validate_seccomp_profile",
    "worktree_branch",
    "worktree_path",
]


def _seccomp_profile(tmp_path: Path) -> tuple[Path, str]:
    profile = tmp_path / "seccomp.json"
    if not profile.exists():
        profile.write_text('{"defaultAction":"SCMP_ACT_ERRNO"}\n')
        profile.chmod(0o600)
    return profile, hashlib.sha256(profile.read_bytes()).hexdigest()


def sandbox_spec(tmp_path: Path) -> SandboxSpec:
    worktree = tmp_path / "worktree"
    artifacts = tmp_path / "artifacts"
    worktree.mkdir()
    artifacts.mkdir()
    seccomp_profile, seccomp_digest = _seccomp_profile(tmp_path)
    return SandboxSpec(
        image=f"example/sandbox@sha256:{'a' * 64}",
        uid=10001,
        gid=10001,
        seccomp_profile=seccomp_profile,
        seccomp_profile_sha256=seccomp_digest,
        working_directory=Path("/workspace"),
        mounts=(
            SandboxMount(
                source=worktree,
                target=Path("/workspace"),
                mode=MountMode.READ_WRITE,
                purpose="worktree",
            ),
            SandboxMount(
                source=artifacts,
                target=Path("/artifacts"),
                mode=MountMode.READ_WRITE,
                purpose="artifacts",
            ),
        ),
        cpu_count=1,
        memory_bytes=128_000_000,
        pids_limit=64,
        tmpfs_bytes=16_000_000,
        open_files_limit=256,
        output_bytes=100_000,
        timeout_seconds=30,
        stop_grace_seconds=2,
    )


def envelope(tmp_path: Path) -> ProcessEnvelope:
    return ProcessEnvelope(
        task_id=uuid.uuid4(),
        run_id=uuid.uuid4(),
        step_id=uuid.uuid4(),
        worktree_id=uuid.uuid4(),
        profile_id="codex-a",
        provider_attempt=1,
        lease_generation=1,
        argv=("codex", "exec", "-"),
        stdin="bounded prompt",
        sandbox=sandbox_spec(tmp_path),
    )


def _tar_file(name: str, content: bytes, *, symlink: bool = False) -> bytes:
    payload = io.BytesIO()
    with tarfile.open(fileobj=payload, mode="w:") as archive:
        member = tarfile.TarInfo(name)
        if symlink:
            member.type = tarfile.SYMTYPE
            member.linkname = "unsafe"
            archive.addfile(member)
        else:
            member.size = len(content)
            archive.addfile(member, io.BytesIO(content))
    return payload.getvalue()


def _reader(content: bytes) -> asyncio.StreamReader:
    reader = asyncio.StreamReader()
    reader.feed_data(content)
    reader.feed_eof()
    return reader


def _sandbox_gate_runner() -> tuple[TrustedGateRunner, AsyncMock, AsyncMock]:
    envelopes = AsyncMock()
    envelope = MagicMock(spec=ProcessEnvelope)
    envelope.sandbox = MagicMock(image="validation@sha256:" + "b" * 64)
    envelopes.build_gate.return_value = envelope
    runtime = AsyncMock()
    runtime.run.return_value = CodexProcessResult(0, "gate passed\n", "", 2)
    return TrustedGateRunner(envelopes, runtime), envelopes, runtime


def _numeric_acl(path: Path) -> str:
    return subprocess.run(
        ("/usr/bin/getfacl", "-ncp", str(path)),
        check=True,
        capture_output=True,
        text=True,
    ).stdout


async def _set_numeric_acl(path: Path, entry: str) -> None:
    process = await asyncio.create_subprocess_exec(
        "/usr/bin/setfacl",
        "-m",
        entry,
        str(path),
    )
    assert await process.wait() == 0


def _git(repository: Path, *args: str) -> str:
    result = subprocess.run(
        ("/usr/bin/git", *args),
        cwd=repository,
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout


class _FixedIdentityResolver:
    def __init__(self, host_uid: int = 60_001, host_gid: int = 60_002) -> None:
        self.host_uid = host_uid
        self.host_gid = host_gid

    def resolve(self, sandbox_uid: int, sandbox_gid: int) -> RootlessIdentity:
        return RootlessIdentity(
            namespace_pid=os.getpid(),
            namespace_inode=os.stat("/proc/self/ns/user").st_ino,
            sandbox_uid=sandbox_uid,
            sandbox_gid=sandbox_gid,
            host_uid=self.host_uid,
            host_gid=self.host_gid,
        )


class _TestAccessManager(WorktreeAccessManager):
    async def _run(self, *argv: object, capture: bool = False) -> str:
        if Path(str(argv[0])).name == "nsenter":
            return ""
        return await super()._run(*argv, capture=capture)
