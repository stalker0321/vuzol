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
from vuzol.execution.finalization import (
    CapturedOutput,
    GateEvidence,
    GateExecutionContext,
    GateRun,
    TrustedGateRunner,
    WorkerFinalizationError,
    WorkerFinalizer,
    _reported_usage,
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
from vuzol.execution.runtime_contract import (
    AgentCertificateStore,
    certification_key,
    new_certificate,
)
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
from vuzol.experiments.domain import (
    BoundedLevel,
    ContextManifest,
    ExecutionMode,
    ReportedUsage,
    RequiredGate,
    RiskLevel,
    TaskClass,
    TaskClassification,
    WorkerEditReport,
    WorkerTaskCapsule,
)
from vuzol.experiments.review import VerificationResult
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
    "AgentCertificateStore",
    "Any",
    "ArtifactSecretError",
    "ArtifactStore",
    "AsyncMock",
    "BoundedLevel",
    "CancellationContext",
    "CapturedOutput",
    "CodexInvocation",
    "CodexProcessResult",
    "ContextManifest",
    "EgressDestination",
    "ExecutionEnvelopeFactory",
    "ExecutionMode",
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
    "ReportedUsage",
    "RequiredGate",
    "RiskLevel",
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
    "TaskClass",
    "TaskClassification",
    "TrustedGateRunner",
    "VerificationResult",
    "WorkerEditReport",
    "WorkerFinalizationError",
    "WorkerFinalizer",
    "WorkerTaskCapsule",
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
    "_certified_codex_profile",
    "_collect_entries",
    "_edit_report",
    "_finalizer_capsule",
    "_finalizer_repository",
    "_gate_context",
    "_get_xattr",
    "_git",
    "_map_id",
    "_normalized_usage",
    "_numeric_acl",
    "_prepare_diagnostic_destinations",
    "_read_id_map",
    "_reader",
    "_reported_usage",
    "_require_trusted_command",
    "_sandbox_gate_runner",
    "_seccomp_profile",
    "_set_numeric_acl",
    "_set_xattr",
    "_single_regular_tar_file",
    "_summarize_grok_process",
    "_tar_file",
    "asyncio",
    "canonical_codex_argv",
    "canonical_grok_argv",
    "certification_key",
    "contained",
    "contextlib",
    "docker_run_argv",
    "envelope",
    "errno",
    "hashlib",
    "io",
    "json",
    "new_certificate",
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


def _finalizer_repository(tmp_path: Path) -> tuple[Path, str, str]:
    repository = tmp_path / "finalizer-repo"
    repository.mkdir()
    _git(repository, "init", "-b", "main")
    (repository / "src").mkdir()
    (repository / "src" / "example.py").write_text("VALUE = 1\n")
    (repository / "Makefile").write_text("test format-check lint type-check:\n\t@true\n")
    _git(repository, "add", ".")
    _git(
        repository,
        "-c",
        "user.name=Base",
        "-c",
        "user.email=base@example.invalid",
        "commit",
        "-m",
        "base",
    )
    base = _git(repository, "rev-parse", "HEAD").strip()
    branch = "step09a/test/finalizer"
    _git(repository, "switch", "-c", branch)
    return repository, base, branch


def _finalizer_capsule(
    base: str,
    branch: str,
    *,
    allowed_paths: tuple[str, ...] = ("src/example.py",),
    gates: tuple[RequiredGate, ...] | None = None,
    parent_attempt: int | None = None,
) -> WorkerTaskCapsule:
    return WorkerTaskCapsule(
        experiment_id="step09a-finalizer-test",
        task_id="bounded-edit",
        worker_profile="grok-a",
        base_commit=base,
        target_branch=branch,
        goal="Change the bounded example.",
        classification=TaskClassification(
            task_class=TaskClass.BOUNDED_FEATURE,
            complexity=BoundedLevel.MEDIUM,
            risk=RiskLevel.LOW,
            testability=BoundedLevel.HIGH,
            blast_radius=BoundedLevel.LOW,
            coupling=BoundedLevel.LOW,
            novelty=BoundedLevel.LOW,
            expected_file_count=1,
        ),
        predicted_mode=ExecutionMode.GROK_REVIEWED,
        actual_mode=ExecutionMode.GROK_REVIEWED,
        allowed_paths=allowed_paths,
        acceptance_criteria=("The measured change passes trusted gates.",),
        required_gates=gates
        or tuple(
            RequiredGate(name=name, command_id=f"make {name}")
            for name in ("test", "format-check", "lint", "type-check")
        ),
        maximum_execution_seconds=30,
        context_manifest=ContextManifest(role="worker"),
        parent_attempt=parent_attempt,
    )


def _edit_report(*, attempt: int = 1, claimed_complete: bool = False) -> WorkerEditReport:
    return WorkerEditReport(
        experiment_id="provider-claim-does-not-control-result",
        task_id="provider-claim-does-not-control-result",
        attempt=attempt,
        claimed_complete=claimed_complete,
        implementation_summary="Implemented the requested bounded change.",
        limitations=("Provider-authored limitation retained as context.",),
        usage=ReportedUsage(input_tokens=999, output_tokens=999),
    )


def _normalized_usage() -> NormalizedUsage:
    return NormalizedUsage(
        input_tokens=11,
        cached_tokens=3,
        output_tokens=7,
        duration_ms=19,
    )


def _gate_context() -> GateExecutionContext:
    return GateExecutionContext(
        task_id=uuid.uuid4(),
        run_id=uuid.uuid4(),
        step_id=uuid.uuid4(),
        worktree_id=uuid.uuid4(),
        profile_id="grok-a",
        provider_attempt=1,
        lease_generation=1,
    )


def _sandbox_gate_runner() -> tuple[TrustedGateRunner, AsyncMock, AsyncMock]:
    envelopes = AsyncMock()
    envelope = MagicMock(spec=ProcessEnvelope)
    envelope.sandbox = MagicMock(image="validation@sha256:" + "b" * 64)
    envelopes.build_gate.return_value = envelope
    envelopes.build_canonicalizer.return_value = envelope
    runtime = AsyncMock()
    runtime.run.return_value = CodexProcessResult(0, "gate passed\n", "", 2)
    return TrustedGateRunner(envelopes, runtime), envelopes, runtime


def _certified_codex_profile() -> Any:
    from vuzol.config.models import ProviderProfileConfig

    return ProviderProfileConfig.model_validate(
        {
            "id": "codex-certified",
            "provider": "codex",
            "model": "codex",
            "launch_mode": "cli",
            "credential_required": False,
            "capabilities": ["repository_read", "code_edit", "project_shell"],
            "concurrency_limit": 1,
            "cost_class": "strong",
            "supported_task_types": ["coding"],
            "runtime_identity": "codex-certified",
            "state_directory": "/var/lib/vuzol-provider-state/codex-certified",
            "agent_runtime_contract": {
                "cli_version": "codex-cli 0.144.1",
                "edit_mechanism": "shell_backed_repository_tools",
                "working_directory": "/workspace",
                "writable_roots": ["/workspace"],
                "protected_roots": ["/workspace/.git"],
                "structured_output_source": "final_agent_message_json",
                "inner_sandbox_mode": "provider_managed",
                "supports_read": True,
                "supports_search": True,
                "supports_edit": True,
                "supports_git": False,
                "supports_network": False,
                "supports_local_checks": False,
            },
        }
    )


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
