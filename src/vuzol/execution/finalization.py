"""System-owned worker gates, Git finalization, and measured result evidence."""

import asyncio
import hashlib
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from pydantic import Field
from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.execution.access import WorktreeAccessError, WorktreeAccessLease
from vuzol.execution.artifacts import ArtifactStore
from vuzol.execution.domain import ProcessEnvelope
from vuzol.execution.git import GitError
from vuzol.execution.ports import GitPort, SandboxRuntime
from vuzol.experiments.domain import (
    FrozenModel,
    GateResult,
    ReportedUsage,
    RequiredGate,
    WorkerEditReport,
    WorkerResultManifest,
    WorkerTaskCapsule,
)
from vuzol.experiments.review import (
    GitWorkerResultVerifier,
    SuspiciousSignal,
    VerificationResult,
    path_is_allowed,
    scan_suspicious_patterns,
)
from vuzol.providers.domain import NormalizedUsage
from vuzol.workflows.ports import CancellationContext

TRUSTED_GATE_COMMANDS: dict[str, tuple[str, ...]] = {
    "make test": ("/usr/bin/make", "test"),
    "make format-check": ("/usr/bin/make", "format-check"),
    "make lint": ("/usr/bin/make", "lint"),
    "make type-check": ("/usr/bin/make", "type-check"),
}


class WorkerFinalizationError(RuntimeError):
    """A measured worker result could not be safely finalized."""

    def __init__(self, category: str, summary: str, result: "FinalizedWorkerResult") -> None:
        super().__init__(summary)
        self.category = category
        self.safe_summary = summary
        self.result = result


class GateEvidence(FrozenModel):
    name: str
    command_id: str
    argv: tuple[str, ...]
    exit_code: int
    duration_ms: int = Field(ge=0)
    stdout_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    stdout_bytes: int = Field(ge=0)
    stdout_truncated: bool
    stderr_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    stderr_bytes: int = Field(ge=0)
    stderr_truncated: bool


class FinalizationEvidence(FrozenModel):
    schema_version: str = "worker-finalization-evidence.v1"
    provider_edit_report: WorkerEditReport
    base_commit: str
    pre_finalize_head: str
    branch: str
    changed_files: tuple[str, ...]
    diff_sha256: str
    suspicious_signals: tuple[SuspiciousSignal, ...]
    gates: tuple[GateEvidence, ...]
    result_manifest: WorkerResultManifest | None = None
    verification: VerificationResult | None = None
    failure_category: str | None = None


@dataclass(frozen=True)
class CapturedOutput:
    content: bytes
    sha256: str
    byte_count: int
    truncated: bool


@dataclass(frozen=True)
class GateRun:
    evidence: GateEvidence
    stdout: CapturedOutput
    stderr: CapturedOutput


@dataclass(frozen=True)
class FinalizedWorkerResult:
    evidence: FinalizationEvidence
    gate_runs: tuple[GateRun, ...]

    @property
    def manifest(self) -> WorkerResultManifest:
        manifest = self.evidence.result_manifest
        if manifest is None:
            raise RuntimeError("failed finalization has no result manifest")
        return manifest


@dataclass(frozen=True)
class GateExecutionContext:
    task_id: uuid.UUID
    run_id: uuid.UUID
    step_id: uuid.UUID
    worktree_id: uuid.UUID
    profile_id: str
    provider_attempt: int
    lease_generation: int


class GateEnvelopePort(Protocol):
    async def build_gate(
        self,
        context: GateExecutionContext,
        argv: tuple[str, ...],
        *,
        timeout_seconds: int,
    ) -> ProcessEnvelope: ...


class GateRunner(Protocol):
    async def run(
        self,
        worktree: Path,
        gates: tuple[RequiredGate, ...],
        *,
        timeout_seconds: int,
        context: GateExecutionContext | None,
        cancellation: CancellationContext | None,
    ) -> tuple[GateRun, ...]: ...


class TrustedGateRunner:
    """Resolve finite command IDs to fixed argv in the pinned rootless sandbox."""

    def __init__(
        self,
        envelopes: GateEnvelopePort,
        runtime: SandboxRuntime,
    ) -> None:
        self._envelopes = envelopes
        self._runtime = runtime

    async def run(
        self,
        worktree: Path,
        gates: tuple[RequiredGate, ...],
        *,
        timeout_seconds: int,
        context: GateExecutionContext | None,
        cancellation: CancellationContext | None,
    ) -> tuple[GateRun, ...]:
        del worktree
        resolved: list[tuple[RequiredGate, tuple[str, ...]]] = []
        for gate in gates:
            argv = TRUSTED_GATE_COMMANDS.get(gate.command_id)
            if argv is None:
                raise ValueError(f"unknown trusted gate command ID: {gate.command_id}")
            resolved.append((gate, argv))
        if context is None or cancellation is None:
            raise ValueError("sandbox gate execution context is unavailable")
        return tuple(
            [
                await self._execute(
                    gate,
                    argv,
                    context=context,
                    cancellation=cancellation,
                    timeout_seconds=timeout_seconds,
                )
                for gate, argv in resolved
            ]
        )

    async def _execute(
        self,
        gate: RequiredGate,
        argv: tuple[str, ...],
        *,
        context: GateExecutionContext,
        cancellation: CancellationContext,
        timeout_seconds: int,
    ) -> GateRun:
        envelope = await self._envelopes.build_gate(
            context,
            argv,
            timeout_seconds=timeout_seconds,
        )
        result = await self._runtime.run(envelope, cancellation)
        stdout = _captured(result.stdout)
        stderr = _captured(result.stderr)
        evidence = GateEvidence(
            name=gate.name,
            command_id=gate.command_id,
            argv=argv,
            exit_code=result.exit_code,
            duration_ms=result.duration_ms,
            stdout_sha256=stdout.sha256,
            stdout_bytes=stdout.byte_count,
            stdout_truncated=stdout.truncated,
            stderr_sha256=stderr.sha256,
            stderr_bytes=stderr.byte_count,
            stderr_truncated=stderr.truncated,
        )
        return GateRun(evidence=evidence, stdout=stdout, stderr=stderr)


class WorkerFinalizer:
    def __init__(
        self,
        git: GitPort,
        *,
        gate_runner: GateRunner | None = None,
        verifier: GitWorkerResultVerifier | None = None,
        artifacts: ArtifactStore | None = None,
    ) -> None:
        self._git = git
        if gate_runner is None:
            raise ValueError("worker finalizer requires an explicit sandbox gate runner")
        self._gates = gate_runner
        self._verifier = verifier or GitWorkerResultVerifier()
        self._artifacts = artifacts

    async def finalize(
        self,
        *,
        worktree: Path,
        capsule: WorkerTaskCapsule,
        edit_report: WorkerEditReport,
        worker_profile: str,
        provider_usage: NormalizedUsage,
        provider_attempt: int,
        gate_context: GateExecutionContext | None = None,
        cancellation: CancellationContext | None = None,
        access: WorktreeAccessLease | None = None,
    ) -> FinalizedWorkerResult:
        inspection = await self._git.inspect(worktree, capsule.base_commit)
        evidence = FinalizationEvidence(
            provider_edit_report=edit_report,
            base_commit=capsule.base_commit,
            pre_finalize_head=inspection.head,
            branch=inspection.branch,
            changed_files=inspection.changed_files,
            diff_sha256=inspection.diff_hash,
            suspicious_signals=scan_suspicious_patterns(
                {"worker.diff": inspection.diff.decode("utf-8", "replace")}
            ),
            gates=(),
        )
        gate_runs: tuple[GateRun, ...] = ()
        try:
            if inspection.head != capsule.base_commit:
                self._fail("worker_precommitted", "provider changed worktree HEAD", evidence)
            if inspection.branch != capsule.target_branch:
                self._fail(
                    "worker_branch_mismatch", "worktree branch differs from capsule", evidence
                )
            try:
                await self._git.require_no_remotes(worktree)
            except GitError as error:
                self._fail("worker_git_isolation", str(error), evidence)
            if not inspection.changed_files:
                self._fail("worker_empty_change", "implementation produced no changes", evidence)
            if not all(
                path_is_allowed(path, capsule.allowed_paths) for path in inspection.changed_files
            ):
                self._fail(
                    "worker_scope_violation", "implementation exceeded allowed paths", evidence
                )
            try:
                gate_runs = await self._gates.run(
                    worktree,
                    capsule.required_gates,
                    timeout_seconds=capsule.maximum_execution_seconds,
                    context=gate_context,
                    cancellation=cancellation,
                )
            except ValueError as error:
                self._fail("worker_gate_registry", str(error), evidence)
            except RuntimeError as error:
                self._fail("worker_gate_execution", type(error).__name__, evidence)
            evidence = evidence.model_copy(
                update={"gates": tuple(run.evidence for run in gate_runs)}
            )
        finally:
            if access is not None:
                try:
                    await access.revoke()
                except WorktreeAccessError as error:
                    self._fail("worker_access_revoke", str(error), evidence, gate_runs)
        if any(run.evidence.exit_code != 0 for run in gate_runs):
            self._fail(
                "worker_gate_failed",
                "a required deterministic gate failed",
                evidence,
                gate_runs,
            )

        post_gate = await self._git.inspect(worktree, capsule.base_commit)
        if (
            post_gate.head != capsule.base_commit
            or post_gate.branch != capsule.target_branch
            or post_gate.changed_files != inspection.changed_files
            or post_gate.diff_hash != inspection.diff_hash
        ):
            self._fail(
                "worker_gate_mutated_worktree",
                "required gates changed measured Git facts",
                evidence,
                gate_runs,
            )
        try:
            await self._git.stage_paths(worktree, inspection.changed_files)
            result_commit = await self._git.create_commit(
                worktree, f"worker({capsule.task_id}): finalize implementation"
            )
            if await self._git.commit_parent(worktree, result_commit) != capsule.base_commit:
                self._fail(
                    "worker_commit_ancestry",
                    "result commit is not a direct child of base",
                    evidence,
                    gate_runs,
                )
            await self._git.require_clean_worktree(worktree)
            await self._git.require_no_remotes(worktree)
            finalized = await self._git.inspect(worktree, capsule.base_commit)
        except GitError as error:
            self._fail("worker_git_finalization", str(error), evidence, gate_runs)
        if finalized.head != result_commit or finalized.changed_files != inspection.changed_files:
            self._fail("worker_git_mismatch", "final Git facts changed during commit", evidence)

        manifest = WorkerResultManifest(
            experiment_id=capsule.experiment_id,
            task_id=capsule.task_id,
            worker_profile=worker_profile,
            base_commit=capsule.base_commit,
            result_commit=result_commit,
            branch=finalized.branch,
            changed_files=finalized.changed_files,
            claimed_complete=True,
            gates=tuple(
                GateResult(
                    name=run.evidence.name,
                    command_id=run.evidence.command_id,
                    exit_code=run.evidence.exit_code,
                    duration_ms=run.evidence.duration_ms,
                )
                for run in gate_runs
            ),
            total_worker_duration_ms=provider_usage.duration_ms
            + sum(run.evidence.duration_ms for run in gate_runs),
            usage=_reported_usage(provider_usage),
            failure_classification=None,
            limitations=edit_report.limitations,
            scope_exceeded=False,
            attempt=provider_attempt,
        )
        verification = await asyncio.to_thread(self._verifier.verify, worktree, capsule, manifest)
        evidence = evidence.model_copy(
            update={"result_manifest": manifest, "verification": verification}
        )
        if not verification.passed:
            self._fail(
                "worker_verification_failed", "generated manifest failed Git verification", evidence
            )
        return FinalizedWorkerResult(evidence=evidence, gate_runs=gate_runs)

    async def persist(
        self,
        session: AsyncSession,
        *,
        task_id: uuid.UUID,
        run_id: uuid.UUID,
        step_id: uuid.UUID,
        result: FinalizedWorkerResult,
    ) -> None:
        if self._artifacts is None:
            return
        for ordinal, run in enumerate(result.gate_runs, start=1):
            for stream_name, captured in (("stdout", run.stdout), ("stderr", run.stderr)):
                await self._artifacts.persist(
                    session,
                    task_id=task_id,
                    run_id=run_id,
                    step_id=step_id,
                    artifact_type=f"worker_gate_{ordinal}_{stream_name}",
                    content=captured.content,
                    media_type="text/plain",
                )
        await self._artifacts.persist(
            session,
            task_id=task_id,
            run_id=run_id,
            step_id=step_id,
            artifact_type="provider_edit_report",
            content=result.evidence.provider_edit_report.model_dump_json().encode(),
            media_type="application/json",
        )
        await self._artifacts.persist(
            session,
            task_id=task_id,
            run_id=run_id,
            step_id=step_id,
            artifact_type="worker_finalization_evidence",
            content=result.evidence.model_dump_json().encode(),
            media_type="application/json",
        )

    @staticmethod
    def _fail(
        category: str,
        summary: str,
        evidence: FinalizationEvidence,
        gate_runs: tuple[GateRun, ...] = (),
    ) -> None:
        failed = evidence.model_copy(update={"failure_category": category})
        raise WorkerFinalizationError(
            category,
            summary,
            FinalizedWorkerResult(evidence=failed, gate_runs=gate_runs),
        )


def _captured(value: str) -> CapturedOutput:
    content = value.encode()
    return CapturedOutput(
        content=content,
        sha256=hashlib.sha256(content).hexdigest(),
        byte_count=len(content),
        truncated=False,
    )


def _reported_usage(usage: NormalizedUsage) -> ReportedUsage:
    values = (usage.input_tokens, usage.cached_tokens, usage.output_tokens)
    return ReportedUsage(
        input_tokens=usage.input_tokens,
        cached_input_tokens=usage.cached_tokens,
        output_tokens=usage.output_tokens,
        reasoning_tokens=None,
        unavailable_reason=(
            "Provider token accounting was unavailable to the deterministic finalizer."
            if all(value is None for value in values)
            else None
        ),
    )
