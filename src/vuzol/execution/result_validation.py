"""Deterministic post-execution validation for the universal coding workflow.

Owns measured Git facts, system checks, optional trusted sandbox gates, and the
host-owned result commit. Model output never decides pass/fail.
"""

from __future__ import annotations

import hashlib
import time
import uuid
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol.config.models import ProjectConfig
from vuzol.config.registries import ConfigurationBundle
from vuzol.execution.access import WorktreeAccessError, WorktreeAccessLease, WorktreeAccessManager
from vuzol.execution.artifacts import ArtifactSecretError, ArtifactStore
from vuzol.execution.finalization import (
    TRUSTED_GATE_COMMANDS,
    GateEvidence,
    GateExecutionContext,
    GateRun,
    TrustedGateRunner,
)
from vuzol.execution.git import GitError, LocalGit
from vuzol.execution.paths import contained, trusted_root
from vuzol.experiments.domain import RequiredGate
from vuzol.experiments.review import scan_suspicious_patterns
from vuzol.storage.errors import LeaseLost
from vuzol.storage.models import Run, Step, ValidationResult, Worktree
from vuzol.storage.types import StepStatus, WorktreeDeliveryState
from vuzol.workflows.domain import OutcomeKind, StepOutcome
from vuzol.workflows.ports import CancellationContext, StepExecutionRequest

RESULT_VALIDATION_SCHEMA = "result-validation.v1"
SYSTEM_GATE_PREFIX = "system:"

_PROHIBITED_NAMES = frozenset(
    {
        ".env",
        ".env.local",
        ".env.production",
        "id_rsa",
        "id_ecdsa",
        "id_ed25519",
        "credentials.json",
        "service-account.json",
    }
)
_PROHIBITED_SUFFIXES = frozenset({".pem", ".key", ".p12", ".pfx", ".jks"})
_PROHIBITED_PATH_PARTS = frozenset({".ssh", ".gnupg", ".aws", ".docker"})


@dataclass(frozen=True, slots=True)
class SystemCheck:
    name: str
    command_id: str
    exit_code: int
    duration_ms: int
    detail: str = ""


class ResultValidationError(RuntimeError):
    """Deterministic validation rejected the retained worktree."""

    def __init__(self, category: str, summary: str) -> None:
        super().__init__(summary)
        self.category = category
        self.safe_summary = summary


class ResultValidationHandler:
    """`coding.v1` validate step: system checks, trusted gates, result commit."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        registries: ConfigurationBundle,
        git: LocalGit,
        *,
        worktree_root: Path,
        gate_runner: TrustedGateRunner | None = None,
        worktree_access: WorktreeAccessManager | None = None,
        artifacts: ArtifactStore | None = None,
        gate_timeout_seconds: int = 3_600,
    ) -> None:
        self._factory = session_factory
        self._registries = registries
        self._git = git
        self._worktree_root = trusted_root(worktree_root, create=False)
        self._gates = gate_runner
        self._worktree_access = worktree_access
        self._artifacts = artifacts
        self._gate_timeout_seconds = gate_timeout_seconds

    async def execute(
        self, request: StepExecutionRequest, cancellation: CancellationContext
    ) -> StepOutcome:
        access: WorktreeAccessLease | None = None
        try:
            worktree, project, path = await self._load(request)
            trusted = resolve_trusted_gates(project)
            if trusted:
                access = await self._grant_access(path, project)
            evidence = await self._validate(
                request,
                worktree=worktree,
                project=project,
                path=path,
                trusted_gates=trusted,
                cancellation=cancellation,
            )
            await self._persist(request, worktree_id=worktree.id, evidence=evidence)
        except LeaseLost:
            cancellation.request()
            return StepOutcome(
                kind=OutcomeKind.CANCELLED,
                result={},
                category="validation_lease_lost",
            )
        except ResultValidationError as error:
            return StepOutcome(
                kind=OutcomeKind.BLOCKED,
                result={},
                category=error.category,
                summary=error.safe_summary,
                unknown_effects=False,
            )
        except (
            GitError,
            ArtifactSecretError,
            WorktreeAccessError,
            LookupError,
            ValueError,
        ) as error:
            return StepOutcome(
                kind=OutcomeKind.BLOCKED,
                result={},
                category="validation_failed",
                summary=str(error)[:500],
                unknown_effects=False,
            )
        finally:
            if access is not None:
                await access.revoke()
        return StepOutcome.succeeded(evidence)

    async def _load(self, request: StepExecutionRequest) -> tuple[Worktree, ProjectConfig, Path]:
        async with self._factory() as session:
            step = await session.get(Step, request.step_id)
            run = await session.get(Run, request.run_id)
            if step is None or run is None:
                raise LookupError("validate step is missing run state")
            if (
                step.status not in {StepStatus.LEASED, StepStatus.RUNNING}
                or step.lease_owner != request.lease.owner
                or step.lease_generation != request.lease.generation
                or step.run_id != request.run_id
                or run.task_id != request.task_id
            ):
                raise ValueError("validate step is not bound to the current fenced lease")
            worktree = await session.scalar(
                select(Worktree).where(
                    Worktree.run_id == request.run_id,
                    Worktree.task_id == request.task_id,
                )
            )
            if worktree is None:
                raise LookupError("validate requires a prepared worktree")
            if worktree.delivery_state not in {
                WorktreeDeliveryState.ACTIVE,
                WorktreeDeliveryState.WORKTREE_RETAINED,
            }:
                raise ValueError("worktree is not available for validation")
            project = self._registries.projects.get(worktree.project_id)
            path = contained(self._worktree_root, Path(worktree.path))
            return worktree, project, path

    async def _grant_access(self, path: Path, project: ProjectConfig) -> WorktreeAccessLease:
        if self._worktree_access is None:
            raise ResultValidationError(
                "validation_access_unavailable",
                "worktree access manager is unavailable for trusted gates",
            )
        if project.validation_sandbox_profile is None:
            raise ResultValidationError(
                "validation_sandbox_missing",
                f"project {project.id} has no validation sandbox profile",
            )
        validation = self._registries.sandboxes.get(project.validation_sandbox_profile)
        if not validation.enabled:
            raise ResultValidationError(
                "validation_sandbox_disabled",
                f"project {project.id} validation sandbox is disabled",
            )
        sandbox = self._registries.sandboxes.get(project.sandbox_profile)
        return await self._worktree_access.grant(
            path, sandbox_uid=sandbox.uid, sandbox_gid=sandbox.gid
        )

    async def _validate(
        self,
        request: StepExecutionRequest,
        *,
        worktree: Worktree,
        project: ProjectConfig,
        path: Path,
        trusted_gates: tuple[RequiredGate, ...],
        cancellation: CancellationContext,
    ) -> dict[str, Any]:
        started = time.perf_counter()
        await self._git.require_clean_source(project.repository_path)
        await self._git.require_no_remotes(path)
        inspection = await self._git.inspect(path, worktree.base_commit)
        system_checks: list[SystemCheck] = []

        if inspection.branch != worktree.branch:
            raise ResultValidationError(
                "validation_branch_mismatch",
                "worktree branch differs from the prepared task branch",
            )

        is_finalized = False
        recovered = False
        if inspection.head != worktree.base_commit:
            # A retry may observe the host-created commit before the transaction
            # that records it. Execute retained the measured diff hash and base
            # commit first, which forms the recovery marker.
            recorded_result = worktree.result_commit
            is_finalized = (
                worktree.diff_hash is not None
                and inspection.diff_hash == worktree.diff_hash
                and recorded_result in {worktree.base_commit, inspection.head}
            )
            recovered = is_finalized and recorded_result == worktree.base_commit
        if is_finalized:
            try:
                await self._git.require_clean_worktree(path)
                parent = await self._git.commit_parent(path, inspection.head)
            except GitError as error:
                raise ResultValidationError(
                    "validation_git_finalization", str(error)[:500]
                ) from error
            if parent != worktree.base_commit:
                raise ResultValidationError(
                    "validation_commit_ancestry",
                    "retained result commit is not a direct child of base",
                )
        elif inspection.head != worktree.base_commit:
            raise ResultValidationError(
                "validation_precommitted",
                "provider changed worktree HEAD before validation",
            )
        if not inspection.changed_files:
            raise ResultValidationError(
                "validation_empty_change",
                "implementation produced no changes",
            )

        prohibited = prohibited_paths(inspection.changed_files)
        if prohibited:
            raise ResultValidationError(
                "validation_prohibited_path",
                f"changed paths include prohibited names: {', '.join(prohibited[:5])}",
            )

        if self._artifacts is not None:
            self._artifacts.reject_secrets(inspection.diff)

        suspicious = scan_suspicious_patterns(
            {"worker.diff": inspection.diff.decode("utf-8", "replace")}
        )
        # Suspicious patterns are recorded; they do not alone block low-risk validation.
        # High-severity classifications still fail closed.
        blocking = tuple(
            signal
            for signal in suspicious
            if signal.classification
            in {"forced_success", "coverage_weakening", "shell_execution", "broad_cleanup"}
        )
        if blocking:
            raise ResultValidationError(
                "validation_suspicious_diff",
                f"diff matched blocking pattern: {blocking[0].classification}",
            )

        system_checks.append(
            SystemCheck(
                name="git-facts",
                command_id=f"{SYSTEM_GATE_PREFIX}git-facts",
                exit_code=0,
                duration_ms=_elapsed_ms(started),
                detail=f"files={len(inspection.changed_files)}",
            )
        )

        gate_runs: tuple[GateRun, ...] = ()
        if trusted_gates:
            if self._gates is None:
                raise ResultValidationError(
                    "validation_gate_runner_unavailable",
                    "trusted gate runner is unavailable",
                )
            context = GateExecutionContext(
                task_id=request.task_id,
                run_id=request.run_id,
                step_id=request.step_id,
                worktree_id=worktree.id,
                profile_id="result-validation",
                provider_attempt=1,
                lease_generation=request.lease.generation,
            )
            try:
                gate_runs = await self._gates.run(
                    path,
                    trusted_gates,
                    timeout_seconds=self._gate_timeout_seconds,
                    context=context,
                    cancellation=cancellation,
                )
            except ValueError as error:
                raise ResultValidationError("validation_gate_registry", str(error)[:500]) from error
            except RuntimeError as error:
                raise ResultValidationError(
                    "validation_gate_execution", type(error).__name__
                ) from error
            if any(run.evidence.exit_code != 0 for run in gate_runs):
                failed = next(run for run in gate_runs if run.evidence.exit_code != 0)
                raise ResultValidationError(
                    "validation_gate_failed",
                    f"required gate failed: {failed.evidence.name}",
                )
            post_gate = await self._git.inspect(path, worktree.base_commit)
            if (
                post_gate.head != inspection.head
                or post_gate.branch != inspection.branch
                or post_gate.changed_files != inspection.changed_files
                or post_gate.diff_hash != inspection.diff_hash
            ):
                raise ResultValidationError(
                    "validation_gate_mutated_worktree",
                    "required gates changed measured Git facts",
                )

        if is_finalized:
            system_checks.append(
                SystemCheck(
                    name="recovered-result-commit" if recovered else "already-finalized",
                    command_id=(
                        f"{SYSTEM_GATE_PREFIX}recovered-result-commit"
                        if recovered
                        else f"{SYSTEM_GATE_PREFIX}already-finalized"
                    ),
                    exit_code=0,
                    duration_ms=_elapsed_ms(started),
                )
            )
            return _success_payload(
                base_commit=worktree.base_commit,
                result_commit=inspection.head,
                branch=inspection.branch,
                changed_files=inspection.changed_files,
                diff_hash=inspection.diff_hash,
                system_checks=system_checks,
                gates=tuple(run.evidence for run in gate_runs),
                gate_runs=gate_runs,
                suspicious=tuple(signal.model_dump(mode="json") for signal in suspicious),
            )

        try:
            await self._git.stage_paths(path, inspection.changed_files)
            await self._git.require_diff_check(path)
            result_commit = await self._git.create_commit(
                path, f"validate({request.task_id}): retain measured implementation"
            )
            if await self._git.commit_parent(path, result_commit) != worktree.base_commit:
                raise ResultValidationError(
                    "validation_commit_ancestry",
                    "result commit is not a direct child of base",
                )
            await self._git.require_clean_worktree(path)
            await self._git.require_no_remotes(path)
            finalized_inspection = await self._git.inspect(path, worktree.base_commit)
        except GitError as error:
            raise ResultValidationError("validation_git_finalization", str(error)[:500]) from error

        if finalized_inspection.head != result_commit:
            raise ResultValidationError(
                "validation_git_mismatch",
                "final Git head does not match the result commit",
            )

        # After commit, changed files vs base are the tree delta of the result commit.
        committed_files = finalized_inspection.changed_files or inspection.changed_files
        system_checks.append(
            SystemCheck(
                name="result-commit",
                command_id=f"{SYSTEM_GATE_PREFIX}result-commit",
                exit_code=0,
                duration_ms=_elapsed_ms(started),
                detail=result_commit[:12],
            )
        )
        return _success_payload(
            base_commit=worktree.base_commit,
            result_commit=result_commit,
            branch=finalized_inspection.branch,
            changed_files=committed_files,
            diff_hash=inspection.diff_hash,
            system_checks=system_checks,
            gates=tuple(run.evidence for run in gate_runs),
            gate_runs=gate_runs,
            suspicious=tuple(signal.model_dump(mode="json") for signal in suspicious),
        )

    async def _persist(
        self,
        request: StepExecutionRequest,
        *,
        worktree_id: uuid.UUID,
        evidence: dict[str, Any],
    ) -> None:
        async with self._factory.begin() as session:
            step = await session.scalar(
                select(Step)
                .where(
                    Step.id == request.step_id,
                    Step.run_id == request.run_id,
                    Step.status.in_((StepStatus.LEASED, StepStatus.RUNNING)),
                    Step.lease_owner == request.lease.owner,
                    Step.lease_generation == request.lease.generation,
                )
                .with_for_update()
            )
            if step is None:
                raise LeaseLost(f"validate step lease lost before persistence: {request.step_id}")
            worktree = await session.scalar(
                select(Worktree).where(Worktree.id == worktree_id).with_for_update()
            )
            if worktree is None:
                raise LookupError("worktree disappeared during validation")
            worktree.result_commit = evidence["result_commit"]
            worktree.diff_hash = evidence["diff_hash"]
            worktree.delivery_state = WorktreeDeliveryState.WORKTREE_RETAINED
            worktree.last_inspected_at = func.now()
            worktree.lifecycle_generation += 1

            for check in evidence["system_checks"]:
                session.add(
                    ValidationResult(
                        step_id=request.step_id,
                        validator_type=check["command_id"],
                        command_hash=hashlib.sha256(check["command_id"].encode()).hexdigest(),
                        status="passed" if check["exit_code"] == 0 else "failed",
                        exit_code=check["exit_code"],
                        result=check,
                    )
                )
            for gate in evidence["gates"]:
                session.add(
                    ValidationResult(
                        step_id=request.step_id,
                        validator_type=gate["command_id"],
                        command_hash=hashlib.sha256(
                            "\0".join(str(part) for part in gate.get("argv", ())).encode()
                        ).hexdigest(),
                        status="passed" if gate["exit_code"] == 0 else "failed",
                        exit_code=gate["exit_code"],
                        result=gate,
                    )
                )

            if self._artifacts is not None:
                gate_runs: tuple[GateRun, ...] = evidence.get("_gate_runs") or ()
                for ordinal, run in enumerate(gate_runs, start=1):
                    for stream_name, captured in (("stdout", run.stdout), ("stderr", run.stderr)):
                        await self._artifacts.persist(
                            session,
                            task_id=request.task_id,
                            run_id=request.run_id,
                            step_id=request.step_id,
                            artifact_type=f"validation_gate_{ordinal}_{stream_name}",
                            content=captured.content,
                            media_type="text/plain",
                        )
                public = {k: v for k, v in evidence.items() if not k.startswith("_")}
                await self._artifacts.persist(
                    session,
                    task_id=request.task_id,
                    run_id=request.run_id,
                    step_id=request.step_id,
                    artifact_type="result_validation_evidence",
                    content=_json_bytes(public),
                    media_type="application/json",
                )
            await session.flush()
        # Strip internal handles from the step result payload.
        evidence.pop("_gate_runs", None)


def resolve_trusted_gates(project: ProjectConfig) -> tuple[RequiredGate, ...]:
    """Map project validation_commands onto the fixed trusted gate allowlist only."""

    gates: list[RequiredGate] = []
    for command in project.validation_commands:
        if not command.required:
            continue
        command_id = " ".join(command.argv)
        if command_id not in TRUSTED_GATE_COMMANDS:
            raise ResultValidationError(
                "validation_untrusted_command",
                f"validation command is not a trusted gate: {command_id}",
            )
        gates.append(RequiredGate(name=command.name, command_id=command_id))
    return tuple(gates)


def prohibited_paths(paths: tuple[str, ...]) -> tuple[str, ...]:
    blocked: list[str] = []
    for raw in paths:
        candidate = PurePosixPath(raw)
        if candidate.is_absolute() or ".." in candidate.parts:
            blocked.append(raw)
            continue
        name = candidate.name.lower()
        if name in _PROHIBITED_NAMES or any(
            name.endswith(suffix) for suffix in _PROHIBITED_SUFFIXES
        ):
            blocked.append(raw)
            continue
        if any(part.lower() in _PROHIBITED_PATH_PARTS for part in candidate.parts):
            blocked.append(raw)
    return tuple(blocked)


def _success_payload(
    *,
    base_commit: str,
    result_commit: str,
    branch: str,
    changed_files: tuple[str, ...],
    diff_hash: str,
    system_checks: list[SystemCheck],
    gates: tuple[GateEvidence, ...],
    gate_runs: tuple[GateRun, ...] = (),
    suspicious: tuple[dict[str, Any], ...] = (),
) -> dict[str, Any]:
    system_payload: list[dict[str, Any]] = [
        {
            "name": check.name,
            "command_id": check.command_id,
            "exit_code": check.exit_code,
            "duration_ms": check.duration_ms,
            "detail": check.detail,
        }
        for check in system_checks
    ]
    gate_payload: list[dict[str, Any]] = [
        {
            "name": gate.name,
            "command_id": gate.command_id,
            "argv": list(gate.argv),
            "exit_code": gate.exit_code,
            "duration_ms": gate.duration_ms,
            "validation_image_digest": gate.validation_image_digest,
        }
        for gate in gates
    ]
    # Approval envelope requires a non-empty gate list with exit_code 0.
    approval_gates: list[dict[str, Any]] = [
        {
            "name": item["name"],
            "command_id": item["command_id"],
            "exit_code": item["exit_code"],
            "duration_ms": item["duration_ms"],
        }
        for item in (*system_payload, *gate_payload)
    ]
    structured = {
        "schema_version": RESULT_VALIDATION_SCHEMA,
        "base_commit": base_commit,
        "result_commit": result_commit,
        "branch": branch,
        "changed_files": list(changed_files),
        "diff_hash": diff_hash,
        "gates": approval_gates,
        "claimed_complete": True,
    }
    return {
        "schema_version": RESULT_VALIDATION_SCHEMA,
        "base_commit": base_commit,
        "result_commit": result_commit,
        "branch": branch,
        "changed_files": list(changed_files),
        "diff_hash": diff_hash,
        "system_checks": system_payload,
        "gates": gate_payload,
        "suspicious_signals": list(suspicious),
        "structured_output": structured,
        "implementation_summary": (
            f"Validated {len(changed_files)} changed path(s); "
            f"{len(gate_payload)} trusted gate(s) and {len(system_payload)} system check(s) passed."
        ),
        "_gate_runs": gate_runs,
    }


def _elapsed_ms(started: float) -> int:
    return max(0, int((time.perf_counter() - started) * 1000))


def _json_bytes(payload: dict[str, Any]) -> bytes:
    import json

    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()
