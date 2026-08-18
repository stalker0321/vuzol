"""Trusted acceptance-artifact production in the validation sandbox."""

from __future__ import annotations

import hashlib
import json
import mimetypes
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol.config.registries import ConfigurationBundle
from vuzol.execution.access import WorktreeAccessLease, WorktreeAccessManager
from vuzol.execution.artifacts import ArtifactError, ArtifactStore
from vuzol.execution.domain import ProcessEnvelope
from vuzol.execution.git import GitError, LocalGit
from vuzol.execution.paths import PathViolation, contained, trusted_root
from vuzol.execution.ports import SandboxRuntime
from vuzol.project_environment import current_environment
from vuzol.projects.artifact_contracts import ArtifactExpectation, expected_artifacts
from vuzol.storage.models import Artifact, Step, Task, Worktree
from vuzol.storage.types import StepStatus
from vuzol.workflows.domain import OutcomeKind, StepOutcome
from vuzol.workflows.ports import CancellationContext, StepExecutionRequest

_WEB_KINDS = frozenset({"static_site", "web_service"})
_FILE_ARTIFACTS = frozenset({"android_apk", "package_archive"})
_MAX_MATCHED_FILES = 20


@dataclass(frozen=True, slots=True)
class ArtifactCommandResult:
    exit_code: int
    stdout: str
    stderr: str
    duration_ms: int
    validation_image_digest: str
    producer_process_id: uuid.UUID | None = None


class ArtifactCommandRunner(Protocol):
    async def run(
        self,
        request: StepExecutionRequest,
        *,
        worktree_id: uuid.UUID,
        argv: tuple[str, ...],
        timeout_seconds: int,
        cancellation: CancellationContext,
    ) -> ArtifactCommandResult: ...


class ArtifactEnvelopeFactory(Protocol):
    async def build_artifact(
        self,
        request: StepExecutionRequest,
        *,
        worktree_id: uuid.UUID,
        argv: tuple[str, ...],
        timeout_seconds: int,
    ) -> ProcessEnvelope: ...


class SandboxedArtifactCommandRunner:
    def __init__(self, envelopes: ArtifactEnvelopeFactory, runtime: SandboxRuntime) -> None:
        self._envelopes = envelopes
        self._runtime = runtime

    async def run(
        self,
        request: StepExecutionRequest,
        *,
        worktree_id: uuid.UUID,
        argv: tuple[str, ...],
        timeout_seconds: int,
        cancellation: CancellationContext,
    ) -> ArtifactCommandResult:
        envelope = await self._envelopes.build_artifact(
            request,
            worktree_id=worktree_id,
            argv=argv,
            timeout_seconds=timeout_seconds,
        )
        result = await self._runtime.run(envelope, cancellation)
        return ArtifactCommandResult(
            exit_code=result.exit_code,
            stdout=result.stdout,
            stderr=result.stderr,
            duration_ms=result.duration_ms,
            validation_image_digest=envelope.sandbox.image,
        )


class ArtifactProductionHandler:
    def __init__(
        self,
        factory: async_sessionmaker[AsyncSession],
        registries: ConfigurationBundle,
        git: LocalGit,
        access: WorktreeAccessManager,
        runner: ArtifactCommandRunner,
        artifacts: ArtifactStore,
        *,
        worktree_root: Path,
    ) -> None:
        self._factory = factory
        self._registries = registries
        self._git = git
        self._access = access
        self._runner = runner
        self._artifacts = artifacts
        self._worktree_root = trusted_root(worktree_root, create=False)

    async def execute(
        self, request: StepExecutionRequest, cancellation: CancellationContext
    ) -> StepOutcome:
        lease: WorktreeAccessLease | None = None
        if cancellation.requested:
            return StepOutcome(kind=OutcomeKind.CANCELLED, result={}, category="cancelled")
        try:
            task, worktree, step, root, contract = await self._load(request)
            assert worktree.result_commit is not None
            expectations = expected_artifacts(contract)
            components = contract.get("components")
            assert isinstance(components, dict)
            pending = tuple(
                expectation
                for expectation in expectations
                if isinstance(components.get(expectation.component_key), dict)
                and components[expectation.component_key].get("kind") not in _WEB_KINDS
            )
            if not pending:
                return StepOutcome.succeeded({"status": "skipped", "reason": "no_artifacts"})
            assert task.project_id is not None
            project = self._registries.projects.get(task.project_id)
            sandbox = self._registries.sandboxes.get(
                project.validation_sandbox_profile or project.sandbox_profile
            )
            lease = await self._access.grant(root, sandbox_uid=sandbox.uid, sandbox_gid=sandbox.gid)
            if await self._git.resolve_commit(root, "HEAD") != worktree.result_commit:
                raise ValueError("artifact source HEAD does not match the retained result")
            await self._git.require_clean_tracked_worktree(root)
            produced: list[dict[str, object]] = []
            for expectation in pending:
                component = components[expectation.component_key]
                assert isinstance(component, dict)
                command = _approved_command(component)
                if command is None:
                    return _needs_setup(expectation, "bounded acceptance command is missing")
                run = await self._runner.run(
                    request,
                    worktree_id=worktree.id,
                    argv=command,
                    timeout_seconds=step.timeout_seconds,
                    cancellation=cancellation,
                )
                report = _report(
                    expectation,
                    command=command,
                    run=run,
                    source_commit=worktree.result_commit,
                )
                if run.exit_code != 0:
                    return StepOutcome(
                        kind=OutcomeKind.BLOCKED,
                        result={"status": "failed", "report": report},
                        category="artifact_command_failed",
                        summary=f"{expectation.label} acceptance command exited {run.exit_code}",
                    )
                if await self._git.resolve_commit(root, "HEAD") != worktree.result_commit:
                    raise ValueError("artifact command changed the retained result HEAD")
                await self._git.require_clean_tracked_worktree(root)
                files = (
                    _matched_artifacts(root, expectation.patterns)
                    if expectation.kind in _FILE_ARTIFACTS
                    else ()
                )
                if expectation.kind in _FILE_ARTIFACTS and not files:
                    return StepOutcome(
                        kind=OutcomeKind.BLOCKED,
                        result={"status": "failed", "report": report},
                        category="artifact_file_missing",
                        summary=f"{expectation.label} produced no declared artifact file",
                    )
                produced.extend(
                    await self._persist(
                        request,
                        expectation=expectation,
                        report=report,
                        files=files,
                        producer_process_id=run.producer_process_id,
                    )
                )
            return StepOutcome.succeeded(
                {
                    "status": "produced",
                    "source_commit": worktree.result_commit,
                    "artifacts": produced,
                }
            )
        except (ArtifactError, GitError, LookupError, PathViolation, ValueError) as error:
            return StepOutcome(
                kind=OutcomeKind.BLOCKED,
                result={},
                category="artifact_production_invalid",
                summary=str(error)[:500],
            )
        finally:
            if lease is not None:
                await lease.revoke()

    async def _load(
        self, request: StepExecutionRequest
    ) -> tuple[Task, Worktree, Step, Path, dict[str, object]]:
        async with self._factory() as session:
            task = await session.get(Task, request.task_id)
            step = await session.get(Step, request.step_id)
            worktree = await session.scalar(
                select(Worktree).where(
                    Worktree.task_id == request.task_id,
                    Worktree.run_id == request.run_id,
                )
            )
            environment = (
                None
                if task is None or task.project_id is None
                else await current_environment(session, task.project_id)
            )
        if task is None or task.project_id is None or step is None or worktree is None:
            raise LookupError("artifact production state is incomplete")
        if step.status not in {StepStatus.LEASED, StepStatus.RUNNING}:
            raise ValueError("artifact production step is not active")
        if worktree.result_commit is None:
            raise ValueError("artifact production requires a retained result commit")
        if environment is None or not isinstance(environment.contract, dict):
            raise ValueError("approved project environment is missing")
        root = contained(self._worktree_root, Path(worktree.path))
        return task, worktree, step, root, environment.contract

    async def _persist(
        self,
        request: StepExecutionRequest,
        *,
        expectation: ArtifactExpectation,
        report: dict[str, object],
        files: tuple[Path, ...],
        producer_process_id: uuid.UUID | None,
    ) -> list[dict[str, object]]:
        records: list[dict[str, object]] = []
        async with self._factory.begin() as session:
            if files:
                for path in files:
                    row = await self._artifacts.persist(
                        session,
                        task_id=request.task_id,
                        run_id=request.run_id,
                        step_id=request.step_id,
                        artifact_type=expectation.kind,
                        content=path.read_bytes(),
                        media_type=mimetypes.guess_type(path.name)[0] or "application/octet-stream",
                        producer_process_id=producer_process_id,
                    )
                    records.append(_artifact_record(row, component_key=expectation.component_key))
            else:
                row = await self._artifacts.persist(
                    session,
                    task_id=request.task_id,
                    run_id=request.run_id,
                    step_id=request.step_id,
                    artifact_type=expectation.kind,
                    content=_json_bytes(report),
                    media_type="application/json",
                    producer_process_id=producer_process_id,
                )
                records.append(_artifact_record(row, component_key=expectation.component_key))
            evidence = await self._artifacts.persist(
                session,
                task_id=request.task_id,
                run_id=request.run_id,
                step_id=request.step_id,
                artifact_type=f"{expectation.kind}_evidence",
                content=_json_bytes(report),
                media_type="application/json",
                producer_process_id=producer_process_id,
            )
            records.append(_artifact_record(evidence, component_key=expectation.component_key))
        return records


def _approved_command(component: dict[str, object]) -> tuple[str, ...] | None:
    raw = component.get("run_command")
    if (
        not isinstance(raw, list)
        or not raw
        or len(raw) > 32
        or not all(
            isinstance(value, str) and value and len(value) <= 500 and "\x00" not in value
            for value in raw
        )
    ):
        return None
    return tuple(raw)


def _matched_artifacts(root: Path, patterns: tuple[str, ...]) -> tuple[Path, ...]:
    if not patterns:
        return ()
    matches: dict[str, Path] = {}
    for pattern in patterns:
        relative = Path(pattern)
        if relative.is_absolute() or ".." in relative.parts:
            raise PathViolation("artifact pattern escapes the worktree")
        for candidate in root.glob(pattern):
            path = contained(root, candidate)
            if path.is_file() and not path.is_symlink():
                matches[path.relative_to(root).as_posix()] = path
                if len(matches) > _MAX_MATCHED_FILES:
                    raise ValueError("artifact pattern matched too many files")
    return tuple(matches[key] for key in sorted(matches))


def _report(
    expectation: ArtifactExpectation,
    *,
    command: tuple[str, ...],
    run: ArtifactCommandResult,
    source_commit: str,
) -> dict[str, object]:
    stdout = run.stdout.encode()
    stderr = run.stderr.encode()
    return {
        "schema_version": "trusted-artifact-evidence.v1",
        "component_key": expectation.component_key,
        "artifact_kind": expectation.kind,
        "validation": expectation.validation,
        "source_commit": source_commit,
        "argv": list(command),
        "exit_code": run.exit_code,
        "duration_ms": run.duration_ms,
        "stdout": run.stdout,
        "stdout_sha256": hashlib.sha256(stdout).hexdigest(),
        "stdout_bytes": len(stdout),
        "stderr": run.stderr,
        "stderr_sha256": hashlib.sha256(stderr).hexdigest(),
        "stderr_bytes": len(stderr),
        "validation_image_digest": run.validation_image_digest,
    }


def _needs_setup(expectation: ArtifactExpectation, detail: str) -> StepOutcome:
    return StepOutcome(
        kind=OutcomeKind.BLOCKED,
        result={
            "status": "needs_setup",
            "component_key": expectation.component_key,
            "artifact_kind": expectation.kind,
        },
        category="artifact_producer_setup_required",
        summary=f"{expectation.label}: {detail}",
    )


def _artifact_record(row: Artifact, *, component_key: str) -> dict[str, object]:
    return {
        "component_key": component_key,
        "artifact_id": str(row.id),
        "artifact_type": row.artifact_type,
        "content_hash": row.content_hash,
        "size_bytes": row.size_bytes,
        "media_type": row.media_type,
    }


def _json_bytes(value: dict[str, object]) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
