"""Coding.v1 review: mechanical inspection plus risk-based independent review."""

from __future__ import annotations

import re
import uuid
from contextlib import suppress
from pathlib import Path, PurePosixPath
from typing import Protocol

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol.execution.domain import GitInspection
from vuzol.execution.git import GitError, LocalGit
from vuzol.execution.paths import contained, trusted_root
from vuzol.execution.scaffold import path_is_docs_only, path_is_executable_product
from vuzol.experiments.review import scan_suspicious_patterns
from vuzol.review.domain import (
    FindingSeverity,
    ReviewFinding,
    ReviewVerdict,
    ReviewVerdictKind,
)
from vuzol.review.independent import IndependentReviewError
from vuzol.storage.models import Run, Step, Task, Worktree
from vuzol.storage.records import LeaseToken
from vuzol.storage.types import RiskLevel, StepStatus, WorktreeDeliveryState
from vuzol.workflows.domain import OutcomeKind, StepOutcome
from vuzol.workflows.ports import CancellationContext, StepExecutionRequest

REVIEW_SCHEMA = "result-review.v1"


class IndependentReviewPort(Protocol):
    async def review(
        self,
        *,
        task: Task,
        risk: RiskLevel,
        inspection: GitInspection,
        base_commit: str,
        result_commit: str,
        diff_hash: str | None,
        gates: list[object],
        mechanical_findings: tuple[ReviewFinding, ...],
        request_ids: tuple[uuid.UUID, uuid.UUID, uuid.UUID],
        timeout_seconds: float,
        cancellation: CancellationContext,
        lease: LeaseToken,
    ) -> ReviewVerdict: ...


_BLOCKING_CLASSIFICATIONS = frozenset(
    {
        "forced_success",
        "coverage_weakening",
        "shell_execution",
        "broad_cleanup",
    }
)
_WARNING_CLASSIFICATIONS = frozenset(
    {
        "exception_swallowing",
        "arbitrary_skip",
        "ignore_added",
        "cleanup_error_assertion",
    }
)
_RISK_ORDER = {
    RiskLevel.LOW: 0,
    RiskLevel.MEDIUM: 1,
    RiskLevel.HIGH: 2,
    RiskLevel.PRIVILEGED: 3,
}
_PRIVILEGED_PATH_PARTS = frozenset(
    {"ansible", "deploy", "deployment", "helm", "infra", "k8s", "systemd", "terraform"}
)
_HIGH_RISK_PATH_PARTS = frozenset(
    {
        "github",
        "alembic",
        "auth",
        "credentials",
        "migration",
        "migrations",
        "permissions",
        "security",
        "secrets",
    }
)
_HIGH_RISK_FILENAMES = frozenset(
    {
        "dockerfile",
        "package-lock.json",
        "pnpm-lock.yaml",
        "poetry.lock",
        "pyproject.toml",
        "requirements.txt",
        "uv.lock",
    }
)
_BUILD_FILENAMES = frozenset(
    {"dockerfile", "justfile", "makefile", "taskfile.yml", "taskfile.yaml"}
)


class ResultReviewHandler:
    """System-owned review bound to a completed validate predecessor."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        git: LocalGit,
        *,
        worktree_root: Path,
        independent_reviewer: IndependentReviewPort | None = None,
    ) -> None:
        self._factory = session_factory
        self._git = git
        self._worktree_root = trusted_root(worktree_root, create=False)
        self._independent = independent_reviewer

    async def execute(
        self, request: StepExecutionRequest, cancellation: CancellationContext
    ) -> StepOutcome:
        try:
            verdict = await self._review(request, cancellation)
        except (GitError, LookupError, ValueError, IndependentReviewError) as error:
            category = (
                "independent_review_required"
                if isinstance(error, IndependentReviewError)
                else "review_failed"
            )
            return StepOutcome(
                kind=OutcomeKind.BLOCKED,
                result={},
                category=category,
                summary=str(error)[:500],
                unknown_effects=False,
            )
        if not verdict.allows_progress:
            return StepOutcome(
                kind=OutcomeKind.BLOCKED,
                result=verdict.as_step_result(),
                category=_blocked_category(verdict),
                summary=verdict.summary[:500],
                unknown_effects=False,
            )
        return StepOutcome.succeeded(verdict.as_step_result())

    async def _review(
        self, request: StepExecutionRequest, cancellation: CancellationContext
    ) -> ReviewVerdict:
        async with self._factory() as session:
            step = await session.get(Step, request.step_id)
            run = await session.get(Run, request.run_id)
            task = await session.get(Task, request.task_id)
            if step is None or run is None or task is None:
                raise LookupError("review step is missing task or run state")
            if (
                step.status not in {StepStatus.LEASED, StepStatus.RUNNING}
                or step.lease_owner != request.lease.owner
                or step.lease_generation != request.lease.generation
                or step.run_id != request.run_id
                or run.task_id != request.task_id
            ):
                raise ValueError("review step is not bound to the current fenced lease")

            validate = await self._require_validate_predecessor(session, step)
            worktree = await session.scalar(
                select(Worktree).where(
                    Worktree.run_id == request.run_id,
                    Worktree.task_id == request.task_id,
                )
            )
            if worktree is None:
                raise LookupError("review requires a prepared worktree")
            if worktree.delivery_state not in {
                WorktreeDeliveryState.WORKTREE_RETAINED,
                WorktreeDeliveryState.APPLIED,
            }:
                raise ValueError("worktree is not retained for review")
            if not worktree.result_commit or not worktree.base_commit:
                raise ValueError("review requires a measured result commit")

            path = contained(self._worktree_root, Path(worktree.path))
            risk = effective_risk(task)
            base_commit = worktree.base_commit
            result_commit = worktree.result_commit
            diff_hash = worktree.diff_hash
            branch = worktree.branch
            structured = (
                validate.result.get("structured_output")
                if isinstance(validate.result, dict)
                else None
            )
            if not isinstance(structured, dict):
                raise ValueError("validate predecessor has no structured validation output")
            gates = structured.get("gates")
            if not isinstance(gates, list) or not gates:
                raise ValueError("validate predecessor has no gate evidence")
            if any(not isinstance(gate, dict) or gate.get("exit_code") != 0 for gate in gates):
                raise ValueError("validate predecessor did not pass all gates")
            if structured.get("result_commit") != result_commit:
                raise ValueError("validate result commit does not match retained worktree")
            if structured.get("base_commit") != base_commit:
                raise ValueError("validate base commit does not match retained worktree")
            bound_task = task

        await self._git.require_clean_worktree(path)
        await self._git.require_no_remotes(path)
        inspection = await self._git.inspect(path, base_commit)
        if inspection.head != result_commit:
            raise ValueError("worktree HEAD does not match the retained result commit")
        if inspection.branch != branch:
            raise ValueError("worktree branch does not match the prepared task branch")
        risk = runtime_risk(risk, inspection)

        findings = (
            *mechanical_findings(inspection.diff),
            *unexpected_file_findings(bound_task, inspection),
        )
        blockers = tuple(item for item in findings if item.severity is FindingSeverity.BLOCKER)
        warnings = tuple(item for item in findings if item.severity is FindingSeverity.WARNING)
        measured_diff_hash = diff_hash or inspection.diff_hash
        if blockers:
            return ReviewVerdict(
                verdict=ReviewVerdictKind.BLOCKED,
                review_kind="mechanical",
                risk=risk.value,
                base_commit=base_commit,
                result_commit=result_commit,
                diff_hash=measured_diff_hash,
                changed_files=inspection.changed_files,
                findings=findings,
                summary=f"Mechanical review blocked: {blockers[0].classification}.",
            )

        requires_independent = risk in {RiskLevel.HIGH, RiskLevel.PRIVILEGED}
        if requires_independent:
            if self._independent is None:
                raise IndependentReviewError(
                    "high or privileged risk requires an independent model reviewer, "
                    "but none is configured on this worker"
                )
            return await self._independent.review(
                task=bound_task,
                risk=risk,
                inspection=inspection,
                base_commit=base_commit,
                result_commit=result_commit,
                diff_hash=measured_diff_hash,
                gates=list(gates),
                mechanical_findings=findings,
                request_ids=(request.task_id, request.run_id, request.step_id),
                timeout_seconds=request.timeout_seconds,
                cancellation=cancellation,
                lease=request.lease,
            )

        if warnings:
            return ReviewVerdict(
                verdict=ReviewVerdictKind.PASSED_WITH_WARNINGS,
                review_kind="mechanical",
                risk=risk.value,
                base_commit=base_commit,
                result_commit=result_commit,
                diff_hash=measured_diff_hash,
                changed_files=inspection.changed_files,
                findings=findings,
                summary=(
                    f"Mechanical review passed with {len(warnings)} warning(s) "
                    f"for {len(inspection.changed_files)} changed path(s)."
                ),
            )
        return ReviewVerdict(
            verdict=ReviewVerdictKind.PASSED,
            review_kind="mechanical",
            risk=risk.value,
            base_commit=base_commit,
            result_commit=result_commit,
            diff_hash=measured_diff_hash,
            changed_files=inspection.changed_files,
            findings=(),
            summary=(
                f"Mechanical review passed for {len(inspection.changed_files)} changed path(s); "
                "validation evidence is present."
            ),
        )

    async def _require_validate_predecessor(self, session: AsyncSession, step: Step) -> Step:
        repaired_validation_id = step.payload.get("repair_validation_step_id")
        if isinstance(repaired_validation_id, str):
            try:
                validation_id = uuid.UUID(repaired_validation_id)
            except ValueError as error:
                raise ValueError("review repair validation binding is invalid") from error
            predecessor = await session.scalar(
                select(Step).where(Step.id == validation_id, Step.run_id == step.run_id)
            )
            if predecessor is None or predecessor.step_type != "validate":
                raise ValueError("review repair validation binding is missing")
            if predecessor.status is not StepStatus.COMPLETED:
                raise ValueError("review repair validation is not completed")
            return predecessor
        predecessors = step.dependency_metadata.get("predecessor_ordinals", [])
        if not isinstance(predecessors, list) or len(predecessors) != 1:
            raise ValueError("review requires exactly one validate predecessor")
        predecessor = await session.scalar(
            select(Step).where(
                Step.run_id == step.run_id,
                Step.ordinal == int(predecessors[0]),
            )
        )
        if predecessor is None or predecessor.step_type != "validate":
            raise ValueError("review predecessor must be a validate step")
        if predecessor.status is not StepStatus.COMPLETED:
            raise ValueError("validate predecessor is not completed")
        assert isinstance(predecessor, Step)
        return predecessor


def mechanical_findings(diff: bytes) -> tuple[ReviewFinding, ...]:
    signals = scan_suspicious_patterns({"worker.diff": diff.decode("utf-8", "replace")})
    findings: list[ReviewFinding] = []
    for signal in signals:
        if signal.classification in _BLOCKING_CLASSIFICATIONS:
            severity = FindingSeverity.BLOCKER
        elif signal.classification in _WARNING_CLASSIFICATIONS:
            severity = FindingSeverity.WARNING
        else:
            severity = FindingSeverity.INFO
        findings.append(
            ReviewFinding(
                severity=severity,
                classification=signal.classification,
                summary=signal.excerpt or signal.classification,
                path=signal.path if signal.path != "worker.diff" else None,
                line=signal.line,
            )
        )
    return tuple(findings)


def unexpected_file_findings(task: Task, inspection: GitInspection) -> tuple[ReviewFinding, ...]:
    """Warn on new executable/build paths outside a narrow, explicitly named request.

    TaskDraft does not provide an enforceable allowed-path contract. This therefore
    activates only when task text explicitly names one changed path, or names only
    documentation paths. It is approval-visible context, never a scope blocker.
    """

    candidates = tuple(
        path for path in inspection.added_files if _is_executable_script_build_or_ci(path)
    )
    if not candidates:
        return ()
    scope_text = _task_scope_text(task)
    mentioned = tuple(
        path for path in inspection.changed_files if _path_is_explicitly_named(scope_text, path)
    )
    if not mentioned:
        return ()
    if len(mentioned) != 1 and not all(path_is_docs_only(path) for path in mentioned):
        return ()
    named = set(mentioned)
    return tuple(
        ReviewFinding(
            severity=FindingSeverity.WARNING,
            classification="unexpected_executable_file",
            summary=(
                "New executable, script, build, or CI path was not explicitly named in the "
                "narrow task request."
            ),
            path=path,
        )
        for path in candidates
        if path not in named
    )


def _task_scope_text(task: Task) -> str:
    raw_draft = getattr(task, "task_draft", None)
    draft = raw_draft if isinstance(raw_draft, dict) else {}
    values: list[str] = []
    original_text = getattr(task, "original_text", None)
    if isinstance(original_text, str):
        values.append(original_text)
    for key in ("goal", "task_summary", "normalized_title"):
        value = draft.get(key)
        if isinstance(value, str):
            values.append(value)
    for key in ("requested_outcomes", "constraints"):
        value = draft.get(key)
        if isinstance(value, (list, tuple)):
            values.extend(item for item in value if isinstance(item, str))
    return "\n".join(values)


def _path_is_explicitly_named(text: str, path: str) -> bool:
    candidate = PurePosixPath(path)
    names = {path, candidate.name}
    if path_is_docs_only(path) and candidate.suffix:
        names.add(candidate.stem)
    return any(
        re.search(rf"(?<![\w/-]){re.escape(name)}(?![\w/-]|\.\w)", text, re.IGNORECASE)
        for name in names
        if len(name) >= 3
    )


def _is_executable_script_build_or_ci(path: str) -> bool:
    candidate = PurePosixPath(path)
    return (
        path_is_executable_product(path)
        or candidate.name.casefold() in _BUILD_FILENAMES
        or tuple(part.casefold() for part in candidate.parts[:2]) == (".github", "workflows")
    )


def effective_risk(task: Task) -> RiskLevel:
    """Use the higher of persisted task risk and draft-suggested risk."""

    candidates = [task.risk]
    draft = task.task_draft if isinstance(task.task_draft, dict) else {}
    raw = draft.get("suggested_risk")
    if isinstance(raw, str):
        with suppress(ValueError):
            candidates.append(RiskLevel(raw))
    return max(candidates, key=lambda value: _RISK_ORDER[value])


def runtime_risk(current: RiskLevel, inspection: GitInspection) -> RiskLevel:
    """Escalate persisted risk from measured paths and diff scope; never downgrade."""

    measured = RiskLevel.LOW
    normalized_paths = tuple(path.lower() for path in inspection.changed_files)
    path_parts = {part for path in normalized_paths for part in re.split(r"[/._-]+", path) if part}
    filenames = {path.rsplit("/", 1)[-1] for path in normalized_paths}
    if path_parts & _PRIVILEGED_PATH_PARTS:
        measured = RiskLevel.PRIVILEGED
    elif (
        path_parts & _HIGH_RISK_PATH_PARTS
        or filenames & _HIGH_RISK_FILENAMES
        or len(inspection.changed_files) > 20
        or len(inspection.diff) > 16_000
    ):
        measured = RiskLevel.HIGH
    elif len(inspection.changed_files) > 5 or len(inspection.diff) > 4_000:
        measured = RiskLevel.MEDIUM
    return max((current, measured), key=lambda value: _RISK_ORDER[value])


def _blocked_category(verdict: ReviewVerdict) -> str:
    if any(finding.classification == "independent_review_required" for finding in verdict.findings):
        return "independent_review_required"
    if verdict.verdict is ReviewVerdictKind.CHANGES_REQUIRED:
        return "review_changes_required"
    return "review_blocked"
