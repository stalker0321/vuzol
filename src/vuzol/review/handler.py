"""Deterministic mechanical review for the coding.v1 review step.

Independent LLM review for high/privileged risk is not yet implemented and
fails closed. Medium risk may advance after mechanical inspection only.
"""

from __future__ import annotations

from contextlib import suppress
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol.execution.git import GitError, LocalGit
from vuzol.execution.paths import contained, trusted_root
from vuzol.experiments.review import scan_suspicious_patterns
from vuzol.review.domain import (
    FindingSeverity,
    ReviewFinding,
    ReviewVerdict,
    ReviewVerdictKind,
)
from vuzol.storage.models import Run, Step, Task, Worktree
from vuzol.storage.types import RiskLevel, StepStatus, WorktreeDeliveryState
from vuzol.workflows.domain import OutcomeKind, StepOutcome
from vuzol.workflows.ports import CancellationContext, StepExecutionRequest

REVIEW_SCHEMA = "result-review.v1"

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


class ResultReviewHandler:
    """System-owned mechanical review bound to a completed validate predecessor."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        git: LocalGit,
        *,
        worktree_root: Path,
    ) -> None:
        self._factory = session_factory
        self._git = git
        self._worktree_root = trusted_root(worktree_root, create=False)

    async def execute(
        self, request: StepExecutionRequest, cancellation: CancellationContext
    ) -> StepOutcome:
        del cancellation
        try:
            verdict = await self._review(request)
        except (GitError, LookupError, ValueError) as error:
            return StepOutcome(
                kind=OutcomeKind.BLOCKED,
                result={},
                category="review_failed",
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

    async def _review(self, request: StepExecutionRequest) -> ReviewVerdict:
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

        await self._git.require_clean_worktree(path)
        await self._git.require_no_remotes(path)
        inspection = await self._git.inspect(path, base_commit)
        if inspection.head != result_commit:
            raise ValueError("worktree HEAD does not match the retained result commit")
        if inspection.branch != branch:
            raise ValueError("worktree branch does not match the prepared task branch")

        if risk in {RiskLevel.HIGH, RiskLevel.PRIVILEGED}:
            return ReviewVerdict(
                verdict=ReviewVerdictKind.BLOCKED,
                review_kind="mechanical",
                risk=risk.value,
                base_commit=base_commit,
                result_commit=result_commit,
                diff_hash=diff_hash,
                changed_files=inspection.changed_files,
                findings=(
                    ReviewFinding(
                        severity=FindingSeverity.BLOCKER,
                        classification="independent_review_required",
                        summary=(
                            "High or privileged risk requires an independent model reviewer "
                            "which is not yet enabled."
                        ),
                    ),
                ),
                summary="Independent review is required and is not yet implemented.",
            )

        findings = mechanical_findings(inspection.diff)
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


def effective_risk(task: Task) -> RiskLevel:
    """Use the higher of persisted task risk and draft-suggested risk."""

    candidates = [task.risk]
    draft = task.task_draft if isinstance(task.task_draft, dict) else {}
    raw = draft.get("suggested_risk")
    if isinstance(raw, str):
        with suppress(ValueError):
            candidates.append(RiskLevel(raw))
    return max(candidates, key=lambda value: _RISK_ORDER[value])


def _blocked_category(verdict: ReviewVerdict) -> str:
    if any(finding.classification == "independent_review_required" for finding in verdict.findings):
        return "independent_review_required"
    if verdict.verdict is ReviewVerdictKind.CHANGES_REQUIRED:
        return "review_changes_required"
    return "review_blocked"
