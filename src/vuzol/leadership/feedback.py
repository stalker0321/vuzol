"""Bounded, scoped result data for the lead. Text is evidence, never authority."""

from __future__ import annotations

import json
from typing import Any

from sqlalchemy import func, or_, select, tuple_
from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.leadership.evidence import GOAL_HEAD_CHECKER, _current_head
from vuzol.leadership.persistence import ManagedGoal, ManagedGoalEvidence
from vuzol.storage.models import MaterializationLink, PlanRevision, Run, Step, WorkPackage

MAX_CONTEXT_BYTES = 16_000
MAX_ROWS = 8


def no_change_claim(structured: dict[str, Any]) -> dict[str, Any]:
    """Project the bounded executor claim without presenting it as verified fact."""

    if structured.get("no_change") is not True:
        return {}
    reason = structured.get("no_change_reason")
    return {
        "no_change": True,
        "no_change_reason": reason[:2_000] if isinstance(reason, str) else "",
        "no_change_reason_untrusted": True,
    }


def _text(value: object, limit: int = 512) -> str | None:
    if not isinstance(value, str):
        return None
    return value if len(value) <= limit else value[:limit] + "…[truncated]"


def _fields(raw: object, names: tuple[str, ...]) -> dict[str, Any]:
    if not isinstance(raw, dict):
        return {}
    return {name: _text(raw[name]) for name in names if isinstance(raw.get(name), str)}


def _strings(raw: object) -> list[str]:
    return (
        [_text(item, 240) or "" for item in raw[:4] if isinstance(item, str)]
        if isinstance(raw, list)
        else []
    )


def _omitted(raw: object) -> int:
    return max(0, len(raw) - 4) if isinstance(raw, list) else 0


def _plan(raw: object) -> dict[str, Any] | None:
    if not isinstance(raw, dict):
        return None
    items = raw.get("items")
    items = items if isinstance(items, list) else []
    return {
        "title": _text(raw.get("title"), 240),
        "items": [
            {
                **_fields(
                    item, ("local_id", "summary", "goal", "expected_outcome", "allowed_scope")
                ),
                "dependencies": _strings(item.get("dependencies")),
                "completion_criteria": _strings(item.get("completion_criteria")),
                "omitted_criteria": _omitted(item.get("completion_criteria")),
            }
            for item in items[:4]
            if isinstance(item, dict)
        ],
        "omitted_items": max(0, len(items) - 4),
    }


def _report(raw: object) -> dict[str, Any] | None:
    if not isinstance(raw, dict) or not isinstance(raw.get("sources"), list):
        return None
    return {
        **_fields(raw, ("summary",)),
        **{key: _strings(raw.get(key)) for key in ("findings", "sources", "limitations")},
        **{
            f"omitted_{key}": _omitted(raw.get(key))
            for key in ("findings", "sources", "limitations")
        },
    }


def _put(context: dict[str, Any], key: str, value: object) -> None:
    """Omit whole sections explicitly, never emit broken/truncated JSON."""
    candidate = {**context, key: value}
    if len(json.dumps(candidate, ensure_ascii=False).encode()) <= MAX_CONTEXT_BYTES - 512:
        context[key] = value
    else:
        context["truncated"] = True


async def result_context(
    session: AsyncSession, goal: ManagedGoal, package: WorkPackage | None
) -> dict[str, Any]:
    if package is not None and (
        package.managed_goal_id != goal.id or package.id != goal.current_package_id
    ):
        package = None
    context: dict[str, Any] = {
        "binding": {
            "goal_id": str(goal.id),
            "scope_revision": goal.scope_revision,
            "package_id": str(package.id) if package else None,
            "result_commit": package.integration_head_commit if package else None,
        },
        "criterion_reviews": [],
        "steps": [],
        "truncated": False,
    }
    if package is not None:
        context["package"] = {
            "id": str(package.id),
            "status": package.status.value,
            "version": package.version,
            "pause_reason": package.pause_reason.value if package.pause_reason else None,
            "failure_task_id": str(package.last_failure_task_id)
            if package.last_failure_task_id
            else None,
        }
    head = await _current_head(session, goal, check_live_head=True) if package else None
    total = int(
        await session.scalar(
            select(func.count())
            .select_from(ManagedGoalEvidence)
            .where(ManagedGoalEvidence.goal_id == goal.id)
        )
        or 0
    )
    matched = 0
    if head is not None:
        wanted = [(item["id"], item["description"]) for item in goal.criteria]
        predicates = (
            ManagedGoalEvidence.goal_id == goal.id,
            ManagedGoalEvidence.scope_revision == goal.scope_revision,
            ManagedGoalEvidence.result_commit == head,
            tuple_(ManagedGoalEvidence.criterion_id, ManagedGoalEvidence.description).in_(wanted),
        )
        matched = int(
            await session.scalar(
                select(func.count()).select_from(ManagedGoalEvidence).where(*predicates)
            )
            or 0
        )
        rows = (
            await session.execute(
                select(
                    ManagedGoalEvidence.id,
                    ManagedGoalEvidence.criterion_id,
                    ManagedGoalEvidence.result_commit,
                    ManagedGoalEvidence.scope_revision,
                    ManagedGoalEvidence.checker,
                    ManagedGoalEvidence.artifact_digest,
                    ManagedGoalEvidence.observation["verdict"].label("verdict"),
                )
                .where(*predicates)
                .order_by(ManagedGoalEvidence.satisfied, ManagedGoalEvidence.criterion_id)
                .limit(MAX_ROWS)
            )
        ).all()
        for row in rows:
            verdict = row.verdict if row.checker == GOAL_HEAD_CHECKER else {}
            verdict = verdict if isinstance(verdict, dict) else {}
            review = {
                "evidence_id": str(row.id),
                "criterion_id": row.criterion_id,
                "result_commit": row.result_commit,
                "scope_revision": row.scope_revision,
                "artifact_digest": row.artifact_digest,
                "verdict": _text(verdict.get("verdict"), 20) or "pending",
                "explanation": _text(verdict.get("explanation"), 1200),
                "evidence_paths": _strings(verdict.get("evidence_paths")),
                "omitted_evidence_paths": _omitted(verdict.get("evidence_paths")),
            }
            _put(context, "criterion_reviews", [*context["criterion_reviews"], review])
        context["truncated"] |= matched > MAX_ROWS
    context["stale_evidence_count"] = total - matched
    context["evidence_binding"] = "current" if head is not None else "no_current_completed_head"
    if package is None:
        return context
    revision_id = package.head_revision_id
    revision = await session.get(PlanRevision, revision_id) if revision_id else None
    if revision is not None and revision.work_package_id == package.id:
        _put(
            context,
            "plan",
            {
                "revision_id": str(revision.id),
                "content_hash": revision.content_hash,
                "plan": _plan(revision.immutable_body),
            },
        )
    # Explicit projection: no arbitrary payload, owner identity, paths to the
    # host repository, credentials, full logs or captured source files.
    rows = (
        await session.execute(
            select(
                Step.id,
                Step.run_id,
                Step.ordinal,
                Step.step_type,
                Step.status,
                Step.failure_category,
                Step.failure_summary,
                Step.unknown_effects,
                Step.result["implementation_summary"].label("implementation_summary"),
                Step.result["summary"].label("summary"),
                Step.result["structured_output"].label("structured"),
                Run.task_id,
                Run.status.label("run_status"),
                Run.failure_category.label("run_failure_category"),
                Run.failure_summary.label("run_failure_summary"),
            )
            .join(Run, Run.id == Step.run_id)
            .join(MaterializationLink, MaterializationLink.task_id == Run.task_id)
            .where(
                MaterializationLink.work_package_id == package.id,
                MaterializationLink.plan_revision_id == revision_id,
                or_(
                    Step.step_type.in_(("execute_code", "execute_agent", "review", "validate")),
                    Step.failure_category.is_not(None),
                ),
            )
            .order_by(Step.updated_at.desc(), Step.ordinal.desc(), Step.id)
            .limit(MAX_ROWS + 1)
        )
    ).all()
    context["truncated"] |= len(rows) > MAX_ROWS
    for row in rows[:MAX_ROWS]:
        structured = row.structured if isinstance(row.structured, dict) else {}
        review = _fields(structured, ("verdict", "summary", "result_commit", "diff_hash"))
        findings = structured.get("findings", [])
        review["findings"] = (
            [
                _fields(
                    item,
                    (
                        "title",
                        "description",
                        "summary",
                        "message",
                        "path",
                        "file",
                        "severity",
                        "category",
                        "suggestion",
                    ),
                )
                for item in findings[:4]
            ]
            if isinstance(findings, list)
            else []
        )
        review["omitted_findings"] = _omitted(findings)
        review_commit = structured.get("result_commit")
        review["binding"] = (
            "current_head"
            if review_commit and review_commit == head
            else "historical_commit"
            if review_commit
            else "run_only"
        )
        _put(
            context,
            "steps",
            [
                *context["steps"],
                {
                    "step_id": str(row.id),
                    "run_id": str(row.run_id),
                    "task_id": str(row.task_id),
                    "ordinal": row.ordinal,
                    "type": row.step_type,
                    "status": row.status.value,
                    **no_change_claim(structured),
                    "run_status": row.run_status.value,
                    "implementation_summary": _text(
                        row.implementation_summary or structured.get("implementation_summary")
                    ),
                    "summary": _text(row.summary),
                    "failure": {
                        "category": row.failure_category or row.run_failure_category,
                        "summary": _text(row.failure_summary or row.run_failure_summary),
                        "unknown_effects": row.unknown_effects,
                    },
                    "review": review,
                    "report": _report(structured.get("report", structured)),
                    "report_hash": _text(structured.get("report_hash"), 64),
                },
            ],
        )
    return context
