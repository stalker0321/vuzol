"""Independent model review for high/privileged coding results.

Read-only: the reviewer receives truncated diff text and metadata only — never
repository write access, sandbox mounts, or production secrets beyond the
scoped API credential for the reviewer profile.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from collections.abc import Sequence
from decimal import Decimal
from typing import Any, Protocol

from vuzol.config.models import LaunchMode, ProviderProfileConfig, ProviderRole
from vuzol.config.registries import ConfigurationBundle
from vuzol.execution.domain import GitInspection
from vuzol.providers.domain import (
    ContextItem,
    ProviderRequest,
    ProviderResult,
    ProviderResultStatus,
)
from vuzol.providers.errors import ProviderFailure
from vuzol.providers.ports import ProviderAdapter
from vuzol.review.domain import (
    FindingSeverity,
    ReviewFinding,
    ReviewVerdict,
    ReviewVerdictKind,
)
from vuzol.storage.models import Task
from vuzol.storage.types import RiskLevel
from vuzol.workflows.ports import CancellationContext

INDEPENDENT_REVIEW_SCHEMA = "independent-review.v1"
_PROMPT_REVISION = "independent-review-v1"
_MAX_DIFF_CHARS = 16_000
_MAX_FILES = 80

_OUTPUT_JSON_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": ["verdict", "summary", "findings"],
    "properties": {
        "verdict": {
            "type": "string",
            "enum": ["pass", "pass_with_warnings", "changes_required", "blocked"],
        },
        "summary": {"type": "string", "minLength": 1, "maxLength": 2000},
        "findings": {
            "type": "array",
            "maxItems": 40,
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["severity", "classification", "summary"],
                "properties": {
                    "severity": {
                        "type": "string",
                        "enum": ["info", "warning", "error", "blocker"],
                    },
                    "classification": {"type": "string", "minLength": 1, "maxLength": 100},
                    "summary": {"type": "string", "minLength": 1, "maxLength": 500},
                    "path": {"type": ["string", "null"], "maxLength": 500},
                    "line": {"type": ["integer", "null"], "minimum": 1},
                },
            },
        },
    },
}


class IndependentReviewError(Exception):
    """Independent review could not complete safely."""


class AdapterLookup(Protocol):
    def get(self, profile_id: str) -> ProviderAdapter: ...


def select_reviewer_profile(
    profiles: Sequence[ProviderProfileConfig],
) -> ProviderProfileConfig | None:
    """Pick the cheapest eligible OpenAI-compatible API reviewer profile."""

    def eligible(role: ProviderRole) -> list[ProviderProfileConfig]:
        return [
            profile
            for profile in profiles
            if profile.enabled
            and profile.provider == "openai-compatible"
            and profile.launch_mode is LaunchMode.API
            and role in profile.roles
            and profile.api_base_url is not None
        ]

    reviewers = eligible(ProviderRole.REVIEWER)
    if reviewers:
        return min(reviewers, key=lambda item: (item.routing_priority, item.id))
    # Planner API profiles are acceptable read-only reviewers when no dedicated
    # reviewer role is configured (same transport, no sandbox).
    planners = eligible(ProviderRole.PLANNER)
    if planners:
        return min(planners, key=lambda item: (item.routing_priority, item.id))
    return None


class IndependentModelReviewer:
    """Call a model-only profile to produce a structured independent verdict."""

    def __init__(
        self,
        registries: ConfigurationBundle,
        adapters: AdapterLookup,
        *,
        policy_revision: str = "independent-review-policy.v1",
    ) -> None:
        self._registries = registries
        self._adapters = adapters
        self._policy_revision = policy_revision

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
    ) -> ReviewVerdict:
        profile = select_reviewer_profile(self._registries.profiles.items())
        if profile is None:
            raise IndependentReviewError(
                "no openai-compatible reviewer or planner profile is configured"
            )
        try:
            adapter = self._adapters.get(profile.id)
        except Exception as error:  # adapter registry raises LookupError/KeyError
            raise IndependentReviewError(
                f"reviewer profile adapter is unavailable: {profile.id}"
            ) from error

        task_id, run_id, step_id = request_ids
        provider_request = _build_request(
            task=task,
            risk=risk,
            inspection=inspection,
            base_commit=base_commit,
            result_commit=result_commit,
            diff_hash=diff_hash,
            gates=gates,
            mechanical_findings=mechanical_findings,
            task_id=task_id,
            run_id=run_id,
            step_id=step_id,
            timeout_seconds=timeout_seconds,
            profile=profile,
            policy_revision=self._policy_revision,
        )
        try:
            result = await adapter.execute(provider_request, profile, cancellation)
        except ProviderFailure as failure:
            raise IndependentReviewError(failure.safe_summary) from failure
        if result.status is not ProviderResultStatus.SUCCEEDED:
            raise IndependentReviewError("independent reviewer did not succeed")
        return _verdict_from_provider_result(
            result,
            risk=risk,
            base_commit=base_commit,
            result_commit=result_commit,
            diff_hash=diff_hash or inspection.diff_hash,
            changed_files=inspection.changed_files,
            profile_id=profile.id,
            mechanical_findings=mechanical_findings,
        )


def _build_request(
    *,
    task: Task,
    risk: RiskLevel,
    inspection: GitInspection,
    base_commit: str,
    result_commit: str,
    diff_hash: str | None,
    gates: list[object],
    mechanical_findings: tuple[ReviewFinding, ...],
    task_id: uuid.UUID,
    run_id: uuid.UUID,
    step_id: uuid.UUID,
    timeout_seconds: float,
    profile: ProviderProfileConfig,
    policy_revision: str,
) -> ProviderRequest:
    files = tuple(inspection.changed_files[:_MAX_FILES])
    diff_text = inspection.diff.decode("utf-8", "replace")
    truncated = len(diff_text) > _MAX_DIFF_CHARS
    if truncated:
        diff_text = diff_text[:_MAX_DIFF_CHARS] + "\n…[diff truncated for independent review]…"
    goal = ""
    draft = task.task_draft if isinstance(task.task_draft, dict) else {}
    for key in ("goal", "normalized_title", "summary"):
        raw = draft.get(key)
        if isinstance(raw, str) and raw.strip():
            goal = raw.strip()[:1_000]
            break
    if not goal and isinstance(task.original_text, str):
        goal = task.original_text.strip()[:1_000]

    payload = {
        "instruction": (
            "You are an independent code reviewer. The change already passed trusted "
            "validation gates. Decide whether the retained result is safe to present for "
            "human apply approval. Be conservative for high/privileged risk. Do not claim "
            "to have executed tools. Return only the required JSON object."
        ),
        "risk": risk.value,
        "goal": goal,
        "base_commit": base_commit,
        "result_commit": result_commit,
        "diff_hash": diff_hash or inspection.diff_hash,
        "changed_files": list(files),
        "changed_file_count": len(inspection.changed_files),
        "diff_truncated": truncated,
        "gates": gates[:20],
        "mechanical_findings": [
            finding.model_dump(mode="json") for finding in mechanical_findings[:20]
        ],
        "diff": diff_text,
    }
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    content_hash = hashlib.sha256(encoded.encode()).hexdigest()
    return ProviderRequest(
        task_id=task_id,
        run_id=run_id,
        step_id=step_id,
        provider_attempt=1,
        role=ProviderRole.REVIEWER,
        original_input_reference=f"task:{task_id}:review",
        original_input=goal or "independent coding result review",
        task_draft={
            "task_type": "coding",
            "suggested_risk": risk.value,
            "review_mode": "independent",
        },
        context=(
            ContextItem(
                source="retained_result_review_bundle",
                reference=f"worktree-diff:{result_commit[:12]}",
                content=encoded[:20_000],
                content_hash=content_hash,
            ),
        ),
        output_schema_name="IndependentReviewReport",
        output_schema_version=INDEPENDENT_REVIEW_SCHEMA,
        output_json_schema=_OUTPUT_JSON_SCHEMA,
        system_policy_revision=policy_revision,
        prompt_revision=_PROMPT_REVISION,
        timeout_seconds=min(float(timeout_seconds), 600.0),
        deadline=None,
        max_input_tokens=min(int(profile.context_limit or 32_000), 32_000),
        max_output_tokens=min(int(profile.output_limit or 2_000), 2_000),
        reserved_cost_units=Decimal("0"),
        reserved_quota_units=Decimal("0"),
        sandbox_reference=None,
    )


def _verdict_from_provider_result(
    result: ProviderResult,
    *,
    risk: RiskLevel,
    base_commit: str,
    result_commit: str,
    diff_hash: str | None,
    changed_files: tuple[str, ...],
    profile_id: str,
    mechanical_findings: tuple[ReviewFinding, ...],
) -> ReviewVerdict:
    structured = result.structured_output
    if not isinstance(structured, dict):
        raise IndependentReviewError("independent reviewer returned no structured output")
    try:
        verdict_kind = ReviewVerdictKind(str(structured["verdict"]))
        summary = str(structured["summary"]).strip()
        raw_findings = structured.get("findings") or []
        if not isinstance(raw_findings, list):
            raise TypeError("findings must be a list")
        findings: list[ReviewFinding] = []
        for item in raw_findings:
            if not isinstance(item, dict):
                continue
            findings.append(
                ReviewFinding(
                    severity=FindingSeverity(str(item["severity"])),
                    classification=str(item["classification"])[:100],
                    summary=str(item["summary"])[:500],
                    path=(str(item["path"])[:500] if item.get("path") is not None else None),
                    line=int(item["line"]) if isinstance(item.get("line"), int) else None,
                )
            )
    except (KeyError, TypeError, ValueError) as error:
        raise IndependentReviewError(
            "independent reviewer output failed schema interpretation"
        ) from error

    # Mechanical blockers already short-circuit before this path; still surface
    # mechanical warnings next to independent findings for the approval card.
    merged = tuple((*mechanical_findings, *findings))
    if verdict_kind is ReviewVerdictKind.PASSED and any(
        item.severity in {FindingSeverity.WARNING, FindingSeverity.ERROR} for item in merged
    ):
        verdict_kind = ReviewVerdictKind.PASSED_WITH_WARNINGS
    if not summary:
        summary = f"Independent review via {profile_id}: {verdict_kind.value}."
    summary = f"[{profile_id}] {summary}"[:2_000]
    return ReviewVerdict(
        verdict=verdict_kind,
        review_kind="independent",
        risk=risk.value,
        base_commit=base_commit,
        result_commit=result_commit,
        diff_hash=diff_hash,
        changed_files=changed_files,
        findings=merged,
        summary=summary,
    )
