"""Structured review verdicts for Step 09."""

from enum import StrEnum
from typing import Any

from pydantic import Field

from vuzol.experiments.domain import FrozenModel


class ReviewVerdictKind(StrEnum):
    PASSED = "pass"
    PASSED_WITH_WARNINGS = "pass_with_warnings"
    CHANGES_REQUIRED = "changes_required"
    BLOCKED = "blocked"


class FindingSeverity(StrEnum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    BLOCKER = "blocker"


class ReviewFinding(FrozenModel):
    severity: FindingSeverity
    classification: str = Field(min_length=1, max_length=100)
    summary: str = Field(min_length=1, max_length=500)
    path: str | None = Field(default=None, max_length=500)
    line: int | None = Field(default=None, ge=1)


class ReviewVerdict(FrozenModel):
    schema_version: str = "result-review.v1"
    verdict: ReviewVerdictKind
    review_kind: str = Field(pattern=r"^(mechanical|independent)$")
    risk: str = Field(pattern=r"^(low|medium|high|privileged)$")
    base_commit: str = Field(pattern=r"^[0-9a-f]{40,64}$")
    result_commit: str = Field(pattern=r"^[0-9a-f]{40,64}$")
    diff_hash: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    changed_files: tuple[str, ...] = ()
    findings: tuple[ReviewFinding, ...] = ()
    summary: str = Field(min_length=1, max_length=2_000)

    @property
    def allows_progress(self) -> bool:
        return self.verdict in {
            ReviewVerdictKind.PASSED,
            ReviewVerdictKind.PASSED_WITH_WARNINGS,
        }

    def as_step_result(self) -> dict[str, Any]:
        payload = self.model_dump(mode="json")
        return {
            **payload,
            "structured_output": payload,
            "implementation_summary": self.summary,
        }
