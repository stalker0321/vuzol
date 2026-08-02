"""Offline contract evaluation for the default-off project-discussion interpreter."""

from __future__ import annotations

import json
from collections import Counter
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field

from vuzol.interpretation.discussion import (
    DiscussionInterpretation,
    DiscussionInterpretRequest,
    EditSessionContext,
    enforce_discussion_policy,
)
from vuzol.storage.types import InteractionMode

DISCUSSION_EVALUATION_VERSION = "discussion-eval-v1"


class EvaluationModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class DiscussionEvaluationFixture(EvaluationModel):
    id: str = Field(min_length=1, max_length=100)
    category: str = Field(min_length=1, max_length=100)
    text: str = Field(min_length=1, max_length=20_000)
    project_id: str = "vuzol"
    user_id: int = 42
    transcription_uncertain: bool = False
    edit_session: EditSessionContext | None = None
    candidate: DiscussionInterpretation
    expected_mode: InteractionMode
    expect_refusal: bool = False


class DiscussionEvaluationReport(EvaluationModel):
    version: str
    total: int
    passed: int
    pass_rate: float
    failures_by_category: dict[str, int]
    mode_mismatches: int
    false_task_creation_violations: int
    authoritative_control_violations: int
    illegal_plan_mutation_violations: int
    edit_fence_violations: int
    policy_contract_passed: bool


def load_discussion_fixtures(path: Path) -> tuple[DiscussionEvaluationFixture, ...]:
    raw = json.loads(path.read_text())
    if not isinstance(raw, list):
        raise ValueError("discussion fixtures must be a JSON list")
    return tuple(DiscussionEvaluationFixture.model_validate(item) for item in raw)


def evaluate_discussion_fixtures(
    fixtures: tuple[DiscussionEvaluationFixture, ...],
) -> DiscussionEvaluationReport:
    categories: Counter[str] = Counter()
    violation_counts: Counter[str] = Counter()
    passed = 0
    for fixture in fixtures:
        request = DiscussionInterpretRequest(
            original_input=fixture.text,
            project_id=fixture.project_id,
            user_id=fixture.user_id,
            transcription_uncertain=fixture.transcription_uncertain,
            edit_session=fixture.edit_session,
        )
        result = enforce_discussion_policy(request, fixture.candidate)
        failures: set[str] = set()
        if result.interaction_mode is not fixture.expected_mode:
            failures.add("mode_mismatch")
        if result.should_create_task:
            failures.add("false_task_creation")
        if result.plan_control is not None and result.plan_control.authoritative:
            failures.add("authoritative_control")
        if result.should_mutate_plan and result.interaction_mode not in {
            InteractionMode.PLAN_REQUEST,
            InteractionMode.ITEM_EDIT,
        }:
            failures.add("illegal_plan_mutation")
        if fixture.edit_session is not None:
            edit = result.item_edit
            if (
                edit is None
                or edit.edit_session_id != fixture.edit_session.edit_session_id
                or edit.package_id != fixture.edit_session.package_id
                or edit.item_id != fixture.edit_session.item_id
            ):
                failures.add("edit_fence")
        if fixture.expect_refusal and result.refusal_code is None:
            failures.add("mode_mismatch")
        if failures:
            categories[fixture.category] += 1
            violation_counts.update(failures)
        else:
            passed += 1
    total = len(fixtures)
    pass_rate = passed / total if total else 0.0
    safety_clean = not any(
        violation_counts[name]
        for name in (
            "false_task_creation",
            "authoritative_control",
            "illegal_plan_mutation",
            "edit_fence",
        )
    )
    return DiscussionEvaluationReport(
        version=DISCUSSION_EVALUATION_VERSION,
        total=total,
        passed=passed,
        pass_rate=pass_rate,
        failures_by_category=dict(categories),
        mode_mismatches=violation_counts["mode_mismatch"],
        false_task_creation_violations=violation_counts["false_task_creation"],
        authoritative_control_violations=violation_counts["authoritative_control"],
        illegal_plan_mutation_violations=violation_counts["illegal_plan_mutation"],
        edit_fence_violations=violation_counts["edit_fence"],
        policy_contract_passed=total > 0 and pass_rate == 1 and safety_clean,
    )
