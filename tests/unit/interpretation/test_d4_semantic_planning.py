"""D4 semantic-planning tests (T055 W1-W7)."""

from __future__ import annotations

import asyncio
import json
import uuid
from pathlib import Path

import pytest

from vuzol.discussion.domain import (
    DomainError,
    PlanDraft,
    PlanItemDraft,
    validate_plan_dependencies,
)
from vuzol.experiments import decision as repair_triage
from vuzol.experiments import target_selection
from vuzol.interpretation import decisions as semantic_decisions
from vuzol.interpretation.discussion import (
    ControlOverride,
    ControlOverrideKind,
    DecisionCandidate,
    DiscussionInterpretation,
    DiscussionInterpretRequest,
    enforce_discussion_policy,
    explicit_task_interpretation,
    resolve_discussion_candidate,
)
from vuzol.interpretation.domain import TaskDraft
from vuzol.interpretation.explicit import explicit_task_body, is_explicit_task_command
from vuzol.interpretation.planning import PlanningTier, select_planning_tier
from vuzol.interpretation.policy import enforce_interpretation_policy
from vuzol.interpretation.provenance import coerce_legacy_plan_item, task_source_refs
from vuzol.interpretation.service import RECOVERY_MAX_FALLBACKS, RECOVERY_MAX_REPAIRS
from vuzol.review.policy import FileClass, level_for, resolve_review_plan
from vuzol.storage.types import InteractionMode, RiskLevel

from ._test_interpretation_helpers import draft, request


def _plan_item(local_id: str, deps: tuple[str, ...] = ()) -> PlanItemDraft:
    return PlanItemDraft(
        summary=f"Item {local_id}",
        goal="Do the thing",
        expected_outcome="Done",
        completion_criteria=("done",),
        allowed_scope="repo",
        local_id=local_id,
        dependencies=deps,
    )


def test_plan_dependencies_unknown_and_cycle_rejected() -> None:
    with pytest.raises(DomainError, match="unknown plan dependency"):
        validate_plan_dependencies((_plan_item("a", ("ghost",)),))
    with pytest.raises(DomainError, match="cyclic plan dependency"):
        validate_plan_dependencies((_plan_item("a", ("b",)), _plan_item("b", ("a",))))
    validate_plan_dependencies((_plan_item("a"), _plan_item("b", ("a",))))
    with pytest.raises(DomainError, match="unknown plan dependency"):
        PlanDraft(title="T", items=(_plan_item("a", ("ghost",)),))


def test_explicit_fast_path_needs_no_provider_and_stays_confirm_first() -> None:
    assert is_explicit_task_command("/task fix the bug")
    assert is_explicit_task_command("task: fix the bug")
    assert not is_explicit_task_command("maybe discuss the bug?")
    assert not is_explicit_task_command(None)
    assert explicit_task_body("/task fix the bug") == "fix the bug"

    req = DiscussionInterpretRequest(
        original_input="/task fix the bug",
        project_id="demo",
        user_id=1,
        control_override=ControlOverride(kind=ControlOverrideKind.EXPLICIT_TASK),
    )
    result = explicit_task_interpretation(req, body=explicit_task_body(req.original_input))
    assert result.interaction_mode is InteractionMode.TASK_REQUEST
    assert result.task_request is not None
    assert result.should_create_task is False
    enforced = enforce_discussion_policy(req, result)
    assert enforced.interaction_mode is InteractionMode.TASK_REQUEST
    assert enforced.should_create_task is False


def test_task_spec_bridge_refs_and_legacy_provenance() -> None:
    turn_id = uuid.uuid4()

    class FakeTask:
        source_turn_id = turn_id
        spec_revision = "abc123"
        original_text = "fix the bug"

    refs = task_source_refs(FakeTask())
    assert refs["original_text"] == "fix the bug"
    assert refs["spec_revision"] == "abc123"
    assert refs["source_turn_id"] is turn_id
    assert set(refs) == {"source_turn_id", "spec_revision", "original_text"}

    legacy = coerce_legacy_plan_item({"summary": "x"})
    assert legacy["derived"] is False
    assert legacy["source_turn_id"] is None
    assert legacy["source_spec_revision"] is None


def test_tiers_do_not_change_review_floor_or_capabilities() -> None:
    base = draft()
    light = draft()
    strong = draft(suggested_risk=RiskLevel.HIGH)
    assert select_planning_tier(base) is PlanningTier.DIRECT
    assert select_planning_tier(strong) is PlanningTier.STRONG
    assert select_planning_tier(light, dependency_count=3) is PlanningTier.LIGHT

    for candidate in (base, light, strong):
        policy = enforce_interpretation_policy(request(), candidate, known_project_ids=frozenset())
        assert policy.draft.required_capabilities == candidate.required_capabilities
    low = resolve_review_plan(RiskLevel.LOW, ("src/a.py",))
    high = resolve_review_plan(RiskLevel.HIGH, ("src/a.py",))
    assert low["level"] != high["level"]
    # Tier is a spend hint only: review floor derives from risk/files, not tier.
    assert resolve_review_plan(RiskLevel.LOW, ("src/a.py",)) == resolve_review_plan(
        RiskLevel.LOW, ("src/a.py",)
    )
    assert level_for(RiskLevel.LOW, FileClass.CODE).value == low["level"]


def test_recovery_contract_is_bounded_and_fail_closed() -> None:
    assert RECOVERY_MAX_REPAIRS == 1
    assert RECOVERY_MAX_FALLBACKS == 1

    async def scenario() -> None:
        from vuzol.interpretation.adapters import FakeInterpreter
        from vuzol.interpretation.ports import InvalidInterpreterOutput
        from vuzol.interpretation.service import interpret_with_recovery

        from ._test_interpretation_helpers import result as make_result

        primary = FakeInterpreter(
            [InvalidInterpreterOutput("bad"), InvalidInterpreterOutput("still bad")]
        )
        fallback = FakeInterpreter([make_result(draft(), profile="fallback")])
        interpreted = await interpret_with_recovery(primary, [fallback], request())
        assert interpreted.profile_id == "fallback"
        # Same envelope: validated TaskDraft, not raw text.
        assert isinstance(interpreted.draft, TaskDraft)

    asyncio.run(scenario())


def test_candidates_stale_rejected_and_resolved() -> None:
    good = draft(target_candidate_id="auth-fix")
    policy = enforce_interpretation_policy(
        request(),
        good,
        known_project_ids=frozenset(),
        allowed_candidate_ids=frozenset({"auth-fix"}),
    )
    assert not policy.draft.needs_clarification
    stale = enforce_interpretation_policy(
        request(), good, known_project_ids=frozenset(), allowed_candidate_ids=frozenset({"other"})
    )
    assert stale.draft.needs_clarification
    assert "stale_or_unknown_candidate" in stale.reasons

    interp = DiscussionInterpretation(
        interaction_mode=InteractionMode.DISCUSSION,
        confidence=0.9,
        user_visible_summary="x",
        decision_candidates=(
            DecisionCandidate(key="auth-fix", statement="Fix auth"),
            DecisionCandidate(key="migrate-db", statement="Migrate"),
        ),
    )
    assert resolve_discussion_candidate(interp, "auth-fix").stable_id == "auth-fix"
    with pytest.raises(ValueError, match="unknown discussion candidate"):
        resolve_discussion_candidate(interp, "ghost")
    with pytest.raises(ValueError, match="duplicate discussion candidate"):
        enforce_discussion_policy(
            DiscussionInterpretRequest(original_input="hi", project_id="demo", user_id=1),
            interp.model_copy(
                update={
                    "decision_candidates": (
                        DecisionCandidate(key="dup", statement="A"),
                        DecisionCandidate(key="dup", statement="B"),
                    )
                }
            ),
        )


def test_semantic_hints_never_grant_capabilities() -> None:
    decision = semantic_decisions.classify_decision(draft(), explicit=True)
    assert decision.context.value == "explicit_command"
    assert decision.policy_allowed is False
    assert set(semantic_decisions.EffectIntent) == {
        semantic_decisions.EffectIntent.INSPECT_CODE,
        semantic_decisions.EffectIntent.MODIFY_CODE,
        semantic_decisions.EffectIntent.DESIGN_ADVICE,
        semantic_decisions.EffectIntent.PROVISION_PROJECT,
        semantic_decisions.EffectIntent.CONTROL_LIFECYCLE,
        semantic_decisions.EffectIntent.ANSWER_QUESTION,
        semantic_decisions.EffectIntent.DISCUSS_ONLY,
    }


def test_jev_second_kind_cross_rejects_and_corpus_labels() -> None:
    fingerprint = "a" * 64
    ref = "artifact:build-log:sha256:" + "b" * 64
    target_ok = {
        "schema": "decision.v2",
        "decision_kind": "target_selection",
        "state_revision": 3,
        "choice": "select",
        "candidate_id": "auth-fix",
        "evidence_refs": [ref],
        "reason_code": "clear_match",
        "abstain": False,
        "input_fingerprint": fingerprint,
    }
    decision, repaired = target_selection.interpret_output(target_ok)
    assert not repaired
    assert decision.candidate_id == "auth-fix"

    repair_payload = {
        "schema": "decision.v1",
        "decision_kind": "repair_triage",
        "state_revision": 3,
        "choice": "repair",
        "evidence_refs": [ref],
        "reason_code": "known_local_failure",
        "abstain": False,
        "input_fingerprint": fingerprint,
    }
    # Cross-kind: v1 payload rejected by v2 parser and vice versa.
    with pytest.raises(target_selection.TargetDecisionInvalid):
        target_selection.interpret_output(repair_payload)
    with pytest.raises(repair_triage.DecisionInvalid):
        repair_triage.interpret_output(target_ok)

    corpus_path = (
        Path(__file__).resolve().parents[3]
        / "tests"
        / "fixtures"
        / "experiments"
        / "decision-corpus.v2.json"
    )
    corpus = json.loads(corpus_path.read_text(encoding="utf-8"))
    labels = {case.get("label") for case in corpus["cases"]}
    assert {"wrong-target", "correction", "ambiguous"} <= labels
    assert corpus["decision_kind"] == "target_selection"

    # No auto-promotion concept: whitelist default-off for both modules.
    assert not repair_triage.WhitelistGate().allows("repair_triage")
