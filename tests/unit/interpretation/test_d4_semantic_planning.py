"""D4 semantic-planning tests (T055 W1-W7)."""

from __future__ import annotations

import asyncio
import uuid

import pytest

from vuzol.discussion.domain import (
    DomainError,
    PlanDraft,
    PlanItemDraft,
    validate_plan_dependencies,
)
from vuzol.interpretation import decisions as semantic_decisions
from vuzol.interpretation.domain import TaskDraft
from vuzol.interpretation.explicit import is_explicit_task_command
from vuzol.interpretation.policy import enforce_interpretation_policy
from vuzol.interpretation.provenance import coerce_legacy_plan_item, task_source_refs
from vuzol.interpretation.service import RECOVERY_MAX_FALLBACKS, RECOVERY_MAX_REPAIRS

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


def test_explicit_task_command_is_recognised_only_when_explicit() -> None:
    assert is_explicit_task_command("/task fix the bug")
    assert is_explicit_task_command("task: fix the bug")
    assert not is_explicit_task_command("maybe discuss the bug?")
    assert not is_explicit_task_command(None)


def test_stale_candidate_is_rejected() -> None:
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
