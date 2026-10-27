"""D4 REDO tests: system-stamped provenance, readers, and missing coverage."""

from __future__ import annotations

import inspect
import uuid
from types import SimpleNamespace
from typing import Any

import pytest

from vuzol.discussion.domain import PlanDraft, PlanItemDraft, canonical_plan_body
from vuzol.discussion.service import WorkPackageService
from vuzol.interpretation.decisions import classify_decision
from vuzol.interpretation.discussion import ControlOverrideKind
from vuzol.interpretation.policy import enforce_interpretation_policy
from vuzol.interpretation.provenance import (
    extract_plan_item_source,
    provenance_reference,
)
from vuzol.interpretation.service import (
    RECOVERY_MAX_FALLBACKS,
    RECOVERY_MAX_REPAIRS,
    interpret_with_recovery,
)
from vuzol.review.handler import _task_scope_text
from vuzol.storage.types import RiskLevel
from vuzol.telegram.work_packages import ContinueDiscussionOverrides

from ._test_interpretation_helpers import (
    FakeInterpreter,
    InterpreterUnavailable,
    InvalidInterpreterOutput,
    TaskAction,
    asyncio,
    draft,
    request,
    result,
)


def test_extract_plan_item_source() -> None:
    turn_id = uuid.uuid4()
    body = {
        "title": "Plan",
        "items": [
            {
                "item_id": str(uuid.uuid4()),
                "ordinal": 1,
                "derived": True,
                "source_turn_ref": str(turn_id),
                "source_spec_revision": "rev1",
            },
            {"item_id": str(uuid.uuid4()), "ordinal": 2},
        ],
    }
    assert extract_plan_item_source(body, 1) == (turn_id, "rev1")
    assert extract_plan_item_source(body, 2) == (None, None)
    assert extract_plan_item_source(body, 9) == (None, None)
    assert extract_plan_item_source({}, 1) == (None, None)
    forged = {
        "items": [{"ordinal": 1, "derived": True, "source_turn_ref": "not-a-uuid"}],
    }
    assert extract_plan_item_source(forged, 1) == (None, None)
    unmarked = {
        "items": [{"ordinal": 1, "derived": False, "source_turn_ref": str(turn_id)}],
    }
    assert extract_plan_item_source(unmarked, 1) == (None, None)


def test_provenance_reference_reader() -> None:
    turn_id = uuid.uuid4()
    task_id = uuid.uuid4()
    linked = SimpleNamespace(id=task_id, source_turn_id=turn_id)
    assert provenance_reference(linked) == f"task:{task_id}:original:turn:{turn_id}"
    legacy = SimpleNamespace(id=task_id, source_turn_id=None)
    assert provenance_reference(legacy) == f"task:{task_id}:original"


def test_reviewer_scope_shows_derived_marker() -> None:
    turn_id = uuid.uuid4()
    derived = SimpleNamespace(
        original_text="fix it",
        task_draft={"goal": "fix it"},
        source_turn_id=turn_id,
        spec_revision="rev9",
    )
    scope = _task_scope_text(derived)  # type: ignore[arg-type]
    assert f"[derived from turn {turn_id}]" in scope
    assert "[spec revision rev9]" in scope
    legacy = SimpleNamespace(
        original_text="fix it", task_draft={}, source_turn_id=None, spec_revision=None
    )
    assert "[provenance unknown]" in _task_scope_text(legacy)  # type: ignore[arg-type]


def test_policy_decision_is_bound_to_verdict() -> None:
    eligible = enforce_interpretation_policy(request(), draft(), known_project_ids=frozenset())
    assert eligible.decision.policy_allowed is True
    assert eligible.decision.policy_allowed == eligible.automatic_execution_eligible
    assert eligible.decision.effect is classify_decision(draft()).effect

    risky = enforce_interpretation_policy(
        request(),
        draft(suggested_risk=RiskLevel.HIGH),
        known_project_ids=frozenset(),
    )
    assert risky.draft.needs_clarification
    assert risky.decision.policy_allowed is False

    control = enforce_interpretation_policy(
        request(),
        draft(action=TaskAction.APPROVE_STEP),
        known_project_ids=frozenset(),
    )
    assert control.decision.policy_allowed is False
    assert "natural_language_control_never_consumes_approval" in control.reasons


@pytest.mark.anyio
async def test_explicit_override_arm_and_consume() -> None:
    overrides = ContinueDiscussionOverrides()
    await overrides.arm(
        chat_id=-100,
        thread_id=7,
        user_id=42,
        kind=ControlOverrideKind.EXPLICIT_TASK,
    )
    assert (
        await overrides.consume(chat_id=-100, thread_id=7, user_id=42)
        is ControlOverrideKind.EXPLICIT_TASK
    )
    assert await overrides.consume(chat_id=-100, thread_id=7, user_id=42) is None


def test_stop_package_awaits_no_retrieval() -> None:
    sources = [
        inspect.getsource(WorkPackageService.stop_package),
        inspect.getsource(WorkPackageService._cancel_current_task),
    ]
    for source in sources:
        lowered = source.lower()
        assert "retriev" not in lowered
        assert "scout" not in lowered


def test_materialize_order_independent_of_row_order() -> None:
    def item(local_id: str, deps: tuple[str, ...] = ()) -> PlanItemDraft:
        return PlanItemDraft(
            summary=f"Item {local_id}",
            goal="Do the thing",
            expected_outcome="Done",
            completion_criteria=("done",),
            allowed_scope="repo",
            local_id=local_id,
            dependencies=deps,
        )

    forward = PlanDraft(title="T", items=(item("a"), item("b", ("a",))))
    backward = PlanDraft(title="T", items=(item("b", ("a",)), item("a")))
    forward_body = canonical_plan_body(forward, (uuid.uuid4(), uuid.uuid4()))
    backward_body = canonical_plan_body(backward, (uuid.uuid4(), uuid.uuid4()))
    assert [row["local_id"] for row in forward_body["items"]] == ["a", "b"]
    assert [row["local_id"] for row in backward_body["items"]] == ["a", "b"]
    assert [row["ordinal"] for row in forward_body["items"]] == [1, 2]
    assert [row["ordinal"] for row in backward_body["items"]] == [1, 2]
    # Dependencies resolve by stable local_id, never by row position.
    assert forward_body["items"][1]["dependencies"] == ["a"]
    assert backward_body["items"][1]["dependencies"] == ["a"]


def test_shared_recovery_budget_single_ledger() -> None:
    assert RECOVERY_MAX_REPAIRS == 1
    assert RECOVERY_MAX_FALLBACKS == 1

    async def scenario() -> None:
        primary = FakeInterpreter(
            [InvalidInterpreterOutput("bad"), InvalidInterpreterOutput("still bad")]
        )
        first_fallback = FakeInterpreter([result(draft(), profile="fallback")])
        second_fallback = FakeInterpreter([result(draft(), profile="never")])
        observed: list[dict[str, object]] = []

        async def observer(**kwargs: object) -> None:
            observed.append(kwargs)

        interpreted = await interpret_with_recovery(
            primary,
            [first_fallback, second_fallback],
            request(),
            on_attempt=observer,
        )
        assert interpreted.profile_id == "fallback"
        # One shared ledger across primary repair and fallback attempts.
        assert [entry["attempt_kind"] for entry in observed] == [
            "initial",
            "repair",
            "retry",
        ]
        assert [entry["outcome"] for entry in observed] == [
            "invalid_output",
            "repair_failed",
            "succeeded",
        ]
        # Bounded: the second fallback is never spent.
        assert second_fallback.requests == []

    asyncio.run(scenario())


def test_rate_limit_and_outage_offline() -> None:
    async def scenario() -> None:
        limited = FakeInterpreter([InterpreterUnavailable("rate_limited")])
        fallback = FakeInterpreter([result(draft(), profile="fallback")])
        recovered = await interpret_with_recovery(limited, [fallback], request())
        assert recovered.profile_id == "fallback"

        down = FakeInterpreter([InterpreterUnavailable("outage")])
        with pytest.raises(InterpreterUnavailable, match="all_interpreters_unavailable"):
            await interpret_with_recovery(down, [], request())

        poisoned = FakeInterpreter([InvalidInterpreterOutput("bad")] * 3)
        bad_fallback = FakeInterpreter([InvalidInterpreterOutput("bad")])
        with pytest.raises(InterpreterUnavailable, match="all_interpreters_unavailable"):
            await interpret_with_recovery(poisoned, [bad_fallback], request())

    asyncio.run(scenario())


def test_false_act_and_wrong_binding_tracked_separately() -> None:
    cases: list[tuple[str, dict[str, Any], dict[str, Any], bool, bool]] = [
        # (label, draft_kwargs, policy_kwargs, expect_execute, expect_binding_ok)
        (
            "clean",
            {},
            {},
            True,
            True,
        ),
        (
            "high-risk-needs-confirm",
            {"suggested_risk": RiskLevel.HIGH},
            {},
            False,
            True,
        ),
        (
            "wrong-continuation-binding",
            {"action": TaskAction.CONTINUE_TASK, "referenced_task_id": uuid.uuid4()},
            {},
            False,
            False,
        ),
        (
            "wrong-candidate-binding",
            {"target_candidate_id": "ghost"},
            {},
            False,
            False,
        ),
        (
            "stale-candidate-clarifies",
            {"target_candidate_id": "auth-fix"},
            {"allowed_candidate_ids": frozenset({"other"})},
            False,
            False,
        ),
        (
            "known-candidate-executes",
            {"target_candidate_id": "auth-fix"},
            {"allowed_candidate_ids": frozenset({"auth-fix"})},
            True,
            True,
        ),
    ]
    false_act = 0
    wrong_binding = 0
    agreements = 0
    for _label, draft_kwargs, policy_kwargs, expect_execute, expect_binding_ok in cases:
        policy = enforce_interpretation_policy(
            request(),
            draft(**draft_kwargs),
            known_project_ids=frozenset(),
            **policy_kwargs,
        )
        executed = policy.automatic_execution_eligible
        binding_ok = "unsupported_task_binding" not in policy.reasons and (
            "stale_or_unknown_candidate" not in policy.reasons
        )
        agreements += executed == expect_execute and binding_ok == expect_binding_ok
        if executed and not expect_execute:
            false_act += 1
        if binding_ok != expect_binding_ok:
            wrong_binding += 1
    assert agreements == len(cases)
    assert false_act == 0
    assert wrong_binding == 0
