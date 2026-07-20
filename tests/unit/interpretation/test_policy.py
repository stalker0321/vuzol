"""Policy tests (split for cohesion)."""

from __future__ import annotations

from ._test_interpretation_helpers import (
    Capability,
    FakeInterpreter,
    InterpreterUnavailable,
    InvalidInterpreterOutput,
    ProjectNameOption,
    RiskLevel,
    TaskAction,
    TaskContext,
    TaskOperation,
    TopicKind,
    asyncio,
    draft,
    enforce_interpretation_policy,
    interpret_with_recovery,
    name_options,
    pytest,
    regenerate_project_names,
    request,
    result,
    uuid,
)


def test_policy_rejects_unknown_project_and_raises_privileged_risk() -> None:
    value = draft(
        project_id="invented",
        required_capabilities=frozenset({Capability.HOST_ADMIN}),
    )
    policy = enforce_interpretation_policy(request(), value, known_project_ids=frozenset({"vuzol"}))
    assert policy.draft.project_id is None
    assert policy.draft.suggested_risk is RiskLevel.PRIVILEGED
    assert policy.draft.needs_clarification
    assert not policy.automatic_execution_eligible


def test_topic_mapping_remains_authoritative_when_interpreter_registry_lags() -> None:
    contextual = request().model_copy(
        update={"topic_kind": TopicKind.PROJECT, "mapped_project_id": "bill-buddy"}
    )
    policy = enforce_interpretation_policy(
        contextual,
        draft(project_id="bill-buddy"),
        known_project_ids=frozenset({"vuzol"}),
    )

    assert policy.draft.project_id == "bill-buddy"
    assert not policy.draft.needs_clarification
    assert "unknown_project" not in policy.reasons


def test_uncertain_dangerous_voice_requires_confirmation() -> None:
    policy = enforce_interpretation_policy(
        request(voice=True, uncertain=True),
        draft(suggested_risk=RiskLevel.HIGH),
        known_project_ids=frozenset(),
    )
    assert policy.draft.needs_clarification
    assert "uncertain_dangerous_transcription" in policy.reasons


def test_policy_supplies_mapped_project_and_blocks_contradictory_control() -> None:
    contextual = request().model_copy(update={"mapped_project_id": "vuzol"})
    policy = enforce_interpretation_policy(
        contextual,
        draft(action=TaskAction.APPROVE_STEP, contradiction_detected=True),
        known_project_ids=frozenset({"vuzol"}),
    )
    assert policy.draft.project_id == "vuzol"
    assert policy.draft.needs_clarification
    assert "contradictory_interpretation" in policy.reasons
    assert "natural_language_control_never_consumes_approval" in policy.reasons


def test_policy_rejects_unsupported_continuation_binding() -> None:
    known_task = uuid.uuid4()
    invented_task = uuid.uuid4()
    contextual = request().model_copy(
        update={"active_tasks": (TaskContext(task_id=known_task, title="Known"),)}
    )
    policy = enforce_interpretation_policy(
        contextual,
        draft(action=TaskAction.CONTINUE_TASK, referenced_task_id=invented_task),
        known_project_ids=frozenset(),
    )
    assert policy.draft.needs_clarification
    assert "unsupported_task_binding" in policy.reasons

    reply_context = request().model_copy(
        update={"reply_linked_task": TaskContext(task_id=known_task, title="Known")}
    )
    supported = enforce_interpretation_policy(
        reply_context,
        draft(action=TaskAction.CONTINUE_TASK, referenced_task_id=known_task),
        known_project_ids=frozenset(),
    )
    assert not supported.draft.needs_clarification
    assert supported.automatic_execution_eligible


def test_policy_requires_confirmation_for_project_mismatch_and_high_risk() -> None:
    contextual = request().model_copy(update={"mapped_project_id": "vuzol"})
    policy = enforce_interpretation_policy(
        contextual,
        draft(project_id="other", suggested_risk=RiskLevel.HIGH),
        known_project_ids=frozenset({"vuzol", "other"}),
    )
    assert policy.draft.project_id == "vuzol"
    assert policy.draft.needs_clarification
    assert "project_topic_mismatch" in policy.reasons


def test_policy_requires_confirmation_for_high_risk_text() -> None:
    policy = enforce_interpretation_policy(
        request(),
        draft(suggested_risk=RiskLevel.HIGH),
        known_project_ids=frozenset(),
    )
    assert policy.draft.needs_clarification
    assert "dangerous_interpretation_confirmation" in policy.reasons


def test_invalid_output_gets_one_repair_then_fallback() -> None:
    async def scenario() -> None:
        primary = FakeInterpreter(
            [InvalidInterpreterOutput("bad"), InvalidInterpreterOutput("still bad")]
        )
        fallback = FakeInterpreter([result(draft(), profile="fallback")])
        interpreted = await interpret_with_recovery(primary, [fallback], request())
        assert interpreted.profile_id == "fallback"
        assert len(primary.requests) == 2
        assert primary.requests[1][1] == "bad"

    asyncio.run(scenario())


def test_project_name_regeneration_rejects_reused_names_and_uses_fallback() -> None:
    async def scenario() -> None:
        previous = frozenset(option.project_id for option in name_options())
        reused = draft(
            action=TaskAction.CREATE_PROJECT,
            operation=TaskOperation.CREATE,
            project_name_options=name_options(),
        )
        fresh_options = tuple(
            ProjectNameOption(display_name=f"Fresh {index}", project_id=f"fresh-{index}")
            for index in range(1, 10)
        )
        fresh = draft(
            action=TaskAction.CREATE_PROJECT,
            operation=TaskOperation.CREATE,
            project_name_options=fresh_options,
        )
        primary = FakeInterpreter([result(reused)])
        fallback = FakeInterpreter([result(fresh, profile="fallback")])
        regenerated = await regenerate_project_names(
            primary,
            [fallback],
            request(),
            previous_project_ids=previous,
        )
        assert regenerated.profile_id == "fallback"
        assert "notes-1" in (primary.requests[0][1] or "")

        unavailable = FakeInterpreter([InterpreterUnavailable("offline")])
        with pytest.raises(InterpreterUnavailable, match="all_interpreters_unavailable"):
            await regenerate_project_names(
                unavailable,
                [],
                request(),
                previous_project_ids=previous,
            )

    asyncio.run(scenario())
