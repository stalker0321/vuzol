import asyncio
import uuid
from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from vuzol.discussion.memory import MemoryPack, MemoryTurn
from vuzol.interpretation.discussion import (
    AmbiguityFlag,
    ControlOverride,
    ControlOverrideKind,
    DiscussionInterpretation,
    DiscussionInterpretationService,
    DiscussionInterpretRequest,
    EditSessionContext,
    ItemEditPayload,
    PlanControlAction,
    PlanControlPayload,
    PlanSnapshot,
    PlanSnapshotItem,
    RefusalCode,
    TaskRequestPayload,
    enforce_discussion_policy,
    plan_draft_from_interpretation,
)
from vuzol.storage.types import ConversationTurnRole, InteractionMode


def request(**changes: object) -> DiscussionInterpretRequest:
    values: dict[str, object] = {
        "original_input": "давай обсудим идею",
        "project_id": "vuzol",
        "user_id": 42,
    }
    values.update(changes)
    return DiscussionInterpretRequest.model_validate(values)


def envelope(**changes: object) -> DiscussionInterpretation:
    values: dict[str, object] = {
        "interaction_mode": "discussion",
        "confidence": 0.9,
        "user_visible_summary": "Обсуждаем идею.",
    }
    values.update(changes)
    return DiscussionInterpretation.model_validate(values)


def test_all_free_text_modes_create_zero_tasks() -> None:
    candidates = (
        envelope(interaction_mode="discussion", should_create_task=True),
        envelope(interaction_mode="query_only", should_create_task=True),
        envelope(
            interaction_mode="task_request",
            task_request=TaskRequestPayload(summary="Изменить UI", goal="Сделать интерфейс яснее"),
            should_create_task=True,
        ),
    )

    assert all(
        not enforce_discussion_policy(request(), item).should_create_task for item in candidates
    )


def test_low_confidence_prefers_discussion_and_strips_action_payloads() -> None:
    candidate = envelope(
        interaction_mode="task_request",
        confidence=0.2,
        should_create_task=True,
        ambiguity_flags={AmbiguityFlag.LOW_CONFIDENCE},
        task_request={"summary": "Do it", "goal": "Do an unclear thing"},
    )

    result = enforce_discussion_policy(request(), candidate)

    assert result.interaction_mode is InteractionMode.DISCUSSION
    assert result.refusal_code is RefusalCode.DISCUSS_PREFER
    assert result.task_request is None
    assert not result.should_create_task
    assert not result.should_mutate_plan


def test_schema_and_prompt_versions_are_exact_literals() -> None:
    with pytest.raises(ValidationError):
        envelope(schema_version="discussion-interpret-v2")
    with pytest.raises(ValidationError):
        envelope(prompt_version="model-invented")


def test_model_plan_control_is_advisory_and_requires_button() -> None:
    candidate = envelope(
        interaction_mode="plan_control",
        should_create_task=True,
        should_mutate_plan=True,
        plan_control=PlanControlPayload(
            action=PlanControlAction.START,
            authoritative=True,
        ),
    )

    result = enforce_discussion_policy(request(), candidate)

    assert result.plan_control is not None and not result.plan_control.authoritative
    assert result.refusal_code is RefusalCode.CONTROL_REQUIRES_BUTTON
    assert not result.should_create_task
    assert not result.should_mutate_plan


def test_mismatched_payload_is_rejected_and_policy_strips_constructed_bypass() -> None:
    with pytest.raises(ValidationError, match="payload does not match interaction_mode"):
        envelope(
            interaction_mode="discussion",
            plan_control={"action": "start", "authoritative": True},
        )

    bypass = DiscussionInterpretation.model_construct(
        interaction_mode=InteractionMode.DISCUSSION,
        confidence=0.9,
        should_create_task=True,
        should_mutate_plan=True,
        user_visible_summary="Обсуждаем.",
        plan_control=PlanControlPayload(action=PlanControlAction.START, authoritative=True),
    )
    result = enforce_discussion_policy(request(), bypass)

    assert result.plan_control is None
    assert result.refusal_code is RefusalCode.CONTROL_REQUIRES_BUTTON
    assert not result.should_create_task
    assert not result.should_mutate_plan


def test_only_plan_draft_and_fenced_item_edit_may_request_draft_mutation() -> None:
    result = enforce_discussion_policy(
        request(),
        envelope(interaction_mode="query_only", should_mutate_plan=True),
    )

    assert not result.should_mutate_plan


def test_open_edit_session_forces_fenced_item_edit() -> None:
    edit = EditSessionContext(
        edit_session_id=uuid.uuid4(),
        package_id=uuid.uuid4(),
        revision_id=uuid.uuid4(),
        revision_number=2,
        revision_hash="a" * 64,
        item_id=uuid.uuid4(),
        item_local_id="ui",
        opened_by_user_id=42,
    )

    result = enforce_discussion_policy(
        request(original_input="кнопка должна быть короче", edit_session=edit),
        envelope(
            interaction_mode="task_request",
            should_create_task=True,
            task_request={"summary": "Change button", "goal": "Shorten it"},
        ),
    )

    assert result.interaction_mode is InteractionMode.ITEM_EDIT
    assert isinstance(result.item_edit, ItemEditPayload)
    assert result.item_edit.edit_session_id == edit.edit_session_id
    assert result.item_edit.item_id == edit.item_id
    assert result.item_edit.refinement_text == "кнопка должна быть короче"
    assert not result.should_create_task


def test_open_edit_session_replaces_model_invented_fences() -> None:
    edit = EditSessionContext(
        edit_session_id=uuid.uuid4(),
        package_id=uuid.uuid4(),
        revision_number=3,
        revision_hash="b" * 64,
        item_id=uuid.uuid4(),
        opened_by_user_id=42,
    )
    invented = ItemEditPayload(
        edit_session_id=uuid.uuid4(),
        package_id=uuid.uuid4(),
        revision_number=99,
        revision_hash="f" * 64,
        item_id=uuid.uuid4(),
        refinement_text="оставить только фактическое изменение",
    )

    result = enforce_discussion_policy(
        request(edit_session=edit),
        envelope(interaction_mode="item_edit", item_edit=invented),
    )

    assert result.item_edit is not None
    assert result.item_edit.edit_session_id == edit.edit_session_id
    assert result.item_edit.package_id == edit.package_id
    assert result.item_edit.revision_number == edit.revision_number
    assert result.item_edit.item_id == edit.item_id
    assert result.item_edit.refinement_text == invented.refinement_text


def test_uncertain_voice_can_never_create_or_mutate() -> None:
    result = enforce_discussion_policy(
        request(source_is_voice=True, transcription_uncertain=True),
        envelope(should_create_task=True, should_mutate_plan=True),
    )

    assert AmbiguityFlag.VOICE_UNCERTAIN in result.ambiguity_flags
    assert result.refusal_code is RefusalCode.CLARIFY_REQUIRED
    assert not result.should_create_task
    assert not result.should_mutate_plan


def test_request_carries_bounded_memory_plan_snapshot_and_override() -> None:
    session_id = uuid.uuid4()
    value = request(
        memory_pack=MemoryPack(
            session_id=session_id,
            summary=None,
            decisions=(),
            turns=(
                MemoryTurn(
                    id=uuid.uuid4(),
                    ordinal=1,
                    role=ConversationTurnRole.USER,
                    content="обсуждаем карточку",
                    created_at=datetime.now(UTC),
                ),
            ),
            raw_turns_truncated=False,
            summary_truncated=False,
            decisions_truncated=False,
        ),
        plan_snapshot=PlanSnapshot(
            package_id=uuid.uuid4(),
            revision_id=uuid.uuid4(),
            revision_number=2,
            revision_hash="a" * 64,
            title="Telegram UI",
            items=(
                PlanSnapshotItem(
                    item_id=uuid.uuid4(), ordinal=1, local_id="card", summary="Карточка"
                ),
            ),
        ),
        control_override=ControlOverride(kind=ControlOverrideKind.CONTINUE_DISCUSSION),
    )

    dumped = value.model_dump(mode="json")

    assert dumped["memory_pack"]["session_id"] == str(session_id)
    assert dumped["plan_snapshot"]["revision_hash"] == "a" * 64
    assert dumped["control_override"]["kind"] == "continue_discussion"


def test_continue_discussion_override_clears_model_action_payload() -> None:
    result = enforce_discussion_policy(
        request(control_override={"kind": "continue_discussion"}),
        envelope(
            interaction_mode="task_request",
            should_create_task=True,
            task_request={"summary": "Создать задачу", "goal": "Что-то изменить"},
        ),
    )

    assert result.interaction_mode is InteractionMode.DISCUSSION
    assert result.task_request is None
    assert not result.should_create_task


def test_plan_request_maps_to_domain_draft_without_control_authority() -> None:
    result = envelope(
        interaction_mode="plan_request",
        should_mutate_plan=True,
        plan_request={
            "intent": "create_draft",
            "title": "Улучшение интерфейса",
            "items": [
                {
                    "local_id": "telegram-ui",
                    "summary": "Упростить карточку",
                    "goal": "Сделать карточку понятнее",
                    "expected_outcome": "Короткая карточка, понятные действия",
                    "completion_criteria": ["Карточка покрыта тестом"],
                    "allowed_scope": "telegram projections",
                    "suggested_risk": "low",
                    "needs_approval": False,
                    "estimated_complexity": "small",
                }
            ],
        },
    )

    draft = plan_draft_from_interpretation(result)

    assert draft.title == "Улучшение интерфейса"
    assert len(draft.items) == 1
    assert draft.items[0].local_id == "telegram-ui"


def test_revision_plan_request_uses_only_injected_snapshot_fence() -> None:
    snapshot = PlanSnapshot(
        package_id=uuid.uuid4(),
        revision_id=uuid.uuid4(),
        revision_number=4,
        revision_hash="c" * 64,
        title="Current plan",
    )
    candidate = envelope(
        interaction_mode="plan_request",
        should_mutate_plan=True,
        plan_request={
            "intent": "revise_draft",
            "base_revision_id": str(uuid.uuid4()),
            "base_revision_hash": "f" * 64,
            "title": "Revised plan",
            "items": [
                {
                    "local_id": "one",
                    "summary": "Первый пункт",
                    "goal": "Обновить пункт",
                    "expected_outcome": "Пункт обновлён",
                    "completion_criteria": ["Изменение видно"],
                    "allowed_scope": "plan",
                    "suggested_risk": "low",
                    "needs_approval": False,
                    "estimated_complexity": "small",
                }
            ],
        },
    )

    result = enforce_discussion_policy(request(plan_snapshot=snapshot), candidate)

    assert result.plan_request is not None
    assert result.plan_request.base_revision_id == snapshot.revision_id
    assert result.plan_request.base_revision_hash == snapshot.revision_hash

    refused = enforce_discussion_policy(request(), candidate)
    assert refused.interaction_mode is InteractionMode.QUERY_REFUSE
    assert refused.plan_request is None
    assert refused.refusal_code is RefusalCode.PLAN_REVISION_STALE
    assert not refused.should_mutate_plan


def test_fake_pipeline_applies_policy_after_provider_output() -> None:
    class FakeDiscussionInterpreter:
        def __init__(self) -> None:
            self.requests: list[DiscussionInterpretRequest] = []

        async def interpret_discussion(
            self, value: DiscussionInterpretRequest
        ) -> DiscussionInterpretation:
            self.requests.append(value)
            return envelope(
                interaction_mode="plan_control",
                should_mutate_plan=True,
                plan_control={"action": "discard", "authoritative": True},
            )

    async def scenario() -> None:
        interpreter = FakeDiscussionInterpreter()
        service = DiscussionInterpretationService(interpreter)

        result = await service.interpret(request())

        assert len(interpreter.requests) == 1
        assert result.refusal_code is RefusalCode.CONTROL_REQUIRES_BUTTON
        assert result.plan_control is not None and not result.plan_control.authoritative
        assert not result.should_mutate_plan

    asyncio.run(scenario())
