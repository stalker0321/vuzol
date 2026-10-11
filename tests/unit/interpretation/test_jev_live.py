"""Verify real Decisions API mapping, authority boundaries and visible accounting."""

import asyncio
import json
import uuid
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest
from pydantic import SecretStr

from vuzol.config import Capability, TopicKind
from vuzol.interpretation.discussion import DiscussionInterpretRequest, RefusalCode
from vuzol.interpretation.domain import InterpretationInput, TaskAction, TaskContext
from vuzol.interpretation.jev import JEV_ENDPOINT, JevDecisionsInterpreter
from vuzol.interpretation.policy import enforce_interpretation_policy
from vuzol.interpretation.ports import InterpreterUnavailable, InvalidInterpreterOutput
from vuzol.interpretation.service import (
    interpret_discussion_with_recovery,
    interpret_with_recovery,
)
from vuzol.storage.types import InteractionMode


def response(
    effect: str = "execute_request", relation: str = "new", target: str = "no_target"
) -> dict[str, Any]:
    return {
        "model": "typesafe/jev-1.13-20260917",
        "id": "test-request",
        "answers": {
            name: {"type": "choice", "choice": choice}
            for name, choice in {
                "effect": effect,
                "relation": relation,
                "target": target,
                "task_type": "coding",
                "operation": "modify",
            }.items()
        },
        "usage": {"input_tokens": 120, "output_tokens": 0},
    }


def intake(text: str = "Добавь поиск книг", **kwargs: Any) -> InterpretationInput:
    return InterpretationInput(
        original_input=text,
        topic_kind=TopicKind.PROJECT,
        mapped_project_id="books",
        capability_vocabulary=frozenset(),
        **kwargs,
    )


def test_native_endpoint_and_task_request_are_bound_to_jev() -> None:
    async def scenario() -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            assert str(request.url) == JEV_ENDPOINT
            body = json.loads(request.content)
            assert body["model"] == "typesafe/jev-1.13"
            assert body["state"]["current_turn"] == "Добавь поиск книг"
            assert "messages" not in body
            return httpx.Response(200, json=response())

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            adapter = JevDecisionsInterpreter(
                credential=SecretStr("test-key"),
                profile_id="jev",
                admitted=lambda _: True,
                client=client,
            )
            attempts: list[dict[str, Any]] = []

            async def observe(**values: Any) -> None:
                attempts.append(values)

            result = await interpret_with_recovery(adapter, (), intake(), on_attempt=observe)
            assert result.draft.action is TaskAction.CREATE_TASK
            assert Capability.CODE_EDIT in result.draft.required_capabilities
            assert len(attempts) == 1
            assert attempts[0]["input_tokens"] == 120
            assert adapter.receipt is not None
            assert adapter.receipt["effect"] == "execute_request"

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "effect,mode",
    [
        ("respond", InteractionMode.DISCUSSION),
        ("execute_request", InteractionMode.TASK_REQUEST),
        ("status", InteractionMode.QUERY_ONLY),
        ("control_request", InteractionMode.QUERY_REFUSE),
        ("abstain", InteractionMode.QUERY_REFUSE),
    ],
)
def test_discussion_never_authorizes_execution_or_approval(
    effect: str,
    mode: InteractionMode,
) -> None:
    async def scenario() -> None:
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(
                lambda _: httpx.Response(200, json=response(effect=effect)),
            )
        ) as client:
            adapter = JevDecisionsInterpreter(
                credential=SecretStr("test-key"),
                profile_id="jev",
                admitted=lambda _: True,
                client=client,
            )
            attempts: list[dict[str, Any]] = []

            async def observe(**values: Any) -> None:
                attempts.append(values)

            result = await interpret_discussion_with_recovery(
                adapter,
                (),
                DiscussionInterpretRequest(original_input="да", project_id="books", user_id=1),
                on_attempt=observe,
            )
            assert result.interaction_mode is mode
            assert not result.should_create_task
            assert not result.should_mutate_plan
            assert len(attempts) == 1
            assert attempts[0]["input_tokens"] == 120

    asyncio.run(scenario())


def test_semantic_response_cannot_be_upgraded_by_old_keyword_policy() -> None:
    async def scenario() -> None:
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(
                lambda _: httpx.Response(200, json=response(effect="respond")),
            )
        ) as client:
            adapter = JevDecisionsInterpreter(
                credential=SecretStr("test-key"),
                profile_id="jev",
                admitted=lambda _: True,
                client=client,
            )
            request = intake("Объясни команду: добавь поле")
            result = await adapter.interpret(request)
            policy = enforce_interpretation_policy(
                request,
                result.draft,
                known_project_ids=frozenset({"books"}),
                semantic_effect="respond",
            )
            assert not policy.automatic_execution_eligible
            assert Capability.CODE_EDIT not in policy.draft.required_capabilities

    asyncio.run(scenario())


@pytest.mark.parametrize("target", ["unresolved", "no_target"])
def test_reference_cannot_silently_turn_into_new_work(target: str) -> None:
    async def scenario() -> None:
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(
                lambda _: httpx.Response(200, json=response(relation="continue", target=target)),
            )
        ) as client:
            adapter = JevDecisionsInterpreter(
                credential=SecretStr("test-key"),
                profile_id="jev",
                admitted=lambda _: True,
                client=client,
            )
            assert (
                await adapter.interpret(intake("Продолжи предыдущую задачу"))
            ).draft.needs_clarification

    asyncio.run(scenario())


def test_known_task_reference_stays_bound() -> None:
    async def scenario() -> None:
        task_id = uuid.uuid4()
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(
                lambda _: httpx.Response(
                    200, json=response(relation="continue", target=str(task_id))
                ),
            )
        ) as client:
            adapter = JevDecisionsInterpreter(
                credential=SecretStr("test-key"),
                profile_id="jev",
                admitted=lambda _: True,
                client=client,
            )
            result = await adapter.interpret(
                intake(active_tasks=(TaskContext(task_id=task_id, title="Поиск книг"),))
            )
            assert result.draft.referenced_task_id == task_id

    asyncio.run(scenario())


@pytest.mark.parametrize("bad", ["model", "answer", "choice"])
def test_invalid_native_output_fails_closed_without_secret(bad: str) -> None:
    async def scenario() -> None:
        body = response()
        if bad == "model":
            body["model"] = "typesafe/jev-router"
        elif bad == "answer":
            del body["answers"]["effect"]
        else:
            body["answers"]["target"]["choice"] = "forged"
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(
                lambda _: httpx.Response(200, json=body),
            )
        ) as client:
            adapter = JevDecisionsInterpreter(
                credential=SecretStr("test-key"),
                profile_id="jev",
                admitted=lambda _: True,
                client=client,
            )
            with pytest.raises(InvalidInterpreterOutput) as error:
                await adapter.interpret(intake())
            assert "test-key" not in str(error.value)

    asyncio.run(scenario())


def test_kill_switch_rejects_an_in_flight_result() -> None:
    async def scenario() -> None:
        enabled = True

        def handler(_request: httpx.Request) -> httpx.Response:
            nonlocal enabled
            enabled = False
            return httpx.Response(200, json=response())

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            adapter = JevDecisionsInterpreter(
                credential=SecretStr("test-key"),
                profile_id="jev",
                admitted=lambda _: enabled,
                client=client,
            )
            with pytest.raises(InterpreterUnavailable):
                await adapter.interpret(intake())
            assert adapter.receipt is None

    asyncio.run(scenario())


def test_manual_project_intake_does_not_generate_names_or_provision() -> None:
    async def scenario() -> None:
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(
                lambda _: httpx.Response(200, json=response()),
            )
        ) as client:
            adapter = JevDecisionsInterpreter(
                credential=SecretStr("test-key"),
                profile_id="jev",
                admitted=lambda _: True,
                client=client,
                manual_project_creation=True,
            )
            request = InterpretationInput(
                original_input="Создай проект книг",
                topic_kind=TopicKind.INBOX,
                capability_vocabulary=frozenset(),
            )
            result = await adapter.interpret(request)
            assert result.draft.needs_clarification
            assert not result.draft.project_name_options

    asyncio.run(scenario())


@pytest.mark.parametrize("relation", ["null", "new", "continue"])
@pytest.mark.parametrize("topic_kind", [TopicKind.PROJECT, TopicKind.INBOX])
def test_mixed_intake_clarifies_whole_turn_without_partial_action(
    relation: str, topic_kind: TopicKind
) -> None:
    async def scenario() -> None:
        task_id = uuid.uuid4()
        calls: list[dict[str, Any]] = []
        raw = "Останови прогон и измени поиск, только если тесты упали. Пока не запускай новый."

        def handler(request: httpx.Request) -> httpx.Response:
            body = json.loads(request.content)
            calls.append(body)
            assert body["state"]["current_turn"] == raw
            assert "mixed" in body["questions"]["effect"]["criteria"]
            assert "whole current user turn" in body["questions"]["effect"]["instructions"]
            assert (
                "one coherent goal remain execute_request"
                in body["questions"]["effect"]["instructions"]
            )
            return httpx.Response(200, json=response("mixed", relation, str(task_id)))

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            structurer = MagicMock()
            structurer.interpret = AsyncMock(side_effect=AssertionError("mixed must not structure"))
            adapter = JevDecisionsInterpreter(
                credential=SecretStr("test-key"),
                profile_id="jev",
                admitted=lambda _: True,
                client=client,
                structurer=structurer,
            )
            request = intake(raw, active_tasks=(TaskContext(task_id=task_id, title="Поиск"),))
            request = request.model_copy(update={"topic_kind": topic_kind})
            result = await adapter.interpret(request)
            assert result.draft.needs_clarification
            assert not result.draft.required_capabilities
            assert result.draft.action is not TaskAction.CONTINUE_TASK
            assert result.draft.goal == raw
            assert adapter.receipt is not None
            assert adapter.receipt["effect"] == "mixed"
            assert adapter.receipt["relation"] == (None if relation == "null" else relation)
            policy = enforce_interpretation_policy(
                request,
                result.draft,
                known_project_ids=frozenset({"books"}),
                semantic_effect="mixed",
            )
            assert not policy.automatic_execution_eligible
            assert not policy.decision.policy_allowed
            assert "mixed_turn_requires_clarification" in policy.reasons
            assert len(calls) == 1
            structurer.interpret.assert_not_awaited()

    asyncio.run(scenario())


def test_mixed_discussion_cannot_be_overridden_into_partial_plan() -> None:
    from vuzol.interpretation.discussion import ControlOverride, ControlOverrideKind

    async def scenario() -> None:
        calls = 0

        def handler(_request: httpx.Request) -> httpx.Response:
            nonlocal calls
            calls += 1
            return httpx.Response(200, json=response("mixed", "null"))

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            structurer = MagicMock()
            structurer.interpret_discussion = AsyncMock(
                side_effect=AssertionError("mixed must not structure a partial plan")
            )
            adapter = JevDecisionsInterpreter(
                credential=SecretStr("test-key"),
                profile_id="jev",
                admitted=lambda _: True,
                client=client,
                structurer=structurer,
            )
            result = await adapter.interpret_discussion(
                DiscussionInterpretRequest(
                    original_input="Останови прогон и переделай план, пока не запускай",
                    project_id="books",
                    user_id=1,
                    control_override=ControlOverride(kind=ControlOverrideKind.REPLAN),
                )
            )
            assert result.interaction_mode is InteractionMode.QUERY_REFUSE
            assert result.refusal_code is RefusalCode.CLARIFY_REQUIRED
            assert result.clarification_question
            assert not result.should_create_task
            assert not result.should_mutate_plan
            assert result.task_request is None
            assert result.plan_request is None
            assert result.plan_control is None
            assert calls == 1
            structurer.interpret_discussion.assert_not_awaited()

    asyncio.run(scenario())


def test_single_coherent_conditional_goal_keeps_execute_route() -> None:
    async def scenario() -> None:
        raw = (
            "Проверь тесты; если упали, исправь только поиск. "
            "Иначе ничего не меняй и сообщи результат."
        )
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(lambda _: httpx.Response(200, json=response()))
        ) as client:
            adapter = JevDecisionsInterpreter(
                credential=SecretStr("test-key"),
                profile_id="jev",
                admitted=lambda _: True,
                client=client,
            )
            result = await adapter.interpret(intake(raw))
            assert result.draft.action is TaskAction.CREATE_TASK
            assert not result.draft.needs_clarification
            assert result.draft.goal == raw
            assert Capability.CODE_EDIT in result.draft.required_capabilities
            discussion = await adapter.interpret_discussion(
                DiscussionInterpretRequest(original_input=raw, project_id="books", user_id=1)
            )
            assert discussion.task_request is not None
            assert discussion.task_request.goal == raw

    asyncio.run(scenario())


def test_null_relation_is_rejected_outside_mixed() -> None:
    async def scenario() -> None:
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(
                lambda _: httpx.Response(200, json=response(relation="null"))
            )
        ) as client:
            adapter = JevDecisionsInterpreter(
                credential=SecretStr("test-key"),
                profile_id="jev",
                admitted=lambda _: True,
                client=client,
            )
            with pytest.raises(InvalidInterpreterOutput, match="null_relation_requires_mixed"):
                await adapter.interpret(intake())

    asyncio.run(scenario())


@pytest.mark.parametrize("effect", ["plan", "respond", "status", "capture"])
def test_non_execution_turn_cannot_resume_work_via_continue_relation(effect: str) -> None:
    async def scenario() -> None:
        task_id = uuid.uuid4()
        raw = "Продолжи обсуждение правок, но пока не запускай работу."
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(
                lambda _: httpx.Response(200, json=response(effect, "continue", str(task_id)))
            )
        ) as client:
            adapter = JevDecisionsInterpreter(
                credential=SecretStr("test-key"),
                profile_id="jev",
                admitted=lambda _: True,
                client=client,
            )
            result = await adapter.interpret(
                intake(raw, active_tasks=(TaskContext(task_id=task_id, title="Поиск"),))
            )
            assert result.draft.action is not TaskAction.CONTINUE_TASK
            assert Capability.CODE_EDIT not in result.draft.required_capabilities
            assert result.draft.goal == raw

    asyncio.run(scenario())


def test_bounded_discussion_task_cannot_silently_drop_trailing_condition() -> None:
    async def scenario() -> None:
        raw = (
            "Проверь поиск: "
            + "подробности " * 400
            + "только если сломан; пока не запускай деплой."
        )
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(lambda _: httpx.Response(200, json=response()))
        ) as client:
            adapter = JevDecisionsInterpreter(
                credential=SecretStr("test-key"),
                profile_id="jev",
                admitted=lambda _: True,
                client=client,
            )
            result = await adapter.interpret_discussion(
                DiscussionInterpretRequest(original_input=raw, project_id="books", user_id=1)
            )
            assert result.interaction_mode is InteractionMode.QUERY_REFUSE
            assert result.refusal_code is RefusalCode.CLARIFY_REQUIRED
            assert result.task_request is None
            assert not result.should_create_task
            assert not result.should_mutate_plan

    asyncio.run(scenario())
