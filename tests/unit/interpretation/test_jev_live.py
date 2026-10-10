"""Verify real Decisions API mapping, authority boundaries and visible accounting."""

import asyncio
import json
import uuid
from typing import Any

import httpx
import pytest
from pydantic import SecretStr

from vuzol.config import Capability, TopicKind
from vuzol.interpretation.discussion import DiscussionInterpretRequest
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
