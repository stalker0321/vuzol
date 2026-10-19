"""Semantic choices must stay coherent through the draft and safety policy."""

import json

import httpx
import pytest
from pydantic import SecretStr

from tests.unit.interpretation.test_jev_live import intake, response
from vuzol.config import Capability
from vuzol.interpretation.discussion import DiscussionInterpretRequest
from vuzol.interpretation.domain import TaskOperation, TaskType
from vuzol.interpretation.jev import JevDecisionsInterpreter
from vuzol.interpretation.policy import enforce_interpretation_policy
from vuzol.interpretation.work_contract import work_contract_error
from vuzol.storage.types import InteractionMode


def choices(task_type: str, operation: str, effect: str = "execute_request") -> dict:
    body = response(effect, "continue", "conversation_context")
    body["answers"]["task_type"]["choice"] = task_type
    body["answers"]["operation"]["choice"] = operation
    return body


@pytest.mark.parametrize("kind", list(TaskType))
@pytest.mark.parametrize("operation", list(TaskOperation))
def test_work_domain_matrix_never_changes_nonexecution_authority(kind, operation):
    for effect in ("respond", "plan", "status", "capture", "mixed", "abstain"):
        assert work_contract_error(effect, kind, operation) is None


@pytest.mark.anyio
@pytest.mark.parametrize(
    "raw",
    [
        "ну окей тогда. давай делать сайт",
        "Начнём воплощать то, что обсудили",
        "Собери приложение по этому описанию",
        "Design and implement the discussed site",
    ],
)
@pytest.mark.parametrize(
    "kind,operation,valid",
    [
        ("general", "create", False),
        ("coding", "create", True),
        ("research", "create", True),
        ("infrastructure", "create", True),
        ("file_processing", "create", True),
        ("architecture", "inspect", True),
        ("architecture", "fix", False),
        ("coding", "explain", False),
    ],
)
async def test_work_kind_is_preserved_or_clarified_not_guessed(raw, kind, operation, valid):
    calls = []

    def handler(request):
        calls.append(json.loads(request.content))
        return httpx.Response(200, json=choices(kind, operation))

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        jev = JevDecisionsInterpreter(
            credential=SecretStr("test-key"),
            profile_id="jev",
            admitted=lambda _: True,
            client=client,
        )
        request = intake(raw, conversational_context=True)
        result = await jev.interpret(request)
        assert result.draft.task_type.value == kind
        assert result.draft.needs_clarification is (not valid)
        assert len(calls) == 1  # No hidden repair/fallback inference.
        assert jev.receipt["selected_choices"]["task_type"] == kind
        assert (jev.receipt["work_contract"]["error"] is None) is valid
        policy = enforce_interpretation_policy(
            request,
            result.draft,
            known_project_ids=frozenset({"books"}),
            semantic_effect="execute_request",
        )
        assert policy.draft.task_type.value == kind
        if not valid:
            assert not policy.automatic_execution_eligible
            assert not policy.draft.required_capabilities
        elif kind == "coding":
            assert Capability.CODE_EDIT in policy.draft.required_capabilities
        elif kind == "architecture":
            assert policy.draft.required_capabilities == {Capability.REPOSITORY_READ}
        elif kind == "infrastructure":
            assert Capability.HOST_ADMIN in policy.draft.required_capabilities
            assert not policy.automatic_execution_eligible  # Risk/approval boundary remains.


@pytest.mark.anyio
async def test_legacy_discussion_port_also_rejects_inconsistent_work():
    body = choices("general", "create")
    body["answers"]["target"]["choice"] = "no_target"
    body["answers"]["relation"]["choice"] = "new"
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(200, json=body))
    ) as client:
        jev = JevDecisionsInterpreter(
            credential=SecretStr("test-key"),
            profile_id="jev",
            admitted=lambda _: True,
            client=client,
        )
        result = await jev.interpret_discussion(
            DiscussionInterpretRequest(original_input="Сделай сайт", project_id="books", user_id=1)
        )
        assert result.interaction_mode is InteractionMode.QUERY_REFUSE
        assert result.task_request is None


@pytest.mark.anyio
@pytest.mark.parametrize("effect", ["respond", "plan", "capture"])
async def test_design_discussion_and_no_start_stay_read_only(effect):
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda _: httpx.Response(200, json=choices("coding", "create", effect))
        )
    ) as client:
        jev = JevDecisionsInterpreter(
            credential=SecretStr("test-key"),
            profile_id="jev",
            admitted=lambda _: True,
            client=client,
        )
        request = intake("Обсудим, как сделать сайт; пока не начинай.", conversational_context=True)
        result = await jev.interpret(request)
        policy = enforce_interpretation_policy(
            request,
            result.draft,
            known_project_ids=frozenset({"books"}),
            semantic_effect=effect,
        )
        assert Capability.CODE_EDIT not in policy.draft.required_capabilities
        if effect == "plan":
            assert result.draft.task_type is TaskType.ARCHITECTURE
            assert result.draft.operation is TaskOperation.INSPECT
