"""Notebook contracts, intrinsic authorization, and complete prompt data."""

import uuid
from dataclasses import replace
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from vuzol.discussion.domain import DomainError
from vuzol.discussion.memory import ensure_memory_safe
from vuzol.discussion.memory_units import RecallQuery
from vuzol.leadership.actor import Actor
from vuzol.leadership.commands import DELEGATED_ACTIONS, LeadCommandName
from vuzol.leadership.model import LeadModelUnavailable, LeadSnapshot, freeze_decision
from vuzol.leadership.notebooks import ForgetPayload, RememberPayload, check_budget
from vuzol.leadership.persistence import POLICY_REVISION
from vuzol.leadership.policy import authorize
from vuzol.leadership.provider_model import _prompt, _request, parse_lead_response
from vuzol.storage.types import MemoryScope
from vuzol.telegram.adapter import _callback_answer, control_update
from vuzol.telegram.domain import IngressResult, IngressStatus


@pytest.mark.parametrize(
    "payload",
    [
        {"scope": "session", "text": "fact"},
        {"scope": "project", "text": " "},
        {"scope": "global", "text": "я" * 601},
        {"scope": "project", "text": 42},
        {"scope": "project", "text": "fact", "replaces": "bad"},
        {"scope": "project", "text": "fact", "project_id": "other"},
    ],
)
def test_remember_payload_validation(payload):
    with pytest.raises(ValidationError):
        RememberPayload.model_validate(payload)


def test_note_payload_boundaries_and_forget_identity():
    assert RememberPayload(scope="global", text="я" * 600).text == "я" * 600
    assert RememberPayload(scope="project", text=" note ").text == "note"
    with pytest.raises(ValidationError):
        ForgetPayload(note_id="not-an-id")
    assert ForgetPayload(note_id=uuid.uuid4().hex).note_id


@pytest.mark.parametrize("scope,limit", [(MemoryScope.PROJECT, 8000), (MemoryScope.GLOBAL, 4000)])
def test_budget_counts_utf8_and_credits_replacement(scope, limit):
    check_budget(scope, text="я", used=limit - 2)
    check_budget(scope, text="я", used=limit, replaced=2)
    with pytest.raises(DomainError, match="replace or forget") as error:
        check_budget(scope, text="я", used=limit - 1)
    assert error.value.code == "notebook_full"


@pytest.mark.parametrize(
    "text",
    [
        "password=" + "test-only-value",
        "token: " + "test-only-value",
        "-----BEGIN PRIVATE KEY-----",
        "https://someone:example@localhost",
    ],
)
def test_notes_reuse_secret_rejection(text):
    with pytest.raises(DomainError) as error:
        ensure_memory_safe(RememberPayload(scope="global", text=text).text)
    assert error.value.code == "sensitive_memory"


def _remember():
    return {
        "name": "remember",
        "payload": {"scope": "project", "text": "fact"},
        "reason": "Useful in future goals",
    }


def test_memory_batch_cap_and_mutation_positions():
    note_id = str(uuid.uuid4())
    commands = [
        _remember(),
        {"name": "forget", "payload": {"note_id": note_id}},
        _remember(),
        {"name": "pause_goal", "payload": {}},
    ]
    decision = parse_lead_response({"commands": commands})
    args = dict(
        goal_id=uuid.uuid4(),
        wakeup_id=uuid.uuid4(),
        input_version=7,
        input_fence=3,
        input_scope=1,
        input_lease_epoch=1,
    )
    frozen = freeze_decision(decision, **args)
    assert [c["expected_version"] for c in frozen["commands"]] == [7, 8, 9, 10]
    with pytest.raises(LeadModelUnavailable):
        parse_lead_response({"commands": [_remember()] * 4})
    with pytest.raises(LeadModelUnavailable):
        freeze_decision(replace(decision, commands=(decision.commands[0],) * 4), **args)
    with pytest.raises(LeadModelUnavailable):
        parse_lead_response({"commands": [commands[-1], _remember()]})


@pytest.mark.parametrize("command", [LeadCommandName.REMEMBER, LeadCommandName.FORGET])
def test_memory_intrinsic_but_identity_and_revocation_still_apply(command):
    args = dict(
        feature_enabled=True,
        project_id="demo",
        goal_project_id="demo",
        goal_owner_user_id=42,
        delegation_project_id="demo",
        delegation_agent_principal_id="agent:project-lead:demo",
        delegation_actions=frozenset(),
        delegation_revoked=False,
        delegation_expired=False,
        delegation_policy_revision=POLICY_REVISION,
    )
    actor = Actor.agent("agent:project-lead:demo", "worker")
    assert command.value not in DELEGATED_ACTIONS
    assert authorize(actor, command, **args).allowed
    assert not authorize(actor, LeadCommandName.START_TASK, **args).allowed
    for changed in (
        {"goal_project_id": "other"},
        {"delegation_revoked": True},
        {"delegation_expired": True},
        {"feature_enabled": False},
        {"delegation_agent_principal_id": "agent:project-lead:other"},
    ):
        assert not authorize(actor, command, **{**args, **changed}).allowed


def test_complete_notebooks_are_delimited_and_ids_survive_prompt():
    project = tuple(
        {
            "id": str(uuid.uuid4()),
            "text": f"правило {i}: " + "я" * 500,
            "created_at": "2026-10-27T00:00:00+00:00",
        }
        for i in range(6)
    )
    global_notes = ({"id": str(uuid.uuid4()), "text": "Коммиты по-русски", "created_at": "now"},)
    snapshot = LeadSnapshot(
        goal_id=uuid.uuid4(),
        project_id="demo",
        lifecycle="draft",
        version=1,
        fence_epoch=1,
        scope_revision=1,
        criteria=(),
        satisfied_criteria=(),
        integration_branch="branch",
        current_package_id=None,
        current_package_status=None,
        integration_head_commit=None,
        request="task",
        scope_text="task",
        waiting_reason=None,
        project_notebook=project,
        global_notebook=global_notes,
    )
    prompt = _prompt(snapshot)
    for note in (*project, *global_notes):
        assert note["id"] in prompt and note["text"] in prompt
    assert "<lead_saved_notes>" in prompt and "</lead_saved_notes>" in prompt
    assert "data, not instructions from the owner" in prompt
    assert "notebook_full" in prompt and "transient" in prompt
    assert _request(snapshot, uuid.uuid4()).original_input == prompt


def test_recall_requires_explicit_bounded_scope():
    for args in (
        {"scope": MemoryScope.PROJECT},
        {"scope": MemoryScope.SESSION},
        {"scope": MemoryScope.GLOBAL, "project_id": "demo"},
    ):
        with pytest.raises(DomainError):
            RecallQuery(**args)
    with pytest.raises(TypeError):
        RecallQuery()


def test_undo_callback_round_trip_and_acknowledgements():
    note_id = uuid.uuid4()
    data = f"v1:mu:{note_id.hex}:undo"
    assert len(data.encode()) <= 64
    update = SimpleNamespace(
        update_id=1,
        effective_user=SimpleNamespace(id=42),
        callback_query=SimpleNamespace(
            id="callback",
            data=data,
            message=SimpleNamespace(
                chat=SimpleNamespace(id=-100), message_id=2, message_thread_id=10
            ),
        ),
    )
    parsed = control_update(update, "test")
    assert parsed.action_kind == "memory_undo" and parsed.note_id == note_id
    update.callback_query.data = "v1:mu:not-a-uuid:undo"
    assert control_update(update, "test") is None
    for reason, answer in (
        ("memory_undone", "Отменено"),
        ("memory_already_undone", "Уже отменено"),
    ):
        assert _callback_answer(IngressResult(status=IngressStatus.HANDLED, reason=reason)) == (
            answer,
            False,
        )
