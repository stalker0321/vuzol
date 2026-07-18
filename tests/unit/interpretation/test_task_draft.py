"""Task draft tests (split for cohesion)."""

from __future__ import annotations

from ._test_interpretation_helpers import *


def test_task_draft_requires_consistent_clarification_and_continuation() -> None:
    with pytest.raises(ValidationError, match="clarification question is required"):
        draft(needs_clarification=True)
    with pytest.raises(ValidationError, match="referenced task"):
        draft(action=TaskAction.CONTINUE_TASK)
    with pytest.raises(ValidationError, match="new project fields"):
        draft(new_project_id="notes", new_project_name="Notes")


def test_inbox_is_explicit_project_provisioning_boundary() -> None:
    inbox = request().model_copy(update={"topic_kind": TopicKind.INBOX})
    value = draft(
        action=TaskAction.CREATE_PROJECT,
        new_project_id="notes",
        new_project_name="Notes",
        project_name_options=name_options(),
    )
    policy = enforce_interpretation_policy(
        inbox,
        value,
        known_project_ids=frozenset({"vuzol"}),
    )
    assert policy.draft.action is TaskAction.CREATE_PROJECT
    assert policy.draft.project_id is None
    assert policy.draft.new_project_id is None
    assert policy.draft.new_project_name is None
    assert len(policy.draft.project_name_options) == 9
    assert policy.draft.required_capabilities == frozenset(
        {Capability.FILESYSTEM_WRITE, Capability.GIT, Capability.TELEGRAM_SEND}
    )
    assert not policy.draft.needs_clarification
    assert policy.automatic_execution_eligible


def test_inbox_requires_name_options_and_rejects_configured_project_collision() -> None:
    inbox = request().model_copy(update={"topic_kind": TopicKind.INBOX})
    missing = enforce_interpretation_policy(
        inbox,
        draft(),
        known_project_ids=frozenset({"vuzol"}),
    )
    assert missing.draft.action is TaskAction.CREATE_PROJECT
    assert missing.draft.needs_clarification
    assert "project_name_options_missing" in missing.reasons

    collision = enforce_interpretation_policy(
        inbox,
        draft(
            action=TaskAction.CREATE_PROJECT,
            project_name_options=name_options(conflicting_id="vuzol"),
        ),
        known_project_ids=frozenset({"vuzol"}),
    )
    assert collision.draft.needs_clarification
    assert "project_name_options_conflict" in collision.reasons


def test_project_topic_cannot_be_reinterpreted_as_new_project() -> None:
    contextual = request().model_copy(
        update={"topic_kind": TopicKind.PROJECT, "mapped_project_id": "bill-buddy"}
    )
    policy = enforce_interpretation_policy(
        contextual,
        draft(
            action=TaskAction.CREATE_PROJECT,
            task_type=TaskType.GENERAL,
            project_name_options=name_options(),
        ),
        known_project_ids=frozenset({"bill-buddy"}),
    )

    assert policy.draft.action is TaskAction.CREATE_TASK
    assert policy.draft.task_type is TaskType.CODING
    assert policy.draft.project_id == "bill-buddy"
    assert policy.draft.project_name_options == ()
    assert "project_creation_confined_to_inbox" in policy.reasons


def test_task_schema_exposes_architecture_as_a_distinct_agent_task() -> None:
    schema = TaskDraft.model_json_schema()
    task_type_schema = schema["$defs"]["TaskType"]
    assert "architecture" in task_type_schema["enum"]
    assert "task_summary" in schema["required"]


def test_legacy_task_draft_derives_summary_from_normalized_title() -> None:
    value = draft()
    assert value.task_summary == "Inspect service"


def test_explicit_task_summary_is_preserved() -> None:
    value = draft(task_summary="Inspect current service health and report anomalies")
    assert value.task_summary == "Inspect current service health and report anomalies"
