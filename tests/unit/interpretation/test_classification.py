"""Classification tests (split for cohesion)."""

from __future__ import annotations

from ._test_interpretation_helpers import (
    Capability,
    TaskAction,
    TaskOperation,
    TaskType,
    TopicKind,
    draft,
    enforce_interpretation_policy,
    request,
)


def test_project_architecture_question_is_a_read_only_agent_task() -> None:
    contextual = request().model_copy(
        update={"topic_kind": TopicKind.PROJECT, "mapped_project_id": "vuzol"}
    )
    policy = enforce_interpretation_policy(
        contextual,
        draft(
            action=TaskAction.ANSWER_QUESTION,
            task_type=TaskType.ARCHITECTURE,
            operation=TaskOperation.EXPLAIN,
            required_capabilities=frozenset({Capability.CODE_EDIT}),
        ),
        known_project_ids=frozenset({"vuzol"}),
    )

    assert policy.draft.action is TaskAction.CREATE_TASK
    assert policy.draft.operation is TaskOperation.INSPECT
    assert policy.draft.required_capabilities == frozenset({Capability.REPOSITORY_READ})
    assert policy.draft.project_id == "vuzol"
    assert "architecture_requires_agent_task" in policy.reasons
    assert "architecture_confined_to_read_only" in policy.reasons


def test_read_only_coding_inspection_is_reclassified_as_architecture() -> None:
    contextual = request().model_copy(
        update={"topic_kind": TopicKind.PROJECT, "mapped_project_id": "vuzol"}
    )
    policy = enforce_interpretation_policy(
        contextual,
        draft(
            task_type=TaskType.CODING,
            operation=TaskOperation.INSPECT,
            required_capabilities=frozenset({Capability.REPOSITORY_READ, Capability.WEB_RESEARCH}),
        ),
        known_project_ids=frozenset({"vuzol"}),
    )

    assert policy.draft.task_type is TaskType.ARCHITECTURE
    assert policy.draft.required_capabilities == frozenset({Capability.REPOSITORY_READ})
    assert "read_only_design_reclassified_as_architecture" in policy.reasons


def test_design_question_survives_coding_create_misclassification() -> None:
    contextual = request().model_copy(
        update={
            "original_input": "Как лучше всего это сделать? Я думаю в виде лёгкого сайта.",
            "topic_kind": TopicKind.PROJECT,
            "mapped_project_id": "vuzol",
        }
    )
    policy = enforce_interpretation_policy(
        contextual,
        draft(
            task_type=TaskType.CODING,
            operation=TaskOperation.CREATE,
            required_capabilities=frozenset(),
        ),
        known_project_ids=frozenset({"vuzol"}),
    )

    assert policy.draft.task_type is TaskType.ARCHITECTURE
    assert policy.draft.operation is TaskOperation.INSPECT
    assert policy.draft.required_capabilities == frozenset({Capability.REPOSITORY_READ})


def test_explicit_implementation_overrides_architecture_misclassification() -> None:
    contextual = request().model_copy(
        update={
            "original_input": (
                "Окей, давай приступать к реализации. Сделай пока сайт с нужным "  # noqa: RUF001
                "функционалом, а апи моделей я добавлю потом."  # noqa: RUF001
            ),
            "topic_kind": TopicKind.PROJECT,
            "mapped_project_id": "bill-buddy",
        }
    )
    policy = enforce_interpretation_policy(
        contextual,
        draft(
            task_type=TaskType.ARCHITECTURE,
            operation=TaskOperation.INSPECT,
            project_id="bill-buddy",
            required_capabilities=frozenset({Capability.REPOSITORY_READ}),
        ),
        known_project_ids=frozenset({"vuzol"}),
    )

    assert policy.draft.action is TaskAction.CREATE_TASK
    assert policy.draft.task_type is TaskType.CODING
    assert policy.draft.operation is TaskOperation.CREATE
    assert policy.draft.required_capabilities == frozenset(
        {Capability.REPOSITORY_READ, Capability.CODE_EDIT}
    )
    assert "explicit_implementation_reclassified_as_coding" in policy.reasons


def test_imperative_modification_is_never_read_only_architecture() -> None:
    contextual = request().model_copy(
        update={
            "original_input": (
                "Доработай существующий Bill Buddy, не переписывая сайт. "
                "Добавь выбор отдельных позиций чека и сумму выбранных позиций."
            ),
            "topic_kind": TopicKind.PROJECT,
            "mapped_project_id": "bill-buddy",
        }
    )
    policy = enforce_interpretation_policy(
        contextual,
        draft(
            task_type=TaskType.ARCHITECTURE,
            operation=TaskOperation.INSPECT,
            project_id="bill-buddy",
            required_capabilities=frozenset({Capability.REPOSITORY_READ}),
        ),
        known_project_ids=frozenset({"bill-buddy"}),
    )

    assert policy.draft.task_type is TaskType.CODING
    assert policy.draft.operation is TaskOperation.CREATE
    assert policy.draft.required_capabilities == frozenset(
        {Capability.REPOSITORY_READ, Capability.CODE_EDIT}
    )


def test_restore_imperative_is_never_read_only_architecture() -> None:
    contextual = request().model_copy(
        update={
            "original_input": (
                "Восстанови README: заголовок # Test Project Alpha, затем описание "
                "Create a test project. Создай Makefile с целью test и выполни make test."  # noqa: RUF001
            ),
            "topic_kind": TopicKind.PROJECT,
            "mapped_project_id": "test-project-alpha",
        }
    )
    policy = enforce_interpretation_policy(
        contextual,
        draft(
            task_type=TaskType.ARCHITECTURE,
            operation=TaskOperation.INSPECT,
            project_id="test-project-alpha",
            required_capabilities=frozenset({Capability.REPOSITORY_READ}),
        ),
        known_project_ids=frozenset({"test-project-alpha"}),
    )

    assert policy.draft.task_type is TaskType.CODING
    assert policy.draft.operation is TaskOperation.CREATE
    assert policy.draft.required_capabilities == frozenset(
        {Capability.REPOSITORY_READ, Capability.CODE_EDIT}
    )
    assert "explicit_implementation_reclassified_as_coding" in policy.reasons
