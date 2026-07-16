"""Deterministic safety checks that can only tighten interpreter suggestions."""

from dataclasses import dataclass

from vuzol.config import Capability, TopicKind
from vuzol.interpretation.domain import (
    InterpretationInput,
    TaskAction,
    TaskDraft,
    TaskOperation,
    TaskType,
)
from vuzol.storage.types import RiskLevel

_RISK_ORDER = {
    RiskLevel.LOW: 0,
    RiskLevel.MEDIUM: 1,
    RiskLevel.HIGH: 2,
    RiskLevel.PRIVILEGED: 3,
}

_DESIGN_DISCUSSION_MARKERS = (
    "как лучше",
    "как правильнее",
    "как это сделать",
    "какой подход",
    "какую архитектур",
    "что думаешь",
    "что скажешь",
    "спроектир",
    "як краще",
    "best way",
    "how should",
    "what do you think",
    "which approach",
    "architecture",
    "design this",
)

_IMPLEMENTATION_MARKERS = (
    "приступать к реализации",
    "приступаем к реализации",
    "давай реализ",
    "реализуй",
    "сделай сайт",
    "сделай приложение",
    "создай сайт",
    "создай приложение",
    "напиши код",
    "начинай разработ",
    "implement",
    "build the site",
    "build the app",
    "start coding",
    "write the code",
    "create the site",
    "create the app",
)


@dataclass(frozen=True, slots=True)
class PolicyResult:
    draft: TaskDraft
    automatic_execution_eligible: bool
    reasons: tuple[str, ...]


def enforce_interpretation_policy(
    request: InterpretationInput,
    draft: TaskDraft,
    *,
    known_project_ids: frozenset[str],
) -> PolicyResult:
    reasons: list[str] = []
    updates: dict[str, object] = {}
    risk = draft.suggested_risk
    normalized_input = request.original_input.casefold()
    implementation_intent = any(marker in normalized_input for marker in _IMPLEMENTATION_MARKERS)
    read_only_request = not draft.required_capabilities & {
        Capability.CODE_EDIT,
        Capability.FILESYSTEM_WRITE,
    }
    architecture_intent = not implementation_intent and (
        draft.task_type is TaskType.ARCHITECTURE
        or (
            draft.task_type is TaskType.CODING
            and read_only_request
            and (
                draft.operation in {TaskOperation.INSPECT, TaskOperation.EXPLAIN}
                or any(marker in normalized_input for marker in _DESIGN_DISCUSSION_MARKERS)
            )
        )
    )
    if request.topic_kind is TopicKind.INBOX:
        updates.update(
            action=TaskAction.CREATE_PROJECT,
            task_type=TaskType.INFRASTRUCTURE,
            operation=TaskOperation.CREATE,
            project_id=None,
            new_project_id=None,
            new_project_name=None,
            required_capabilities=frozenset(
                {Capability.FILESYSTEM_WRITE, Capability.GIT, Capability.TELEGRAM_SEND}
            ),
        )
        conflicting_options = tuple(
            option
            for option in draft.project_name_options
            if option.project_id in known_project_ids
        )
        if len(draft.project_name_options) != 9:
            updates.update(
                needs_clarification=True,
                clarification_question=(
                    "I could not generate nine project names. Please restate the project idea."
                ),
            )
            reasons.append("project_name_options_missing")
        elif conflicting_options:
            updates.update(
                needs_clarification=True,
                clarification_question=(
                    "Generated project names conflict with existing projects. Regenerate them."
                ),
            )
            reasons.append("project_name_options_conflict")
    elif request.topic_kind is TopicKind.PROJECT and draft.action is TaskAction.CREATE_PROJECT:
        updates.update(
            action=TaskAction.CREATE_TASK,
            task_type=TaskType.CODING,
            new_project_id=None,
            new_project_name=None,
            project_name_options=(),
        )
        reasons.append("project_creation_confined_to_inbox")
    elif request.topic_kind is TopicKind.PROJECT and implementation_intent:
        updates.update(
            action=TaskAction.CREATE_TASK,
            task_type=TaskType.CODING,
            operation=(
                TaskOperation.CREATE
                if draft.operation in {TaskOperation.INSPECT, TaskOperation.EXPLAIN}
                else draft.operation
            ),
            required_capabilities=frozenset({Capability.REPOSITORY_READ, Capability.CODE_EDIT}),
        )
        if draft.task_type is not TaskType.CODING:
            reasons.append("explicit_implementation_reclassified_as_coding")
    elif request.topic_kind is TopicKind.PROJECT and architecture_intent:
        if draft.task_type is not TaskType.ARCHITECTURE:
            updates.update(task_type=TaskType.ARCHITECTURE, operation=TaskOperation.INSPECT)
            reasons.append("read_only_design_reclassified_as_architecture")
        if draft.action in {TaskAction.ANSWER_QUESTION, TaskAction.GENERAL_CONVERSATION}:
            updates.update(action=TaskAction.CREATE_TASK, operation=TaskOperation.INSPECT)
            reasons.append("architecture_requires_agent_task")
        if draft.action in {
            TaskAction.CREATE_TASK,
            TaskAction.CONTINUE_TASK,
            TaskAction.ANSWER_QUESTION,
            TaskAction.GENERAL_CONVERSATION,
        } and draft.required_capabilities != frozenset({Capability.REPOSITORY_READ}):
            updates["required_capabilities"] = frozenset({Capability.REPOSITORY_READ})
            reasons.append("architecture_confined_to_read_only")
    if (
        draft.project_id is not None
        and draft.project_id not in known_project_ids
        and draft.project_id != request.mapped_project_id
    ):
        updates.update(
            project_id=None,
            needs_clarification=True,
            clarification_question="Which configured project should this request use?",
        )
        reasons.append("unknown_project")
    if request.mapped_project_id is not None:
        if draft.project_id is None:
            updates["project_id"] = request.mapped_project_id
        elif draft.project_id != request.mapped_project_id:
            updates.update(
                project_id=request.mapped_project_id,
                needs_clarification=True,
                clarification_question=(
                    "The requested project differs from this topic. Which project is intended?"
                ),
            )
            reasons.append("project_topic_mismatch")
    privileged_capabilities = {Capability.HOST_ADMIN, Capability.SECRETS}
    if draft.required_capabilities & privileged_capabilities and _RISK_ORDER[risk] < 3:
        risk = RiskLevel.PRIVILEGED
        updates["suggested_risk"] = risk
        reasons.append("privileged_capability_risk_raised")
    if draft.contradiction_detected:
        updates.update(
            needs_clarification=True,
            clarification_question=(
                "The original request conflicts with its interpretation. Which intent is correct?"
            ),
        )
        reasons.append("contradictory_interpretation")
    if request.source_is_voice and request.transcription_uncertain and _RISK_ORDER[risk] >= 2:
        updates.update(
            needs_clarification=True,
            clarification_question=(
                "Please confirm the potentially risky action transcribed from voice."
            ),
        )
        reasons.append("uncertain_dangerous_transcription")
    needs_clarification = bool(updates.get("needs_clarification", draft.needs_clarification))
    if _RISK_ORDER[risk] >= 2 and not needs_clarification:
        updates.update(
            needs_clarification=True,
            clarification_question="Please confirm this high-risk interpretation before execution.",
        )
        reasons.append("dangerous_interpretation_confirmation")
    if draft.action is TaskAction.CONTINUE_TASK and draft.referenced_task_id is not None:
        allowed_tasks = {task.task_id for task in request.active_tasks}
        if request.reply_linked_task is not None:
            allowed_tasks.add(request.reply_linked_task.task_id)
        if draft.referenced_task_id not in allowed_tasks:
            updates.update(
                needs_clarification=True,
                clarification_question="Which active task should this continue?",
            )
            reasons.append("unsupported_task_binding")
    if draft.action in {TaskAction.APPROVE_STEP, TaskAction.REJECT_STEP}:
        reasons.append("natural_language_control_never_consumes_approval")
    tightened = draft.model_copy(update=updates)
    eligible = not tightened.needs_clarification and tightened.action not in {
        TaskAction.APPROVE_STEP,
        TaskAction.REJECT_STEP,
        TaskAction.GENERAL_CONVERSATION,
    }
    return PolicyResult(tightened, eligible, tuple(reasons))
