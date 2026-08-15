"""Fenced Telegram outbox dispatch from canonical PostgreSQL state."""

import asyncio
import uuid
from contextlib import suppress
from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from telegram.error import BadRequest, Forbidden, NetworkError, RetryAfter, TelegramError, TimedOut

from vuzol.config import TelegramDogfoodSettings, TopicKind, TopicRegistry
from vuzol.config.registries import ProfileRegistry, ProjectRegistry
from vuzol.interpretation.discussion import (
    DISCUSSION_REPLY_DESTINATION,
    DISCUSSION_THINKING_ROLE,
)
from vuzol.observability import get_logger
from vuzol.ops.telegram_dogfood import DogfoodFault, consume_fault
from vuzol.projects.executor_preference import load_preference
from vuzol.storage.errors import LeaseLost
from vuzol.storage.leasing import (
    claim_outbox_item,
    complete_outbox_item,
    dead_letter_outbox_item,
    defer_outbox_item,
    mark_outbox_ambiguous,
    retry_outbox_item,
)
from vuzol.storage.models import (
    ConversationTurn,
    Interpretation,
    ProjectDiscussionSession,
    ProjectNamingRequest,
    ProjectProvisioning,
    ProviderBudgetReservation,
    Run,
    Step,
    Task,
    TelegramIntakeMessage,
    TelegramMessageLink,
    TopicMapping,
    TransactionalOutbox,
    UsageRecord,
    WorkPackage,
)
from vuzol.storage.records import OutboxLeaseToken
from vuzol.storage.types import ConversationTurnRole, ConversationTurnSource, TaskStatus
from vuzol.telegram.formatting import telegram_markdown_html
from vuzol.telegram.layout import (
    HELP_CARD_ROLE,
    HISTORY_TOPIC_KIND,
    STATUS_DASHBOARD_TOPIC_KIND,
    build_help_card,
)
from vuzol.telegram.model_command import PROJECT_MODEL_CONFIRM_ROLE, PROJECT_MODEL_PICKER_ROLE
from vuzol.telegram.projections import (
    PROJECT_STATUS_DASHBOARD_ROLE,
    TASK_HISTORY_ROLE,
    LostTelegramResponse,
    TelegramClient,
    build_approval_card,
    build_project_status_dashboard,
    build_status_card,
    build_task_history_report,
    telegram_html,
)
from vuzol.telegram.tracing import (
    INTERPRETER_TRACE_KIND,
    ORCHESTRATION_TRACE_ROLE,
    PLANNER_TRACE_KIND,
    build_interpreter_trace_html,
    build_planner_trace_html,
    interpreter_trace_is_anomalous,
    planner_trace_is_anomalous,
    should_deliver_orchestration_trace,
)
from vuzol.telegram.work_package_projections import (
    WORK_PACKAGE_ACTION_ROLE,
    WORK_PACKAGE_DETAIL_ROLE,
    WORK_PACKAGE_PLAN_ROLE,
    WORK_PACKAGE_PROJECTION_DESTINATION,
    WORK_PACKAGE_STATUS_ROLE,
    WorkPackageProjectionError,
    build_work_package_action_card,
    build_work_package_detail_card,
    build_work_package_plan_card,
    build_work_package_status_card,
    enqueue_project_topic_idle,
)

TELEGRAM_DESTINATIONS = frozenset({"telegram", WORK_PACKAGE_PROJECTION_DESTINATION})


class DeliveryAction(StrEnum):
    SEND_STATUS = "send_status"
    EDIT_STATUS = "edit_status"
    EDIT_MESSAGE = "edit_message"
    SEND_CLARIFICATION = "send_clarification"
    SEND_PROJECT_WELCOME = "send_project_welcome"
    SEND_PROJECT_NAMES = "send_project_names"
    SEND_MODEL_PICKER = "send_model_picker"
    SEND_HELP = "send_help"
    SEND_DISCUSSION_REPLY = "send_discussion_reply"
    DELETE_MESSAGE = "delete_message"
    NOOP = "noop"


@dataclass(frozen=True, slots=True)
class PreparedDelivery:
    action: DeliveryAction
    chat_id: int
    thread_id: int | None
    html: str = ""
    fallback_html: str | None = None
    task_id: uuid.UUID | None = None
    run_id: uuid.UUID | None = None
    step_id: uuid.UUID | None = None
    revision: int | None = None
    link_id: uuid.UUID | None = None
    message_id: int | None = None
    buttons: tuple[str, ...] = ()
    approval_id: uuid.UUID | None = None
    message_role: str | None = None
    callback_buttons: tuple[tuple[tuple[str, str], ...], ...] = ()
    work_package_id: uuid.UUID | None = None
    plan_revision_id: uuid.UUID | None = None
    control_status_generation: int | None = None
    pin_after_send: bool = False
    project_id: str | None = None


class PermanentDeliveryError(RuntimeError):
    """A safe, categorized delivery failure that must not be retried."""

    def __init__(self, category: str) -> None:
        super().__init__(category)
        self.category = category


class DeliveryRunner(Protocol):
    async def deliver_one(self) -> bool: ...


async def prepare_delivery(
    session: AsyncSession,
    item: TransactionalOutbox,
    topics: TopicRegistry | None = None,
    projects: ProjectRegistry | None = None,
    profiles: ProfileRegistry | None = None,
    trace_enabled: bool = True,
    trace_sample_percent: int = 100,
    trace_always_include_anomalies: bool = True,
) -> PreparedDelivery:
    if getattr(item, "destination", "telegram") == WORK_PACKAGE_PROJECTION_DESTINATION:
        return await _prepare_work_package_projection(session, item)
    if item.operation_type not in {"send_message", "delete_message"}:
        raise PermanentDeliveryError("unsupported_telegram_operation")
    if (
        item.operation_type == "delete_message"
        and item.payload.get("role") == "user_command_delete"
    ):
        try:
            chat_id = int(item.payload["chat_id"])
            message_id = int(item.payload["message_id"])
        except (KeyError, TypeError, ValueError) as error:
            raise PermanentDeliveryError("invalid_command_delete_payload") from error
        thread_id = item.payload.get("message_thread_id")
        return PreparedDelivery(
            DeliveryAction.DELETE_MESSAGE,
            chat_id=chat_id,
            thread_id=int(thread_id) if thread_id is not None else None,
            message_id=message_id,
        )
    if item.payload.get("role") == HELP_CARD_ROLE:
        try:
            chat_id = int(item.payload["chat_id"])
            thread_id = int(item.payload["message_thread_id"])
            topic_kind = TopicKind(str(item.payload["topic_kind"]))
        except (KeyError, TypeError, ValueError) as error:
            raise PermanentDeliveryError("invalid_help_payload") from error
        return PreparedDelivery(
            DeliveryAction.SEND_HELP,
            chat_id=chat_id,
            thread_id=thread_id,
            html=build_help_card(topic_kind),
            message_role=HELP_CARD_ROLE,
        )
    if item.payload.get("role") in {PROJECT_MODEL_PICKER_ROLE, PROJECT_MODEL_CONFIRM_ROLE}:
        return _prepare_project_model_message(item)
    if item.payload.get("role") == "secret_ingress":
        try:
            chat_id = int(item.payload["chat_id"])
            thread_id = int(item.payload["message_thread_id"])
            html_body = str(item.payload["html"])
            raw_buttons = item.payload["callback_buttons"]
            buttons = tuple(
                tuple((str(label), str(data)) for label, data in row) for row in raw_buttons
            )
        except (KeyError, TypeError, ValueError) as error:
            raise PermanentDeliveryError("invalid_secret_ingress_payload") from error
        return PreparedDelivery(
            DeliveryAction.SEND_STATUS,
            chat_id=chat_id,
            thread_id=thread_id,
            html=html_body,
            message_role="secret_ingress",
            callback_buttons=buttons,
        )
    if item.payload.get("role") == PROJECT_STATUS_DASHBOARD_ROLE:
        return await _prepare_project_status_dashboard(
            session, item, topics=topics, projects=projects, profiles=profiles
        )
    if item.payload.get("role") == TASK_HISTORY_ROLE:
        return await _prepare_task_history_report(session, item, topics=topics, projects=projects)
    if item.payload.get("role") == ORCHESTRATION_TRACE_ROLE:
        return await _prepare_orchestration_trace(
            session,
            item,
            topics=topics,
            trace_enabled=trace_enabled,
            trace_sample_percent=trace_sample_percent,
            trace_always_include_anomalies=trace_always_include_anomalies,
        )
    if item.payload.get("role") == DISCUSSION_REPLY_DESTINATION:
        return await _prepare_discussion_reply(session, item)
    if item.payload.get("role") == DISCUSSION_THINKING_ROLE:
        return await _prepare_discussion_thinking(session, item)
    if item.linked_entity_type == "project_naming":
        naming = await session.get(ProjectNamingRequest, item.linked_entity_id)
        if naming is None:
            raise PermanentDeliveryError("project_naming_missing")
        link = await session.scalar(
            select(TelegramMessageLink).where(
                TelegramMessageLink.task_id == naming.task_id,
                TelegramMessageLink.message_role == "project_naming",
            )
        )
        if item.operation_type == "delete_message":
            if link is None:
                return PreparedDelivery(
                    DeliveryAction.NOOP,
                    chat_id=naming.chat_id,
                    thread_id=naming.source_thread_id,
                )
            return PreparedDelivery(
                DeliveryAction.DELETE_MESSAGE,
                chat_id=link.chat_id,
                thread_id=link.message_thread_id,
                link_id=link.id,
                message_id=link.message_id,
            )
        revision = item.payload.get("revision")
        if (
            item.payload.get("role") != "project_name_options"
            or revision != naming.revision
            or naming.status.value != "pending"
            or len(naming.options) != 9
        ):
            raise PermanentDeliveryError("invalid_project_naming_delivery")
        if link is not None and link.projection_revision >= naming.revision:
            return PreparedDelivery(
                DeliveryAction.NOOP,
                chat_id=naming.chat_id,
                thread_id=naming.source_thread_id,
            )
        rows: list[tuple[tuple[str, str], ...]] = []
        for offset in range(0, 9, 3):
            rows.append(
                tuple(
                    (
                        str(option["display_name"]),
                        f"v1:pn:{naming.id.hex}:{naming.revision}:{index}",
                    )
                    for index, option in enumerate(
                        naming.options[offset : offset + 3], start=offset
                    )
                )
            )
        rows.append((("Другие варианты", f"v1:pn:{naming.id.hex}:{naming.revision}:r"),))
        html = (
            "<b>Выберите название проекта</b>\n"
            "Каждый вариант включает безопасное имя репозитория.\n\n"
            f"{telegram_html(naming.description)}"
        )
        return PreparedDelivery(
            DeliveryAction.SEND_PROJECT_NAMES,
            chat_id=naming.chat_id,
            thread_id=naming.source_thread_id,
            html=html,
            task_id=naming.task_id,
            revision=naming.revision,
            message_role="project_naming",
            callback_buttons=tuple(rows),
        )
    if item.linked_entity_type == "project_provisioning":
        if item.payload.get("role") != "project_created":
            raise PermanentDeliveryError("invalid_project_delivery_payload")
        provisioning = await session.get(ProjectProvisioning, item.linked_entity_id)
        if provisioning is None or provisioning.topic_thread_id is None:
            raise PermanentDeliveryError("project_provisioning_missing")
        html = (
            f"<b>{telegram_html(provisioning.display_name)}</b>\n"
            f"Проект создан: <code>{telegram_html(provisioning.project_id)}</code>\n\n"
            f"{telegram_html(provisioning.description)}"
        )
        return PreparedDelivery(
            DeliveryAction.SEND_PROJECT_WELCOME,
            chat_id=provisioning.chat_id,
            thread_id=provisioning.topic_thread_id,
            html=html,
            task_id=provisioning.task_id,
            message_role="project_welcome",
        )
    role = item.payload.get("role")
    intake: TelegramIntakeMessage | None = None
    if item.linked_entity_type == "telegram_intake":
        intake = await session.get(TelegramIntakeMessage, item.linked_entity_id)
        if intake is None:
            raise PermanentDeliveryError("telegram_intake_missing")
        task_id = intake.task_id
        chat_id = intake.chat_id
        thread_id = intake.message_thread_id
    elif item.linked_entity_type == "task" and role in {"intake_ack", "approval_card"}:
        task = await session.get(Task, item.linked_entity_id)
        if task is None or task.source_chat_id is None or task.source_thread_id is None:
            raise PermanentDeliveryError("telegram_task_projection_missing")
        task_id = task.id
        chat_id = task.source_chat_id
        thread_id = task.source_thread_id
    else:
        raise PermanentDeliveryError("unsupported_telegram_operation")
    if role == "semantic_clarification":
        assert intake is not None
        raw_id = item.payload.get("interpretation_id")
        try:
            interpretation_id = uuid.UUID(str(raw_id))
        except ValueError as error:
            raise PermanentDeliveryError("invalid_interpretation_id") from error
        interpretation = await session.get(Interpretation, interpretation_id)
        if interpretation is None or interpretation.task_id != intake.task_id:
            raise PermanentDeliveryError("interpretation_missing")
        question = interpretation.task_draft.get("clarification_question")
        title = interpretation.task_draft.get("normalized_title")
        if not isinstance(question, str) or not question:
            raise PermanentDeliveryError("clarification_question_missing")
        html = f"<b>{telegram_html(title or 'Clarification required')}</b>\n"
        html += telegram_html(question)
        return PreparedDelivery(
            DeliveryAction.SEND_CLARIFICATION,
            chat_id=intake.chat_id,
            thread_id=intake.message_thread_id,
            html=html,
            task_id=intake.task_id,
        )
    if role == "clarification":
        assert intake is not None
        try:
            candidate_ids = [uuid.UUID(value) for value in intake.ambiguous_task_ids]
        except ValueError as error:
            raise PermanentDeliveryError("invalid_candidate_task_id") from error
        candidates = (await session.scalars(select(Task).where(Task.id.in_(candidate_ids)))).all()
        summaries = [
            f"• <code>{task.id}</code> — {telegram_html(task.original_text.strip()[:80])}"
            for task in candidates
        ]
        html = "Multiple active tasks matched. Reply to the intended task status card:\n"
        html += "\n".join(summaries) if summaries else "No active candidates remain."
        return PreparedDelivery(
            DeliveryAction.SEND_CLARIFICATION,
            chat_id=intake.chat_id,
            thread_id=intake.message_thread_id,
            html=html,
        )
    if role not in {"intake_ack", "approval_card"} or task_id is None:
        raise PermanentDeliveryError("invalid_telegram_payload")
    approval_projection = role == "approval_card"
    card = (
        await build_approval_card(session, task_id)
        if approval_projection
        else await build_status_card(session, task_id)
    )
    message_role = "approval_card" if approval_projection else "task_status"
    if approval_projection:
        destination = topics.system_topic(chat_id, TopicKind.APPROVALS) if topics else None
        if destination is None or not destination.enabled:
            raise PermanentDeliveryError("approval_topic_missing")
        thread_id = destination.message_thread_id
    link = await session.scalar(
        select(TelegramMessageLink).where(
            TelegramMessageLink.task_id == card.task_id,
            TelegramMessageLink.message_role == message_role,
            *(
                (TelegramMessageLink.approval_id == card.approval_id,)
                if approval_projection
                else ()
            ),
        )
    )
    if link is not None and card.revision <= link.projection_revision:
        return PreparedDelivery(
            DeliveryAction.NOOP,
            chat_id=chat_id,
            thread_id=thread_id,
        )
    if link is None:
        return PreparedDelivery(
            DeliveryAction.SEND_STATUS,
            chat_id=chat_id,
            thread_id=thread_id,
            html=card.html,
            task_id=card.task_id,
            revision=card.revision,
            buttons=card.buttons,
            approval_id=card.approval_id,
            message_role=message_role,
        )
    return PreparedDelivery(
        DeliveryAction.EDIT_STATUS,
        chat_id=link.chat_id,
        thread_id=link.message_thread_id,
        html=card.html,
        task_id=card.task_id,
        revision=card.revision,
        link_id=link.id,
        message_id=link.message_id,
        buttons=card.buttons,
        approval_id=card.approval_id,
        message_role=message_role,
    )


async def _prepare_work_package_projection(
    session: AsyncSession, item: TransactionalOutbox
) -> PreparedDelivery:
    if item.operation_type == "render_topic_status":
        return await _prepare_project_topic_status(session, item)
    if item.operation_type == "render_topic_idle":
        return await _prepare_project_topic_status(session, item, idle=True)
    if item.linked_entity_type != "work_package":
        raise PermanentDeliveryError("invalid_work_package_projection_entity")
    package_id = item.linked_entity_id
    role = {
        "render_plan": WORK_PACKAGE_PLAN_ROLE,
        "render_status": WORK_PACKAGE_STATUS_ROLE,
        "render_action": WORK_PACKAGE_ACTION_ROLE,
        "render_detail": WORK_PACKAGE_DETAIL_ROLE,
        "clear_detail": WORK_PACKAGE_DETAIL_ROLE,
    }.get(item.operation_type)
    if role is None:
        raise PermanentDeliveryError("invalid_work_package_projection_operation")
    link = None
    if role != WORK_PACKAGE_STATUS_ROLE:
        link = await session.scalar(
            select(TelegramMessageLink).where(
                TelegramMessageLink.work_package_id == package_id,
                TelegramMessageLink.message_role == role,
            )
        )
    if item.operation_type == "clear_detail":
        if link is None:
            return PreparedDelivery(DeliveryAction.NOOP, chat_id=0, thread_id=None)
        return PreparedDelivery(
            DeliveryAction.DELETE_MESSAGE,
            chat_id=link.chat_id,
            thread_id=link.message_thread_id,
            link_id=link.id,
            message_id=link.message_id,
            work_package_id=package_id,
        )
    try:
        if item.operation_type == "render_plan":
            raw_page = item.payload.get("page", 1)
            if not isinstance(raw_page, int):
                raise WorkPackageProjectionError("invalid_page")
            card = await build_work_package_plan_card(session, package_id, page=raw_page)
        elif item.operation_type == "render_status":
            card = await build_work_package_status_card(session, package_id)
        elif item.operation_type == "render_action":
            card = await build_work_package_action_card(session, package_id)
        else:
            detail_card = await build_work_package_detail_card(session, package_id)
            if detail_card is None:
                if link is None:
                    return PreparedDelivery(DeliveryAction.NOOP, chat_id=0, thread_id=None)
                return PreparedDelivery(
                    DeliveryAction.DELETE_MESSAGE,
                    chat_id=link.chat_id,
                    thread_id=link.message_thread_id,
                    link_id=link.id,
                    message_id=link.message_id,
                    work_package_id=package_id,
                )
            card = detail_card
    except WorkPackageProjectionError as error:
        raise PermanentDeliveryError(str(error)) from error
    if role == WORK_PACKAGE_STATUS_ROLE:
        link = await session.scalar(
            select(TelegramMessageLink).where(
                TelegramMessageLink.chat_id == card.chat_id,
                TelegramMessageLink.message_thread_id == card.thread_id,
                TelegramMessageLink.message_role == WORK_PACKAGE_STATUS_ROLE,
            )
        )
    action = DeliveryAction.SEND_STATUS if link is None else DeliveryAction.EDIT_STATUS
    return PreparedDelivery(
        action,
        chat_id=card.chat_id,
        thread_id=card.thread_id,
        html=card.html,
        revision=card.status_generation,
        link_id=None if link is None else link.id,
        message_id=None if link is None else link.message_id,
        message_role=card.role,
        callback_buttons=card.callback_buttons,
        work_package_id=card.package_id,
        plan_revision_id=card.revision_id,
        control_status_generation=card.status_generation,
        pin_after_send=role == WORK_PACKAGE_STATUS_ROLE and link is None,
    )


async def _prepare_project_topic_status(
    session: AsyncSession, item: TransactionalOutbox, *, idle: bool = False
) -> PreparedDelivery:
    if item.linked_entity_type != "topic_mapping":
        raise PermanentDeliveryError("invalid_topic_status_entity")
    mapping = await session.get(TopicMapping, item.linked_entity_id)
    if mapping is None or not mapping.enabled or mapping.project_id is None:
        raise PermanentDeliveryError("topic_status_mapping_missing")
    try:
        chat_id = int(item.payload["chat_id"])
        thread_id = int(item.payload["thread_id"])
        revision = int(item.payload["revision"])
        project_id = str(item.payload["project_id"])
    except (KeyError, TypeError, ValueError) as error:
        raise PermanentDeliveryError("invalid_topic_status_payload") from error
    if (
        mapping.chat_id != chat_id
        or mapping.message_thread_id != thread_id
        or mapping.project_id != project_id
    ):
        raise PermanentDeliveryError("topic_status_mapping_mismatch")
    link = await session.scalar(
        select(TelegramMessageLink).where(
            TelegramMessageLink.chat_id == chat_id,
            TelegramMessageLink.message_thread_id == thread_id,
            TelegramMessageLink.message_role == WORK_PACKAGE_STATUS_ROLE,
        )
    )
    if link is not None and link.projection_revision >= revision:
        return PreparedDelivery(DeliveryAction.NOOP, chat_id=chat_id, thread_id=thread_id)
    preference = await load_preference(session, project_id)
    worker = "Auto" if preference.worker_key is None else preference.worker_key.value.title()
    state = "Waiting" if idle else "Thinking"
    return PreparedDelivery(
        DeliveryAction.SEND_STATUS if link is None else DeliveryAction.EDIT_STATUS,
        chat_id=chat_id,
        thread_id=thread_id,
        html=f"<b>{state} | {telegram_html(worker)}</b>",
        revision=revision,
        link_id=None if link is None else link.id,
        message_id=None if link is None else link.message_id,
        message_role=WORK_PACKAGE_STATUS_ROLE,
        pin_after_send=link is None,
        project_id=project_id,
    )


async def _resolve_status_dashboard_thread(
    session: AsyncSession,
    chat_id: int,
    topics: TopicRegistry | None,
) -> int:
    """Resolve the existing task_dashboard topic thread for this forum chat."""

    destination = (
        topics.system_topic(chat_id, STATUS_DASHBOARD_TOPIC_KIND) if topics is not None else None
    )
    if destination is not None and destination.enabled:
        return destination.message_thread_id
    mapping = await session.scalar(
        select(TopicMapping).where(
            TopicMapping.chat_id == chat_id,
            TopicMapping.topic_kind == STATUS_DASHBOARD_TOPIC_KIND.value,
            TopicMapping.enabled.is_(True),
        )
    )
    if mapping is None:
        raise PermanentDeliveryError("status_dashboard_topic_missing")
    return mapping.message_thread_id


async def _prepare_discussion_reply(
    session: AsyncSession, item: TransactionalOutbox
) -> PreparedDelivery:
    if item.linked_entity_type != "conversation_turn":
        raise PermanentDeliveryError("invalid_discussion_reply_entity")
    turn = await session.get(ConversationTurn, item.linked_entity_id)
    if (
        turn is None
        or turn.role is not ConversationTurnRole.ASSISTANT
        or turn.source is not ConversationTurnSource.MODEL
    ):
        raise PermanentDeliveryError("discussion_reply_turn_missing")
    discussion = await session.get(ProjectDiscussionSession, turn.session_id)
    if discussion is None or str(discussion.id) != item.payload.get("session_id"):
        raise PermanentDeliveryError("discussion_reply_session_mismatch")
    source_turn_id = item.payload.get("source_turn_id")
    thinking_link = None
    if isinstance(source_turn_id, str):
        try:
            thinking_role = _discussion_thinking_message_role(uuid.UUID(source_turn_id))
        except ValueError:
            thinking_role = ""
        if thinking_role:
            thinking_link = await session.scalar(
                select(TelegramMessageLink).where(
                    TelegramMessageLink.chat_id == discussion.chat_id,
                    TelegramMessageLink.message_thread_id == discussion.message_thread_id,
                    TelegramMessageLink.message_role == thinking_role,
                )
            )
    if thinking_link is not None:
        return PreparedDelivery(
            DeliveryAction.EDIT_MESSAGE,
            chat_id=discussion.chat_id,
            thread_id=discussion.message_thread_id,
            html=telegram_markdown_html(turn.content),
            fallback_html=telegram_html(turn.content),
            link_id=thinking_link.id,
            message_id=thinking_link.message_id,
            message_role=DISCUSSION_REPLY_DESTINATION,
            project_id=discussion.project_id,
        )
    return PreparedDelivery(
        DeliveryAction.SEND_DISCUSSION_REPLY,
        chat_id=discussion.chat_id,
        thread_id=discussion.message_thread_id,
        html=telegram_markdown_html(turn.content),
        fallback_html=telegram_html(turn.content),
        message_role=DISCUSSION_REPLY_DESTINATION,
        project_id=discussion.project_id,
    )


async def _prepare_discussion_thinking(
    session: AsyncSession, item: TransactionalOutbox
) -> PreparedDelivery:
    if item.linked_entity_type != "conversation_turn":
        raise PermanentDeliveryError("invalid_discussion_thinking_entity")
    turn = await session.get(ConversationTurn, item.linked_entity_id)
    if turn is None or turn.role is not ConversationTurnRole.USER:
        raise PermanentDeliveryError("discussion_thinking_turn_missing")
    discussion = await session.get(ProjectDiscussionSession, turn.session_id)
    if discussion is None or str(discussion.id) != item.payload.get("session_id"):
        raise PermanentDeliveryError("discussion_thinking_session_mismatch")
    role = _discussion_thinking_message_role(turn.id)
    existing = await session.scalar(
        select(TelegramMessageLink).where(
            TelegramMessageLink.chat_id == discussion.chat_id,
            TelegramMessageLink.message_thread_id == discussion.message_thread_id,
            TelegramMessageLink.message_role == role,
        )
    )
    if existing is not None:
        return PreparedDelivery(
            DeliveryAction.NOOP,
            chat_id=discussion.chat_id,
            thread_id=discussion.message_thread_id,
        )
    return PreparedDelivery(
        DeliveryAction.SEND_DISCUSSION_REPLY,
        chat_id=discussion.chat_id,
        thread_id=discussion.message_thread_id,
        html="<i>Думаю…</i>",
        message_role=role,
    )


def _discussion_thinking_message_role(turn_id: uuid.UUID) -> str:
    return f"thinking:{turn_id.hex}"


async def _resolve_system_trace_thread(
    session: AsyncSession,
    chat_id: int,
    topics: TopicRegistry | None,
) -> int:
    destination = topics.system_topic(chat_id, TopicKind.SYSTEM) if topics is not None else None
    if destination is not None and destination.enabled:
        return destination.message_thread_id
    mapping = await session.scalar(
        select(TopicMapping).where(
            TopicMapping.chat_id == chat_id,
            TopicMapping.topic_kind == TopicKind.SYSTEM.value,
            TopicMapping.enabled.is_(True),
        )
    )
    if mapping is None:
        raise PermanentDeliveryError("system_topic_missing")
    return mapping.message_thread_id


async def _prepare_orchestration_trace(
    session: AsyncSession,
    item: TransactionalOutbox,
    *,
    topics: TopicRegistry | None,
    trace_enabled: bool,
    trace_sample_percent: int,
    trace_always_include_anomalies: bool,
) -> PreparedDelivery:
    try:
        task_id = uuid.UUID(str(item.payload["task_id"]))
    except (KeyError, TypeError, ValueError) as error:
        raise PermanentDeliveryError("invalid_orchestration_trace_task_id") from error
    task = await session.get(Task, task_id)
    if task is None:
        raise PermanentDeliveryError("orchestration_trace_task_missing")
    trace_kind = item.payload.get("trace_kind")
    if trace_kind == INTERPRETER_TRACE_KIND:
        interpretation = await session.get(Interpretation, item.linked_entity_id)
        if interpretation is None or interpretation.task_id != task.id:
            raise PermanentDeliveryError("orchestration_trace_interpretation_missing")
        if not should_deliver_orchestration_trace(
            task.id,
            enabled=trace_enabled,
            sample_percent=trace_sample_percent,
            anomalous=interpreter_trace_is_anomalous(interpretation, item.payload),
            always_include_anomalies=trace_always_include_anomalies,
        ):
            return PreparedDelivery(
                DeliveryAction.NOOP,
                chat_id=task.source_chat_id,
                thread_id=None,
            )
        thread_id = await _resolve_system_trace_thread(session, task.source_chat_id, topics)
        return PreparedDelivery(
            DeliveryAction.SEND_STATUS,
            chat_id=task.source_chat_id,
            thread_id=thread_id,
            html=build_interpreter_trace_html(task, interpretation, item.payload),
            task_id=task.id,
            revision=1,
            message_role="trace_interpreter",
        )
    if trace_kind != PLANNER_TRACE_KIND:
        raise PermanentDeliveryError("invalid_orchestration_trace_kind")
    step = await session.get(Step, item.linked_entity_id)
    if step is None or step.step_type != "plan":
        raise PermanentDeliveryError("orchestration_trace_planner_missing")
    run = await session.get(Run, step.run_id)
    if run is None or run.task_id != task.id:
        raise PermanentDeliveryError("orchestration_trace_run_missing")
    if not should_deliver_orchestration_trace(
        task.id,
        enabled=trace_enabled,
        sample_percent=trace_sample_percent,
        anomalous=planner_trace_is_anomalous(step),
        always_include_anomalies=trace_always_include_anomalies,
    ):
        return PreparedDelivery(
            DeliveryAction.NOOP,
            chat_id=task.source_chat_id,
            thread_id=None,
        )
    thread_id = await _resolve_system_trace_thread(session, task.source_chat_id, topics)
    usage = await session.scalar(
        select(UsageRecord)
        .where(UsageRecord.step_id == step.id)
        .order_by(UsageRecord.created_at.desc())
        .limit(1)
    )
    reservation = None
    raw_reservation_id = step.payload.get("budget_reservation_id")
    try:
        reservation_id = uuid.UUID(str(raw_reservation_id))
    except (TypeError, ValueError):
        reservation_id = None
    if reservation_id is not None:
        reservation = await session.get(ProviderBudgetReservation, reservation_id)
    return PreparedDelivery(
        DeliveryAction.SEND_STATUS,
        chat_id=task.source_chat_id,
        thread_id=thread_id,
        html=build_planner_trace_html(task, step, usage=usage, reservation=reservation),
        task_id=task.id,
        run_id=run.id,
        step_id=step.id,
        revision=max(1, step.attempt_count),
        message_role="trace_planner",
    )


async def _prepare_task_history_report(
    session: AsyncSession,
    item: TransactionalOutbox,
    *,
    topics: TopicRegistry | None,
    projects: ProjectRegistry | None,
) -> PreparedDelivery:
    """Deliver a one-shot completion report into «История» (changelog)."""

    raw_task_id = item.payload.get("task_id")
    try:
        task_id = uuid.UUID(str(raw_task_id))
    except (TypeError, ValueError) as error:
        raise PermanentDeliveryError("invalid_history_task_id") from error
    project_names = (
        {project.id: project.display_name for project in projects.items()}
        if projects is not None
        else None
    )
    raw_status = item.payload.get("terminal_status")
    try:
        expected_status = TaskStatus(str(raw_status)) if raw_status else None
    except ValueError as error:
        raise PermanentDeliveryError("invalid_history_terminal_status") from error
    report = await build_task_history_report(
        session,
        task_id,
        project_names=project_names,
        expected_status=expected_status,
    )
    if report is None:
        return PreparedDelivery(
            DeliveryAction.NOOP,
            chat_id=int(item.payload.get("chat_id") or 0),
            thread_id=None,
        )
    thread_id = report.thread_id
    if topics is not None:
        destination = topics.system_topic(report.chat_id, HISTORY_TOPIC_KIND)
        if destination is not None and destination.enabled:
            thread_id = destination.message_thread_id
    if expected_status is None or expected_status is TaskStatus.COMPLETED:
        message_role = TASK_HISTORY_ROLE
    else:
        message_role = f"{TASK_HISTORY_ROLE}_{expected_status.value}"
    existing = await session.scalar(
        select(TelegramMessageLink).where(
            TelegramMessageLink.task_id == task_id,
            TelegramMessageLink.message_role == message_role,
        )
    )
    if existing is not None:
        return PreparedDelivery(
            DeliveryAction.NOOP,
            chat_id=report.chat_id,
            thread_id=thread_id,
        )
    return PreparedDelivery(
        DeliveryAction.SEND_STATUS,
        chat_id=report.chat_id,
        thread_id=thread_id,
        html=report.html,
        task_id=task_id,
        revision=report.revision,
        message_role=message_role,
    )


def _prepare_project_model_message(item: TransactionalOutbox) -> PreparedDelivery:
    """Deliver the project ``/model`` picker or confirmation card."""

    try:
        chat_id = int(item.payload["chat_id"])
        thread_id = int(item.payload["message_thread_id"])
        html = str(item.payload["html"])
    except (KeyError, TypeError, ValueError) as error:
        raise PermanentDeliveryError("invalid_model_picker_payload") from error
    if not html.strip():
        raise PermanentDeliveryError("invalid_model_picker_payload")
    raw_buttons = item.payload.get("callback_buttons") or ()
    callback_buttons: list[tuple[tuple[str, str], ...]] = []
    if not isinstance(raw_buttons, (list, tuple)):
        raise PermanentDeliveryError("invalid_model_picker_payload")
    for row in raw_buttons:
        if not isinstance(row, (list, tuple)):
            raise PermanentDeliveryError("invalid_model_picker_payload")
        parsed_row: list[tuple[str, str]] = []
        for button in row:
            if not isinstance(button, (list, tuple)) or len(button) != 2:
                raise PermanentDeliveryError("invalid_model_picker_payload")
            label, data = button
            if not isinstance(label, str) or not isinstance(data, str):
                raise PermanentDeliveryError("invalid_model_picker_payload")
            parsed_row.append((label, data))
        callback_buttons.append(tuple(parsed_row))
    message_id = item.payload.get("message_id")
    if message_id is not None:
        try:
            message_id = int(message_id)
        except (TypeError, ValueError) as error:
            raise PermanentDeliveryError("invalid_model_picker_payload") from error
        if message_id < 1:
            raise PermanentDeliveryError("invalid_model_picker_payload")
    return PreparedDelivery(
        DeliveryAction.SEND_MODEL_PICKER if message_id is None else DeliveryAction.EDIT_MESSAGE,
        chat_id=chat_id,
        thread_id=thread_id,
        html=html,
        message_id=message_id,
        message_role=str(item.payload.get("role") or PROJECT_MODEL_PICKER_ROLE),
        callback_buttons=tuple(callback_buttons),
        project_id=str(item.payload.get("project_id") or "") or None,
    )


async def _prepare_project_status_dashboard(
    session: AsyncSession,
    item: TransactionalOutbox,
    *,
    topics: TopicRegistry | None,
    projects: ProjectRegistry | None,
    profiles: ProfileRegistry | None,
) -> PreparedDelivery:
    """Deliver into the forum's configured «Статус проектов» (task_dashboard) topic."""

    try:
        chat_id = int(item.payload["chat_id"])
    except (KeyError, TypeError, ValueError) as error:
        raise PermanentDeliveryError("invalid_dashboard_chat_id") from error
    thread_id = await _resolve_status_dashboard_thread(session, chat_id, topics)
    project_names = (
        {project.id: project.display_name for project in projects.items()}
        if projects is not None
        else None
    )
    profile_models = (
        {profile.id: profile.model for profile in profiles.items()}
        if profiles is not None
        else None
    )
    profile_efforts = (
        {profile.id: profile.model_reasoning_effort for profile in profiles.items()}
        if profiles is not None
        else None
    )
    profile_providers = (
        {profile.id: profile.provider for profile in profiles.items()}
        if profiles is not None
        else None
    )
    card = await build_project_status_dashboard(
        session,
        chat_id,
        project_names=project_names,
        profile_models=profile_models,
        profile_efforts=profile_efforts,
        profile_providers=profile_providers,
    )
    link = await session.scalar(
        select(TelegramMessageLink).where(
            TelegramMessageLink.chat_id == chat_id,
            TelegramMessageLink.message_thread_id == thread_id,
            TelegramMessageLink.message_role == PROJECT_STATUS_DASHBOARD_ROLE,
        )
    )
    if link is not None and card.revision == link.projection_revision:
        return PreparedDelivery(
            DeliveryAction.NOOP,
            chat_id=chat_id,
            thread_id=thread_id,
        )
    if link is None:
        return PreparedDelivery(
            DeliveryAction.SEND_STATUS,
            chat_id=chat_id,
            thread_id=thread_id,
            html=card.html,
            revision=card.revision,
            message_role=PROJECT_STATUS_DASHBOARD_ROLE,
        )
    return PreparedDelivery(
        DeliveryAction.EDIT_STATUS,
        chat_id=link.chat_id,
        thread_id=link.message_thread_id,
        html=card.html,
        revision=card.revision,
        link_id=link.id,
        message_id=link.message_id,
        message_role=PROJECT_STATUS_DASHBOARD_ROLE,
    )


class TelegramDeliveryService:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        client: TelegramClient,
        *,
        owner: str,
        lease_seconds: int,
        max_attempts: int,
        retry_min_seconds: float,
        retry_max_seconds: float,
        topics: TopicRegistry | None = None,
        projects: ProjectRegistry | None = None,
        profiles: ProfileRegistry | None = None,
        trace_enabled: bool = True,
        trace_sample_percent: int = 100,
        trace_always_include_anomalies: bool = True,
        dogfood_settings: TelegramDogfoodSettings | None = None,
    ) -> None:
        self._factory = session_factory
        self._client = client
        self._owner = owner
        self._lease_seconds = lease_seconds
        self._max_attempts = max_attempts
        self._retry_min = retry_min_seconds
        self._retry_max = retry_max_seconds
        self._topics = topics
        self._projects = projects
        self._profiles = profiles
        self._trace_enabled = trace_enabled
        self._trace_sample_percent = trace_sample_percent
        self._trace_always_include_anomalies = trace_always_include_anomalies
        self._dogfood_settings = dogfood_settings or TelegramDogfoodSettings()
        self._logger = get_logger(__name__)

    async def deliver_one(self) -> bool:
        async with self._factory.begin() as session:
            token = await claim_outbox_item(
                session,
                owner=self._owner,
                lease_seconds=self._lease_seconds,
                allowed_destinations=TELEGRAM_DESTINATIONS,
            )
        if token is None:
            return False
        try:
            async with self._factory() as session:
                item = await session.get(TransactionalOutbox, token.item_id)
                assert item is not None
                attempt_count = item.attempt_count
                prepared = await prepare_delivery(
                    session,
                    item,
                    self._topics,
                    projects=self._projects,
                    profiles=self._profiles,
                    trace_enabled=self._trace_enabled,
                    trace_sample_percent=self._trace_sample_percent,
                    trace_always_include_anomalies=self._trace_always_include_anomalies,
                )
            if prepared.action == DeliveryAction.NOOP:
                await self._complete(token, prepared, None)
                return True
            if await self._consume_transient_dogfood_fault(prepared):
                raise TimedOut("controlled dogfood Telegram failure before request")
            try:
                confirmed_message_id = await self._call_telegram(prepared)
            except (TimedOut, NetworkError) as error:
                if prepared.action in {
                    DeliveryAction.SEND_STATUS,
                    DeliveryAction.SEND_CLARIFICATION,
                    DeliveryAction.SEND_PROJECT_WELCOME,
                    DeliveryAction.SEND_PROJECT_NAMES,
                    DeliveryAction.SEND_MODEL_PICKER,
                    DeliveryAction.SEND_HELP,
                    DeliveryAction.SEND_DISCUSSION_REPLY,
                }:
                    raise LostTelegramResponse("Telegram send outcome is unknown") from error
                raise
            await self._complete(token, prepared, confirmed_message_id)
            self._logger.info(
                "Telegram outbox item delivered",
                extra={"event": "telegram.delivery.delivered", "outbox_id": str(token.item_id)},
            )
        except LostTelegramResponse:
            await self._mark_ambiguous(token)
        except RetryAfter as error:
            await self._defer_rate_limit(token, error)
        except (TimedOut, NetworkError) as error:
            await self._handle_transient(token, attempt_count, type(error).__name__.lower())
        except (BadRequest, Forbidden) as error:
            await self._dead_letter(token, f"telegram_{type(error).__name__.lower()}")
        except TelegramError as error:
            await self._dead_letter(token, f"telegram_{type(error).__name__.lower()}")
        except PermanentDeliveryError as error:
            await self._dead_letter(token, error.category)
        except LeaseLost:
            self._logger.warning(
                "Telegram delivery lease was lost",
                extra={"event": "telegram.delivery.lease_lost", "outbox_id": str(token.item_id)},
            )
        return True

    async def _consume_transient_dogfood_fault(self, prepared: PreparedDelivery) -> bool:
        project_id = prepared.project_id
        async with self._factory.begin() as session:
            if project_id is None and prepared.task_id is not None:
                project_id = await session.scalar(
                    select(Task.project_id).where(Task.id == prepared.task_id)
                )
            if project_id is None and prepared.work_package_id is not None:
                project_id = await session.scalar(
                    select(WorkPackage.project_id).where(WorkPackage.id == prepared.work_package_id)
                )
            if project_id is None:
                return False
            return (
                await consume_fault(
                    session,
                    self._dogfood_settings,
                    project_id=project_id,
                    fault=DogfoodFault.TELEGRAM_TRANSIENT_BEFORE_REQUEST,
                    consumer=self._owner,
                )
                is not None
            )

    async def _call_telegram(self, prepared: PreparedDelivery) -> int | None:
        if prepared.action in {
            DeliveryAction.SEND_STATUS,
            DeliveryAction.SEND_CLARIFICATION,
            DeliveryAction.SEND_PROJECT_WELCOME,
            DeliveryAction.SEND_PROJECT_NAMES,
            DeliveryAction.SEND_MODEL_PICKER,
            DeliveryAction.SEND_HELP,
            DeliveryAction.SEND_DISCUSSION_REPLY,
        }:
            try:
                message_id = await self._client.send_message(
                    chat_id=prepared.chat_id,
                    thread_id=prepared.thread_id,
                    html=prepared.html,
                    buttons=prepared.buttons,
                    task_id=prepared.task_id,
                    approval_id=prepared.approval_id,
                    callback_buttons=prepared.callback_buttons,
                )
            except BadRequest:
                if prepared.fallback_html is None:
                    raise
                message_id = await self._client.send_message(
                    chat_id=prepared.chat_id,
                    thread_id=prepared.thread_id,
                    html=prepared.fallback_html,
                    buttons=prepared.buttons,
                    task_id=prepared.task_id,
                    approval_id=prepared.approval_id,
                    callback_buttons=prepared.callback_buttons,
                )
            if not message_id:
                raise LostTelegramResponse("Telegram returned no confirmed message ID")
            if prepared.pin_after_send:
                await self._client.pin_message(chat_id=prepared.chat_id, message_id=message_id)
            return message_id
        if prepared.action == DeliveryAction.DELETE_MESSAGE:
            assert prepared.message_id is not None
            await self._client.delete_message(
                chat_id=prepared.chat_id,
                message_id=prepared.message_id,
            )
            return None
        assert prepared.message_id is not None
        await self._client.edit_message(
            chat_id=prepared.chat_id,
            message_id=prepared.message_id,
            html=prepared.html,
            buttons=prepared.buttons,
            task_id=prepared.task_id,
            approval_id=prepared.approval_id,
            callback_buttons=prepared.callback_buttons,
        )
        return None

    async def _complete(
        self,
        token: OutboxLeaseToken,
        prepared: PreparedDelivery,
        confirmed_message_id: int | None,
    ) -> None:
        async with self._factory.begin() as session:
            if prepared.action == DeliveryAction.SEND_STATUS:
                assert prepared.revision is not None
                assert confirmed_message_id is not None
                if (
                    prepared.message_role
                    not in {PROJECT_STATUS_DASHBOARD_ROLE, WORK_PACKAGE_STATUS_ROLE}
                    and prepared.work_package_id is None
                ):
                    assert prepared.task_id is not None
                session.add(
                    TelegramMessageLink(
                        chat_id=prepared.chat_id,
                        message_thread_id=prepared.thread_id,
                        message_id=confirmed_message_id,
                        task_id=prepared.task_id,
                        run_id=prepared.run_id,
                        step_id=prepared.step_id,
                        approval_id=prepared.approval_id,
                        work_package_id=prepared.work_package_id,
                        plan_revision_id=prepared.plan_revision_id,
                        control_status_generation=prepared.control_status_generation,
                        message_role=prepared.message_role or "task_status",
                        projection_revision=prepared.revision,
                    )
                )
            elif prepared.action == DeliveryAction.EDIT_STATUS:
                assert prepared.link_id is not None and prepared.revision is not None
                link = await session.get(TelegramMessageLink, prepared.link_id)
                if link is None:
                    raise LeaseLost(f"Telegram projection disappeared: {prepared.link_id}")
                link.projection_revision = prepared.revision
                if prepared.message_role == WORK_PACKAGE_STATUS_ROLE:
                    link.work_package_id = prepared.work_package_id
                    link.plan_revision_id = prepared.plan_revision_id
                    link.control_status_generation = prepared.control_status_generation
                elif prepared.work_package_id is not None:
                    link.plan_revision_id = prepared.plan_revision_id
                    link.control_status_generation = prepared.control_status_generation
            elif prepared.action == DeliveryAction.SEND_CLARIFICATION:
                assert confirmed_message_id is not None
                session.add(
                    TelegramMessageLink(
                        chat_id=prepared.chat_id,
                        message_thread_id=prepared.thread_id,
                        message_id=confirmed_message_id,
                        task_id=prepared.task_id,
                        message_role="clarification",
                    )
                )
            elif prepared.action == DeliveryAction.SEND_DISCUSSION_REPLY:
                assert confirmed_message_id is not None
                session.add(
                    TelegramMessageLink(
                        chat_id=prepared.chat_id,
                        message_thread_id=prepared.thread_id,
                        message_id=confirmed_message_id,
                        task_id=None,
                        message_role=prepared.message_role or DISCUSSION_REPLY_DESTINATION,
                    )
                )
            elif prepared.action == DeliveryAction.SEND_PROJECT_WELCOME:
                assert confirmed_message_id is not None
                session.add(
                    TelegramMessageLink(
                        chat_id=prepared.chat_id,
                        message_thread_id=prepared.thread_id,
                        message_id=confirmed_message_id,
                        task_id=prepared.task_id,
                        message_role=prepared.message_role or "project_welcome",
                    )
                )
            elif prepared.action == DeliveryAction.SEND_PROJECT_NAMES:
                assert prepared.task_id is not None and prepared.revision is not None
                assert confirmed_message_id is not None
                session.add(
                    TelegramMessageLink(
                        chat_id=prepared.chat_id,
                        message_thread_id=prepared.thread_id,
                        message_id=confirmed_message_id,
                        task_id=prepared.task_id,
                        message_role="project_naming",
                        projection_revision=prepared.revision,
                    )
                )
            elif prepared.action == DeliveryAction.DELETE_MESSAGE:
                # Project-naming deletes track a projection link; /update command deletes do not.
                if prepared.link_id is not None:
                    link = await session.get(TelegramMessageLink, prepared.link_id)
                    if link is not None:
                        await session.delete(link)
            if (
                prepared.message_role == DISCUSSION_REPLY_DESTINATION
                and prepared.project_id is not None
                and prepared.thread_id is not None
            ):
                await enqueue_project_topic_idle(
                    session,
                    chat_id=prepared.chat_id,
                    thread_id=prepared.thread_id,
                    project_id=prepared.project_id,
                    source_outbox_id=token.item_id,
                )
            await complete_outbox_item(session, token)

    async def _mark_ambiguous(self, token: OutboxLeaseToken) -> None:
        async with self._factory.begin() as session:
            await mark_outbox_ambiguous(session, token)

    async def _handle_transient(
        self, token: OutboxLeaseToken, attempt_count: int, category: str
    ) -> None:
        if attempt_count >= self._max_attempts:
            await self._dead_letter(token, category)
            return
        delay = min(self._retry_max, self._retry_min * (2 ** (attempt_count - 1)))
        async with self._factory.begin() as session:
            await retry_outbox_item(session, token, delay_seconds=delay, error_category=category)

    async def _defer_rate_limit(self, token: OutboxLeaseToken, error: RetryAfter) -> None:
        """Honor Telegram backpressure without consuming the retry budget."""

        retry_after = error.retry_after
        delay = (
            retry_after.total_seconds() if hasattr(retry_after, "total_seconds") else retry_after
        )
        delay_seconds = max(self._retry_min, min(self._retry_max, float(delay)))
        async with self._factory.begin() as session:
            await defer_outbox_item(
                session,
                token,
                delay_seconds=delay_seconds,
                reason="retryafter",
            )

    async def _dead_letter(self, token: OutboxLeaseToken, category: str) -> None:
        async with self._factory.begin() as session:
            await dead_letter_outbox_item(session, token, error_category=category[:100])


async def run_delivery_loop(
    service: DeliveryRunner,
    *,
    poll_interval_seconds: float,
    stop_event: asyncio.Event,
) -> None:
    logger = get_logger(__name__)
    logger.info("Telegram delivery ready", extra={"event": "telegram.delivery.ready"})
    while not stop_event.is_set():
        delivered = await service.deliver_one()
        if not delivered:
            with suppress(TimeoutError):
                await asyncio.wait_for(stop_event.wait(), timeout=poll_interval_seconds)
    logger.info("Telegram delivery stopped", extra={"event": "telegram.delivery.stopped"})
