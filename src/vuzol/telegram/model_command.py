"""Project-topic ``/model`` command and callback UX for executor preferences."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from enum import StrEnum

from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.config import RegistryError, RuntimeConfiguration
from vuzol.config.models import ProviderProfileConfig, TopicKind
from vuzol.projects.executor_preference import (
    REASONING_EFFORTS,
    ExecutorPreferenceError,
    ExecutorWorkerKey,
    WorkerOption,
    auto_callback_data,
    connection_label,
    effort_callback_data,
    ensure_preference_row,
    executor_connections,
    format_preference_label,
    load_preference,
    set_auto_preference,
    set_worker_preference,
    worker_callback_data,
    workers_for_profile,
)
from vuzol.storage.models import TransactionalOutbox
from vuzol.telegram.domain import ControlUpdate
from vuzol.telegram.projections import enqueue_project_status_dashboard, telegram_html

PROJECT_MODEL_PICKER_ROLE = "project_model_picker"
PROJECT_MODEL_CONFIRM_ROLE = "project_model_confirm"


class ModelPickerStage(StrEnum):
    CONNECTION = "connection"
    MODEL = "model"
    EFFORT = "effort"
    CONFIRM = "confirm"


@dataclass(frozen=True, slots=True)
class ModelCommandOutcome:
    project_id: str
    stage: ModelPickerStage


def build_worker_picker_html(*, project_id: str, current_label: str) -> str:
    return (
        f"<b>Модель проекта</b> <code>{telegram_html(project_id)}</code>\n"
        f"Сейчас: <b>{telegram_html(current_label)}</b>\n\n"
        "Выберите подключение. Настройка применяется к новым задачам, пока вы её не измените."
    )


def build_effort_picker_html(*, project_id: str, worker_label: str) -> str:
    return (
        f"<b>Глубина рассуждения</b> · <b>{telegram_html(worker_label)}</b>\n"
        f"Проект: <code>{telegram_html(project_id)}</code>\n\n"
        "Чем выше уровень, тем медленнее и обычно тщательнее работа."
    )


def build_model_picker_html(*, project_id: str, connection: str) -> str:
    return (
        f"<b>Выберите модель</b> · <b>{telegram_html(connection)}</b>\n"
        f"Проект: <code>{telegram_html(project_id)}</code>"
    )


def build_confirm_html(*, project_id: str, label: str) -> str:
    return (
        f"<b>Модель изменена</b>\n"
        f"Проект: <code>{telegram_html(project_id)}</code>\n"
        f"Воркер: <b>{telegram_html(label)}</b>\n\n"
        "Выбор применяется к новым задачам проекта."
    )


def worker_keyboard(
    *,
    revision: int,
    connections: tuple[ProviderProfileConfig, ...] = (),
    workers: tuple[WorkerOption, ...] = (),
) -> tuple[tuple[tuple[str, str], ...], ...]:
    rows: list[tuple[tuple[str, str], ...]] = [
        (("Routing (auto)", auto_callback_data(revision)),),
    ]
    current: list[tuple[str, str]] = []
    choices = (
        tuple(
            (connection_label(profile), f"v2:pm:c:{revision}:{profile.id}")
            for profile in connections
        )
        if connections
        else tuple((option.label, worker_callback_data(revision, option.key)) for option in workers)
    )
    for label, callback in choices:
        current.append((label, callback))
        if len(current) == 2:
            rows.append(tuple(current))
            current = []
    if current:
        rows.append(tuple(current))
    return tuple(rows)


def effort_keyboard(
    *,
    revision: int,
    worker: ExecutorWorkerKey,
    profile_id: str | None = None,
) -> tuple[tuple[tuple[str, str], ...], ...]:
    buttons = [
        (
            effort,
            f"v2:pm:e:{revision}:{profile_id}:{worker.value}:{effort}"
            if profile_id
            else effort_callback_data(revision, worker, effort),
        )
        for effort in REASONING_EFFORTS
    ]
    rows: list[tuple[tuple[str, str], ...]] = []
    for index in range(0, len(buttons), 2):
        rows.append(tuple(buttons[index : index + 2]))
    return tuple(rows)


async def enqueue_worker_picker(
    session: AsyncSession,
    *,
    runtime: RuntimeConfiguration,
    project_id: str,
    chat_id: int,
    message_thread_id: int,
    inbox_id: uuid.UUID,
) -> None:
    await ensure_preference_row(session, project_id)
    preference = await load_preference(session, project_id)
    connections = executor_connections(runtime.registries)
    session.add(
        TransactionalOutbox(
            destination="telegram",
            operation_type="send_message",
            linked_entity_type="telegram_inbox",
            linked_entity_id=inbox_id,
            idempotency_key=(
                f"telegram:model_picker:{chat_id}:{message_thread_id}:{preference.revision}:"
                f"{inbox_id}:worker"
            ),
            payload={
                "role": PROJECT_MODEL_PICKER_ROLE,
                "stage": ModelPickerStage.CONNECTION.value,
                "chat_id": chat_id,
                "message_thread_id": message_thread_id,
                "project_id": project_id,
                "revision": preference.revision,
                "html": build_worker_picker_html(
                    project_id=project_id,
                    current_label=format_preference_label(preference),
                ),
                "callback_buttons": worker_keyboard(
                    revision=preference.revision, connections=connections
                ),
            },
        )
    )


class ProjectModelController:
    def __init__(self, runtime: RuntimeConfiguration) -> None:
        self._runtime = runtime

    async def apply(
        self,
        session: AsyncSession,
        update: ControlUpdate,
        *,
        action_id: uuid.UUID,
    ) -> ModelCommandOutcome:
        if update.message_thread_id is None or update.preference_revision is None:
            raise ExecutorPreferenceError("model preference target is incomplete")
        try:
            topic = self._runtime.registries.topics.resolve(
                update.chat_id, update.message_thread_id
            )
        except RegistryError as error:
            raise ExecutorPreferenceError("project topic is not mapped") from error
        if topic.kind is not TopicKind.PROJECT or topic.project_id is None:
            raise ExecutorPreferenceError("/model is only available in a project topic")
        project_id = topic.project_id
        if update.action_kind == "project_model_select_auto":
            view = await set_auto_preference(
                session,
                project_id=project_id,
                user_id=update.user_id,
                expected_revision=update.preference_revision,
            )
            await self._enqueue_confirm(
                session,
                update=update,
                action_id=action_id,
                project_id=project_id,
                label=format_preference_label(view),
            )
            return ModelCommandOutcome(project_id=project_id, stage=ModelPickerStage.CONFIRM)
        if update.action_kind == "project_model_select_connection":
            if update.preference_profile_id is None:
                raise ExecutorPreferenceError("connection selection is missing")
            try:
                profile = self._runtime.registries.profiles.get(update.preference_profile_id)
            except RegistryError as error:
                raise ExecutorPreferenceError("that connection is not available") from error
            if profile not in executor_connections(self._runtime.registries):
                raise ExecutorPreferenceError("that connection is not available")
            preference = await load_preference(session, project_id)
            if preference.revision != update.preference_revision:
                raise ExecutorPreferenceError("model options are stale; send /model again")
            options = workers_for_profile(profile)
            buttons = tuple(
                ((option.label, f"v2:pm:m:{preference.revision}:{profile.id}:{option.key.value}"),)
                for option in options
            )
            session.add(
                TransactionalOutbox(
                    destination="telegram",
                    operation_type="send_message",
                    linked_entity_type="telegram_control_action",
                    linked_entity_id=action_id,
                    idempotency_key=f"telegram:model_models:{update.callback_query_id}",
                    payload={
                        "role": PROJECT_MODEL_PICKER_ROLE,
                        "stage": ModelPickerStage.MODEL.value,
                        "chat_id": update.chat_id,
                        "message_thread_id": update.message_thread_id,
                        "message_id": update.message_id,
                        "project_id": project_id,
                        "revision": preference.revision,
                        "html": build_model_picker_html(
                            project_id=project_id, connection=connection_label(profile)
                        ),
                        "callback_buttons": buttons,
                    },
                )
            )
            return ModelCommandOutcome(project_id=project_id, stage=ModelPickerStage.MODEL)
        if update.action_kind == "project_model_select_worker":
            if update.preference_worker is None:
                raise ExecutorPreferenceError("model selection is incomplete")
            worker = ExecutorWorkerKey(update.preference_worker)
            profile = _selected_profile(
                self._runtime, profile_id=update.preference_profile_id, worker=worker
            )
            option = next(
                (item for item in workers_for_profile(profile) if item.key is worker), None
            )
            if option is None:
                raise ExecutorPreferenceError("that worker is not available")
            if not option.supports_reasoning_effort:
                view = await set_worker_preference(
                    session,
                    project_id=project_id,
                    user_id=update.user_id,
                    expected_revision=update.preference_revision,
                    worker_key=worker,
                    profile_id=profile.id,
                    reasoning_effort=None,
                    registries=self._runtime.registries,
                )
                await self._enqueue_confirm(
                    session,
                    update=update,
                    action_id=action_id,
                    project_id=project_id,
                    label=format_preference_label(view),
                )
                return ModelCommandOutcome(project_id=project_id, stage=ModelPickerStage.CONFIRM)
            preference = await load_preference(session, project_id)
            if preference.revision != update.preference_revision:
                raise ExecutorPreferenceError("model options are stale; send /model again")
            session.add(
                TransactionalOutbox(
                    destination="telegram",
                    operation_type="send_message",
                    linked_entity_type="telegram_control_action",
                    linked_entity_id=action_id,
                    idempotency_key=(
                        f"telegram:model_effort:{update.chat_id}:{update.message_thread_id}:"
                        f"{preference.revision}:{worker.value}:{update.callback_query_id}"
                    ),
                    payload={
                        "role": PROJECT_MODEL_PICKER_ROLE,
                        "stage": ModelPickerStage.EFFORT.value,
                        "chat_id": update.chat_id,
                        "message_thread_id": update.message_thread_id,
                        "message_id": update.message_id,
                        "project_id": project_id,
                        "revision": preference.revision,
                        "worker_key": worker.value,
                        "html": build_effort_picker_html(
                            project_id=project_id, worker_label=option.label
                        ),
                        "callback_buttons": effort_keyboard(
                            revision=preference.revision, worker=worker, profile_id=profile.id
                        ),
                    },
                )
            )
            return ModelCommandOutcome(project_id=project_id, stage=ModelPickerStage.EFFORT)
        if update.action_kind == "project_model_select_effort":
            if update.preference_worker is None or update.preference_effort is None:
                raise ExecutorPreferenceError("model and effort are required")
            worker = ExecutorWorkerKey(update.preference_worker)
            profile = _selected_profile(
                self._runtime, profile_id=update.preference_profile_id, worker=worker
            )
            view = await set_worker_preference(
                session,
                project_id=project_id,
                user_id=update.user_id,
                expected_revision=update.preference_revision,
                worker_key=worker,
                profile_id=profile.id if update.preference_profile_id else None,
                reasoning_effort=update.preference_effort,
                registries=self._runtime.registries,
            )
            await self._enqueue_confirm(
                session,
                update=update,
                action_id=action_id,
                project_id=project_id,
                label=format_preference_label(view),
            )
            return ModelCommandOutcome(project_id=project_id, stage=ModelPickerStage.CONFIRM)
        raise ExecutorPreferenceError("unsupported model preference action")

    async def _enqueue_confirm(
        self,
        session: AsyncSession,
        *,
        update: ControlUpdate,
        action_id: uuid.UUID,
        project_id: str,
        label: str,
    ) -> None:
        assert update.message_thread_id is not None
        session.add(
            TransactionalOutbox(
                destination="telegram",
                operation_type="send_message",
                linked_entity_type="telegram_control_action",
                linked_entity_id=action_id,
                idempotency_key=(
                    f"telegram:model_confirm:{update.chat_id}:{update.message_thread_id}:"
                    f"{project_id}:{update.callback_query_id}"
                ),
                payload={
                    "role": PROJECT_MODEL_CONFIRM_ROLE,
                    "stage": ModelPickerStage.CONFIRM.value,
                    "chat_id": update.chat_id,
                    "message_thread_id": update.message_thread_id,
                    "message_id": update.message_id,
                    "project_id": project_id,
                    "html": build_confirm_html(project_id=project_id, label=label),
                    "callback_buttons": (),
                },
            )
        )
        # Preference mutations must refresh «Статус проектов» so project-default labels
        # are not stale until an unrelated task event.
        await enqueue_project_status_dashboard(session, update.chat_id)
        await _enqueue_project_topic_refresh(
            session,
            project_id=project_id,
            chat_id=update.chat_id,
            thread_id=update.message_thread_id,
            action_id=action_id,
        )


async def _enqueue_project_topic_refresh(
    session: AsyncSession,
    *,
    project_id: str,
    chat_id: int,
    thread_id: int,
    action_id: uuid.UUID,
) -> None:
    from sqlalchemy import select

    from vuzol.storage.models import ProjectDiscussionSession, TopicMapping

    discussion = await session.scalar(
        select(ProjectDiscussionSession)
        .where(
            ProjectDiscussionSession.project_id == project_id,
            ProjectDiscussionSession.chat_id == chat_id,
            ProjectDiscussionSession.message_thread_id == thread_id,
        )
        .order_by(ProjectDiscussionSession.updated_at.desc())
        .limit(1)
    )
    if discussion is not None and discussion.active_work_package_id is not None:
        session.add(
            TransactionalOutbox(
                destination="work_package_projection",
                operation_type="render_status",
                linked_entity_type="work_package",
                linked_entity_id=discussion.active_work_package_id,
                idempotency_key=f"wp:projection:model-change:{action_id}",
                payload={"package_id": str(discussion.active_work_package_id)},
            )
        )
        return
    mapping = await session.scalar(
        select(TopicMapping).where(
            TopicMapping.chat_id == chat_id,
            TopicMapping.message_thread_id == thread_id,
            TopicMapping.project_id == project_id,
            TopicMapping.enabled.is_(True),
        )
    )
    if mapping is not None:
        session.add(
            TransactionalOutbox(
                destination="work_package_projection",
                operation_type="render_topic_idle",
                linked_entity_type="topic_mapping",
                linked_entity_id=mapping.id,
                idempotency_key=f"topic-idle:model-change:{action_id}",
                payload={
                    "chat_id": chat_id,
                    "thread_id": thread_id,
                    "project_id": project_id,
                    "revision": 1,
                },
            )
        )


def _selected_profile(
    runtime: RuntimeConfiguration, *, profile_id: str | None, worker: ExecutorWorkerKey
) -> ProviderProfileConfig:
    if profile_id is not None:
        try:
            return runtime.registries.profiles.get(profile_id)
        except RegistryError as error:
            raise ExecutorPreferenceError("that connection is not available") from error
    provider = {
        ExecutorWorkerKey.GROK: "grok",
        ExecutorWorkerKey.KIMI: "kimi",
    }.get(worker, "codex")
    profile = next(
        (item for item in executor_connections(runtime.registries) if item.provider == provider),
        None,
    )
    if profile is None:
        raise ExecutorPreferenceError("that connection is not available")
    return profile
