"""Project-topic ``/model`` command and callback UX for executor preferences."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from enum import StrEnum

from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.config import RegistryError, RuntimeConfiguration
from vuzol.config.models import TopicKind
from vuzol.projects.executor_preference import (
    REASONING_EFFORTS,
    ExecutorPreferenceError,
    ExecutorWorkerKey,
    WorkerOption,
    auto_callback_data,
    available_workers,
    effort_callback_data,
    ensure_preference_row,
    format_preference_label,
    load_preference,
    set_auto_preference,
    set_worker_preference,
    worker_callback_data,
)
from vuzol.storage.models import TransactionalOutbox
from vuzol.telegram.domain import ControlUpdate
from vuzol.telegram.projections import telegram_html

PROJECT_MODEL_PICKER_ROLE = "project_model_picker"
PROJECT_MODEL_CONFIRM_ROLE = "project_model_confirm"


class ModelPickerStage(StrEnum):
    WORKER = "worker"
    EFFORT = "effort"
    CONFIRM = "confirm"


@dataclass(frozen=True, slots=True)
class ModelCommandOutcome:
    project_id: str
    stage: ModelPickerStage


def build_worker_picker_html(*, project_id: str, current_label: str) -> str:
    return (
        f"<b>Model for project</b> <code>{telegram_html(project_id)}</code>\n"
        f"Current: <b>{telegram_html(current_label)}</b>\n\n"
        "Choose the default executor for this project. "
        "The setting applies to future coding tasks until you change it."
    )


def build_effort_picker_html(*, project_id: str, worker_label: str) -> str:
    return (
        f"<b>Reasoning effort</b> for <b>{telegram_html(worker_label)}</b>\n"
        f"Project: <code>{telegram_html(project_id)}</code>\n\n"
        "Higher effort is slower and usually more thorough."
    )


def build_confirm_html(*, project_id: str, label: str) -> str:
    return (
        f"<b>Executor updated</b>\n"
        f"Project: <code>{telegram_html(project_id)}</code>\n"
        f"Worker: <b>{telegram_html(label)}</b>\n\n"
        "New coding tasks in this project will use this selection."
    )


def worker_keyboard(
    *,
    revision: int,
    workers: tuple[WorkerOption, ...],
) -> tuple[tuple[tuple[str, str], ...], ...]:
    rows: list[tuple[tuple[str, str], ...]] = [
        (("Routing (auto)", auto_callback_data(revision)),),
    ]
    current: list[tuple[str, str]] = []
    for option in workers:
        current.append((option.label, worker_callback_data(revision, option.key)))
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
) -> tuple[tuple[tuple[str, str], ...], ...]:
    buttons = [
        (effort, effort_callback_data(revision, worker, effort)) for effort in REASONING_EFFORTS
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
    workers = available_workers(runtime.registries)
    session.add(
        TransactionalOutbox(
            destination="telegram",
            operation_type="send_message",
            linked_entity_type="telegram_inbox",
            linked_entity_id=inbox_id,
            idempotency_key=(
                f"telegram:model_picker:{chat_id}:{message_thread_id}:{preference.revision}:worker"
            ),
            payload={
                "role": PROJECT_MODEL_PICKER_ROLE,
                "stage": ModelPickerStage.WORKER.value,
                "chat_id": chat_id,
                "message_thread_id": message_thread_id,
                "project_id": project_id,
                "revision": preference.revision,
                "html": build_worker_picker_html(
                    project_id=project_id,
                    current_label=format_preference_label(preference),
                ),
                "callback_buttons": worker_keyboard(revision=preference.revision, workers=workers),
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
            self._enqueue_confirm(
                session,
                update=update,
                action_id=action_id,
                project_id=project_id,
                label=format_preference_label(view),
            )
            return ModelCommandOutcome(project_id=project_id, stage=ModelPickerStage.CONFIRM)
        if update.action_kind == "project_model_select_worker":
            if update.preference_worker is None:
                raise ExecutorPreferenceError("worker selection is missing")
            worker = ExecutorWorkerKey(update.preference_worker)
            workers = available_workers(self._runtime.registries)
            option = next((item for item in workers if item.key is worker), None)
            if option is None:
                raise ExecutorPreferenceError("that worker is not available")
            if not option.supports_reasoning_effort:
                view = await set_worker_preference(
                    session,
                    project_id=project_id,
                    user_id=update.user_id,
                    expected_revision=update.preference_revision,
                    worker_key=worker,
                    reasoning_effort=None,
                    registries=self._runtime.registries,
                )
                self._enqueue_confirm(
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
                        "project_id": project_id,
                        "revision": preference.revision,
                        "worker_key": worker.value,
                        "html": build_effort_picker_html(
                            project_id=project_id, worker_label=option.label
                        ),
                        "callback_buttons": effort_keyboard(
                            revision=preference.revision, worker=worker
                        ),
                    },
                )
            )
            return ModelCommandOutcome(project_id=project_id, stage=ModelPickerStage.EFFORT)
        if update.action_kind == "project_model_select_effort":
            if update.preference_worker is None or update.preference_effort is None:
                raise ExecutorPreferenceError("worker and effort are required")
            worker = ExecutorWorkerKey(update.preference_worker)
            view = await set_worker_preference(
                session,
                project_id=project_id,
                user_id=update.user_id,
                expected_revision=update.preference_revision,
                worker_key=worker,
                reasoning_effort=update.preference_effort,
                registries=self._runtime.registries,
            )
            self._enqueue_confirm(
                session,
                update=update,
                action_id=action_id,
                project_id=project_id,
                label=format_preference_label(view),
            )
            return ModelCommandOutcome(project_id=project_id, stage=ModelPickerStage.CONFIRM)
        raise ExecutorPreferenceError("unsupported model preference action")

    def _enqueue_confirm(
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
                    "project_id": project_id,
                    "html": build_confirm_html(project_id=project_id, label=label),
                    "callback_buttons": (),
                },
            )
        )
