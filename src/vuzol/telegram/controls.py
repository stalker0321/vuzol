"""Persisted idempotent Telegram callback handling."""

import hashlib

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol.config import RuntimeConfiguration
from vuzol.storage.errors import EntityNotFound
from vuzol.storage.models import TelegramControlAction
from vuzol.storage.unit_of_work import UnitOfWork
from vuzol.telegram.domain import ControlUpdate, IngressResult, IngressStatus
from vuzol.telegram.policy import TelegramPolicyError, authorize


class TelegramControlService:
    def __init__(
        self,
        runtime: RuntimeConfiguration,
        session_factory: async_sessionmaker[AsyncSession],
    ) -> None:
        self._runtime = runtime
        self._session_factory = session_factory

    async def accept(self, update: ControlUpdate) -> IngressResult:
        try:
            authorize(
                self._runtime.settings,
                chat_id=update.chat_id,
                user_id=update.user_id,
            )
            if update.task_id is None and update.approval_id is None:
                raise TelegramPolicyError("control action requires a persisted target")
            if update.action_kind in {"approve", "reject"} and update.approval_id is None:
                raise TelegramPolicyError("approval action requires approval_id")
        except TelegramPolicyError as error:
            return IngressResult(status=IngressStatus.REJECTED, reason=str(error))

        payload_hash = hashlib.sha256(update.model_dump_json().encode()).hexdigest()
        try:
            async with UnitOfWork(self._session_factory) as uow:
                inbox_id, update_created = await uow.inbox.receive_once(
                    source="telegram_callback",
                    consumer=f"bot:{update.bot_id}",
                    external_event_id=str(update.update_id),
                    payload_hash=payload_hash,
                )
                if not update_created:
                    return IngressResult(status=IngressStatus.DUPLICATE)
                if update.task_id is not None:
                    await uow.tasks.get(update.task_id, for_update=True)
                action = TelegramControlAction(
                    external_action_id=update.callback_query_id,
                    action_kind=update.action_kind,
                    requested_by_user_id=update.user_id,
                    task_id=update.task_id,
                    step_id=update.step_id,
                    approval_id=update.approval_id,
                    payload={},
                )
                action_id, action_created = await uow.telegram_actions.queue_once(action)
                if action_created:
                    await uow.outbox.enqueue(
                        destination="workflow_control",
                        operation_type=update.action_kind,
                        entity_type="telegram_control_action",
                        entity_id=action_id,
                        idempotency_key=f"telegram:control:{update.callback_query_id}",
                        payload=update.model_dump(mode="json"),
                    )
                await uow.inbox.mark_processed(
                    inbox_id,
                    entity_type="telegram_control_action",
                    entity_id=action_id,
                )
        except EntityNotFound as error:
            return IngressResult(status=IngressStatus.REJECTED, reason=str(error))

        return IngressResult(
            status=IngressStatus.CREATED if action_created else IngressStatus.DUPLICATE,
            action_id=action_id,
        )
