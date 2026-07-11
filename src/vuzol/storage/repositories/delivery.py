"""Durable ingress, outbox, and Telegram projection repositories."""

import uuid
from collections.abc import Mapping
from typing import Any

from sqlalchemy import delete, func, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.storage.models import (
    ExternalInbox,
    TelegramControlAction,
    TelegramIntakeMessage,
    TelegramMessageLink,
    TopicMapping,
    TransactionalOutbox,
)
from vuzol.storage.types import ControlActionStatus, DeliveryStatus, InboxStatus


class InboxRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def receive_once(
        self,
        *,
        source: str,
        consumer: str,
        external_event_id: str,
        payload_hash: str,
    ) -> tuple[uuid.UUID, bool]:
        statement = (
            insert(ExternalInbox)
            .values(
                id=uuid.uuid4(),
                source=source,
                consumer=consumer,
                external_event_id=external_event_id,
                payload_hash=payload_hash,
                status=InboxStatus.RECEIVED,
            )
            .on_conflict_do_nothing(index_elements=["source", "consumer", "external_event_id"])
            .returning(ExternalInbox.id)
        )
        inserted = await self._session.scalar(statement)
        if inserted is not None:
            return inserted, True
        existing = await self._session.scalar(
            select(ExternalInbox.id).where(
                ExternalInbox.source == source,
                ExternalInbox.consumer == consumer,
                ExternalInbox.external_event_id == external_event_id,
            )
        )
        assert existing is not None
        return existing, False

    async def mark_processed(
        self, inbox_id: uuid.UUID, *, entity_type: str | None, entity_id: uuid.UUID | None
    ) -> None:
        await self._session.execute(
            update(ExternalInbox)
            .where(ExternalInbox.id == inbox_id)
            .values(
                status=InboxStatus.PROCESSED,
                processed_at=func.now(),
                linked_entity_type=entity_type,
                linked_entity_id=entity_id,
            )
        )


class OutboxRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def enqueue(
        self,
        *,
        destination: str,
        operation_type: str,
        entity_type: str,
        entity_id: uuid.UUID,
        idempotency_key: str,
        payload: Mapping[str, Any],
    ) -> uuid.UUID:
        item = TransactionalOutbox(
            destination=destination,
            operation_type=operation_type,
            linked_entity_type=entity_type,
            linked_entity_id=entity_id,
            idempotency_key=idempotency_key,
            payload=dict(payload),
            status=DeliveryStatus.PENDING,
        )
        self._session.add(item)
        await self._session.flush()
        return item.id


class TopicMappingRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def add(self, mapping: TopicMapping) -> uuid.UUID:
        self._session.add(mapping)
        await self._session.flush()
        return mapping.id

    async def upsert(self, mapping: TopicMapping) -> uuid.UUID:
        mapping_id = uuid.uuid4()
        statement = (
            insert(TopicMapping)
            .values(
                id=mapping_id,
                chat_id=mapping.chat_id,
                message_thread_id=mapping.message_thread_id,
                topic_kind=mapping.topic_kind,
                project_id=mapping.project_id,
                accepts_new_tasks=mapping.accepts_new_tasks,
                default_workflow=mapping.default_workflow,
                enabled=mapping.enabled,
            )
            .on_conflict_do_update(
                index_elements=["chat_id", "message_thread_id"],
                set_={
                    "topic_kind": mapping.topic_kind,
                    "project_id": mapping.project_id,
                    "accepts_new_tasks": mapping.accepts_new_tasks,
                    "default_workflow": mapping.default_workflow,
                    "enabled": mapping.enabled,
                    "updated_at": func.now(),
                },
            )
            .returning(TopicMapping.id)
        )
        result = await self._session.scalar(statement)
        assert result is not None
        return result


class TelegramMessageLinkRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def add(self, link: TelegramMessageLink) -> uuid.UUID:
        self._session.add(link)
        await self._session.flush()
        return link.id

    async def delete_projection(self, link_id: uuid.UUID) -> None:
        await self._session.execute(
            delete(TelegramMessageLink).where(TelegramMessageLink.id == link_id)
        )

    async def resolve_task(self, chat_id: int, message_id: int) -> uuid.UUID | None:
        return await self._session.scalar(
            select(TelegramMessageLink.task_id).where(
                TelegramMessageLink.chat_id == chat_id,
                TelegramMessageLink.message_id == message_id,
            )
        )


class TelegramIntakeRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def add(self, message: TelegramIntakeMessage) -> uuid.UUID:
        self._session.add(message)
        await self._session.flush()
        return message.id


class TelegramControlActionRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def queue_once(self, action: TelegramControlAction) -> tuple[uuid.UUID, bool]:
        action_id = uuid.uuid4()
        statement = (
            insert(TelegramControlAction)
            .values(
                id=action_id,
                external_action_id=action.external_action_id,
                action_kind=action.action_kind,
                requested_by_user_id=action.requested_by_user_id,
                task_id=action.task_id,
                step_id=action.step_id,
                approval_id=action.approval_id,
                payload=action.payload,
                status=ControlActionStatus.QUEUED,
            )
            .on_conflict_do_nothing(index_elements=["external_action_id"])
            .returning(TelegramControlAction.id)
        )
        inserted = await self._session.scalar(statement)
        if inserted is not None:
            return inserted, True
        existing = await self._session.scalar(
            select(TelegramControlAction.id).where(
                TelegramControlAction.external_action_id == action.external_action_id
            )
        )
        assert existing is not None
        return existing, False
