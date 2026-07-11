"""Durable ingress, outbox, and Telegram projection repositories."""

import uuid
from collections.abc import Mapping
from typing import Any

from sqlalchemy import delete, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.storage.models import (
    ExternalInbox,
    TelegramMessageLink,
    TopicMapping,
    TransactionalOutbox,
)
from vuzol.storage.types import DeliveryStatus, InboxStatus


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
