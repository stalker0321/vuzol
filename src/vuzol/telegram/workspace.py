"""Idempotent synchronization of configured Telegram forum topics."""

from dataclasses import dataclass
from typing import Protocol

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol.config import TopicRegistry
from vuzol.observability import get_logger
from vuzol.storage.models import TopicMapping
from vuzol.storage.unit_of_work import UnitOfWork
from vuzol.telegram.layout import effective_display_name, topic_wants_pin


class TelegramWorkspaceClient(Protocol):
    async def rename_topic(self, *, chat_id: int, thread_id: int, name: str) -> None: ...

    async def create_topic(self, *, chat_id: int, name: str) -> int: ...

    async def set_topic_pinned(self, *, chat_id: int, thread_id: int, pinned: bool) -> None: ...


class TopicSynchronizationError(RuntimeError):
    """A categorized Bot API failure while updating one configured topic."""


class TopicCreationOutcomeUnknown(RuntimeError):
    """Telegram may have created a topic but no stable thread ID was received."""


class TopicPinUnsupported(TopicSynchronizationError):
    """Forum-topic pin/unpin is not available through the current Bot API surface."""


@dataclass(frozen=True, slots=True)
class WorkspaceSyncResult:
    mapped_topics: int
    named_topics: int
    pinned_topics: int
    failed_topics: int


class TelegramWorkspaceService:
    """Materialize the configured topic registry in PostgreSQL and Telegram."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        topics: TopicRegistry,
    ) -> None:
        self._factory = session_factory
        self._topics = topics
        self._logger = get_logger(__name__)

    async def synchronize(self, client: TelegramWorkspaceClient) -> WorkspaceSyncResult:
        configured = self._topics.items()
        async with UnitOfWork(self._factory) as uow:
            for topic in configured:
                await uow.topics.upsert(
                    TopicMapping(
                        chat_id=topic.chat_id,
                        message_thread_id=topic.message_thread_id,
                        topic_kind=topic.kind.value,
                        project_id=topic.project_id,
                        accepts_new_tasks=topic.accepts_new_tasks,
                        default_workflow=topic.default_workflow,
                        enabled=topic.enabled,
                    )
                )

        named_topics = 0
        pinned_topics = 0
        failed_topics = 0
        for topic in configured:
            if not topic.enabled:
                continue
            name = effective_display_name(topic)
            if name is not None:
                try:
                    await client.rename_topic(
                        chat_id=topic.chat_id,
                        thread_id=topic.message_thread_id,
                        name=name,
                    )
                except TopicSynchronizationError as error:
                    failed_topics += 1
                    self._logger.warning(
                        "Telegram topic name synchronization failed",
                        extra={
                            "event": "telegram.workspace.topic_name_failed",
                            "chat_id": topic.chat_id,
                            "message_thread_id": topic.message_thread_id,
                            "error_type": type(error).__name__,
                        },
                    )
                else:
                    named_topics += 1

            try:
                await client.set_topic_pinned(
                    chat_id=topic.chat_id,
                    thread_id=topic.message_thread_id,
                    pinned=topic_wants_pin(topic),
                )
            except TopicPinUnsupported:
                # Desired pin state is still product policy; Bot API cannot enforce it yet.
                self._logger.info(
                    "Telegram forum topic pin is product policy only until Bot API support",
                    extra={
                        "event": "telegram.workspace.topic_pin_unsupported",
                        "chat_id": topic.chat_id,
                        "message_thread_id": topic.message_thread_id,
                        "pinned": topic_wants_pin(topic),
                    },
                )
            except TopicSynchronizationError as error:
                failed_topics += 1
                self._logger.warning(
                    "Telegram topic pin synchronization failed",
                    extra={
                        "event": "telegram.workspace.topic_pin_failed",
                        "chat_id": topic.chat_id,
                        "message_thread_id": topic.message_thread_id,
                        "pinned": topic_wants_pin(topic),
                        "error_type": type(error).__name__,
                    },
                )
            else:
                pinned_topics += 1

        self._logger.info(
            "Telegram workspace synchronized",
            extra={
                "event": "telegram.workspace.synchronized",
                "mapped_topics": len(configured),
                "named_topics": named_topics,
                "pinned_topics": pinned_topics,
                "failed_topics": failed_topics,
            },
        )
        return WorkspaceSyncResult(
            mapped_topics=len(configured),
            named_topics=named_topics,
            pinned_topics=pinned_topics,
            failed_topics=failed_topics,
        )
