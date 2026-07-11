"""Explicit async transaction ownership."""

from types import TracebackType

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol.storage.repositories import (
    ApprovalRepository,
    EventRepository,
    InboxRepository,
    ModelRepository,
    OutboxRepository,
    RunRepository,
    StepRepository,
    TaskRepository,
    TelegramMessageLinkRepository,
    TopicMappingRepository,
)


class UnitOfWork:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory
        self.session: AsyncSession | None = None
        self.tasks: TaskRepository
        self.runs: RunRepository
        self.steps: StepRepository
        self.events: EventRepository
        self.inbox: InboxRepository
        self.outbox: OutboxRepository
        self.approvals: ApprovalRepository
        self.evidence: ModelRepository
        self.topics: TopicMappingRepository
        self.telegram_links: TelegramMessageLinkRepository

    async def __aenter__(self) -> "UnitOfWork":
        self.session = self._session_factory()
        await self.session.begin()
        self.tasks = TaskRepository(self.session)
        self.runs = RunRepository(self.session)
        self.steps = StepRepository(self.session)
        self.events = EventRepository(self.session)
        self.inbox = InboxRepository(self.session)
        self.outbox = OutboxRepository(self.session)
        self.approvals = ApprovalRepository(self.session)
        self.evidence = ModelRepository(self.session)
        self.topics = TopicMappingRepository(self.session)
        self.telegram_links = TelegramMessageLinkRepository(self.session)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        assert self.session is not None
        try:
            if exc_type is None:
                await self.session.commit()
            else:
                await self.session.rollback()
        finally:
            await self.session.close()
