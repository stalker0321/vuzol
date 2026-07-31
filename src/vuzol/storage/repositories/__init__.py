"""Explicit persistence repository interfaces."""

from vuzol.storage.repositories.core import (
    EventRepository,
    RunRepository,
    StepRepository,
    TaskRepository,
)
from vuzol.storage.repositories.delivery import (
    InboxRepository,
    OutboxRepository,
    TelegramControlActionRepository,
    TelegramIntakeRepository,
    TelegramMessageLinkRepository,
    TopicMappingRepository,
)
from vuzol.storage.repositories.discussion import DiscussionRepository
from vuzol.storage.repositories.evidence import ApprovalRepository, ModelRepository
from vuzol.storage.repositories.work_packages import WorkPackageRepository

__all__ = [
    "ApprovalRepository",
    "DiscussionRepository",
    "EventRepository",
    "InboxRepository",
    "ModelRepository",
    "OutboxRepository",
    "RunRepository",
    "StepRepository",
    "TaskRepository",
    "TelegramControlActionRepository",
    "TelegramIntakeRepository",
    "TelegramMessageLinkRepository",
    "TopicMappingRepository",
    "WorkPackageRepository",
]
