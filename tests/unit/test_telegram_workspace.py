from types import SimpleNamespace
from typing import cast
from unittest.mock import AsyncMock

import pytest
from pytest import MonkeyPatch
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol.config import RegistryDocument, Settings, TopicConfig, TopicKind, build_bundle
from vuzol.storage.models import TopicMapping
from vuzol.telegram import workspace
from vuzol.telegram.workspace import TelegramWorkspaceService, TopicSynchronizationError


@pytest.mark.anyio
async def test_workspace_sync_persists_all_topics_and_renames_best_effort(
    monkeypatch: MonkeyPatch,
) -> None:
    topics = build_bundle(
        RegistryDocument(
            topics=(
                TopicConfig(
                    chat_id=-100,
                    message_thread_id=1,
                    kind=TopicKind.PERSONAL,
                    display_name="Личное",
                    default_workflow="simple_model_task",
                ),
                TopicConfig(
                    chat_id=-100,
                    message_thread_id=2,
                    kind=TopicKind.APPROVALS,
                    display_name="Vuzol",
                    default_workflow="simple_model_task",
                ),
                TopicConfig(
                    chat_id=-100,
                    message_thread_id=3,
                    kind=TopicKind.CHANGELOG,
                    display_name="Disabled",
                    default_workflow="simple_model_task",
                    enabled=False,
                ),
            )
        ),
        Settings(environment="test"),
    ).topics
    persisted: list[TopicMapping] = []

    class UnitOfWork:
        def __init__(self, _factory: object) -> None:
            self.topics = SimpleNamespace(upsert=AsyncMock(side_effect=persisted.append))

        async def __aenter__(self) -> "UnitOfWork":
            return self

        async def __aexit__(self, *_args: object) -> None:
            return None

    client = SimpleNamespace(rename_topic=AsyncMock())
    client.rename_topic.side_effect = (None, TopicSynchronizationError("forbidden"))
    monkeypatch.setattr(workspace, "UnitOfWork", UnitOfWork)

    factory = cast(async_sessionmaker[AsyncSession], object())
    result = await TelegramWorkspaceService(factory, topics).synchronize(client)

    assert result.mapped_topics == 3
    assert result.named_topics == 1
    assert result.failed_topics == 1
    assert len(persisted) == 3
    assert persisted[1].topic_kind == "approvals"
    assert client.rename_topic.await_count == 2
