from pathlib import Path
from types import SimpleNamespace
from typing import cast
from unittest.mock import AsyncMock

import pytest
from pytest import MonkeyPatch
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol.config import (
    Capability,
    ProjectConfig,
    RegistryDocument,
    SandboxProfileConfig,
    Settings,
    TopicConfig,
    TopicKind,
    build_bundle,
)
from vuzol.storage.models import TopicMapping
from vuzol.telegram import workspace
from vuzol.telegram.workspace import (
    TelegramWorkspaceService,
    TopicPinUnsupported,
    TopicSynchronizationError,
)


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

    client = SimpleNamespace(
        rename_topic=AsyncMock(),
        set_topic_pinned=AsyncMock(side_effect=TopicPinUnsupported("unsupported")),
    )
    client.rename_topic.side_effect = (None, TopicSynchronizationError("forbidden"))
    monkeypatch.setattr(workspace, "UnitOfWork", UnitOfWork)

    factory = cast(async_sessionmaker[AsyncSession], object())
    result = await TelegramWorkspaceService(factory, topics).synchronize(client)

    assert result.mapped_topics == 3
    # PERSONAL renamed; APPROVALS rename failed. Disabled CHANGELOG skipped.
    assert result.named_topics == 1
    assert result.failed_topics == 1
    assert result.pinned_topics == 0
    assert len(persisted) == 3
    assert persisted[1].topic_kind == "approvals"
    assert client.rename_topic.await_count == 2
    # Enabled topics only: personal + approvals.
    assert client.set_topic_pinned.await_count == 2
    # Product layout forces the canonical approvals name.
    assert client.rename_topic.await_args_list[1].kwargs["name"] == "Апрувы"


@pytest.mark.anyio
async def test_workspace_sync_applies_pin_state_when_supported(monkeypatch: MonkeyPatch) -> None:
    settings = Settings(environment="test")
    topics = build_bundle(
        RegistryDocument(
            projects=(
                ProjectConfig(
                    id="notes",
                    display_name="Notes",
                    repository_path=Path("notes"),
                    default_branch="main",
                    allowed_capabilities=frozenset({Capability.REPOSITORY_READ}),
                    sandbox_profile="project-default",
                    enabled=False,
                ),
            ),
            sandboxes=(
                SandboxProfileConfig(
                    id="project-default",
                    image=f"example/sandbox@sha256:{'0' * 64}",
                    enabled=False,
                ),
            ),
            topics=(
                TopicConfig(
                    chat_id=-100,
                    message_thread_id=1,
                    kind=TopicKind.CHANGELOG,
                    default_workflow="simple_model_task",
                ),
                TopicConfig(
                    chat_id=-100,
                    message_thread_id=2,
                    kind=TopicKind.PROJECT,
                    project_id="notes",
                    display_name="Notes",
                    default_workflow="adaptive_task",
                    pinned=True,
                ),
                TopicConfig(
                    chat_id=-100,
                    message_thread_id=3,
                    kind=TopicKind.SYSTEM,
                    default_workflow="simple_model_task",
                ),
            ),
        ),
        settings,
    ).topics

    class UnitOfWork:
        def __init__(self, _factory: object) -> None:
            self.topics = SimpleNamespace(upsert=AsyncMock())

        async def __aenter__(self) -> "UnitOfWork":
            return self

        async def __aexit__(self, *_args: object) -> None:
            return None

    client = SimpleNamespace(
        rename_topic=AsyncMock(),
        set_topic_pinned=AsyncMock(),
    )
    monkeypatch.setattr(workspace, "UnitOfWork", UnitOfWork)
    factory = cast(async_sessionmaker[AsyncSession], object())
    result = await TelegramWorkspaceService(factory, topics).synchronize(client)
    assert result.pinned_topics == 3
    pin_calls = {
        call.kwargs["thread_id"]: call.kwargs["pinned"]
        for call in client.set_topic_pinned.await_args_list
    }
    assert pin_calls == {1: True, 2: True, 3: False}
    rename_names = [call.kwargs["name"] for call in client.rename_topic.await_args_list]
    assert "История" in rename_names
    assert "Notes" in rename_names
    assert "Система" in rename_names
