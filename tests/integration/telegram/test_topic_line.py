"""The pinned project line follows what runs in the project; equal lines never re-edit."""

import pytest
from sqlalchemy import func, select

from tests.integration.leadership.test_managed_goal_flow import (
    CHAT,
    PROJECT,
    THREAD,
    _enable,
    _factory,
    _session,
)
from vuzol.storage.models import TelegramMessageLink, TopicMapping, TransactionalOutbox
from vuzol.telegram.delivery import DeliveryAction, _prepare_topic_line
from vuzol.telegram.topic_line import line_revision, refresh_topic_lines, topic_line
from vuzol.telegram.work_package_projections import WORK_PACKAGE_STATUS_ROLE

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]


@pytest.fixture
def anyio_backend():
    return "asyncio"


async def queued(session):
    return await session.scalar(
        select(func.count())
        .select_from(TransactionalOutbox)
        .where(TransactionalOutbox.operation_type == "render_topic_line")
    )


async def test_the_line_says_idle_then_working_and_edits_only_on_change(postgres_dsn):
    async with _factory(postgres_dsn) as factory:
        async with factory.begin() as session:
            session.add(
                TopicMapping(
                    chat_id=CHAT,
                    message_thread_id=THREAD,
                    topic_kind="project",
                    project_id=PROJECT,
                    default_workflow="adaptive_task",
                )
            )
        async with factory.begin() as session:
            assert await topic_line(session, PROJECT, {}) == "💤 Свободен"
            assert await refresh_topic_lines(session, {}) == 1
        async with factory.begin() as session:
            assert await refresh_topic_lines(session, {}) == 0  # the same line is on its way
            item = await session.scalar(select(TransactionalOutbox))
            prepared = await _prepare_topic_line(session, item)
            assert prepared.action is DeliveryAction.SEND_STATUS and prepared.pin_after_send
            session.add(
                TelegramMessageLink(
                    chat_id=CHAT,
                    message_thread_id=THREAD,
                    message_id=777,
                    message_role=WORK_PACKAGE_STATUS_ROLE,
                    projection_revision=line_revision("💤 Свободен"),
                )
            )
            item.status = "delivered"
        async with factory.begin() as session:
            assert await refresh_topic_lines(session, {}) == 0  # already shown
        await _enable(factory, await _session(factory))
        async with factory.begin() as session:
            line = await topic_line(session, PROJECT, {})
            assert line in {"🎯 В работе", "⏳ Ждёт тебя"}  # noqa: RUF001
            assert await refresh_topic_lines(session, {}) == 1
            assert await queued(session) == 2
