"""Real ingress/model selection with deployment-shaped CLI profiles, no live I/O."""

import asyncio

import pytest
from sqlalchemy import func, select

from tests.integration.interpretation.test_conversation_modes import modes_runtime, turn
from tests.integration.storage.helpers import storage
from vuzol.config import RegistryDocument, RuntimeConfiguration, build_bundle
from vuzol.leadership.persistence import ManagedGoal
from vuzol.projects.model_matrix import eligible_profiles, set_assignment
from vuzol.storage.models import Task, TopicMapping, TransactionalOutbox
from vuzol.telegram.ingress import TelegramIngressService

pytestmark = pytest.mark.postgresql


@pytest.mark.parametrize("mode", ["think", "lead"])
@pytest.mark.parametrize("explicit_assignment", [False, True])
def test_cli_conversation_does_not_require_architecture(
    postgres_dsn, tmp_path, mode, explicit_assignment
):
    async def scenario():
        engine, factory = storage(postgres_dsn)
        try:
            original = modes_runtime(tmp_path)
            settings = original.settings.model_copy(
                update={
                    "chat": original.settings.chat.model_copy(update={"profile_id": "other-worker"})
                }
            )
            profiles = tuple(
                p.model_copy(update={"supported_task_types": frozenset({"general"})})
                if p.id == "other-worker"
                else p.model_copy(update={"fallback_profile_ids": ()})
                if p.id == "sol-chat"
                else p
                for p in original.registries.profiles.items()
            )
            document = RegistryDocument(
                projects=original.registries.projects.items(),
                topics=original.registries.topics.items(),
                profiles=profiles,
                sandboxes=original.registries.sandboxes.items(),
            )
            runtime = RuntimeConfiguration(
                settings=settings, registries=build_bundle(document, settings)
            )
            profile_id = "lead-only" if mode == "lead" else "other-worker"
            profile = runtime.registries.profiles.get(profile_id)
            assert "architecture" not in profile.supported_task_types
            assert profile in eligible_profiles(runtime.registries, mode)
            if explicit_assignment:
                async with factory.begin() as session:
                    await set_assignment(
                        session,
                        project_id="vuzol",
                        slot=mode,
                        profile_id=profile_id,
                        expected_revision=1,
                        user_id=42,
                        registries=runtime.registries,
                    )
            ingress = TelegramIngressService(runtime, factory)
            await ingress.accept_message(turn(f"/{mode}"))
            await ingress.accept_message(turn("Discuss the plan; do not execute it.", 1102))
            async with factory() as session:
                mapping = (await session.scalars(select(TopicMapping))).one()
                assert mapping.conversation_mode == mode
                assert mapping.conversation_mode_message_id == 1101
                item = await session.scalar(
                    select(TransactionalOutbox).where(
                        TransactionalOutbox.destination == "project_lead"
                    )
                )
                assert item.payload["mode"] == mode
                assert item.payload["wakeup_kind"] == "owner_message"
                assert "conversation_route" not in item.payload
                assert await session.scalar(select(func.count()).select_from(Task)) == 0
                assert (await session.scalar(select(ManagedGoal))).lifecycle == "draft"
        finally:
            await engine.dispose()

    asyncio.run(scenario())


def test_default_lead_discussion_has_a_route_without_an_explicit_switch(postgres_dsn, tmp_path):
    async def scenario():
        engine, factory = storage(postgres_dsn)
        try:
            runtime = modes_runtime(tmp_path)
            await TelegramIngressService(runtime, factory).accept_message(
                turn("Explain the project state; do not execute anything.")
            )
            async with factory() as session:
                item = await session.scalar(
                    select(TransactionalOutbox).where(
                        TransactionalOutbox.destination == "project_lead"
                    )
                )
                assert item.payload["mode"] == "lead"
                assert item.payload["wakeup_kind"] == "owner_message"
                assert await session.scalar(select(func.count()).select_from(Task)) == 0
        finally:
            await engine.dispose()

    asyncio.run(scenario())
