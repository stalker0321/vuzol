"""Provider entrypoint and owner-facing goal card. No live provider call."""
# ruff: noqa: RUF001

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path

import anyio
import pytest
from sqlalchemy import func, select
from tests.integration.leadership.test_managed_goal_flow import (
    CHAT,
    OWNER,
    PROJECT,
    THREAD,
    _enable,
    _factory,
    _lead_service,
    _package_count,
    _runtime,
    _session,
)

from vuzol.config import Capability, RuntimeConfiguration
from vuzol.config.models import CostClass, LaunchMode, ProviderProfileConfig, ProviderRole
from vuzol.config.registries import ProfileRegistry
from vuzol.config.settings import ProjectLeadSettings, Settings, get_settings
from vuzol.leadership.actor import server_agent_principal
from vuzol.leadership.commands import LeadCommand
from vuzol.leadership.composition import build_project_lead_worker
from vuzol.leadership.persistence import ManagedGoal
from vuzol.providers.domain import (
    EffectiveProfileState,
    NormalizedUsage,
    ProviderRequest,
    ProviderResult,
    ProviderResultStatus,
)
from vuzol.storage.models import Task, UsageRecord
from vuzol.telegram.delivery import TelegramDeliveryService
from vuzol.telegram.domain import IngressStatus, MessageUpdate
from vuzol.telegram.ingress import TelegramIngressService
from vuzol.telegram.projections import FakeTelegramClient
from vuzol.workflows.application import Principal

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


def _profile() -> ProviderProfileConfig:
    return ProviderProfileConfig(
        id="lead-local",
        provider="openai-compatible",
        model="lead-test",
        launch_mode=LaunchMode.API,
        api_base_url="https://example.com/v1",
        credential_reference="env:VUZOL_UNUSED_LEAD_TOKEN",
        capabilities=frozenset({Capability.REPOSITORY_READ}),
        concurrency_limit=1,
        context_limit=4000,
        output_limit=1000,
        cost_class=CostClass.CHEAP,
        roles=frozenset({ProviderRole.LEAD}),
        supported_task_types=frozenset({"project_lead"}),
        sandbox_required=False,
    )


def _settings(monkeypatch: pytest.MonkeyPatch, profile_id: str | None) -> Settings:
    monkeypatch.delenv("VUZOL_TEST_DATABASE_DSN", raising=False)
    get_settings.cache_clear()
    return Settings(
        environment="test",
        project_lead=ProjectLeadSettings(enabled=True, profile_id=profile_id),
    )


class _Transport:
    def __init__(self, payload: Mapping[str, object]) -> None:
        self.payload = payload
        self.requests: list[ProviderRequest] = []

    async def execute(
        self,
        request: ProviderRequest,
        profile: ProviderProfileConfig,
        cancellation: object,
    ) -> ProviderResult:
        del profile, cancellation
        self.requests.append(request)
        return ProviderResult(
            status=ProviderResultStatus.SUCCEEDED,
            structured_output=dict(self.payload),
            usage=NormalizedUsage(input_tokens=2, output_tokens=3, duration_ms=4),
            adapter_version="fake-lead",
        )

    async def health(self, profile: ProviderProfileConfig) -> EffectiveProfileState:
        del profile
        return EffectiveProfileState()


async def test_entrypoint_records_provider_usage_without_a_task(
    postgres_dsn: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    transport = _Transport({"commands": []})
    async with _factory(postgres_dsn) as factory:
        await _enable(factory, await _session(factory))
        worker = build_project_lead_worker(
            factory,
            settings=_settings(monkeypatch, "lead-local"),
            profiles=ProfileRegistry((_profile(),)),
            owner="lead-entrypoint",
            default_branches={PROJECT: "main"},
            adapter=transport,
        )
        assert await worker.process_one()
        assert len(transport.requests) == 1
        request = transport.requests[0]
        assert request.role is ProviderRole.LEAD
        assert request.task_id == request.run_id == request.step_id
        async with factory() as session:
            goal = await session.scalar(select(ManagedGoal))
            row = await session.scalar(
                select(UsageRecord).where(UsageRecord.invocation_id == request.task_id)
            )
            assert goal is not None and goal.waiting_reason == "model_idle"
            assert goal.proposed_plan is None
            assert await session.get(Task, request.task_id) is None
            assert row is not None
            assert row.task_id is None and row.run_id is None and row.step_id is None
            assert row.purpose == "project_lead"
            assert row.cost_known is False
            assert row.cost_units is not None and row.cost_units > 0
            assert row.profile_id == "lead-local"
        assert await _package_count(factory) == 0
    get_settings.cache_clear()


async def test_invalid_provider_response_parks_without_a_plan(
    postgres_dsn: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    transport = _Transport({"commands": [{"name": "accept_criterion", "payload": {}}]})
    async with _factory(postgres_dsn) as factory:
        await _enable(factory, await _session(factory))
        worker = build_project_lead_worker(
            factory,
            settings=_settings(monkeypatch, "lead-local"),
            profiles=ProfileRegistry((_profile(),)),
            owner="lead-invalid",
            default_branches={PROJECT: "main"},
            adapter=transport,
        )
        assert await worker.process_one()
        async with factory() as session:
            goal = await session.scalar(select(ManagedGoal))
            count = await session.scalar(select(func.count()).select_from(UsageRecord))
            assert goal is not None and goal.waiting_reason == "lead_response_invalid"
            assert goal.proposed_plan is None
            assert count == 1
        assert await _package_count(factory) == 0
    get_settings.cache_clear()


async def test_missing_profile_blocks_before_a_provider_call(
    postgres_dsn: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    transport = _Transport({"commands": []})
    async with _factory(postgres_dsn) as factory:
        await _enable(factory, await _session(factory))
        worker = build_project_lead_worker(
            factory,
            settings=_settings(monkeypatch, None),
            profiles=ProfileRegistry(()),
            owner="lead-unselected",
            default_branches={PROJECT: "main"},
            adapter=transport,
        )
        assert await worker.process_one()
        async with factory() as session:
            goal = await session.scalar(select(ManagedGoal))
            assert goal is not None and goal.waiting_reason == "lead_profile_unselected"
            assert goal.proposed_plan is None
        assert transport.requests == []
        assert await _package_count(factory) == 0
    get_settings.cache_clear()


def _message(update_id: int, text: str) -> MessageUpdate:
    return MessageUpdate(
        bot_id="bot",
        update_id=update_id,
        chat_id=CHAT,
        message_thread_id=THREAD,
        message_id=update_id,
        user_id=OWNER,
        text=text,
    )


async def _drain(factory: object) -> str:
    client = FakeTelegramClient()
    delivery = TelegramDeliveryService(
        factory,  # type: ignore[arg-type]
        client,
        owner="delivery-test",
        lease_seconds=30,
        max_attempts=3,
        retry_min_seconds=0,
        retry_max_seconds=1,
    )
    for _ in range(8):
        if not await delivery.deliver_one():
            break
    return "\n".join(text for _, _, text in client.sent)


async def test_feature_off_still_pauses_and_shows_the_goal_card(
    postgres_dsn: str, tmp_path: object, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("VUZOL_TEST_DATABASE_DSN", raising=False)
    get_settings.cache_clear()
    root = Path(str(tmp_path))
    repository = root / "repositories" / PROJECT
    repository.mkdir(parents=True)
    enabled = _runtime(root, repository, lead=True, discussion=False)
    disabled = _runtime(root, repository, lead=False, discussion=False)
    async with _factory(postgres_dsn) as factory:
        opened = await TelegramIngressService(enabled, factory).accept_message(
            _message(41, "/lead ship the marker")
        )
        assert opened.status is IngressStatus.HANDLED
        paused = await TelegramIngressService(disabled, factory).accept_message(
            _message(42, "/lead_pause")
        )
        assert paused.status is IngressStatus.HANDLED
        status = await TelegramIngressService(disabled, factory).accept_message(
            _message(43, "/lead_status")
        )
        assert status.status is IngressStatus.HANDLED
        text = await _drain(factory)
        assert "⏸ <b>НА ПАУЗЕ · Руководитель</b>" in text
        assert "Ты поставил работу на паузу" in text
        assert "/lead_resume" in text
        assert "Готовность пока не подтверждена" in text
        assert "owner_pause" not in text and "Статус: paused" not in text
        async with factory() as session:
            goal = await session.scalar(select(ManagedGoal))
            tasks = await session.scalar(select(func.count()).select_from(Task))
            assert goal is not None and goal.lifecycle == "paused"
            assert tasks == 0
    get_settings.cache_clear()


async def test_owner_question_and_answer_bind_the_goal_revision(
    postgres_dsn: str, tmp_path: object, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("VUZOL_TEST_DATABASE_DSN", raising=False)
    get_settings.cache_clear()
    question = "Нужен ли отдельный вход?"
    answer = "да, в рамках маркера"
    root = Path(str(tmp_path))
    repository = root / "repositories" / PROJECT
    repository.mkdir(parents=True)
    runtime = _runtime(root, repository, lead=True, discussion=False)
    async with _factory(postgres_dsn) as factory:
        opened = await TelegramIngressService(runtime, factory).accept_message(
            _message(51, "/lead ship the marker")
        )
        assert opened.status is IngressStatus.HANDLED
        async with factory() as session:
            goal = await session.scalar(select(ManagedGoal))
            assert goal is not None
            goal_id = goal.id
            version = goal.version
        asked = await _lead_service(factory).apply_agent(
            server_agent_principal(PROJECT),
            "worker",
            LeadCommand(
                name="request_permission",
                project_id=PROJECT,
                goal_id=goal_id,
                expected_version=version,
                idempotency_key=f"ask-{goal_id.hex[:8]}",
                payload={"question": question},
            ),
        )
        assert asked.status == "waiting"
        question_text = await _drain(factory)
        assert f"<b>Вопрос</b>\n{question}" in question_text
        assert f"/lead_answer {goal_id} {version + 1}" in question_text
        stale = await _lead_service(factory).apply_human(
            Principal(OWNER, "telegram"),
            LeadCommand(
                name="answer_permission",
                project_id=PROJECT,
                goal_id=goal_id,
                expected_version=version,
                idempotency_key="answer-stale",
                payload={"answer": answer},
            ),
        )
        assert stale.status == "stale"
        answered = await TelegramIngressService(runtime, factory).accept_message(
            _message(52, f"/lead_answer {goal_id} {version + 1} {answer}")
        )
        assert answered.status is IngressStatus.HANDLED
        card = await _drain(factory)
        assert "Твой ответ сохранён" in card
        assert "В РАБОТЕ · Руководитель" in card
        assert "<b>Результат</b>" in card
        await TelegramIngressService(runtime, factory).accept_message(_message(53, "/lead_details"))
        details = await _drain(factory)
        assert f"<b>Твой последний ответ</b>\n\n<pre><code>{answer}\n</code></pre>" in details
        async with factory() as session:
            stored = await session.get(ManagedGoal, goal_id)
            assert stored is not None
            body = stored.constraints["owner_answer"]
            assert body["question"] == question
            assert body["answer"] == answer
            assert body["scope_revision"] == stored.scope_revision
            assert body["question_version"] == version + 1
            assert stored.wakeup_condition is None
    get_settings.cache_clear()


async def test_paused_answer_keeps_the_question_revision(
    postgres_dsn: str,
) -> None:
    question = "Остановить пакет?"
    async with _factory(postgres_dsn) as factory:
        goal_id = await _enable(factory, await _session(factory))
        async with factory() as session:
            version = (await session.get(ManagedGoal, goal_id)).version
        asked = await _lead_service(factory).apply_agent(
            server_agent_principal(PROJECT),
            "worker",
            LeadCommand(
                name="request_permission",
                project_id=PROJECT,
                goal_id=goal_id,
                expected_version=version,
                idempotency_key="ask-pause",
                payload={"question": question},
            ),
        )
        assert asked.status == "waiting"
        paused = await _lead_service(factory).apply_human(
            Principal(OWNER, "telegram"),
            LeadCommand(
                name="pause_goal",
                project_id=PROJECT,
                goal_id=goal_id,
                expected_version=version + 1,
                idempotency_key="pause-after-question",
                payload={},
            ),
        )
        assert paused.status == "applied"
        async with factory() as session:
            current = (await session.get(ManagedGoal, goal_id)).version
        answered = await _lead_service(factory).apply_human(
            Principal(OWNER, "telegram"),
            LeadCommand(
                name="answer_permission",
                project_id=PROJECT,
                goal_id=goal_id,
                expected_version=current,
                idempotency_key="answer-while-paused",
                payload={"answer": "да", "question_version": version + 1},
            ),
        )
        assert answered.status == "applied"
        async with factory() as session:
            goal = await session.get(ManagedGoal, goal_id)
            assert goal is not None and goal.lifecycle == "paused"
            body = goal.constraints["owner_answer"]
            assert body["question"] == question
            assert body["question_version"] == version + 1
            assert body["scope_revision"] == goal.scope_revision
            assert goal.wakeup_condition is None


async def owner_message(factory, tmp_path, message_id=900, text="Change the next step"):
    """Real Telegram ingress, including inbox and source deduplication."""
    from vuzol.storage.models import TransactionalOutbox

    repository = tmp_path / "repositories" / PROJECT
    repository.mkdir(parents=True, exist_ok=True)
    runtime = _runtime(tmp_path, repository, lead=True, discussion=False)
    ingress = TelegramIngressService(runtime, factory)
    result = await ingress.accept_message(_message(message_id, text))
    assert result.status is IngressStatus.HANDLED
    async with factory() as session:
        return await session.scalar(
            select(TransactionalOutbox).where(
                TransactionalOutbox.idempotency_key == f"lead:message:{CHAT}:{message_id}"
            )
        )


@pytest.mark.parametrize("lifecycle", ["active", "waiting", "paused"])
async def test_owner_turn_reaches_executing_lead_and_preserves_lifecycle(
    postgres_dsn, tmp_path, lifecycle
):
    from tests.integration.leadership.test_lead_review_regressions import StartModel, control
    from tests.integration.leadership.test_managed_goal_flow import _worker

    from vuzol.leadership.invocation_context import current_turn, invocation_current
    from vuzol.leadership.model import LeadCommandDraft, LeadDecision
    from vuzol.leadership.persistence import LeadershipCommandReceipt
    from vuzol.storage.models import ConversationTurn, TransactionalOutbox
    from vuzol.storage.types import ConversationTurnRole

    async with _factory(postgres_dsn) as factory:
        goal_id = await _enable(factory, await _session(factory))
        await _worker(factory, StartModel()).process_one()
        await _worker(factory, StartModel()).process_one()
        if lifecycle == "paused":
            await control(factory, goal_id, "pause_goal")
        elif lifecycle == "waiting":
            async with factory.begin() as session:
                goal = await session.get(ManagedGoal, goal_id)
                goal.lifecycle = "waiting"
                goal.waiting_reason = "wait_for"
                goal.wakeup_condition = {
                    "kind": "package",
                    "package_id": str(goal.current_package_id),
                }
        event = await owner_message(factory, tmp_path)
        seen = []

        class Reply:
            async def decide(self, snapshot):
                seen.append(snapshot.owner_event)
                assert snapshot.lifecycle == lifecycle
                async with factory.begin() as session:
                    assert await invocation_current(session, current_turn.get())
                return LeadDecision(
                    reply="Вижу сообщение. Обсудим изменение.",
                    commands=(
                        ()
                        if lifecycle == "active"
                        else (LeadCommandDraft(name="start_batch", payload={}),)
                    ),
                )

        assert await _worker(factory, Reply()).process_one()
        assert [item["source_event_id"] for item in seen] == [str(event.id)]
        async with factory() as session:
            goal = await session.get(ManagedGoal, goal_id)
            assert goal.lifecycle == lifecycle
            assert await session.scalar(select(func.count()).select_from(Task)) == 1
            turns = (
                await session.scalars(select(ConversationTurn).order_by(ConversationTurn.ordinal))
            ).all()
            assert [turn.role for turn in turns] == [
                ConversationTurnRole.USER,
                ConversationTurnRole.ASSISTANT,
            ]
            assert turns[0].content == "Change the next step"
            assert not await session.scalar(
                select(TransactionalOutbox.id).where(
                    TransactionalOutbox.destination == "discussion_classify"
                )
            )
            assert not await session.scalar(
                select(LeadershipCommandReceipt.id).where(
                    LeadershipCommandReceipt.reason == "goal_already_live"
                )
            )
        assert "Вижу сообщение" in await _drain(factory)


@pytest.mark.parametrize("lifecycle", ["waiting", "paused"])
@pytest.mark.parametrize("crash_at", ["_save_decision", "_apply_if_current"])
async def test_owner_reply_and_paid_inference_survive_restart(
    postgres_dsn, tmp_path, monkeypatch, lifecycle, crash_at
):
    from tests.integration.leadership.test_lead_review_regressions import control
    from tests.integration.leadership.test_managed_goal_flow import _worker

    from vuzol.leadership.model import LeadDecision
    from vuzol.storage.models import ConversationTurn, TransactionalOutbox
    from vuzol.storage.types import ConversationTurnRole

    class Idle:
        async def decide(self, snapshot):
            return LeadDecision()

    async with _factory(postgres_dsn) as factory:
        goal_id = await _enable(factory, await _session(factory))
        await _worker(factory, Idle()).process_one()
        if lifecycle == "paused":
            await control(factory, goal_id, "pause_goal")
        await owner_message(factory, tmp_path)
        transport = _Transport({"commands": [], "reply": "Ответ сохранён"})
        worker = build_project_lead_worker(
            factory,
            settings=_settings(monkeypatch, "lead-local"),
            profiles=ProfileRegistry((_profile(),)),
            owner="reply-restart",
            default_branches={PROJECT: "main"},
            adapter=transport,
        )

        async def crash(*args, **kwargs):
            raise RuntimeError("after-save")

        monkeypatch.setattr(worker, crash_at, crash)
        with pytest.raises(RuntimeError, match="after-save"):
            await worker.process_one()
        # Exception handling released the outbox, while the old goal lease
        # remains live until process death/recovery expires it.
        async with factory.begin() as session:
            goal = await session.get(ManagedGoal, goal_id)
            goal.lease_until = datetime.now(UTC) - timedelta(seconds=1)
        replacement = build_project_lead_worker(
            factory,
            settings=_settings(monkeypatch, "lead-local"),
            profiles=ProfileRegistry((_profile(),)),
            owner="reply-replacement",
            default_branches={PROJECT: "main"},
            adapter=transport,
        )
        assert await replacement.process_one()
        assert len(transport.requests) == 1
        async with factory() as session:
            assert (await session.get(ManagedGoal, goal_id)).lifecycle == lifecycle
            assert (
                await session.scalar(
                    select(func.count())
                    .select_from(ConversationTurn)
                    .where(ConversationTurn.role == ConversationTurnRole.ASSISTANT)
                )
                == 1
            )
            assert (
                await session.scalar(
                    select(func.count())
                    .select_from(TransactionalOutbox)
                    .where(TransactionalOutbox.idempotency_key.like("lead:reply:%"))
                )
                == 1
            )
        assert (await _drain(factory)).count("Ответ сохранён") == 1
        assert "Ответ сохранён" not in await _drain(factory)


async def test_two_owner_events_are_serial_even_across_workers(postgres_dsn, tmp_path):
    import asyncio

    from tests.integration.leadership.test_managed_goal_flow import _worker

    from vuzol.leadership.model import LeadDecision
    from vuzol.storage.models import ConversationTurn
    from vuzol.storage.types import ConversationTurnRole

    class Idle:
        async def decide(self, snapshot):
            return LeadDecision()

    async with _factory(postgres_dsn) as factory:
        await _enable(factory, await _session(factory))
        await _worker(factory, Idle()).process_one()
        await owner_message(factory, tmp_path, 900, "First")
        await owner_message(factory, tmp_path, 901, "Second")
        # Different update id, same Telegram source message must not append a turn.
        runtime = _runtime(
            tmp_path, tmp_path / "repositories" / PROJECT, lead=True, discussion=False
        )
        ingress = TelegramIngressService(runtime, factory)
        for update_id in (900, 1900):
            duplicate = await ingress.accept_message(
                _message(900, "First").model_copy(update={"update_id": update_id})
            )
            assert duplicate.status is IngressStatus.DUPLICATE
        ready, release = asyncio.Event(), asyncio.Event()
        seen = []

        class Reply:
            async def decide(self, snapshot):
                seen.append(snapshot.owner_event["text"])
                if len(seen) == 1:
                    ready.set()
                    await release.wait()
                return LeadDecision(reply=snapshot.owner_event["text"])

        model = Reply()
        first = asyncio.create_task(_worker(factory, model, owner="one").process_one())
        try:
            await asyncio.wait_for(ready.wait(), 5)
            await _worker(factory, model, owner="two").process_one()
            assert seen == ["First"]
        finally:
            release.set()
            await first
        await _worker(factory, model, owner="two").process_one()
        assert seen == ["First", "Second"]
        async with factory() as session:
            assert (
                await session.scalar(
                    select(func.count())
                    .select_from(ConversationTurn)
                    .where(ConversationTurn.role == ConversationTurnRole.USER)
                )
                == 2
            )


async def test_voice_is_transcribed_before_later_owner_text(postgres_dsn, tmp_path):
    from tests.integration.interpretation.test_pipeline import FakeDownloader
    from tests.integration.leadership.test_managed_goal_flow import _worker

    from vuzol.interpretation.adapters import FakeInterpreter, FakeTranscriber
    from vuzol.interpretation.domain import TranscriptionResult
    from vuzol.interpretation.service import InterpretationPipeline
    from vuzol.leadership.model import LeadDecision
    from vuzol.storage.models import ConversationTurn, TransactionalOutbox
    from vuzol.storage.types import ConversationTurnRole
    from vuzol.telegram.domain import AttachmentKind, TelegramAttachment

    class Reply:
        def __init__(self):
            self.seen = []

        async def decide(self, snapshot):
            if snapshot.owner_event is not None:
                self.seen.append(snapshot.owner_event)
                return LeadDecision(reply="Received: " + snapshot.owner_event["text"])
            return LeadDecision()

    repository = tmp_path / "repositories" / PROJECT
    repository.mkdir(parents=True)
    runtime = _runtime(tmp_path, repository, lead=True, discussion=False)
    async with _factory(postgres_dsn) as factory:
        await _enable(factory, await _session(factory))
        model = Reply()
        worker = _worker(factory, model)
        await worker.process_one()
        voice = _message(900, "Voice caption").model_copy(
            update={
                "attachments": (
                    TelegramAttachment(
                        file_id="telegram-file",
                        file_unique_id="voice-unique",
                        kind=AttachmentKind.VOICE,
                        file_size=len(b"voice-content"),
                        media_type="audio/ogg",
                    ),
                )
            }
        )
        ingress = TelegramIngressService(runtime, factory)
        accepted = await ingress.accept_message(voice)
        assert accepted.status is IngressStatus.HANDLED and accepted.task_id is None
        duplicate = await ingress.accept_message(voice.model_copy(update={"update_id": 1900}))
        assert duplicate.status is IngressStatus.DUPLICATE
        await owner_message(factory, tmp_path, 901, "Then update the tests")
        assert await worker.process_one()
        assert model.seen == []  # The earlier voice event is still being transcribed.
        interpreter = FakeInterpreter([])
        transcriber = FakeTranscriber(
            TranscriptionResult(
                transcript="Change the scope",
                profile_id="fake-transcriber",
                model="fake-audio",
                duration_ms=3,
            )
        )
        pipeline = InterpretationPipeline(
            runtime,
            factory,
            interpreter=interpreter,
            downloader=FakeDownloader(),
            transcriber=transcriber,
            owner="lead-transcriber",
        )
        assert await pipeline.process_one()
        assert not await pipeline.process_one()
        for _ in range(4):
            await worker.process_one()
        assert [event["text"] for event in model.seen] == [
            "Voice caption\nChange the scope",
            "Then update the tests",
        ]
        transcription = model.seen[0]["transcription"]
        assert transcription["text"] == "Change the scope"
        assert await anyio.Path(transcription["content_uri"]).read_bytes() == b"voice-content"
        assert model.seen[0]["attachments"][0]["file_id"] == "telegram-file"
        assert len(transcriber.requests) == 1 and interpreter.requests == []
        async with factory() as session:
            assert await session.scalar(select(func.count()).select_from(Task)) == 0
            assert (
                await session.scalar(
                    select(func.count())
                    .select_from(ConversationTurn)
                    .where(ConversationTurn.role == ConversationTurnRole.USER)
                )
                == 2
            )
            assert not await session.scalar(
                select(TransactionalOutbox.id).where(
                    TransactionalOutbox.destination.in_(("interpretation", "discussion_classify"))
                )
            )


@pytest.mark.parametrize("case", ["no_live_goal", "think", "other_owner"])
async def test_messages_outside_live_owner_lead_keep_discussion_route(postgres_dsn, tmp_path, case):
    from vuzol.storage.models import TopicMapping, TransactionalOutbox

    repository = tmp_path / "repositories" / PROJECT
    repository.mkdir(parents=True)
    runtime = _runtime(tmp_path, repository, lead=True)
    runtime = RuntimeConfiguration(
        settings=runtime.settings.model_copy(update={"allowed_user_ids": (OWNER, 99)}),
        registries=runtime.registries,
    )
    async with _factory(postgres_dsn) as factory:
        if case != "no_live_goal":
            await _enable(factory, await _session(factory))
        if case == "think":
            async with factory.begin() as session:
                session.add(
                    TopicMapping(
                        chat_id=CHAT,
                        message_thread_id=THREAD,
                        project_id=PROJECT,
                        topic_kind="project",
                        accepts_new_tasks=True,
                        default_workflow="coding_task",
                        enabled=True,
                        conversation_mode="think",
                        conversation_mode_message_id=899,
                    )
                )
        message = _message(900, "Describe the current work")
        if case == "other_owner":
            message = message.model_copy(update={"user_id": 99})
        accepted = await TelegramIngressService(runtime, factory).accept_message(message)
        assert accepted.status is IngressStatus.HANDLED
        async with factory() as session:
            assert (
                await session.scalar(
                    select(TransactionalOutbox.id).where(
                        TransactionalOutbox.destination == "discussion_classify"
                    )
                )
                is not None
            )
            assert not await session.scalar(
                select(TransactionalOutbox.id).where(
                    TransactionalOutbox.idempotency_key == f"lead:message:{CHAT}:900"
                )
            )


async def test_project_messages_from_two_chats_reply_to_their_source_topic(postgres_dsn, tmp_path):
    from tests.integration.leadership.test_managed_goal_flow import _worker

    from vuzol.config.registries import TopicRegistry
    from vuzol.leadership.model import LeadDecision

    class Reply:
        def __init__(self):
            self.seen = []

        async def decide(self, snapshot):
            if snapshot.owner_event is not None:
                self.seen.append(snapshot.owner_event["text"])
                return LeadDecision(reply=snapshot.owner_event["text"])
            return LeadDecision()

    repository = tmp_path / "repositories" / PROJECT
    repository.mkdir(parents=True)
    runtime = _runtime(tmp_path, repository, lead=True, discussion=False)
    second_topic = runtime.registries.topics.resolve(CHAT, THREAD).model_copy(
        update={"chat_id": CHAT - 1, "message_thread_id": THREAD + 1}
    )
    runtime = runtime.model_copy(
        update={
            "settings": runtime.settings.model_copy(update={"allowed_chat_ids": (CHAT, CHAT - 1)}),
            "registries": runtime.registries.model_copy(
                update={
                    "topics": TopicRegistry(
                        (*runtime.registries.topics.items(), second_topic),
                        projects=runtime.registries.projects,
                    ),
                }
            ),
        }
    )
    async with _factory(postgres_dsn) as factory:
        await _enable(factory, await _session(factory))
        model = Reply()
        worker = _worker(factory, model)
        await worker.process_one()
        ingress = TelegramIngressService(runtime, factory)
        await ingress.accept_message(_message(900, "First source"))
        # Message ids from different chats must not reorder project events.
        await ingress.accept_message(
            _message(901, "Second source").model_copy(
                update={
                    "chat_id": CHAT - 1,
                    "message_thread_id": THREAD + 1,
                    "message_id": 2,
                }
            )
        )
        await worker.process_one()
        await worker.process_one()
        assert model.seen == ["First source", "Second source"]
        client = FakeTelegramClient()
        delivery = TelegramDeliveryService(
            factory,
            client,
            owner="source-topic-delivery",
            lease_seconds=30,
            max_attempts=3,
            retry_min_seconds=0,
            retry_max_seconds=1,
        )
        for _ in range(12):
            if not await delivery.deliver_one():
                break
        replies = [
            (chat, thread, text)
            for chat, thread, text in client.sent
            if "First source" in text or "Second source" in text
        ]
        assert [(chat, thread) for chat, thread, _ in replies] == [
            (CHAT, THREAD),
            (CHAT - 1, THREAD + 1),
        ]
