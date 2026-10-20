"""Provider entrypoint and owner-facing goal card. No live provider call."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

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

from vuzol.config import Capability
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
        assert "Статус: paused" in text
        assert "Блокер: owner_pause" in text
        assert "Пакет: нет" in text
        assert "Дальше: пауза" in text
        assert "Результат: доказательств нет" in text
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
        assert f"Вопрос: {question}" in question_text
        assert "Версия:" in question_text
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
        assert f"Ответ: {answer}" in card
        assert "Статус:" in card
        assert "Результат:" in card
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
