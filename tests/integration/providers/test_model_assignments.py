"""PostgreSQL matrix boundaries, with real ingress/routing and fake external I/O.

Uses the shared destructive PostgreSQL fixture: run only in an allocated DB window.
No Telegram delivery, provider inference, Docker daemon, or production configuration.
"""

import asyncio
import hashlib
import json

import httpx
import pytest
from sqlalchemy import func, select
from telegram import Bot, Update

from tests.integration.providers._test_routing_helpers import profile
from tests.integration.storage.helpers import seed_task_run_step, storage
from tests.integration.telegram.helpers import telegram_runtime
from vuzol.config import Capability, ProviderRole, ScopedSecretResolver
from vuzol.config.registries import ProfileRegistry
from vuzol.config.revision import content_revision
from vuzol.execution.domain import GitInspection
from vuzol.projects.model_matrix import MATRIX_KEY, ModelAssignment
from vuzol.providers.catalog import credential_access_policy
from vuzol.providers.health import synchronize_profiles
from vuzol.providers.registry import AdapterRegistry
from vuzol.review.independent import DatabaseReviewAccounting, IndependentModelReviewer
from vuzol.storage.leasing import claim_step, start_step
from vuzol.storage.models import (
    ProjectExecutorPreference,
    ProviderBudgetReservation,
    Run,
    Step,
    Task,
    TelegramControlAction,
    TransactionalOutbox,
    UsageRecord,
)
from vuzol.storage.types import BudgetReservationStatus, RiskLevel, StepStatus
from vuzol.telegram.adapter import control_update
from vuzol.telegram.controls import TelegramControlService
from vuzol.telegram.domain import ControlUpdate, IngressStatus, MessageUpdate
from vuzol.telegram.ingress import TelegramIngressService
from vuzol.workflows.ports import CancellationContext

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
def runtime(tmp_path, monkeypatch):
    runtime = telegram_runtime(tmp_path)
    state = tmp_path / "cli-state"
    state.mkdir()
    seccomp = tmp_path / "seccomp.json"
    seccomp.write_text("{}")
    seccomp.chmod(0o600)
    monkeypatch.setenv("VUZOL_TEST_MATRIX_API_KEY", "matrix-fixture-not-a-credential")
    common = {
        "model": "same-arbitrary-model",
        "roles": frozenset(
            {ProviderRole.LEAD, ProviderRole.PLANNER, ProviderRole.REVIEWER, ProviderRole.EXECUTOR}
        ),
        "capabilities": frozenset({Capability.REPOSITORY_READ}),
        "supported_task_types": frozenset({"coding", "general"}),
        "context_limit": 64_000,
        "output_limit": 4_000,
        "input_cost_units_per_million": 1.0,
        "output_cost_units_per_million": 2.0,
        "routing_priority": 100,
        "fallback_profile_ids": ("api-fallback",),
    }
    profiles = (
        profile("api-choice", credential_reference="env:VUZOL_TEST_MATRIX_API_KEY", **common),
        profile(
            "cli-choice",
            provider="pi",
            api_base_url=None,
            launch_mode="cli",
            runtime_identity="fixture-cli",
            state_directory=state,
            sandbox_required=True,
            **common,
        ),
        profile(
            "api-fallback",
            credential_reference="env:VUZOL_TEST_MATRIX_API_KEY",
            **{**common, "model": "other-model", "routing_priority": 1, "fallback_profile_ids": ()},
        ),
    )
    settings = runtime.settings.model_copy(
        update={
            "worktree_root": tmp_path / "worktrees",
            "execution": runtime.settings.execution.model_copy(
                update={
                    "enabled": True,
                    "require_preflight": False,
                    "sandbox_seccomp_profile": seccomp,
                    "sandbox_seccomp_profile_sha256": hashlib.sha256(b"{}").hexdigest(),
                    "reviewer_profile_id": "api-fallback",
                }
            ),
            "project_lead": runtime.settings.project_lead.model_copy(update={"enabled": False}),
            "dependency_provisioning": runtime.settings.dependency_provisioning.model_copy(
                update={"enabled": False, "environment_root": tmp_path / "dependencies"}
            ),
        }
    )
    return runtime.model_copy(
        update={
            "settings": settings,
            "registries": runtime.registries.model_copy(
                update={"profiles": ProfileRegistry(profiles)}
            ),
        }
    )


@pytest.fixture
def http_boundary(monkeypatch):
    calls = []
    reply = {"text": "Inspect the repository, make the bounded change, and run trusted tests."}

    def respond(request):
        assert request.url.host == "provider.example"
        calls.append(json.loads(request.content))
        return httpx.Response(
            200,
            json={
                "choices": [{"message": {"content": reply["text"]}, "finish_reason": "stop"}],
                "usage": {"prompt_tokens": 20, "completion_tokens": 10},
            },
            headers={"x-request-id": f"matrix-fixture-{len(calls)}"},
        )

    client = httpx.AsyncClient
    monkeypatch.setattr(
        httpx,
        "AsyncClient",
        lambda **kwargs: client(transport=httpx.MockTransport(respond), **kwargs),
    )
    return calls, reply


def _pin(configured):
    return ModelAssignment(
        profile_id=configured.id,
        model=configured.model,
        reasoning_effort=configured.model_reasoning_effort,
        content_revision=content_revision(configured),
    ).model_dump(mode="json")


def _callback(data, number, *, user_id=42):
    bot = Bot("123:FAKE_TOKEN_NOT_A_CREDENTIAL")
    update = Update.de_json(
        {
            "update_id": number,
            "callback_query": {
                "id": f"matrix-pick-{number}",
                "chat_instance": "fixture-chat",
                "from": {"id": user_id, "is_bot": False, "first_name": "Owner"},
                "data": data,
                "message": {
                    "message_id": 50,
                    "message_thread_id": 10,
                    "date": 1792000000,
                    "chat": {"id": -100, "type": "supergroup", "title": "Fixture"},
                    "text": "model picker",
                },
            },
        },
        bot,
    )
    parsed = control_update(update, "matrix-test")
    assert isinstance(parsed, ControlUpdate)
    return parsed


async def _latest_picker(factory):
    async with factory() as session:
        outbox = await session.scalar(
            select(TransactionalOutbox)
            .where(TransactionalOutbox.payload["role"].astext == "project_model_picker")
            .order_by(TransactionalOutbox.created_at.desc())
            .limit(1)
        )
        assert outbox is not None
        return [button for row in outbox.payload["callback_buttons"] for button in row]


async def test_authorized_model_ingress_cas_and_duplicate_callbacks(postgres_dsn, runtime):
    engine, factory = storage(postgres_dsn)
    try:
        ingress = TelegramIngressService(runtime, factory)
        message = MessageUpdate(
            bot_id="matrix-test",
            update_id=1,
            chat_id=-100,
            message_thread_id=10,
            message_id=1,
            user_id=42,
            text="/model",
        )
        assert (await ingress.accept_message(message)).status is IngressStatus.HANDLED
        assert (await ingress.accept_message(message)).status is IngressStatus.DUPLICATE
        menu = await _latest_picker(factory)
        controls = TelegramControlService(runtime, factory)
        slot = next(data for _, data in menu if data.endswith(":planner_light"))
        assert (await controls.accept(_callback(slot, 2))).status is IngressStatus.CREATED
        choices = await _latest_picker(factory)
        assert all(len(data.encode()) <= 64 for _, data in choices)
        picks = [
            _callback(next(data for label, data in choices if name in label), number)
            for name, number in (("api-choice", 3), ("cli-choice", 4))
        ]
        unauthorized = picks[0].model_copy(update={"update_id": 5, "user_id": 999})
        assert (await controls.accept(unauthorized)).status is IngressStatus.REJECTED
        results = await asyncio.gather(*(controls.accept(pick) for pick in picks))
        assert sorted(result.status.value for result in results) == ["created", "rejected"]
        winner = next(
            index for index, result in enumerate(results) if result.status is IngressStatus.CREATED
        )
        # Both update-level replay and a new update carrying the same callback ID are deduplicated.
        assert (await controls.accept(picks[winner])).status is IngressStatus.DUPLICATE
        replay = picks[winner].model_copy(update={"update_id": 6})
        assert (await controls.accept(replay)).status is IngressStatus.DUPLICATE
        async with factory() as session:
            preference = await session.get(ProjectExecutorPreference, "vuzol")
            chosen = runtime.registries.profiles.get(("api-choice", "cli-choice")[winner])
            assert preference.revision == 2 and preference.updated_by_user_id == 42
            assert preference.model_assignments == {"planner_light": _pin(chosen)}
            assert (
                await session.scalar(select(func.count()).select_from(TelegramControlAction)) == 2
            )
            assert await session.scalar(select(func.count()).select_from(Task)) == 0
            assert (
                await session.scalar(select(func.count()).select_from(ProviderBudgetReservation))
                == 0
            )
    finally:
        await engine.dispose()


@pytest.mark.parametrize("pin_source", ["run", "first-review"])
async def test_api_reviewer_pin_survives_restart_and_changed_default(
    postgres_dsn,
    runtime,
    http_boundary,
    pin_source,
):
    engine, factory = storage(postgres_dsn)
    try:
        task_record, run_id, step_record = await seed_task_run_step(factory, step_type="review")
        chosen = runtime.registries.profiles.get("api-choice")
        http_boundary[1]["text"] = json.dumps(
            {"verdict": "pass", "summary": "reviewed", "findings": []}
        )
        async with factory.begin() as session:
            await synchronize_profiles(
                session,
                runtime.registries.profiles.items(),
                configuration_revision=runtime.registries.revision,
            )
            run = await session.get(Run, run_id)
            run.selected_route = {
                MATRIX_KEY: {"reviewer_light": _pin(chosen)} if pin_source == "run" else {}
            }
        resolver = ScopedSecretResolver(
            access_policy=credential_access_policy(runtime.registries.profiles.items()),
            secret_file_root=runtime.settings.secret_file_root,
        )
        for attempt in range(2):
            async with factory.begin() as session:
                if attempt:
                    # Fault injection after receipt: requeue without changing the persisted pin.
                    stored = await session.get(Step, step_record.id, with_for_update=True)
                    stored.status = StepStatus.QUEUED
                    stored.lease_owner = stored.lease_expires_at = None
                    await session.flush()
                lease = await claim_step(
                    session,
                    owner=f"reviewer-{attempt}",
                    lease_seconds=60,
                    capabilities=frozenset(),
                    step_types=frozenset({"review"}),
                )
                assert lease is not None and lease.step.id == step_record.id
                await start_step(session, lease)
                task = await session.get(Task, task_record.id)
            settings = runtime.settings.execution.model_copy(
                update={
                    "reviewer_profile_id": "api-fallback"
                    if attempt or pin_source == "run"
                    else chosen.id
                }
            )
            reviewer = IndependentModelReviewer(
                runtime.registries,
                AdapterRegistry(runtime.registries.profiles, resolver, session_factory=factory),
                DatabaseReviewAccounting(factory, runtime.settings.limits),
                profile_id=settings.reviewer_profile_id,
                session_factory=factory,
            )
            result = await reviewer.review(
                task=task,
                risk=RiskLevel.MEDIUM,
                inspection=GitInspection(
                    head="b" * 40,
                    branch="review",
                    changed_files=("src/module.py",),
                    diff=b"+safe\n",
                ),
                base_commit="a" * 40,
                result_commit="b" * 40,
                diff_hash=None,
                gates=[],
                mechanical_findings=(),
                request_ids=(task_record.id, run_id, step_record.id),
                timeout_seconds=30,
                cancellation=CancellationContext(),
                lease=lease,
            )
            assert result.allows_progress
        async with factory() as session:
            step = await session.get(Step, step_record.id)
            assert step.payload["independent_reviewer_pin"] == _pin(chosen)
            usage = (await session.scalars(select(UsageRecord))).all()
            reservations = (await session.scalars(select(ProviderBudgetReservation))).all()
            assert len(usage) == len(reservations) == 2
            assert {row.profile_id for row in usage} == {chosen.id}
            assert all(row.status is BudgetReservationStatus.RECONCILED for row in reservations)
            assert {row.task_id for row in usage} == {task_record.id}
        assert [call["model"] for call in http_boundary[0]] == [chosen.model, chosen.model]
    finally:
        await engine.dispose()
