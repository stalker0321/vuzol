"""Real CLI composition, adapter registry, HTTP adapter and ledger; fake HTTP only."""

import json

import httpx
import pytest
from pydantic import SecretStr
from sqlalchemy import select
from tests.integration.leadership.test_lead_surface import _profile, _settings, _Transport
from tests.integration.leadership.test_managed_goal_flow import (
    PROJECT,
    _enable,
    _factory,
    _runtime,
    _session,
)

from vuzol.config.registries import ProfileRegistry
from vuzol.leadership.composition import build_project_lead_worker
from vuzol.leadership.persistence import ManagedGoal
from vuzol.storage.models import ProviderBudgetReservation, UsageRecord
from vuzol.storage.types import BudgetReservationStatus

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]


@pytest.fixture
def anyio_backend():
    return "asyncio"


async def test_cli_composes_actual_http_provider_and_settles_its_reservation(
    postgres_dsn, tmp_path, monkeypatch
):
    from vuzol.cli import worker
    from vuzol.config.settings import ProjectLeadSettings

    monkeypatch.delenv("VUZOL_TEST_DATABASE_DSN", raising=False)
    monkeypatch.setenv("VUZOL_UNUSED_LEAD_TOKEN", "fixture-only-token")
    repository = tmp_path / "repositories" / PROJECT
    repository.mkdir(parents=True)
    (tmp_path / "worktrees").mkdir()
    runtime = _runtime(tmp_path, repository, lead=True)
    runtime = runtime.model_copy(
        update={
            "settings": runtime.settings.model_copy(
                update={"project_lead": ProjectLeadSettings(enabled=True, profile_id="lead-local")}
            ),
            "registries": runtime.registries.model_copy(
                update={"profiles": ProfileRegistry((_profile(),))}
            ),
        }
    )
    requests = []

    def respond(request):
        payload = json.loads(request.content)
        assert request.url.path == "/v1/chat/completions"
        assert payload["model"] == "lead-test"
        requests.append(payload)
        return httpx.Response(
            200,
            json={
                "choices": [{"message": {"content": '{"commands":[]}'}, "finish_reason": "stop"}],
                "usage": {"prompt_tokens": 12, "completion_tokens": 4},
            },
            headers={"x-request-id": "fixture-request"},
        )

    original_client = httpx.AsyncClient
    monkeypatch.setattr(
        httpx,
        "AsyncClient",
        lambda **kwargs: original_client(transport=httpx.MockTransport(respond), **kwargs),
    )
    monkeypatch.setattr(worker, "get_runtime_configuration", lambda **kwargs: runtime)
    monkeypatch.setattr(worker, "resolve_database_dsn", lambda settings: SecretStr(postgres_dsn))
    monkeypatch.setattr(worker.signal, "signal", lambda *args: None)

    async def one_iteration(**kwargs):
        assert await kwargs["controls"].process_one()
        assert len(requests) == 1

    monkeypatch.setattr(worker, "run_runtime", one_iteration)
    async with _factory(postgres_dsn) as factory:
        await _enable(factory, await _session(factory))
        await worker.run()
        async with factory() as session:
            usage = await session.scalar(select(UsageRecord))
            reservation = await session.scalar(select(ProviderBudgetReservation))
            assert usage.purpose == "project_lead"
            assert usage.provider_request_id == "fixture-request"
            assert usage.input_tokens == 12
            assert usage.output_tokens == 4
            assert reservation.status is not BudgetReservationStatus.RESERVED
            assert usage.invocation_id == reservation.invocation_id
            goal = await session.scalar(select(ManagedGoal))
            assert goal.waiting_reason == "model_idle"


async def test_budget_refusal_never_reaches_transport(postgres_dsn, monkeypatch):
    transport = _Transport({"commands": []})
    settings = _settings(monkeypatch, "lead-local")
    settings = settings.model_copy(
        update={"limits": settings.limits.model_copy(update={"daily_cost_units": 0})}
    )
    async with _factory(postgres_dsn) as factory:
        await _enable(factory, await _session(factory))
        worker = build_project_lead_worker(
            factory,
            settings=settings,
            profiles=ProfileRegistry((_profile(),)),
            owner="budget-test",
            adapter=transport,
        )
        await worker.process_one()
        assert transport.requests == []
        async with factory() as session:
            goal = await session.scalar(select(ManagedGoal))
            assert goal.waiting_reason == "lead_budget_exceeded"
            assert await session.scalar(select(UsageRecord)) is None


async def test_transport_exception_preserves_unknown_usage_and_parks(postgres_dsn, monkeypatch):
    class Failing(_Transport):
        async def execute(self, request, profile, cancellation):
            raise TimeoutError("fake provider timeout after send")

    async with _factory(postgres_dsn) as factory:
        await _enable(factory, await _session(factory))
        worker = build_project_lead_worker(
            factory,
            settings=_settings(monkeypatch, "lead-local"),
            profiles=ProfileRegistry((_profile(),)),
            owner="error-test",
            adapter=Failing({}),
        )
        await worker.process_one()
        async with factory() as session:
            usage = await session.scalar(select(UsageRecord))
            reservation = await session.scalar(select(ProviderBudgetReservation))
            goal = await session.scalar(select(ManagedGoal))
            assert usage is not None and usage.cost_known is False
            assert usage.cost_units > 0
            assert reservation.status is not BudgetReservationStatus.RESERVED
            assert goal.waiting_reason == "lead_model_unavailable"
            assert goal.wakeup_condition["kind"] == "owner"
