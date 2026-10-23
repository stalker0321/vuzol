"""Real executor refresh + PostgreSQL + dashboard; only external HTTP is fake."""

import asyncio
import uuid
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
from sqlalchemy import select

from tests.integration.storage.helpers import storage
from tests.unit.config.test_account_descriptors import document
from tests.unit.providers.test_opencode_oauth import oauth_account, oauth_document, oauth_payload
from tests.unit.providers.test_opencode_usage import account, payload
from vuzol.cli.executor import _refresh_subscription_limits_tick
from vuzol.config import SubscriptionLimitSettings
from vuzol.config.loader import load_document
from vuzol.config.registries import ProfileRegistry
from vuzol.config.settings import OpenCodeSnapshotBinding
from vuzol.providers.account_persistence import ProviderAccountLease
from vuzol.providers.opencode_snapshot import (
    UsageSnapshot,
    execution_binding,
    publish_usage_snapshot,
)
from vuzol.providers.opencode_usage import OpenCodeUsageCollector, parse_oauth_go_usage
from vuzol.providers.subscription_limits import (
    LimitWindow,
    SubscriptionLimitSnapshot,
    load_subscription_limits,
    persist_subscription_limits,
)
from vuzol.storage.models import TransactionalOutbox
from vuzol.storage.repositories.delivery import OutboxRepository
from vuzol.storage.types import DeliveryStatus
from vuzol.telegram.projections import build_project_status_dashboard

pytestmark = pytest.mark.postgresql


@pytest.mark.parametrize("auth", ["static", "oauth", "snapshot"])
def test_executor_update_roundtrip_restart_dashboard_and_no_model_slots(
    postgres_dsn: str, tmp_path: Path, monkeypatch, auth: str
) -> None:
    async def scenario() -> None:
        now = datetime.now(UTC)
        if auth in {"oauth", "snapshot"}:
            binding = oauth_account(tmp_path, now=now)
            profiles = load_document(oauth_document(tmp_path, binding)).profiles
        else:
            account(tmp_path)
            profiles = load_document(document(tmp_path)).profiles
        registry = ProfileRegistry(profiles)
        calls = []
        settings = SubscriptionLimitSettings()
        if auth == "snapshot":
            path = tmp_path / "usage.json"
            await asyncio.to_thread(tmp_path.chmod, 0o700)
            publish_usage_snapshot(
                path,
                UsageSnapshot(
                    account_id=binding.id,
                    binding_digest=execution_binding(profiles[0]),
                    scope_id=binding.quota_scope_id,
                    generated_at=now,
                    attempted_at=now,
                    observed_at=now,
                    windows=parse_oauth_go_usage(
                        oauth_payload(now), observed_at=now, scope_id=binding.quota_scope_id
                    ),
                ),
            )
            settings = SubscriptionLimitSettings(
                opencode_snapshots=(
                    OpenCodeSnapshotBinding(
                        account_id=binding.id,
                        scope_id=binding.quota_scope_id,
                        path=path,
                    ),
                )
            )

        def transport(request):
            assert auth != "snapshot", "executor must not call provider for an owner snapshot"
            calls.append(request)
            if auth == "oauth":
                assert request.url.path == "/inference/go/v1/usage"
                assert request.headers["x-opencode-org-id"] == "org-c"
                assert request.headers["authorization"] == "Bearer synthetic-access-c"
            return httpx.Response(200, json=oauth_payload(now) if auth == "oauth" else payload())

        collector = OpenCodeUsageCollector(
            client_factory=lambda: httpx.Client(transport=httpx.MockTransport(transport))
        )
        monkeypatch.setattr("vuzol.providers.opencode_usage.go_usage_collector", collector)
        engine, factory = storage(postgres_dsn)
        try:
            async with factory.begin() as session:
                request_id = await OutboxRepository(session).enqueue(
                    destination="subscription_limits",
                    operation_type="refresh",
                    entity_type="dashboard",
                    entity_id=uuid.uuid4(),
                    idempotency_key="go-test-update",
                    payload={"chat_id": -100},
                )
                # Old Codex/Grok integer columns must still load normally.
                await persist_subscription_limits(
                    session,
                    (
                        SubscriptionLimitSnapshot(
                            "legacy-codex",
                            "OpenAI",
                            "Pro",
                            LimitWindow(37, None),
                            LimitWindow(62, None),
                            datetime.now(UTC),
                            True,
                        ),
                    ),
                )
            claimed = await _refresh_subscription_limits_tick(
                factory,
                SimpleNamespace(profiles=registry),
                due_periodic=False,
                settings=settings,
            )
            assert claimed
            async with factory() as session:
                outbox = await session.get(TransactionalOutbox, request_id)
                assert outbox.status == DeliveryStatus.DELIVERED
                assert list(await session.scalars(select(ProviderAccountLease))) == []
                before = await load_subscription_limits(session)
            assert len(calls) == (0 if auth == "snapshot" else 1)
        finally:
            await engine.dispose()
        # Fresh database composition, not an OS restart or a second provider request.
        engine, factory = storage(postgres_dsn)
        try:
            async with factory() as session:
                after = await load_subscription_limits(session)
                assert before == after
                actual = next(s for s in after if s.source)
                assert [w.remaining_percent for w in actual.windows] == (
                    [87.5, 0, 100] if auth == "static" else [86.3, 0, 38.8]
                )
                if auth in {"oauth", "snapshot"}:
                    assert actual.source == "opencode_go_oauth_v1"
                    assert actual.quota_scope_id == binding.quota_scope_id
                    assert all(w.scope_id == binding.quota_scope_id for w in actual.windows)
                assert len({w.reset_at for w in actual.windows}) == 3
                assert actual.plan and actual.plan.tier_id == "go"
                assert actual.plan.source == "opencode_go_usage"
                legacy = next(s for s in after if not s.source)
                assert legacy.weekly.remaining_percent == 62
                card = await build_project_status_dashboard(
                    session, chat_id=-100, subscription_profiles=profiles
                )
                assert (
                    "1mo" in card.html and ("87.5%" if auth == "static" else "86.3%") in card.html
                )
                assert "OpenCode · Go" in card.html and "UTC" not in card.html
                assert "offline-fixture-only" not in card.html
                assert "synthetic-access" not in card.html and "refresh-token" not in card.html
        finally:
            await engine.dispose()

    asyncio.run(scenario())


def test_failed_attempt_has_no_measurement_after_database_reload(
    postgres_dsn: str, tmp_path: Path, monkeypatch
) -> None:
    async def scenario() -> None:
        account(tmp_path)
        profiles = load_document(document(tmp_path)).profiles
        monkeypatch.setattr(
            "vuzol.providers.opencode_usage.go_usage_collector",
            OpenCodeUsageCollector(
                client_factory=lambda: httpx.Client(
                    transport=httpx.MockTransport(lambda r: httpx.Response(401))
                )
            ),
        )
        engine, factory = storage(postgres_dsn)
        try:
            assert not await _refresh_subscription_limits_tick(
                factory,
                SimpleNamespace(profiles=ProfileRegistry(profiles)),
                due_periodic=True,
                settings=SubscriptionLimitSettings(),
            )
            async with factory() as session:
                snapshots = await load_subscription_limits(session)
                assert len(snapshots) == 1 and snapshots[0].observed_at is None
                assert snapshots[0].attempted_at is not None
                assert snapshots[0].plan is None
                card = await build_project_status_dashboard(
                    session, chat_id=-100, subscription_profiles=profiles
                )
                assert "повторный вход" in card.html and "100%" not in card.html
        finally:
            await engine.dispose()

    asyncio.run(scenario())


def test_codex_tariff_upgrade_real_refresh_persist_reload_dashboard(
    postgres_dsn: str, tmp_path: Path, monkeypatch
) -> None:
    import io
    import json

    from tests.unit.providers._test_subscription_limits_helpers import _cli_profile

    profile = _cli_profile("codex-plan-test", "codex", tmp_path)
    (tmp_path / "auth.json").write_text(json.dumps({"tokens": {"access_token": "offline-only"}}))
    tier = "plus"
    calls = []

    def transport(request, **kwargs):
        assert request.full_url == "https://chatgpt.com/backend-api/wham/usage"
        assert request.get_method() == "GET"
        calls.append(request)
        return io.BytesIO(json.dumps({"plan_type": tier, "rate_limit": {}}).encode())

    monkeypatch.setattr("vuzol.providers.subscription_limits.urllib.request.urlopen", transport)

    async def scenario() -> None:
        nonlocal tier
        cards = []
        engine, factory = storage(postgres_dsn)
        try:
            for tier, expected in (("plus", "Plus"), ("pro", "Pro")):
                await _refresh_subscription_limits_tick(
                    factory,
                    SimpleNamespace(profiles=ProfileRegistry([profile])),
                    due_periodic=True,
                    settings=SubscriptionLimitSettings(),
                )
                async with factory() as session:
                    snapshots = await load_subscription_limits(session)
                    assert len(snapshots) == 1
                    assert snapshots[0].plan and snapshots[0].plan.tier_id == tier
                    assert snapshots[0].plan.source == "codex_usage"
                    card = await build_project_status_dashboard(
                        session, chat_id=-100, subscription_profiles=[profile]
                    )
                    assert "OpenAI / Codex · " + expected in card.html
                    assert "offline-only" not in card.html
                    assert list(await session.scalars(select(ProviderAccountLease))) == []
                    cards.append(card)
            assert len(calls) == 2 and cards[0].revision != cards[1].revision
        finally:
            await engine.dispose()

    asyncio.run(scenario())
