"""Account failover keeps durable model choices and records the actual provider."""

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from sqlalchemy import select
from tests.integration.leadership.test_lead_surface import _profile, _settings, _Transport
from tests.integration.leadership.test_managed_goal_flow import PROJECT, _enable, _factory, _session

from vuzol.config.models import LaunchMode
from vuzol.config.registries import ProfileRegistry
from vuzol.config.revision import assignment_revision
from vuzol.leadership.composition import build_project_lead_worker
from vuzol.leadership.persistence import LeadershipInference, LeadershipTurn
from vuzol.projects.model_matrix import ModelAssignment
from vuzol.storage.models import (
    ProjectExecutorPreference,
    SubscriptionLimitSnapshotRow,
    UsageRecord,
)

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]


@pytest.fixture
def anyio_backend():
    return "asyncio"


def _account_profile(account):
    return _profile().model_copy(
        update={
            "id": f"pi-opencode-go-{account}-muse",
            "account_id": account,
            "provider": "pi",
            "model": "opencode-go/muse-spark",
            "model_reasoning_effort": "high",
            "launch_mode": LaunchMode.CLI,
            "api_base_url": None,
            "credential_reference": None,
            "credential_required": False,
            "runtime_identity": f"account-{account}",
            "state_directory": Path(f"/var/lib/vuzol/account-{account}"),
            "sandbox_required": True,
            "account_pool": "opencode-go",
        }
    )


@pytest.mark.parametrize(
    ("case", "expected_account"),
    [
        ("entitlement", "a"),
        ("stale", "c"),
        ("collector_failure", "c"),
        ("different_model", "c"),
        ("different_effort", "c"),
        ("different_reasoning_tokens", "c"),
        ("different_reasoning_enabled", "c"),
        ("disabled", "c"),
        ("wrong_role", "c"),
        ("unusable_sibling", "c"),
        ("no_sibling", "c"),
        ("account_variant", "a"),
        ("priority", "b"),
        ("id_order", "a"),
        ("healthy_pin", "c"),
        ("fallback", "a"),
        ("skip_unusable", "a"),
        ("outside_pool", "c"),
        ("unpooled_pin", "c"),
    ],
)
async def test_pinned_lead_account_failover(postgres_dsn, monkeypatch, case, expected_account):
    pinned, sibling = _account_profile("c"), _account_profile("a")
    changes = {
        "different_model": {"model": "another-model"},
        "different_effort": {"model_reasoning_effort": "low"},
        "different_reasoning_tokens": {"max_reasoning_tokens": 100},
        "different_reasoning_enabled": {"reasoning_enabled": False},
        "disabled": {"enabled": False},
        "wrong_role": {"roles": frozenset()},
        # The same model behind a metered API key must not inherit subscription work.
        "outside_pool": {"account_pool": None},
        "unpooled_pin": {},
    }
    sibling = sibling.model_copy(update=changes.get(case, {}))
    if case == "unpooled_pin":
        pinned = pinned.model_copy(update={"account_pool": None})
    profiles = [pinned] if case == "no_sibling" else [pinned, sibling]
    if case in {"priority", "id_order", "skip_unusable"}:
        profiles.insert(
            0,
            _account_profile("b").model_copy(
                update={"routing_priority": 100 if case == "id_order" else 1}
            ),
        )
    observed_profile = pinned
    if case == "account_variant":
        observed_profile = pinned.model_copy(update={"id": "c-other", "model": "other"})
        profiles.append(observed_profile)
    choice = _profile() if case == "fallback" else pinned
    if case == "fallback":
        profiles.append(choice)
    settings = _settings(monkeypatch, choice.id)
    if case == "fallback":
        settings = settings.model_copy(
            update={
                "project_lead": settings.project_lead.model_copy(
                    update={"lead_fallback_after": 0, "lead_fallback_profile_ids": [pinned.id]}
                )
            }
        )
    pin = ModelAssignment(
        profile_id=choice.id,
        model=choice.model,
        reasoning_effort=choice.model_reasoning_effort,
        content_revision=assignment_revision(choice),
    ).model_dump(mode="json")
    selected = []

    class Observe(_Transport):
        async def execute(self, request, profile, cancellation):
            selected.append(profile.id)
            return await super().execute(request, profile, cancellation)

    async with _factory(postgres_dsn) as factory:
        await _enable(factory, await _session(factory))
        async with factory.begin() as session:
            session.add(
                ProjectExecutorPreference(project_id=PROJECT, model_assignments={"lead": pin})
            )
            session.add(
                SubscriptionLimitSnapshotRow(
                    profile_id=observed_profile.id,
                    company="OpenCode",
                    plan_label="Go",
                    ok=case == "healthy_pin",
                    detail="usage endpoint failed"
                    if case == "collector_failure"
                    else "go_entitlement_missing",
                    observed_at=datetime.now(UTC) - timedelta(hours=3 if case == "stale" else 0),
                    payload={"measurement_available": False},
                )
            )
            if case in {"unusable_sibling", "skip_unusable"}:
                session.add(
                    SubscriptionLimitSnapshotRow(
                        profile_id=sibling.id
                        if case == "unusable_sibling"
                        else _account_profile("b").id,
                        company="OpenCode",
                        plan_label="Go",
                        ok=False,
                        detail="auth_expired",
                        observed_at=datetime.now(UTC),
                    )
                )
        worker = build_project_lead_worker(
            factory,
            settings=settings,
            profiles=ProfileRegistry(profiles),
            owner="account-failover",
            adapter=Observe({"commands": []}),
        )
        assert await worker.process_one()
        expected_id = _account_profile(expected_account).id
        assert selected == [expected_id]
        async with factory() as session:
            turn = await session.scalar(select(LeadershipTurn))
            assert turn.profile_id == expected_id
            assert turn.model_assignments == {"lead": pin}
            preference = await session.get(ProjectExecutorPreference, PROJECT)
            assert preference.model_assignments == {"lead": pin}
            assert preference.revision == 1
            assert (await session.scalar(select(UsageRecord))).profile_id == expected_id
            assert (await session.scalar(select(LeadershipInference))).profile_id == expected_id
