"""The health summary's queries run against the real schema."""

import asyncio
from datetime import UTC, datetime, timedelta

import pytest
from tests.integration.ops.test_outbox_replay import _outbox, _seed
from tests.integration.storage.helpers import storage

from vuzol.ops.health_report import DatabaseFacts, database_facts
from vuzol.storage.models import SubscriptionLimitSnapshotRow
from vuzol.storage.types import DeliveryStatus

pytestmark = pytest.mark.postgresql


def test_an_idle_database_reports_nothing_and_undelivered_messages_are_counted(
    postgres_dsn: str,
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        try:
            async with factory() as session:
                assert await database_facts(session, now=datetime.now(UTC)) == DatabaseFacts()
            await _seed(
                factory,
                _outbox(idempotency_key="health-dead", destination="telegram"),
                _outbox(
                    idempotency_key="health-ambiguous",
                    destination="telegram",
                    status=DeliveryStatus.AMBIGUOUS,
                ),
                _outbox(idempotency_key="health-ok", status=DeliveryStatus.DELIVERED),
            )
            async with factory() as session:
                facts = await database_facts(session, now=datetime.now(UTC))
            assert facts.undelivered == {"telegram": 2}
        finally:
            await engine.dispose()

    asyncio.run(scenario())


def test_fresh_account_snapshots_name_unusable_accounts(postgres_dsn: str) -> None:
    now = datetime.now(UTC)

    def snapshot(profile_id: str, *, ok: bool, detail: str | None = None, **values: object):
        return SubscriptionLimitSnapshotRow(
            profile_id=profile_id,
            company="OpenCode",
            plan_label="Go",
            ok=ok,
            detail=detail,
            observed_at=values.pop("observed_at", now),
            **values,
        )

    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        try:
            async with factory() as session, session.begin():
                session.add_all(
                    [
                        snapshot("lost", ok=False, detail="go_entitlement_missing"),
                        snapshot("no-collector", ok=False, detail="collector_not_supported"),
                        snapshot("healthy", ok=True, weekly_remaining_percent=40),
                        snapshot(
                            "drained",
                            ok=True,
                            weekly_remaining_percent=40,
                            five_hour_remaining_percent=0,
                        ),
                        # A long-retired account keeps its last row; it is not news.
                        snapshot(
                            "retired",
                            ok=False,
                            detail="limits_stale",
                            observed_at=now - timedelta(days=30),
                        ),
                    ]
                )
            async with factory() as session:
                facts = await database_facts(session, now=now)
            assert facts.accounts == (("drained", "exhausted"), ("lost", "go_entitlement_missing"))
        finally:
            await engine.dispose()

    asyncio.run(scenario())
