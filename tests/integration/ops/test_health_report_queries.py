"""The health summary's queries run against the real schema."""

import asyncio
from datetime import UTC, datetime

import pytest
from tests.integration.ops.test_outbox_replay import _outbox, _seed
from tests.integration.storage.helpers import storage

from vuzol.ops.health_report import DatabaseFacts, database_facts
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
