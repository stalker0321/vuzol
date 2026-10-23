"""Control-plane headroom belongs to the real account, not fake account IDs."""

import asyncio

import pytest

from vuzol.config.models import ProviderRole
from vuzol.providers.errors import ProviderFailure, ProviderNotStartedError
from vuzol.workflows.ports import CancellationContext

from ._test_providers_helpers import profile, provider_request
from .test_accounts import Adapter, Database, bound, wait_for


def test_busy_worker_cannot_consume_reserved_lead_slot():
    async def scenario():
        database = Database()
        configured = profile(
            "shared", account_id="real-account", concurrency_limit=2, control_reserved_slots=1
        )
        worker, leader = Adapter(), Adapter()
        first = asyncio.create_task(
            bound(database, worker, configured).execute(
                provider_request(), configured, CancellationContext()
            )
        )
        await wait_for(worker.started)
        with pytest.raises(ProviderFailure, match="busy"):
            await bound(database, Adapter(), configured).execute(
                provider_request(), configured, CancellationContext()
            )
        second = asyncio.create_task(
            bound(database, leader, configured).execute(
                provider_request().model_copy(update={"role": ProviderRole.LEAD}),
                configured,
                CancellationContext(),
            )
        )
        await wait_for(leader.started)
        assert set(database.tickets) == {("real-account", 0), ("real-account", 1)}
        worker.finish.set()
        leader.finish.set()
        await asyncio.gather(first, second)
        assert not database.tickets and not database.locks

    asyncio.run(scenario())


def test_trusted_prelaunch_rejection_releases_admission():
    async def scenario():
        database, adapter = Database(), Adapter()
        configured = profile("shared", account_id="real-account")
        adapter.failure = ProviderNotStartedError("network rejected before runtime")
        adapter.finish.set()
        with pytest.raises(ProviderNotStartedError):
            await bound(database, adapter, configured).execute(
                provider_request(), configured, CancellationContext()
            )
        assert not database.tickets and not database.locks

    asyncio.run(scenario())
