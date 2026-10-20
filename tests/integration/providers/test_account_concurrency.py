"""Real PostgreSQL account admission; all provider executions are test doubles."""

import asyncio
import uuid
from pathlib import Path

import pytest
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncConnection

from tests.unit.providers._test_providers_helpers import profile, provider_request
from tests.unit.providers.test_accounts import Adapter, wait_for
from vuzol.config import ProfileRegistry, ScopedSecretResolver
from vuzol.providers import accounts
from vuzol.providers.account_persistence import (
    ProviderAccountLease,
    account_slot_key,
    release_account_ticket_after_cleanup,
)
from vuzol.providers.domain import ProviderErrorCategory
from vuzol.providers.errors import ProviderFailure
from vuzol.providers.registry import AdapterRegistry
from vuzol.workflows.ports import CancellationContext

from ..storage.helpers import storage

pytestmark = pytest.mark.postgresql


@pytest.mark.parametrize("limit", [1, 2])
def test_real_account_slots_are_combined_and_committed_before_execution(
    postgres_dsn: str, tmp_path: Path, limit: int
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        other_engine, other_factory = storage(postgres_dsn)
        configured = tuple(
            profile(
                f"profile-{index}",
                account_id="shared",
                credential_reference="env:SHARED_KEY",
                model=f"model-{index}",
                roles=frozenset({"executor" if index % 2 else "reviewer"}),
                concurrency_limit=limit,
            )
            for index in range(limit + 1)
        )
        resolver = ScopedSecretResolver(access_policy={}, secret_file_root=tmp_path, environment={})
        adapters = [Adapter() for _ in configured]
        wrappers = [
            AdapterRegistry(
                ProfileRegistry(configured),
                resolver,
                session_factory=factory if index % 2 else other_factory,
                adapters={item.id: adapter},
            ).get(item.id)
            for index, (item, adapter) in enumerate(zip(configured, adapters, strict=True))
        ]
        tasks = []
        try:
            for index in range(limit):
                tasks.append(
                    asyncio.create_task(
                        wrappers[index].execute(
                            provider_request(), configured[index], CancellationContext()
                        )
                    )
                )
                await wait_for(adapters[index].started)
            async with factory() as session:
                rows = tuple(await session.scalars(select(ProviderAccountLease)))
                assert len(rows) == limit
                assert {row.token for row in rows} == {
                    adapter.executions[0][2].token
                    for adapter in adapters[:limit]
                    if adapter.executions[0][2] is not None
                }
            with pytest.raises(ProviderFailure, match="busy") as error:
                await wrappers[-1].execute(
                    provider_request(), configured[-1], CancellationContext()
                )
            assert not error.value.request_sent and not adapters[-1].executions
            for adapter in adapters:
                adapter.finish.set()
            await asyncio.gather(*tasks)
            await wrappers[-1].execute(provider_request(), configured[-1], CancellationContext())
            async with factory() as session:
                assert (
                    await session.scalar(select(func.count()).select_from(ProviderAccountLease))
                    == 0
                )
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await engine.dispose()
            await other_engine.dispose()

    asyncio.run(scenario())


def test_backend_death_keeps_durable_ticket_without_any_process_row(
    postgres_dsn: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(accounts, "ACCOUNT_LOCK_POLL_SECONDS", 0.01)

    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        configured = profile("first", account_id="shared")
        adapter = Adapter()
        wrapper = AdapterRegistry(
            ProfileRegistry((configured,)),
            ScopedSecretResolver(access_policy={}, secret_file_root=tmp_path, environment={}),
            session_factory=factory,
            adapters={configured.id: adapter},
        ).get(configured.id)
        cancellation = CancellationContext()
        task = asyncio.create_task(wrapper.execute(provider_request(), configured, cancellation))
        try:
            await wait_for(adapter.started)
            execution = adapter.executions[0][2]
            assert execution is not None
            key = account_slot_key("shared", 0)
            async with engine.connect() as connection:
                pid = await connection.scalar(
                    text(
                        "SELECT pid FROM pg_locks WHERE locktype = 'advisory' "
                        "AND classid = :high AND objid = :low AND objsubid = 1 AND granted"
                    ),
                    {"high": (key >> 32) & 0xFFFFFFFF, "low": key & 0xFFFFFFFF},
                )
                assert pid is not None
                assert await connection.scalar(
                    text("SELECT pg_terminate_backend(:pid)"), {"pid": pid}
                )
            with pytest.raises(ProviderFailure, match="lock connection lost"):
                await asyncio.wait_for(task, timeout=5)
            assert cancellation.requested and adapter.cleaned.is_set()
            with pytest.raises(ProviderFailure, match="ticket"):
                await wrapper.execute(provider_request(), configured, CancellationContext())
            assert len(adapter.executions) == 1
            async with factory() as session:
                ticket = await session.get(ProviderAccountLease, ("shared", 0))
                assert ticket is not None and ticket.token == execution.token
            assert not await release_account_ticket_after_cleanup(
                factory,
                account_id="shared",
                slot=0,
                token=uuid.uuid4(),
                account_profile_ids=(configured.id,),
                cleanup_confirmed=True,
            )
            assert await release_account_ticket_after_cleanup(
                factory,
                account_id="shared",
                slot=0,
                token=execution.token,
                account_profile_ids=(configured.id,),
                cleanup_confirmed=True,
            )
        finally:
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            await engine.dispose()

    asyncio.run(scenario())


def test_lost_commit_acknowledgment_never_executes_and_blocks_retry(
    postgres_dsn: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    original_commit = AsyncConnection.commit
    committed = False

    async def uncertain_commit(connection: AsyncConnection) -> None:
        nonlocal committed
        await original_commit(connection)
        if not committed:
            committed = True
            raise ConnectionError("simulated lost COMMIT acknowledgment")

    monkeypatch.setattr(AsyncConnection, "commit", uncertain_commit)

    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        configured = profile("first", account_id="shared")
        adapter = Adapter()
        wrapper = AdapterRegistry(
            ProfileRegistry((configured,)),
            ScopedSecretResolver(access_policy={}, secret_file_root=tmp_path, environment={}),
            session_factory=factory,
            adapters={configured.id: adapter},
        ).get(configured.id)
        try:
            with pytest.raises(ConnectionError, match="COMMIT acknowledgment"):
                await wrapper.execute(provider_request(), configured, CancellationContext())
            assert not adapter.executions
            async with factory() as session:
                assert await session.get(ProviderAccountLease, ("shared", 0)) is not None
            with pytest.raises(ProviderFailure, match="ticket"):
                await wrapper.execute(provider_request(), configured, CancellationContext())
            assert not adapter.executions
        finally:
            await engine.dispose()

    asyncio.run(scenario())


def test_no_send_failure_releases_real_ticket_and_allows_next_call(
    postgres_dsn: str, tmp_path: Path
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        configured = profile("first", account_id="shared")
        adapter = Adapter()
        adapter.failure = ProviderFailure(
            ProviderErrorCategory.QUOTA_EXHAUSTED, retryable=False, request_sent=False
        )
        adapter.finish.set()
        wrapper = AdapterRegistry(
            ProfileRegistry((configured,)),
            ScopedSecretResolver(access_policy={}, secret_file_root=tmp_path, environment={}),
            session_factory=factory,
            adapters={configured.id: adapter},
        ).get(configured.id)
        try:
            with pytest.raises(ProviderFailure):
                await wrapper.execute(provider_request(), configured, CancellationContext())
            async with factory() as session:
                assert await session.get(ProviderAccountLease, ("shared", 0)) is None
            adapter.failure = None
            await wrapper.execute(provider_request(), configured, CancellationContext())
            assert len(adapter.executions) == 2
        finally:
            await engine.dispose()

    asyncio.run(scenario())
