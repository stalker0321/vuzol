"""Unit-only account concurrency and connection-lifecycle fault injection."""

from __future__ import annotations

import asyncio
import uuid
from pathlib import Path
from typing import Any, cast

import pytest
from sqlalchemy import Select
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.sql.dml import Delete, Insert
from sqlalchemy.sql.elements import TextClause

from vuzol.config import ProfileRegistry, ProviderProfileConfig, ScopedSecretResolver
from vuzol.providers import accounts
from vuzol.providers.accounts import (
    AccountBoundAdapter,
    AccountExecution,
    account_slot_key,
    current_account_execution,
)
from vuzol.providers.domain import (
    EffectiveProfileState,
    NormalizedUsage,
    ProviderRequest,
    ProviderResult,
    ProviderResultStatus,
)
from vuzol.providers.errors import ProviderFailure
from vuzol.providers.registry import AdapterRegistry
from vuzol.workflows.ports import CancellationContext

from ._test_providers_helpers import codex_profile, profile, provider_request


class Database:
    """Separate connections sharing server-like session locks; no real DB."""

    def __init__(self) -> None:
        self.locks: dict[int, Connection] = {}
        self.connections: list[Connection] = []
        self.records: list[dict[str, object]] = []
        self.queries: list[Select[Any]] = []
        self.unlock_failure: str | None = None
        self.acquire_pause: asyncio.Event | None = None
        self.commit_failure: str | None = None
        self.commit_pause: asyncio.Event | None = None
        self.committed = asyncio.Event()
        self.tickets: dict[tuple[str, int], uuid.UUID] = {}
        self.acquired = asyncio.Event()

    def __call__(self) -> Session:
        connection = Connection(self)
        self.connections.append(connection)
        return Session(connection)

    @property
    def factory(self) -> async_sessionmaker[AsyncSession]:
        return cast(async_sessionmaker[AsyncSession], self)


class Session:
    def __init__(self, connection: Connection) -> None:
        self.bound = connection

    async def __aenter__(self) -> Session:
        return self

    async def __aexit__(self, *_: object) -> None:
        # Returning a connection to the pool must not leave session locks.
        assert self.bound not in self.bound.database.locks.values()
        self.bound.closed = True

    async def connection(self) -> Connection:
        return self.bound


class Connection:
    def __init__(self, database: Database) -> None:
        self.database = database
        self.invalidated = False
        self.closed = False
        self.drop = False
        self.backend_changed = False
        self.pid = len(database.connections) + 1
        self.pending: tuple[tuple[str, int], uuid.UUID | None] | None = None

    async def scalar(
        self, statement: TextClause | Insert | Delete, parameters: dict[str, int] | None = None
    ) -> int | bool | uuid.UUID | None:
        if self.drop:
            raise ConnectionError("simulated disconnected backend")
        if isinstance(statement, Insert):
            values = statement.compile().params
            assert values is not None
            ticket_key = (values["account_id"], values["slot"])
            if ticket_key in self.database.tickets:
                return None
            self.pending = (ticket_key, values["token"])
            return cast(uuid.UUID, values["token"])
        if isinstance(statement, Delete):
            values = statement.compile().params
            assert values is not None
            ticket_key = (values["account_id_1"], values["slot_1"])
            if self.database.tickets.get(ticket_key) == values["token_1"]:
                self.pending = (ticket_key, None)
                return cast(uuid.UUID, values["token_1"])
            return None
        sql = str(statement)
        if "pg_try_advisory_lock" in sql:
            assert parameters is not None
            key = parameters["key"]
            if key in self.database.locks:
                return False
            self.database.locks[key] = self
            self.database.acquired.set()
            if self.database.acquire_pause is not None:
                await self.database.acquire_pause.wait()
            return True
        if "pg_advisory_unlock" in sql:
            assert parameters is not None
            if self.database.unlock_failure == "error":
                raise ConnectionError("simulated unlock error")
            if self.database.unlock_failure == "false":
                return False
            key = parameters["key"]
            if self.database.locks.get(key) is self:
                del self.database.locks[key]
                return True
            return False
        assert "pg_backend_pid" in sql
        return self.pid + int(self.backend_changed)

    async def scalars(self, statement: Select[Any]) -> list[dict[str, object]]:
        self.database.queries.append(statement)
        return self.database.records

    async def invalidate(self) -> None:
        self.invalidated = True
        self.pending = None
        for key, owner in tuple(self.database.locks.items()):
            if owner is self:
                del self.database.locks[key]

    async def commit(self) -> None:
        if self.drop:
            raise ConnectionError("simulated disconnected backend")
        if self.database.commit_failure == "before":
            raise ConnectionError("commit failed before persistence")
        if self.pending is not None:
            key, value = self.pending
            if value is None:
                self.database.tickets.pop(key, None)
            else:
                self.database.tickets[key] = value
            self.pending = None
        self.database.committed.set()
        if self.database.commit_failure == "after":
            raise ConnectionError("commit response lost after persistence")
        if self.database.commit_pause is not None:
            await self.database.commit_pause.wait()


class Adapter:
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.finish = asyncio.Event()
        self.cleaning = asyncio.Event()
        self.cleaned = asyncio.Event()
        self.allow_cleanup = asyncio.Event()
        self.allow_cleanup.set()
        self.executions: list[
            tuple[ProviderRequest, ProviderProfileConfig, AccountExecution | None]
        ] = []
        self.failure: Exception | None = None
        self.result_status = ProviderResultStatus.SUCCEEDED

    async def execute(
        self,
        request: ProviderRequest,
        profile: ProviderProfileConfig,
        cancellation: CancellationContext,
    ) -> ProviderResult:
        self.executions.append((request, profile, current_account_execution()))
        self.started.set()
        try:
            await self.finish.wait()
            if self.failure is not None:
                raise self.failure
            return ProviderResult(
                status=self.result_status,
                text="ok",
                usage=NormalizedUsage(duration_ms=0),
                adapter_version="fake.v1",
            )
        finally:
            self.cleaning.set()
            await self.allow_cleanup.wait()
            self.cleaned.set()

    async def health(self, profile: ProviderProfileConfig) -> EffectiveProfileState:
        return EffectiveProfileState(active_leases=3)


def bound(
    database: Database,
    adapter: Adapter,
    configured: ProviderProfileConfig,
    profile_ids: tuple[str, ...] | None = None,
) -> AccountBoundAdapter:
    return AccountBoundAdapter(
        adapter,
        configured,
        database.factory,
        account_profile_ids=profile_ids or (configured.id,),
    )


async def wait_for(event: asyncio.Event) -> None:
    await asyncio.wait_for(event.wait(), timeout=2)


def test_registry_requires_factory_even_for_injected_account_adapter(tmp_path: Path) -> None:
    configured = profile("first", account_id="shared")
    resolver = ScopedSecretResolver(access_policy={}, secret_file_root=tmp_path, environment={})
    with pytest.raises(ValueError, match="session_factory"):
        AdapterRegistry(ProfileRegistry((configured,)), resolver, adapters={"first": Adapter()})


def test_registry_wraps_built_and_injected_adapters_and_preserves_default(tmp_path: Path) -> None:
    configured = profile("first", account_id="shared")
    resolver = ScopedSecretResolver(
        access_policy={"env:FIRST_KEY": frozenset({"profile:first"})},
        secret_file_root=tmp_path,
        environment={"FIRST_KEY": "unit-test-key"},  # pragma: allowlist secret
    )
    database = Database()
    registry = AdapterRegistry(
        ProfileRegistry((configured,)), resolver, session_factory=database.factory
    )
    assert isinstance(registry.get("first"), AccountBoundAdapter)
    assert registry.get("first") is registry.get("first")
    adapter = Adapter()
    isolated = profile("isolated")
    registry = AdapterRegistry(
        ProfileRegistry((isolated,)), resolver, adapters={"isolated": adapter}
    )
    assert registry.get("isolated") is adapter


@pytest.mark.parametrize("limit", [1, 2, 3])
def test_independent_registries_enforce_combined_concurrency_across_models_roles(
    tmp_path: Path, limit: int
) -> None:
    async def scenario() -> None:
        database = Database()
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
                session_factory=database.factory,
                adapters={item.id: adapter},
            ).get(item.id)
            for item, adapter in zip(configured, adapters, strict=True)
        ]
        running = []
        for index in range(limit):
            running.append(
                asyncio.create_task(
                    wrappers[index].execute(
                        provider_request().model_copy(
                            update={"role": next(iter(configured[index].roles))}
                        ),
                        configured[index],
                        CancellationContext(),
                    )
                )
            )
            await wait_for(adapters[index].started)
        with pytest.raises(ProviderFailure, match="busy") as error:
            await wrappers[-1].execute(provider_request(), configured[-1], CancellationContext())
        assert not error.value.request_sent and error.value.retryable
        assert not adapters[-1].executions
        assert len(database.locks) == limit
        for adapter in adapters:
            adapter.finish.set()
        await asyncio.gather(*running)
        await wrappers[-1].execute(provider_request(), configured[-1], CancellationContext())
        assert not database.locks
        assert current_account_execution() is None
        assert all(connection.closed for connection in database.connections)

    asyncio.run(scenario())


def test_other_accounts_are_independent_and_health_does_not_take_a_slot() -> None:
    async def scenario() -> None:
        database = Database()
        first, second = profile("first", account_id="first"), profile("second", account_id="second")
        adapters = [Adapter(), Adapter()]
        wrappers = [
            bound(database, adapter, item)
            for adapter, item in zip(adapters, (first, second), strict=True)
        ]
        assert (await wrappers[0].health(first)).active_leases == 3
        assert not database.connections
        running = [
            asyncio.create_task(wrapper.execute(provider_request(), item, CancellationContext()))
            for wrapper, item in zip(wrappers, (first, second), strict=True)
        ]
        for adapter in adapters:
            await wait_for(adapter.started)
        assert len(database.locks) == 2
        for adapter in adapters:
            adapter.finish.set()
        await asyncio.gather(*running)
        assert not database.locks

    asyncio.run(scenario())


@pytest.mark.parametrize("failure", ["error", "false"])
def test_unlock_failure_invalidates_connection_and_returns_receipt(failure: str) -> None:
    async def scenario() -> None:
        database, adapter = Database(), Adapter()
        database.unlock_failure = failure
        adapter.finish.set()
        configured = profile("first", account_id="shared")
        result = await bound(database, adapter, configured).execute(
            provider_request(), configured, CancellationContext()
        )
        assert result.text == "ok" and result.usage is not None
        assert database.connections[0].invalidated
        assert not database.locks

    asyncio.run(scenario())


def test_cancel_during_acquire_evicts_connection_with_uncertain_lock_result() -> None:
    async def scenario() -> None:
        database, adapter = Database(), Adapter()
        database.acquire_pause = asyncio.Event()
        configured = profile("first", account_id="shared")
        task = asyncio.create_task(
            bound(database, adapter, configured).execute(
                provider_request(), configured, CancellationContext()
            )
        )
        await wait_for(database.acquired)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert database.connections[0].invalidated
        assert not database.locks
        assert not adapter.executions

    asyncio.run(scenario())


def test_cancellation_drains_actual_execute_before_unlock_even_when_cancelled_again() -> None:
    async def scenario() -> None:
        database, adapter = Database(), Adapter()
        adapter.allow_cleanup.clear()
        configured = profile("first", account_id="shared")
        cancellation = CancellationContext()
        task = asyncio.create_task(
            bound(database, adapter, configured).execute(
                provider_request(), configured, cancellation
            )
        )
        await wait_for(adapter.started)
        task.cancel()
        await wait_for(adapter.cleaning)
        task.cancel()
        await asyncio.sleep(0)
        assert cancellation.requested
        assert database.locks and not task.done()
        adapter.allow_cleanup.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert adapter.cleaned.is_set()
        assert not database.locks

    asyncio.run(scenario())


@pytest.mark.parametrize("mode", ["drop", "backend_changed", "invalidated"])
def test_watchdog_cancels_runtime_and_drains_cleanup_on_connection_loss(
    monkeypatch: pytest.MonkeyPatch, mode: str
) -> None:
    monkeypatch.setattr(accounts, "ACCOUNT_LOCK_POLL_SECONDS", 0.001)

    async def scenario() -> None:
        database, adapter = Database(), Adapter()
        adapter.allow_cleanup.clear()
        configured = profile("first", account_id="shared")
        cancellation = CancellationContext()
        task = asyncio.create_task(
            bound(database, adapter, configured).execute(
                provider_request(), configured, cancellation
            )
        )
        await wait_for(adapter.started)
        setattr(database.connections[0], mode, True)
        await wait_for(adapter.cleaning)
        assert cancellation.requested and not task.done()
        adapter.allow_cleanup.set()
        with pytest.raises(ProviderFailure, match="lock connection lost") as error:
            await task
        assert error.value.request_sent and not error.value.retryable
        assert not error.value.execution_completed
        assert database.tickets
        with pytest.raises(ProviderFailure, match="ticket"):
            await bound(database, adapter, configured).execute(
                provider_request(), configured, CancellationContext()
            )
        assert len(adapter.executions) == 1
        assert adapter.cleaned.is_set()
        assert not database.locks

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "metadata",
    [
        {},
        {"account_id": "shared"},
        {"account_id": "shared", "account_slot": True},
        {"account_id": "shared", "account_slot": -1},
        {"account_id": "old", "account_slot": 0},
    ],
)
def test_unresolved_legacy_or_malformed_process_blocks_entire_account(
    tmp_path: Path, metadata: dict[str, object]
) -> None:
    async def scenario() -> None:
        database, adapter = Database(), Adapter()
        database.records = [metadata]
        configured = codex_profile(tmp_path).model_copy(
            update={"account_id": "shared", "concurrency_limit": 2}
        )
        with pytest.raises(ProviderFailure, match="unresolved process") as error:
            await bound(database, adapter, configured).execute(
                provider_request(), configured, CancellationContext()
            )
        assert not error.value.request_sent
        assert not adapter.executions
        assert not database.locks

    asyncio.run(scenario())


def test_unresolved_slot_survives_lost_lock_and_other_slot_can_execute(tmp_path: Path) -> None:
    async def scenario() -> None:
        database, adapter = Database(), Adapter()
        database.records = [{"account_id": "shared", "account_slot": 0}]
        adapter.finish.set()
        configured = codex_profile(tmp_path).model_copy(
            update={"account_id": "shared", "concurrency_limit": 2}
        )
        await bound(
            database, adapter, configured, (configured.id, "another-model-profile")
        ).execute(provider_request(), configured, CancellationContext())
        execution = adapter.executions[0][2]
        assert execution is not None
        assert (execution.account_id, execution.slot) == ("shared", 1)
        assert execution.runtime_metadata == {
            "account_id": "shared",
            "account_slot": 1,
            "account_lease_token": str(execution.token),
        }
        compiled = database.queries[0].compile(dialect=postgresql.dialect())  # type: ignore[no-untyped-call]
        sql = str(compiled)
        assert "profile_id IN" in sql and "runtime_metadata" in sql
        assert "status !=" in sql and "reaped_at IS NULL" in sql
        assert "another-model-profile" in compiled.params["profile_id_1"]
        assert "shared" in compiled.params.values()
        assert "exited" in compiled.params.values()
        assert not database.locks
        assert current_account_execution() is None

    asyncio.run(scenario())


def test_cancelled_before_launch_and_changed_binding_never_call_provider() -> None:
    async def scenario() -> None:
        database, adapter = Database(), Adapter()
        configured = profile("first", account_id="shared")
        wrapper = bound(database, adapter, configured)
        cancellation = CancellationContext()
        cancellation.request()
        with pytest.raises(ProviderFailure, match="cancelled") as error:
            await wrapper.execute(provider_request(), configured, cancellation)
        assert not error.value.request_sent
        for changes in (
            {"provider": "different"},
            {"account_id": "different"},
            {"concurrency_limit": 2},
            {"credential_reference": "env:OTHER_KEY"},
        ):
            with pytest.raises(ValueError, match="binding changed"):
                await wrapper.execute(
                    provider_request(), configured.model_copy(update=changes), CancellationContext()
                )
        assert not adapter.executions and not database.connections

    asyncio.run(scenario())


def test_adapter_error_preserves_failure_and_releases_slot() -> None:
    async def scenario() -> None:
        database, adapter = Database(), Adapter()
        adapter.finish.set()
        failure = RuntimeError("adapter failed")
        adapter.failure = failure
        configured = profile("first", account_id="shared")
        with pytest.raises(RuntimeError) as error:
            await bound(database, adapter, configured).execute(
                provider_request(), configured, CancellationContext()
            )
        assert error.value is failure
        assert not database.locks
        assert current_account_execution() is None

    asyncio.run(scenario())


def test_slot_keys_are_stable_signed_bigints() -> None:
    assert account_slot_key("shared", 0) == -8870189052205783262
    keys = {
        account_slot_key(account, slot) for account in ("first", "second") for slot in range(100)
    }
    assert len(keys) == 200
    assert all(-(2**63) <= key < 2**63 for key in keys)


@pytest.mark.parametrize("lose_connection", [False, True])
def test_simultaneous_receipt_and_monitor_failure_keeps_usage(monkeypatch, lose_connection):
    async def scenario():
        database, adapter = Database(), Adapter()
        adapter.finish.set()
        configured = profile("first", account_id="shared")
        wrapper = bound(database, adapter, configured)

        async def failed_monitor(connection, backend_pid):
            connection.drop = lose_connection
            raise ConnectionError("watcher failed at the same time as receipt")

        monkeypatch.setattr(wrapper, "_monitor", failed_monitor)
        result = await wrapper.execute(provider_request(), configured, CancellationContext())
        assert result.text == "ok" and result.usage is not None
        assert bool(database.tickets) == lose_connection
        assert not database.locks

    asyncio.run(scenario())


def test_isolated_cli_profile_also_has_durable_cross_role_admission(tmp_path):
    async def scenario():
        database, adapter = Database(), Adapter()
        configured = codex_profile(tmp_path)
        assert configured.account_id is None
        resolver = ScopedSecretResolver(access_policy={}, secret_file_root=tmp_path, environment={})
        registry = AdapterRegistry(
            ProfileRegistry((configured,)),
            resolver,
            adapters={configured.id: adapter},
            session_factory=database.factory,
        )
        wrapper = registry.get(configured.id)
        assert isinstance(wrapper, AccountBoundAdapter)
        running = asyncio.create_task(
            wrapper.execute(provider_request(), configured, CancellationContext())
        )
        await wait_for(adapter.started)
        with pytest.raises(ProviderFailure, match="busy") as raised:
            await wrapper.execute(provider_request(), configured, CancellationContext())
        assert not raised.value.request_sent
        assert len(adapter.executions) == 1
        assert adapter.executions[0][2].account_id.startswith("profile:")
        adapter.finish.set()
        await running
        assert not database.tickets and not database.locks
        database.records = [{}]  # Old unresolved process with no account metadata.
        with pytest.raises(ProviderFailure, match="unresolved process"):
            await wrapper.execute(provider_request(), configured, CancellationContext())
        assert len(adapter.executions) == 1

    asyncio.run(scenario())
