"""Durable admission survives lost lock connections and uncertain commits."""

import asyncio
import uuid
from pathlib import Path

import pytest

from vuzol.providers.account_persistence import release_account_ticket_after_cleanup
from vuzol.providers.accounts import account_slot_key
from vuzol.providers.domain import NormalizedUsage, ProviderErrorCategory, ProviderResultStatus
from vuzol.providers.errors import ProviderFailure
from vuzol.workflows.ports import CancellationContext

from ._test_providers_helpers import codex_profile, profile, provider_request
from .test_accounts import Adapter, Database, bound, wait_for


def test_durable_commit_must_finish_before_execute_and_ticket_matches_context() -> None:
    async def scenario() -> None:
        database, adapter = Database(), Adapter()
        database.commit_pause = asyncio.Event()
        configured = profile("first", account_id="shared")
        task = asyncio.create_task(
            bound(database, adapter, configured).execute(
                provider_request(), configured, CancellationContext()
            )
        )
        await wait_for(database.committed)
        assert database.tickets and not adapter.executions
        database.commit_pause.set()
        await wait_for(adapter.started)
        execution = adapter.executions[0][2]
        assert execution is not None
        assert database.tickets[("shared", 0)] == execution.token
        assert database.locks[account_slot_key("shared", 0)] is database.connections[0]
        adapter.finish.set()
        await task
        assert not database.tickets and not database.locks

    asyncio.run(scenario())


@pytest.mark.parametrize("failure", ["before", "after"])
def test_uncertain_commit_never_executes_and_preserves_any_durable_ticket(failure: str) -> None:
    async def scenario() -> None:
        database, adapter = Database(), Adapter()
        database.commit_failure = failure
        configured = profile("first", account_id="shared")
        wrapper = bound(database, adapter, configured)
        with pytest.raises(ConnectionError, match="commit"):
            await wrapper.execute(provider_request(), configured, CancellationContext())
        assert not adapter.executions and not database.locks
        assert database.connections[0].invalidated
        assert bool(database.tickets) == (failure == "after")
        database.commit_failure = None
        if failure == "after":
            with pytest.raises(ProviderFailure, match="ticket"):
                await wrapper.execute(provider_request(), configured, CancellationContext())
            assert not adapter.executions

    asyncio.run(scenario())


def test_cancelled_commit_response_preserves_ticket_without_sending() -> None:
    async def scenario() -> None:
        database, adapter = Database(), Adapter()
        database.commit_pause = asyncio.Event()
        configured = profile("first", account_id="shared")
        task = asyncio.create_task(
            bound(database, adapter, configured).execute(
                provider_request(), configured, CancellationContext()
            )
        )
        await wait_for(database.committed)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert not adapter.executions and not database.locks
        assert database.tickets and database.connections[0].invalidated

    asyncio.run(scenario())


@pytest.mark.parametrize("cancelled", [False, True])
def test_uncertain_execute_keeps_ticket_after_lock_released(cancelled: bool) -> None:
    async def scenario() -> None:
        database, adapter = Database(), Adapter()
        configured = profile("first", account_id="shared")
        wrapper = bound(database, adapter, configured)
        task = asyncio.create_task(
            wrapper.execute(provider_request(), configured, CancellationContext())
        )
        await wait_for(adapter.started)
        if cancelled:
            task.cancel()
        else:
            adapter.failure = RuntimeError("uncertain execute")
            adapter.finish.set()
        with pytest.raises((asyncio.CancelledError, RuntimeError)):
            await task
        assert database.tickets and not database.locks
        with pytest.raises(ProviderFailure, match="ticket"):
            await wrapper.execute(provider_request(), configured, CancellationContext())
        assert len(adapter.executions) == 1

    asyncio.run(scenario())


@pytest.mark.parametrize("unresolved", [False, True])
@pytest.mark.parametrize("sent", [False, True])
@pytest.mark.parametrize("measured", [False, True])
def test_authoritative_failure_releases_only_after_confirmed_cleanup(
    tmp_path: Path, unresolved: bool, sent: bool, measured: bool
) -> None:
    async def scenario() -> None:
        database, adapter = Database(), Adapter()
        configured = codex_profile(tmp_path).model_copy(update={"account_id": "shared"})
        failure = ProviderFailure(
            ProviderErrorCategory.INVALID_STRUCTURED_OUTPUT
            if sent
            else ProviderErrorCategory.QUOTA_EXHAUSTED,
            retryable=False,
            request_sent=sent,
            usage=NormalizedUsage(duration_ms=1) if measured else None,
            execution_completed=sent,
        )
        adapter.failure = failure
        task = asyncio.create_task(
            bound(database, adapter, configured).execute(
                provider_request(), configured, CancellationContext()
            )
        )
        await wait_for(adapter.started)
        if unresolved:
            database.records = [{"account_id": "shared", "account_slot": 0}]
        adapter.finish.set()
        with pytest.raises(ProviderFailure) as error:
            await task
        assert error.value is failure
        assert bool(database.tickets) == unresolved
        assert not database.locks

    asyncio.run(scenario())


@pytest.mark.parametrize("obstacle", ["none", "wrong-token", "ownership", "cancelled"])
def test_completion_evidence_cannot_bypass_release_fences(obstacle: str) -> None:
    async def scenario() -> None:
        database, adapter = Database(), Adapter()
        configured = profile("first", account_id="shared")
        cancellation = CancellationContext()
        failure = ProviderFailure(
            ProviderErrorCategory.RATE_LIMITED,
            retryable=True,
            request_sent=True,
            execution_completed=True,
        )
        adapter.failure = failure
        wrapper = bound(database, adapter, configured)
        task = asyncio.create_task(wrapper.execute(provider_request(), configured, cancellation))
        await wait_for(adapter.started)
        if obstacle == "wrong-token":
            database.tickets[("shared", 0)] = uuid.uuid4()
        elif obstacle == "ownership":
            database.connections[0].backend_changed = True
        elif obstacle == "cancelled":
            cancellation.request()
        expected_token = database.tickets[("shared", 0)]
        adapter.finish.set()
        with pytest.raises(ProviderFailure) as error:
            await task
        assert error.value is failure and failure.usage is None
        assert not database.locks
        if obstacle == "none":
            assert not database.tickets
        else:
            assert database.tickets[("shared", 0)] == expected_token
            with pytest.raises(ProviderFailure, match="ticket"):
                await wrapper.execute(provider_request(), configured, CancellationContext())
            assert len(adapter.executions) == 1

    asyncio.run(scenario())


def test_usage_alone_is_not_completion_evidence() -> None:
    async def scenario() -> None:
        database, adapter = Database(), Adapter()
        configured = profile("first", account_id="shared")
        adapter.failure = ProviderFailure(
            ProviderErrorCategory.TIMEOUT,
            retryable=True,
            request_sent=True,
            usage=NormalizedUsage(input_tokens=42, duration_ms=1),
        )
        adapter.finish.set()
        wrapper = bound(database, adapter, configured)
        with pytest.raises(ProviderFailure) as failure:
            await wrapper.execute(provider_request(), configured, CancellationContext())
        assert failure.value.usage is not None and not failure.value.execution_completed
        assert database.tickets and not database.locks
        with pytest.raises(ProviderFailure, match="ticket"):
            await wrapper.execute(provider_request(), configured, CancellationContext())
        assert len(adapter.executions) == 1

    asyncio.run(scenario())


def test_normalized_failed_result_releases_ticket() -> None:
    async def scenario() -> None:
        database, adapter = Database(), Adapter()
        adapter.result_status = ProviderResultStatus.FAILED
        adapter.finish.set()
        configured = profile("first", account_id="shared")
        result = await bound(database, adapter, configured).execute(
            provider_request(), configured, CancellationContext()
        )
        assert result.status is ProviderResultStatus.FAILED
        assert not database.tickets and not database.locks

    asyncio.run(scenario())


def test_success_with_unresolved_process_preserves_ticket_and_surfaces_reason(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        database, adapter = Database(), Adapter()
        configured = codex_profile(tmp_path).model_copy(update={"account_id": "shared"})
        task = asyncio.create_task(
            bound(database, adapter, configured).execute(
                provider_request(), configured, CancellationContext()
            )
        )
        await wait_for(adapter.started)
        database.records = [{"account_id": "shared", "account_slot": 0}]
        adapter.finish.set()
        with pytest.raises(ProviderFailure, match="cleanup unresolved; ticket retained"):
            await task
        assert database.tickets and not database.locks

    asyncio.run(scenario())


@pytest.mark.parametrize("refusal", ["no-proof", "lock-held", "unresolved", "wrong-token", "none"])
def test_reaper_requires_proof_free_lock_resolved_processes_and_matching_token(
    refusal: str,
) -> None:
    async def scenario() -> None:
        database = Database()
        token = uuid.uuid4()
        database.tickets[("shared", 0)] = token
        owner = None
        if refusal == "lock-held":
            owner = database().bound
            database.locks[account_slot_key("shared", 0)] = owner
        if refusal == "unresolved":
            database.records = [{"account_id": "shared", "account_slot": 0}]
        removed = await release_account_ticket_after_cleanup(
            database.factory,
            account_id="shared",
            slot=0,
            token=uuid.uuid4() if refusal == "wrong-token" else token,
            account_profile_ids=("first",),
            cleanup_confirmed=refusal != "no-proof",
        )
        assert removed == (refusal == "none")
        assert bool(database.tickets) == (refusal != "none")
        if refusal == "no-proof":
            assert not database.connections
        if owner is not None:
            assert database.locks[account_slot_key("shared", 0)] is owner
        else:
            assert not database.locks

    asyncio.run(scenario())
