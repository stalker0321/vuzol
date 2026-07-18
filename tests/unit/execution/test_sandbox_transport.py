"""Sandbox transport tests (split for cohesion)."""

from __future__ import annotations

from ._execution_helpers import (
    AsyncMock,
    CancellationContext,
    CodexProcessResult,
    LocalGit,
    MagicMock,
    Path,
    ProxyNetworkLease,
    ProxyServiceError,
    ProxyServiceLease,
    RequiredGate,
    SandboxCodexTransport,
    SandboxError,
    WorkerFinalizer,
    WorktreeAccessError,
    _sandbox_gate_runner,
    asyncio,
    envelope,
    pytest,
    uuid,
)


@pytest.mark.anyio
async def test_missing_validation_sandbox_prevents_provider_execution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from vuzol.providers import handlers as provider_handlers
    from vuzol.providers.handlers import ProviderStepHandler

    monkeypatch.setattr(provider_handlers, "release_reservation", AsyncMock())

    factory = MagicMock()
    handler = ProviderStepHandler(
        factory,
        MagicMock(),
        MagicMock(),
        worktrees=MagicMock(),
        finalizer=MagicMock(),
        worktree_access=MagicMock(),
    )
    provider_request = MagicMock(task_draft={"step09a_capsule": {}})
    handler._build_request = AsyncMock(  # type: ignore[method-assign]
        return_value=(provider_request, "grok-a", uuid.uuid4(), "revision")
    )
    handler._grant_worktree_access = AsyncMock(  # type: ignore[method-assign]
        side_effect=WorktreeAccessError("project has no validation sandbox profile")
    )
    handler._unwind_pre_provider = AsyncMock()  # type: ignore[method-assign]
    handler._execute_built = AsyncMock()  # type: ignore[method-assign]

    outcome = await handler.execute(MagicMock(step_type="execute_code"), CancellationContext())

    assert outcome.category == "worker_access_unavailable"
    handler._execute_built.assert_not_awaited()


@pytest.mark.anyio
async def test_gate_runner_requires_fenced_sandbox_context(tmp_path: Path) -> None:
    runner, envelopes, runtime = _sandbox_gate_runner()
    with pytest.raises(ValueError, match="context is unavailable"):
        await runner.run(
            tmp_path,
            (RequiredGate(name="test", command_id="make test"),),
            timeout_seconds=10,
            context=None,
            cancellation=None,
        )
    envelopes.build_gate.assert_not_awaited()
    runtime.run.assert_not_awaited()


def test_worker_finalizer_requires_explicit_sandbox_runner() -> None:
    with pytest.raises(ValueError, match="explicit sandbox gate runner"):
        WorkerFinalizer(LocalGit())


@pytest.mark.anyio
async def test_sandbox_codex_transport_records_success_and_failure(tmp_path: Path) -> None:
    configured = envelope(tmp_path)
    process_id = uuid.uuid4()
    envelopes = MagicMock()
    envelopes.proxy_targets = AsyncMock(return_value=())
    envelopes.build = AsyncMock(return_value=(configured, process_id))
    envelopes.mark_running = AsyncMock()
    envelopes.complete = AsyncMock()
    envelopes.fail_unknown = AsyncMock()
    runtime = MagicMock()
    runtime.run = AsyncMock(return_value=CodexProcessResult(0, "ok", "", 5))
    transport = SandboxCodexTransport(runtime, envelopes, MagicMock())

    result = await transport.run(MagicMock(), CancellationContext())
    assert result.stdout == "ok"
    envelopes.mark_running.assert_awaited_once()
    envelopes.complete.assert_awaited_once()

    runtime.run.side_effect = SandboxError("failed after start")
    with pytest.raises(SandboxError):
        await transport.run(MagicMock(), CancellationContext())
    envelopes.fail_unknown.assert_awaited_once_with(process_id)


@pytest.mark.anyio
async def test_sandbox_transport_materializes_and_cleans_controlled_proxy(
    tmp_path: Path,
) -> None:
    configured = envelope(tmp_path)
    process_id = uuid.uuid4()
    invocation = MagicMock(
        task_id=configured.task_id,
        run_id=configured.run_id,
        step_id=configured.step_id,
        lease_generation=configured.lease_generation,
    )
    target = MagicMock()
    envelopes = MagicMock()
    envelopes.proxy_targets = AsyncMock(return_value=(target,))
    envelopes.build = AsyncMock(return_value=(configured, process_id))
    envelopes.mark_running = AsyncMock()
    envelopes.complete = AsyncMock()
    envelopes.fail_unknown = AsyncMock()
    runtime = MagicMock()
    runtime.run = AsyncMock(return_value=CodexProcessResult(0, "ok", "", 5))
    networks = ProxyNetworkLease(
        internal_name="vuzol-internal",
        egress_name="vuzol-egress",
        task_id=configured.task_id,
        run_id=configured.run_id,
        step_id=configured.step_id,
        lease_generation=configured.lease_generation,
    )
    lease = ProxyServiceLease(
        container_name="vuzol-proxy",
        networks=networks,
        task_id=configured.task_id,
        run_id=configured.run_id,
        step_id=configured.step_id,
        lease_generation=configured.lease_generation,
        policy_hash="a" * 64,
    )
    proxy = MagicMock()
    proxy.create = AsyncMock(return_value=lease)
    never_dead = asyncio.Event()

    async def wait_until_dead(_lease: ProxyServiceLease) -> None:
        await never_dead.wait()

    proxy.wait_until_dead = AsyncMock(side_effect=wait_until_dead)
    proxy.cleanup = AsyncMock()

    result = await SandboxCodexTransport(runtime, envelopes, MagicMock(), proxy).run(
        invocation, CancellationContext()
    )
    assert result.stdout == "ok"
    proxy.create.assert_awaited_once_with(
        configured.task_id,
        configured.run_id,
        configured.step_id,
        configured.lease_generation,
        (target,),
    )
    envelopes.build.assert_awaited_once_with(
        invocation,
        proxy_network="vuzol-internal",
        https_proxy_url="http://vuzol-proxy:8888",
    )
    proxy.cleanup.assert_awaited_once_with(lease)


@pytest.mark.anyio
async def test_proxy_death_cancels_sandbox_and_fails_closed(tmp_path: Path) -> None:
    configured = envelope(tmp_path)
    process_id = uuid.uuid4()
    invocation = MagicMock(
        task_id=configured.task_id,
        run_id=configured.run_id,
        step_id=configured.step_id,
        lease_generation=configured.lease_generation,
    )
    envelopes = MagicMock()
    envelopes.proxy_targets = AsyncMock(return_value=(MagicMock(),))
    envelopes.build = AsyncMock(return_value=(configured, process_id))
    envelopes.mark_running = AsyncMock()
    envelopes.complete = AsyncMock()
    envelopes.fail_unknown = AsyncMock()
    runtime = MagicMock()
    runtime_started = asyncio.Event()

    async def running(*_args: object) -> CodexProcessResult:
        runtime_started.set()
        await asyncio.Event().wait()
        raise AssertionError("cancelled sandbox must not return")

    runtime.run = AsyncMock(side_effect=running)
    networks = ProxyNetworkLease(
        internal_name="vuzol-internal",
        egress_name="vuzol-egress",
        task_id=configured.task_id,
        run_id=configured.run_id,
        step_id=configured.step_id,
        lease_generation=configured.lease_generation,
    )
    lease = ProxyServiceLease(
        container_name="vuzol-proxy",
        networks=networks,
        task_id=configured.task_id,
        run_id=configured.run_id,
        step_id=configured.step_id,
        lease_generation=configured.lease_generation,
        policy_hash="a" * 64,
    )
    proxy = MagicMock()
    proxy.create = AsyncMock(return_value=lease)

    async def dies(_lease: ProxyServiceLease) -> None:
        await runtime_started.wait()

    proxy.wait_until_dead = AsyncMock(side_effect=dies)
    proxy.cleanup = AsyncMock()
    with pytest.raises(RuntimeError, match="proxy exited"):
        await SandboxCodexTransport(runtime, envelopes, MagicMock(), proxy).run(
            invocation, CancellationContext()
        )
    proxy.cleanup.assert_awaited_once_with(lease)
    envelopes.fail_unknown.assert_awaited_once_with(process_id)


@pytest.mark.anyio
async def test_proxy_start_failure_prevents_sandbox_build_and_start(tmp_path: Path) -> None:
    configured = envelope(tmp_path)
    invocation = MagicMock(
        task_id=configured.task_id,
        run_id=configured.run_id,
        step_id=configured.step_id,
        lease_generation=configured.lease_generation,
    )
    envelopes = MagicMock()
    envelopes.proxy_targets = AsyncMock(return_value=(MagicMock(),))
    envelopes.build = AsyncMock()
    proxy = MagicMock()
    proxy.create = AsyncMock(side_effect=ProxyServiceError("startup failed"))
    runtime = MagicMock()
    runtime.run = AsyncMock()
    with pytest.raises(ProxyServiceError, match="startup failed"):
        await SandboxCodexTransport(runtime, envelopes, MagicMock(), proxy).run(
            invocation, CancellationContext()
        )
    envelopes.build.assert_not_awaited()
    runtime.run.assert_not_awaited()
