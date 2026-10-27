"""Trusted gate registry tests."""

from __future__ import annotations

from ._execution_helpers import (
    AsyncMock,
    CancellationContext,
    GateExecutionContext,
    Path,
    RequiredGate,
    TrustedGateRunner,
    _sandbox_gate_runner,
    pytest,
    uuid,
)


def _gate_context() -> GateExecutionContext:
    return GateExecutionContext(
        task_id=uuid.uuid4(),
        run_id=uuid.uuid4(),
        step_id=uuid.uuid4(),
        worktree_id=uuid.uuid4(),
        profile_id="grok-a",
        provider_attempt=1,
        lease_generation=1,
    )


@pytest.mark.anyio
async def test_trusted_gate_registry_rejects_arbitrary_text_before_execution(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    execute = AsyncMock()
    monkeypatch.setattr(TrustedGateRunner, "_execute", execute)
    runner, _envelopes, _runtime = _sandbox_gate_runner()
    with pytest.raises(ValueError, match="unknown trusted gate"):
        await runner.run(
            tmp_path,
            (RequiredGate(name="unsafe", command_id="make test && git commit -am bad"),),
            timeout_seconds=10,
            context=_gate_context(),
            cancellation=CancellationContext(),
        )
    execute.assert_not_awaited()


@pytest.mark.anyio
async def test_trusted_gate_registry_resolves_offline_security_preflight(
    tmp_path: Path,
) -> None:
    runner, envelopes, runtime = _sandbox_gate_runner()
    result = await runner.run(
        tmp_path,
        (RequiredGate(name="security", command_id="make security"),),
        timeout_seconds=10,
        context=_gate_context(),
        cancellation=CancellationContext(),
    )
    assert result[0].evidence.argv == ("/usr/bin/make", "security")
    assert result[0].evidence.exit_code == 0
    envelopes.build_gate.assert_awaited_once()
    runtime.run.assert_awaited_once()
