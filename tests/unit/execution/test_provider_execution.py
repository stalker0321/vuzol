"""Domain tests split from the former monolithic test_execution module."""

from __future__ import annotations

from ._execution_helpers import (
    AsyncMock,
    CancellationContext,
    MagicMock,
    _certified_codex_profile,
    pytest,
    uuid,
)


@pytest.mark.anyio
async def test_uncertified_exact_runtime_fails_before_provider_invocation() -> None:
    from vuzol.providers.handlers import ProviderStepHandler

    profile = _certified_codex_profile()
    registries = MagicMock()
    registries.profiles.get.return_value = profile
    handler = ProviderStepHandler(
        MagicMock(),
        registries,
        MagicMock(),
        worktrees=MagicMock(),
        finalizer=MagicMock(),
        worktree_access=MagicMock(),
        agent_certificates=MagicMock(),
    )
    provider_request = MagicMock(task_draft={"step09a_capsule": {}})
    handler._build_request = AsyncMock(  # type: ignore[method-assign]
        return_value=(provider_request, profile.id, uuid.uuid4(), "revision")
    )
    handler._require_agent_certificate = AsyncMock(  # type: ignore[method-assign]
        side_effect=ValueError("agent runtime is uncertified")
    )
    handler._unwind_pre_provider = AsyncMock()  # type: ignore[method-assign]
    handler._grant_worktree_access = AsyncMock()  # type: ignore[method-assign]
    handler._execute_built = AsyncMock()  # type: ignore[method-assign]

    outcome = await handler.execute(MagicMock(step_type="execute_code"), CancellationContext())

    assert outcome.category == "agent_runtime_uncertified"
    handler._grant_worktree_access.assert_not_awaited()
    handler._execute_built.assert_not_awaited()


@pytest.mark.anyio
@pytest.mark.parametrize("error", (LookupError("missing state"), ValueError("invalid state")))
async def test_provider_request_preparation_failure_unwinds_before_adapter(
    error: Exception,
) -> None:
    from vuzol.providers.handlers import ProviderStepHandler
    from vuzol.workflows.domain import OutcomeKind

    adapters = MagicMock()
    handler = ProviderStepHandler(MagicMock(), MagicMock(), adapters)
    handler._build_request = AsyncMock(side_effect=error)  # type: ignore[method-assign]
    handler._unwind_pre_provider = AsyncMock()  # type: ignore[method-assign]
    request = MagicMock(
        task_id=uuid.uuid4(),
        run_id=uuid.uuid4(),
        step_id=uuid.uuid4(),
        step_type="execute_model",
        payload={"budget_reservation_id": str(uuid.uuid4()), "provider_attempt": 1},
    )

    outcome = await handler.execute(request, CancellationContext())

    assert outcome.kind is OutcomeKind.PERMANENT_FAILURE
    assert outcome.category == "provider_request_invalid"
    assert outcome.summary == type(error).__name__
    handler._unwind_pre_provider.assert_awaited_once()
    adapters.get.assert_not_called()


@pytest.mark.anyio
async def test_pre_provider_unwind_failure_preserves_both_safe_failure_types() -> None:
    from vuzol.providers.handlers import ProviderStepHandler

    handler = ProviderStepHandler(MagicMock(), MagicMock(), MagicMock())
    handler._unwind_pre_provider = AsyncMock(  # type: ignore[method-assign]
        side_effect=RuntimeError("private unwind detail")
    )
    request = MagicMock(
        task_id=uuid.uuid4(),
        run_id=uuid.uuid4(),
        step_id=uuid.uuid4(),
        lease=MagicMock(generation=3),
    )

    outcome = await handler._pre_provider_failure(
        request,
        reservation_id=uuid.uuid4(),
        category="provider_request_invalid",
        error=ValueError("private preparation detail"),
    )

    assert outcome.category == "pre_provider_unwind_failed"
    assert outcome.summary == "provider_request_invalid followed by unwind failure (RuntimeError)"
    assert "private" not in outcome.summary


def test_reservation_reference_parser_rejects_malformed_values() -> None:
    from vuzol.providers.handlers import _reservation_id, _safe_exception_location

    reservation_id = uuid.uuid4()
    assert _reservation_id({"budget_reservation_id": str(reservation_id)}) == reservation_id
    assert _reservation_id({"budget_reservation_id": "../reservation"}) is None
    assert _reservation_id({"budget_reservation_id": 1}) is None
    assert _reservation_id({}) is None
    assert _safe_exception_location(RuntimeError()) is None
    try:
        raise RuntimeError("safe location")
    except RuntimeError as error:
        location = _safe_exception_location(error)
        traceback = error.__traceback__
        assert traceback is not None
        line_number = traceback.tb_lineno
    assert location is not None and location.endswith(
        f"test_reservation_reference_parser_rejects_malformed_values:{line_number}"
    )


@pytest.mark.anyio
@pytest.mark.parametrize("process_id", (None, uuid.uuid4()))
async def test_provider_launch_detection_uses_exact_durable_process(
    process_id: uuid.UUID | None,
) -> None:
    from vuzol.providers.handlers import ProviderStepHandler

    session = AsyncMock()
    session.scalar.return_value = process_id
    factory = MagicMock()
    factory.return_value.__aenter__.return_value = session
    factory.return_value.__aexit__.return_value = False
    request = MagicMock(
        task_id=uuid.uuid4(),
        run_id=uuid.uuid4(),
        step_id=uuid.uuid4(),
        lease=MagicMock(generation=4),
    )

    detected = await ProviderStepHandler(factory, MagicMock(), MagicMock())._provider_launch_exists(
        request
    )

    assert detected is (process_id is not None)
    session.scalar.assert_awaited_once()


@pytest.mark.anyio
async def test_non_code_failure_does_not_retain_a_worktree() -> None:
    from vuzol.providers.handlers import ProviderStepHandler

    worktrees = MagicMock()
    worktrees.retain = AsyncMock()
    factory = MagicMock()
    handler = ProviderStepHandler(factory, MagicMock(), MagicMock(), worktrees=worktrees)

    await handler._retain_active_worktree(MagicMock(step_type="execute_model"))

    factory.begin.assert_not_called()
    worktrees.retain.assert_not_awaited()


@pytest.mark.anyio
async def test_unexpected_post_launch_failure_uses_conservative_accounting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from vuzol.providers import handlers as provider_handlers
    from vuzol.providers.handlers import ProviderStepHandler

    reconcile = AsyncMock()
    observe = AsyncMock()
    monkeypatch.setattr(provider_handlers, "reconcile_usage", reconcile)
    monkeypatch.setattr(provider_handlers, "record_failure_observation", observe)
    transaction = AsyncMock()
    transaction.__aenter__.return_value = AsyncMock()
    transaction.__aexit__.return_value = False
    factory = MagicMock()
    factory.begin.return_value = transaction
    handler = ProviderStepHandler(factory, MagicMock(), MagicMock())
    handler._retain_active_worktree = AsyncMock()  # type: ignore[method-assign]
    request = MagicMock(
        task_id=uuid.uuid4(),
        run_id=uuid.uuid4(),
        step_id=uuid.uuid4(),
        lease=MagicMock(generation=2),
    )
    profile = MagicMock(provider="codex", model="codex", model_reasoning_effort=None)
    reservation_id = uuid.uuid4()

    outcome = await handler._unexpected_launched_provider_failure(
        request,
        reservation_id=reservation_id,
        profile=profile,
        configuration_revision="a" * 64,
        error=PermissionError("private path"),
    )

    assert outcome.category == "provider_execution_unexpected"
    assert outcome.summary == "PermissionError"
    reconcile.assert_awaited_once()
    reconciliation_call = reconcile.await_args
    assert reconciliation_call is not None
    assert reconciliation_call.kwargs["reservation_id"] == reservation_id
    assert reconciliation_call.kwargs["conservative"] is True
    observe.assert_awaited_once()
    handler._retain_active_worktree.assert_awaited_once_with(request)
