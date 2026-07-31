"""Fake-only tests for B3.5 restore CLI hooks."""

from __future__ import annotations

import asyncio
import traceback
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from vuzol.ops.backup import restore_cli_hooks as hooks
from vuzol.ops.backup.restore_cli_hooks import (
    LockProbeFailed,
    LockProbeUnreachable,
    RestoreTargetNotEmpty,
    assert_restore_target_empty,
    make_assert_empty_target,
    make_b34_lock_always_true,
    probe_capture_lock_session,
)

_PROBE_DSN = (
    "postgresql+asyncpg://probe_user:s3cret@127.0.0.1:5432/vuzol"  # pragma: allowlist secret
)
_RESTORE_DSN = (
    "postgresql+asyncpg://restore_user:other@127.0.0.1:5432/restore"  # pragma: allowlist secret
)


def _engine_with_connection(connection: Any) -> MagicMock:
    engine = MagicMock()
    engine.connect = AsyncMock(return_value=connection)
    engine.dispose = AsyncMock()
    connection.close = AsyncMock()
    return engine


@pytest.mark.anyio
async def test_busy_does_not_unlock() -> None:
    connection = MagicMock()
    engine = _engine_with_connection(connection)
    with (
        patch.object(hooks, "create_async_engine", return_value=engine),
        patch.object(hooks, "try_advisory_lock", new_callable=AsyncMock, return_value=False),
        patch.object(hooks, "advisory_unlock", new_callable=AsyncMock) as unlock,
    ):
        assert await probe_capture_lock_session(_PROBE_DSN) == "busy"
    unlock.assert_not_called()
    connection.close.assert_awaited_once()
    engine.dispose.assert_awaited_once()


@pytest.mark.anyio
async def test_free_unlocks_before_return() -> None:
    connection = MagicMock()
    engine = _engine_with_connection(connection)
    with (
        patch.object(hooks, "create_async_engine", return_value=engine),
        patch.object(hooks, "try_advisory_lock", new_callable=AsyncMock, return_value=True),
        patch.object(hooks, "advisory_unlock", new_callable=AsyncMock) as unlock,
    ):
        assert await probe_capture_lock_session(_PROBE_DSN) == "free"
    unlock.assert_awaited_once()
    engine.dispose.assert_awaited_once()


@pytest.mark.anyio
async def test_unlock_failure_is_distinct_and_cleanup_retries() -> None:
    connection = MagicMock()
    engine = _engine_with_connection(connection)
    with (
        patch.object(hooks, "create_async_engine", return_value=engine),
        patch.object(hooks, "try_advisory_lock", new_callable=AsyncMock, return_value=True),
        patch.object(
            hooks,
            "advisory_unlock",
            new_callable=AsyncMock,
            side_effect=RuntimeError("sensitive detail"),
        ) as unlock,
        pytest.raises(LockProbeFailed) as caught,
    ):
        await probe_capture_lock_session(_PROBE_DSN)
    assert caught.value.code == "lock_probe_failed"
    assert str(caught.value) == "capture lock probe unlock failed"
    assert unlock.await_count == 2
    engine.dispose.assert_awaited_once()


@pytest.mark.anyio
@pytest.mark.parametrize("error", [OSError("down"), TimeoutError(), RuntimeError("detail")])
async def test_connect_failure_is_redacted_unreachable(error: Exception) -> None:
    engine = MagicMock()
    engine.connect = AsyncMock(side_effect=error)
    engine.dispose = AsyncMock()
    with (
        patch.object(hooks, "create_async_engine", return_value=engine),
        pytest.raises(LockProbeUnreachable) as caught,
    ):
        await probe_capture_lock_session(_PROBE_DSN)
    assert caught.value.code == "lock_probe_unreachable"
    assert str(caught.value) == "capture lock probe unreachable"
    engine.dispose.assert_awaited_once()


@pytest.mark.anyio
async def test_engine_factory_failure_is_redacted_unreachable() -> None:
    with (
        patch.object(hooks, "create_async_engine", side_effect=ValueError(_PROBE_DSN)),
        pytest.raises(LockProbeUnreachable) as caught,
    ):
        await probe_capture_lock_session(_PROBE_DSN)
    assert str(caught.value) == "capture lock probe unreachable"
    assert _PROBE_DSN not in "".join(traceback.format_exception(caught.value))


def test_always_true_hook_has_no_io() -> None:
    with (
        patch.object(hooks, "create_async_engine") as create_engine,
        patch.object(hooks, "try_advisory_lock", new_callable=AsyncMock) as try_lock,
    ):
        hook = make_b34_lock_always_true()
        assert hook() is True
    create_engine.assert_not_called()
    try_lock.assert_not_called()


@pytest.mark.anyio
async def test_nonempty_target_fails_closed() -> None:
    connection = MagicMock()
    result = MagicMock()
    result.first.return_value = (1,)
    connection.execute = AsyncMock(return_value=result)
    engine = _engine_with_connection(connection)
    with (
        patch.object(hooks, "create_async_engine", return_value=engine),
        pytest.raises(RestoreTargetNotEmpty) as caught,
    ):
        await assert_restore_target_empty(_RESTORE_DSN)
    assert caught.value.code == "preflight_target_not_empty"
    engine.dispose.assert_awaited_once()


@pytest.mark.anyio
async def test_empty_target_passes() -> None:
    connection = MagicMock()
    result = MagicMock()
    result.first.return_value = None
    connection.execute = AsyncMock(return_value=result)
    engine = _engine_with_connection(connection)
    with patch.object(hooks, "create_async_engine", return_value=engine):
        await assert_restore_target_empty(_RESTORE_DSN)
    engine.dispose.assert_awaited_once()


@pytest.mark.anyio
async def test_empty_target_probe_failure_is_redacted_and_closed() -> None:
    engine = MagicMock()
    engine.connect = AsyncMock(side_effect=OSError("sensitive"))
    engine.dispose = AsyncMock()
    with (
        patch.object(hooks, "create_async_engine", return_value=engine),
        pytest.raises(RestoreTargetNotEmpty) as caught,
    ):
        await assert_restore_target_empty(_RESTORE_DSN)
    assert str(caught.value) == "restore target is not empty"
    engine.dispose.assert_awaited_once()


@pytest.mark.anyio
async def test_empty_target_engine_factory_failure_is_redacted_and_closed() -> None:
    with (
        patch.object(hooks, "create_async_engine", side_effect=ValueError(_RESTORE_DSN)),
        pytest.raises(RestoreTargetNotEmpty) as caught,
    ):
        await assert_restore_target_empty(_RESTORE_DSN)
    assert str(caught.value) == "restore target is not empty"
    assert _RESTORE_DSN not in "".join(traceback.format_exception(caught.value))


@pytest.mark.anyio
async def test_cancellation_during_try_lock_still_closes_and_disposes() -> None:
    connection = MagicMock()
    engine = _engine_with_connection(connection)
    with (
        patch.object(hooks, "create_async_engine", return_value=engine),
        patch.object(
            hooks,
            "try_advisory_lock",
            new_callable=AsyncMock,
            side_effect=asyncio.CancelledError,
        ),
        patch.object(hooks, "advisory_unlock", new_callable=AsyncMock) as unlock,
        pytest.raises(asyncio.CancelledError),
    ):
        await probe_capture_lock_session(_PROBE_DSN)
    unlock.assert_not_called()
    connection.close.assert_awaited_once()
    engine.dispose.assert_awaited_once()


@pytest.mark.anyio
async def test_cancellation_during_unlock_retries_cleanup_then_propagates() -> None:
    connection = MagicMock()
    engine = _engine_with_connection(connection)
    with (
        patch.object(hooks, "create_async_engine", return_value=engine),
        patch.object(hooks, "try_advisory_lock", new_callable=AsyncMock, return_value=True),
        patch.object(
            hooks,
            "advisory_unlock",
            new_callable=AsyncMock,
            side_effect=(asyncio.CancelledError(), None),
        ) as unlock,
        pytest.raises(asyncio.CancelledError),
    ):
        await probe_capture_lock_session(_PROBE_DSN)
    assert unlock.await_count == 2
    connection.close.assert_awaited_once()
    engine.dispose.assert_awaited_once()


@pytest.mark.anyio
async def test_cancellation_during_empty_query_still_closes_and_disposes() -> None:
    connection = MagicMock()
    connection.execute = AsyncMock(side_effect=asyncio.CancelledError)
    engine = _engine_with_connection(connection)
    with (
        patch.object(hooks, "create_async_engine", return_value=engine),
        pytest.raises(asyncio.CancelledError),
    ):
        await assert_restore_target_empty(_RESTORE_DSN)
    connection.close.assert_awaited_once()
    engine.dispose.assert_awaited_once()


@pytest.mark.anyio
async def test_cleanup_cancellation_still_disposes_then_propagates() -> None:
    connection = MagicMock()
    result = MagicMock()
    result.first.return_value = None
    connection.execute = AsyncMock(return_value=result)
    engine = _engine_with_connection(connection)
    connection.close = AsyncMock(side_effect=asyncio.CancelledError)
    with (
        patch.object(hooks, "create_async_engine", return_value=engine),
        pytest.raises(asyncio.CancelledError),
    ):
        await assert_restore_target_empty(_RESTORE_DSN)
    connection.close.assert_awaited_once()
    engine.dispose.assert_awaited_once()


@pytest.mark.anyio
async def test_empty_target_factory_binds_dsn() -> None:
    with patch.object(hooks, "assert_restore_target_empty", new_callable=AsyncMock) as check:
        await make_assert_empty_target(_RESTORE_DSN)()
    check.assert_awaited_once_with(_RESTORE_DSN)


def test_no_raising_product_probe_factory() -> None:
    assert not hasattr(hooks, "make_b34_probe_capture_lock")
