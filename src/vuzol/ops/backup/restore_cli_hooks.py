"""Capture-lock and empty-target hooks for the default-off restore CLI."""

from __future__ import annotations

import sys
from collections.abc import Awaitable, Callable
from typing import Literal

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine

from vuzol.ops.backup.locks import (
    BACKUP_CAPTURE_LOCK_KEY,
    advisory_unlock,
    try_advisory_lock,
)

LockProbeOutcome = Literal["free", "busy"]

_MSG_UNREACHABLE = "capture lock probe unreachable"
_MSG_UNLOCK_FAILED = "capture lock probe unlock failed"
_MSG_TARGET_NOT_EMPTY = "restore target is not empty"

_EMPTY_RELATIONS_SQL = text(
    """
    SELECT 1
    FROM pg_catalog.pg_class c
    JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relkind IN ('r', 'p', 'v', 'm', 'S')
      AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
      AND n.nspname NOT LIKE 'pg_temp%'
      AND n.nspname NOT LIKE 'pg_toast_temp%'
    LIMIT 1
    """
)


class RestoreCliHookError(Exception):
    """Base hook error with a stable code and redacted message."""

    code = "restore_cli_hook_error"


class LockProbeUnreachable(RestoreCliHookError):
    code = "lock_probe_unreachable"

    def __init__(self, message: str = _MSG_UNREACHABLE) -> None:
        super().__init__(message)


class LockProbeFailed(RestoreCliHookError):
    code = "lock_probe_failed"

    def __init__(self, message: str = _MSG_UNLOCK_FAILED) -> None:
        super().__init__(message)


class RestoreTargetNotEmpty(RestoreCliHookError):
    code = "preflight_target_not_empty"

    def __init__(self, message: str = _MSG_TARGET_NOT_EMPTY) -> None:
        super().__init__(message)


async def _cleanup_resources(
    engine: AsyncEngine,
    connection: AsyncConnection | None,
    *,
    acquired: bool = False,
    lock_key: int = BACKUP_CAPTURE_LOCK_KEY,
) -> BaseException | None:
    """Attempt every cleanup step and retain cleanup-time cancellation."""

    cancellation: BaseException | None = None

    async def attempt(operation: Awaitable[object]) -> None:
        nonlocal cancellation
        try:
            await operation
        except BaseException as error:
            if not isinstance(error, Exception) and cancellation is None:
                cancellation = error

    if acquired and connection is not None:
        await attempt(advisory_unlock(connection, lock_key))
    if connection is not None:
        await attempt(connection.close())
    await attempt(engine.dispose())
    return cancellation


async def probe_capture_lock_session(
    probe_dsn: str,
    *,
    lock_key: int = BACKUP_CAPTURE_LOCK_KEY,
) -> LockProbeOutcome:
    """Probe and immediately release the capture lock; never label outages busy."""

    try:
        engine = create_async_engine(probe_dsn, pool_pre_ping=True)
    except Exception:
        raise LockProbeUnreachable() from None
    acquired = False
    connection = None
    try:
        try:
            connection = await engine.connect()
            acquired = await try_advisory_lock(connection, lock_key)
            if not acquired:
                return "busy"
            try:
                await advisory_unlock(connection, lock_key)
                acquired = False
            except Exception:
                raise LockProbeFailed() from None
            return "free"
        except LockProbeFailed:
            raise
        except (TimeoutError, OSError, SQLAlchemyError):
            raise LockProbeUnreachable() from None
        except Exception:
            raise LockProbeUnreachable() from None
    finally:
        active_error = sys.exception()
        cleanup_cancellation = await _cleanup_resources(
            engine,
            connection,
            acquired=acquired,
            lock_key=lock_key,
        )
        if active_error is None and cleanup_cancellation is not None:
            raise cleanup_cancellation


def make_b34_lock_always_true() -> Callable[[], bool]:
    """Return a no-I/O singleton-True hook after a successful CLI-side probe."""

    def _always_true() -> bool:
        return True

    return _always_true


async def assert_restore_target_empty(restore_dsn: str) -> None:
    """Fail closed if the restore target has any non-system relation."""

    try:
        engine = create_async_engine(restore_dsn, pool_pre_ping=True)
    except Exception:
        raise RestoreTargetNotEmpty() from None
    connection = None
    try:
        try:
            connection = await engine.connect()
            result = await connection.execute(_EMPTY_RELATIONS_SQL)
            if result.first() is not None:
                raise RestoreTargetNotEmpty()
        except RestoreTargetNotEmpty:
            raise
        except Exception:
            raise RestoreTargetNotEmpty() from None
    finally:
        active_error = sys.exception()
        cleanup_cancellation = await _cleanup_resources(engine, connection)
        if active_error is None and cleanup_cancellation is not None:
            raise cleanup_cancellation


def make_assert_empty_target(restore_dsn: str) -> Callable[[], Awaitable[None]]:
    """Bind the restore DSN into the orchestrator's zero-argument hook."""

    async def _assert_empty() -> None:
        await assert_restore_target_empty(restore_dsn)

    return _assert_empty
