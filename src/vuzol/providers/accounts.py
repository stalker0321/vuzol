"""Account-wide admission around the actual provider call.

Session advisory locks plus a committed durable ticket require a dedicated
PostgreSQL connection for the entire call (no transaction-pooling proxy). CLI
transports must persist the current
account execution's runtime_metadata on SupervisedProcess BEFORE spawning, and
only mark EXITED/reaped once runtime cleanup is confirmed. This durable slot
reservation survives a worker crash or a dropped advisory-lock connection.
"""

from __future__ import annotations

import asyncio
import hashlib
import uuid
from contextvars import ContextVar
from dataclasses import dataclass

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession, async_sessionmaker

from vuzol.config.models import LaunchMode, ProviderProfileConfig
from vuzol.providers.account_persistence import (
    account_slot_key as account_slot_key,
)
from vuzol.providers.account_persistence import (
    account_slot_reserved,
    clear_account_ticket,
    create_account_ticket,
    finish_account_cleanup,
    unlock_account_slot,
)
from vuzol.providers.domain import (
    EffectiveProfileState,
    ProviderErrorCategory,
    ProviderRequest,
    ProviderResult,
)
from vuzol.providers.errors import ProviderFailure
from vuzol.providers.ports import ProviderAdapter
from vuzol.workflows.ports import CancellationContext

ACCOUNT_LOCK_POLL_SECONDS = 1.0
ACCOUNT_LOCK_QUERY_TIMEOUT_SECONDS = 5.0


@dataclass(frozen=True, slots=True)
class AccountExecution:
    account_id: str
    slot: int
    token: uuid.UUID

    @property
    def runtime_metadata(self) -> dict[str, str | int]:
        return {
            "account_id": self.account_id,
            "account_slot": self.slot,
            "account_lease_token": str(self.token),
        }


_execution: ContextVar[AccountExecution | None] = ContextVar("account_execution", default=None)


def current_account_execution() -> AccountExecution | None:
    """Transport handoff; inherited by the adapter's execution task."""
    return _execution.get()


def effective_account_id(profile: ProviderProfileConfig) -> str:
    """Legacy isolated profiles still need one durable slot across all their roles."""
    if profile.account_id is not None:
        return profile.account_id
    if profile.launch_mode is not LaunchMode.CLI:
        raise ValueError("API account admission requires explicit account_id")
    return "profile:" + hashlib.sha256(profile.id.encode()).hexdigest()


class AccountBoundAdapter:
    def __init__(
        self,
        adapter: ProviderAdapter,
        profile: ProviderProfileConfig,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        account_profile_ids: tuple[str, ...],
    ) -> None:
        self._adapter = adapter
        self._profile = profile
        self._factory = session_factory
        self._account_id = effective_account_id(profile)
        self._account_profile_ids = tuple(set((*account_profile_ids, profile.id)))

    def _validate_profile(self, profile: ProviderProfileConfig) -> None:
        if (
            profile.id != self._profile.id
            or profile.account_binding != self._profile.account_binding
        ):
            raise ValueError("provider profile account binding changed")

    async def health(self, profile: ProviderProfileConfig) -> EffectiveProfileState:
        self._validate_profile(profile)
        return await self._adapter.health(profile)

    async def execute(
        self,
        request: ProviderRequest,
        profile: ProviderProfileConfig,
        cancellation: CancellationContext,
    ) -> ProviderResult:
        self._validate_profile(profile)
        self._check_cancellation(cancellation)
        async with self._factory() as session:
            connection = await session.connection()
            try:
                execution = await self._claim(connection, request)
            except ProviderFailure:
                raise
            except BaseException:
                # Acquisition may have succeeded on the server before the
                # client was cancelled or lost its response. Never pool it.
                await finish_account_cleanup(connection.invalidate())
                raise
            primary: BaseException | None = None
            result: ProviderResult | None = None
            try:
                self._check_cancellation(cancellation)
                # Pin the backend identity before starting any provider work.
                backend_pid = await connection.scalar(text("SELECT pg_backend_pid()"))
                self._check_cancellation(cancellation)
                token = _execution.set(execution)
                try:
                    try:
                        result = await self._execute_guarded(
                            connection, backend_pid, request, profile, cancellation
                        )
                    except ProviderFailure as failure:
                        if not failure.request_sent or failure.execution_completed:
                            await self._release_if_confirmed(
                                connection, backend_pid, execution, cancellation
                            )
                        raise
                    if not await self._release_if_confirmed(
                        connection, backend_pid, execution, cancellation
                    ):
                        raise ProviderFailure(
                            ProviderErrorCategory.PROVIDER_UNAVAILABLE,
                            retryable=False,
                            request_sent=True,
                            safe_summary="provider account cleanup unresolved; ticket retained",
                            usage=result.usage,
                            provider_request_id=result.provider_request_id,
                        )
                    return result
                finally:
                    _execution.reset(token)
            except BaseException as error:
                primary = error
                raise
            finally:
                try:
                    await finish_account_cleanup(
                        unlock_account_slot(connection, self._account_id, execution.slot)
                    )
                except Exception:
                    if primary is None:
                        if result is not None:
                            raise ProviderFailure(
                                ProviderErrorCategory.PROVIDER_UNAVAILABLE,
                                retryable=False,
                                request_sent=True,
                                safe_summary="provider account unlock failed after receipt",
                                usage=result.usage,
                                provider_request_id=result.provider_request_id,
                            ) from None
                        raise

    async def _release_if_confirmed(
        self,
        connection: AsyncConnection,
        backend_pid: object,
        execution: AccountExecution,
        cancellation: CancellationContext,
    ) -> bool:
        try:
            if cancellation.requested or await self._slot_reserved(connection, execution.slot):
                return False
            await self._assert_connection(connection, backend_pid)
            return await clear_account_ticket(
                connection,
                account_id=self._account_id,
                slot=execution.slot,
                token=execution.token,
            )
        except Exception:
            # Loss/ambiguous deletion cannot certify release. Retain the known
            # provider receipt; disconnect the lock connection, never pool it.
            await finish_account_cleanup(connection.invalidate())
            return False

    @staticmethod
    def _check_cancellation(cancellation: CancellationContext) -> None:
        if cancellation.requested:
            raise ProviderFailure(
                ProviderErrorCategory.CANCELLED,
                retryable=False,
                request_sent=False,
                safe_summary="account call cancelled before launch",
            )

    async def _claim(
        self, connection: AsyncConnection, request: ProviderRequest
    ) -> AccountExecution:
        for slot in range(self._profile.concurrency_limit):
            acquired = await connection.scalar(
                text("SELECT pg_try_advisory_lock(:key)"),
                {"key": account_slot_key(self._account_id, slot)},
            )
            if acquired is not True:
                continue
            execution = AccountExecution(self._account_id, slot, uuid.uuid4())
            if not await self._slot_reserved(connection, slot) and await create_account_ticket(
                connection,
                account_id=self._account_id,
                slot=slot,
                token=execution.token,
                profile_id=self._profile.id,
                request=request,
                binding_kind=self._profile.launch_mode.value,
            ):
                return execution
            await unlock_account_slot(connection, self._account_id, slot)
        raise ProviderFailure(
            ProviderErrorCategory.PROVIDER_UNAVAILABLE,
            retryable=True,
            request_sent=False,
            safe_summary="selected provider account is busy or has an unresolved process/ticket",
        )

    async def _slot_reserved(self, connection: AsyncConnection, slot: int) -> bool:
        if self._profile.launch_mode is not LaunchMode.CLI:
            return False
        return await account_slot_reserved(
            connection,
            account_id=self._account_id,
            slot=slot,
            account_profile_ids=self._account_profile_ids,
            concurrency_limit=self._profile.concurrency_limit,
        )

    async def _monitor(self, connection: AsyncConnection, backend_pid: object) -> None:
        while True:
            await asyncio.sleep(ACCOUNT_LOCK_POLL_SECONDS)
            async with asyncio.timeout(ACCOUNT_LOCK_QUERY_TIMEOUT_SECONDS):
                await self._assert_connection(connection, backend_pid)

    @staticmethod
    async def _assert_connection(connection: AsyncConnection, backend_pid: object) -> None:
        if connection.invalidated:
            raise RuntimeError("provider account lock connection was invalidated")
        observed = await connection.scalar(text("SELECT pg_backend_pid()"))
        if observed != backend_pid:
            raise RuntimeError("provider account lock connection changed")

    async def _execute_guarded(
        self,
        connection: AsyncConnection,
        backend_pid: object,
        request: ProviderRequest,
        profile: ProviderProfileConfig,
        cancellation: CancellationContext,
    ) -> ProviderResult:
        call = asyncio.create_task(self._adapter.execute(request, profile, cancellation))
        monitor = asyncio.create_task(self._monitor(connection, backend_pid))

        async def drain() -> None:
            monitor.cancel()
            if not call.done():
                cancellation.request()
                call.cancel()
            await asyncio.gather(call, monitor, return_exceptions=True)

        try:
            done, _ = await asyncio.wait((call, monitor), return_when=asyncio.FIRST_COMPLETED)
            if call in done:
                return call.result()
            if monitor in done:
                # The request may have reached the provider. Do not classify
                # this as a safe pre-send retry; durable CLI rows stay reserved.
                raise ProviderFailure(
                    ProviderErrorCategory.PROVIDER_UNAVAILABLE,
                    retryable=False,
                    request_sent=True,
                    safe_summary="provider account lock connection lost; execution stopped",
                )
            return call.result()
        finally:
            await finish_account_cleanup(drain())
