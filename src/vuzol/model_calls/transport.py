"""Rootless CLI inference bound to a leased model call, with no workspace at all.

The lead's turn transport is the model: the lease is rechecked before launch and
watched while the process runs, the account slot must already be held, and only
the frozen request may reach stdin.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import uuid
from datetime import UTC, datetime
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession, async_sessionmaker

from vuzol.config.models import LaunchMode, SandboxNetworkMode
from vuzol.config.registries import ConfigurationBundle
from vuzol.config.settings import Settings
from vuzol.execution.codex import _provider_state_runtime, _require_provider_command
from vuzol.execution.domain import (
    MountMode,
    ProcessEnvelope,
    SandboxMount,
    SandboxSpec,
    TurnProcessBinding,
)
from vuzol.execution.egress import AllowedConnectTarget, compile_proxy_allowlist
from vuzol.execution.ports import SandboxRuntime
from vuzol.execution.proxy_service import ProxyServiceManager
from vuzol.execution.turn_transport import (
    TURN_DB_TIMEOUT_SECONDS,
    cleanup_turn_runtime,
    finish_cleanup,
    turn_runtime_lock,
)
from vuzol.model_calls.queue import ModelCallClaim, call_current, current_model_call
from vuzol.model_calls.request import attempt_id, provider_request
from vuzol.providers.accounts import current_account_execution
from vuzol.providers.ports import CodexInvocation, CodexProcessResult
from vuzol.storage.errors import LeaseLost
from vuzol.storage.models import ModelCall, SupervisedProcess
from vuzol.storage.types import ProcessOutcome, ProcessStatus, TerminationStage
from vuzol.workflows.ports import CancellationContext


class ModelCallSandboxTransport:
    def __init__(
        self,
        runtime: SandboxRuntime,
        factory: async_sessionmaker[AsyncSession],
        settings: Settings,
        registries: ConfigurationBundle,
        proxy_manager: ProxyServiceManager | None = None,
    ) -> None:
        self._runtime = runtime
        self._factory = factory
        self._settings = settings
        self._registries = registries
        self._proxy = proxy_manager

    async def run(
        self, invocation: CodexInvocation, cancellation: CancellationContext
    ) -> CodexProcessResult:
        claim = current_model_call.get()
        if (
            claim is None
            or cancellation.requested
            or invocation.sandbox_reference != claim.sandbox_reference
            or invocation.lease_generation != claim.generation
            or invocation.task_id != attempt_id(claim)
            or invocation.run_id != invocation.task_id
            or invocation.step_id != invocation.task_id
            or invocation.provider_attempt != 1
            or invocation.profile_id is None
        ):
            raise ValueError("CLI inference lacks a current model call binding")
        async with turn_runtime_lock(self._factory, invocation.task_id) as connection:
            if connection is None:
                raise ValueError("model call runtime is already owned")
            return await self._run_owned(invocation, cancellation, claim, connection)

    async def _run_owned(
        self,
        invocation: CodexInvocation,
        cancellation: CancellationContext,
        claim: ModelCallClaim,
        connection: AsyncConnection,
    ) -> CodexProcessResult:
        from vuzol.providers.errors import ProviderNotStartedError

        try:
            envelope, targets, process_id = await self._prepare(invocation, claim)
        except (ValueError, LookupError) as error:
            raise ProviderNotStartedError(str(error)) from error
        binding = envelope.turn_binding
        assert binding is not None
        result: CodexProcessResult | None = None
        try:
            assert self._proxy is not None
            lease = await self._proxy.create(
                None, None, None, envelope.lease_generation, targets, turn_binding=binding
            )
            envelope = envelope.model_copy(
                update={
                    "sandbox": envelope.sandbox.model_copy(
                        update={
                            "proxy_network": lease.networks.internal_name,
                            "https_proxy_url": lease.proxy_url,
                        }
                    )
                }
            )
            async with self._factory.begin() as session:
                if cancellation.requested or not await call_current(session, claim):
                    raise LeaseLost("model call was fenced before sandbox launch")
                process = await session.get(SupervisedProcess, process_id, with_for_update=True)
                assert process is not None
                process.status = ProcessStatus.RUNNING
                process.started_at = datetime.now(UTC)
                process.heartbeat_at = process.started_at
                process.command_envelope_hash = envelope.stable_hash
                process.command_envelope = envelope.redacted
                process.sandbox_spec_hash = envelope.sandbox.stable_hash
                process.runtime_metadata = {
                    **process.runtime_metadata,
                    "runtime_envelope": envelope.model_dump(mode="json", exclude={"stdin"}),
                }
            run = asyncio.create_task(self._runtime.run(envelope, cancellation))
            guard = asyncio.create_task(self._watch(claim, cancellation, connection))
            proxy_watch = asyncio.create_task(self._proxy.wait_until_dead(lease))
            tasks = {run, guard, proxy_watch}

            async def drain_tasks() -> None:
                for task in tasks:
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)

            try:
                done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                if run in done:
                    result = await run
                    return result
                if guard in done:
                    await guard
                if proxy_watch in done:
                    await proxy_watch
                    raise RuntimeError("controlled proxy exited during model call")
                result = await run
                return result
            finally:
                await finish_cleanup(drain_tasks())
        finally:
            await finish_cleanup(self._finish(envelope, process_id, result))

    async def _prepare(
        self, invocation: CodexInvocation, claim: ModelCallClaim
    ) -> tuple[ProcessEnvelope, tuple[AllowedConnectTarget, ...], uuid.UUID]:
        async with self._factory.begin() as session:
            if not await call_current(session, claim):
                raise LeaseLost("model call claim is stale")
            call = await session.get(ModelCall, claim.call_id)
            assert call is not None
            profile = self._registries.profiles.get(call.profile_id)
            if (
                profile.id != invocation.profile_id
                or not profile.enabled
                or profile.launch_mode is not LaunchMode.CLI
                or not profile.sandbox_required
                or profile.state_directory is None
                or profile.runtime_identity != invocation.runtime_identity
                or str(profile.state_directory) != invocation.state_directory
            ):
                raise ValueError("model call profile does not match its call")
            _require_provider_command(
                invocation.argv,
                profile.provider,
                profile.model,
                reasoning_effort=profile.model_reasoning_effort,
                require_read_only=True,
                require_prompt_only=profile.provider == "pi",
                web_search=bool(call.request.get("web_search")),
            )
            # The adapter must forward the frozen call, not a different prompt.
            request = provider_request(call, claim)
            expected = {
                "schema_version": request.schema_version,
                "original_input": request.original_input,
                "task_draft": request.task_draft,
                "context": [item.model_dump(mode="json") for item in request.context],
                "role": request.role.value,
                "output_schema": request.output_json_schema,
            }
            body = json.loads(invocation.stdin)
            if (
                not isinstance(body, dict)
                or set(body) != {*expected, "execution_policy"}
                or any(body.get(key) != value for key, value in expected.items())
                or invocation.timeout_seconds != call.timeout_seconds
            ):
                raise ValueError("model call CLI input does not match the frozen call")
            sandbox_id = self._settings.execution.model_call_sandbox_profile
            if sandbox_id is None:
                raise ValueError("model calls have no sandbox profile")
            sandbox = self._registries.sandboxes.get(sandbox_id)
            targets = compile_proxy_allowlist(profile.runtime_network)
            if (
                not sandbox.enabled
                or sandbox.network_mode is not SandboxNetworkMode.HTTPS_PROXY
                or not targets
                or self._proxy is None
            ):
                raise ValueError("model call requires a proxied sandbox")
            seccomp = self._settings.execution.sandbox_seccomp_profile
            digest = self._settings.execution.sandbox_seccomp_profile_sha256
            if seccomp is None or digest is None:
                raise ValueError("model call requires pinned seccomp policy")
            state_target, environment = _provider_state_runtime(profile.provider)
            binding = TurnProcessBinding(turn_id=call.id, invocation_id=attempt_id(claim))
            spec = SandboxSpec(
                image=sandbox.image,
                uid=sandbox.uid,
                gid=sandbox.gid,
                seccomp_profile=seccomp,
                seccomp_profile_sha256=digest,
                working_directory=Path("/workspace"),
                empty_workspace=True,
                mounts=(
                    SandboxMount(
                        source=profile.state_directory.resolve(strict=True),
                        target=state_target,
                        mode=MountMode.READ_ONLY
                        if profile.provider == "pi"
                        else MountMode.READ_WRITE,
                        purpose="provider-state",
                    ),
                ),
                cpu_count=sandbox.cpu_count,
                memory_bytes=sandbox.memory_bytes,
                pids_limit=sandbox.pids_limit,
                tmpfs_bytes=sandbox.tmpfs_bytes,
                open_files_limit=sandbox.open_files_limit,
                output_bytes=sandbox.output_bytes,
                timeout_seconds=max(1, min(sandbox.timeout_seconds, call.timeout_seconds)),
                stop_grace_seconds=sandbox.stop_grace_seconds,
                environment=environment,
                network_disabled=False,
                proxy_network=binding.resource_name(claim.generation, "internal"),
                https_proxy_url="http://vuzol-proxy:8888",
            )
            envelope = ProcessEnvelope(
                turn_binding=binding,
                profile_id=profile.id,
                provider_attempt=1,
                lease_generation=claim.generation,
                argv=invocation.argv,
                stdin=invocation.stdin,
                sandbox=spec,
            )
            if await session.scalar(
                select(SupervisedProcess.id).where(
                    SupervisedProcess.invocation_id == binding.invocation_id
                )
            ):
                raise ValueError("model call process already exists; reconciliation required")
            account = current_account_execution()
            if profile.account_id is not None and (
                account is None or account.account_id != profile.account_id
            ):
                raise ValueError("model call requires account admission")
            process = SupervisedProcess(
                model_call_id=call.id,
                invocation_id=binding.invocation_id,
                profile_id=profile.id,
                provider_attempt=1,
                lease_generation=claim.generation,
                idempotency_key=hashlib.sha256(
                    f"model-call:{binding.invocation_id}".encode()
                ).hexdigest(),
                command_envelope_hash=envelope.stable_hash,
                command_envelope=envelope.redacted,
                sandbox_spec_hash=spec.stable_hash,
                container_runtime="rootless-docker",
                container_id=envelope.container_name,
                image_digest=spec.image,
                working_directory="/workspace",
                status=ProcessStatus.STARTING,
                termination_stage=TerminationStage.NONE,
                runtime_metadata={
                    **({} if account is None else account.runtime_metadata),
                    "executor_model": profile.model,
                    "executor_reasoning_effort": profile.model_reasoning_effort,
                    "model_call": {
                        "call_id": str(claim.call_id),
                        "owner": claim.owner,
                        "generation": claim.generation,
                    },
                    "runtime_envelope": envelope.model_dump(mode="json", exclude={"stdin"}),
                    "proxy_required": True,
                },
            )
            session.add(process)
            await session.flush()
            return envelope, targets, process.id

    async def _watch(
        self,
        claim: ModelCallClaim,
        cancellation: CancellationContext,
        connection: AsyncConnection,
    ) -> None:
        while True:
            await asyncio.sleep(0.25)
            try:
                async with asyncio.timeout(TURN_DB_TIMEOUT_SECONDS):
                    if connection.invalidated:
                        raise LeaseLost("model call runtime ownership connection was lost")
                    await connection.execute(select(1))
                    await connection.commit()
                    async with self._factory.begin() as session:
                        if cancellation.requested or not await call_current(
                            session, claim, lock=False
                        ):
                            raise LeaseLost("model call authority was revoked")
            except Exception:
                cancellation.request()
                raise

    async def _finish(
        self, envelope: ProcessEnvelope, process_id: uuid.UUID, result: CodexProcessResult | None
    ) -> None:
        await settle_model_call_process(
            self._factory, self._runtime, self._proxy, envelope, process_id, result
        )


async def settle_model_call_process(
    factory: async_sessionmaker[AsyncSession],
    runtime: SandboxRuntime,
    proxy: ProxyServiceManager | None,
    envelope: ProcessEnvelope,
    process_id: uuid.UUID,
    result: CodexProcessResult | None,
) -> None:
    """Remove the sandbox and its proxy, then record the process as reaped."""
    try:
        await cleanup_turn_runtime(runtime, proxy, envelope)
    except BaseException:
        async with asyncio.timeout(TURN_DB_TIMEOUT_SECONDS), factory.begin() as session:
            process = await session.get(SupervisedProcess, process_id, with_for_update=True)
            assert process is not None
            process.status = ProcessStatus.UNKNOWN
            process.outcome = ProcessOutcome.UNKNOWN
            process.runtime_metadata = {
                **process.runtime_metadata,
                "cleanup_confirmed": False,
                "launcher_drained": True,
            }
        raise
    async with asyncio.timeout(TURN_DB_TIMEOUT_SECONDS), factory.begin() as session:
        process = await session.get(SupervisedProcess, process_id, with_for_update=True)
        assert process is not None
        process.status = ProcessStatus.EXITED
        process.outcome = (
            ProcessOutcome.UNKNOWN
            if result is None
            else ProcessOutcome.SUCCEEDED
            if result.exit_code == 0
            else ProcessOutcome.FAILED
        )
        process.exit_code = None if result is None else result.exit_code
        process.ended_at = datetime.now(UTC)
        process.reaped_at = process.ended_at
        process.runtime_metadata = {
            **process.runtime_metadata,
            "cleanup_confirmed": True,
            "launcher_drained": True,
        }
