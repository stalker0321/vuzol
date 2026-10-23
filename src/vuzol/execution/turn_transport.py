"""Rootless CLI inference bound to a trusted leadership turn, never fake tasks."""

from __future__ import annotations

import asyncio
import hashlib
import json
import uuid
from collections.abc import AsyncIterator, Coroutine
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession, async_sessionmaker

from vuzol.config.models import LaunchMode, ProviderRole, SandboxNetworkMode
from vuzol.config.registries import ConfigurationBundle
from vuzol.config.revision import content_revision
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
from vuzol.execution.repo_snapshot import build_snapshot, cleanup_snapshot, snapshot_path
from vuzol.leadership.invocation_context import (
    TurnInvocationContext,
    current_turn,
    invocation_current,
)
from vuzol.leadership.persistence import LeadershipInference
from vuzol.providers.accounts import current_account_execution
from vuzol.providers.ports import CodexInvocation, CodexProcessResult
from vuzol.storage.errors import LeaseLost
from vuzol.storage.models import SupervisedProcess
from vuzol.storage.types import ProcessOutcome, ProcessStatus, TerminationStage
from vuzol.workflows.ports import CancellationContext

TURN_DB_TIMEOUT_SECONDS = 5.0


class TurnSandboxTransport:
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
        context = current_turn.get()
        if (
            context is None
            or cancellation.requested
            or invocation.sandbox_reference != f"lead-turn:{context.turn_id}"
            or invocation.lease_generation != context.claim.lease_epoch
            or invocation.task_id is None
            or invocation.run_id != invocation.task_id
            or invocation.step_id != invocation.task_id
            or invocation.provider_attempt != 1
            or invocation.profile_id is None
        ):
            raise ValueError("CLI inference lacks a current trusted turn binding")
        async with turn_runtime_lock(self._factory, invocation.task_id) as connection:
            if connection is None:
                raise ValueError("turn inference runtime is already owned")
            return await self._run_owned(invocation, cancellation, context, connection)

    async def _run_owned(
        self,
        invocation: CodexInvocation,
        cancellation: CancellationContext,
        context: TurnInvocationContext,
        connection: AsyncConnection,
    ) -> CodexProcessResult:
        from vuzol.providers.errors import ProviderNotStartedError

        try:
            envelope, targets, process_id = await self._prepare(invocation, context)
        except (ValueError, LookupError) as error:
            raise ProviderNotStartedError(str(error)) from error
        binding = envelope.turn_binding
        assert binding is not None
        result: CodexProcessResult | None = None
        try:
            if not envelope.sandbox.empty_workspace:
                async with self._factory() as session:
                    phase = await session.get(LeadershipInference, binding.invocation_id)
                    assert phase is not None
                    commit = phase.request["task_draft"]["source_repo_commit"]
                project = self._registries.projects.get(context.project_id)
                await build_snapshot(project.repository_path, commit, binding.invocation_id)
            if targets:
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
            else:
                lease = None
            async with self._factory.begin() as session:
                if cancellation.requested or not await invocation_current(session, context):
                    raise LeaseLost("turn was fenced before sandbox launch")
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
            guard = asyncio.create_task(self._watch(context, cancellation, connection))
            proxy_watch = None
            if lease is not None:
                assert self._proxy is not None
                proxy_watch = asyncio.create_task(self._proxy.wait_until_dead(lease))
            tasks = {run, guard} | ({proxy_watch} if proxy_watch is not None else set())

            async def drain_tasks() -> None:
                for task in tasks:
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)

            try:
                done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                # Preserve an already-complete receipt even if revocation was
                # observed in the same scheduler tick. The command/application
                # fence remains the caller's responsibility after settlement.
                if run in done:
                    result = await run
                    return result
                if guard in done:
                    await guard
                if proxy_watch is not None and proxy_watch in done:
                    await proxy_watch
                    raise RuntimeError("controlled proxy exited during turn inference")
                result = await run
                return result
            finally:
                # A second cancellation must not interrupt joining the runtime
                # and then incorrectly attest launcher_drained in _finish.
                await finish_cleanup(drain_tasks())
        finally:
            # Cleanup is deliberately independent of the now-stale goal claim.
            # This path cannot run until runtime/guard tasks are joined (or no
            # runtime was started). Only it may attest launcher_drained.
            await finish_cleanup(self._finish(envelope, process_id, result))

    async def _prepare(
        self, invocation: CodexInvocation, context: TurnInvocationContext
    ) -> tuple[ProcessEnvelope, tuple[AllowedConnectTarget, ...], uuid.UUID]:
        async with self._factory.begin() as session:
            if not await invocation_current(session, context):
                raise LeaseLost("turn inference claim is stale")
            phase = await session.get(LeadershipInference, invocation.task_id, with_for_update=True)
            if (
                phase is None
                or phase.turn_id != context.turn_id
                or phase.state != "sending"
                or phase.profile_id != invocation.profile_id
            ):
                raise ValueError("turn inference has no matching durable send intent")
            profile = self._registries.profiles.get(phase.profile_id)
            expected_role = {
                "lead": ProviderRole.LEAD,
                "goal_review": ProviderRole.REVIEWER,
                "planner": ProviderRole.PLANNER,
            }[phase.phase]
            if (
                not profile.enabled
                or profile.launch_mode is not LaunchMode.CLI
                or expected_role not in profile.roles
                or not profile.sandbox_required
                or content_revision(profile) != phase.profile_revision
                or profile.runtime_identity != invocation.runtime_identity
                or str(profile.state_directory) != invocation.state_directory
            ):
                raise ValueError("turn inference profile does not match its pinned assignment")
            _require_provider_command(
                invocation.argv,
                profile.provider,
                profile.model,
                reasoning_effort=profile.model_reasoning_effort,
                require_read_only=True,
                require_prompt_only=profile.provider == "pi" and phase.phase != "planner",
            )
            # Verify the adapter forwarded the frozen phase input, not a different prompt.
            body = json.loads(invocation.stdin)
            expected_body = {
                "schema_version": phase.request.get("schema_version"),
                "original_input": phase.request.get("original_input"),
                "task_draft": phase.request.get("task_draft"),
                "context": phase.request.get("context"),
                "role": expected_role.value,
                "output_schema": phase.request.get("output_json_schema"),
            }
            # All four canonical adapters share these fields. Their trusted
            # execution_policy differs (Grok uses an object, others a string).
            if (
                not isinstance(body, dict)
                or set(body) != {*expected_body, "execution_policy"}
                or any(body.get(key) != value for key, value in expected_body.items())
                or invocation.timeout_seconds != phase.request.get("timeout_seconds")
            ):
                raise ValueError("turn CLI input does not match durable inference input")
            project = self._registries.projects.get(context.project_id)
            sandbox = self._registries.sandboxes.get(project.sandbox_profile)
            if not project.enabled or not sandbox.enabled or profile.state_directory is None:
                raise ValueError("turn sandbox or project is unavailable")
            targets = compile_proxy_allowlist(profile.runtime_network)
            allowed = {
                (target.hostname, target.port)
                for target in compile_proxy_allowlist(project.network)
            }
            if any((target.hostname, target.port) not in allowed for target in targets):
                raise ValueError("turn CLI egress exceeds project policy")
            networked = sandbox.network_mode is SandboxNetworkMode.HTTPS_PROXY
            if networked and (not targets or self._proxy is None):
                raise ValueError("turn inference requires a controlled proxy")
            if not networked:
                targets = ()
            seccomp = self._settings.execution.sandbox_seccomp_profile
            digest = self._settings.execution.sandbox_seccomp_profile_sha256
            if seccomp is None or digest is None:
                raise ValueError("turn inference requires pinned seccomp policy")
            state_target, environment = _provider_state_runtime(profile.provider)
            state_path = profile.state_directory.resolve(strict=True)
            binding = TurnProcessBinding(turn_id=context.turn_id, invocation_id=phase.id)
            # Names are deterministic and already known before proxy materialization.
            spec = SandboxSpec(
                image=sandbox.image,
                uid=sandbox.uid,
                gid=sandbox.gid,
                seccomp_profile=seccomp,
                seccomp_profile_sha256=digest,
                working_directory=Path("/workspace"),
                empty_workspace=phase.phase != "planner",
                mounts=(
                    *(
                        (
                            SandboxMount(
                                source=snapshot_path(phase.id),
                                target=Path("/workspace"),
                                mode=MountMode.READ_ONLY,
                                purpose="planner-snapshot",
                            ),
                        )
                        if phase.phase == "planner"
                        else ()
                    ),
                    SandboxMount(
                        source=state_path,
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
                timeout_seconds=max(
                    1, min(sandbox.timeout_seconds, int(invocation.timeout_seconds))
                ),
                stop_grace_seconds=sandbox.stop_grace_seconds,
                environment=environment,
                network_disabled=not networked,
                proxy_network=binding.resource_name(context.claim.lease_epoch, "internal")
                if networked
                else None,
                https_proxy_url="http://vuzol-proxy:8888" if networked else None,
            )
            envelope = ProcessEnvelope(
                turn_binding=binding,
                profile_id=profile.id,
                provider_attempt=1,
                lease_generation=context.claim.lease_epoch,
                argv=invocation.argv,
                stdin=invocation.stdin,
                sandbox=spec,
            )
            if await session.scalar(
                select(SupervisedProcess.id).where(SupervisedProcess.invocation_id == phase.id)
            ):
                raise ValueError("turn inference process already exists; reconciliation required")
            account = current_account_execution()
            if profile.account_id is not None and (
                account is None or account.account_id != profile.account_id
            ):
                raise ValueError("turn inference requires account admission")
            metadata = {
                **({} if account is None else account.runtime_metadata),
                "executor_model": profile.model,
                "executor_reasoning_effort": profile.model_reasoning_effort,
                "turn_context": context_payload(context),
                "runtime_envelope": envelope.model_dump(mode="json", exclude={"stdin"}),
                "proxy_required": networked,
            }
            process = SupervisedProcess(
                leadership_turn_id=context.turn_id,
                invocation_id=phase.id,
                profile_id=profile.id,
                provider_attempt=1,
                lease_generation=context.claim.lease_epoch,
                idempotency_key=hashlib.sha256(f"lead-turn:{phase.id}".encode()).hexdigest(),
                command_envelope_hash=envelope.stable_hash,
                command_envelope=envelope.redacted,
                sandbox_spec_hash=spec.stable_hash,
                container_runtime="rootless-docker",
                container_id=envelope.container_name,
                image_digest=spec.image,
                working_directory="/workspace",
                status=ProcessStatus.STARTING,
                termination_stage=TerminationStage.NONE,
                runtime_metadata=metadata,
            )
            session.add(process)
            await session.flush()
            return envelope, targets, process.id

    async def _watch(
        self,
        context: TurnInvocationContext,
        cancellation: CancellationContext,
        connection: AsyncConnection,
    ) -> None:
        while True:
            await asyncio.sleep(0.25)
            # A lost ownership connection must stop the runtime, not silently
            # reconnect with no session advisory lock.
            try:
                async with asyncio.timeout(TURN_DB_TIMEOUT_SECONDS):
                    if connection.invalidated:
                        raise LeaseLost("turn runtime ownership connection was lost")
                    await connection.execute(text("SELECT 1"))
                    await connection.commit()
                    async with self._factory.begin() as session:
                        if cancellation.requested or not await invocation_current(session, context):
                            raise LeaseLost("turn inference authority was revoked")
            except Exception:
                cancellation.request()
                raise

    async def _finish(
        self, envelope: ProcessEnvelope, process_id: uuid.UUID, result: CodexProcessResult | None
    ) -> None:
        try:
            await cleanup_turn_runtime(self._runtime, self._proxy, envelope)
        except BaseException:
            async with asyncio.timeout(TURN_DB_TIMEOUT_SECONDS), self._factory.begin() as session:
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
        async with asyncio.timeout(TURN_DB_TIMEOUT_SECONDS), self._factory.begin() as session:
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


def context_payload(context: TurnInvocationContext) -> dict[str, str | int]:
    return {
        "goal_id": str(context.claim.goal_id),
        "turn_id": str(context.turn_id),
        "wakeup_id": str(context.claim.wakeup_id),
        "owner": context.claim.owner,
        "lease_epoch": context.claim.lease_epoch,
        "outbox_generation": context.claim.outbox_generation,
        "project_id": context.project_id,
        "version": context.version,
        "fence_epoch": context.fence_epoch,
        "scope_revision": context.scope_revision,
        "conversational": context.conversational,
    }


async def cleanup_turn_runtime(
    runtime: SandboxRuntime, proxy: ProxyServiceManager | None, envelope: ProcessEnvelope
) -> None:
    if envelope.turn_binding is None:
        raise ValueError("turn cleanup requires a turn binding")
    if not envelope.sandbox.network_disabled:
        if proxy is None:
            raise RuntimeError("turn proxy cleanup is unavailable")
        await proxy.cleanup_turn(envelope.turn_binding, envelope.lease_generation)
    else:
        cleanup = getattr(runtime, "cleanup", None)
        if cleanup is None:
            raise RuntimeError("turn sandbox cleanup is unavailable")
        await cleanup(envelope)
    if not envelope.sandbox.empty_workspace:
        if not valid_turn_workspace(envelope):
            raise ValueError("invalid planner snapshot cleanup binding")
        cleanup_snapshot(envelope.turn_binding.invocation_id)


def valid_turn_workspace(envelope: ProcessEnvelope) -> bool:
    if envelope.sandbox.empty_workspace:
        return True
    binding = envelope.turn_binding
    workspace = [
        m
        for m in envelope.sandbox.mounts
        if m.target == Path("/workspace") or Path("/workspace") in m.target.parents
    ]
    return (
        binding is not None
        and len(workspace) == 1
        and workspace[0]
        == SandboxMount(
            source=snapshot_path(binding.invocation_id),
            target=Path("/workspace"),
            mode=MountMode.READ_ONLY,
            purpose="planner-snapshot",
        )
    )


async def finish_cleanup(operation: Coroutine[Any, Any, None]) -> None:
    """Let cleanup finish even if the caller receives another cancellation."""
    task = asyncio.create_task(operation)
    cancelled = False
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            cancelled = True
    task.result()
    if cancelled:
        raise asyncio.CancelledError


@asynccontextmanager
async def turn_runtime_lock(
    factory: async_sessionmaker[AsyncSession], invocation_id: uuid.UUID
) -> AsyncIterator[AsyncConnection | None]:
    """Serialize cooperating launch/cleanup owners; this is not launcher-death proof."""
    key = int.from_bytes(
        hashlib.sha256(f"vuzol:turn-runtime:{invocation_id}".encode()).digest()[:8],
        "big",
        signed=True,
    )
    async with factory() as session:
        connection = await session.connection()
        try:
            async with asyncio.timeout(TURN_DB_TIMEOUT_SECONDS):
                acquired = await connection.scalar(
                    text("SELECT pg_try_advisory_lock(:key)"), {"key": key}
                )
                await connection.commit()
        except BaseException:
            # Even a cancelled/lost acknowledgement can mean the server holds
            # the session lock. Never return that connection to the pool.
            await finish_cleanup(connection.invalidate())
            raise
        if acquired is not True:
            yield None
            return

        async def unlock() -> None:
            try:
                async with asyncio.timeout(TURN_DB_TIMEOUT_SECONDS):
                    if connection.invalidated:
                        raise LeaseLost("turn runtime ownership connection was lost")
                    released = await connection.scalar(
                        text("SELECT pg_advisory_unlock(:key)"), {"key": key}
                    )
                    if released is not True:
                        raise LeaseLost("turn runtime ownership lock was lost")
                    await connection.commit()
            except BaseException:
                await connection.invalidate()
                raise

        try:
            yield connection
        finally:
            await finish_cleanup(unlock())
