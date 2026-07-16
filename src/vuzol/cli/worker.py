"""Step 06 workflow manager and internal worker runtime."""

import asyncio
import os
import signal
import socket
import threading
from collections.abc import Awaitable, Callable
from contextlib import AbstractAsyncContextManager, suppress
from typing import Protocol

from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.config import (
    LaunchMode,
    ProviderRole,
    ScopedSecretResolver,
    get_runtime_configuration,
)
from vuzol.execution.git import LocalGit
from vuzol.observability import configure_logging, get_logger
from vuzol.providers.handlers import ProviderStepHandler, provider_handlers
from vuzol.providers.health import synchronize_profiles
from vuzol.providers.registry import AdapterRegistry
from vuzol.review import ResultReviewHandler
from vuzol.storage import create_engine, create_session_factory, resolve_database_dsn
from vuzol.workflows.controls import WorkflowControlConsumer
from vuzol.workflows.dispatch import WorkflowDispatcher
from vuzol.workflows.recovery import recover_expired_steps
from vuzol.workflows.worker import BASE_INTERNAL_HANDLERS, RoutedWorkflowWorker, WorkflowWorker


class Processor(Protocol):
    async def process_one(self) -> bool: ...


class TransactionFactory(Protocol):
    def begin(self) -> AbstractAsyncContextManager[AsyncSession]: ...


class ProcessorChain:
    def __init__(self, *processors: Processor) -> None:
        self._processors = processors

    async def process_one(self) -> bool:
        for processor in self._processors:
            if await processor.process_one():
                return True
        return False


def run_worker(*, poll_interval_seconds: float, stop_event: threading.Event) -> None:
    """Wait for future workflow work without owning in-memory business state."""

    logger = get_logger(__name__)
    logger.info("worker ready", extra={"event": "worker.ready"})
    while not stop_event.wait(poll_interval_seconds):
        logger.debug("worker idle", extra={"event": "worker.idle"})
    logger.info("worker stopped", extra={"event": "worker.stopped"})


def main() -> None:
    asyncio.run(run())


async def run() -> None:
    """Run dispatch, recovery, and registered step handlers until drained."""

    runtime = get_runtime_configuration(validate_profile_credentials=False)
    settings = runtime.settings
    configure_logging(service=f"{settings.service_name}-worker", level=settings.log_level)
    stop_event = asyncio.Event()

    def request_stop(signum: int, _frame: object) -> None:
        get_logger(__name__).info(
            "worker stop requested",
            extra={"event": "worker.stop_requested", "signal": signum},
        )
        stop_event.set()

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)
    engine = create_engine(settings, resolve_database_dsn(settings))
    factory = create_session_factory(engine)
    async with factory.begin() as session:
        await synchronize_profiles(
            session,
            runtime.registries.profiles.items(),
            configuration_revision=runtime.registries.revision,
        )
    owner = f"{socket.gethostname()}:{os.getpid()}"
    dispatcher = WorkflowDispatcher(runtime, factory, owner=f"{owner}:dispatch")
    controls = WorkflowControlConsumer(settings, factory, owner=f"{owner}:control")
    handlers = {
        **BASE_INTERNAL_HANDLERS,
        "review": ResultReviewHandler(
            factory,
            LocalGit(),
            worktree_root=settings.worktree_root,
        ),
    }
    internal_worker = WorkflowWorker(
        settings,
        factory,
        owner=f"{owner}:worker",
        handlers=handlers,
        profile_limits={
            profile.id: profile.concurrency_limit for profile in runtime.registries.profiles.items()
        },
    )
    model_roles = frozenset({ProviderRole.EXECUTOR, ProviderRole.PLANNER, ProviderRole.SUMMARIZER})
    routable_profiles = tuple(
        profile
        for profile in runtime.registries.profiles.items()
        if profile.enabled
        and profile.provider == "openai-compatible"
        and profile.launch_mode is LaunchMode.API
        and profile.roles.intersection(model_roles)
    )
    worker: Processor = internal_worker
    if routable_profiles:
        resolver = ScopedSecretResolver(
            access_policy={
                profile.credential_reference: frozenset({f"profile:{profile.id}"})
                for profile in runtime.registries.profiles.items()
                if profile.credential_reference is not None
            },
            secret_file_root=settings.secret_file_root,
        )
        adapter_registry = AdapterRegistry(runtime.registries.profiles, resolver)
        provider_handler = ProviderStepHandler(factory, runtime.registries, adapter_registry)
        routed_worker = RoutedWorkflowWorker(
            settings,
            factory,
            registries=runtime.registries,
            owner=f"{owner}:provider",
            handlers=provider_handlers(provider_handler),
        )
        worker = ProcessorChain(internal_worker, routed_worker)
    get_logger(__name__).info("worker ready", extra={"event": "worker.ready"})
    try:
        await run_runtime(
            controls=controls,
            dispatcher=dispatcher,
            worker=worker,
            factory=factory,
            stop_event=stop_event,
            poll_interval_seconds=settings.workflow.poll_interval_seconds,
            recovery_interval_seconds=settings.workflow.recovery_interval_seconds,
            recovery_batch_size=settings.workflow.recovery_batch_size,
            shutdown_deadline_seconds=settings.workflow.shutdown_deadline_seconds,
            recovery=recover_expired_steps,
        )
    finally:
        await engine.dispose()
        get_logger(__name__).info("worker stopped", extra={"event": "worker.stopped"})


async def run_runtime(
    *,
    controls: Processor,
    dispatcher: Processor,
    worker: Processor,
    factory: TransactionFactory,
    stop_event: asyncio.Event,
    poll_interval_seconds: float,
    recovery_interval_seconds: float,
    recovery_batch_size: int,
    shutdown_deadline_seconds: float,
    recovery: Callable[..., Awaitable[int]],
) -> None:
    """Keep control-plane loops responsive while execution drains independently."""

    async with factory.begin() as session:
        await recovery(session, batch_size=recovery_batch_size)
    control_task = asyncio.create_task(
        _processor_loop(controls, stop_event, poll_interval_seconds), name="workflow-controls"
    )
    dispatch_task = asyncio.create_task(
        _processor_loop(dispatcher, stop_event, poll_interval_seconds), name="workflow-dispatch"
    )
    recovery_task = asyncio.create_task(
        _recovery_loop(
            factory,
            stop_event,
            interval_seconds=recovery_interval_seconds,
            batch_size=recovery_batch_size,
            recovery=recovery,
        ),
        name="workflow-recovery",
    )
    execution_task = asyncio.create_task(
        _processor_loop(worker, stop_event, poll_interval_seconds), name="workflow-execution"
    )
    background = {control_task, dispatch_task, recovery_task, execution_task}
    stopping = asyncio.create_task(stop_event.wait(), name="workflow-stop")
    done, _pending = await asyncio.wait(
        background | {stopping}, return_when=asyncio.FIRST_COMPLETED
    )
    failure = next(
        (error for task in done & background if (error := task.exception()) is not None), None
    )
    stop_event.set()
    deadline = asyncio.get_running_loop().time() + shutdown_deadline_seconds
    control_plane = {control_task, dispatch_task, recovery_task}
    _done, pending_control = await asyncio.wait(
        control_plane, timeout=max(0.0, deadline - asyncio.get_running_loop().time())
    )
    for task in pending_control:
        task.cancel()
    await asyncio.gather(*control_plane, return_exceptions=True)
    if not execution_task.done():
        drained, _ = await asyncio.wait(
            {execution_task}, timeout=max(0.0, deadline - asyncio.get_running_loop().time())
        )
        if not drained:
            execution_task.cancel()
    await asyncio.gather(execution_task, return_exceptions=True)
    stopping.cancel()
    await asyncio.gather(stopping, return_exceptions=True)
    if failure is not None:
        raise failure


async def _processor_loop(
    processor: Processor, stop_event: asyncio.Event, poll_interval_seconds: float
) -> None:
    while not stop_event.is_set():
        processed = await processor.process_one()
        if not processed:
            await _wait_for_stop(stop_event, poll_interval_seconds)


async def _recovery_loop(
    factory: TransactionFactory,
    stop_event: asyncio.Event,
    *,
    interval_seconds: float,
    batch_size: int,
    recovery: Callable[..., Awaitable[int]],
) -> None:
    while not stop_event.is_set():
        await _wait_for_stop(stop_event, interval_seconds)
        if stop_event.is_set():
            return
        async with factory.begin() as session:
            await recovery(session, batch_size=batch_size)


async def _wait_for_stop(stop_event: asyncio.Event, delay_seconds: float) -> None:
    with suppress(TimeoutError):
        await asyncio.wait_for(stop_event.wait(), timeout=delay_seconds)


if __name__ == "__main__":
    main()
