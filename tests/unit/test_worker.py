import asyncio
import logging
import threading

from pytest import LogCaptureFixture

from vuzol.cli.worker import run_runtime, run_worker


def test_worker_stops_when_requested(caplog: LogCaptureFixture) -> None:
    stop_event = threading.Event()
    stop_event.set()

    with caplog.at_level(logging.INFO):
        run_worker(poll_interval_seconds=0.01, stop_event=stop_event)

    assert [record.message for record in caplog.records] == ["worker ready", "worker stopped"]


def test_runtime_keeps_controls_responsive_and_drains_execution() -> None:
    class Transaction:
        async def __aenter__(self) -> object:
            return object()

        async def __aexit__(self, *_args: object) -> None:
            return None

    class Factory:
        def begin(self) -> Transaction:
            return Transaction()

    class IdleProcessor:
        async def process_one(self) -> bool:
            return False

    class Controls:
        def __init__(self) -> None:
            self.observed = asyncio.Event()
            self.calls = 0

        async def process_one(self) -> bool:
            self.calls += 1
            if self.calls >= 2:
                self.observed.set()
            return False

    class Execution:
        def __init__(self) -> None:
            self.started = asyncio.Event()
            self.release = asyncio.Event()
            self.cancelled = False

        async def process_one(self) -> bool:
            self.started.set()
            try:
                await self.release.wait()
            except asyncio.CancelledError:
                self.cancelled = True
                raise
            return True

    async def scenario() -> None:
        stop_event = asyncio.Event()
        controls = Controls()
        execution = Execution()

        async def recover(_session: object, *, batch_size: int) -> int:
            assert batch_size == 5
            return 0

        runtime = asyncio.create_task(
            run_runtime(
                controls=controls,
                dispatcher=IdleProcessor(),
                worker=execution,
                factory=Factory(),  # type: ignore[arg-type]
                stop_event=stop_event,
                poll_interval_seconds=0.01,
                recovery_interval_seconds=0.01,
                recovery_batch_size=5,
                shutdown_deadline_seconds=1,
                recovery=recover,
            )
        )
        await asyncio.wait_for(execution.started.wait(), timeout=1)
        await asyncio.wait_for(controls.observed.wait(), timeout=1)
        stop_event.set()
        await asyncio.sleep(0.02)
        assert not runtime.done()
        execution.release.set()
        await asyncio.wait_for(runtime, timeout=1)
        assert not execution.cancelled

    asyncio.run(scenario())
