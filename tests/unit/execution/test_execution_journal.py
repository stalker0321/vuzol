"""Crash recovery, confidentiality and tamper detection for runtime traces."""

import asyncio
import base64
import os
from unittest.mock import AsyncMock, MagicMock

import pytest
from cryptography.exceptions import InvalidTag

from vuzol.config.settings import Settings
from vuzol.execution.codex import SandboxCodexTransport
from vuzol.execution.journal import ExecutionJournal
from vuzol.execution.sandbox import RootlessDockerRuntime, SandboxInterrupted
from vuzol.workflows.ports import CancellationContext

from ._execution_helpers import envelope


def _store(tmp_path):
    key = tmp_path / "journal.key"
    key.write_text(os.urandom(32).hex())
    key.chmod(0o600)
    return ExecutionJournal(tmp_path / "traces", key)


def test_trace_is_incremental_private_and_survives_partial_frame(tmp_path):
    store = _store(tmp_path)
    configured = envelope(tmp_path)
    trace = store.open(configured)
    trace.append("stdout", b'{"tool":"bash","result":"private evidence"}\n')
    (path,) = store.root.glob("*.vtrace")
    # Read before close: evidence is already durable, not buffered until exit.
    records = list(store.read(path))
    assert [record["channel"] for record in records] == ["start", "stdin", "stdout"]
    assert base64.b64decode(records[2]["base64"]) == (
        b'{"tool":"bash","result":"private evidence"}\n'
    )
    assert b"private evidence" not in path.read_bytes()
    assert path.stat().st_mode & 0o777 == 0o600
    trace.close()
    with path.open("ab") as stream:
        stream.write(b"\x00\x00\x01")
    assert list(store.read(path)) == records


def test_tampered_frame_is_rejected(tmp_path):
    store = _store(tmp_path)
    trace = store.open(envelope(tmp_path))
    trace.close()
    (path,) = store.root.glob("*.vtrace")
    payload = bytearray(path.read_bytes())
    payload[-1] ^= 1
    path.write_bytes(payload)
    with pytest.raises(InvalidTag):
        list(store.read(path))


def test_same_runtime_identity_can_have_distinct_attempt_traces(tmp_path):
    store = _store(tmp_path)
    configured = envelope(tmp_path)
    store.open(configured).close()
    store.open(configured).close()
    assert len(list(store.root.glob("*.vtrace"))) == 2


def test_world_readable_key_is_rejected(tmp_path):
    key = tmp_path / "unsafe.key"
    key.write_text(os.urandom(32).hex())
    key.chmod(0o644)
    with pytest.raises(ValueError, match="private"):
        ExecutionJournal(tmp_path / "traces", key)


def test_enabled_journal_cannot_be_swept_as_an_orphan_artifact(tmp_path):
    with pytest.raises(ValueError, match="roots must be distinct"):
        Settings(
            _env_file=None,
            artifact_root=tmp_path / "artifacts",
            execution={
                "journal_root": tmp_path / "artifacts" / "traces",
                "journal_key_file": tmp_path / "key",
            },
        )
    Settings(
        _env_file=None,
        artifact_root=tmp_path / "artifacts",
        execution={"journal_root": tmp_path / "traces", "journal_key_file": tmp_path / "key"},
    )


@pytest.mark.anyio
@pytest.mark.parametrize("external_cancel", [False, True])
async def test_cancellation_preserves_already_streamed_tool_event(
    tmp_path, monkeypatch, external_cancel
):
    store = _store(tmp_path)
    executable = tmp_path / "docker"
    executable.write_text(
        '#!/bin/sh\nprintf \'{"tool":"test","result":"started"}\\n\'\nexec sleep 10\n'
    )
    executable.chmod(0o700)
    monkeypatch.setenv("PATH", f"{tmp_path}:/usr/bin:/bin")
    runtime = RootlessDockerRuntime(tmp_path / "docker.sock", journal=store)
    monkeypatch.setattr(runtime, "_stop_owned_container", AsyncMock())
    monkeypatch.setattr(runtime, "_remove_owned_container", AsyncMock())
    cancellation = CancellationContext()
    task = asyncio.create_task(runtime.run(envelope(tmp_path), cancellation))
    try:
        async with asyncio.timeout(2):
            while True:
                paths = list(store.root.glob("*.vtrace"))
                if paths and any(r["channel"] == "stdout" for r in store.read(paths[0])):
                    break
                await asyncio.sleep(0.01)
        assert not task.done(), "history must be visible while provider is still running"
        if external_cancel:
            task.cancel()
        else:
            cancellation.request()
        with pytest.raises(SandboxInterrupted) as stopped:
            await task
        assert '"tool":"test"' in stopped.value.result.stdout
        records = list(store.read(paths[0]))
        assert records[-1]["channel"] == "cleanup"
        assert records[-1]["data"]["confirmed"] is True
    finally:
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.anyio
async def test_proxy_monitored_cancellation_commits_partial_cleanup_receipt(tmp_path, monkeypatch):
    store = _store(tmp_path)
    executable = tmp_path / "docker"
    executable.write_text('#!/bin/sh\nprintf "tool evidence\\n"\nexec sleep 10\n')
    executable.chmod(0o700)
    monkeypatch.setenv("PATH", f"{tmp_path}:/usr/bin:/bin")
    configured = envelope(tmp_path)
    runtime = RootlessDockerRuntime(tmp_path / "docker.sock", journal=store)
    monkeypatch.setattr(runtime, "_stop_owned_container", AsyncMock())
    monkeypatch.setattr(runtime, "_remove_owned_container", AsyncMock())
    envelopes = MagicMock()
    envelopes.proxy_targets = AsyncMock(return_value=(object(),))
    envelopes.build = AsyncMock(return_value=(configured, configured.step_id))
    envelopes.mark_running = AsyncMock()
    envelopes.complete = AsyncMock()
    envelopes.fail_unknown = AsyncMock()
    proxy = MagicMock()
    proxy.create = AsyncMock(return_value=MagicMock())
    proxy.cleanup = AsyncMock()

    async def wait_until_dead(lease):
        await asyncio.Event().wait()

    proxy.wait_until_dead = wait_until_dead
    transport = SandboxCodexTransport(runtime, envelopes, MagicMock(), proxy)
    invocation = MagicMock(
        task_id=configured.task_id,
        run_id=configured.run_id,
        step_id=configured.step_id,
        lease_generation=configured.lease_generation,
    )
    task = asyncio.create_task(transport.run(invocation, CancellationContext()))
    try:
        async with asyncio.timeout(2):
            while True:
                paths = list(store.root.glob("*.vtrace"))
                if paths and any(r["channel"] == "stdout" for r in store.read(paths[0])):
                    break
                await asyncio.sleep(0.01)
        task.cancel()
        result = await task
        assert result.exit_code == 130
        assert result.stdout == "tool evidence\n"
        envelopes.complete.assert_awaited_once()
        envelopes.fail_unknown.assert_not_awaited()
        proxy.cleanup.assert_awaited_once()
    finally:
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
