"""Sandbox diagnostics tests (split for cohesion)."""

from __future__ import annotations

from ._execution_helpers import *


def test_grok_diagnostic_tar_accepts_one_bounded_regular_file() -> None:
    assert (
        _single_regular_tar_file(
            _tar_file("events.jsonl", b"safe"), expected_name="events.jsonl", maximum=4
        )
        == b"safe"
    )
    assert (
        _single_regular_tar_file(
            _tar_file("events.jsonl", b"unsafe", symlink=True),
            expected_name="events.jsonl",
            maximum=100,
        )
        is None
    )
    assert (
        _single_regular_tar_file(
            _tar_file("events.jsonl", b"large"), expected_name="events.jsonl", maximum=4
        )
        is None
    )
    assert (
        _single_regular_tar_file(
            _tar_file("auth.json", b"secret"), expected_name="events.jsonl", maximum=100
        )
        is None
    )
    assert _single_regular_tar_file(b"not a tar", expected_name="events.jsonl", maximum=100) is None


@pytest.mark.anyio
async def test_bounded_read_handles_absent_stream() -> None:
    assert await _bounded_read(None, 1) == b""


@pytest.mark.anyio
async def test_container_copy_accepts_only_bounded_exact_tar(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    good = MagicMock(
        stdout=_reader(_tar_file("events.jsonl", b"safe")),
        stderr=_reader(b""),
    )
    good.wait = AsyncMock(return_value=0)
    missing = MagicMock(stdout=_reader(b""), stderr=_reader(b"missing"))
    missing.wait = AsyncMock(return_value=1)
    oversized = MagicMock(
        stdout=_reader(b"x" * (GROK_DIAGNOSTIC_FILE_MAX_BYTES + 1_048_577)),
        stderr=_reader(b""),
    )
    oversized.wait = AsyncMock(return_value=0)
    oversized.kill = MagicMock()
    create = AsyncMock(side_effect=(good, missing, oversized))
    monkeypatch.setattr("vuzol.execution.sandbox.asyncio.create_subprocess_exec", create)
    runtime = RootlessDockerRuntime(tmp_path / "docker.sock")

    assert await runtime._copy_container_regular_file("a" * 64, "/exact/events.jsonl") == b"safe"
    assert await runtime._copy_container_regular_file("a" * 64, "/exact/events.jsonl") is None
    assert await runtime._copy_container_regular_file("a" * 64, "/exact/events.jsonl") is None
    oversized.kill.assert_called_once()


def test_diagnostic_staging_rejects_invalid_mounts_and_destinations(tmp_path: Path) -> None:
    configured = envelope(tmp_path)
    assert _artifact_staging(configured) == tmp_path / "artifacts"
    no_artifacts = configured.model_copy(
        update={
            "sandbox": configured.sandbox.model_copy(
                update={"mounts": configured.sandbox.mounts[:1]}
            )
        }
    )
    assert _artifact_staging(no_artifacts) is None
    missing_source = tmp_path / "missing-artifacts"
    missing_mount = configured.sandbox.mounts[1].model_copy(update={"source": missing_source})
    missing = configured.model_copy(
        update={
            "sandbox": configured.sandbox.model_copy(
                update={"mounts": (configured.sandbox.mounts[0], missing_mount)}
            )
        }
    )
    assert _artifact_staging(missing) is None

    session_id = "019f6149-44c0-7520-932c-5e0f41c99351"
    paths = staged_grok_diagnostic_paths(tmp_path / "artifacts", session_id)
    assert paths is not None
    with pytest.raises(ValueError, match="bounded limit"):
        _atomic_write_diagnostic(
            tmp_path / "artifacts",
            paths[0],
            b"x" * (GROK_DIAGNOSTIC_FILE_MAX_BYTES + 1),
        )
    with pytest.raises(ValueError, match="destination is invalid"):
        _atomic_write_diagnostic(tmp_path / "artifacts", tmp_path / "other", b"safe")

    paths[0].parent.mkdir(parents=True)
    paths[0].symlink_to(tmp_path / "outside")
    with pytest.raises(ValueError, match="destination is unsafe"):
        _prepare_diagnostic_destinations(tmp_path / "artifacts", paths)


def test_atomic_diagnostic_write_cleans_partial_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    session_id = "019f6149-44c0-7520-932c-5e0f41c99351"
    paths = staged_grok_diagnostic_paths(tmp_path, session_id)
    assert paths is not None
    monkeypatch.setattr("vuzol.execution.sandbox.os.write", lambda _fd, _content: 0)
    with pytest.raises(OSError, match="no progress"):
        _atomic_write_diagnostic(tmp_path, paths[0], b"safe")
    assert not list(paths[0].parent.glob("*.tmp"))


@pytest.mark.anyio
async def test_grok_staging_degrades_for_missing_identity_or_session(tmp_path: Path) -> None:
    runtime = RootlessDockerRuntime(tmp_path / "docker.sock")
    configured = envelope(tmp_path).model_copy(update={"argv": ("grok",)})
    runtime._owned_container_id = AsyncMock(return_value=None)  # type: ignore[method-assign]
    runtime._copy_container_regular_file = AsyncMock()  # type: ignore[method-assign]
    session_id = "019f6149-44c0-7520-932c-5e0f41c99351"
    await runtime._stage_grok_diagnostics(
        "missing", configured, f'{{"type":"end","sessionId":"{session_id}"}}'
    )
    await runtime._stage_grok_diagnostics("missing", configured, '{"type":"end"}')
    runtime._copy_container_regular_file.assert_not_awaited()


@pytest.mark.anyio
async def test_rootless_runtime_stages_exact_grok_session_and_always_removes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    session_id = "019f6149-44c0-7520-932c-5e0f41c99351"
    executable = tmp_path / "docker"
    executable.write_text(
        "#!/bin/sh\ncat >/dev/null\n"
        f'printf \'%s\\n\' \'{{"type":"end","stopReason":"EndTurn",'
        f'"sessionId":"{session_id}"}}\'\n'
    )
    executable.chmod(0o700)
    monkeypatch.setenv("PATH", f"{tmp_path}:/usr/bin:/bin")
    base = envelope(tmp_path)
    configured = base.model_copy(update={"argv": ("grok",)})
    runtime = RootlessDockerRuntime(tmp_path / "docker.sock")
    runtime._owned_container_id = AsyncMock(return_value="a" * 64)  # type: ignore[method-assign]
    runtime._copy_container_regular_file = AsyncMock(  # type: ignore[method-assign]
        side_effect=[b'{"type":"turn_started","schema_version":"1.0"}\n', b""]
    )
    runtime._remove_owned_container = AsyncMock()  # type: ignore[method-assign]

    result = await runtime.run(configured, CancellationContext())

    assert result.exit_code == 0
    staged = staged_grok_diagnostic_paths(tmp_path / "artifacts", session_id)
    assert staged is not None
    assert staged[0].read_bytes().startswith(b'{"type"')
    assert staged[1].read_bytes() == b""
    requested = [call.args[1] for call in runtime._copy_container_regular_file.await_args_list]
    assert requested == [
        f"/grok-home/.grok/sessions/%2Fworkspace/{session_id}/events.jsonl",
        f"/grok-home/.grok/sessions/%2Fworkspace/{session_id}/updates.jsonl",
    ]
    runtime._remove_owned_container.assert_awaited_once()


@pytest.mark.anyio
async def test_grok_extraction_failure_preserves_result_and_removes_container(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    session_id = "019f6149-44c0-7520-932c-5e0f41c99351"
    executable = tmp_path / "docker"
    executable.write_text(
        "#!/bin/sh\ncat >/dev/null\n"
        f'printf \'%s\\n\' \'{{"type":"end","stopReason":"EndTurn",'
        f'"sessionId":"{session_id}"}}\'\n'
    )
    executable.chmod(0o700)
    monkeypatch.setenv("PATH", f"{tmp_path}:/usr/bin:/bin")
    configured = envelope(tmp_path).model_copy(update={"argv": ("grok",)})
    stale_paths = staged_grok_diagnostic_paths(tmp_path / "artifacts", session_id)
    assert stale_paths is not None
    stale_paths[0].parent.mkdir(parents=True)
    stale_paths[0].write_text("stale evidence")
    stale_paths[1].write_text("stale evidence")
    runtime = RootlessDockerRuntime(tmp_path / "docker.sock")
    runtime._owned_container_id = AsyncMock(return_value="a" * 64)  # type: ignore[method-assign]
    runtime._copy_container_regular_file = AsyncMock(  # type: ignore[method-assign]
        side_effect=SandboxError("copy failed")
    )
    runtime._remove_owned_container = AsyncMock()  # type: ignore[method-assign]

    result = await runtime.run(configured, CancellationContext())

    assert result.exit_code == 0
    assert not stale_paths[0].exists()
    assert not stale_paths[1].exists()
    runtime._remove_owned_container.assert_awaited_once()


def test_grok_summary_uses_only_exact_bounded_staged_session(tmp_path: Path) -> None:
    session_id = "019f6149-44c0-7520-932c-5e0f41c99351"
    stale_id = "019f5e8d-d90b-7e40-a698-8a71fa87eff8"
    for current, decision in ((session_id, "allow"), (stale_id, "cancelled")):
        paths = staged_grok_diagnostic_paths(tmp_path, current)
        assert paths is not None
        paths[0].parent.mkdir(parents=True)
        paths[0].write_text(
            "\n".join(
                (
                    '{"type":"turn_started","schema_version":"1.0"}',
                    '{"type":"tool_started","tool_name":"run_terminal_command"}',
                    '{"type":"permission_requested","tool_name":"run_terminal_command"}',
                    f'{{"type":"permission_resolved","decision":"{decision}"}}',
                    '{"type":"tool_completed","outcome":"success"}',
                )
            )
        )
        paths[1].write_text(
            json.dumps(
                {
                    "method": "session/update",
                    "params": {
                        "update": {
                            "sessionUpdate": "tool_call",
                            "toolCallId": f"call-{current}-31",
                            "rawInput": {"command": "make test"},
                            "_meta": {"x.ai/tool": {"name": "run_terminal_command"}},
                        }
                    },
                }
            )
        )
    stdout = f'{{"type":"end","stopReason":"EndTurn","sessionId":"{session_id}"}}'
    summary = _summarize_grok_process(stdout, tmp_path)
    assert summary["last_permission_decision"] == "allowed"
    assert summary["last_safe_command_identity"] == "make test"
    assert summary["last_tool_result_received"] is True
    assert summary["evidence_completeness"] == "complete"

    exact_paths = staged_grok_diagnostic_paths(tmp_path, session_id)
    assert exact_paths is not None
    assert not exact_paths[0].exists() and not exact_paths[1].exists()
    exact_paths[0].parent.mkdir(parents=True)
    exact_paths[0].write_text(
        "\n".join(
            (
                '{"type":"turn_started","schema_version":"1.0"}',
                '{"type":"tool_started","tool_name":"run_terminal_command"}',
            )
        )
    )
    partial = _summarize_grok_process(stdout, tmp_path)
    assert partial["evidence_completeness"] == "partial"
    exact_paths[0].parent.mkdir(parents=True)
    exact_paths[1].write_text("{}")
    missing_events = _summarize_grok_process(stdout, tmp_path)
    assert missing_events["evidence_completeness"] == "unavailable"
    unavailable = _summarize_grok_process(stdout, tmp_path)
    assert unavailable["evidence_completeness"] == "unavailable"


def test_grok_summary_rejects_oversized_or_symlinked_staged_diagnostics(
    tmp_path: Path,
) -> None:
    session_id = "019f6149-44c0-7520-932c-5e0f41c99351"
    paths = staged_grok_diagnostic_paths(tmp_path, session_id)
    assert paths is not None
    paths[0].parent.mkdir(parents=True)
    paths[0].write_bytes(b"x" * (GROK_DIAGNOSTIC_FILE_MAX_BYTES + 1))
    paths[1].symlink_to(paths[0])
    stdout = f'{{"type":"end","stopReason":"Cancelled","sessionId":"{session_id}"}}'
    summary = _summarize_grok_process(stdout, tmp_path)
    assert summary["evidence_completeness"] == "unavailable"
    assert summary["cancellation_evidence_category"] == "PROVIDER_CANCELLED_UNATTRIBUTED"
