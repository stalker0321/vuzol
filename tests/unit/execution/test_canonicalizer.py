"""Canonicalizer tests (split for cohesion)."""

from __future__ import annotations

from ._execution_helpers import *


@pytest.mark.anyio
async def test_trusted_canonicalizer_handles_no_python_and_missing_context(
    tmp_path: Path,
) -> None:
    runner = TrustedGateRunner(AsyncMock(), AsyncMock())
    empty = await runner.canonicalize(
        tmp_path,
        ("README.md",),
        timeout_seconds=30,
        context=None,
        cancellation=None,
    )
    assert empty.input_files == ()
    assert empty.changed_files == ()

    source = tmp_path / "changed.py"
    source.write_text("VALUE=1\n")
    with pytest.raises(ValueError, match="context is unavailable"):
        await runner.canonicalize(
            tmp_path,
            ("changed.py",),
            timeout_seconds=30,
            context=None,
            cancellation=None,
        )


@pytest.mark.anyio
@pytest.mark.parametrize("failure", ("formatter_exit", "scope"))
async def test_finalizer_fails_closed_on_canonicalization_failure(
    tmp_path: Path, failure: str
) -> None:
    repository, base, branch = _finalizer_repository(tmp_path)
    (repository / "src" / "example.py").write_text("VALUE=2\n")
    runner, envelopes, runtime = _sandbox_gate_runner()
    if failure == "formatter_exit":
        runtime.run.side_effect = [CodexProcessResult(2, "", "formatter failed", 1)]
        expected = "worker_canonicalization_failed"
    else:

        async def escape_scope(*_args: object) -> CodexProcessResult:
            extra = repository / "outside.py"
            extra.write_text("OUTSIDE = True\n")
            return CodexProcessResult(0, "formatted", "", 1)

        runtime.run.side_effect = escape_scope
        expected = "worker_canonicalization_scope"
    envelopes.build_canonicalizer.return_value.sandbox.image = "validation@sha256:" + "b" * 64

    with pytest.raises(WorkerFinalizationError) as captured:
        await WorkerFinalizer(LocalGit(), gate_runner=runner).finalize(
            worktree=repository,
            capsule=_finalizer_capsule(base, branch),
            edit_report=_edit_report(claimed_complete=True),
            worker_profile="codex",
            provider_usage=_normalized_usage(),
            provider_attempt=1,
            gate_context=_gate_context(),
            cancellation=CancellationContext(),
        )
    assert captured.value.category == expected


@pytest.mark.anyio
async def test_trusted_canonicalizer_formats_only_measured_python_files(tmp_path: Path) -> None:
    source = tmp_path / "changed.py"
    source.write_text('VALUE={"b":2,"a":1}\n')
    untouched = tmp_path / "notes.txt"
    untouched.write_text("not python\n")
    envelopes = AsyncMock()
    envelope = MagicMock(spec=ProcessEnvelope)
    envelope.sandbox = MagicMock(image="validation@sha256:" + "c" * 64)
    envelopes.build_canonicalizer.return_value = envelope
    runtime = AsyncMock()

    async def format_file(*_args: object) -> CodexProcessResult:
        source.write_text('VALUE = {"b": 2, "a": 1}\n')
        return CodexProcessResult(0, "1 file reformatted\n", "", 7)

    runtime.run.side_effect = format_file
    runner = TrustedGateRunner(envelopes, runtime)
    evidence = await runner.canonicalize(
        tmp_path,
        ("changed.py", "notes.txt"),
        timeout_seconds=30,
        context=_gate_context(),
        cancellation=CancellationContext(),
    )
    assert evidence.input_files == ("changed.py",)
    assert evidence.changed_files == ("changed.py",)
    assert evidence.validation_image_digest == "validation@sha256:" + "c" * 64
    envelopes.build_canonicalizer.assert_awaited_once_with(ANY, ("changed.py",), timeout_seconds=30)
    assert untouched.read_text() == "not python\n"


@pytest.mark.anyio
@pytest.mark.parametrize("path", ("../escape.py", "/absolute.py", "not-python.txt"))
async def test_canonicalizer_rejects_untrusted_paths_before_envelope_build(path: str) -> None:
    factory = object.__new__(ExecutionEnvelopeFactory)
    with pytest.raises(ValueError, match="unsafe"):
        await factory.build_canonicalizer(_gate_context(), (path,), timeout_seconds=30)
