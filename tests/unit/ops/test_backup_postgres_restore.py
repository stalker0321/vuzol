"""Fake-process unit tests for B3.3 supervised pg_restore (no Docker/DB)."""

from __future__ import annotations

import contextlib
import os
import select
import signal
import subprocess
import threading
import time
from collections.abc import Iterator
from pathlib import Path
from typing import ClassVar

import pytest

from vuzol.ops.backup.postgres_restore import (
    CODE_BROKEN_PIPE,
    CODE_CANCELLED,
    CODE_PLAINTEXT_FAILED,
    CODE_PREFLIGHT,
    CODE_PROCESS_FAILED,
    CODE_RESTORED,
    CODE_TIMEOUT,
    PostgresRestoreError,
    _get_last_helper_liveness,
    _get_last_watchdog_stats,
    build_pg_restore_argv,
    run_pg_restore_from_path,
    run_pg_restore_stdin,
)


class _StdinEnd:
    def __init__(self, fd: int) -> None:
        self._fd = fd
        self.closed = False

    def fileno(self) -> int:
        return self._fd

    def close(self) -> None:
        if not self.closed:
            os.close(self._fd)
            self.closed = True


class _StderrEnd:
    def __init__(self, read_fd: int) -> None:
        self._fd = read_fd

    def read(self, n: int = -1) -> bytes:
        try:
            if n < 0:
                return os.read(self._fd, 65536)
            return os.read(self._fd, n)
        except OSError:
            return b""

    def close(self) -> None:
        with contextlib.suppress(Exception):
            os.close(self._fd)


class FakePopen:
    """Minimal process double with real pipe fds for non-blocking writes."""

    instances: ClassVar[list[FakePopen]] = []

    def __init__(
        self,
        argv: list[str],
        stdin: object = None,
        stdout: object = None,
        stderr: object = None,
        env: object = None,
        start_new_session: bool = False,
        *,
        exit_code: int = 0,
        wait_delay: float = 0.0,
        drain_stdin: bool = True,
        fail_fileno: bool = False,
    ) -> None:
        self.argv = argv
        self.env = env
        self.start_new_session = start_new_session
        self.pid = 90_000 + len(FakePopen.instances)
        self.returncode: int | None = None
        self.exit_code = exit_code
        self.wait_delay = wait_delay
        self.wait_calls = 0
        self.fail_fileno = fail_fileno

        self._in_r, self._in_w = os.pipe()
        self._err_r, self._err_w = os.pipe()
        os.close(self._err_w)
        self.stdin: _StdinEnd | _FailFileno = (
            _FailFileno() if fail_fileno else _StdinEnd(self._in_w)
        )
        if fail_fileno:
            os.close(self._in_w)
            os.close(self._in_r)
            self._in_r = -1
        self.stderr = _StderrEnd(self._err_r)
        self._reader_stop = threading.Event()
        self._reader: threading.Thread | None = None
        if drain_stdin and not fail_fileno:
            self._reader = threading.Thread(target=self._drain_stdin, daemon=True)
            self._reader.start()
        FakePopen.instances.append(self)

    def _drain_stdin(self) -> None:
        while not self._reader_stop.is_set():
            try:
                data = os.read(self._in_r, 65536)
            except OSError:
                break
            if not data:
                break

    def poll(self) -> int | None:
        return self.returncode

    def wait(self, timeout: float | None = None) -> int:
        self.wait_calls += 1
        delay = self.wait_delay
        if timeout is not None and delay > timeout:
            time.sleep(timeout)
            raise subprocess.TimeoutExpired(cmd=self.argv, timeout=timeout)
        if delay:
            time.sleep(delay)
        self.returncode = self.exit_code
        return self.exit_code

    def close_pipes(self) -> None:
        self._reader_stop.set()
        with contextlib.suppress(Exception):
            if hasattr(self.stdin, "closed") and not self.stdin.closed:
                self.stdin.close()
        with contextlib.suppress(Exception):
            if self._in_r >= 0:
                os.close(self._in_r)


class _FailFileno:
    closed = True

    def fileno(self) -> int:
        raise OSError("fileno failed")

    def close(self) -> None:
        return None


@pytest.fixture(autouse=True)
def _reset_fakes(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    FakePopen.instances.clear()

    def _killpg(pid: int, sig: int) -> None:
        for proc in FakePopen.instances:
            if (
                proc.pid == pid
                and proc.returncode is None
                and sig in {signal.SIGTERM, signal.SIGKILL}
            ):
                proc.returncode = -sig if proc.exit_code == 0 else proc.exit_code

    monkeypatch.setattr(os, "killpg", _killpg)
    import vuzol.ops.backup.postgres_restore as restore_mod

    monkeypatch.setattr(restore_mod, "_KILL_WAIT_SECONDS", 0.05)
    yield
    for proc in FakePopen.instances:
        proc.close_pipes()


def _patch_popen(monkeypatch: pytest.MonkeyPatch, **kwargs: object) -> None:
    def factory(argv: list[str], *args: object, **kw: object) -> FakePopen:
        return FakePopen(list(argv), **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(subprocess, "Popen", factory)


def test_build_default_argv_no_clean() -> None:
    argv = build_pg_restore_argv(container="pg", user="u", database="db_restore")
    assert argv[:3] == ["docker", "exec", "-i"]
    assert "pg_restore" in argv
    assert "--no-owner" in argv and "--no-acl" in argv
    assert "--clean" not in argv
    assert "--if-exists" not in argv


def test_build_force_clean_adds_flags() -> None:
    argv = build_pg_restore_argv(
        container="pg",
        user="u",
        database="db_restore",
        force_clean_isolated=True,
    )
    assert "--clean" in argv and "--if-exists" in argv


def test_build_override_without_clean_ok() -> None:
    argv = build_pg_restore_argv(
        container="pg",
        user="u",
        database="db",
        override=(
            "docker",
            "exec",
            "-i",
            "{container}",
            "pg_restore",
            "-U",
            "{user}",
            "-d",
            "{database}",
        ),
    )
    assert argv[3] == "pg"
    assert "pg_restore" in argv
    assert "--clean" not in argv


def test_build_override_rejects_force_clean_and_shell() -> None:
    with pytest.raises(PostgresRestoreError, match="force_clean"):
        build_pg_restore_argv(
            container="pg",
            user="u",
            database="db",
            override=("docker", "exec", "-i", "{container}", "pg_restore"),
            force_clean_isolated=True,
        )
    with pytest.raises(PostgresRestoreError, match="shell"):
        build_pg_restore_argv(
            container="pg",
            user="u",
            database="db",
            override=("bash", "-c", "x"),
        )
    with pytest.raises(PostgresRestoreError, match="metachar"):
        build_pg_restore_argv(
            container="pg",
            user="u",
            database="db",
            override=("docker", "exec", "a;b"),
        )


def test_build_override_non_string_raises_preflight() -> None:
    with pytest.raises(PostgresRestoreError) as raised:
        build_pg_restore_argv(
            container="pg",
            user="u",
            database="db",
            override=(123, "pg_restore"),  # type: ignore[arg-type]
        )
    assert raised.value.code == CODE_PREFLIGHT
    assert "strings" in str(raised.value)


def test_invalid_write_poll_and_timeout_preflight(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    called = {"n": 0}

    def factory(*_a: object, **_k: object) -> object:
        called["n"] += 1
        raise AssertionError("must not spawn")

    monkeypatch.setattr(subprocess, "Popen", factory)
    for poll in (0.0, -1.0, float("nan"), float("inf")):
        result = run_pg_restore_stdin(
            ["pg_restore"],
            iter([b"x"]),
            write_poll_seconds=poll,
        )
        assert result.code == CODE_PREFLIGHT
        assert result.process_started is False
    for timeout in (-0.1, float("nan"), float("-inf")):
        result = run_pg_restore_stdin(
            ["pg_restore"],
            iter([b"x"]),
            overall_timeout_seconds=timeout,
        )
        assert result.code == CODE_PREFLIGHT
        assert result.process_started is False
    assert called["n"] == 0


def test_t1_happy_multi_chunk_helpers_dead(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_popen(monkeypatch, exit_code=0)
    closed = {"v": False}

    def gen() -> Iterator[bytes]:
        try:
            yield b"abc"
            yield b"def"
        finally:
            closed["v"] = True

    result = run_pg_restore_stdin(["pg_restore"], gen())
    assert result.ok is True
    assert result.code == CODE_RESTORED
    assert result.bytes_written == 6
    assert closed["v"] is True
    liveness = _get_last_helper_liveness()
    assert liveness == (False, False)


def test_t6_blocked_write_cancel(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_popen(monkeypatch, exit_code=0, drain_stdin=False)
    cancelled = {"v": False}

    def flag() -> bool:
        return cancelled["v"]

    def gen() -> Iterator[bytes]:
        yield b"x" * (1024 * 1024)

    def arm_cancel() -> None:
        time.sleep(0.05)
        cancelled["v"] = True

    threading.Thread(target=arm_cancel, daemon=True).start()
    result = run_pg_restore_stdin(
        ["pg_restore"],
        gen(),
        cancel_flag=flag,
        write_size=4096,
        write_poll_seconds=0.02,
        overall_timeout_seconds=5.0,
    )
    assert result.ok is False
    assert result.code == CODE_CANCELLED
    assert _get_last_helper_liveness() == (False, False)


def test_t7_overall_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_popen(monkeypatch, exit_code=0, drain_stdin=False)
    result = run_pg_restore_stdin(
        ["pg_restore"],
        iter([b"z" * (512 * 1024)]),
        write_size=4096,
        write_poll_seconds=0.02,
        overall_timeout_seconds=0.05,
    )
    assert result.ok is False
    assert result.code == CODE_TIMEOUT
    assert _get_last_helper_liveness() == (False, False)


def test_t13_helpers_dead_after_slow_kill(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_popen(monkeypatch, exit_code=0, wait_delay=0.08)
    cancelled = {"v": True}
    result = run_pg_restore_stdin(
        ["pg_restore"],
        iter([b"data"]),
        cancel_flag=lambda: cancelled["v"],
        overall_timeout_seconds=2.0,
    )
    assert result.code == CODE_CANCELLED
    alive = _get_last_helper_liveness()
    assert alive is not None
    assert alive[0] is False
    assert alive[1] is False


def test_t13b_watchdog_instrumentation(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_popen(monkeypatch, exit_code=0)
    cancelled = {"v": True}
    run_pg_restore_stdin(
        ["pg_restore"],
        iter([b"ok"]),
        cancel_flag=lambda: cancelled["v"],
    )
    stats = _get_last_watchdog_stats()
    assert stats is not None
    # Actual watchdog ops list: only poll/arm_*, never killpg or proc_wait.
    assert stats.killpg_calls == 0
    assert stats.wait_calls == 0
    assert "killpg" not in stats.ops
    assert "proc_wait" not in stats.ops
    assert any(op in {"poll", "arm_cancelled", "arm_timeout"} for op in stats.ops)
    assert stats.max_sleep_seconds < 1.0


def test_fileno_failure_always_returns(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_popen(monkeypatch, exit_code=0, fail_fileno=True)
    result = run_pg_restore_stdin(["pg_restore"], iter([b"x"]))
    assert result.ok is False
    assert result.process_started is True
    assert result.code == CODE_PROCESS_FAILED
    assert _get_last_helper_liveness() == (False, False)


def test_t8_broken_pipe(monkeypatch: pytest.MonkeyPatch) -> None:
    def factory(*_a: object, **_k: object) -> FakePopen:
        proc = FakePopen(["pg_restore"], drain_stdin=False, exit_code=1)
        os.close(proc._in_r)
        proc._in_r = -1
        return proc

    monkeypatch.setattr(subprocess, "Popen", factory)
    result = run_pg_restore_stdin(["pg_restore"], iter([b"x" * 10000]), write_size=1024)
    assert result.ok is False
    assert result.code == CODE_BROKEN_PIPE
    assert result.process_started is True


def test_t9_plaintext_raises_partial(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_popen(monkeypatch, exit_code=0)

    def gen() -> Iterator[bytes]:
        yield b"partial"
        raise RuntimeError("crypto boom")

    result = run_pg_restore_stdin(["pg_restore"], gen())
    assert result.ok is False
    assert result.code == CODE_PLAINTEXT_FAILED
    assert result.process_started is True


def test_t10_nonzero_exit(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_popen(monkeypatch, exit_code=3)
    result = run_pg_restore_stdin(["pg_restore"], iter([b"ok"]))
    assert result.ok is False
    assert result.code == CODE_PROCESS_FAILED
    assert result.exit_code == 3
    assert _get_last_helper_liveness() == (False, False)


def test_t11_file_assert_failure_no_spawn_redaction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    called = {"popen": False}

    def factory(*_a: object, **_k: object) -> FakePopen:
        called["popen"] = True
        return FakePopen(["x"])

    monkeypatch.setattr(subprocess, "Popen", factory)
    secret_path = Path("/tmp/secret-drill-path-should-not-leak")  # noqa: S108

    def refuse(_path: Path) -> None:
        raise ValueError(f"conflict with {secret_path}")

    result = run_pg_restore_from_path(
        ["pg_restore"],
        secret_path,
        assert_plaintext_path=refuse,
    )
    assert result.code == CODE_PREFLIGHT
    assert result.process_started is False
    assert called["popen"] is False
    text = str(result) + str(result.to_operational_payload())
    assert "secret-drill-path" not in text
    assert str(secret_path) not in text


def test_t12_file_mode_ok_and_closed(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _patch_popen(monkeypatch, exit_code=0)
    path = tmp_path / "plain.dump"
    path.write_bytes(b"hello-file")
    seen: list[Path] = []

    def ok_assert(p: Path) -> None:
        seen.append(p)

    result = run_pg_restore_from_path(
        ["pg_restore"],
        path,
        assert_plaintext_path=ok_assert,
    )
    assert result.ok is True
    assert result.code == CODE_RESTORED
    assert result.bytes_written == 10
    assert seen == [path]
    # File remains readable (closed cleanly, not unlinked by primitive).
    assert path.read_bytes() == b"hello-file"


def test_file_open_failure_before_spawn(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    missing = tmp_path / "no-such-file.dump"
    called = {"popen": False}

    def factory(*_a: object, **_k: object) -> FakePopen:
        called["popen"] = True
        return FakePopen(["x"])

    monkeypatch.setattr(subprocess, "Popen", factory)
    result = run_pg_restore_from_path(
        ["pg_restore"],
        missing,
        assert_plaintext_path=lambda _p: None,
    )
    assert result.code == CODE_PREFLIGHT
    assert result.process_started is False
    assert called["popen"] is False


def test_t14_redaction_no_env_leak(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_popen(monkeypatch, exit_code=0)
    result = run_pg_restore_stdin(
        ["pg_restore"],
        iter([b"x"]),
        env={"PGPASSWORD": "super-secret-value"},
    )
    text = str(result) + str(result.to_operational_payload())
    assert "super-secret-value" not in text
    assert "PGPASSWORD" not in text


def test_no_public_last_watchdog_stats_attr() -> None:
    assert not hasattr(run_pg_restore_stdin, "last_watchdog_stats")


def test_preflight_non_string_argv() -> None:
    result = run_pg_restore_stdin([123], iter([b"x"]))  # type: ignore[list-item]
    assert result.code == CODE_PREFLIGHT
    assert result.process_started is False


def test_spawn_oserror(monkeypatch: pytest.MonkeyPatch) -> None:
    def boom(*_a: object, **_k: object) -> object:
        raise OSError("nope")

    monkeypatch.setattr(subprocess, "Popen", boom)
    result = run_pg_restore_stdin(["pg_restore"], iter([b"x"]))
    assert result.code == CODE_PREFLIGHT
    assert result.process_started is False


def test_select_hard_failure_fail_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_popen(monkeypatch, exit_code=0, drain_stdin=False)

    def boom_select(*_a: object, **_k: object) -> object:
        raise OSError("select failed")

    monkeypatch.setattr(select, "select", boom_select)
    # Non-blocking write will hit BlockingIOError then failed select.
    result = run_pg_restore_stdin(
        ["pg_restore"],
        iter([b"x" * 100_000]),
        write_size=4096,
        write_poll_seconds=0.01,
        overall_timeout_seconds=30.0,
    )
    assert result.ok is False
    assert result.code == CODE_PROCESS_FAILED
    assert result.process_started is True


def test_force_clean_requires_actual_bool() -> None:
    with pytest.raises(PostgresRestoreError) as raised:
        build_pg_restore_argv(
            container="pg",
            user="u",
            database="db",
            force_clean_isolated=1,  # type: ignore[arg-type]
        )
    assert raised.value.code == CODE_PREFLIGHT
    argv = build_pg_restore_argv(
        container="pg",
        user="u",
        database="db",
        force_clean_isolated=False,
    )
    assert "--clean" not in argv


def test_builder_rejects_nul_and_empty_identities() -> None:
    with pytest.raises(PostgresRestoreError):
        build_pg_restore_argv(container="pg\x00x", user="u", database="db")
    with pytest.raises(PostgresRestoreError):
        build_pg_restore_argv(container="pg", user="", database="db")
    with pytest.raises(PostgresRestoreError):
        build_pg_restore_argv(container="pg", user="u", database="db", override=())
    with pytest.raises(PostgresRestoreError):
        build_pg_restore_argv(
            container="pg",
            user="u",
            database="db",
            override=("pg_restore", "a\x00b"),
        )


class _CloseableIter:
    """Iterator with explicit close for pre-spawn ownership tests."""

    def __init__(self, chunks: list[bytes]) -> None:
        self._chunks = list(chunks)
        self.closed = False
        self._i = 0

    def __iter__(self) -> _CloseableIter:
        return self

    def __next__(self) -> bytes:
        if self._i >= len(self._chunks):
            raise StopIteration
        item = self._chunks[self._i]
        self._i += 1
        return item

    def close(self) -> None:
        self.closed = True


def test_write_size_bool_and_poll_bool_preflight(monkeypatch: pytest.MonkeyPatch) -> None:
    called = {"n": 0}

    def factory(*_a: object, **_k: object) -> object:
        called["n"] += 1
        raise AssertionError("must not spawn")

    monkeypatch.setattr(subprocess, "Popen", factory)
    it = _CloseableIter([b"x"])
    true_flag: object = True
    result = run_pg_restore_stdin(["pg_restore"], it, write_size=true_flag)  # type: ignore[arg-type]
    assert result.code == CODE_PREFLIGHT
    assert result.process_started is False
    assert it.closed is True

    it2 = _CloseableIter([b"x"])
    result2 = run_pg_restore_stdin(
        ["pg_restore"],
        it2,
        write_poll_seconds=true_flag,  # type: ignore[arg-type]
    )
    assert result2.code == CODE_PREFLIGHT
    assert it2.closed is True
    assert called["n"] == 0


def test_non_bytes_chunk_fail_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_popen(monkeypatch, exit_code=0)

    def gen() -> Iterator[object]:
        yield ""  # empty string must not be treated as success path
        yield b"ok"

    result = run_pg_restore_stdin(["pg_restore"], gen())  # type: ignore[arg-type]
    assert result.ok is False
    assert result.code == CODE_PLAINTEXT_FAILED
    assert result.process_started is True


def test_empty_string_chunks_only_not_restored(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_popen(monkeypatch, exit_code=0)

    def gen() -> Iterator[object]:
        yield ""
        yield ""

    result = run_pg_restore_stdin(["pg_restore"], gen())  # type: ignore[arg-type]
    assert result.ok is False
    assert result.code == CODE_PLAINTEXT_FAILED


def test_cancel_flag_raises_process_failed(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_popen(monkeypatch, exit_code=0, drain_stdin=False)

    def bad_flag() -> bool:
        raise RuntimeError("cancel boom")

    result = run_pg_restore_stdin(
        ["pg_restore"],
        iter([b"z" * 50_000]),
        cancel_flag=bad_flag,
        write_size=4096,
        write_poll_seconds=0.02,
        overall_timeout_seconds=5.0,
    )
    assert result.ok is False
    assert result.code == CODE_PROCESS_FAILED
    assert result.process_started is True
    assert _get_last_helper_liveness() == (False, False)


def test_spawn_valueerror_redacted(monkeypatch: pytest.MonkeyPatch) -> None:
    it = _CloseableIter([b"x"])

    def boom(*_a: object, **_k: object) -> object:
        raise ValueError("embedded null byte in /secret/path")

    monkeypatch.setattr(subprocess, "Popen", boom)
    result = run_pg_restore_stdin(["pg_restore"], it)
    assert result.code == CODE_PREFLIGHT
    assert result.process_started is False
    assert it.closed is True
    text = str(result) + str(result.to_operational_payload())
    assert "secret" not in text
    assert "null" not in text


def test_env_non_string_preflight(monkeypatch: pytest.MonkeyPatch) -> None:
    called = {"n": 0}

    def factory(*_a: object, **_k: object) -> object:
        called["n"] += 1
        raise AssertionError("spawn")

    monkeypatch.setattr(subprocess, "Popen", factory)
    result = run_pg_restore_stdin(
        ["pg_restore"],
        iter([b"x"]),
        env={"OK": "1", "BAD": 2},  # type: ignore[dict-item]
    )
    assert result.code == CODE_PREFLIGHT
    assert called["n"] == 0


def test_stderr_drain_to_eof_does_not_surface_body(monkeypatch: pytest.MonkeyPatch) -> None:
    """Drain reads to EOF; all body discarded — nothing retained on the result."""

    _patch_popen(monkeypatch, exit_code=0)
    result = run_pg_restore_stdin(["pg_restore"], iter([b"ok"]))
    assert result.ok is True
    text = str(result) + str(result.to_operational_payload())
    assert "stderr" not in text
    assert "pg_restore:" not in text


def test_write_size_above_chunk_max_preflight_no_spawn(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from vuzol.ops.backup.crypto import CHUNK_PLAINTEXT_MAX

    called = {"n": 0}

    def factory(*_a: object, **_k: object) -> object:
        called["n"] += 1
        raise AssertionError("must not spawn")

    monkeypatch.setattr(subprocess, "Popen", factory)
    it = _CloseableIter([b"x"])
    result = run_pg_restore_stdin(
        ["pg_restore"],
        it,
        write_size=CHUNK_PLAINTEXT_MAX + 1,
    )
    assert result.code == CODE_PREFLIGHT
    assert result.process_started is False
    assert it.closed is True
    assert called["n"] == 0


def test_write_poll_above_watchdog_preflight_no_spawn(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    called = {"n": 0}

    def factory(*_a: object, **_k: object) -> object:
        called["n"] += 1
        raise AssertionError("must not spawn")

    monkeypatch.setattr(subprocess, "Popen", factory)
    it = _CloseableIter([b"x"])
    result = run_pg_restore_stdin(
        ["pg_restore"],
        it,
        write_poll_seconds=0.3,  # > _WATCHDOG_POLL_SECONDS (0.2)
    )
    assert result.code == CODE_PREFLIGHT
    assert result.process_started is False
    assert it.closed is True
    assert called["n"] == 0


def test_oversized_plaintext_chunk_rejected_before_copy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from vuzol.ops.backup.crypto import CHUNK_PLAINTEXT_MAX

    _patch_popen(monkeypatch, exit_code=0)
    # Use a memoryview-like oversized bytes once — reject before write loop success.
    huge = b"x" * (CHUNK_PLAINTEXT_MAX + 1)

    def gen() -> Iterator[bytes]:
        yield huge

    result = run_pg_restore_stdin(["pg_restore"], gen())
    assert result.ok is False
    assert result.code == CODE_PLAINTEXT_FAILED
    assert result.process_started is True


def test_bounded_cancel_under_backpressure_with_max_write(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from vuzol.ops.backup.crypto import CHUNK_PLAINTEXT_MAX

    _patch_popen(monkeypatch, exit_code=0, drain_stdin=False)
    cancelled = {"v": False}

    def flag() -> bool:
        return cancelled["v"]

    def gen() -> Iterator[bytes]:
        # Chunks at the hard max; cancel under backpressure must still terminate.
        yield b"y" * CHUNK_PLAINTEXT_MAX
        yield b"z" * CHUNK_PLAINTEXT_MAX

    def arm_cancel() -> None:
        time.sleep(0.05)
        cancelled["v"] = True

    threading.Thread(target=arm_cancel, daemon=True).start()
    result = run_pg_restore_stdin(
        ["pg_restore"],
        gen(),
        cancel_flag=flag,
        write_size=CHUNK_PLAINTEXT_MAX,
        write_poll_seconds=0.05,
        overall_timeout_seconds=5.0,
    )
    assert result.ok is False
    assert result.code == CODE_CANCELLED
    assert _get_last_helper_liveness() == (False, False)


def test_kill_unconfirmed_prefers_process_failed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """TERM PermissionError + wait TimeoutExpired + KILL failure → PROCESS_FAILED."""

    _patch_popen(monkeypatch, exit_code=0, drain_stdin=False)

    def bad_killpg(pid: int, sig: int) -> None:
        raise PermissionError("term denied")

    def bad_wait(self: FakePopen, timeout: float | None = None) -> int:
        self.wait_calls += 1
        raise subprocess.TimeoutExpired(cmd=self.argv, timeout=timeout or 0)

    monkeypatch.setattr(os, "killpg", bad_killpg)
    monkeypatch.setattr(FakePopen, "wait", bad_wait)
    # Keep poll None so process appears live / unreaped.
    monkeypatch.setattr(FakePopen, "poll", lambda self: None)

    cancelled = {"v": True}
    result = run_pg_restore_stdin(
        ["pg_restore"],
        iter([b"data"]),
        cancel_flag=lambda: cancelled["v"],
        overall_timeout_seconds=2.0,
    )
    assert result.ok is False
    assert result.code == CODE_PROCESS_FAILED
    assert result.code != CODE_CANCELLED
    assert result.code != CODE_RESTORED
    liveness = _get_last_helper_liveness()
    assert liveness is not None


def test_term_permission_error_successful_wait_is_process_failed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """TERM PermissionError + immediate successful wait must not claim CANCELLED.

    Child reaped alone is insufficient without group-side signal confirmation.
    """

    _patch_popen(monkeypatch, exit_code=0, drain_stdin=False)

    def bad_term_only(pid: int, sig: int) -> None:
        if sig == signal.SIGTERM:
            raise PermissionError("term denied")
        # SIGKILL also denied so wait path alone would otherwise look "ok".
        raise PermissionError("kill denied")

    monkeypatch.setattr(os, "killpg", bad_term_only)

    # Successful wait reaps child without any group signal delivery.
    def ok_wait(self: FakePopen, timeout: float | None = None) -> int:
        self.wait_calls += 1
        self.returncode = self.exit_code
        return self.exit_code

    monkeypatch.setattr(FakePopen, "wait", ok_wait)

    result = run_pg_restore_stdin(
        ["pg_restore"],
        iter([b"data"]),
        cancel_flag=lambda: True,
        overall_timeout_seconds=2.0,
    )
    assert result.ok is False
    assert result.code == CODE_PROCESS_FAILED
    assert result.code != CODE_CANCELLED


def test_process_lookup_plus_poll_error_is_process_failed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """ProcessLookupError + poll exception must never confirm kill (False)."""

    _patch_popen(monkeypatch, exit_code=0, drain_stdin=False)

    def gone_group(pid: int, sig: int) -> None:
        raise ProcessLookupError(3, "No such process")

    monkeypatch.setattr(os, "killpg", gone_group)

    def boom_poll(self: FakePopen) -> int | None:
        raise OSError("poll failed after ProcessLookup")

    def boom_wait(self: FakePopen, timeout: float | None = None) -> int:
        self.wait_calls += 1
        raise OSError("wait failed after ProcessLookup")

    monkeypatch.setattr(FakePopen, "poll", boom_poll)
    monkeypatch.setattr(FakePopen, "wait", boom_wait)

    result = run_pg_restore_stdin(
        ["pg_restore"],
        iter([b"data"]),
        cancel_flag=lambda: True,
        overall_timeout_seconds=2.0,
    )
    assert result.ok is False
    assert result.code == CODE_PROCESS_FAILED
    assert result.code != CODE_CANCELLED


def test_successful_term_wait_preserves_cancelled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Confirmed TERM delivery + wait keeps CANCELLED (not PROCESS_FAILED)."""

    _patch_popen(monkeypatch, exit_code=0, drain_stdin=False)
    # Default fixture killpg + wait confirm both group and child.
    result = run_pg_restore_stdin(
        ["pg_restore"],
        iter([b"x" * 20_000]),
        cancel_flag=lambda: True,
        write_size=4096,
        write_poll_seconds=0.05,
        overall_timeout_seconds=5.0,
    )
    assert result.ok is False
    assert result.code == CODE_CANCELLED


def test_successful_term_wait_preserves_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Confirmed TERM delivery + wait keeps TIMEOUT (not PROCESS_FAILED)."""

    _patch_popen(monkeypatch, exit_code=0, drain_stdin=False)
    result = run_pg_restore_stdin(
        ["pg_restore"],
        iter([b"x" * 20_000]),
        write_size=4096,
        write_poll_seconds=0.05,
        overall_timeout_seconds=0.0,
    )
    assert result.ok is False
    assert result.code == CODE_TIMEOUT


def test_watchdog_start_failure_cleans_child(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_popen(monkeypatch, exit_code=0)
    real_start = threading.Thread.start

    def flaky_start(self: threading.Thread) -> None:
        name = getattr(self, "name", "") or ""
        if name == "pg-restore-watchdog":
            raise RuntimeError("watchdog start failed")
        return real_start(self)

    monkeypatch.setattr(threading.Thread, "start", flaky_start)
    it = _CloseableIter([b"x"])
    result = run_pg_restore_stdin(["pg_restore"], it)
    assert result.ok is False
    assert result.code == CODE_PROCESS_FAILED
    assert result.process_started is True
    assert it.closed is True


def test_drain_start_failure_joins_watchdog(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_popen(monkeypatch, exit_code=0)
    real_start = threading.Thread.start

    def flaky_start(self: threading.Thread) -> None:
        name = getattr(self, "name", "") or ""
        if name == "pg-restore-stderr":
            raise RuntimeError("drain start failed")
        return real_start(self)

    monkeypatch.setattr(threading.Thread, "start", flaky_start)
    it = _CloseableIter([b"x"])
    result = run_pg_restore_stdin(["pg_restore"], it)
    assert result.ok is False
    assert result.code == CODE_PROCESS_FAILED
    assert result.process_started is True
    assert it.closed is True
    assert _get_last_helper_liveness() is not None


def test_watchdog_start_failure_poll_raises_process_failed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """First Thread.start failure: poll exceptions stay inside always-return."""

    _patch_popen(monkeypatch, exit_code=0)
    real_start = threading.Thread.start

    def flaky_start(self: threading.Thread) -> None:
        name = getattr(self, "name", "") or ""
        if name == "pg-restore-watchdog":
            raise RuntimeError("watchdog start failed")
        return real_start(self)

    def boom_poll(self: FakePopen) -> int | None:
        raise OSError("poll boom after start failure")

    monkeypatch.setattr(threading.Thread, "start", flaky_start)
    monkeypatch.setattr(FakePopen, "poll", boom_poll)
    it = _CloseableIter([b"x"])
    result = run_pg_restore_stdin(["pg_restore"], it)
    assert result.ok is False
    assert result.code == CODE_PROCESS_FAILED
    assert result.process_started is True
    assert it.closed is True


def test_drain_start_failure_poll_raises_process_failed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Second Thread.start failure: poll exceptions stay inside always-return."""

    _patch_popen(monkeypatch, exit_code=0)
    real_start = threading.Thread.start

    def flaky_start(self: threading.Thread) -> None:
        name = getattr(self, "name", "") or ""
        if name == "pg-restore-stderr":
            raise RuntimeError("drain start failed")
        return real_start(self)

    def boom_poll(self: FakePopen) -> int | None:
        raise OSError("poll boom after drain start failure")

    monkeypatch.setattr(threading.Thread, "start", flaky_start)
    monkeypatch.setattr(FakePopen, "poll", boom_poll)
    it = _CloseableIter([b"x"])
    result = run_pg_restore_stdin(["pg_restore"], it)
    assert result.ok is False
    assert result.code == CODE_PROCESS_FAILED
    assert result.process_started is True
    assert it.closed is True
    assert _get_last_helper_liveness() is not None


def test_stderr_read_failure_arms_process_failed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def factory(*_a: object, **_k: object) -> FakePopen:
        proc = FakePopen(["pg_restore"], drain_stdin=True, exit_code=0)

        def boom_read(n: int = -1) -> bytes:
            raise OSError("stderr broken")

        proc.stderr.read = boom_read  # type: ignore[method-assign]
        return proc

    monkeypatch.setattr(subprocess, "Popen", factory)
    result = run_pg_restore_stdin(["pg_restore"], iter([b"ok"]))
    assert result.ok is False
    assert result.code == CODE_PROCESS_FAILED
    assert result.process_started is True


def test_close_plaintext_getattr_failure_prespawn(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    called = {"n": 0}

    def factory(*_a: object, **_k: object) -> object:
        called["n"] += 1
        raise AssertionError("spawn")

    monkeypatch.setattr(subprocess, "Popen", factory)

    class Evil:
        def __iter__(self) -> Evil:
            return self

        def __next__(self) -> bytes:
            raise StopIteration

        @property
        def close(self) -> object:
            raise RuntimeError("close attr boom")

    # Must not escape; preflight path.
    result = run_pg_restore_stdin([123], Evil())  # type: ignore[list-item]
    assert result.code == CODE_PREFLIGHT
    assert called["n"] == 0


def test_close_plaintext_invoke_failure_postspawn(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_popen(monkeypatch, exit_code=0)

    class BoomClose:
        def __init__(self) -> None:
            self._done = False

        def __iter__(self) -> BoomClose:
            return self

        def __next__(self) -> bytes:
            if self._done:
                raise StopIteration
            self._done = True
            return b"ok"

        def close(self) -> None:
            raise RuntimeError("close invoke boom")

    result = run_pg_restore_stdin(["pg_restore"], BoomClose())
    # Close failure must not escape; happy path still classifies from process.
    assert result.process_started is True
    assert result.code in {CODE_RESTORED, CODE_PROCESS_FAILED}


def test_stderr_over_8kib_secret_drained_no_leak(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    secret = b"SECRET_STDERR_BODY_" + (b"Z" * 10_000)

    def factory(*_a: object, **_k: object) -> FakePopen:
        proc = FakePopen(["pg_restore"], drain_stdin=True, exit_code=0)
        # Replace stderr with a stream that yields a large secret then EOF.
        data = {"buf": secret}

        class BigErr:
            def read(self, n: int = -1) -> bytes:
                buf = data["buf"]
                if not buf:
                    return b""
                if n < 0 or n >= len(buf):
                    data["buf"] = b""
                    return buf
                out, data["buf"] = buf[:n], buf[n:]
                return out

            def close(self) -> None:
                return None

        proc.stderr = BigErr()  # type: ignore[assignment]
        return proc

    monkeypatch.setattr(subprocess, "Popen", factory)
    result = run_pg_restore_stdin(["pg_restore"], iter([b"ok"]))
    text = str(result) + str(result.to_operational_payload())
    assert b"SECRET_STDERR_BODY_".decode() not in text
    assert "SECRET" not in text
    # Result contract still holds (may restore or fail process depending on timing).
    assert result.process_started is True


def test_cancel_flag_non_bool_process_failed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_popen(monkeypatch, exit_code=0, drain_stdin=False)

    def bad_flag() -> object:
        return 1  # truthy non-bool

    result = run_pg_restore_stdin(
        ["pg_restore"],
        iter([b"z" * 50_000]),
        cancel_flag=bad_flag,  # type: ignore[arg-type]
        write_size=4096,
        write_poll_seconds=0.05,
        overall_timeout_seconds=5.0,
    )
    assert result.ok is False
    assert result.code == CODE_PROCESS_FAILED
    assert result.process_started is True
