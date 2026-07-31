"""Supervised docker exec pg_restore process-group pipeline (B3.2).

Option-A lifecycle (design v4): Event-only watchdog; exclusive non-blocking
raw-fd stdin writer; writer-only process-group kill/waits; always-return
redacted RestoreProcessResult. No DSN/KEK/CLI/orchestration in this module.

``cancel_flag``, when provided, **must be non-blocking** (cheap poll). A blocking
callback can stall the watchdog and delay cancellation detection.

Stderr: the child pipe is drained to EOF with bounded per-read size so a
chatty pg_restore cannot deadlock; all stderr body is discarded and never
returned on the result (reads stop after EOF; no retained body is kept).
"""

from __future__ import annotations

import contextlib
import errno
import math
import os
import re
import select
import signal
import subprocess
import threading
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from pathlib import Path

from vuzol.ops.backup.crypto import CHUNK_PLAINTEXT_MAX

_CONTAINER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")
_IDENT_RE = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9_-]{0,62}$")

DEFAULT_WRITE_SIZE = 65_536
DEFAULT_WRITE_POLL_SECONDS = 0.05
_WATCHDOG_POLL_SECONDS = 0.2
_WATCHDOG_JOIN_SECONDS = 1.0
_DRAIN_JOIN_SECONDS = 10.0
_STDERR_LIMIT = 8 * 1024
_KILL_WAIT_SECONDS = 5.0

CODE_RESTORED = "restored"
CODE_PROCESS_FAILED = "restore_process_failed"
CODE_BROKEN_PIPE = "restore_broken_pipe"
CODE_CANCELLED = "cancelled"
CODE_TIMEOUT = "restore_timeout"
CODE_PLAINTEXT_FAILED = "restore_plaintext_failed"
CODE_PREFLIGHT = "preflight_postgres"


class PostgresRestoreError(RuntimeError):
    """Argv / programmer misuse for the restore primitive."""

    def __init__(self, code: str, message: str = "") -> None:
        super().__init__(message or code)
        self.code = code


@dataclass(frozen=True, slots=True)
class RestoreProcessResult:
    """Safe operational result — never carries stderr, paths, DSN, or env secrets."""

    ok: bool
    code: str
    exit_code: int | None
    bytes_written: int
    process_started: bool

    def to_operational_payload(self) -> dict[str, object]:
        return {
            "ok": self.ok,
            "code": self.code,
            "exit_code": self.exit_code,
            "bytes_written": self.bytes_written,
            "process_started": self.process_started,
            "schedule": "disabled",
        }


@dataclass(frozen=True, slots=True)
class _WatchdogStats:
    """Internal/test hygiene for the Event-only watchdog (not on Result)."""

    ops: tuple[str, ...]
    max_sleep_seconds: float

    @property
    def killpg_calls(self) -> int:
        return sum(1 for op in self.ops if op == "killpg")

    @property
    def wait_calls(self) -> int:
        return sum(1 for op in self.ops if op == "proc_wait")


# Thread-local last run hygiene for unit tests (not a public mutable API attribute).
_thread_local = threading.local()


def _get_last_watchdog_stats() -> _WatchdogStats | None:
    """Test-only: watchdog hygiene from the last ``run_pg_restore_stdin`` on this thread."""

    return getattr(_thread_local, "watchdog_stats", None)


def _get_last_helper_liveness() -> tuple[bool, bool] | None:
    """Test-only: ``(watchdog_alive, drain_alive)`` after the last run on this thread."""

    return getattr(_thread_local, "helper_liveness", None)


def _is_clean_token(value: object) -> bool:
    return isinstance(value, str) and value != "" and "\x00" not in value


def build_pg_restore_argv(
    *,
    container: str,
    user: str,
    database: str,
    override: tuple[str, ...] | None = None,
    force_clean_isolated: bool = False,
) -> list[str]:
    """Build argv for supervised pg_restore (no default --clean)."""

    if not isinstance(force_clean_isolated, bool):
        raise PostgresRestoreError(CODE_PREFLIGHT, "force_clean must be a bool")
    if not _is_clean_token(container) or not _CONTAINER_RE.fullmatch(container):
        raise PostgresRestoreError(CODE_PREFLIGHT, "container name rejected")
    if (
        not _is_clean_token(user)
        or not _is_clean_token(database)
        or not _IDENT_RE.fullmatch(user)
        or not _IDENT_RE.fullmatch(database)
    ):
        raise PostgresRestoreError(CODE_PREFLIGHT, "user/database rejected")
    if override is not None and force_clean_isolated:
        raise PostgresRestoreError(CODE_PREFLIGHT, "force_clean incompatible with override")
    if override is not None:
        if not isinstance(override, tuple) or len(override) == 0:
            raise PostgresRestoreError(CODE_PREFLIGHT, "override argv rejected")
        # Type-check every element before any string operations (C-A1).
        if any(not isinstance(part, str) for part in override):
            raise PostgresRestoreError(CODE_PREFLIGHT, "argv must be strings")
        if any(part == "" or "\x00" in part for part in override):
            raise PostgresRestoreError(CODE_PREFLIGHT, "argv must be nonempty and NUL-free")
        argv = [
            part.replace("{container}", container)
            .replace("{user}", user)
            .replace("{database}", database)
            for part in override
        ]
        if any(part == "" or "\x00" in part for part in argv):
            raise PostgresRestoreError(CODE_PREFLIGHT, "argv must be nonempty and NUL-free")
        forbidden = {"bash", "sh", "zsh", "-c", "cmd.exe"}
        if any(part in forbidden for part in argv):
            raise PostgresRestoreError(CODE_PREFLIGHT, "shell override rejected")
        if any(any(ch in part for ch in ";|&`$") for part in argv):
            raise PostgresRestoreError(CODE_PREFLIGHT, "argv metacharacters rejected")
        return list(argv)
    argv = [
        "docker",
        "exec",
        "-i",
        container,
        "pg_restore",
        "-U",
        user,
        "-d",
        database,
        "--no-owner",
        "--no-acl",
    ]
    if force_clean_isolated:
        argv.extend(["--clean", "--if-exists"])
    return argv


def _preflight_refused(
    plaintext_iter: object,
    *,
    close_plaintext: bool,
) -> RestoreProcessResult:
    if close_plaintext:
        _close_plaintext(plaintext_iter)
    return _result(
        ok=False,
        code=CODE_PREFLIGHT,
        exit_code=None,
        bytes_written=0,
        process_started=False,
    )


def _validate_run_inputs(
    argv: list[str],
    *,
    env: dict[str, str] | None,
    cancel_flag: Callable[[], bool] | None,
    write_size: object,
    write_poll_seconds: object,
    overall_timeout_seconds: object,
    close_plaintext: object,
) -> str | None:
    """Return fixed preflight message code path if invalid; else None."""

    if not isinstance(argv, list) or len(argv) == 0:
        return CODE_PREFLIGHT
    if any(not isinstance(part, str) or part == "" or "\x00" in part for part in argv):
        return CODE_PREFLIGHT
    if not isinstance(write_size, int) or isinstance(write_size, bool) or write_size < 1:
        return CODE_PREFLIGHT
    if write_size > CHUNK_PLAINTEXT_MAX:
        return CODE_PREFLIGHT
    if isinstance(write_poll_seconds, bool) or not isinstance(write_poll_seconds, (int, float)):
        return CODE_PREFLIGHT
    if (
        not math.isfinite(write_poll_seconds)
        or write_poll_seconds <= 0
        or write_poll_seconds > _WATCHDOG_POLL_SECONDS
    ):
        return CODE_PREFLIGHT
    if overall_timeout_seconds is not None:
        if isinstance(overall_timeout_seconds, bool) or not isinstance(
            overall_timeout_seconds, (int, float)
        ):
            return CODE_PREFLIGHT
        if not math.isfinite(overall_timeout_seconds) or overall_timeout_seconds < 0:
            return CODE_PREFLIGHT
    if not isinstance(close_plaintext, bool):
        return CODE_PREFLIGHT
    if cancel_flag is not None and not callable(cancel_flag):
        return CODE_PREFLIGHT
    if env is not None:
        if not isinstance(env, dict):
            return CODE_PREFLIGHT
        for key, value in env.items():
            if not isinstance(key, str) or not isinstance(value, str):
                return CODE_PREFLIGHT
            if key == "" or "\x00" in key or "\x00" in value:
                return CODE_PREFLIGHT
    return None


def run_pg_restore_stdin(
    argv: list[str],
    plaintext_iter: Iterator[bytes],
    *,
    env: dict[str, str] | None = None,
    cancel_flag: Callable[[], bool] | None = None,
    write_size: int = DEFAULT_WRITE_SIZE,
    write_poll_seconds: float = DEFAULT_WRITE_POLL_SECONDS,
    overall_timeout_seconds: float | None = None,
    close_plaintext: bool = True,
) -> RestoreProcessResult:
    """Feed plaintext to supervised pg_restore stdin (Option-A lifecycle).

    Always returns RestoreProcessResult for operational outcomes (including
    post-spawn fileno/set_blocking failures). BaseException (e.g. KeyboardInterrupt)
    still propagates after best-effort teardown. Never logs env values or returns
    stderr bodies.

    ``cancel_flag`` must be non-blocking when provided. Exceptions from the callback
    arm teardown and classify as ``restore_process_failed``.
    """

    # Only an actual True closes owned iterators on preflight refuse.
    close_on_refuse = close_plaintext is True
    if _validate_run_inputs(
        argv,
        env=env,
        cancel_flag=cancel_flag,
        write_size=write_size,
        write_poll_seconds=write_poll_seconds,
        overall_timeout_seconds=overall_timeout_seconds,
        close_plaintext=close_plaintext,
    ):
        return _preflight_refused(plaintext_iter, close_plaintext=close_on_refuse)

    # After validation these are real numbers / bools (narrow for type checkers).
    assert isinstance(write_poll_seconds, (int, float)) and not isinstance(write_poll_seconds, bool)
    write_poll = float(write_poll_seconds)
    overall_timeout: float | None
    if overall_timeout_seconds is None:
        overall_timeout = None
    else:
        assert isinstance(overall_timeout_seconds, (int, float)) and not isinstance(
            overall_timeout_seconds, bool
        )
        overall_timeout = float(overall_timeout_seconds)
    assert isinstance(close_plaintext, bool)
    close_pt = close_plaintext
    assert isinstance(write_size, int) and not isinstance(write_size, bool)
    write_sz = write_size

    proc_env = os.environ.copy()
    if env:
        proc_env.update(env)

    t0 = time.monotonic()
    deadline = t0 + overall_timeout if overall_timeout is not None else None

    try:
        proc = subprocess.Popen(  # noqa: S603
            argv,
            stdin=subprocess.PIPE,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            env=proc_env,
            start_new_session=True,
        )
    except (OSError, ValueError, TypeError):
        return _preflight_refused(plaintext_iter, close_plaintext=close_pt)

    assert proc.stdin is not None
    assert proc.stderr is not None

    kill_requested = threading.Event()
    reason_cancelled = threading.Event()
    reason_timeout = threading.Event()
    reason_watchdog_failed = threading.Event()
    stop_event = threading.Event()
    arm_lock = threading.Lock()
    # Instrumentation: watchdog appends only allowed ops (poll/arm_*), never killpg/wait.
    wd_ops: list[str] = []
    wd_max_sleep = 0.0
    wd_lock = threading.Lock()

    def arm(reason: str) -> None:
        with arm_lock:
            if kill_requested.is_set():
                return
            kill_requested.set()
            if reason == CODE_CANCELLED:
                reason_cancelled.set()
            elif reason == CODE_TIMEOUT:
                reason_timeout.set()
            else:
                reason_watchdog_failed.set()

    def overdue() -> bool:
        return deadline is not None and time.monotonic() >= deadline

    def remaining() -> float | None:
        if deadline is None:
            return None
        return max(0.0, deadline - time.monotonic())

    def watchdog_main() -> None:
        nonlocal wd_max_sleep
        while not stop_event.is_set():
            # cancel_flag must be non-blocking (documented contract).
            if cancel_flag is not None:
                try:
                    cancelled = cancel_flag()
                except Exception:
                    with wd_lock:
                        wd_ops.append("arm_cancel_flag_failed")
                    arm(CODE_PROCESS_FAILED)
                    break
                if not isinstance(cancelled, bool):
                    with wd_lock:
                        wd_ops.append("arm_cancel_flag_non_bool")
                    arm(CODE_PROCESS_FAILED)
                    break
                if cancelled:
                    with wd_lock:
                        wd_ops.append("arm_cancelled")
                    arm(CODE_CANCELLED)
                    break
            if overdue():
                with wd_lock:
                    wd_ops.append("arm_timeout")
                arm(CODE_TIMEOUT)
                break
            # Bounded poll only — never killpg / proc.wait.
            with wd_lock:
                wd_ops.append("poll")
            started = time.monotonic()
            stop_event.wait(timeout=_WATCHDOG_POLL_SECONDS)
            slept = time.monotonic() - started
            with wd_lock:
                if slept > wd_max_sleep:
                    wd_max_sleep = slept

    def drain_stderr() -> None:
        """Drain child stderr to EOF with bounded per-read size.

        All body is discarded (never returned). Continue reading to EOF so a
        chatty child cannot deadlock on a full stderr buffer. Read failures
        arm process-failure teardown.
        """

        assert proc.stderr is not None
        try:
            while True:
                block = proc.stderr.read(4096)
                if not block:
                    break
                # Discard immediately; no retained body.
                del block
        except Exception:
            arm(CODE_PROCESS_FAILED)
            return

    watchdog = threading.Thread(target=watchdog_main, name="pg-restore-watchdog", daemon=False)
    drain = threading.Thread(target=drain_stderr, name="pg-restore-stderr", daemon=False)

    def _teardown_after_spawn(
        *,
        join_watchdog: bool,
        join_drain: bool,
        close_pt_flag: bool,
    ) -> tuple[bool, tuple[bool, bool]]:
        """Stop helpers, close streams, kill/reap child. Returns (kill_ok, liveness)."""

        stop_event.set()
        if join_watchdog:
            with contextlib.suppress(Exception):
                watchdog.join(timeout=_WATCHDOG_JOIN_SECONDS)
        _close_stdin(proc)
        if close_pt_flag:
            _close_plaintext(plaintext_iter)
        try:
            kill_ok = _kill_group(proc) if proc.poll() is None else True
        except Exception:
            kill_ok = False
        if join_drain:
            with contextlib.suppress(Exception):
                drain.join(timeout=_DRAIN_JOIN_SECONDS)
        with contextlib.suppress(Exception):
            if proc.stderr is not None:
                proc.stderr.close()
        try:
            wd_alive = watchdog.is_alive() if join_watchdog else False
        except Exception:
            wd_alive = True
            kill_ok = False
        try:
            dr_alive = drain.is_alive() if join_drain else False
        except Exception:
            dr_alive = True
            kill_ok = False
        return kill_ok, (wd_alive, dr_alive)

    # Staged helper startup: failure after first start joins only started threads.
    try:
        watchdog.start()
    except Exception:
        kill_ok, liveness = _teardown_after_spawn(
            join_watchdog=False, join_drain=False, close_pt_flag=close_pt
        )
        _thread_local.helper_liveness = liveness
        _thread_local.watchdog_stats = _WatchdogStats(ops=(), max_sleep_seconds=0.0)
        return _classify_result(
            exit_code=proc.poll(),
            bytes_written=0,
            process_started=True,
            reason_cancelled=False,
            reason_timeout=False,
            broken_pipe=False,
            plaintext_failed=False,
            io_failed=True,
            kill_confirmed=kill_ok,
            watchdog_alive=liveness[0],
            drain_alive=liveness[1],
        )
    try:
        drain.start()
    except Exception:
        kill_ok, liveness = _teardown_after_spawn(
            join_watchdog=True, join_drain=False, close_pt_flag=close_pt
        )
        with wd_lock:
            stats = _WatchdogStats(ops=tuple(wd_ops), max_sleep_seconds=wd_max_sleep)
        _thread_local.watchdog_stats = stats
        _thread_local.helper_liveness = liveness
        return _classify_result(
            exit_code=proc.poll(),
            bytes_written=0,
            process_started=True,
            reason_cancelled=False,
            reason_timeout=False,
            broken_pipe=False,
            plaintext_failed=False,
            io_failed=True,
            kill_confirmed=kill_ok,
            watchdog_alive=liveness[0],
            drain_alive=liveness[1],
        )

    bytes_written = 0
    exit_code: int | None = None
    plaintext_failed = False
    broken_pipe = False
    io_failed = False
    kill_confirmed = True

    try:
        try:
            stdin_fd = proc.stdin.fileno()
            os.set_blocking(stdin_fd, False)
        except Exception:
            # Post-spawn setup failure → always-return Result (not plaintext taxonomy).
            io_failed = True
        else:
            try:
                for chunk in plaintext_iter:
                    if kill_requested.is_set() or overdue():
                        if overdue() and not kill_requested.is_set():
                            arm(CODE_TIMEOUT)
                        break
                    if not isinstance(chunk, (bytes, bytearray)):
                        # Empty strings and other non-bytes must not be skipped as success.
                        plaintext_failed = True
                        break
                    if not chunk:
                        continue
                    if len(chunk) > CHUNK_PLAINTEXT_MAX:
                        # Reject oversized chunks before copying into a new bytes object.
                        plaintext_failed = True
                        break
                    data = bytes(chunk)
                    try:
                        written, hit_pipe, select_failed = _write_chunk_nonblocking(
                            stdin_fd,
                            data,
                            write_size=write_sz,
                            write_poll_seconds=write_poll,
                            kill_requested=kill_requested,
                            overdue=overdue,
                            arm=arm,
                        )
                    except BrokenPipeError:
                        broken_pipe = True
                        break
                    except OSError:
                        io_failed = True
                        break
                    bytes_written += written
                    if hit_pipe:
                        broken_pipe = True
                        break
                    if select_failed:
                        io_failed = True
                        break
                    if kill_requested.is_set() or overdue():
                        if overdue() and not kill_requested.is_set():
                            arm(CODE_TIMEOUT)
                        break
                else:
                    if not (
                        kill_requested.is_set()
                        or overdue()
                        or broken_pipe
                        or io_failed
                        or plaintext_failed
                    ):
                        _close_stdin(proc)
                        wait_timeout = remaining()
                        try:
                            if wait_timeout is None:
                                exit_code = proc.wait()
                            else:
                                exit_code = proc.wait(timeout=wait_timeout)
                        except subprocess.TimeoutExpired:
                            arm(CODE_TIMEOUT)
                        except Exception:
                            kill_confirmed = False
            except Exception:
                # Iterator / crypto errors mid-stream → plaintext_failed.
                plaintext_failed = True
    finally:
        stop_event.set()
        with contextlib.suppress(Exception):
            watchdog.join(timeout=_WATCHDOG_JOIN_SECONDS)
        _close_stdin(proc)
        if close_pt:
            _close_plaintext(plaintext_iter)
        try:
            if proc.poll() is None:
                kill_confirmed = _kill_group(proc) and kill_confirmed
        except Exception:
            kill_confirmed = False
        with contextlib.suppress(Exception):
            drain.join(timeout=_DRAIN_JOIN_SECONDS)
        with contextlib.suppress(Exception):
            if proc.stderr is not None:
                proc.stderr.close()
        if exit_code is None:
            try:
                exit_code = proc.poll()
            except Exception:
                exit_code = None
                kill_confirmed = False

        with wd_lock:
            stats = _WatchdogStats(
                ops=tuple(wd_ops),
                max_sleep_seconds=wd_max_sleep,
            )
        _thread_local.watchdog_stats = stats
        try:
            helper_liveness = (watchdog.is_alive(), drain.is_alive())
        except Exception:
            helper_liveness = (True, True)
            kill_confirmed = False
        _thread_local.helper_liveness = helper_liveness

    wd_alive, dr_alive = helper_liveness
    io_failed = io_failed or reason_watchdog_failed.is_set()

    return _classify_result(
        exit_code=exit_code,
        bytes_written=bytes_written,
        process_started=True,
        reason_cancelled=reason_cancelled.is_set(),
        reason_timeout=reason_timeout.is_set(),
        broken_pipe=broken_pipe,
        plaintext_failed=plaintext_failed and not io_failed,
        io_failed=io_failed,
        kill_confirmed=kill_confirmed,
        watchdog_alive=wd_alive,
        drain_alive=dr_alive,
    )


def run_pg_restore_from_path(
    argv: list[str],
    plaintext_path: Path,
    *,
    assert_plaintext_path: Callable[[Path], None],
    env: dict[str, str] | None = None,
    cancel_flag: Callable[[], bool] | None = None,
    write_size: int = DEFAULT_WRITE_SIZE,
    write_poll_seconds: float = DEFAULT_WRITE_POLL_SECONDS,
    overall_timeout_seconds: float | None = None,
    close_plaintext: bool = True,
) -> RestoreProcessResult:
    """File-mode: assert + open before spawn; guarantee file closure."""

    if not isinstance(plaintext_path, Path):
        return _result(
            ok=False,
            code=CODE_PREFLIGHT,
            exit_code=None,
            bytes_written=0,
            process_started=False,
        )
    if not callable(assert_plaintext_path):
        return _result(
            ok=False,
            code=CODE_PREFLIGHT,
            exit_code=None,
            bytes_written=0,
            process_started=False,
        )

    # Validate run inputs before callback/open side effects where possible.
    if _validate_run_inputs(
        argv,
        env=env,
        cancel_flag=cancel_flag,
        write_size=write_size,
        write_poll_seconds=write_poll_seconds,
        overall_timeout_seconds=overall_timeout_seconds,
        close_plaintext=close_plaintext,
    ):
        return _result(
            ok=False,
            code=CODE_PREFLIGHT,
            exit_code=None,
            bytes_written=0,
            process_started=False,
        )

    try:
        assert_plaintext_path(plaintext_path)
    except Exception:
        return _result(
            ok=False,
            code=CODE_PREFLIGHT,
            exit_code=None,
            bytes_written=0,
            process_started=False,
        )

    try:
        handle = plaintext_path.open("rb")
    except (OSError, ValueError, TypeError, AttributeError):
        return _result(
            ok=False,
            code=CODE_PREFLIGHT,
            exit_code=None,
            bytes_written=0,
            process_started=False,
        )

    def _iter_file() -> Iterator[bytes]:
        try:
            chunk_size = (
                write_size
                if isinstance(write_size, int) and not isinstance(write_size, bool)
                else DEFAULT_WRITE_SIZE
            )
            while True:
                block = handle.read(chunk_size)
                if not block:
                    break
                yield block
        finally:
            with contextlib.suppress(Exception):
                handle.close()

    try:
        return run_pg_restore_stdin(
            argv,
            _iter_file(),
            env=env,
            cancel_flag=cancel_flag,
            write_size=write_size,
            write_poll_seconds=write_poll_seconds,
            overall_timeout_seconds=overall_timeout_seconds,
            close_plaintext=close_plaintext,
        )
    finally:
        # Guarantee closure even if generator never started / close_plaintext=False.
        with contextlib.suppress(Exception):
            handle.close()


def _write_chunk_nonblocking(
    fd: int,
    chunk: bytes,
    *,
    write_size: int,
    write_poll_seconds: float,
    kill_requested: threading.Event,
    overdue: Callable[[], bool],
    arm: Callable[[str], None],
) -> tuple[int, bool, bool]:
    """Write one chunk via exclusive raw-fd I/O.

    Returns ``(bytes_written, hit_epipe, select_failed)``.
    """

    offset = 0
    written = 0
    while offset < len(chunk):
        if kill_requested.is_set() or overdue():
            if overdue() and not kill_requested.is_set():
                arm(CODE_TIMEOUT)
            return written, False, False
        to_send = chunk[offset : offset + write_size]
        try:
            n = os.write(fd, to_send)
        except BlockingIOError:
            ok = _select_writable(fd, write_poll_seconds)
            if not ok:
                # Bounded backoff already spent in select; fail closed on hard error.
                if kill_requested.is_set() or overdue():
                    if overdue() and not kill_requested.is_set():
                        arm(CODE_TIMEOUT)
                    return written, False, False
                # select hard-failed: do not busy-spin
                return written, False, True
            if kill_requested.is_set() or overdue():
                if overdue() and not kill_requested.is_set():
                    arm(CODE_TIMEOUT)
                return written, False, False
            continue
        except BrokenPipeError:
            return written, True, False
        except OSError as error:
            if error.errno in {errno.EPIPE, errno.ECONNRESET}:
                return written, True, False
            if error.errno in {errno.EAGAIN, errno.EWOULDBLOCK}:
                ok = _select_writable(fd, write_poll_seconds)
                if not ok and not (kill_requested.is_set() or overdue()):
                    return written, False, True
                continue
            raise
        if n is None or n == 0:
            ok = _select_writable(fd, write_poll_seconds)
            if kill_requested.is_set() or overdue():
                if overdue() and not kill_requested.is_set():
                    arm(CODE_TIMEOUT)
                return written, False, False
            if not ok:
                return written, False, True
            continue
        offset += n
        written += n
    return written, False, False


def _select_writable(fd: int, timeout: float) -> bool:
    """Wait up to ``timeout`` for writability. False on hard select failure.

    Timeout with empty ready set returns True (caller re-checks Events) so we
    never busy-spin without sleeping the poll interval.
    """

    try:
        select.select([], [fd], [], timeout)
        return True
    except (ValueError, OSError, InterruptedError):
        # Hard failure or bad fd: backoff once then signal fail-closed.
        time.sleep(min(timeout, 0.05))
        return False


def _close_stdin(proc: subprocess.Popen[bytes]) -> None:
    stdin = proc.stdin
    if stdin is None:
        return
    with contextlib.suppress(Exception):
        stdin.close()
    proc.stdin = None


def _close_plaintext(plaintext_iter: object) -> None:
    """Best-effort close; suppress getattr and invocation failures."""

    try:
        close = getattr(plaintext_iter, "close", None)
    except Exception:
        return
    if callable(close):
        with contextlib.suppress(Exception):
            close()


def _kill_group(proc: subprocess.Popen[bytes]) -> bool:
    """TERM then KILL process group; return True only if reaped/confirmed dead.

    Never raises. Signal/wait failures are reflected by a False return so callers
    cannot claim restored/cancelled success under uncertain termination.
    """

    if proc.pid is None:
        try:
            return proc.poll() is not None
        except Exception:
            return False

    try:
        os.killpg(proc.pid, signal.SIGTERM)
    except ProcessLookupError:
        try:
            return proc.poll() is not None
        except Exception:
            return True
    except Exception:
        # Signal delivery failed; continue wait/KILL path.
        _ = None

    try:
        proc.wait(timeout=_KILL_WAIT_SECONDS)
        return True
    except Exception:
        # Wait failed; escalate to SIGKILL.
        _ = None

    try:
        os.killpg(proc.pid, signal.SIGKILL)
    except ProcessLookupError:
        try:
            return proc.poll() is not None
        except Exception:
            return True
    except Exception:
        # Kill delivery failed; final wait/poll.
        _ = None

    try:
        proc.wait(timeout=_KILL_WAIT_SECONDS)
        return True
    except Exception:
        try:
            return proc.poll() is not None
        except Exception:
            return False


def _classify_result(
    *,
    exit_code: int | None,
    bytes_written: int,
    process_started: bool,
    reason_cancelled: bool,
    reason_timeout: bool,
    broken_pipe: bool,
    plaintext_failed: bool,
    io_failed: bool,
    kill_confirmed: bool,
    watchdog_alive: bool,
    drain_alive: bool,
) -> RestoreProcessResult:
    # Unconfirmed termination outranks cancel/timeout/plaintext so uncertainty
    # never classifies as success or cancel/timeout. Then cancel/timeout >
    # plaintext > broken_pipe > io > restored/process_failed. Live helpers never
    # claim restored.
    if not kill_confirmed or io_failed:
        code = CODE_PROCESS_FAILED
    elif reason_cancelled:
        code = CODE_CANCELLED
    elif reason_timeout:
        code = CODE_TIMEOUT
    elif plaintext_failed:
        code = CODE_PLAINTEXT_FAILED
    elif broken_pipe:
        code = CODE_BROKEN_PIPE
    elif exit_code == 0 and not watchdog_alive and not drain_alive and exit_code is not None:
        code = CODE_RESTORED
    else:
        code = CODE_PROCESS_FAILED

    ok = code == CODE_RESTORED
    return _result(
        ok=ok,
        code=code,
        exit_code=exit_code,
        bytes_written=bytes_written,
        process_started=process_started,
    )


def _result(
    *,
    ok: bool,
    code: str,
    exit_code: int | None,
    bytes_written: int,
    process_started: bool,
) -> RestoreProcessResult:
    return RestoreProcessResult(
        ok=ok,
        code=code,
        exit_code=exit_code,
        bytes_written=bytes_written,
        process_started=process_started,
    )
