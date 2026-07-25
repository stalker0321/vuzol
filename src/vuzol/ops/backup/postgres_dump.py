"""Supervised docker exec pg_dump process-group pipeline (B2)."""

from __future__ import annotations

import contextlib
import os
import re
import signal
import subprocess
import threading
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from urllib.parse import unquote, urlparse

_CONTAINER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")
_IDENT_RE = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9_-]{0,62}$")


class PostgresDumpError(RuntimeError):
    def __init__(self, code: str, message: str = "") -> None:
        super().__init__(message or code)
        self.code = code


@dataclass(frozen=True, slots=True)
class DumpIdentity:
    user: str
    database: str
    password: str | None
    host: str


def parse_dump_identity(dsn: str) -> DumpIdentity:
    cleaned = dsn.strip()
    parsed = urlparse(cleaned)
    scheme = (parsed.scheme or "").lower()
    if "postgres" not in scheme:
        raise PostgresDumpError("preflight_dsn", "unsupported DSN scheme")
    user = unquote(parsed.username or "")
    database = unquote(parsed.path.lstrip("/") if parsed.path else "")
    password = unquote(parsed.password) if parsed.password is not None else None
    host = parsed.hostname or ""
    if not user or not _IDENT_RE.fullmatch(user):
        raise PostgresDumpError("preflight_dsn", "invalid database user")
    if not database or not _IDENT_RE.fullmatch(database):
        raise PostgresDumpError("preflight_dsn", "invalid database name")
    return DumpIdentity(user=user, database=database, password=password, host=host)


def build_pg_dump_argv(
    *,
    container: str,
    user: str,
    database: str,
    override: tuple[str, ...] | None = None,
) -> list[str]:
    if not _CONTAINER_RE.fullmatch(container):
        raise PostgresDumpError("preflight_postgres", "container name rejected")
    if not _IDENT_RE.fullmatch(user) or not _IDENT_RE.fullmatch(database):
        raise PostgresDumpError("preflight_postgres", "user/database rejected")
    if override is not None:
        argv = [
            part.replace("{container}", container)
            .replace("{user}", user)
            .replace("{database}", database)
            for part in override
        ]
        forbidden = {"bash", "sh", "zsh", "-c", "cmd.exe"}
        if any(part in forbidden for part in argv):
            raise PostgresDumpError("preflight_postgres", "shell override rejected")
        if any(any(ch in part for ch in ";|&`$") for part in argv):
            raise PostgresDumpError("preflight_postgres", "argv metacharacters rejected")
        return argv
    return [
        "docker",
        "exec",
        "-i",
        container,
        "pg_dump",
        "-U",
        user,
        "-d",
        database,
        "-Fc",
        "--no-owner",
        "--no-acl",
    ]


def iter_dump_stdout(
    argv: list[str],
    *,
    env: dict[str, str] | None = None,
    cancel_flag: Callable[[], bool] | None = None,
    read_size: int = 65_536,
    wait_timeout_seconds: float | None = None,
) -> Iterator[bytes]:
    """Spawn dump in a new process group; single stdout reader; bounded stderr drain."""

    if any(not isinstance(part, str) for part in argv):
        raise PostgresDumpError("preflight_postgres", "argv must be strings")
    proc_env = os.environ.copy()
    if env:
        proc_env.update(env)
    try:
        proc = subprocess.Popen(  # noqa: S603
            argv,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=proc_env,
            start_new_session=True,
        )
    except OSError as error:
        raise PostgresDumpError(
            "preflight_postgres", f"spawn failed: {type(error).__name__}"
        ) from error

    assert proc.stdout is not None
    assert proc.stderr is not None
    stderr_chunks: list[bytes] = []
    stderr_limit = 8 * 1024

    def _drain_stderr() -> None:
        assert proc.stderr is not None
        try:
            while True:
                block = proc.stderr.read(4096)
                if not block:
                    break
                stderr_chunks.append(block)
                total = sum(len(item) for item in stderr_chunks)
                while total > stderr_limit and stderr_chunks:
                    dropped = stderr_chunks.pop(0)
                    total -= len(dropped)
        except Exception:
            return

    drain_thread = threading.Thread(target=_drain_stderr, name="pg-dump-stderr", daemon=True)
    drain_thread.start()
    try:
        while True:
            if cancel_flag is not None and cancel_flag():
                _kill_group(proc)
                raise PostgresDumpError("cancelled", "dump cancelled")
            chunk = proc.stdout.read(read_size)
            if not chunk:
                break
            yield chunk
        if wait_timeout_seconds is None:
            code = proc.wait()
        else:
            code = proc.wait(timeout=wait_timeout_seconds)
        drain_thread.join(timeout=10.0)
        if code != 0:
            raise PostgresDumpError("pg_dump_failed", f"pg_dump exit {code}")
    except PostgresDumpError:
        _kill_group(proc)
        drain_thread.join(timeout=10.0)
        raise
    except Exception as error:
        _kill_group(proc)
        drain_thread.join(timeout=10.0)
        raise PostgresDumpError("pg_dump_failed", type(error).__name__) from error
    finally:
        with contextlib.suppress(Exception):
            proc.stdout.close()
        with contextlib.suppress(Exception):
            proc.stderr.close()


def _kill_group(proc: subprocess.Popen[bytes]) -> None:
    if proc.pid is None:
        return
    try:
        os.killpg(proc.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    try:
        proc.wait(timeout=5)
        return
    except subprocess.TimeoutExpired:
        pass
    try:
        os.killpg(proc.pid, signal.SIGKILL)
    except ProcessLookupError:
        return
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        return
