#!/usr/bin/env python3
"""Run the Vuzol suite offline with an isolated local PostgreSQL instance."""

import os
import shutil
import subprocess
import time
from pathlib import Path

POSTGRES_BIN = Path("/usr/lib/postgresql/15/bin")
TEMPLATE = Path("/opt/vuzol-postgres-template")
DATA = Path("/tmp/vuzol-postgres-data")  # noqa: S108
SOCKET = Path("/tmp/vuzol-postgres-socket")  # noqa: S108
TEST_TMP = Path("/workspace/.vuzol-validation-tmp")


def _run(*argv: str, env: dict[str, str] | None = None) -> None:
    subprocess.run(argv, check=True, env=env)  # noqa: S603 - image-owned fixed argv


def main() -> int:
    shutil.rmtree(DATA, ignore_errors=True)
    shutil.rmtree(SOCKET, ignore_errors=True)
    shutil.rmtree(TEST_TMP, ignore_errors=True)
    TEST_TMP.mkdir(mode=0o700)
    _run("/usr/bin/setfacl", "-b", "-k", str(TEST_TMP))
    shutil.copytree(TEMPLATE, DATA)
    for root, directories, files in os.walk(DATA):
        os.chmod(root, 0o700)
        for name in directories:
            os.chmod(Path(root) / name, 0o700)
        for name in files:
            os.chmod(Path(root) / name, 0o600)
    SOCKET.mkdir(mode=0o700)
    log = (DATA / "postgres.log").open("wb")
    postgres = subprocess.Popen(  # noqa: S603 - image-owned absolute binary
        (
            str(POSTGRES_BIN / "postgres"),
            "-D",
            str(DATA),
            "-k",
            str(SOCKET),
            "-c",
            "listen_addresses=",
            "-c",
            "shared_buffers=16MB",
            "-c",
            "max_connections=20",
            "-c",
            "dynamic_shared_memory_type=mmap",
        ),
        stdin=subprocess.DEVNULL,
        stdout=log,
        stderr=subprocess.STDOUT,
    )
    try:
        for _ in range(100):
            ready = subprocess.run(  # noqa: S603 - image-owned absolute binary
                (str(POSTGRES_BIN / "pg_isready"), "-h", str(SOCKET)),
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            if ready.returncode == 0:
                break
            if postgres.poll() is not None:
                raise RuntimeError("offline PostgreSQL exited during startup")
            time.sleep(0.05)
        else:
            raise RuntimeError("offline PostgreSQL did not become ready")
        _run(
            str(POSTGRES_BIN / "createuser"),
            "-h",
            str(SOCKET),
            "-U",
            "postgres",
            "vuzol",
        )
        _run(
            str(POSTGRES_BIN / "createdb"),
            "-h",
            str(SOCKET),
            "-U",
            "postgres",
            "-O",
            "vuzol",
            "vuzol_test",
        )
        environment = dict(os.environ)
        environment["TMPDIR"] = str(TEST_TMP)
        sync_dsn = "postgresql://vuzol@/vuzol_test?host=/tmp/vuzol-postgres-socket"
        async_dsn = sync_dsn.replace("postgresql://", "postgresql+psycopg://", 1)
        environment["VUZOL_TEST_DATABASE_DSN"] = sync_dsn
        migration_environment = dict(environment)
        migration_environment["VUZOL_DATABASE_DSN_REFERENCE"] = "env:VUZOL_DATABASE_DSN"
        migration_environment["VUZOL_DATABASE_DSN"] = async_dsn
        _run(
            "/opt/vuzol-validation/bin/alembic",
            "upgrade",
            "head",
            env=migration_environment,
        )
        completed = subprocess.run(
            (
                "/opt/vuzol-validation/bin/pytest",
                "-m",
                "not docker",
                "-o",
                "cache_dir=/tmp/pytest-cache",
            ),
            check=False,
            env=environment,
        )
        return completed.returncode
    finally:
        postgres.terminate()
        try:
            postgres.wait(timeout=10)
        except subprocess.TimeoutExpired:
            postgres.kill()
            postgres.wait()
        log.close()
        shutil.rmtree(DATA, ignore_errors=True)
        shutil.rmtree(SOCKET, ignore_errors=True)
        shutil.rmtree(TEST_TMP, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
