"""Backup B2 capture orchestration: partial encrypted postgres dump to local staging."""

from __future__ import annotations

import contextlib
import math
import socket
import uuid
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from vuzol.config.settings import BackupSettings, Settings
from vuzol.ops.backup.crypto import (
    BackupCryptoError,
    encrypt_blob_stream,
    generate_dek,
    load_kek_from_env_value,
    load_kek_from_file_bytes,
    wrap_dek,
)
from vuzol.ops.backup.locks import (
    BACKUP_CAPTURE_LOCK_KEY,
    RETENTION_SWEEP_LOCK_KEY,
    advisory_unlock,
    try_advisory_lock,
)
from vuzol.ops.backup.manifest import (
    BackupComponent,
    BackupManifest,
    store_manifest,
    validate_manifest,
)
from vuzol.ops.backup.paths import ProductionRoots, normalize_dsn_identity
from vuzol.ops.backup.postgres_dump import (
    DumpIdentity,
    PostgresDumpError,
    build_pg_dump_argv,
    iter_dump_stdout,
    parse_dump_identity,
)
from vuzol.ops.backup.staging import (
    STATE_DUMPING,
    STATE_FAILED,
    STATE_MANIFESTING,
    assert_safe_staging_root,
    cleanup_run_dir,
    ensure_staging_tree,
    free_space_bytes,
    prune_published_runs,
    publish_run,
    write_state,
)


class CaptureMode(StrEnum):
    DRY_RUN = "dry_run"
    APPLY = "apply"


@dataclass(frozen=True, slots=True)
class CaptureReport:
    ok: bool
    mode: CaptureMode
    code: str
    message: str
    run_id: str | None = None
    schedule: str = "disabled"
    detail: dict[str, Any] | None = None

    def to_operational_payload(self) -> dict[str, object]:
        return {
            "ok": self.ok,
            "mode": self.mode.value,
            "code": self.code,
            "message": self.message,
            "run_id": self.run_id,
            "schedule": self.schedule,
            "detail": self.detail or {},
        }


class BackupCaptureRunner:
    def __init__(
        self,
        settings: Settings,
        *,
        dsn: str | None = None,
        kek_bytes: bytes | None = None,
        dump_stream_factory: Callable[[list[str]], Iterable[bytes]] | None = None,
    ) -> None:
        self._settings = settings
        self._backup = settings.backup
        self._dsn = dsn
        self._kek_bytes = kek_bytes
        self._dump_stream_factory = dump_stream_factory

    async def run(self, mode: CaptureMode = CaptureMode.DRY_RUN) -> CaptureReport:
        backup = self._backup
        if mode is CaptureMode.APPLY and not backup.capture_cli_permitted:
            return CaptureReport(
                ok=False,
                mode=mode,
                code="capture_not_permitted",
                message="capture_cli_permitted is false",
            )
        if backup.staging_root is None:
            return CaptureReport(
                ok=False,
                mode=mode,
                code="preflight_staging",
                message="staging_root is required",
            )
        try:
            production = ProductionRoots(
                repository_root=self._settings.repository_root,
                worktree_root=self._settings.worktree_root,
                artifact_root=self._settings.artifact_root,
                secret_file_root=self._settings.secret_file_root,
            )
            staging = assert_safe_staging_root(backup.staging_root, production)
        except Exception as error:
            return CaptureReport(
                ok=False,
                mode=mode,
                code="preflight_path_conflict",
                message=type(error).__name__,
            )

        dsn = self._dsn
        if dsn is None:
            return CaptureReport(
                ok=False,
                mode=mode,
                code="preflight_dsn",
                message="database DSN not provided",
            )
        try:
            identity = parse_dump_identity(dsn)
            host, _port, _db = normalize_dsn_identity(dsn)
        except Exception as error:
            return CaptureReport(
                ok=False,
                mode=mode,
                code="preflight_dsn",
                message=type(error).__name__,
            )
        allowed_hosts = {
            "127.0.0.1",
            "localhost",
            "::1",
            "unix:default",
            "postgres",
            "vuzol-postgres-1",
        }
        if (
            not backup.allow_non_loopback_dump
            and host not in allowed_hosts
            and not host.startswith("unix:")
        ):
            return CaptureReport(
                ok=False,
                mode=mode,
                code="preflight_dsn",
                message="non-loopback dump refused",
            )

        try:
            argv = build_pg_dump_argv(
                container=backup.postgres_container,
                user=identity.user,
                database=identity.database,
                override=backup.postgres_dump_argv,
            )
        except PostgresDumpError as error:
            return CaptureReport(ok=False, mode=mode, code=error.code, message=str(error))

        if mode is CaptureMode.DRY_RUN:
            return CaptureReport(
                ok=True,
                mode=mode,
                code="would_capture",
                message="dry-run preflight ok",
                detail={
                    "schedule": "disabled",
                    "argv": [part if part != identity.user else "<user>" for part in argv],
                    "staging_root": str(staging),
                    "database": identity.database,
                },
            )

        kek = self._kek_bytes
        if kek is None:
            try:
                kek = _load_kek(self._settings, backup)
            except Exception as error:
                return CaptureReport(
                    ok=False,
                    mode=mode,
                    code="preflight_kek",
                    message=type(error).__name__,
                )

        engine: AsyncEngine | None = None
        try:
            engine = create_async_engine(dsn, pool_pre_ping=True)
            connection = await engine.connect()
        except Exception as error:
            if engine is not None:
                with contextlib.suppress(Exception):
                    await engine.dispose()
            return CaptureReport(
                ok=False,
                mode=mode,
                code="capture_failed",
                message=type(error).__name__,
            )

        run_id = uuid.uuid4()
        run_dir: Path | None = None
        backup_locked = False
        try:
            # E1: probe retention first (same connection briefly is ok for try+unlock)
            retention_held = await try_advisory_lock(connection, RETENTION_SWEEP_LOCK_KEY)
            if retention_held:
                await advisory_unlock(connection, RETENTION_SWEEP_LOCK_KEY)
            else:
                # try_lock false means another session holds it
                return CaptureReport(
                    ok=False,
                    mode=mode,
                    code="lock_busy_retention",
                    message="retention lock held",
                )
            # Wait - if retention is free, try_lock succeeds and we unlock. If held, try fails.
            # Correct: if try returns False, busy. If True, we acquired - unlock immediately.
            # Good.

            acquired = await try_advisory_lock(connection, BACKUP_CAPTURE_LOCK_KEY)
            if not acquired:
                return CaptureReport(
                    ok=False, mode=mode, code="lock_busy", message="backup lock held"
                )
            backup_locked = True

            staging.mkdir(parents=True, exist_ok=True)
            # re-check after mkdir
            staging = assert_safe_staging_root(staging, production)

            db_size = await connection.scalar(text("SELECT pg_database_size(current_database())"))
            if db_size is None:
                return CaptureReport(
                    ok=False, mode=mode, code="preflight_db_size", message="size query failed"
                )
            db_size_i = int(db_size)
            overhead = 16 * max(1, math.ceil(db_size_i / 1_048_576)) + 4096 + 32_000_000
            need = max(backup.min_free_bytes, int(db_size_i * 1.5) + overhead)
            avail = free_space_bytes(staging)
            if avail < need:
                return CaptureReport(
                    ok=False,
                    mode=mode,
                    code="preflight_disk",
                    message="insufficient free space",
                    detail={"avail": avail, "need": need},
                )

            alembic = await connection.scalar(
                text("SELECT version_num FROM alembic_version LIMIT 1")
            )
            alembic_s = str(alembic or "unknown")

            run_dir, tmp, publish = ensure_staging_tree(staging, run_id)
            write_state(run_dir, STATE_DUMPING)
            t_start = datetime.now(UTC)
            dek = generate_dek()
            enc_path = tmp / "postgres.dump.enc"
            wrap_path = tmp / "dek.wrap"
            try:
                stream = self._open_dump_stream(argv, identity)
                result = encrypt_blob_stream(
                    dek=dek,
                    run_id=run_id,
                    component="postgres",
                    fmt="pg_custom",
                    plaintext_iter=stream,
                    out_path=enc_path,
                )
                wrap_dek(kek=kek, dek=dek, run_id=run_id, out_path=wrap_path)
            except (PostgresDumpError, BackupCryptoError) as error:
                write_state(run_dir, STATE_FAILED)
                cleanup_run_dir(run_dir, staging)
                code = getattr(error, "code", "encrypt_failed")
                return CaptureReport(ok=False, mode=mode, code=str(code), message=str(error))
            finally:
                # best-effort zeroize
                dek = b"\x00" * len(dek)

            write_state(run_dir, STATE_MANIFESTING)
            t_end = datetime.now(UTC)
            import hashlib

            head_hash = hashlib.sha256(alembic_s.encode()).hexdigest()
            component = BackupComponent(
                filename="postgres.dump.enc",
                sha256_ciphertext=result.sha256_ciphertext,
                size_ciphertext=result.size_ciphertext,
                cipher="aes-256-gcm",
                format="pg_custom",
            )
            manifest = BackupManifest.model_validate(
                {
                    "run_id": run_id,
                    "created_at": t_start,
                    "t_start": t_start,
                    "t_end": t_end,
                    "hostname": _safe_hostname(),
                    "app": {
                        "git_commit": "0" * 40,
                        "deploy_path": str(backup.deploy_path),
                        "service_name": self._settings.service_name,
                    },
                    "schema": {
                        "alembic_head_expected": head_hash,
                        "alembic_head_observed": head_hash,
                    },
                    "components": {"postgres": component},
                    "partial": True,
                    "artifact_reconciliation": {
                        "db_rows": 0,
                        "fs_objects": 0,
                        "missing_blobs": [],
                        "orphan_files": [],
                        "skipped_symlinks": 0,
                    },
                    "config": {"registry_revision": "0" * 64, "files": []},
                    "rpo_rto": {
                        "rpo_seconds_target": backup.rpo_seconds_target,
                        "rto_seconds_target": backup.rto_seconds_target,
                    },
                    "quiesce": {"mode": backup.quiesce_mode, "duration_seconds": 0},
                    "retention": {
                        "keep_local_runs": backup.keep_local_runs,
                        "keep_offhost_days": backup.keep_offhost_days,
                    },
                }
            )
            validated = validate_manifest(manifest)

            manifest_path = tmp / "manifest.v1.json"
            digest = store_manifest(manifest_path, validated)
            sha_path = tmp / "manifest.sha256"
            sha_path.write_text(digest + "\n", encoding="utf-8")
            publish_run(
                run_dir=run_dir,
                tmp=tmp,
                publish=publish,
                files={
                    "postgres.dump.enc": enc_path,
                    "dek.wrap": wrap_path,
                    "manifest.v1.json": manifest_path,
                    "manifest.sha256": sha_path,
                },
            )
            prune_published_runs(staging, keep=backup.keep_local_runs)
            return CaptureReport(
                ok=True,
                mode=mode,
                code="captured",
                message="partial postgres backup published",
                run_id=str(run_id),
                detail={
                    "schedule": "disabled",
                    "sha256_ciphertext": result.sha256_ciphertext,
                    "size_ciphertext": result.size_ciphertext,
                    "partial": True,
                },
            )
        except Exception as error:
            if run_dir is not None and run_dir.exists():
                with contextlib.suppress(Exception):
                    write_state(run_dir, STATE_FAILED)
                    if not backup.keep_failed_runs:
                        cleanup_run_dir(run_dir, staging)
            return CaptureReport(
                ok=False,
                mode=mode,
                code="capture_failed",
                message=type(error).__name__,
            )
        finally:
            if backup_locked:
                with contextlib.suppress(Exception):
                    await advisory_unlock(connection, BACKUP_CAPTURE_LOCK_KEY)
            await connection.close()
            await engine.dispose()

    def _open_dump_stream(self, argv: list[str], identity: DumpIdentity) -> Iterable[bytes]:
        if self._dump_stream_factory is not None:
            return self._dump_stream_factory(argv)
        env = None
        if identity.password:
            env = {"PGPASSWORD": identity.password}
        return iter_dump_stdout(argv, env=env)


def _load_kek(settings: Settings, backup: BackupSettings) -> bytes:
    ref = backup.kek_reference
    if not ref:
        raise BackupCryptoError("kek_reference missing")
    if ref.startswith("file:"):
        name = ref.split(":", 1)[1]
        path = settings.secret_file_root / name
        return load_kek_from_file_bytes(path.read_bytes())
    if ref.startswith("env:"):
        import os

        key = ref.split(":", 1)[1]
        value = os.environ.get(key)
        if value is None:
            raise BackupCryptoError("kek env missing")
        return load_kek_from_env_value(value)
    raise BackupCryptoError("unsupported kek_reference scheme")


def _safe_hostname() -> str:
    name = socket.gethostname()
    return "".join(ch if ch.isalnum() or ch in ".-_" else "-" for ch in name)[:100] or "unknown"
