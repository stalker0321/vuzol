"""B3.3 pure restore orchestration (default dry-run; dormant APPLY; no CLI/settings).

Sequences B3.0 package preflight, B3.1 target preflight, optional crypto verify,
and APPLY decrypt→supervised B3.2 pg_restore. Never retrieves secrets, never
writes CLI/config, never claims full restore. Operational payloads stay
path/DSN/key/stderr-free. Schedule is always ``disabled``.
"""

from __future__ import annotations

import contextlib
import inspect
import stat
import uuid
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any

from vuzol.ops.backup.crypto import BackupCryptoError, decrypt_blob_stream, unwrap_dek
from vuzol.ops.backup.paths import ProductionRoots, resolve_isolation_path
from vuzol.ops.backup.postgres_restore import (
    CODE_RESTORED,
    RestoreProcessResult,
    build_pg_restore_argv,
    run_pg_restore_stdin,
)
from vuzol.ops.backup.restore import (
    PUBLISH_BLOB,
    PUBLISH_MANIFEST,
    PUBLISH_WRAP,
    PackagePreflightReport,
    preflight_published_package,
)
from vuzol.ops.backup.restore_target import (
    TargetPreflightReport,
    preflight_restore_target,
)
from vuzol.ops.backup.staging import assert_safe_staging_root

CODE_WOULD_RESTORE = "would_restore"
CODE_CRYPTO_VERIFIED = "crypto_verified"
CODE_RESTORED_ORCH = "restored"
CODE_NOT_PERMITTED = "restore_not_permitted"
CODE_PREFLIGHT_KEK = "preflight_kek"
CODE_WRAP_AUTH = "wrap_auth_failed"
CODE_BLOB_AUTH = "blob_auth_failed"
CODE_CRYPTO = "crypto_failed"
CODE_LOCK_BUSY = "lock_busy_capture"
CODE_TARGET_NOT_EMPTY = "preflight_target_not_empty"
CODE_PACKAGE_REBIND = "preflight_package"
CODE_FAILED = "restore_failed"
CODE_PREFLIGHT_POSTGRES = "preflight_postgres"

_KEK_LEN = 32


class RestoreMode(StrEnum):
    DRY_RUN = "dry_run"
    APPLY = "apply"


@dataclass(frozen=True, slots=True)
class PublishedPackageHandle:
    """In-process sealed paths after successful package preflight. Never on payload."""

    run_id: uuid.UUID
    resolved_staging_root: Path
    resolved_run_dir: Path
    resolved_publish_dir: Path
    wrap_path: Path
    blob_path: Path
    manifest_path: Path
    package_report: PackagePreflightReport


@dataclass(frozen=True, slots=True)
class RestoreOrchestrationReport:
    """Redacted orchestration report — no paths, DSN, KEK, DEK, argv, or stderr."""

    ok: bool
    code: str
    message: str
    mode: str
    run_id: str | None = None
    partial: bool | None = None
    package_code: str | None = None
    target_code: str | None = None
    process_code: str | None = None
    process_started: bool = False
    bytes_written: int = 0
    target_may_be_dirty: bool = False
    host: str | None = None
    port: int | None = None
    database: str | None = None
    schedule: str = "disabled"

    def to_operational_payload(self) -> dict[str, object]:
        return {
            "ok": self.ok,
            "code": self.code,
            "message": self.message,
            "mode": self.mode,
            "run_id": self.run_id,
            "partial": self.partial,
            "package_code": self.package_code,
            "target_code": self.target_code,
            "process_code": self.process_code,
            "process_started": self.process_started,
            "bytes_written": self.bytes_written,
            "target_may_be_dirty": self.target_may_be_dirty,
            "host": self.host,
            "port": self.port,
            "database": self.database,
            "schedule": self.schedule,
        }


async def run_restore_orchestration(
    *,
    mode: RestoreMode | str = RestoreMode.DRY_RUN,
    apply_authorized: bool = False,
    staging_root: Path,
    run_id: uuid.UUID | str,
    production: ProductionRoots,
    production_dsn: str,
    restore_dsn: str,
    drill_root: Path,
    required_database_suffix: str = "_restore",
    allow_local_hosts_only: bool = True,
    kek: bytes | None = None,
    verify_crypto: bool = False,
    postgres_container: str = "vuzol-postgres-1",
    restore_user: str = "vuzol",
    restore_database: str = "vuzol_restore",
    overall_timeout_seconds: float | None = None,
    cancel_flag: Callable[[], bool] | None = None,
    assert_empty_target: Callable[[], Any] | None = None,
    probe_capture_lock: Callable[[], Any] | None = None,
    package_preflight: Callable[..., PackagePreflightReport] = preflight_published_package,
    target_preflight: Callable[..., TargetPreflightReport] = preflight_restore_target,
    unwrap: Callable[..., bytes] = unwrap_dek,
    decrypt_stream: Callable[..., Iterator[bytes]] = decrypt_blob_stream,
    run_restore: Callable[..., RestoreProcessResult] = run_pg_restore_stdin,
    build_argv: Callable[..., list[str]] = build_pg_restore_argv,
    # Test seam: sealed handle without filesystem rebind (defaults re-bind after preflight).
    bind_package_handle: (
        Callable[..., PublishedPackageHandle | RestoreOrchestrationReport] | None
    ) = None,
) -> RestoreOrchestrationReport:
    """Compose lower-layer preflights and optional APPLY restore (pure library)."""

    # Normalize mode before any branch so str "apply" cannot skip apply_authorized.
    if isinstance(mode, RestoreMode):
        resolved_mode = mode
    else:
        try:
            resolved_mode = RestoreMode(mode)
        except (ValueError, TypeError):
            return _report(
                ok=False,
                code=CODE_FAILED,
                message="invalid restore mode",
                mode="invalid",
            )
    mode_s = resolved_mode.value
    binder = bind_package_handle or _default_bind_package_handle

    # verify_crypto must be an actual bool before any preflight / lower-layer work.
    if not isinstance(verify_crypto, bool):
        return _report(
            ok=False,
            code=CODE_FAILED,
            message="invalid verify mode",
            mode=mode_s,
        )

    # Only actual singleton True authorizes APPLY; truthy 1/"yes"/etc refuse.
    if resolved_mode is RestoreMode.APPLY and apply_authorized is not True:
        return _report(
            ok=False,
            code=CODE_NOT_PERMITTED,
            message="APPLY requires apply_authorized=True",
            mode=mode_s,
        )

    # --- dry-run / verify: single package+target pass ---
    if resolved_mode is RestoreMode.DRY_RUN:
        package = package_preflight(
            staging_root=staging_root,
            run_id=run_id,
            production=production,
        )
        if not package.ok:
            return _from_package_fail(package, mode_s)
        target = target_preflight(
            production_dsn=production_dsn,
            restore_dsn=restore_dsn,
            production=production,
            drill_root=drill_root,
            required_database_suffix=required_database_suffix,
            allow_local_hosts_only=allow_local_hosts_only,
        )
        if not target.ok:
            return _from_target_fail(target, mode_s, package=package)

        if verify_crypto is False:
            return _report(
                ok=True,
                code=CODE_WOULD_RESTORE,
                message="dry-run: package and target preflight ok",
                mode=mode_s,
                run_id=package.run_id,
                partial=True,
                package_code=package.code,
                target_code=target.code,
                host=target.host,
                port=target.port,
                database=target.database,
            )

        return await _verify_crypto(
            package=package,
            target=target,
            staging_root=staging_root,
            run_id=run_id,
            production=production,
            kek=kek,
            unwrap=unwrap,
            decrypt_stream=decrypt_stream,
            mode_s=mode_s,
            binder=binder,
        )

    # --- APPLY: package1 -> target1 -> lock -> empty -> package2 -> target2 -> crypto/B3.2 ---
    package = package_preflight(
        staging_root=staging_root,
        run_id=run_id,
        production=production,
    )
    if not package.ok:
        return _from_package_fail(package, mode_s)
    target = target_preflight(
        production_dsn=production_dsn,
        restore_dsn=restore_dsn,
        production=production,
        drill_root=drill_root,
        required_database_suffix=required_database_suffix,
        allow_local_hosts_only=allow_local_hosts_only,
    )
    if not target.ok:
        return _from_target_fail(target, mode_s, package=package)

    if probe_capture_lock is not None:
        free = await _maybe_await(probe_capture_lock())
        # Only explicit True may proceed; False/None/other refuse before pass two.
        if free is not True:
            return _report(
                ok=False,
                code=CODE_LOCK_BUSY,
                message="capture lock held",
                mode=mode_s,
                run_id=package.run_id,
                package_code=package.code,
                target_code=target.code,
                host=target.host,
                port=target.port,
                database=target.database,
            )

    if assert_empty_target is not None:
        try:
            await _maybe_await(assert_empty_target())
        except Exception:
            return _report(
                ok=False,
                code=CODE_TARGET_NOT_EMPTY,
                message="restore target is not empty",
                mode=mode_s,
                run_id=package.run_id,
                package_code=package.code,
                target_code=target.code,
                host=target.host,
                port=target.port,
                database=target.database,
            )

    # TOCTOU second pass — discard first-pass paths; no first-pass open after this.
    package = package_preflight(
        staging_root=staging_root,
        run_id=run_id,
        production=production,
    )
    if not package.ok:
        return _from_package_fail(package, mode_s)
    target = target_preflight(
        production_dsn=production_dsn,
        restore_dsn=restore_dsn,
        production=production,
        drill_root=drill_root,
        required_database_suffix=required_database_suffix,
        allow_local_hosts_only=allow_local_hosts_only,
    )
    if not target.ok:
        return _from_target_fail(target, mode_s, package=package)

    handle = binder(
        staging_root=staging_root,
        run_id=run_id,
        production=production,
        package_report=package,
        mode_s=mode_s,
    )
    if isinstance(handle, RestoreOrchestrationReport):
        return handle

    if not _is_key_bytes32(kek):
        return _report(
            ok=False,
            code=CODE_PREFLIGHT_KEK,
            message="KEK missing or invalid length",
            mode=mode_s,
            run_id=package.run_id,
            package_code=package.code,
            target_code=target.code,
            host=target.host,
            port=target.port,
            database=target.database,
        )
    # Narrowed for type checkers; runtime already checked actual bytes len 32.
    assert isinstance(kek, bytes)

    try:
        argv = build_argv(
            container=postgres_container,
            user=restore_user,
            database=restore_database,
            override=None,
            force_clean_isolated=False,
        )
    except Exception:
        return _report(
            ok=False,
            code=CODE_PREFLIGHT_POSTGRES,
            message="restore argv rejected",
            mode=mode_s,
            run_id=package.run_id,
            package_code=package.code,
            target_code=target.code,
            host=target.host,
            port=target.port,
            database=target.database,
        )

    dek_buf: bytearray | None = None
    gen: Iterator[bytes] | None = None
    try:
        try:
            dek = unwrap(
                kek=kek,
                wrap_path=handle.wrap_path,
                expected_run_id=handle.run_id,
            )
        except BackupCryptoError:
            return _report(
                ok=False,
                code=CODE_WRAP_AUTH,
                message="dek wrap authentication failed",
                mode=mode_s,
                run_id=package.run_id,
                package_code=package.code,
                target_code=target.code,
                host=target.host,
                port=target.port,
                database=target.database,
            )
        except Exception:
            return _report(
                ok=False,
                code=CODE_CRYPTO,
                message="unwrap failed",
                mode=mode_s,
                run_id=package.run_id,
                package_code=package.code,
                target_code=target.code,
                host=target.host,
                port=target.port,
                database=target.database,
            )
        if not _is_key_bytes32(dek):
            return _report(
                ok=False,
                code=CODE_CRYPTO,
                message="unwrap failed",
                mode=mode_s,
                run_id=package.run_id,
                package_code=package.code,
                target_code=target.code,
                host=target.host,
                port=target.port,
                database=target.database,
            )
        dek_buf = bytearray(dek)
        try:
            try:
                gen = decrypt_stream(
                    dek=bytes(dek_buf),
                    blob_path=handle.blob_path,
                    run_id=handle.run_id,
                    component="postgres",
                    fmt="pg_custom",
                )
            except BackupCryptoError:
                return _report(
                    ok=False,
                    code=CODE_BLOB_AUTH,
                    message="blob authentication failed",
                    mode=mode_s,
                    run_id=package.run_id,
                    package_code=package.code,
                    target_code=target.code,
                    host=target.host,
                    port=target.port,
                    database=target.database,
                )
            except Exception:
                return _report(
                    ok=False,
                    code=CODE_CRYPTO,
                    message="decrypt failed",
                    mode=mode_s,
                    run_id=package.run_id,
                    package_code=package.code,
                    target_code=target.code,
                    host=target.host,
                    port=target.port,
                    database=target.database,
                )
            try:
                # Pass cancel_flag / deadline unchanged; own the generator (close_plaintext=True).
                result = run_restore(
                    argv,
                    gen,
                    overall_timeout_seconds=overall_timeout_seconds,
                    cancel_flag=cancel_flag,
                    close_plaintext=True,
                )
            except Exception:
                # Ordinary Exception: fail-closed dirty (spawn state may be ambiguous).
                # BaseException is not caught so it propagates after finally close/wipe.
                return _report(
                    ok=False,
                    code=CODE_FAILED,
                    message="restore process raised",
                    mode=mode_s,
                    run_id=package.run_id,
                    package_code=package.code,
                    target_code=target.code,
                    process_started=True,
                    target_may_be_dirty=True,
                    host=target.host,
                    port=target.port,
                    database=target.database,
                )
            process_started = bool(result.process_started)
            bytes_written = int(result.bytes_written)
            process_code = result.code
            # Success requires B3.2 invariant: ok, restored, process started.
            ok = bool(result.ok and result.code == CODE_RESTORED and process_started)
            # Dirty iff process started and orchestration non-success (incl. kill uncertainty).
            dirty = bool(process_started and not ok)
            return _report(
                ok=ok,
                code=CODE_RESTORED_ORCH if ok else result.code,
                message=(
                    "partial postgres restore completed"
                    if ok
                    else "partial postgres restore did not complete"
                ),
                mode=mode_s,
                run_id=package.run_id,
                partial=True,
                package_code=package.code,
                target_code=target.code,
                process_code=process_code,
                process_started=process_started,
                bytes_written=bytes_written,
                target_may_be_dirty=dirty,
                host=target.host,
                port=target.port,
                database=target.database,
            )
        finally:
            if gen is not None:
                _close_generator(gen)
    finally:
        if dek_buf is not None:
            _zeroize(dek_buf)


async def _verify_crypto(
    *,
    package: PackagePreflightReport,
    target: TargetPreflightReport,
    staging_root: Path,
    run_id: uuid.UUID | str,
    production: ProductionRoots,
    kek: bytes | None,
    unwrap: Callable[..., bytes],
    decrypt_stream: Callable[..., Iterator[bytes]],
    mode_s: str,
    binder: Callable[..., PublishedPackageHandle | RestoreOrchestrationReport],
) -> RestoreOrchestrationReport:
    if not _is_key_bytes32(kek):
        return _report(
            ok=False,
            code=CODE_PREFLIGHT_KEK,
            message="KEK missing or invalid length",
            mode=mode_s,
            run_id=package.run_id,
            package_code=package.code,
            target_code=target.code,
            host=target.host,
            port=target.port,
            database=target.database,
        )
    assert isinstance(kek, bytes)

    handle = binder(
        staging_root=staging_root,
        run_id=run_id,
        production=production,
        package_report=package,
        mode_s=mode_s,
    )
    if isinstance(handle, RestoreOrchestrationReport):
        return handle

    dek_buf: bytearray | None = None
    gen: Iterator[bytes] | None = None
    try:
        try:
            dek = unwrap(
                kek=kek,
                wrap_path=handle.wrap_path,
                expected_run_id=handle.run_id,
            )
        except BackupCryptoError:
            return _report(
                ok=False,
                code=CODE_WRAP_AUTH,
                message="dek wrap authentication failed",
                mode=mode_s,
                run_id=package.run_id,
                package_code=package.code,
                target_code=target.code,
                host=target.host,
                port=target.port,
                database=target.database,
            )
        except Exception:
            return _report(
                ok=False,
                code=CODE_CRYPTO,
                message="unwrap failed",
                mode=mode_s,
                run_id=package.run_id,
                package_code=package.code,
                target_code=target.code,
                host=target.host,
                port=target.port,
                database=target.database,
            )
        if not _is_key_bytes32(dek):
            return _report(
                ok=False,
                code=CODE_CRYPTO,
                message="unwrap failed",
                mode=mode_s,
                run_id=package.run_id,
                package_code=package.code,
                target_code=target.code,
                host=target.host,
                port=target.port,
                database=target.database,
            )
        dek_buf = bytearray(dek)
        try:
            try:
                gen = decrypt_stream(
                    dek=bytes(dek_buf),
                    blob_path=handle.blob_path,
                    run_id=handle.run_id,
                    component="postgres",
                    fmt="pg_custom",
                )
                # Fully consume: streaming decrypt rejects missing final auth chunk.
                for _chunk in gen:
                    pass
            except BackupCryptoError:
                return _report(
                    ok=False,
                    code=CODE_BLOB_AUTH,
                    message="blob authentication failed",
                    mode=mode_s,
                    run_id=package.run_id,
                    package_code=package.code,
                    target_code=target.code,
                    host=target.host,
                    port=target.port,
                    database=target.database,
                )
            except Exception:
                return _report(
                    ok=False,
                    code=CODE_CRYPTO,
                    message="decrypt failed",
                    mode=mode_s,
                    run_id=package.run_id,
                    package_code=package.code,
                    target_code=target.code,
                    host=target.host,
                    port=target.port,
                    database=target.database,
                )
            return _report(
                ok=True,
                code=CODE_CRYPTO_VERIFIED,
                message="dry-run crypto verify ok",
                mode=mode_s,
                run_id=package.run_id,
                partial=True,
                package_code=package.code,
                target_code=target.code,
                host=target.host,
                port=target.port,
                database=target.database,
            )
        finally:
            if gen is not None:
                _close_generator(gen)
    finally:
        if dek_buf is not None:
            _zeroize(dek_buf)


def _is_key_bytes32(value: object) -> bool:
    """True only for actual ``bytes`` of length 32 (not bytearray / memoryview)."""

    return isinstance(value, bytes) and len(value) == _KEK_LEN


def _default_bind_package_handle(
    *,
    staging_root: Path,
    run_id: uuid.UUID | str,
    production: ProductionRoots,
    package_report: PackagePreflightReport,
    mode_s: str,
) -> PublishedPackageHandle | RestoreOrchestrationReport:
    """Re-bind publish paths under staging after a successful package preflight.

    Mirrors current B3.0 containment: resolved staging/run/publish plus
    ``lstat``-style non-symlink regular-file checks for wrap/blob/manifest.
    Handles and paths never enter operational payloads.
    """

    try:
        run_uuid = run_id if isinstance(run_id, uuid.UUID) else uuid.UUID(str(run_id))
    except (ValueError, TypeError, AttributeError):
        return _report(
            ok=False,
            code=CODE_PACKAGE_REBIND,
            message="run_id is not a valid UUID",
            mode=mode_s,
            package_code=package_report.code,
        )
    # Sealed identity: only a successful second-pass report matching this run_id.
    # Never echo package_report.run_id on mismatch — injected values may carry secrets.
    if package_report.ok is not True or package_report.run_id != str(run_uuid):
        return _report(
            ok=False,
            code=CODE_PACKAGE_REBIND,
            message="package report identity rejected",
            mode=mode_s,
            package_code=package_report.code,
            run_id=None,
        )
    try:
        staging = assert_safe_staging_root(staging_root, production)
        run_dir = resolve_isolation_path(staging / "runs" / str(run_uuid))
        run_dir.relative_to(staging)
        publish_candidate = run_dir / "publish"
        try:
            publish_st = publish_candidate.lstat()
        except OSError:
            return _report(
                ok=False,
                code=CODE_PACKAGE_REBIND,
                message="publish directory invalid",
                mode=mode_s,
                package_code=package_report.code,
                run_id=package_report.run_id,
            )
        if stat.S_ISLNK(publish_st.st_mode) or not stat.S_ISDIR(publish_st.st_mode):
            return _report(
                ok=False,
                code=CODE_PACKAGE_REBIND,
                message="publish directory invalid",
                mode=mode_s,
                package_code=package_report.code,
                run_id=package_report.run_id,
            )
        publish = resolve_isolation_path(publish_candidate)
        publish.relative_to(run_dir)
    except Exception:
        return _report(
            ok=False,
            code=CODE_PACKAGE_REBIND,
            message="package path rebind failed",
            mode=mode_s,
            package_code=package_report.code,
            run_id=package_report.run_id,
        )
    # Post-resolve: refuse symlink / non-dir publish (follow-check after resolve).
    try:
        if publish.is_symlink() or not publish.is_dir():
            return _report(
                ok=False,
                code=CODE_PACKAGE_REBIND,
                message="publish directory invalid",
                mode=mode_s,
                package_code=package_report.code,
                run_id=package_report.run_id,
            )
    except OSError:
        return _report(
            ok=False,
            code=CODE_PACKAGE_REBIND,
            message="publish directory invalid",
            mode=mode_s,
            package_code=package_report.code,
            run_id=package_report.run_id,
        )

    wrap = publish / PUBLISH_WRAP
    blob = publish / PUBLISH_BLOB
    manifest = publish / PUBLISH_MANIFEST
    for path in (wrap, blob, manifest):
        try:
            st = path.lstat()
        except OSError:
            return _report(
                ok=False,
                code=CODE_PACKAGE_REBIND,
                message="publish file missing or not regular",
                mode=mode_s,
                package_code=package_report.code,
                run_id=package_report.run_id,
            )
        if stat.S_ISLNK(st.st_mode) or not stat.S_ISREG(st.st_mode):
            return _report(
                ok=False,
                code=CODE_PACKAGE_REBIND,
                message="publish file missing or not regular",
                mode=mode_s,
                package_code=package_report.code,
                run_id=package_report.run_id,
            )
    return PublishedPackageHandle(
        run_id=run_uuid,
        resolved_staging_root=staging,
        resolved_run_dir=run_dir,
        resolved_publish_dir=publish,
        wrap_path=wrap,
        blob_path=blob,
        manifest_path=manifest,
        package_report=package_report,
    )


def _close_generator(gen: object) -> None:
    """Best-effort close; suppress getattr and invocation failures."""

    try:
        close = getattr(gen, "close", None)
    except Exception:
        return
    if callable(close):
        with contextlib.suppress(Exception):
            close()


def _zeroize(buf: bytearray) -> None:
    for i in range(len(buf)):
        buf[i] = 0


async def _maybe_await(value: object) -> object:
    if inspect.isawaitable(value):
        return await value
    return value


def _from_package_fail(package: PackagePreflightReport, mode_s: str) -> RestoreOrchestrationReport:
    return _report(
        ok=False,
        code=package.code,
        message=package.message,
        mode=mode_s,
        run_id=package.run_id,
        package_code=package.code,
    )


def _from_target_fail(
    target: TargetPreflightReport,
    mode_s: str,
    *,
    package: PackagePreflightReport,
) -> RestoreOrchestrationReport:
    return _report(
        ok=False,
        code=target.code,
        message=target.message,
        mode=mode_s,
        run_id=package.run_id,
        package_code=package.code,
        target_code=target.code,
    )


def _report(
    *,
    ok: bool,
    code: str,
    message: str,
    mode: str,
    run_id: str | None = None,
    partial: bool | None = None,
    package_code: str | None = None,
    target_code: str | None = None,
    process_code: str | None = None,
    process_started: bool = False,
    bytes_written: int = 0,
    target_may_be_dirty: bool = False,
    host: str | None = None,
    port: int | None = None,
    database: str | None = None,
) -> RestoreOrchestrationReport:
    return RestoreOrchestrationReport(
        ok=ok,
        code=code,
        message=message,
        mode=mode,
        run_id=run_id,
        partial=partial,
        package_code=package_code,
        target_code=target_code,
        process_code=process_code,
        process_started=process_started,
        bytes_written=bytes_written,
        target_may_be_dirty=target_may_be_dirty,
        host=host,
        port=port,
        database=database,
        schedule="disabled",
    )
