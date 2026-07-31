"""Manual backup capture and default-off restore drill CLI."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import math
import os
import uuid
from pathlib import Path
from typing import TYPE_CHECKING

from vuzol.config import get_runtime_configuration
from vuzol.config.secrets import ScopedSecretResolver
from vuzol.config.settings import Settings
from vuzol.observability import configure_logging, get_logger
from vuzol.ops.backup.capture import BackupCaptureRunner, CaptureMode
from vuzol.ops.backup.crypto import load_kek_from_env_value, load_kek_from_file_bytes
from vuzol.ops.backup.paths import BackupPathError, ProductionRoots
from vuzol.ops.backup.staging import BackupStagingError, gc_incomplete_runs

if TYPE_CHECKING:
    from vuzol.ops.backup.restore_orchestrator import RestoreOrchestrationReport

# Stable operational codes without path leakage.
_GC_STAGING_PATH_CONFLICT = "preflight_path_conflict"
_GC_STAGING_IO_ERROR = "gc_io_error"
_RESTORE_MESSAGES = {
    "restore_flags_conflict": "restore mode flags conflict",
    "restore_not_permitted": "restore APPLY is not permitted",
    "restore_confirmation_required": "restore APPLY requires explicit confirmation",
    "preflight_staging": "staging root is not configured",
    "preflight_drill": "drill root is not configured",
    "preflight_secret": "required secret could not be resolved",  # pragma: allowlist secret
    "preflight_kek": "KEK could not be resolved",
    "preflight_dsn": "restore DSN identity is invalid",
    "preflight_run_id": "run_id is not a valid UUID",
    "preflight_timeout": "timeout seconds must be positive and finite",
    "lock_probe_unreachable": "capture lock probe unreachable",
    "lock_probe_failed": "capture lock probe unlock failed",
    "lock_busy_capture": "capture lock held",
    "restore_failed": "restore failed",
}
_RESTORE_EXIT2 = frozenset({"lock_busy_capture", "cancelled", "restore_timeout"})


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)
    raise SystemExit(asyncio.run(_run(args)))


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Manual partial postgres backup capture. "
            "Default is dry-run; pass --apply only when capture_cli_permitted=true. "
            "Timers are not implemented (schedule=disabled)."
        )
    )
    sub = parser.add_subparsers(dest="command", required=True)
    capture = sub.add_parser("capture", help="Dry-run or apply local encrypted PG capture")
    capture.add_argument(
        "--dry-run",
        action="store_true",
        default=True,
        help="Preflight only; no ciphertext (default)",
    )
    capture.add_argument(
        "--apply",
        action="store_true",
        help="Apply capture (requires capture_cli_permitted)",
    )
    capture.add_argument("--json", action="store_true", help="JSON report on stdout")
    gc_p = sub.add_parser("gc-staging", help="Remove incomplete staging runs")
    gc_p.add_argument("--json", action="store_true")
    restore = sub.add_parser(
        "restore",
        help="Dry-run, verify, or apply a partial PostgreSQL restore drill",
        allow_abbrev=False,
    )
    restore.add_argument("--run-id", required=True, help="Published package run UUID")
    restore.add_argument(
        "--staging-root",
        type=Path,
        default=None,
        help="Staging root override; defaults to backup settings",
    )
    restore.add_argument("--dry-run", action="store_true", default=False)
    restore.add_argument("--apply", action="store_true")
    restore.add_argument("--verify-crypto", action="store_true")
    restore.add_argument(
        "--i-understand-partial-postgres-only",
        action="store_true",
        help="Required acknowledgement for APPLY",
    )
    restore.add_argument("--json", action="store_true")
    restore.add_argument("--timeout-seconds", type=float, default=None)
    restore.add_argument(
        "--allow-non-empty-target",
        action="store_true",
        help="Lab only: disable the default empty-target check",
    )
    restore.add_argument(
        "--production-dsn-reference",
        default=None,
        help="Production DSN secret reference; never a raw DSN",
    )
    restore.add_argument(
        "--restore-dsn-reference",
        default=None,
        help="Restore DSN secret reference; never a raw DSN",
    )
    return parser.parse_args(argv)


async def _run(args: argparse.Namespace) -> int:
    runtime = get_runtime_configuration(validate_profile_credentials=False)
    settings = runtime.settings
    configure_logging(service=f"{settings.service_name}-backup", level=settings.log_level)
    logger = get_logger(__name__)

    if args.command == "restore":
        return await _run_restore(args, settings, logger)
    if args.command == "gc-staging":
        if settings.backup.staging_root is None:
            print(json.dumps({"ok": False, "code": "preflight_staging", "schedule": "disabled"}))
            return 1
        production = ProductionRoots(
            repository_root=settings.repository_root,
            worktree_root=settings.worktree_root,
            artifact_root=settings.artifact_root,
            secret_file_root=settings.secret_file_root,
        )
        try:
            # Isolation reasserted inside gc_incomplete_runs before traverse/delete.
            removed = gc_incomplete_runs(settings.backup.staging_root, production)
        except (BackupPathError, BackupStagingError):
            # Isolation / containment refusal (C1) — never include exception text (paths).
            code = _GC_STAGING_PATH_CONFLICT
            payload = {"ok": False, "code": code, "schedule": "disabled"}
            print(json.dumps(payload))
            logger.info(
                "backup.gc_staging",
                extra={
                    "event": "ops.backup.gc_staging",
                    "ok": False,
                    "code": code,
                    "schedule": "disabled",
                },
            )
            return 1
        except OSError:
            # Staging I/O failure distinct from isolation (C1) — still no path leakage.
            code = _GC_STAGING_IO_ERROR
            payload = {"ok": False, "code": code, "schedule": "disabled"}
            print(json.dumps(payload))
            logger.info(
                "backup.gc_staging",
                extra={
                    "event": "ops.backup.gc_staging",
                    "ok": False,
                    "code": code,
                    "schedule": "disabled",
                },
            )
            return 1
        payload = {"ok": True, "removed": removed, "schedule": "disabled"}
        if args.json:
            print(json.dumps(payload))
        logger.info("backup.gc_staging", extra={"event": "ops.backup.gc_staging", **payload})
        return 0

    mode = CaptureMode.APPLY if args.apply else CaptureMode.DRY_RUN
    dsn: str | None = None
    kek: bytes | None = None
    try:
        dsn = _resolve_backup_dsn(settings)
    except Exception:
        dsn = None
    if mode is CaptureMode.APPLY and settings.backup.kek_reference:
        try:
            kek = _resolve_kek(settings)
        except Exception:
            kek = None

    runner = BackupCaptureRunner(settings, dsn=dsn, kek_bytes=kek)
    report = await runner.run(mode=mode)
    payload = report.to_operational_payload()
    logger.info(
        "backup.capture.finished",
        extra={
            "event": "ops.backup.capture.finished",
            "schedule": "disabled",
            "ok": report.ok,
            "code": report.code,
            "mode": report.mode.value,
        },
    )
    if args.json:
        print(json.dumps(payload, sort_keys=True))
    else:
        print(f"{report.code}: {report.message} (schedule=disabled)")
    if report.code in {"lock_busy", "lock_busy_retention", "cancelled"}:
        return 2
    return 0 if report.ok else 1


async def _run_restore(args: argparse.Namespace, settings: Settings, logger: logging.Logger) -> int:
    # Keep capture/gc cold paths independent from the restore stack.
    from vuzol.ops.backup.postgres_dump import parse_dump_identity
    from vuzol.ops.backup.restore_cli_hooks import (
        LockProbeFailed,
        LockProbeUnreachable,
        make_assert_empty_target,
        make_b34_lock_always_true,
        probe_capture_lock_session,
    )
    from vuzol.ops.backup.restore_orchestrator import RestoreMode, run_restore_orchestration

    if args.apply and (args.dry_run or args.verify_crypto):
        return _restore_fail("restore_flags_conflict", json_out=args.json, logger=logger)

    if args.apply:
        if settings.backup.restore_cli_permitted is not True:
            return _restore_fail("restore_not_permitted", json_out=args.json, logger=logger)
        if args.i_understand_partial_postgres_only is not True:
            return _restore_fail(
                "restore_confirmation_required",
                json_out=args.json,
                logger=logger,
            )
        mode = RestoreMode.APPLY
        apply_authorized = True
        verify_crypto = False
    else:
        mode = RestoreMode.DRY_RUN
        apply_authorized = False
        verify_crypto = args.verify_crypto is True

    staging_root = args.staging_root or settings.backup.staging_root
    if staging_root is None:
        return _restore_fail("preflight_staging", json_out=args.json, logger=logger)
    if settings.backup.drill_root is None:
        return _restore_fail("preflight_drill", json_out=args.json, logger=logger)

    try:
        run_id = uuid.UUID(str(args.run_id))
    except (ValueError, TypeError, AttributeError):
        return _restore_fail("preflight_run_id", json_out=args.json, logger=logger)

    try:
        timeout = _restore_timeout(
            args.timeout_seconds,
            settings.backup.restore_overall_timeout_seconds,
        )
    except ValueError:
        return _restore_fail("preflight_timeout", json_out=args.json, logger=logger)

    production_ref = args.production_dsn_reference or settings.database_dsn_reference
    restore_ref = args.restore_dsn_reference or settings.backup.restore_dsn_reference
    if production_ref is None or restore_ref is None:
        return _restore_fail("preflight_secret", json_out=args.json, logger=logger)
    try:
        production_dsn = _resolve_scoped_secret(settings, production_ref)
        restore_dsn = _resolve_scoped_secret(settings, restore_ref)
    except Exception:
        return _restore_fail("preflight_secret", json_out=args.json, logger=logger)

    try:
        identity = parse_dump_identity(restore_dsn)
        restore_user = identity.user
        restore_database = identity.database
        del identity
    except Exception:
        return _restore_fail("preflight_dsn", json_out=args.json, logger=logger)

    kek_buffer: bytearray | None = None
    try:
        if verify_crypto or mode is RestoreMode.APPLY:
            if settings.backup.kek_reference is None:
                return _restore_fail("preflight_kek", json_out=args.json, logger=logger)
            try:
                kek_buffer = bytearray(_resolve_kek(settings))
            except Exception:
                return _restore_fail("preflight_kek", json_out=args.json, logger=logger)
            kek: bytes | None = bytes(kek_buffer)
        else:
            kek = None

        production = ProductionRoots(
            repository_root=settings.repository_root,
            worktree_root=settings.worktree_root,
            artifact_root=settings.artifact_root,
            secret_file_root=settings.secret_file_root,
        )
        lock_hook = None
        empty_hook = None
        if mode is RestoreMode.APPLY:
            if settings.backup.restore_probe_capture_lock:
                try:
                    lock_state = await probe_capture_lock_session(production_dsn)
                except LockProbeUnreachable:
                    return _restore_fail(
                        "lock_probe_unreachable",
                        json_out=args.json,
                        logger=logger,
                    )
                except LockProbeFailed:
                    return _restore_fail("lock_probe_failed", json_out=args.json, logger=logger)
                if lock_state == "busy":
                    return _restore_fail(
                        "lock_busy_capture",
                        json_out=args.json,
                        logger=logger,
                    )
                if lock_state != "free":
                    return _restore_fail("lock_probe_failed", json_out=args.json, logger=logger)
                lock_hook = make_b34_lock_always_true()
            if settings.backup.restore_require_empty_target and not args.allow_non_empty_target:
                empty_hook = make_assert_empty_target(restore_dsn)

        report = await run_restore_orchestration(
            mode=mode,
            apply_authorized=apply_authorized,
            staging_root=staging_root,
            run_id=run_id,
            production=production,
            production_dsn=production_dsn,
            restore_dsn=restore_dsn,
            drill_root=settings.backup.drill_root,
            required_database_suffix=settings.backup.drill_database_name_suffix,
            allow_local_hosts_only=True,
            kek=kek,
            verify_crypto=verify_crypto,
            postgres_container=settings.backup.postgres_container,
            restore_user=restore_user,
            restore_database=restore_database,
            overall_timeout_seconds=timeout,
            assert_empty_target=empty_hook,
            probe_capture_lock=lock_hook,
        )
        return _emit_restore_report(report, json_out=args.json, logger=logger)
    finally:
        if kek_buffer is not None:
            for index in range(len(kek_buffer)):
                kek_buffer[index] = 0


def _restore_timeout(cli_value: float | None, configured: float | None) -> float | None:
    value = cli_value if cli_value is not None else configured
    if value is None:
        return None
    if not math.isfinite(value) or value <= 0:
        raise ValueError("timeout must be positive and finite")
    return float(value)


def _restore_fail(code: str, *, json_out: bool, logger: logging.Logger) -> int:
    message = _RESTORE_MESSAGES.get(code, _RESTORE_MESSAGES["restore_failed"])
    payload = {"ok": False, "code": code, "message": message, "schedule": "disabled"}
    if json_out:
        print(json.dumps(payload, sort_keys=True))
    else:
        print(f"{code}: {message} (schedule=disabled)")
    logger.info(
        "backup.restore.finished",
        extra={
            "event": "ops.backup.restore.finished",
            "ok": False,
            "code": code,
            "schedule": "disabled",
        },
    )
    return 2 if code in _RESTORE_EXIT2 else 1


def _emit_restore_report(
    report: RestoreOrchestrationReport,
    *,
    json_out: bool,
    logger: logging.Logger,
) -> int:
    payload = report.to_operational_payload()
    if json_out:
        print(json.dumps(payload, sort_keys=True))
    else:
        print(f"{report.code}: {report.message} (schedule=disabled)")
    logger.info(
        "backup.restore.finished",
        extra={
            "event": "ops.backup.restore.finished",
            "ok": report.ok,
            "code": report.code,
            "mode": report.mode,
            "schedule": "disabled",
        },
    )
    if report.code in _RESTORE_EXIT2:
        return 2
    return 0 if report.ok else 1


def _resolve_scoped_secret(settings: Settings, reference: str) -> str:
    resolver = ScopedSecretResolver(
        access_policy={reference: frozenset({"system:backup"})},
        secret_file_root=settings.secret_file_root,
    )
    return resolver.get(reference, "system:backup").get_secret_value()


def _resolve_backup_dsn(settings: object) -> str:
    assert isinstance(settings, Settings)
    reference = settings.database_dsn_reference
    if reference is None:
        raise ValueError("database_dsn_reference is required")
    return _resolve_scoped_secret(settings, reference)


def _resolve_kek(settings: object) -> bytes:
    assert isinstance(settings, Settings)
    ref = settings.backup.kek_reference
    assert ref is not None
    if ref.startswith("file:"):
        name = ref.split(":", 1)[1]
        root = settings.secret_file_root.resolve()
        requested = Path(name)
        candidate = requested if requested.is_absolute() else root / requested
        candidate = candidate.resolve()
        try:
            candidate.relative_to(root)
        except ValueError as error:
            raise ValueError("KEK file escapes configured secret root") from error
        return load_kek_from_file_bytes(candidate.read_bytes())
    if ref.startswith("env:"):
        return load_kek_from_env_value(os.environ[ref.split(":", 1)[1]])
    raise ValueError("unsupported kek reference")


if __name__ == "__main__":
    main()
