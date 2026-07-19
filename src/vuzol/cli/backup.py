"""Manual backup capture CLI (B2). Default dry-run; schedule never enabled."""

from __future__ import annotations

import argparse
import asyncio
import json

from vuzol.config import get_runtime_configuration
from vuzol.config.secrets import ScopedSecretResolver
from vuzol.observability import configure_logging, get_logger
from vuzol.ops.backup.capture import BackupCaptureRunner, CaptureMode
from vuzol.ops.backup.crypto import load_kek_from_env_value, load_kek_from_file_bytes
from vuzol.ops.backup.paths import BackupPathError, ProductionRoots
from vuzol.ops.backup.staging import BackupStagingError, gc_incomplete_runs

# Stable operational code: isolation refusal without path leakage.
_GC_STAGING_PATH_CONFLICT = "preflight_path_conflict"


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
    return parser.parse_args(argv)


async def _run(args: argparse.Namespace) -> int:
    runtime = get_runtime_configuration(validate_profile_credentials=False)
    settings = runtime.settings
    configure_logging(service=f"{settings.service_name}-backup", level=settings.log_level)
    logger = get_logger(__name__)

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
        except (BackupPathError, BackupStagingError, OSError):
            payload = {
                "ok": False,
                "code": _GC_STAGING_PATH_CONFLICT,
                "schedule": "disabled",
            }
            print(json.dumps(payload))
            logger.info(
                "backup.gc_staging",
                extra={
                    "event": "ops.backup.gc_staging",
                    "ok": False,
                    "code": _GC_STAGING_PATH_CONFLICT,
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


def _resolve_backup_dsn(settings: object) -> str:
    from vuzol.config.settings import Settings

    assert isinstance(settings, Settings)
    reference = settings.database_dsn_reference
    if reference is None:
        raise ValueError("database_dsn_reference is required")
    resolver = ScopedSecretResolver(
        access_policy={reference: frozenset({"system:backup"})},
        secret_file_root=settings.secret_file_root,
    )
    secret = resolver.get(reference, "system:backup")
    return secret.get_secret_value()


def _resolve_kek(settings: object) -> bytes:
    from vuzol.config.settings import Settings

    assert isinstance(settings, Settings)
    ref = settings.backup.kek_reference
    assert ref is not None
    if ref.startswith("file:"):
        name = ref.split(":", 1)[1]
        return load_kek_from_file_bytes((settings.secret_file_root / name).read_bytes())
    if ref.startswith("env:"):
        import os

        return load_kek_from_env_value(os.environ[ref.split(":", 1)[1]])
    raise ValueError("unsupported kek reference")


if __name__ == "__main__":
    main()
