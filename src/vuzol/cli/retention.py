"""Read-only dry-run and explicit apply entrypoint for retention sweeping."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import socket
import sys

from vuzol.config import get_runtime_configuration
from vuzol.execution.git import LocalGit
from vuzol.observability import configure_logging, get_logger
from vuzol.ops.retention import RetentionSweeper, RetentionSweepMode
from vuzol.storage import create_engine, create_session_factory, resolve_database_dsn


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)
    raise SystemExit(asyncio.run(_run(args)))


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Sweep expired managed worktrees and artifacts. "
            "Default mode is dry-run; pass --apply to mutate state."
        )
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--dry-run",
        action="store_true",
        default=True,
        help="Report eligible actions without deleting or quarantining (default)",
    )
    mode.add_argument(
        "--apply",
        action="store_true",
        help="Apply eligible cleanup and quarantine actions",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit the full operational report as JSON on stdout",
    )
    return parser.parse_args(argv)


async def _run(args: argparse.Namespace) -> int:
    runtime = get_runtime_configuration(validate_profile_credentials=False)
    settings = runtime.settings
    configure_logging(service=f"{settings.service_name}-retention", level=settings.log_level)
    mode = RetentionSweepMode.APPLY if args.apply else RetentionSweepMode.DRY_RUN
    engine = create_engine(settings, resolve_database_dsn(settings))
    factory = create_session_factory(engine)
    owner = f"{socket.gethostname()}:{os.getpid()}:retention"
    try:
        report = await RetentionSweeper(
            factory,
            worktree_root=settings.worktree_root,
            artifact_root=settings.artifact_root,
            repository_root=settings.repository_root,
            retention=settings.retention,
            owner=owner,
            projects=runtime.registries.projects,
            git=LocalGit(),
        ).run(mode=mode)
    finally:
        await engine.dispose()

    payload = report.to_operational_payload()
    logger = get_logger(__name__)
    logger.info(
        "retention sweep finished",
        extra={
            "event": "ops.retention.finished",
            "mode": report.mode.value,
            "lock_acquired": report.lock_acquired,
            "cleaned_count": report.cleaned_count,
            "skipped_count": report.skipped_count,
            "failure_count": report.failure_count,
            "action_count": len(report.actions),
        },
    )
    if args.json:
        json.dump(payload, sys.stdout, indent=2, sort_keys=True)
        sys.stdout.write("\n")
    else:
        summary = (
            f"mode={report.mode.value} lock={report.lock_acquired} "
            f"cleaned={report.cleaned_count} skipped={report.skipped_count} "
            f"failures={report.failure_count} actions={len(report.actions)}"
        )
        print(summary)
        for action in report.actions:
            print(
                f"{action.outcome.value}\t{action.resource_type}\t"
                f"{action.resource_id}\t{action.reason}"
            )
    if not report.lock_acquired:
        return 2
    if report.failure_count:
        return 1
    return 0


if __name__ == "__main__":
    main()
