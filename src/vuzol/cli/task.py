"""Operator task controls over the transport-neutral application service (WP09).

Identical transitions to the Telegram path, explicit CLI principal, no fake
chat/user IDs. Inspect is read-only. Exit codes: 0 applied/inspected,
2 usage/validation error, 3 domain rejection (stale, unknown, invalid).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import uuid
from decimal import Decimal, InvalidOperation

from vuzol.config import get_runtime_configuration
from vuzol.observability import configure_logging, get_logger
from vuzol.storage import create_engine, create_session_factory, resolve_database_dsn
from vuzol.workflows.application import OwnerDecision, Principal, TaskControlService

_OWNER_COMMANDS = frozenset({"admit_ceiling", "accept_uncertainty", "reconcile_cost"})


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)
    raise SystemExit(asyncio.run(_run(args)))


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Start, pause, cancel, resume or inspect a task (operator CLI)."
    )
    parser.add_argument(
        "command",
        choices=[
            "start",
            "pause",
            "cancel",
            "resume",
            "retry",
            "revalidate",
            "rereview",
            "admit_ceiling",
            "accept_uncertainty",
            "reconcile_cost",
            "inspect",
        ],
    )
    parser.add_argument("--step-id", type=uuid.UUID, default=None, help="Bounded retry step UUID")
    parser.add_argument("--reason", default=None, help="Owner reason for a continuation decision")
    parser.add_argument("--cost", default=None, help="Measured receipt for reconcile_cost")
    parser.add_argument("--usage-id", type=uuid.UUID, default=None, help="Unknown usage row")
    parser.add_argument("--task-id", required=True, help="Task UUID")
    parser.add_argument("--user-id", required=True, type=int, help="Operator user ID")
    parser.add_argument(
        "--expected-version",
        type=int,
        default=None,
        help="Task-level CAS: reject when the task moved (not needed for inspect)",
    )
    parser.add_argument("--json", action="store_true", help="Emit the result as JSON")
    return parser.parse_args(argv)


async def _run(args: argparse.Namespace) -> int:
    try:
        task_id = uuid.UUID(args.task_id)
    except ValueError:
        print("error: --task-id must be a UUID", file=sys.stderr)
        return 2
    decision = None
    if args.command in _OWNER_COMMANDS:
        if not isinstance(args.reason, str) or not args.reason.strip():
            print("error: --reason is required", file=sys.stderr)
            return 2
        cost = None
        if args.cost is not None:
            try:
                cost = Decimal(args.cost)
            except InvalidOperation:
                print("error: --cost must be a positive decimal", file=sys.stderr)
                return 2
        if args.command == "reconcile_cost" and (cost is None or cost <= 0):
            print("error: --cost must be a positive decimal", file=sys.stderr)
            return 2
        decision = OwnerDecision(
            reason=args.reason.strip(),
            cost_units=cost,
            usage_id=args.usage_id,
        )
    runtime = get_runtime_configuration(validate_profile_credentials=False)
    settings = runtime.settings
    configure_logging(service=f"{settings.service_name}-task", level=settings.log_level)
    engine = create_engine(settings, resolve_database_dsn(settings))
    factory = create_session_factory(engine)
    logger = get_logger(__name__)
    try:
        service = TaskControlService(factory)
        try:
            result = await service.execute(
                task_id=task_id,
                command=args.command,
                principal=Principal(user_id=args.user_id, ingress_source="cli"),
                expected_task_version=args.expected_version,
                step_id=getattr(args, "step_id", None),
                decision=decision,
            )
        except ValueError as error:
            logger.info("task command rejected", extra={"event": "ops.task.rejected"})
            print(f"rejected: {error}", file=sys.stderr)
            return 3
    finally:
        await engine.dispose()
    logger.info("task command finished", extra={"event": "ops.task.finished"})
    if args.json:
        json.dump(
            {
                "task_id": str(result.task_id),
                "version": result.version,
                "status": result.status,
                "applied": result.applied,
            },
            sys.stdout,
            indent=2,
            sort_keys=True,
        )
        sys.stdout.write("\n")
    else:
        print(
            f"task={result.task_id} status={result.status} "
            f"version={result.version} applied={result.applied}"
        )
    return 0


if __name__ == "__main__":
    main()
