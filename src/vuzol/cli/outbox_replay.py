"""Read-only dry-run and explicit apply entrypoint for dead-letter replay."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import uuid

from vuzol.config import get_runtime_configuration
from vuzol.observability import configure_logging, get_logger
from vuzol.ops.outbox_replay import (
    DeadLetterItem,
    list_dead_letter_items,
    requeue_dead_letter_items,
)
from vuzol.storage import create_engine, create_session_factory, resolve_database_dsn


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)
    raise SystemExit(asyncio.run(_run(args)))


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Review dead-lettered outbox items and requeue them for a fresh retry budget. "
            "Default mode is dry-run; pass --apply to mutate state."
        )
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--dry-run",
        action="store_true",
        default=True,
        help="List eligible items without requeueing (default)",
    )
    mode.add_argument(
        "--apply",
        action="store_true",
        help="Requeue the selected dead-lettered items",
    )
    parser.add_argument(
        "--destination",
        action="append",
        default=[],
        metavar="NAME",
        help="Only consider items with this destination (repeatable)",
    )
    parser.add_argument(
        "--item-id",
        action="append",
        default=[],
        metavar="UUID",
        help="Only consider this outbox item id (repeatable)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit the report as JSON on stdout",
    )
    return parser.parse_args(argv)


def _selected_items(
    args: argparse.Namespace,
) -> tuple[frozenset[str] | None, tuple[uuid.UUID, ...]]:
    destinations = frozenset(args.destination) if args.destination else None
    try:
        item_ids = tuple(uuid.UUID(value) for value in args.item_id)
    except ValueError as error:
        raise SystemExit(f"invalid --item-id: {error}") from error
    return destinations, item_ids


def _report_payload(
    *, mode: str, selected: tuple[DeadLetterItem, ...], requeued: int
) -> dict[str, object]:
    return {
        "mode": mode,
        "selected_count": len(selected),
        "requeued_count": requeued,
        "items": [
            {
                "item_id": str(item.item_id),
                "destination": item.destination,
                "operation_type": item.operation_type,
                "linked_entity_type": item.linked_entity_type,
                "linked_entity_id": str(item.linked_entity_id),
                "attempt_count": item.attempt_count,
                "error_category": item.error_category,
                "created_at": item.created_at.isoformat(),
            }
            for item in selected
        ],
    }


async def _run(args: argparse.Namespace) -> int:
    runtime = get_runtime_configuration(validate_profile_credentials=False)
    settings = runtime.settings
    configure_logging(service=f"{settings.service_name}-outbox-replay", level=settings.log_level)
    destinations, item_ids = _selected_items(args)
    engine = create_engine(settings, resolve_database_dsn(settings))
    factory = create_session_factory(engine)
    try:
        async with factory() as session:
            selected = await list_dead_letter_items(
                session,
                destinations=destinations,
                item_ids=item_ids,
            )
            if args.apply:
                async with session.begin():
                    requeued = await requeue_dead_letter_items(session, items=selected)
            else:
                requeued = 0
    finally:
        await engine.dispose()

    logger = get_logger(__name__)
    logger.info(
        "dead-letter replay finished",
        extra={
            "event": "ops.outbox_replay.finished",
            "mode": "apply" if args.apply else "dry_run",
            "selected_count": len(selected),
            "requeued_count": requeued,
        },
    )
    if args.json:
        payload = _report_payload(
            mode="apply" if args.apply else "dry_run",
            selected=selected,
            requeued=requeued,
        )
        json.dump(payload, sys.stdout, indent=2, sort_keys=True)
        sys.stdout.write("\n")
    else:
        print(
            f"mode={'apply' if args.apply else 'dry_run'} "
            f"selected={len(selected)} requeued={requeued}"
        )
        for item in selected:
            print(
                f"{item.item_id}\t{item.destination}\t{item.operation_type}\t"
                f"attempts={item.attempt_count}\tcategory={item.error_category}\t"
                f"{item.created_at.isoformat()}"
            )
    return 0


if __name__ == "__main__":
    main()
