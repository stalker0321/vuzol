"""Send the daily health summary to the owner's system topic (``--dry-run`` prints it)."""

from __future__ import annotations

import argparse
import asyncio
import os
from datetime import UTC, datetime
from pathlib import Path

import httpx

from vuzol.config import get_runtime_configuration
from vuzol.ops.health_report import database_facts, evaluate, host_facts, render
from vuzol.storage import create_engine, create_session_factory, resolve_database_dsn

SERVICES = (
    "vuzol-executor.service",
    "vuzol-applier.service",
    "vuzol-static-publisher-worker.service",
    "vuzol-project-setup.service",
)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="print instead of sending")
    parser.add_argument("--service", action="append", help="systemd unit to check (repeatable)")
    parser.add_argument("--backup-unit", default="vuzol-backup.service")
    parser.add_argument("--usage-file", type=Path)
    args = parser.parse_args(argv)
    text = asyncio.run(
        _report(
            services=tuple(args.service or SERVICES),
            backup_unit=args.backup_unit,
            usage_file=args.usage_file,
        )
    )
    if args.dry_run:
        print(text)
        return
    asyncio.run(_send(text))


async def _report(*, services: tuple[str, ...], backup_unit: str, usage_file: Path | None) -> str:
    settings = get_runtime_configuration(validate_profile_credentials=False).settings
    engine = create_engine(settings, resolve_database_dsn(settings))
    now = datetime.now(UTC)
    try:
        async with create_session_factory(engine)() as session:
            db = await database_facts(session, now=now)
    finally:
        await engine.dispose()
    host = host_facts(services=services, backup_unit=backup_unit, usage_file=usage_file)
    return render(evaluate(host, db, now=now))


async def _send(text: str) -> None:
    token = os.environ["VUZOL_TELEGRAM_BOT_TOKEN"]
    async with httpx.AsyncClient(timeout=20) as client:
        response = await client.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={
                "chat_id": int(os.environ["VUZOL_HEALTH_CHAT_ID"]),
                "message_thread_id": int(os.environ["VUZOL_HEALTH_THREAD_ID"]),
                "text": text,
                "parse_mode": "HTML",
                "disable_notification": text.startswith("🟢"),
            },
        )
    # Never echo the request: its URL carries the bot token.
    if response.status_code != 200:
        raise SystemExit(f"Telegram refused the health report: HTTP {response.status_code}")


if __name__ == "__main__":
    main()
